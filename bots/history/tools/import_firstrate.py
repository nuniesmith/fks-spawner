"""Import bought FirstRate Data futures history (individual contracts) into the lake.

    python -I bots/history/tools/import_firstrate.py OUT SOURCE [SOURCE ...]

    OUT     e.g. /srv/fks-lake/raw/cme/firstrate (a new folder, append-only)
    SOURCE  FirstRate contract files (ES_M24_1min.txt), .zip archives of
            them, or folders holding either

FirstRate's contract files are headerless CSV, one file per contract,
`timestamp,open,high,low,close,volume`, the timestamp the bar's START in
US Eastern time (EST/EDT). They are bought per contract, not as their
continuous series (back-adjusted prices can't be traded as quoted).

The lake gets the layout the CME recorder writes (bots/cme-history), so
the search reads it like any CME folder (`raw/cme/firstrate 1m A..B`):

    <ROOT>/1m/<ROOT>-1m-<YYYY-MM-DD>.csv.gz   open_time_ms,open,high,low,close,volume
                                              one UTC day, ONE contract
    <ROOT>/rolls.csv                          day, from_contract, to_contract,
                                              gap_points, detected_by
    _manifest.jsonl                           one line per file written, and
                                              one per source file read (sha256)

THE ROLL, the recorder's rule (tools/contracts.py, as cme_history does it):
a UTC day follows the contract with the most volume on the previous WEEKDAY
(known before the day starts), never moving back to an earlier contract
month. On a day
the followed contract has no bars any more (it expired), the most-traded
later one is taken. A roll day is listed in rolls.csv, and the search
doesn't trade a listed day.

Refused rather than repaired: a file name that isn't ROOT_MYY_1min, a row
that doesn't parse, high below low or below open/close, a timestamp Eastern
time skips (DST spring-forward). A timestamp repeated in the hour Eastern
time repeats (DST fall-back) is read as the second occurrence the second
time. Exact duplicate rows are dropped; a minute with two different bars
in one contract is refused.

Data the search already looked at must not be imported as if it were
unseen: for the futures hypotheses, history before 2024-09-09 is unseen,
the massive-cache window after it is not. The dataset spec at registration
decides what is searched; this tool imports what it is given.
"""

from __future__ import annotations

import collections
import hashlib
import io
import json
import re
import sys
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# The importers' shared code sits beside this file (run as a script, with -I,
# its folder isn't on sys.path by itself).
sys.path.insert(0, str(Path(__file__).resolve().parent))

from contracts import (  # noqa: E402
    HEADER,
    MONTHS,
    Contract,
    ImportError_,
    Row,
    choose_days,
    csv_gz,
    gap,
    new_root_folder,
    utc_day,
    write_rolls,
)

EASTERN = ZoneInfo("America/New_York")
NAME = re.compile(r"^(?P<root>[A-Z0-9]{1,4})_(?P<month>[FGHJKMNQUVXZ])(?P<yy>\d{2})_1min\.(txt|csv)$")
__all__ = ["HEADER", "MONTHS", "Contract", "ImportError_", "run", "parse", "utc_ms", "contract_of", "stitch"]


def contract_of(name: str) -> Contract:
    m = NAME.match(Path(name).name)
    if not m:
        raise ImportError_(f"{name}: not a FirstRate contract file name (ROOT_MYY_1min.txt)")
    return Contract(m["root"], MONTHS.index(m["month"]) + 1, 2000 + int(m["yy"]))


def utc_ms(stamp: str, fold: int) -> int:
    """An Eastern wall-clock timestamp as UTC epoch milliseconds."""
    local = datetime.strptime(stamp.strip()[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=EASTERN, fold=fold)
    # A wall time that doesn't exist (spring forward) doesn't survive the round trip.
    back = local.astimezone(UTC).astimezone(EASTERN)
    if back.replace(tzinfo=None) != local.replace(tzinfo=None):
        raise ImportError_(f"{stamp}: not a time in US Eastern (the DST gap)")
    return int(local.astimezone(UTC).timestamp() * 1000)


def parse(name: str, text: str) -> list[Row]:
    """One contract file's bars, in time order."""
    out: dict[int, Row] = {}
    seen_wall: set[str] = set()
    for n, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        parts = line.split(",")
        if len(parts) != 6:
            raise ImportError_(f"{name}:{n}: {len(parts)} columns, expected 6")
        try:
            o, h, lo, c, v = (float(x) for x in parts[1:])
        except ValueError:
            raise ImportError_(f"{name}:{n}: a number doesn't parse") from None
        wall = parts[0].strip()
        try:
            t = utc_ms(wall, 1 if wall in seen_wall else 0)
        except ValueError:
            raise ImportError_(f"{name}:{n}: timestamp {wall!r} doesn't parse") from None
        seen_wall.add(wall)
        if not (h >= max(o, c) and lo <= min(o, c) and h >= lo) or v < 0:
            raise ImportError_(f"{name}:{n}: an impossible bar {parts[1:]}")
        row = (t, o, h, lo, c, v)
        if t in out and out[t] != row:
            raise ImportError_(f"{name}:{n}: two different bars at {wall}")
        out[t] = row
    return [out[t] for t in sorted(out)]


def sources(paths: list[Path]):
    """(name, text, sha256 of the file it came from, that file) for every contract file."""
    for p in paths:
        if p.is_dir():
            yield from sources(sorted(q for q in p.iterdir() if q.suffix.lower() in (".txt", ".csv", ".zip")))
            continue
        data = p.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        if p.suffix.lower() == ".zip":
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                for member in sorted(z.namelist()):
                    if NAME.match(Path(member).name):
                        yield Path(member).name, z.read(member).decode(), digest, p
        else:
            yield p.name, data.decode(), digest, p


def stitch(by_contract: dict[Contract, list[Row]]) -> tuple[dict[date, tuple[Contract, list[Row]]], list[dict]]:
    """One contract per UTC day (contracts.choose_days), with its rows."""
    days: dict[Contract, dict[date, list[Row]]] = {}
    for k, rows in by_contract.items():
        per: dict[date, list[Row]] = collections.defaultdict(list)
        for r in rows:
            per[utc_day(r[0])].append(r)
        days[k] = per
    volumes = {k: {d: sum(r[5] for r in rows) for d, rows in per.items()} for k, per in days.items()}
    chosen, rolls = choose_days(volumes)
    out = {d: (k, days[k][d]) for d, k in chosen.items()}
    return out, [{"day": d.isoformat(), "from_contract": a.code, "to_contract": b.code,
                  "gap_points": gap(days[a].get(d, []), days[b][d]), "detected_by": "volume"}
                 for d, a, b in rolls]


def run(out: Path, paths: list[Path]) -> dict:
    by_root: dict[str, dict[Contract, list[Row]]] = collections.defaultdict(dict)
    provenance: dict[Path, str] = {}
    for name, text, digest, origin in sources(paths):
        k = contract_of(name)
        if k in by_root[k.root]:
            raise ImportError_(f"{name}: contract {k.code} given twice")
        by_root[k.root][k] = parse(name, text)
        provenance[origin] = digest
    if not by_root:
        raise ImportError_("no FirstRate contract files in the sources")
    now = datetime.now(UTC).isoformat(timespec="seconds")
    manifest: list[dict] = [
        {"source_file": str(p), "sha256": d, "imported_at": now, "source": "firstrate (individual contracts)"}
        for p, d in sorted(provenance.items())
    ]
    summary: dict = {}
    for root, contracts in sorted(by_root.items()):
        chosen, rolls = stitch(contracts)
        folder = new_root_folder(out, root)
        for d, (k, rows) in sorted(chosen.items()):
            path = folder / "1m" / f"{root}-1m-{d.isoformat()}.csv.gz"
            data = csv_gz(rows)
            path.write_bytes(data)
            manifest.append({"path": str(path.relative_to(out)), "sha256": hashlib.sha256(data).hexdigest(),
                             "bytes": len(data), "rows": len(rows), "symbol": root, "contract": k.code,
                             "dataset": "bars", "imported_at": now})
        write_rolls(folder, rolls)
        days = sorted(chosen)
        summary[root] = {"contracts": len(contracts), "days": len(days), "rolls": len(rolls),
                         "first": days[0].isoformat() if days else None,
                         "last": days[-1].isoformat() if days else None}
    with (out / "_manifest.jsonl").open("a") as f:
        for m in manifest:
            f.write(json.dumps(m) + "\n")
    return summary


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 2
    try:
        summary = run(Path(argv[0]), [Path(a) for a in argv[1:]])
    except ImportError_ as e:
        print(f"refused: {e}", file=sys.stderr)
        return 1
    for root, s in summary.items():
        print(f"{root}: {s['contracts']} contracts, {s['days']} days {s['first']}..{s['last']}, {s['rolls']} rolls")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
