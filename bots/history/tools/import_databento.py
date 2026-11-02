"""Import a Databento GLBX.MDP3 ohlcv-1m purchase (every contract) into the lake.

    pip install databento-dbn zstandard        # a decoder only: no API key
    python -I bots/history/tools/import_databento.py OUT FREEZE SOURCE [SOURCE ...] \\
        [--exclude DAY:REASON[,DAY:REASON...]]

    OUT      e.g. /srv/fks-lake/raw/cme/databento (a new folder, append-only)
    FREEZE   the purchase's hash freeze: lines `FILE <name> ... sha256=<hex>`
             (the staging run's fullverify.log). Every source must match it,
             or nothing is imported.
    SOURCE   the .dbn.zst files, or folders of them (ROOT_YEAR.dbn.zst)
    --exclude  days the vendor flagged as degraded, written to each root's
             excluded_days.csv: the search doesn't trade them

What a purchase with parent symbology (ES.FUT, NQ.FUT, GC.FUT) holds, and
what is done with it:

  * every contract's 1-minute bars, with CALENDAR SPREADS among them
    (ESM5-ESH6; GC has ~1,700 spread instruments a year): only outrights
    (ROOT + month code + one year digit) are kept, the rest counted;
  * one-digit years (ESH5): the year is the first one on or after the bar's
    own year that ends in that digit (a contract has no bars after it
    expires, and none trades ten years ahead);
  * prices are fixed-point (x 1e9); a bar's ts_event is its start, UTC.

The roll is the CME recorder's (tools/contracts.py): one contract per UTC
day, by the previous weekday's volume, never back. Two streaming passes per
root (the day's volumes, then the chosen contract's bars), so memory stays
at one day of bars.

The lake gets the recorder's layout (contracts.py), plus _manifest.jsonl
naming every source file by its frozen sha256. THE LICENCE: Databento
prohibits redistribution. The lake is local; nothing here may be committed,
pushed, or put in a backup that leaves this machine (fks-state's manifest
leaves the lake out). Results computed from it are not the restricted act.
"""

from __future__ import annotations

import collections
import hashlib
import json
import re
import sys
from datetime import UTC, date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from contracts import (  # noqa: E402
    MONTHS,
    Contract,
    ImportError_,
    Row,
    choose_days,
    csv_gz,
    gap,
    new_root_folder,
    utc_day,
    write_excluded,
    write_rolls,
)

OUTRIGHT = re.compile(r"^(?P<root>[A-Z0-9]{1,4}?)(?P<month>[FGHJKMNQUVXZ])(?P<y>\d)$")
FILE_NAME = re.compile(r"^(?P<root>[A-Z0-9]{1,4})_(?P<year>\d{4})\.dbn\.zst$")
PRICE = 1e9  # fixed-point scale: divide (exact for tick prices), never multiply by 1e-9


def contract_year(digit: int, bar_year: int) -> int:
    """The first year on or after the bar's that ends in `digit`."""
    return bar_year + (digit - bar_year) % 10


def outright(symbol: str, root: str, bar_day: date) -> Contract | None:
    """The contract an outright symbol names on `bar_day`; None for a spread
    or another root's symbol."""
    m = OUTRIGHT.match(symbol)
    if not m or m["root"] != root:
        return None
    return Contract(root, MONTHS.index(m["month"]) + 1, contract_year(int(m["y"]), bar_day.year))


def read_freeze(path: Path) -> dict[str, str]:
    out = {}
    for line in path.read_text().splitlines():
        if line.startswith("FILE "):
            parts = line.split()
            sha = next((p.split("=", 1)[1] for p in parts if p.startswith("sha256=")), None)
            if sha:
                out[parts[1]] = sha
    if not out:
        raise ImportError_(f"{path}: no `FILE <name> ... sha256=` lines: not a hash freeze")
    return out


def verify(files: list[Path], freeze: dict[str, str]) -> dict[Path, str]:
    """Every source's sha256, refusing any file the freeze doesn't name or match."""
    out = {}
    for f in files:
        want = freeze.get(f.name)
        if want is None:
            raise ImportError_(f"{f.name} isn't in the freeze")
        h = hashlib.sha256()
        with f.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        if h.hexdigest() != want:
            raise ImportError_(f"{f.name}: sha256 {h.hexdigest()[:12]}… doesn't match the freeze {want[:12]}…")
        out[f] = want
    return out


def bars(path: Path):
    """(symbol, row) for every OHLCV record, in the file's (time) order."""
    import databento_dbn as dbn
    import zstandard

    dec = dbn.DBNDecoder()
    id_sym: dict[int, str] = {}
    with path.open("rb") as f:
        reader = zstandard.ZstdDecompressor().stream_reader(f)
        while chunk := reader.read(1 << 22):
            dec.write(chunk)
            for r in dec.decode():
                if isinstance(r, dbn.Metadata):
                    for raw, intervals in r.mappings.items():
                        for iv in intervals:
                            id_sym[int(iv["symbol"])] = raw
                    continue
                if type(r).__name__ != "OHLCVMsg":
                    continue
                sym = id_sym.get(r.instrument_id)
                if sym is None:
                    raise ImportError_(f"{path.name}: instrument {r.instrument_id} has no symbol mapping")
                yield sym, (r.ts_event // 1_000_000, r.open / PRICE, r.high / PRICE, r.low / PRICE,
                            r.close / PRICE, float(r.volume))


def import_root(out: Path, root: str, files: list[Path], excluded: dict[date, str], read=bars) -> dict:
    """One root: volumes, the roll, then the chosen contract's bars per day."""
    volumes: dict[Contract, dict[date, float]] = collections.defaultdict(lambda: collections.defaultdict(float))
    skipped: collections.Counter = collections.Counter()
    for f in files:
        for sym, r in read(f):
            k = outright(sym, root, utc_day(r[0]))
            if k is None:
                skipped["spread" if "-" in sym else "other"] += 1
                continue
            volumes[k][utc_day(r[0])] += r[5]
    if not volumes:
        raise ImportError_(f"{root}: no outright bars in {[f.name for f in files]}")
    chosen, rolls = choose_days({k: dict(v) for k, v in volumes.items()})
    roll_from = {d: a for d, a, _ in rolls}
    folder = new_root_folder(out, root)
    written: list[dict] = []
    gaps: dict[date, float | str] = {}
    day: date | None = None
    rows: list[Row] = []
    old_rows: list[Row] = []
    seen: dict[int, Row] = {}

    def flush() -> None:
        if day is None or not rows:
            return
        rows.sort()
        data = csv_gz(rows)
        path = folder / "1m" / f"{root}-1m-{day.isoformat()}.csv.gz"
        path.write_bytes(data)
        written.append({"path": str(path.relative_to(out)), "sha256": hashlib.sha256(data).hexdigest(),
                        "bytes": len(data), "rows": len(rows), "symbol": root, "contract": chosen[day].code})
        if day in roll_from:
            gaps[day] = gap(old_rows, rows)

    for f in files:
        for sym, r in read(f):
            d = utc_day(r[0])
            k = outright(sym, root, d)
            if k is None or d not in chosen:
                continue
            if d != day:
                flush()
                day, rows, old_rows, seen = d, [], [], {}
            if k == chosen[d]:
                if r[0] in seen:
                    if seen[r[0]] != r:
                        raise ImportError_(f"{root} {k.code}: two different bars at {r[0]}")
                    continue
                seen[r[0]] = r
                rows.append(r)
            elif roll_from.get(d) == k:
                old_rows.append(r)
    flush()
    write_rolls(folder, [{"day": d.isoformat(), "from_contract": a.code, "to_contract": b.code,
                          "gap_points": gaps.get(d, ""), "detected_by": "volume"} for d, a, b in rolls])
    if excluded:
        write_excluded(folder, excluded)
    days = sorted(chosen)
    return {"contracts": len(volumes), "days": len(days), "rolls": len(rolls), "files": written,
            "skipped": dict(skipped), "first": days[0].isoformat(), "last": days[-1].isoformat()}


def parse_exclude(text: str) -> dict[date, str]:
    out = {}
    for item in filter(None, (x.strip() for x in text.split(","))):
        day, _, reason = item.partition(":")
        out[date.fromisoformat(day)] = reason or "excluded"
    return out


def run(out: Path, freeze_path: Path, sources: list[Path], excluded: dict[date, str]) -> dict:
    files = sorted({p for s in sources for p in (sorted(s.glob("*.dbn.zst")) if s.is_dir() else [s])})
    by_root: dict[str, list[Path]] = collections.defaultdict(list)
    for f in files:
        m = FILE_NAME.match(f.name)
        if not m:
            raise ImportError_(f"{f.name}: not ROOT_YEAR.dbn.zst")
        by_root[m["root"]].append(f)
    if not by_root:
        raise ImportError_("no .dbn.zst files in the sources")
    hashes = verify(files, read_freeze(freeze_path))
    now = datetime.now(UTC).isoformat(timespec="seconds")
    summary = {}
    out.mkdir(parents=True, exist_ok=True)
    manifest = [{"source_file": f.name, "sha256": hashes[f], "imported_at": now,
                 "source": "databento GLBX.MDP3 ohlcv-1m (parent symbology)", "licence": "no redistribution"}
                for f in files]
    for root, root_files in sorted(by_root.items()):
        s = import_root(out, root, sorted(root_files, key=lambda p: p.name), excluded)
        manifest += [{**w, "dataset": "bars", "imported_at": now} for w in s.pop("files")]
        summary[root] = s
    with (out / "_manifest.jsonl").open("a") as f:
        for m in manifest:
            f.write(json.dumps(m) + "\n")
    return summary


def main(argv: list[str]) -> int:
    excluded: dict[date, str] = {}
    if "--exclude" in argv:
        i = argv.index("--exclude")
        excluded = parse_exclude(argv[i + 1])
        argv = argv[:i] + argv[i + 2:]
    if len(argv) < 3:
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 2
    try:
        summary = run(Path(argv[0]), Path(argv[1]), [Path(a) for a in argv[2:]], excluded)
    except ImportError_ as e:
        print(f"refused: {e}", file=sys.stderr)
        return 1
    for root, s in summary.items():
        print(f"{root}: {s['contracts']} contracts, {s['days']} days {s['first']}..{s['last']}, "
              f"{s['rolls']} rolls, skipped {s['skipped']}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
