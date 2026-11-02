"""Import the futures repo's Massive cache into the lake, once.

    python -I bots/history/tools/import_futures_cache.py \
        ~/github/futures/data/futures.sqlite /srv/fks-lake/raw/cme/massive-cache

The cache (`~/github/futures`, `bars` + `rolls`) holds CME 1-minute bars per
ROOT (ES, NQ, GC, MGC), already stitched into one series by the futures repo,
2024-09-09..2026-09-11. Massive's API now only reaches back about two years,
so this cache is the only copy of its oldest months.

Written to the lake as raw files, never edited afterwards:

    <ROOT>/1m/<ROOT>-1m-<YYYY-MM>.csv.gz   open_time_ms,open,high,low,close,volume
    <ROOT>/rolls.csv                        the cache's roll records, as stored
    <ROOT>/roll_flips.csv                   days the series flips between two
                                            contracts minute by minute
    _manifest.jsonl                         one line per file: sha256, bytes, rows

THE KNOWN DEFECT, recorded rather than repaired: on some roll days the
stitched series interleaves two contracts' bars (ES 2024-09-16 alternates
5633 / 5694 minute by minute). `roll_flips.csv` lists every day with at
least 4 jumps of 60% of the smallest recorded roll gap. A family that reads
this data must not trade those days. GC and MGC have none.
"""
from __future__ import annotations

import collections
import csv
import gzip
import hashlib
import io
import json
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOTS = ("ES", "NQ", "GC", "MGC")
FLIP_SHARE = 0.6
FLIP_MIN = 4


def write(path: Path, data: bytes, manifest: list[dict], rows: int, kind: str, root: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise SystemExit(f"{path} exists: the lake is append-only, remove the import by hand first")
    path.write_bytes(data)
    manifest.append({
        "path": str(path.relative_to(path.parents[2] if kind == "bars" else path.parents[1])),
        "sha256": hashlib.sha256(data).hexdigest(),
        "bytes": len(data),
        "rows": rows,
        "dataset": kind,
        "symbol": root,
        "source": "massive via ~/github/futures data/futures.sqlite (bars, rolls)",
        "imported_at": datetime.now(UTC).isoformat(timespec="seconds"),
    })


def csv_bytes(header: list[str], rows: list[tuple], gz: bool) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    raw = buf.getvalue().encode()
    return gzip.compress(raw, mtime=0) if gz else raw


def main(db: str, out: str) -> int:
    src = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    lake = Path(out)
    manifest: list[dict] = []
    for root in ROOTS:
        rows = src.execute(
            "SELECT ts, open, high, low, close, volume FROM bars "
            "WHERE symbol = ? AND bar_minutes = 1 AND source = 'massive' ORDER BY ts",
            (root,),
        ).fetchall()
        by_month: dict[str, list[tuple]] = collections.defaultdict(list)
        for ts, o, h, lo, c, v in rows:
            month = datetime.fromtimestamp(ts, UTC).strftime("%Y-%m")
            by_month[month].append((ts * 1000, o, h, lo, c, v))
        for month, bars in sorted(by_month.items()):
            data = csv_bytes(["open_time_ms", "open", "high", "low", "close", "volume"], bars, gz=True)
            write(lake / root / "1m" / f"{root}-1m-{month}.csv.gz", data, manifest, len(bars), "bars", root)

        rolls = src.execute(
            "SELECT day, from_contract, to_contract, gap_points, detected_by FROM rolls "
            "WHERE symbol = ? ORDER BY day, detected_by",
            (root,),
        ).fetchall()
        write(lake / root / "rolls.csv",
              csv_bytes(["day", "from_contract", "to_contract", "gap_points", "detected_by"], rolls, gz=False),
              manifest, len(rolls), "rolls", root)

        threshold = FLIP_SHARE * min(abs(r[3]) for r in rolls) if rolls else float("inf")
        flips: collections.Counter = collections.Counter()
        prev = None
        for ts, o, _h, _lo, c, _v in rows:
            if prev is not None and abs(o - prev) >= threshold:
                flips[datetime.fromtimestamp(ts, UTC).date().isoformat()] += 1
            prev = c
        bad = sorted((d, n) for d, n in flips.items() if n >= FLIP_MIN)
        write(lake / root / "roll_flips.csv",
              csv_bytes(["day_utc", "jumps", "threshold_points"], [(d, n, round(threshold, 2)) for d, n in bad], gz=False),
              manifest, len(bad), "roll_flips", root)
        print(f"{root}: {len(rows)} bars in {len(by_month)} months, {len(rolls)} roll records, "
              f"{len(bad)} interleaved days")

    with (lake / "_manifest.jsonl").open("a") as f:
        for m in manifest:
            f.write(json.dumps(m, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:3]))
