"""The recorder's lake folder (DATA_DIR = raw/cme/massive/<instance>).

  <ROOT>/1m/<ROOT>-1m-<YYYY-MM-DD>.csv.gz            the stitched series, one UTC day
  <ROOT>/contracts/<TICKER>/<TICKER>-1m-<day>.csv.gz  every candidate's own bars (audit)
  <ROOT>/rolls.csv                                    day, from, to, gap_points, detected_by
  _manifest.jsonl                                     one line per file: sha256, bytes, rows
  _state/<ROOT>.json                                  where the root is: day, contract, volumes

Append-only like the rest of the lake: a file that exists is never
rewritten. Files are written to <name>.part and renamed, so a file present
is always complete. The columns are the ones the search job reads
(edge_search/dataset.py load_cme_minutes).
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

from .massive import Row

HEADER = ["open_time_ms", "open", "high", "low", "close", "volume"]


def _csv(rows: list[Row]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(HEADER)
    for r in rows:
        w.writerow([r[0], *(repr(float(x)) for x in r[1:])])
    return gzip.compress(buf.getvalue().encode(), mtime=0)


class Lake:
    def __init__(self, root: Path):
        self.root = root

    def _atomic(self, path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        part = path.with_name(path.name + ".part")
        part.write_bytes(data)
        os.replace(part, path)

    def _manifest(self, path: Path, data: bytes, rows: int, **meta: str) -> None:
        line = {
            "path": str(path.relative_to(self.root)),
            "sha256": hashlib.sha256(data).hexdigest(),
            "bytes": len(data),
            "rows": rows,
            "source": "massive",
            "written_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            **meta,
        }
        with (self.root / "_manifest.jsonl").open("a") as f:
            f.write(json.dumps(line, sort_keys=True) + "\n")

    def write_bars(self, path: Path, rows: list[Row], **meta: str) -> bool:
        """False when the file is already there (never rewritten)."""
        if path.exists():
            return False
        data = _csv(rows)
        self._atomic(path, data)
        self._manifest(path, data, len(rows), **meta)
        return True

    def day_path(self, root: str, day: date) -> Path:
        return self.root / root / "1m" / f"{root}-1m-{day.isoformat()}.csv.gz"

    def contract_path(self, root: str, ticker: str, day: date) -> Path:
        return self.root / root / "contracts" / ticker / f"{ticker}-1m-{day.isoformat()}.csv.gz"

    def append_roll(self, root: str, day: date, frm: str, to: str, gap: float | None) -> None:
        path = self.root / root / "rolls.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        new = not path.exists()
        with path.open("a", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            if new:
                w.writerow(["day", "from_contract", "to_contract", "gap_points", "detected_by"])
            w.writerow([day.isoformat(), frm, to, "" if gap is None else gap, "volume"])

    def state(self, root: str) -> dict:
        path = self.root / "_state" / f"{root}.json"
        if not path.exists():
            return {"last_day": None, "contract": None, "prev_volume": {}, "candidates": None}
        return json.loads(path.read_text())

    def save_state(self, root: str, state: dict) -> None:
        self._atomic(self.root / "_state" / f"{root}.json", json.dumps(state, sort_keys=True, indent=1).encode())
