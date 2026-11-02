"""What the futures importers share: contracts, the roll, and the lake's files.

One rule, the CME recorder's (bots/cme-history, cme_history/stitch.py and
core.py), so a bought decade and the recorder that continues it roll alike:

  * a UTC day follows the contract with the most volume on the previous
    WEEKDAY (a Sunday evening's hour of trading is not Monday's evidence),
    among the contracts trading that day;
  * never back to an earlier contract month; when the followed contract has
    no bars any more (it expired), the most-traded later one is taken;
  * with no volume to go by, stay on the current contract, or take the
    nearest month;
  * a day whose contract differs from the previous day's is a roll day, and
    rolls.csv lists it (the search doesn't trade it).

The lake's layout per root (what dataset.load_cme_minutes reads):

    <ROOT>/1m/<ROOT>-1m-<YYYY-MM-DD>.csv.gz   open_time_ms,open,high,low,close,volume
    <ROOT>/rolls.csv                          day, from_contract, to_contract, gap_points, detected_by
    <ROOT>/excluded_days.csv                  day_utc, reason (optional: days not to trade)
"""

from __future__ import annotations

import csv
import gzip
import io
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

MONTHS = "FGHJKMNQUVXZ"
HEADER = ["open_time_ms", "open", "high", "low", "close", "volume"]
ROLL_FIELDS = ["day", "from_contract", "to_contract", "gap_points", "detected_by"]

Row = tuple[int, float, float, float, float, float]


class ImportError_(ValueError):
    """The input can't be imported as it is; the message says where."""


@dataclass(frozen=True)
class Contract:
    root: str
    month: int  # 1..12
    year: int

    @property
    def code(self) -> str:
        return f"{self.root}{MONTHS[self.month - 1]}{self.year % 100:02d}"

    @property
    def order(self) -> tuple[int, int]:
        return (self.year, self.month)


def utc_day(t: int) -> date:
    return datetime.fromtimestamp(t / 1000, UTC).date()


def choose_days(volumes: dict[Contract, dict[date, float]]) -> tuple[dict[date, Contract], list[tuple[date, Contract, Contract]]]:
    """The contract each UTC day follows (module docs), and the rolls as
    (day, from, to). `volumes[contract][day]` is that day's volume; a day
    present at all means the contract had bars."""
    order = sorted(volumes, key=lambda k: k.order)
    all_days = sorted({d for per in volumes.values() for d in per})
    chosen: dict[date, Contract] = {}
    rolls: list[tuple[date, Contract, Contract]] = []
    current: Contract | None = None
    evidence: dict[Contract, float] = {}  # the last weekday's volumes
    for d in all_days:
        pool = order[order.index(current):] if current is not None else order
        trading = [k for k in pool if d in volumes[k]]
        if trading:
            best = max(trading, key=lambda k: (evidence.get(k, 0.0), -trading.index(k)))
            if evidence.get(best, 0.0) <= 0:
                best = current if current in trading else trading[0]
            if current is not None and best != current:
                rolls.append((d, current, best))
            current = best
            chosen[d] = best
        if d.weekday() < 5:
            today = {k: per[d] for k, per in volumes.items() if d in per}
            if any(v > 0 for v in today.values()):
                evidence = today
    return chosen, rolls


def gap(old: list[Row], new: list[Row]) -> float | str:
    """The new contract's close minus the old one's at the first minute both
    traded on the roll day (positive in contango); "" if they never overlap."""
    closes = {r[0]: r[4] for r in old}
    for r in new:
        if r[0] in closes:
            return round(r[4] - closes[r[0]], 6)
    return ""


def csv_gz(rows: list[Row]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(HEADER)
    for r in rows:
        w.writerow([r[0], *(repr(float(x)) for x in r[1:])])
    return gzip.compress(buf.getvalue().encode(), mtime=0)


def new_root_folder(out: Path, root: str) -> Path:
    folder = out / root
    if folder.exists():
        raise ImportError_(f"{folder} exists: the lake is append-only, import into a new folder")
    (folder / "1m").mkdir(parents=True)
    return folder


def write_rolls(folder: Path, rolls: list[dict]) -> None:
    with (folder / "rolls.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ROLL_FIELDS, lineterminator="\n")
        w.writeheader()
        w.writerows(rolls)


def write_excluded(folder: Path, days: dict[date, str]) -> None:
    with (folder / "excluded_days.csv").open("w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["day_utc", "reason"])
        for d in sorted(days):
            w.writerow([d.isoformat(), days[d]])
