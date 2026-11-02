"""The dataset a hypothesis names, read from the lake.

A hypothesis's `dataset` is text until dataset manifests exist (EDGE_FACTORY
plan 4.1/4.3). This job reads one form of it:

    <lake folder> <interval> <from>..<to>
    raw/binance/spot/spot-history 1d 2017-08-17..2026-10-26

The folder is a history recorder's (fks-spawner bots/history), whose files
are the archive's own zips: <SYMBOL>/klines/<interval>/<SYMBOL>-<interval>-
<YYYY-MM | YYYY-MM-DD>.zip. Monthly and daily files overlap near the end of
a month; rows are merged by open time.

CME futures folders (raw/cme/..., read by the `orb` family) hold one
stitched 1-minute series per root instead: <ROOT>/1m/<ROOT>-1m-<period>.csv.gz
(open_time_ms,open,high,low,close,volume), with <ROOT>/rolls.csv and
<ROOT>/roll_flips.csv naming the days a series changes contract. Several
folders can be joined with '+' (the imported cache, then the recorder that
keeps it current): for a UTC day in more than one, the FIRST folder wins the
whole day. Per minute, a partial day in one folder could be completed from
another one on a different contract, and pass as one clean session.

Two traps from the archive, handled here (bots/history README):
  - spot klines are in MICROSECONDS from 2025 on, milliseconds before, and
    futures in milliseconds: times are normalized to milliseconds;
  - spot klines have no header row, futures klines do.
"""

from __future__ import annotations

import csv
import gzip
import io
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
}

SPEC_RE = re.compile(
    r"^\s*(?P<path>raw/[a-z0-9._/-]+?(?:\+raw/[a-z0-9._/-]+?)*)/?\s+(?P<interval>\d+[mhd])\s+"
    r"(?P<start>\d{4}-\d{2}-\d{2})\s*\.\.\s*(?P<end>\d{4}-\d{2}-\d{2})\s*$"
)


class DatasetError(ValueError):
    """The dataset can't be read as named; the message says why."""


@dataclass(frozen=True)
class DatasetSpec:
    path: str  # under the lake, without the leading raw/ ('+' joins folders)
    interval: str
    start: date
    end: date  # inclusive

    @property
    def paths(self) -> list[str]:
        """The folders, in the order named: the first one wins a minute."""
        return self.path.split("+")

    @property
    def bar_ms(self) -> int:
        return INTERVAL_MS[self.interval]

    @property
    def bar_minutes(self) -> int:
        return self.bar_ms // 60_000

    @property
    def start_ms(self) -> int:
        return int(datetime(self.start.year, self.start.month, self.start.day, tzinfo=timezone.utc).timestamp() * 1000)

    @property
    def end_ms(self) -> int:
        """Exclusive: the first millisecond after `end`."""
        return int(datetime(self.end.year, self.end.month, self.end.day, tzinfo=timezone.utc).timestamp() * 1000) + 86_400_000


def parse_spec(text: str) -> DatasetSpec:
    m = SPEC_RE.match(text or "")
    if not m:
        raise DatasetError(
            f"dataset {text!r} isn't '<lake folder> <interval> <from>..<to>', "
            "e.g. 'raw/binance/spot/spot-history 1d 2017-08-17..2026-10-26'"
        )
    interval = m.group("interval")
    if interval not in INTERVAL_MS:
        raise DatasetError(f"interval {interval!r} isn't one of {list(INTERVAL_MS)}")
    try:
        start = date.fromisoformat(m.group("start"))
        end = date.fromisoformat(m.group("end"))
    except ValueError as e:
        raise DatasetError(f"dataset dates: {e}") from None
    if end <= start:
        raise DatasetError("the dataset's end must come after its start")
    parts = [p.removeprefix("raw/").strip("/") for p in m.group("path").split("+")]
    if any(".." in p.split("/") for p in parts):
        raise DatasetError("the dataset folder can't climb out of the lake")
    return DatasetSpec(path="+".join(parts), interval=interval, start=start, end=end)


@dataclass
class Bars:
    """One symbol's bars on its own clock: open time (ms), open, close."""

    symbol: str
    open_ms: np.ndarray
    open: np.ndarray
    close: np.ndarray


def _rows(raw: bytes):
    text = raw.decode("utf-8", "replace")
    for row in csv.reader(io.StringIO(text)):
        if not row or not row[0].strip().lstrip("-").isdigit():
            continue  # the futures header row, or a blank line
        yield row


def _to_ms(t: int) -> int:
    # 16 digits = microseconds (spot from 2025), 13 = milliseconds.
    return t // 1000 if t >= 100_000_000_000_000 else t


def load_bars(lake: Path, spec: DatasetSpec, symbol: str) -> Bars:
    if len(spec.paths) > 1:
        raise DatasetError("a history recorder's dataset names one folder; '+' joins CME folders only")
    folder = lake / spec.path / symbol / "klines" / spec.interval
    if not folder.is_dir():
        raise DatasetError(f"no {spec.interval} klines for {symbol} at {folder}")
    by_time: dict[int, tuple[float, float]] = {}
    for f in sorted(folder.glob(f"{symbol}-{spec.interval}-*.zip")):
        try:
            with zipfile.ZipFile(f) as z:
                for name in z.namelist():
                    for row in _rows(z.read(name)):
                        t = _to_ms(int(row[0]))
                        if spec.start_ms <= t < spec.end_ms:
                            by_time[t] = (float(row[1]), float(row[4]))
        except (zipfile.BadZipFile, ValueError, IndexError) as e:
            raise DatasetError(f"{f.name}: {e}") from None
    if not by_time:
        raise DatasetError(f"{symbol}: no bars between {spec.start} and {spec.end}")
    times = np.array(sorted(by_time), dtype=np.int64)
    values = np.array([by_time[t] for t in times], dtype=float)
    return Bars(symbol=symbol, open_ms=times, open=values[:, 0], close=values[:, 1])


def coverage(bars: Bars, spec: DatasetSpec) -> float:
    """Share of the bars expected from the symbol's first bar (its listing,
    or the window's start) to the window's end that are present. A symbol
    that lists late isn't a hole; a missing bar after it is."""
    first = int(bars.open_ms[0])
    expected = (spec.end_ms - first) // spec.bar_ms
    return min(1.0, len(bars.open_ms) / expected) if expected > 0 else 0.0


def align(series: list[Bars], bar_ms: int) -> tuple[np.ndarray, np.ndarray]:
    """A common clock for all symbols: (times, closes[symbol, bar]), NaN
    where a symbol has no bar (before it listed, or a hole)."""
    first = min(int(b.open_ms[0]) for b in series)
    last = max(int(b.open_ms[-1]) for b in series)
    times = np.arange(first, last + bar_ms, bar_ms, dtype=np.int64)
    closes = np.full((len(series), len(times)), np.nan)
    for i, b in enumerate(series):
        idx = (b.open_ms - first) // bar_ms
        closes[i, idx] = b.close
    return times, closes


# ── CME futures: one stitched 1-minute series per root ─────────────────────────


@dataclass
class MinuteBars:
    """One root's 1-minute OHLCV on its own clock, and the days it must not
    be traded: the recorded roll days (any detection) and the days the
    stitched series interleaves two contracts (roll_flips.csv)."""

    root: str
    open_ms: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    roll_days: set[date]


def _day_column(path: Path, column: str) -> set[date]:
    if not path.is_file():
        return set()
    with path.open(newline="") as f:
        return {date.fromisoformat(r[column]) for r in csv.DictReader(f) if r.get(column)}


def _read_minutes(path: Path) -> np.ndarray:
    """One file's rows as [n, 6] (open_time_ms, open, high, low, close, volume)."""
    with gzip.open(path, "rt") as fh:
        a = np.loadtxt(fh, delimiter=",", skiprows=1, ndmin=2, dtype=float)
    if a.size and a.shape[1] < 6:
        raise ValueError(f"{a.shape[1]} columns, expected 6")
    return a.reshape(-1, 6) if a.size else np.zeros((0, 6))


def load_cme_minutes(lake: Path, spec: DatasetSpec, root: str) -> MinuteBars:
    if spec.interval != "1m":
        raise DatasetError(f"CME folders hold 1-minute bars; the dataset asks for {spec.interval}")
    parts: list[np.ndarray] = []
    roll_days: set[date] = set()
    found = False
    for rank, part in enumerate(spec.paths):
        folder = lake / part / root
        files = sorted((folder / "1m").glob(f"{root}-1m-*.csv.gz")) if folder.is_dir() else []
        found = found or bool(files)
        roll_days |= _day_column(folder / "rolls.csv", "day")
        roll_days |= _day_column(folder / "roll_flips.csv", "day_utc")
        for f in files:
            try:
                a = _read_minutes(f)
            except (OSError, ValueError) as e:
                raise DatasetError(f"{f.name}: {e}") from None
            keep = (a[:, 0] >= spec.start_ms) & (a[:, 0] < spec.end_ms)
            parts.append(np.column_stack([a[keep], np.full(int(keep.sum()), rank)]))
    if not found:
        raise DatasetError(f"no 1m bars for {root} in {' + '.join(spec.paths)}")
    rows = np.vstack(parts) if parts else np.zeros((0, 7))
    if not len(rows):
        raise DatasetError(f"{root}: no bars between {spec.start} and {spec.end}")
    # A UTC day belongs to the earliest-named folder that has any bar in it:
    # never a day stitched from two folders (two contracts, possibly).
    day = (rows[:, 0] // 86_400_000).astype(np.int64)
    owner: dict[int, int] = {}
    for d, r in zip(day.tolist(), rows[:, 6].astype(int).tolist()):
        if d not in owner or r < owner[d]:
            owner[d] = r
    rows = rows[np.array([owner[d] == r for d, r in zip(day.tolist(), rows[:, 6].astype(int).tolist())])]
    rows = rows[np.argsort(rows[:, 0], kind="stable")]
    rows = rows[np.concatenate([[True], rows[1:, 0] != rows[:-1, 0]])]
    return MinuteBars(root=root, open_ms=rows[:, 0].astype(np.int64), open=rows[:, 1], high=rows[:, 2],
                      low=rows[:, 3], close=rows[:, 4], volume=rows[:, 5], roll_days=roll_days)
