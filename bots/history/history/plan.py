"""Which archive files the lake should hold, as of a given UTC day.

Pure, so the rules are tested without a network:

  - every complete month since HISTORY_START, as its monthly file, for the
    datasets the archive publishes monthly (klines, fundingRate);
  - the last DAILY_WINDOW_DAYS days, as daily files, for the datasets the
    archive publishes daily (klines, metrics). A month's monthly file appears
    a few days after the month ends; the daily files cover that stretch, and
    readers prefer a monthly file wherever both exist;
  - metrics are published daily only, so every day since HISTORY_START.

The archive publishes a day about a day after it closes, so "yesterday" is
the newest day asked for, and a 404 for a recent file means "not yet"
(core.RECENT_DAYS), not "never".
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from .config import DATASETS, Config

DAILY_WINDOW_DAYS = 35


@dataclass(frozen=True, order=True)
class Target:
    dataset: str
    symbol: str
    interval: str | None
    period: str  # "monthly" | "daily"
    day: date  # the first of the month for a monthly file

    @property
    def last_day(self) -> date:
        """The period's last day: when the archive can start to have it."""
        if self.period == "daily":
            return self.day
        nxt = date(self.day.year + 1, 1, 1) if self.day.month == 12 else date(self.day.year, self.day.month + 1, 1)
        return nxt - timedelta(days=1)

    @property
    def stamp(self) -> str:
        return self.day.strftime("%Y-%m") if self.period == "monthly" else self.day.isoformat()

    @property
    def filename(self) -> str:
        middle = self.interval if self.interval else self.dataset
        return f"{self.symbol}-{middle}-{self.stamp}.zip"

    def url(self, cfg: Config) -> str:
        parts = [cfg.base_url, "data", cfg.market_path, self.period, self.dataset, self.symbol]
        if self.interval:
            parts.append(self.interval)
        return "/".join(parts + [self.filename])

    def path(self, cfg: Config) -> Path:
        """`<SYMBOL>/<dataset>/[<interval>/]<file>` under DATA_DIR, symbol
        first like the recorder's folders. The market is the lake folder's
        (raw/binance/<market>/<instance>), so it isn't repeated here."""
        p = cfg.data_dir / self.symbol / self.dataset
        if self.interval:
            p = p / self.interval
        return p / self.filename

    @property
    def series(self) -> tuple[str, str, str]:
        """(dataset, symbol, interval or '-'): the unit status is kept in."""
        return self.dataset, self.symbol, self.interval or "-"


def months(start: date, end_exclusive: date) -> list[date]:
    """First days of every month from `start`'s month up to, not including,
    `end_exclusive`'s month."""
    out, y, m = [], start.year, start.month
    while (y, m) < (end_exclusive.year, end_exclusive.month):
        out.append(date(y, m, 1))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def days(first: date, last: date) -> list[date]:
    return [first + timedelta(days=i) for i in range((last - first).days + 1)] if last >= first else []


def targets(cfg: Config, today: date) -> list[Target]:
    yesterday = today - timedelta(days=1)
    window_start = max(cfg.start, today - timedelta(days=DAILY_WINDOW_DAYS))
    out: list[Target] = []
    for name in cfg.datasets:
        ds = DATASETS[name]
        for symbol in cfg.symbols:
            for interval in (cfg.intervals if ds.per_interval else (None,)):
                if "monthly" in ds.periods:
                    out += [Target(name, symbol, interval, "monthly", m) for m in months(cfg.start, today)]
                    if "daily" in ds.periods:
                        out += [Target(name, symbol, interval, "daily", d) for d in days(window_start, yesterday)]
                else:  # daily only
                    out += [Target(name, symbol, interval, "daily", d) for d in days(cfg.start, yesterday)]
    return sorted(out)
