"""History-recorder configuration, read from the environment.

The spawner sets these from a `history.binance-*` template's form (one
template per market, since the market names the lake folder); the same names
work by hand with `docker run -e ...`.

  HISTORY_MARKET      spot | usdm (USDⓈ-M futures); required
  HISTORY_SYMBOLS     comma-separated, e.g. BTCUSDT,ETHUSDT; required
  HISTORY_DATASETS    klines (default); usdm also offers fundingRate, metrics
  HISTORY_INTERVALS   kline intervals (default 1h,1d)
  HISTORY_START       first month, YYYY-MM (default 2020-01)
  HISTORY_POLL_HOURS  hours between checks for new files (default 6)
  HISTORY_CONCURRENCY parallel downloads (default 4, at most 8)
  HISTORY_BASE_URL    the archive (default https://data.binance.vision)
  DATA_DIR            where files go (default /data)
  RECORDER_PORT       the /health, /metrics, /status port (default 9091)
  FKS_BOT_ID          set by the spawner; names this instance in logs
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Mapping


class ConfigError(ValueError):
    """A setting the recorder can't run with; the message says which."""


@dataclass(frozen=True)
class Dataset:
    name: str
    # Which archive periods exist for it: "monthly" files for complete months,
    # "daily" files a day or so after each day closes.
    periods: tuple[str, ...]
    # klines have an interval folder and an interval in the file name.
    per_interval: bool


DATASETS = {
    "klines": Dataset("klines", ("monthly", "daily"), True),
    "fundingRate": Dataset("fundingRate", ("monthly",), False),
    "metrics": Dataset("metrics", ("daily",), False),
}

# Archive path per market, and the datasets it publishes. The names match
# the lake's (raw/binance/<market>/...).
MARKETS = {
    "spot": ("spot", ("klines",)),
    "usdm": ("futures/um", ("klines", "fundingRate", "metrics")),
}

INTERVALS = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d")
SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,20}$")
MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")


@dataclass(frozen=True)
class Config:
    market: str
    symbols: tuple[str, ...]
    datasets: tuple[str, ...]
    intervals: tuple[str, ...]
    start: date
    poll_hours: float
    concurrency: int
    base_url: str
    data_dir: Path
    port: int
    bot_id: str

    @property
    def market_path(self) -> str:
        return MARKETS[self.market][0]


def _list(raw: str | None) -> list[str]:
    out: list[str] = []
    for part in (raw or "").split(","):
        part = part.strip()
        if part and part not in out:
            out.append(part)
    return out


def load(env: Mapping[str, str] | None = None) -> Config:
    env = os.environ if env is None else env

    market = env.get("HISTORY_MARKET", "").strip()
    if market not in MARKETS:
        raise ConfigError(f"HISTORY_MARKET must be one of {sorted(MARKETS)}, not {market!r}")
    offered = MARKETS[market][1]

    symbols = [s.upper() for s in _list(env.get("HISTORY_SYMBOLS"))]
    if not symbols:
        raise ConfigError("HISTORY_SYMBOLS is required, e.g. BTCUSDT")
    bad = [s for s in symbols if not SYMBOL_RE.match(s)]
    if bad:
        raise ConfigError(f"HISTORY_SYMBOLS has symbols that aren't a venue symbol: {bad}")

    datasets = _list(env.get("HISTORY_DATASETS")) or ["klines"]
    unknown = [d for d in datasets if d not in offered]
    if unknown:
        raise ConfigError(f"HISTORY_DATASETS has {unknown}, which {market} doesn't publish (offered: {list(offered)})")

    intervals = _list(env.get("HISTORY_INTERVALS")) or ["1h", "1d"]
    unknown = [i for i in intervals if i not in INTERVALS]
    if unknown:
        raise ConfigError(f"HISTORY_INTERVALS has {unknown} (offered: {list(INTERVALS)})")
    if "klines" not in datasets:
        intervals = []

    m = MONTH_RE.match(env.get("HISTORY_START", "2020-01").strip())
    if not m or not 1 <= int(m.group(2)) <= 12 or int(m.group(1)) < 2017:
        raise ConfigError("HISTORY_START must be a month YYYY-MM, 2017-01 or later")
    start = date(int(m.group(1)), int(m.group(2)), 1)

    try:
        poll_hours = float(env.get("HISTORY_POLL_HOURS", "6"))
        concurrency = int(env.get("HISTORY_CONCURRENCY", "4"))
        port = int(env.get("RECORDER_PORT", "9091"))
    except ValueError:
        raise ConfigError("HISTORY_POLL_HOURS, HISTORY_CONCURRENCY and RECORDER_PORT must be numbers") from None
    if not 0.25 <= poll_hours <= 48:
        raise ConfigError("HISTORY_POLL_HOURS must be between 0.25 and 48")
    if not 1 <= concurrency <= 8:
        raise ConfigError("HISTORY_CONCURRENCY must be between 1 and 8 (be polite to the archive)")

    return Config(
        market=market,
        symbols=tuple(dict.fromkeys(symbols)),
        datasets=tuple(datasets),
        intervals=tuple(intervals),
        start=start,
        poll_hours=poll_hours,
        concurrency=concurrency,
        base_url=env.get("HISTORY_BASE_URL", "https://data.binance.vision").rstrip("/"),
        data_dir=Path(env.get("DATA_DIR", "/data")),
        port=port,
        bot_id=env.get("FKS_BOT_ID", "local"),
    )
