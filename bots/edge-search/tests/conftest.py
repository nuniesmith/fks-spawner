import sys
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from edge_search.dataset import Bars  # noqa: E402
from edge_search.search import Hypothesis  # noqa: E402

DAY = 86_400_000
T0 = int(datetime(2018, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def bars(symbol: str, closes, start_ms: int = T0) -> Bars:
    closes = np.asarray(closes, dtype=float)
    times = start_ms + DAY * np.arange(len(closes), dtype=np.int64)
    return Bars(symbol=symbol, open_ms=times, open=closes.copy(), close=closes)


def random_walk(n: int, seed: int, drift: float = 0.0, vol: float = 0.03) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return 100 * np.exp(np.cumsum(rng.normal(drift, vol, n)))


def regimes(n: int, seed: int, period: int = 240) -> np.ndarray:
    """Long bull and bear stretches: a trend band's home ground."""
    rng = np.random.default_rng(seed)
    drift = np.where((np.arange(n) // period) % 2 == 0, 0.004, -0.004)
    return 100 * np.exp(np.cumsum(drift + rng.normal(0, 0.01, n)))


@pytest.fixture
def hypothesis():
    def make(**over):
        base = dict(
            id=7,
            edge_id="trend-band",
            idea="Long stretches trend; a band around the SMA keeps the trend and skips the chop.",
            symbols=["AAAUSDT", "BBBUSDT", "CCCUSDT"],
            dataset="raw/binance/spot/spot-history 1d 2018-01-01..2026-10-01",
            holdout_share=0.25,
            params=[
                {"name": "lookback", "min": 50, "max": 250, "step": 50},
                {"name": "band", "min": 0.02, "max": 0.08, "step": 0.02},
            ],
            baseline="buy-and-hold",
            metric="net return after fees",
            costs={"maker_bps": 40, "taker_bps": 80, "slippage_bps": 5},
            registered_at=datetime(2026, 10, 27, 8, 0, tzinfo=timezone.utc),
        )
        base.update(over)
        return Hypothesis(**base)

    return make


def write_klines_zip(folder: Path, symbol: str, interval: str, stamp: str, rows: list[list], header: bool):
    folder.mkdir(parents=True, exist_ok=True)
    name = f"{symbol}-{interval}-{stamp}"
    lines = (["open_time,open,high,low,close,volume,close_time"] if header else []) + [",".join(map(str, r)) for r in rows]
    with zipfile.ZipFile(folder / f"{name}.zip", "w") as z:
        z.writestr(f"{name}.csv", "\n".join(lines) + "\n")


def day_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def days(start: date, n: int):
    return [start + timedelta(days=i) for i in range(n)]
