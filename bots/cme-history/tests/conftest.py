import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cme_history.config import Config  # noqa: E402


def day_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


class FakeMassive:
    """Scripted contracts and bars; counts requests like the real client."""

    def __init__(self, contracts: dict[str, list[dict]], volume: dict[tuple[str, date], float],
                 price: dict[str, float] | None = None, fail_on: set | None = None):
        self._contracts = contracts
        self._volume = volume
        self._price = price or {}
        self._fail_on = fail_on or set()
        self.requests_made = 0
        self.bar_calls: list[tuple[str, date]] = []

    def contracts(self, product: str, day: date) -> list[dict]:
        self.requests_made += 1
        return self._contracts[product]

    def bars(self, ticker: str, day: date):
        from cme_history.massive import MassiveError

        self.requests_made += 1
        self.bar_calls.append((ticker, day))
        if (ticker, day) in self._fail_on:
            raise MassiveError("scripted failure")
        v = self._volume.get((ticker, day), 0.0)
        if v <= 0:
            return []
        p = self._price.get(ticker, 5000.0)
        t0 = day_ms(day) + 13 * 3_600_000 + 30 * 60_000
        return [(t0 + i * 60_000, p, p + 1, p - 1, p, v / 10) for i in range(10)]


@pytest.fixture
def cfg(tmp_path):
    return Config(roots=("ES",), start=date(2026, 9, 14), poll_hours=6, api_key="k", rpm=0,
                  base_url="https://example.test", data_dir=tmp_path, port=0, bot_id="cme")
