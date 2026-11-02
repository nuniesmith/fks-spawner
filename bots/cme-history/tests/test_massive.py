from datetime import date

import pytest
from conftest import day_ms

from cme_history.massive import Massive, MassiveError


class Resp:
    def __init__(self, body, code=200):
        self._body, self.status_code = body, code

    def json(self):
        return self._body


class Session:
    def __init__(self, replies):
        self.replies, self.calls = list(replies), []

    def get(self, url, params=None, timeout=None, headers=None):
        self.calls.append((url, params, headers))
        return self.replies.pop(0)


NS = 1_000_000


def client(replies):
    return Massive("key", per_minute=0, session=Session(replies), sleep=lambda s: None)


def test_an_error_inside_a_200_is_an_error():
    c = client([Resp({"status": "ERROR", "error": "bad ticker"})])
    with pytest.raises(MassiveError, match="bad ticker"):
        c.bars("ESZ6", date(2026, 10, 1))


def test_a_rate_limit_is_waited_out_then_the_request_retried():
    day = date(2026, 10, 1)
    t = day_ms(day)
    ok = {"status": "OK", "results": [{"window_start": t * NS, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 7}]}
    c = client([Resp({"status": "ERROR", "error": "You've exceeded the maximum requests per minute"}), Resp(ok)])
    rows = c.bars("ESZ6", day)
    assert rows == [(t, 1.0, 2.0, 0.5, 1.5, 7.0)] and c.requests_made == 2


def test_bars_follow_the_cursor_and_keep_only_the_day():
    t = day_ms(date(2026, 10, 1))
    page1 = {"status": "OK", "next_url": "https://example.test/next",
             "results": [{"window_start": (t - 60_000) * NS, "open": 9, "high": 9, "low": 9, "close": 9, "volume": 1},
                         {"window_start": t * NS, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]}
    page2 = {"status": "OK", "results": [{"window_start": (t + 86_400_000) * NS, "open": 8, "high": 8, "low": 8, "close": 8, "volume": 1},
                                         {"window_start": (t + 60_000) * NS, "open": 2, "high": 2, "low": 2, "close": 2, "volume": 1}]}
    s = Session([Resp(page1), Resp(page2)])
    c = Massive("key", per_minute=0, session=s, sleep=lambda x: None)
    rows = c.bars("ESZ6", date(2026, 10, 1))
    assert [r[0] for r in rows] == [t, t + 60_000]
    assert s.calls[1][0] == "https://example.test/next" and s.calls[1][1] is None
    assert s.calls[0][2]["Authorization"] == "Bearer key"


def test_contracts_are_one_row_per_ticker():
    rows = [{"ticker": "ESZ6", "last_trade_date": "2026-12-18"}] * 3 + [{"ticker": "ESH7", "last_trade_date": "2027-03-19"}]
    c = client([Resp({"status": "OK", "results": rows})])
    assert [r["ticker"] for r in c.contracts("ES", date(2026, 10, 1))] == ["ESZ6", "ESH7"]


def test_the_limiter_paces_before_sending():
    waits = []
    now = [0.0]
    c = Massive("key", per_minute=5, session=Session([Resp({"status": "OK", "results": []})] * 2),
                sleep=lambda s: (waits.append(s), now.__setitem__(0, now[0] + s)), clock=lambda: now[0])
    c.contracts("ES", date(2026, 10, 1))
    c.contracts("ES", date(2026, 10, 1))
    assert waits == [12.0]
