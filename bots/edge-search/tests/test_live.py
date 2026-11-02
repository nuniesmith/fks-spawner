"""The signal runner (edge_search/live.py): the searched code, one bar at a time."""

from datetime import date, datetime, timedelta, timezone

import numpy as np
import pytest
import test_orb as T

from edge_search import live, orb
from edge_search.dataset import DatasetSpec

DAYS = T.weekdays(date(2026, 11, 2), 40)


def rows_of(bars, day):
    s, e = live.session_bounds("ES", day)
    keep = (bars.open_ms >= s) & (bars.open_ms < e)
    return [(int(t), float(o), float(h), float(lo), float(c), float(v)) for t, o, h, lo, c, v in zip(
        bars.open_ms[keep], bars.open[keep], bars.high[keep], bars.low[keep], bars.close[keep],
        bars.volume[keep])]


def searched(symbol, bars, day, params, costs):
    """The search's own verdict on one whole session."""
    spec = DatasetSpec(path="x", interval="1m", start=day, end=day)
    data = orb.prepare([symbol], [bars], spec, costs)
    data.sessions[0].complete[:] = True
    trades = orb.trades_for(data, params)
    if not trades:
        return None
    t = trades[0]
    s = data.sessions[0]
    k = s.k
    slip = data.slip_ticks * k.tick
    entry = float(s.c[0, t.entry_min]) + t.side * slip
    m = int(params["orb_minutes"])
    hi, lo = float(np.nanmax(s.h[0, :m])), float(np.nanmin(s.l[0, :m]))
    stop = lo if t.side > 0 else hi
    _, px = orb.exit_trade(s, 0, t.entry_min, t.side, entry, stop, abs(entry - stop),
                           float(params["target_r"]), params["exit"], slip)
    return t.entry_min, t.side, entry, t.exit_min, float(px)


def replay(symbol, rows, day, params, costs):
    """Feed the bars one at a time, as the socket would; collect every event
    the runner would say, with the minute it was first said."""
    start, end = live.session_bounds(symbol, day)
    said = {}
    for i in range(1, len(rows) + 1):
        now = rows[i - 1][0] + 60_000  # the bar just closed
        for ev in live.evaluate(symbol, day, rows[:i], params, costs, now).events:
            said.setdefault(ev.kind, (ev, i - 1))
    for ev in live.evaluate(symbol, day, rows, params, costs, end + 60_000).events:
        said.setdefault(ev.kind, (ev, len(rows)))
    return said


@pytest.mark.parametrize("params", [
    {"orb_minutes": 15, "target_r": 2.0, "exit": "trail"},
    {"orb_minutes": 5, "target_r": 1.5, "exit": "target"},
    {"orb_minutes": 30, "target_r": 3.0, "exit": "target"},
])
def test_signals_bar_by_bar_are_the_searched_trade(params):
    """Parity: on every session, the runner's entry and exit (fed one bar at
    a time) are the search's trade on the whole session, and each is said on
    the bar it happens, never earlier."""
    checked = 0
    for b in T.series_for(DAYS, 0.0002)[:1]:  # ES
        for day in DAYS:
            rows = rows_of(b, day)
            if len(rows) < 300:
                continue
            want = searched("ES", b, day, params, T.COSTS)
            said = replay("ES", rows, day, params, T.COSTS)
            m = params["orb_minutes"]
            assert said["range"][1] == m - 1, day  # said when the range's last bar closes
            if want is None:
                assert "entry" not in said and "exit" not in said, day
                continue
            e_min, side, entry, x_min, px = want
            ev, at = said["entry"]
            assert (ev.side, at) == (side, e_min) and ev.price == pytest.approx(entry, abs=1e-9), day
            ev, at = said["exit"]
            assert ev.price == pytest.approx(px, abs=1e-9), (day, ev.reason)
            # Said when its bar closed; a close-out at the session's end is
            # said once the session is over.
            assert at in (x_min, len(rows)), (day, at, x_min)
            assert at >= x_min
            checked += 1
    assert checked >= 10


def test_a_bar_missing_from_the_range_means_no_signal():
    d = date(2026, 11, 4)
    rows = rows_of(T.series_for([d], 0.0002)[0], d)
    del rows[3]  # minute 3 of a 15-minute range
    out = live.evaluate("ES", d, rows, {"orb_minutes": 15, "target_r": 2.0, "exit": "trail"}, T.COSTS,
                        rows[-1][0] + 60_000)
    assert out.events == []


def test_the_range_is_not_said_before_its_last_bar():
    d = date(2026, 11, 4)
    rows = rows_of(T.series_for([d], 0.0002)[0], d)
    p = {"orb_minutes": 15, "target_r": 2.0, "exit": "trail"}
    assert live.evaluate("ES", d, rows[:14], p, T.COSTS, rows[13][0] + 60_000).events == []
    assert [e.kind for e in live.evaluate("ES", d, rows[:15], p, T.COSTS, rows[14][0] + 60_000).events] == ["range"]


def test_the_contract_is_the_previous_weekdays_most_traded():
    assert live.choose(["ESZ6", "ESH7"], {"ESZ6": 900, "ESH7": 100}) == "ESZ6"
    assert live.choose(["ESZ6", "ESH7"], {"ESZ6": 100, "ESH7": 900}) == "ESH7"
    assert live.choose(["ESZ6", "ESH7"], {}) == "ESZ6"
    assert live.previous_weekday(date(2026, 11, 2)) == date(2026, 10, 30)  # Monday -> Friday


class FakeMassive:
    def __init__(self, volumes):
        self.volumes = volumes  # (ticker, day) -> volume

    def nearest(self, product, day, n=3):
        return [f"{product}Z6", f"{product}H7"]

    def minutes(self, ticker, day):
        v = self.volumes.get((ticker, day), 0)
        return [(0, 1, 1, 1, 1, v)] if v else []


def test_a_roll_day_and_a_holiday_are_not_traded():
    mon, fri, thu = date(2026, 11, 2), date(2026, 10, 30), date(2026, 10, 29)
    m = FakeMassive({("ESZ6", thu): 900, ("ESH7", fri): 900, ("ESZ6", fri): 100})
    d = live.plan_day(m, ["MES"], mon)
    assert d.contracts == {"MES": "ESH7"} and "roll day" in d.blocked["MES"]
    m = FakeMassive({("ESZ6", thu): 900, ("ESZ6", fri): 900})
    assert live.plan_day(m, ["MES"], mon).blocked == {}
    assert live.plan_day(m, ["MES"], date(2026, 11, 26)).blocked["MES"] == "a US exchange holiday"


def test_messages_say_paper_or_live_and_what_to_do():
    ev = live.Event("entry", "MES", date(2026, 11, 4), 1793799000000, 1, 6012.25, 5998.0, 6040.75,
                    "close 6012.00 beyond the range; 2R trail from the target")
    text = live.message(ev, "ESZ6", "forward")
    assert text.startswith("[PAPER] MES (ESZ6) ENTER LONG ~6012.25 stop 5998.00 target 6040.75")
    assert live.message(ev, "ESZ6", "live").startswith("[LIVE]")
    gold = live.Event("exit", "MGC", date(2026, 11, 4), 1793799000000, -1, 2650.3, 2660.0, 2630.0, "the stop")
    assert "EXIT SHORT ~2650.3: the stop" in live.message(gold, "MGCZ6", "live")


def test_socket_bars_and_config():
    assert live.socket_bar({"ev": "AM", "sym": "ESZ6", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 7,
                            "s": 1000, "e": 61000}) == ("ESZ6", (1000, 1.0, 2.0, 0.5, 1.5, 7.0))
    assert live.socket_bar({"ev": "status", "status": "connected"}) is None
    base = {"EDGE_DEPLOYMENT_ID": "4", "FKS_DB_URL": "postgres://x", "MASSIVE_API_KEY": "k",
            "FKS_CODE_REVISION": "abc"}
    c = live.load_config(base)
    assert (c.deployment_id, c.webhook, c.ws_url) == (4, None, live.WS_URL)
    with pytest.raises(live.SignalError, match="Settings -> API keys"):
        live.load_config({**base, "MASSIVE_API_KEY": ""})


class Ledger:
    """A stand-in for edge_signals: one row per (day, symbol, kind)."""

    def __init__(self):
        self.rows, self.delivered = {}, set()

    def execute(self, sql, args):
        if sql.startswith("INSERT"):
            key = (args[1], args[2], args[3])
            if key in self.rows:
                return Cursor(None)
            self.rows[key] = len(self.rows) + 1
            return Cursor((self.rows[key], "live"))
        self.delivered.add(args[0])
        return Cursor(None)

    def commit(self):
        pass


class Cursor:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


def test_a_signal_is_recorded_then_sent_once_and_marked_delivered():
    cfg = live.load_config({"EDGE_DEPLOYMENT_ID": "4", "FKS_DB_URL": "x", "MASSIVE_API_KEY": "k",
                            "FKS_CODE_REVISION": "abc"})
    ledger, sent = Ledger(), []
    ev = live.Event("range", "MES", date(2026, 11, 4), 0, reason="15-minute range")
    assert live.say(ledger, cfg, ev, "ESZ6", lambda t: sent.append(t) or True)
    assert not live.say(ledger, cfg, ev, "ESZ6", lambda t: sent.append(t) or True)  # a restart
    assert len(sent) == 1 and ledger.delivered == {1}
    # A send that fails leaves it recorded and undelivered.
    ev2 = live.Event("entry", "MES", date(2026, 11, 4), 0, 1, 1.0, 0.5, 2.0, "x")
    live.say(ledger, cfg, ev2, "ESZ6", lambda t: False)
    assert ledger.delivered == {1}


# ── Safety (2026-11-02 review) ───────────────────────────────────────────────


def test_a_late_signal_says_so_and_not_to_chase_it():
    cfg = live.load_config({"EDGE_DEPLOYMENT_ID": "4", "FKS_DB_URL": "x", "MASSIVE_API_KEY": "k",
                            "FKS_CODE_REVISION": "abc"})
    bar = 1_793_799_000_000
    ev = live.Event("entry", "MES", date(2026, 11, 4), bar, 1, 6012.25, 5998.0, 6040.75, "close beyond")
    ledger, sent = Ledger(), []
    # Said 75 minutes after its bar closed: LATE, with the advice, and noted in the ledger.
    assert live.say(ledger, cfg, ev, "ESZ6", lambda t: sent.append(t) or True, now_ms=bar + 60_000 + 75 * 60_000)
    assert sent[0].startswith("[LIVE] LATE by 75 min: MES (ESZ6) ENTER LONG ~6012.25")
    assert sent[0].endswith("For the record: don't chase it.")
    # On time (the bar just closed): no tag.
    ev2 = live.Event("range", "MES", date(2026, 11, 5), bar, reason="15-minute range")
    live.say(ledger, cfg, ev2, "ESZ6", lambda t: sent.append(t) or True, now_ms=bar + 61_000)
    assert "LATE" not in sent[1]
    exit_ev = live.Event("exit", "MES", date(2026, 11, 6), bar, 1, 6040.75, 5998.0, 6040.75, "the target")
    live.say(ledger, cfg, exit_ev, "ESZ6", lambda t: sent.append(t) or True, now_ms=bar + 20 * 60_000)
    assert sent[2].endswith("If you're still in, the model is out.")


def test_early_closes_block_the_equity_session_not_gold():
    # 2026: Thanksgiving Nov 26 -> Friday Nov 27; Christmas Eve is a Thursday.
    assert date(2026, 11, 27) in live.early_close_days(2026)
    assert date(2026, 12, 24) in live.early_close_days(2026)
    assert date(2026, 7, 3) not in live.early_close_days(2026)  # a Friday: the observed holiday
    assert date(2025, 7, 3) in live.early_close_days(2025)  # a Thursday
    assert date(2022, 12, 24) not in live.early_close_days(2022)  # a Saturday
    m = FakeMassive({("ESZ6", date(2026, 11, 25)): 900, ("ESZ6", date(2026, 11, 24)): 900,
                     ("MGCZ6", date(2026, 11, 25)): 900, ("MGCZ6", date(2026, 11, 24)): 900})
    d = live.plan_day(m, ["MES", "MGC"], date(2026, 11, 27))
    assert "early close (trading stops 13:15 ET" in d.blocked["MES"]
    assert "MGC" not in d.blocked  # metals stop 13:45 ET, after gold's 13:30 session


def test_a_named_channel_without_its_webhook_is_an_error():
    base = {"EDGE_DEPLOYMENT_ID": "4", "FKS_DB_URL": "x", "MASSIVE_API_KEY": "k", "FKS_CODE_REVISION": "abc"}
    assert live.config_problem(live.load_config(base)) is None  # no channel: record-only, by choice
    named = live.load_config({**base, "SIGNAL_CHANNEL": "futures"})
    assert "channel 'futures' was named but its webhook didn't arrive" in live.config_problem(named)
    ok = live.load_config({**base, "SIGNAL_CHANNEL": "futures", "SIGNAL_DISCORD_WEBHOOK_URL": "https://x"})
    assert live.config_problem(ok) is None
    assert "fks_signal_error 1" in live.render_metrics({"config_error": "x"})
    assert "fks_signal_error 0" in live.render_metrics({})


def session_rows(day, upto=None):
    return rows_of(T.series_for([day], 0.0002)[0], day)[:upto]


class Dep:
    def __init__(self, params):
        self.symbols = ["ES"]
        self.params = params
        self.hypothesis = type("H", (), {"costs": T.COSTS})()


def a_traded_day(params):
    for day in DAYS:
        b = T.series_for([day], 0.0002)[0]
        want = searched("ES", b, day, params, T.COSTS)
        if want is not None and want[3] > want[0] + 3:
            return day, want
    raise AssertionError("no session with a trade that lasts")


def test_retired_with_a_trade_open_only_its_exit_is_said():
    p = {"orb_minutes": 15, "target_r": 2.0, "exit": "trail"}
    day, (e_min, side, entry, x_min, px) = a_traded_day(p)
    rows = session_rows(day)
    today = live.Day(day, {"ES": "ESZ6"}, {}, {"ESZ6": {r[0]: r for r in rows}})
    cfg = live.load_config({"EDGE_DEPLOYMENT_ID": "4", "FKS_DB_URL": "x", "MASSIVE_API_KEY": "k",
                            "FKS_CODE_REVISION": "abc"})
    start, _ = live.session_bounds("ES", day)
    ledger, sent = Ledger(), []
    # Before the exit, with the entry said: one trade open.
    mid = live.Day(day, {"ES": "ESZ6"}, {}, {"ESZ6": {r[0]: r for r in rows[:e_min + 2]}})
    out = live.step(ledger, cfg, Dep(p), mid, start + (e_min + 2) * 60_000, lambda t: sent.append(t) or True)
    assert out["open"] == 1 and [k for _, _, k in ledger.rows] == ["range", "entry"]
    # Retired: exits only. After the exit bar, the exit is said and nothing else.
    out = live.step(ledger, cfg, Dep(p), today, start + (x_min + 1) * 60_000 + 1,
                    lambda t: sent.append(t) or True, exits_only=True)
    assert [k for _, _, k in ledger.rows] == ["range", "entry", "exit"] and out["open"] == 0


def test_a_stale_feed_counts_only_inside_a_traded_session():
    day = date(2026, 11, 4)
    dep = Dep({"orb_minutes": 15, "target_r": 2.0, "exit": "trail"})
    start, end = live.session_bounds("ES", day)
    today = live.Day(day, {"ES": "ESZ6"}, {})
    assert live.feed_stale(dep, today, start + 3_600_000, start + 3_600_000 - 200_000)  # 200 s, in session
    assert not live.feed_stale(dep, today, start + 3_600_000, start + 3_600_000 - 60_000)  # a minute: fine
    assert not live.feed_stale(dep, today, end + 3_600_000, end)  # after the close
    blocked = live.Day(day, {"ES": "ESZ6"}, {"ES": "a roll day"})
    assert not live.feed_stale(dep, blocked, start + 3_600_000, start)  # not traded today
