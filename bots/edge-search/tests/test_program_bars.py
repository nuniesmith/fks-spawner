"""The futures program's bars, U10-U13 (search.py, orb.account_path)."""

from datetime import date, timedelta

import numpy as np
import pytest
from test_orb import COSTS, DAYS, NOW, minute_bars, orb_hypothesis, orb_session, series_for, spec_for, window

from edge_search import orb, search




def test_the_planted_edge_meets_the_program_bars_with_an_account():
    h = orb_hypothesis(costs={**COSTS, "contracts": 1, "account_usd": 150_000, "trail_usd": 4_500})
    r = search.run_search(h, "orb", window(DAYS), series_for(DAYS, 0.00015), prior_trials=0, now=NOW)
    by_id = {c.id: c for c in r.verdict.clauses}
    for cid in ("U10", "U11", "U12", "U13"):
        assert by_id[cid].ok, (cid, by_id[cid].text)
    assert "no breach" in by_id["U13"].text and "$150,000" in by_id["U13"].text


def test_noise_fails_the_program_bars_and_a_futures_edge_must_declare_its_account():
    r = search.run_search(orb_hypothesis(), "orb", window(DAYS), series_for(DAYS, 0.0), prior_trials=0, now=NOW)
    by_id = {c.id: c for c in r.verdict.clauses}
    assert not by_id["U13"].ok and "no prop account declared" in by_id["U13"].text
    assert not (by_id["U10"].ok and by_id["U11"].ok and by_id["U12"].ok)


def test_a_tight_trail_breaches_on_a_losing_streak():
    d0 = date(2026, 10, 19)
    days = [d0 + timedelta(i) for i in range(5)]
    sessions = {}
    for d in days:  # every day a long breakout that reverses to the stop
        s = orb_session()
        s[15] = [5001.0, 5001.5, 5000.5, 5001.0]
        s[16:] = 4990.0
        sessions[d] = s
    data = orb.prepare(["ES"], [minute_bars("ES", sessions)], spec_for(days), COSTS)
    trades = orb.trades_for(data, {"orb_minutes": 15, "target_r": 2.0, "exit": "target"})
    assert len(trades) == 5 and all(t.pnl_usd < 0 and t.mae_usd <= t.pnl_usd for t in trades)
    path = orb.account_path(data, trades, 0, 1, 150_000, 4_500)
    assert not path["breached"]  # five 1-contract stop-outs: ~$2-3k
    tight = orb.account_path(data, trades, 0, 3, 150_000, 4_500)
    assert tight["breached"] and tight["worst_headroom"] <= 0
    # A loss is in dollars: one ES contract, entry 5001 + 1 tick, out at 4990 - 1 tick.
    assert trades[0].pnl_usd == pytest.approx((4990.0 - 0.25 - 5001.25) * 50 - 4.0)


def test_the_floor_trails_then_locks_at_the_starting_balance():
    class T:
        def __init__(self, day, pnl):
            self.day, self.pnl_usd, self.mae_usd = day, pnl, min(0.0, pnl)
    data = type("D", (), {"times": np.array([86_400_000 * i for i in range(4)])})()
    # +5,000 locks the floor at 150,000; then -4,000 leaves 151,000: no breach;
    # a further -1,500 closes at 149,500 under the locked floor.
    up_then_down = [T(0, 5_000), T(1, -4_000), T(2, -1_500)]
    p = orb.account_path(data, up_then_down, 0, 1, 150_000, 4_500)
    assert p["breached"] and p["breach_day"] == "1970-01-03" and p["floor_locked"]
    assert not orb.account_path(data, up_then_down[:2], 0, 1, 150_000, 4_500)["breached"]


def test_an_account_is_all_three_terms_or_none():
    assert search.account_terms(COSTS) is None
    with pytest.raises(search.SearchError, match="together"):
        search.account_terms({**COSTS, "contracts": 1})
