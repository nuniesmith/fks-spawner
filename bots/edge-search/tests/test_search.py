from datetime import datetime, timezone

import numpy as np
import pytest
from conftest import T0, bars, random_walk, regimes

from edge_search import config, search
from edge_search.dataset import parse_spec

NOW = datetime(2026, 10, 27, 9, 0, tzinfo=timezone.utc)
N = 3200  # ~8.8 years of daily bars


@pytest.fixture(autouse=True)
def fewer_random_strategies(monkeypatch):
    monkeypatch.setitem(config.THRESHOLDS, "random_strategies", 200)


def spec():
    return parse_spec("raw/binance/spot/spot-history 1d 2018-01-01..2026-10-07")


def test_a_random_walk_is_not_an_edge(hypothesis):
    h = hypothesis()
    series = [bars(s, random_walk(N, seed=i)) for i, s in enumerate(h.symbols)]
    r = search.run_search(h, "trend-band", spec(), series, prior_trials=100, now=NOW)
    assert r.verdict.label == "DO NOT PASTE"
    assert r.n_trials == 20 and r.trials_total == 120
    assert len(r.verdict.clauses) == 9
    assert [c.id for c in r.verdict.clauses] == [f"U{i}" for i in range(1, 10)]
    assert r.coverage == {s: pytest.approx(1.0, abs=1e-3) for s in h.symbols}
    # The holdout is the last quarter, by time.
    assert r.test_days == pytest.approx(N * 0.25, abs=2)


def test_a_trend_beats_holding_but_three_coins_trade_too_rarely_to_prove_it(hypothesis):
    h = hypothesis()
    series = [bars(s, regimes(N, seed=10 + i)) for i, s in enumerate(h.symbols)]
    r = search.run_search(h, "trend-band", spec(), series, prior_trials=0, now=NOW)
    by_id = {c.id: c for c in r.verdict.clauses}
    assert by_id["U3"].ok, by_id["U3"].text  # beats buy-and-hold on the holdout
    assert r.holdout_score > r.baseline_holdout_score
    assert not by_id["U4"].ok, by_id["U4"].text  # a handful of trades, not 30
    assert r.verdict.label == "DO NOT PASTE"
    assert any(x.startswith("U4:") for x in r.verdict.reasons)


def test_holes_and_a_late_registration_fail_their_clauses(hypothesis):
    h = hypothesis(registered_at=datetime(2026, 10, 28, tzinfo=timezone.utc))
    closes = random_walk(N, seed=5)
    holey = bars("AAAUSDT", closes)
    keep = np.ones(N, dtype=bool)
    keep[1000:1100] = False  # 100 missing days
    holey.open_ms, holey.open, holey.close = holey.open_ms[keep], holey.open[keep], holey.close[keep]
    series = [holey, bars("BBBUSDT", random_walk(N, seed=6)), bars("CCCUSDT", random_walk(N, seed=7))]
    r = search.run_search(h, "trend-band", spec(), series, prior_trials=0, now=NOW)
    by_id = {c.id: c for c in r.verdict.clauses}
    assert not by_id["U1"].ok and "AAAUSDT" in by_id["U1"].text
    assert not by_id["U2"].ok


def test_a_one_point_grid_has_no_neighbours_or_edges_to_check(hypothesis):
    h = hypothesis(params=[{"name": "lookback", "choices": [200]}, {"name": "band", "choices": [0.05]}])
    series = [bars(s, regimes(N, seed=20 + i)) for i, s in enumerate(h.symbols)]
    r = search.run_search(h, "trend-band", spec(), series, prior_trials=0, now=NOW)
    by_id = {c.id: c for c in r.verdict.clauses}
    assert r.n_trials == 1
    assert by_id["U6"].ok and "one-point grid" in by_id["U6"].text
    assert by_id["U9"].ok


@pytest.mark.parametrize(
    "over, needle",
    [
        ({"metric": "vibes"}, "metric 'vibes'"),
        ({"baseline": "zero"}, "baseline 'zero'"),
        ({"params": [{"name": "lookback", "min": 1, "max": 5000, "step": 1},
                     {"name": "band", "min": 0.0, "max": 0.1, "step": 0.01}]}, "more than 5000"),
        ({"params": [{"name": "lookback", "min": 2000, "max": 3000, "step": 500},
                     {"name": "band", "min": 0.02, "max": 0.04, "step": 0.02}]}, "not enough data"),
    ],
)
def test_a_search_that_cant_run_as_registered_says_why(hypothesis, over, needle):
    h = hypothesis(**over)
    series = [bars(s, random_walk(N, seed=1)) for s in h.symbols]
    with pytest.raises(search.SearchError, match=needle):
        search.run_search(h, "trend-band", spec(), series, prior_trials=0, now=NOW)


def test_the_fingerprint_names_the_problem(hypothesis):
    s = spec()
    a = search.fingerprint("trend-band", hypothesis(), s, ["lookback"], [[50, 100]])
    assert a == search.fingerprint("trend-band", hypothesis(), s, ["lookback"], [[50, 100]])
    assert a != search.fingerprint("trend-band", hypothesis(), s, ["lookback"], [[50, 150]])
    assert a != search.fingerprint("trend-band", hypothesis(costs={"maker_bps": 0, "taker_bps": 0, "slippage_bps": 0}),
                                   s, ["lookback"], [[50, 100]])
    assert a.startswith("trend-band:")


def test_the_series_start_is_where_the_data_starts(hypothesis):
    # A coin that lists two years in joins the pool when it lists.
    h = hypothesis()
    late = bars("CCCUSDT", random_walk(N - 730, seed=9), start_ms=T0 + 730 * 86_400_000)
    series = [bars("AAAUSDT", random_walk(N, seed=8)), bars("BBBUSDT", random_walk(N, seed=10)), late]
    r = search.run_search(h, "trend-band", spec(), series, prior_trials=0, now=NOW)
    assert r.coverage["CCCUSDT"] == pytest.approx(1.0, abs=1e-3)
