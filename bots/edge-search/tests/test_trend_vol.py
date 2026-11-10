"""trend-vol: the live rule's signal, sized by realised volatility.

The strongest check is parity: with a target no volatility can bind, the
family must BE trend-band, bar for bar. The rest pins the sizing, the
rebalance band, what counts as a trade, and the fees on fractional changes.
"""

import math
from datetime import datetime, timezone

import numpy as np
import pytest
from conftest import bars, regimes

from edge_search import config, engine, search
from edge_search.dataset import parse_spec

NOW = datetime(2026, 10, 27, 9, 0, tzinfo=timezone.utc)
N = 3200


@pytest.fixture(autouse=True)
def fewer_random_strategies(monkeypatch):
    monkeypatch.setitem(config.THRESHOLDS, "random_strategies", 200)


def spec():
    return parse_spec("raw/binance/spot/spot-history 1d 2018-01-01..2026-10-07")


def test_realised_vol_is_the_annualised_sample_deviation_known_at_the_close():
    x = 0.02
    lr = [x, -x, x, -x, x]
    closes = np.array([100 * np.exp(np.cumsum([0.0] + lr))])
    v = engine.realised_vol(closes, 4, 365.0)
    # bars 0-3 have fewer than 4 returns; bar 4 uses returns 1..4: x, -x, x, -x
    assert np.isnan(v[0, :4]).all()
    w = np.array([x, -x, x, -x])
    assert v[0, 4] == pytest.approx(np.std(w, ddof=1) * math.sqrt(365))
    assert v[0, 5] == pytest.approx(np.std([-x, x, -x, x], ddof=1) * math.sqrt(365))


def test_sizing_enters_at_once_holds_inside_the_band_and_exits_at_once():
    signal = np.array([[0, 1, 1, 1, 1, 1, 0]], dtype=float)
    vol = np.array([[0.5, 1.0, 1.05, 2.0, np.nan, 0.4, 0.4]])
    pos = engine.vol_sized(signal, vol, target_vol=0.6, rebalance=0.1)
    #   0: no signal; 1: enters at 0.6/1.0; 2: 0.571 is inside the band, holds 0.6;
    #   3: 0.3 is outside, resizes; 4: vol unknown -> flat; 5: re-enters, capped
    #   at 1 (0.6/0.4 = 1.5); 6: signal off -> flat
    assert pos[0].tolist() == pytest.approx([0, 0.6, 0.6, 0.3, 0, 1.0, 0])


def test_a_resize_is_not_a_trade_and_pays_on_its_size():
    pos = np.array([[0, 0.5, 0.8, 0.8, 0, 0.4]])
    assert engine.entries_of(pos)[0].tolist() == [0, 1, 0, 0, 0, 1]
    assert engine.trades(pos, 0, 6) == 2
    rets = np.zeros((1, 6))
    rets[0, 0] = np.nan
    net = engine.run(pos, rets, 0.01).net[0]
    # each change pays cost x its size: 0.5, 0.3, 0.8, 0.4
    assert net[1:].tolist() == pytest.approx([-0.005, -0.003, 0.0, -0.008, -0.004])


def hyp(hypothesis, **params):
    grid = [{"name": k, "choices": v} for k, v in params.items()]
    return hypothesis(params=grid, baseline="trend-200d-5pct", evidence="low-frequency")


def test_an_unbinding_target_is_exactly_trend_band(hypothesis):
    series = [bars(s, regimes(N, seed=60 + i)) for i, s in enumerate(["AAAUSDT", "BBBUSDT", "CCCUSDT"])]
    tv = search.run_search(hyp(hypothesis, lookback=[200], band=[0.05], vol_lookback=[30],
                               target_vol=[1e9], rebalance=[0.0]),
                           "trend-vol", spec(), series, prior_trials=0, now=NOW)
    tb = search.run_search(hyp(hypothesis, lookback=[200], band=[0.05]),
                           "trend-band", spec(), series, prior_trials=0, now=NOW)
    # same warm-up (the baseline's 200 bars), so the same split and bars
    assert np.asarray(tv.best.holdout) == pytest.approx(np.asarray(tb.best.holdout))
    assert tv.holdout_trades == tb.holdout_trades
    assert tv.holdout_score == pytest.approx(tb.holdout_score)


def test_a_binding_target_cuts_size_but_not_the_trade_count(hypothesis):
    series = [bars(s, regimes(N, seed=70 + i)) for i, s in enumerate(["AAAUSDT", "BBBUSDT", "CCCUSDT"])]
    r = search.run_search(hyp(hypothesis, lookback=[200], band=[0.05], vol_lookback=[20, 30, 60],
                              target_vol=[0.05, 0.1, 0.2], rebalance=[0.1]),
                          "trend-vol", spec(), series, prior_trials=0, now=NOW)
    tb = search.run_search(hyp(hypothesis, lookback=[200], band=[0.05]),
                           "trend-band", spec(), series, prior_trials=0, now=NOW)
    assert r.n_trials == 9 and len(r.verdict.clauses) == 13
    # the regimes' daily vol (~1% a day, ~0.19 a year) binds these targets, but
    # sizing never adds an entry: the same signal, the same entries
    assert r.holdout_trades == tb.holdout_trades
    held = np.asarray(r.best.holdout)
    assert np.abs(held).max() < np.abs(np.asarray(tb.best.holdout)).max()


@pytest.mark.parametrize("bad, needle", [
    ({"vol_lookback": [1]}, "vol_lookback"),
    ({"target_vol": [0]}, "target_vol"),
    ({"rebalance": [1.0]}, "rebalance"),
])
def test_a_grid_that_cannot_size_is_refused_before_a_trial(hypothesis, bad, needle):
    params = {"lookback": [200], "band": [0.05], "vol_lookback": [30], "target_vol": [0.6], "rebalance": [0.1]}
    params.update(bad)
    series = [bars("AAAUSDT", regimes(N, seed=1))]
    with pytest.raises(search.SearchError, match=needle):
        search.run_search(hyp(hypothesis, **params), "trend-vol", spec(), series, prior_trials=0, now=NOW)
