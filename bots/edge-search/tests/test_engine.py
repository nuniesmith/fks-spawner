import numpy as np
import pytest

from edge_search import engine


def test_sma_and_the_band_with_hysteresis():
    closes = np.array([[10, 10, 10, 10, 12, 11.5, 10.5, 9.4, 9.9, 10.9, 11.2]], dtype=float)
    pos = engine.trend_band_positions(closes, lookback=3, band=0.05)
    # SMA(3) needs 3 bars. Bar 4: 12 > 10.67 x 1.05, in. Bar 5: 11.5 is inside
    # the band (10.61..11.73), held. Bar 6: 10.5 < 11.33 x 0.95, out. Bar 8:
    # 9.9 inside (9.44..10.43), held flat. Bar 9: 10.9 > 10.57, in. Bar 10:
    # 11.2 is ON the upper band (10.67 x 1.05), not through it: held.
    assert list(pos[0]) == [0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1]


def test_costs_are_paid_on_every_change_and_returns_lag_one_bar():
    rets = np.array([[np.nan, 0.10, 0.10, 0.10, 0.10]])
    pos = np.array([[0, 1, 1, 0, 0]], dtype=float)
    r = engine.run(pos, rets, cost=0.01)
    # enter at bar 1's close (cost), earn bars 2 and 3, exit at bar 3's close (cost)
    assert np.allclose(r.net[0, 1:], [-0.01, 0.10, 0.10 - 0.01, 0.0])
    assert engine.trades(pos, 0, 5) == 1


def test_a_pool_is_the_listed_symbols_equal_weight():
    rets = np.array([[np.nan, 0.02, 0.02], [np.nan, np.nan, 0.04]])
    pos = np.ones_like(rets)
    pos[1, :2] = 0
    r = engine.run(pos, rets, cost=0.0)
    assert r.pool[1] == pytest.approx(0.02)  # only the first had listed
    assert r.pool[2] == pytest.approx((0.02 + 0.0) / 2)  # the second just entered


def test_scores():
    flat = np.zeros(365)
    assert engine.cagr(flat, 365) == 0 and engine.sharpe(flat, 365) == 0
    up = np.full(365, np.expm1(np.log(1.1) / 365))
    assert engine.cagr(up, 365) == pytest.approx(0.1)
    assert engine.max_drawdown(np.array([0.1, -0.5, 0.2])) == pytest.approx(0.5)
    assert engine.score("calmar", np.array([0.1, -0.5, 0.2]), 3) == pytest.approx(
        engine.cagr(np.array([0.1, -0.5, 0.2]), 3) / 0.5
    )


def test_folds_and_grids():
    assert engine.folds(10, 30, 4) == [(10, 15), (15, 20), (20, 25), (25, 30)]
    names, values, combos = engine.grid(
        "trend-band",
        [{"name": "lookback", "min": 50, "max": 200, "step": 50}, {"name": "band", "min": 0.02, "max": 0.06, "step": 0.02}],
    )
    assert names == ["lookback", "band"]
    assert values == [[50, 100, 150, 200], [0.02, 0.04, 0.06]]
    assert len(combos) == 12 and combos[0] == {"lookback": 50, "band": 0.02}
    assert engine.grid_values({"name": "side", "choices": ["long", "short"]}) == ["long", "short"]


@pytest.mark.parametrize(
    "params, needle",
    [
        ([{"name": "lookback", "min": 50, "max": 200, "step": 50}], "missing ['band']"),
        ([{"name": "lookback", "min": 1, "max": 9, "step": 1}, {"name": "band", "min": 0, "max": 1, "step": 1},
          {"name": "z", "min": 0, "max": 1, "step": 1}], "not used: ['z']"),
        ([{"name": "lookback", "min": 50, "max": 200}, {"name": "band", "min": 0, "max": 1, "step": 1}], "positive step"),
    ],
)
def test_a_hypothesis_must_fit_the_family(params, needle):
    with pytest.raises(engine.FamilyError, match=needle.replace("[", r"\[").replace("]", r"\]")):
        engine.grid("trend-band", params)
