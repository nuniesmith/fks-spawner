import numpy as np
import pytest

from edge_search import verdict


def test_the_expected_best_of_n_rises_with_n():
    sharpes = np.random.default_rng(1).normal(0, 0.05, 200)
    a, b = verdict.expected_max_sharpe(sharpes, 10), verdict.expected_max_sharpe(sharpes, 10_000)
    assert 0 < a < b
    assert verdict.expected_max_sharpe(sharpes, 1) == 0.0


def test_the_deflated_sharpe_is_a_probability_that_falls_as_the_bar_rises():
    r = np.random.default_rng(2).normal(0.002, 0.01, 1000)
    low, high = verdict.deflated_sharpe(r, 0.0), verdict.deflated_sharpe(r, 0.3)
    assert 0.0 <= high < low <= 1.0
    assert low > 0.99  # a real 0.2/bar Sharpe over 1000 bars clears zero
    assert verdict.deflated_sharpe(np.zeros(10), 0.0) == 0.0


def test_random_strategies_have_the_same_spells_and_never_overlap():
    pos = np.zeros((2, 200))
    for s in (10, 60, 120):
        pos[0, s:s + 15] = 1
    pos[1, 50:150] = 1
    rng = np.random.default_rng(3)
    for _ in range(50):
        rp = verdict.random_positions(pos, 0, 200, rng)
        entries = (np.diff(rp[0]) > 0).sum() + (rp[0, 0] > 0)
        assert entries == 3 and rp[0].sum() == 45
        assert rp[1].sum() == 100


def test_boundaries_and_neighbours():
    names, values = ["lookback", "band"], [[50, 100, 150], [0.02, 0.04]]
    assert verdict.boundary_params({"lookback": 100, "band": 0.04}, names, values) == ["band=0.04"]
    assert verdict.boundary_params({"lookback": 100, "band": 0.04}, ["lookback"], [[100]]) == []
    near = verdict.neighbours({"lookback": 100, "band": 0.02}, names, values)
    assert {"lookback": 50, "band": 0.02} in near and {"lookback": 150, "band": 0.02} in near
    assert {"lookback": 100, "band": 0.04} in near and len(near) == 3


def test_the_verdict_lists_what_failed():
    v = verdict.Verdict()
    v.add("U1", True, "fine")
    v.add("U4", False, "9 trades")
    assert (v.usable, v.label, v.reasons) == (False, "DO NOT PASTE", ["U4: 9 trades"])
    assert verdict.Verdict().usable is False
