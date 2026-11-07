"""The carry family: long spot, short the perpetual, on one venue.

Per bar held: spot return - perpetual return + funding, on capital
1 + 1/leverage; fees on both legs; the short liquidated on the bar whose
HIGH reaches entry x (1 + 1/leverage - MAINT_MARGIN). Every number below is
worked by hand, so a wrong sign or a wrong fill fails loudly.
"""

import gzip
from datetime import date, datetime, timezone

import numpy as np
import pytest
from conftest import T0, bars, day_ms, days

from edge_search import carry, config, search
from edge_search.dataset import (
    CarryPair,
    DatasetError,
    carry_folders,
    load_carry,
    parse_spec,
    spot_symbol,
)

H = 3_600_000
DAY = 86_400_000
NOW = datetime(2026, 10, 27, 9, 0, tzinfo=timezone.utc)
M = carry.MAINT_MARGIN


@pytest.fixture(autouse=True)
def fewer_random_strategies(monkeypatch):
    monkeypatch.setitem(config.THRESHOLDS, "random_strategies", 200)


def pair(symbol, perp_close, spot_close, funding_per_day, high=None, opens=None):
    p = bars(symbol, perp_close)
    if high is not None:
        p.high = np.asarray(high, dtype=float)
    if opens is not None:
        p.open = np.asarray(opens, dtype=float)
    f = np.asarray(funding_per_day, dtype=float)
    # one settlement at each bar's close, carrying that bar's funding
    p.funding_ms = T0 + DAY * np.arange(1, len(f) + 1, dtype=np.int64)
    p.funding_rate = f
    return CarryPair(perp=p, spot=bars("S" + symbol, spot_close))


def data(*pairs_):
    spec = parse_spec("raw/kucoin/futures/kf+raw/kucoin/spot/ks 1d 2018-01-01..2018-01-31")
    return carry.prepare(list(pairs_), DAY, spec)


# ── the dataset ───────────────────────────────────────────────────────────────


def test_a_kucoin_perpetual_hedges_with_its_spot_pair():
    assert spot_symbol("kucoin/futures/kf", "XBTUSDTM") == "BTCUSDT"
    assert spot_symbol("kucoin/futures/kf", "ETHUSDTM") == "ETHUSDT"
    assert spot_symbol("binance/usdm/u", "BTCUSDT") == "BTCUSDT"
    with pytest.raises(DatasetError, match="USDT perpetual"):
        spot_symbol("kucoin/futures/kf", "XBTUSDCM")


@pytest.mark.parametrize("text, needle", [
    ("raw/kucoin/futures/kf 1d 2020-01-01..2021-01-01", "perpetual first"),
    ("raw/kucoin/spot/ks+raw/kucoin/futures/kf 1d 2020-01-01..2021-01-01", "isn't a perpetual folder"),
    ("raw/kucoin/futures/kf+raw/binance/spot/s 1d 2020-01-01..2021-01-01", "perpetual's venue"),
])
def test_the_two_legs_are_named_perpetual_first_on_one_venue(text, needle):
    with pytest.raises(DatasetError, match=needle):
        carry_folders(parse_spec(text))


def write_csv_gz(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress((header + "\n" + "".join(",".join(map(str, r)) + "\n" for r in rows)).encode(), mtime=0))


def test_a_pair_loads_from_the_kucoin_recorders(tmp_path):
    d = days(date(2026, 9, 1), 30)
    k = "open_time_ms,open,high,low,close,volume,turnover"
    write_csv_gz(tmp_path / "kucoin/futures/kf/XBTUSDTM/klines/1d/XBTUSDTM-1d-2026-09.csv.gz", k,
                 [[day_ms(x), 100, 103, 99, 101, 5, 7] for x in d])
    write_csv_gz(tmp_path / "kucoin/futures/kf/XBTUSDTM/fundingRate/XBTUSDTM-fundingRate-2026-09.csv.gz",
                 "funding_time_ms,funding_rate", [[day_ms(d[0]) + k_ * 8 * H, 0.0001] for k_ in range(1, 91)])
    write_csv_gz(tmp_path / "kucoin/spot/ks/BTCUSDT/klines/1d/BTCUSDT-1d-2026-09.csv.gz", k,
                 [[day_ms(x), 100, 102, 98, 100.5, 5, 7] for x in d])
    p = load_carry(tmp_path, parse_spec("raw/kucoin/futures/kf+raw/kucoin/spot/ks 1d 2026-09-01..2026-09-30"), "XBTUSDTM")
    assert p.spot.symbol == "BTCUSDT" and len(p.spot.open_ms) == 30
    assert list(p.perp.high) == [103.0] * 30 and len(p.perp.funding_ms) == 90
    assert p.spot.funding_ms is None


# ── the rule ──────────────────────────────────────────────────────────────────


def test_leverage_is_declared_and_exits_sit_below_entries():
    ok = [{"lookback": 7, "enter_apr": 0.2, "exit_apr": 0.05, "leverage": 2}]
    assert carry.check_params(ok) == 2.0
    with pytest.raises(carry.CarryError, match="not searched"):
        carry.check_params(ok + [{"lookback": 7, "enter_apr": 0.2, "exit_apr": 0.05, "leverage": 3}])
    with pytest.raises(carry.CarryError, match="at or below every entry"):
        carry.check_params(ok + [{"lookback": 7, "enter_apr": 0.1, "exit_apr": 0.15, "leverage": 2}])
    with pytest.raises(carry.CarryError, match="outside"):
        carry.check_params([{"lookback": 7, "enter_apr": 0.2, "exit_apr": 0.05, "leverage": 50}])


def test_the_trailing_rate_enters_and_leaves_with_hysteresis():
    # 1 bar a day, so a daily funding of x is x * 365 a year
    f = [0.0, 0.0006, 0.0006, 0.0003, 0.0001, 0.0006, 0.0]
    d = data(pair("XBTUSDTM", [100] * 7, [100] * 7, f))
    # lookback 1: the trailing rate is that bar's own funding x 365
    w = carry.wanted(d, 1, enter_apr=0.2, exit_apr=0.05)
    #   bar:  0     1(0.219) 2(0.219) 3(0.1095: hold) 4(0.0365: out) 5(in) 6(0: out)
    assert w[0].tolist() == [0, 1, 1, 1, 0, 1, 0]
    # lookback 3 has no rate (so stays out) until three bars exist
    assert carry.wanted(d, 3, 0.2, 0.05)[0, :2].tolist() == [0, 0]


# ── the pair's arithmetic ─────────────────────────────────────────────────────


def test_a_held_pair_earns_funding_and_the_basis_and_pays_both_legs():
    f, c, lev = 0.001, 0.0005, 2.0
    perp = [100, 100, 100, 101, 101]
    spot = [100, 100, 100, 100.5, 100.5]
    d = data(pair("XBTUSDTM", perp, spot, [f] * 5))
    sim = carry.simulate(d, np.array([[0, 1, 1, 1, 0]], dtype=float), c, lev)
    cap = 1 + 1 / lev
    want = [
        0.0,
        -2 * c / cap,                              # enter at close 1: both legs
        f / cap,                                   # held through bar 2
        (0.005 - 0.01 + f) / cap,                  # bar 3: spot +0.5%, perp +1% (the short loses the basis)
        (f - 2 * c) / cap,                         # bar 4 held, exit at its close
    ]
    assert np.isnan(sim.net[0, 0]) or sim.net[0, 0] == 0.0
    assert sim.net[0, 1:].tolist() == pytest.approx(want[1:])
    assert sim.liquidated.sum() == 0
    assert sim.received[0].tolist() == pytest.approx([0, 0, f, f, f])


def test_a_wick_through_the_liquidation_price_is_a_real_loss():
    lev, c = 2.0, 0.0005
    level = 100 * (1 + 1 / lev - M)  # 149
    perp = [100, 100, 110, 112, 112]
    spot = [100, 100, 110, 112, 112]
    high = [100, 100, 155, 113, 113]  # bar 2 wicks to 155 and closes at 110
    d = data(pair("XBTUSDTM", perp, spot, [0.001] * 5, high=high))
    sim = carry.simulate(d, np.array([[0, 1, 1, 0, 0]], dtype=float), c, lev)
    cap = 1 + 1 / lev
    # bar 2: the short closes at 149 (from 100), the spot sells at 110, the
    # maintenance margin is lost, one leg's fee, no funding; then the rule
    # (still in) enters again at 110: both legs' fees.
    liq_bar = 0.10 - (level / 100 - 1) - M - c - 2 * c
    assert sim.net[0, 2] == pytest.approx(liq_bar / cap)
    assert sim.liquidated[0].tolist() == [0, 0, 1, 0, 0]
    assert sim.received[0, 2] == 0.0
    # re-entered at 110: bar 3 is an ordinary held bar (both legs +1.82%)
    assert sim.net[0, 3] == pytest.approx((0.001 - 2 * c) / cap)
    assert sim.pos[0].tolist() == [0, 1, 1, 0, 0]


def test_a_bar_that_opens_beyond_the_level_fills_at_the_open():
    lev, c = 2.0, 0.0
    perp = [100, 100, 165, 165]
    spot = [100, 100, 165, 165]
    d = data(pair("XBTUSDTM", perp, spot, [0.0] * 4, high=[100, 100, 170, 165], opens=[100, 100, 160, 165]))
    sim = carry.simulate(d, np.array([[0, 1, 0, 0]], dtype=float), c, lev)
    # filled at the open, 160, not at 149: the gap is the short's
    assert sim.net[0, 2] == pytest.approx((0.65 - 0.60 - M) / (1 + 1 / lev))


def test_higher_leverage_liquidates_sooner():
    perp = [100, 100, 130, 130]
    d = data(pair("XBTUSDTM", perp, perp, [0.0] * 4, high=[100, 100, 131, 130]))
    want = np.array([[0, 1, 1, 1]], dtype=float)
    assert carry.simulate(d, want, 0.0, 2.0).liquidated.sum() == 0  # level 149
    assert carry.simulate(d, want, 0.0, 4.0).liquidated.sum() == 1  # level 124


def test_always_carry_and_the_report():
    f = 0.0005
    d = data(pair("XBTUSDTM", [100] * 6, [100] * 6, [f] * 6))
    w = carry.always(d, 2)
    assert w[0].tolist() == [0, 0, 1, 1, 1, 1]
    sim = carry.simulate(d, w, 0.0, 2.0)
    r = carry.report(d, sim, 4, 2.0)
    assert r["funding_received_per_year_in_pair"] == pytest.approx(f * 365)
    assert r["liquidations"] == 0 and r["capital_per_notional"] == 1.5
    # a settlement at every bar's close, bar 0's included: the full rate
    assert r["mean_funding_apr"]["XBTUSDTM"] == pytest.approx(f * 365)


# ── the search ────────────────────────────────────────────────────────────────


def synthetic_pairs(n, symbols, seed=3):
    rng = np.random.default_rng(seed)
    out = []
    for k, s in enumerate(symbols):
        spot = 100 * np.exp(np.cumsum(rng.normal(0.0005, 0.03, n)))
        basis = 1 + 0.002 * np.sin(np.arange(n) / 9.0) + rng.normal(0, 0.0005, n)
        perp = spot * basis
        # funding regimes: crowded longs for ~200 days, then quiet
        regime = (np.arange(n) // 200 + k) % 2 == 0
        f = np.where(regime, 0.0009, 0.00005) + rng.normal(0, 0.0001, n)
        out.append(pair(s, perp, spot, f, high=perp * (1 + np.abs(rng.normal(0, 0.01, n)))))
    return out


def test_a_carry_search_runs_every_clause_and_says_what_the_pair_did(hypothesis):
    n = 3200
    h = hypothesis(
        symbols=["XBTUSDTM", "ETHUSDTM", "SOLUSDTM"],
        dataset="raw/kucoin/futures/kf+raw/kucoin/spot/ks 1d 2018-01-01..2026-10-07",
        params=[{"name": "lookback", "choices": [7, 30]}, {"name": "enter_apr", "choices": [0.15, 0.25]},
                {"name": "exit_apr", "choices": [0.0, 0.05]}, {"name": "leverage", "choices": [2]}],
        baseline="always-carry",
        costs={"maker_bps": 2, "taker_bps": 10, "slippage_bps": 2},
    )
    r = search.run_search(h, "carry", parse_spec(h.dataset), synthetic_pairs(n, h.symbols), prior_trials=0, now=NOW)
    assert r.n_trials == 8 and [c.id for c in r.verdict.clauses] == [f"U{i}" for i in range(1, 14)]
    by_id = {c.id: c for c in r.verdict.clauses}
    assert by_id["U13"].ok and "cash account" in by_id["U13"].text
    rep = r.carry
    assert rep["leverage"] == 2.0 and rep["capital_per_notional"] == 1.5
    assert set(rep) >= {"liquidations", "holdout_liquidations", "funding_received_per_year_in_pair", "mean_funding_apr"}
    # the regimes pay ~0.0009 a day when crowded: a selection that waits for
    # them receives far more than the average rate
    assert rep["funding_received_per_year_in_pair"] > np.mean(list(rep["mean_funding_apr"].values()))
    assert r.funding is None


def test_carry_is_judged_against_always_carry(hypothesis):
    h = hypothesis(
        symbols=["XBTUSDTM"], dataset="raw/kucoin/futures/kf+raw/kucoin/spot/ks 1d 2018-01-01..2026-10-07",
        params=[{"name": "lookback", "choices": [7]}, {"name": "enter_apr", "choices": [0.2]},
                {"name": "exit_apr", "choices": [0.05]}, {"name": "leverage", "choices": [2]}],
    )
    with pytest.raises(search.SearchError, match="always-carry"):
        search.run_search(h, "carry", parse_spec(h.dataset), synthetic_pairs(3200, h.symbols), prior_trials=0, now=NOW)


def test_u10_stresses_the_fees_not_the_funding_or_the_margin():
    f, c = 0.001, 0.0005
    d = data(pair("XBTUSDTM", [100] * 5, [100] * 5, [f] * 5))
    w = np.array([[0, 1, 1, 1, 0]], dtype=float)
    one, two = carry.simulate(d, w, c, 2.0), carry.simulate(d, w, 2 * c, 2.0)
    # only the entry and exit bars move, by one more fee on each leg
    diff = (one.net - two.net)[0, 1:]
    assert diff.tolist() == pytest.approx([2 * c / 1.5, 0.0, 0.0, 2 * c / 1.5])
