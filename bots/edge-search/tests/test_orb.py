"""The orb family (edge_search/orb.py) on synthetic 1-minute sessions."""

import csv
import gzip
from datetime import date, datetime, time, timedelta, timezone

import numpy as np
import pytest

from edge_search import config, orb, search
from edge_search.dataset import DatasetError, MinuteBars, load_bars, load_cme_minutes, parse_spec

NOW = datetime(2026, 10, 27, 9, 0, tzinfo=timezone.utc)
COSTS = {"maker_bps": 0, "taker_bps": 0, "slippage_bps": 0, "commission_usd_rt": 4.0, "slippage_ticks": 1}


@pytest.fixture(autouse=True)
def fewer_random_strategies(monkeypatch):
    monkeypatch.setitem(config.THRESHOLDS, "random_strategies", 200)


def session_start_ms(d: date, root: str = "ES") -> int:
    k = orb.CONTRACTS[root]
    return int(datetime.combine(d, k.start, tzinfo=orb.ET).timestamp() * 1000)


def minute_bars(root: str, sessions: dict[date, np.ndarray], roll_days=()) -> MinuteBars:
    """sessions: date -> [minutes, 4] (o, h, l, c); NaN rows are missing bars."""
    t, rows = [], []
    for d in sorted(sessions):
        s0 = session_start_ms(d, root)
        for j, r in enumerate(sessions[d]):
            if not np.isnan(r).any():
                t.append(s0 + j * 60_000)
                rows.append(r)
    a = np.array(rows, dtype=float)
    return MinuteBars(root=root, open_ms=np.array(t, dtype=np.int64), open=a[:, 0], high=a[:, 1],
                      low=a[:, 2], close=a[:, 3], volume=np.ones(len(a)), roll_days=set(roll_days))


def flat(n: int, price: float) -> np.ndarray:
    return np.tile([price, price, price, price], (n, 1)).astype(float)


def orb_session(n: int = 390, base: float = 5000.0) -> np.ndarray:
    """A 15-minute range of 4999.5..5000.5, then flat at 5000 unless changed."""
    s = flat(n, base)
    s[:15] = [base, base + 0.5, base - 0.5, base]
    return s


def spec_for(days, path="raw/cme/test"):
    # A window ends after it starts: one session gets the day after, too.
    return parse_spec(f"{path} 1m {min(days)}..{max(max(days), min(days) + timedelta(1))}")


def one_trade(sess: np.ndarray, d=date(2026, 10, 20), params=None, costs=COSTS, root="ES"):
    params = params or {"orb_minutes": 15, "target_r": 2.0, "exit": "target"}
    data = orb.prepare([root], [minute_bars(root, {d: sess})], spec_for([d]), costs)
    return data, orb.trades_for(data, params)


# ── The calendar ──────────────────────────────────────────────────────────────


def test_us_exchange_holidays():
    h24 = orb.us_holidays(2024)
    for d in ("2024-01-01", "2024-01-15", "2024-02-19", "2024-03-29", "2024-05-27", "2024-06-19",
              "2024-07-04", "2024-09-02", "2024-11-28", "2024-12-25"):
        assert date.fromisoformat(d) in h24, d
    assert len(h24) == 10
    # Saturday New Year isn't moved to Friday; Sunday holidays move to Monday.
    assert date(2021, 12, 31) not in orb.us_holidays(2021) and date(2022, 1, 1) not in orb.us_holidays(2022)
    assert {date(2022, 6, 20), date(2022, 12, 26)} <= orb.us_holidays(2022)
    assert {date(2021, 7, 5), date(2021, 12, 24)} <= orb.us_holidays(2021)
    assert date(2025, 4, 18) in orb.us_holidays(2025) and date(2025, 1, 9) in orb.us_holidays(2025)
    assert date(2026, 4, 3) in orb.us_holidays(2026)
    assert not any(d.month == 6 and d.day in (19, 20) for d in orb.us_holidays(2021))  # Juneteenth from 2022


def test_sessions_follow_new_york_time_across_dst():
    fri, mon = date(2026, 3, 6), date(2026, 3, 9)  # DST starts 2026-03-08
    assert session_start_ms(fri) - int(datetime(2026, 3, 6, 14, 30, tzinfo=timezone.utc).timestamp() * 1000) == 0
    assert session_start_ms(mon) - int(datetime(2026, 3, 9, 13, 30, tzinfo=timezone.utc).timestamp() * 1000) == 0
    data = orb.prepare(["ES"], [minute_bars("ES", {fri: orb_session(), mon: orb_session()})],
                       spec_for([fri, mon]), COSTS)
    s = data.sessions[0]
    assert s.dates == [fri, mon] and s.complete.all()
    gold = orb.CONTRACTS["GC"]
    assert (gold.start, gold.end) == (time(8, 20), time(13, 30))


# ── One trade, by hand ────────────────────────────────────────────────────────


def test_a_breakout_to_the_target():
    s = orb_session()
    s[20] = [5000.0, 5001.0, 5000.0, 5001.0]  # closes above 5000.5: long at 5001 + 0.25
    s[30] = [5004.0, 5004.75, 5003.5, 5004.5]  # touches 5004.75, not through it
    s[31] = [5004.5, 5005.0, 5004.0, 5004.5]  # trades a tick through: filled at the target
    _, trades = one_trade(s)
    (t,) = trades
    entry, risk = 5001.25, 5001.25 - 4999.5
    target = entry + 2 * risk
    assert (t.side, t.entry_min, t.exit_min) == (1, 20, 31)
    assert t.ret == pytest.approx((target - entry) / entry - 4.0 / (entry * 50))


def test_a_bar_that_touches_stop_and_target_is_the_stop():
    s = orb_session()
    s[20] = [5000.0, 5001.0, 5000.0, 5001.0]
    s[25] = [5001.0, 5010.0, 4990.0, 5001.0]  # both in one bar
    _, (t,) = one_trade(s)
    assert t.exit_min == 25
    assert t.ret == pytest.approx((4999.5 - 0.25 - 5001.25) / 5001.25 - 4.0 / (5001.25 * 50))


def test_a_gap_through_the_stop_fills_at_the_open():
    s = orb_session()
    s[20] = [5000.0, 5001.0, 5000.0, 5001.0]
    s[21:] = 4990.0  # the next bar opens far below the 4999.5 stop
    _, (t,) = one_trade(s)
    assert t.exit_min == 21
    assert t.ret == pytest.approx((4990.0 - 0.25 - 5001.25) / 5001.25 - 4.0 / (5001.25 * 50))


def test_a_short_breakout_and_a_close_at_the_session_end():
    s = orb_session()
    s[40] = [5000.0, 5000.0, 4999.0, 4999.0]  # closes below 4999.5: short at 4999 - 0.25
    s[41:] = 4998.0  # drifts lower, never to the target
    _, (t,) = one_trade(s)
    assert (t.side, t.entry_min, t.exit_min) == (-1, 40, 389)
    entry = 4998.75
    assert t.ret == pytest.approx((entry - (4998.0 + 0.25)) / entry - 4.0 / (entry * 50))


def test_no_breakout_no_trade():
    _, trades = one_trade(orb_session())
    assert trades == []


def test_the_trail_keeps_target_minus_one_r_and_follows_the_best_price():
    s = orb_session()
    s[20] = [5000.0, 5001.0, 5000.0, 5001.0]  # long 5001.25, risk 1.75, 2R = 5004.75
    s[30] = [5004.0, 5004.75, 5004.0, 5004.5]  # touches 2R: the stop is now 1R below 5004.75
    s[31] = [5004.5, 5008.0, 5004.5, 5007.5]  # best 5008: the stop moves to 5006.25
    s[32] = [5007.5, 5007.5, 5006.0, 5006.0]  # through 5006.25
    s[33:] = 5006.0
    _, (t,) = one_trade(s, params={"orb_minutes": 15, "target_r": 2.0, "exit": "trail"})
    assert t.exit_min == 32
    assert t.ret == pytest.approx((5006.25 - 0.25 - 5001.25) / 5001.25 - 4.0 / (5001.25 * 50))
    _, (u,) = one_trade(s)  # the same path with exit=target takes 2R at minute 31
    assert u.exit_min == 31


def test_roll_days_holidays_and_thin_sessions_are_not_traded():
    breakout = orb_session()
    breakout[20] = [5000.0, 5001.0, 5000.0, 5001.0]
    thin = breakout.copy()
    thin[100:120] = np.nan  # 95% of the minutes
    roll, holiday, ok, gap = date(2026, 10, 19), date(2026, 11, 26), date(2026, 10, 20), date(2026, 10, 21)
    bars = minute_bars("ES", {roll: breakout, holiday: breakout, ok: breakout, gap: thin}, roll_days=[roll])
    data = orb.prepare(["ES"], [bars], spec_for([roll, holiday]), COSTS)
    trades = orb.trades_for(data, {"orb_minutes": 15, "target_r": 2.0, "exit": "target"})
    assert [data.sessions[0].dates[t.session] for t in trades] == [ok]


def test_orb_costs_are_per_contract():
    with pytest.raises(orb.OrbError, match="taker_bps"):
        orb.costs_of({**COSTS, "taker_bps": 80})
    with pytest.raises(orb.OrbError, match="commission_usd_rt"):
        orb.costs_of({"maker_bps": 0, "taker_bps": 0, "slippage_bps": 0})
    assert orb.costs_of(COSTS) == (4.0, 1.0)


def test_micro_contracts_trade_the_full_size_series_with_their_own_point_value():
    s = orb_session()
    s[20] = [5000.0, 5001.0, 5000.0, 5001.0]
    s[21:] = 4990.0
    d = date(2026, 10, 20)
    data = orb.prepare(["MES"], [minute_bars("ES", {d: s})], spec_for([d]), COSTS)
    (t,) = orb.trades_for(data, {"orb_minutes": 15, "target_r": 2.0, "exit": "target"})
    assert orb.contract("MES").root == "ES"
    assert orb.contract("MGC").root == "GC"  # micro gold on gold's prices (Databento has no MGC)
    assert t.ret == pytest.approx((4990.0 - 0.25 - 5001.25) / 5001.25 - 4.0 / (5001.25 * 5))


# ── Synthetic markets for whole searches ─────────────────────────────────────


def weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    hol = orb.us_holidays(start.year) | orb.us_holidays(start.year + 1) | orb.us_holidays(start.year + 2)
    while len(out) < n:
        if d.weekday() < 5 and d not in hol:
            out.append(d)
        d += timedelta(1)
    return out


def market(days: list[date], seed: int, momentum: float, n: int = 390, base: float = 5000.0) -> dict:
    """Minute sessions: noise, plus (momentum > 0) a drift after the first 15
    minutes in a random direction per session: a breakout's home ground."""
    rng = np.random.default_rng(seed)
    out = {}
    price = base
    for d in days:
        drift = np.zeros(n)
        drift[15:] = momentum * (1 if rng.random() < 0.5 else -1)
        steps = rng.normal(0, 0.0004, n) + drift
        closes = price * np.exp(np.cumsum(steps))
        opens = np.concatenate([[price], closes[:-1]])
        wiggle = np.abs(rng.normal(0, 0.0002, n)) * closes
        highs = np.maximum(opens, closes) + wiggle
        lows = np.minimum(opens, closes) - wiggle
        tick = 0.25
        q = lambda x: np.round(x / tick) * tick  # noqa: E731
        out[d] = np.column_stack([q(opens), q(highs), q(lows), q(closes)])
        price = float(closes[-1])
    return out


def orb_hypothesis(**over):
    base = dict(
        id=11, edge_id="orb-cme", idea="Opening-range breakouts carry on when the session trends.",
        symbols=["ES", "NQ", "MGC"], dataset="raw/cme/test 1m 2025-01-02..2026-08-31", holdout_share=0.25,
        params=[{"name": "orb_minutes", "choices": [5, 15, 30]},
                {"name": "target_r", "min": 1.0, "max": 3.0, "step": 0.5},
                {"name": "exit", "choices": ["target", "trail"]}],
        baseline="session-hold", metric="sharpe after fees", costs=COSTS,
        registered_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )
    base.update(over)
    return search.Hypothesis(**base)


def series_for(days, momentum):
    return [
        minute_bars("ES", market(days, 1, momentum)),
        minute_bars("NQ", market(days, 2, momentum, base=20000.0)),
        minute_bars("GC", market(days, 3, momentum, n=310, base=3000.0)),
    ]


DAYS = weekdays(date(2025, 1, 2), 420)


def window(days):
    return parse_spec(f"raw/cme/test 1m {days[0]}..{days[-1]}")


def test_a_planted_momentum_edge_beats_session_hold_and_random():
    h = orb_hypothesis()
    r = search.run_search(h, "orb", window(DAYS), series_for(DAYS, 0.00015), prior_trials=0, now=NOW)
    by_id = {c.id: c for c in r.verdict.clauses}
    assert r.n_trials == 3 * 5 * 2
    assert by_id["U1"].ok, by_id["U1"].text
    assert by_id["U3"].ok, by_id["U3"].text
    assert by_id["U4"].ok and r.holdout_trades >= 30, by_id["U4"].text
    assert by_id["U8"].ok, by_id["U8"].text
    assert r.holdout_score > r.baseline_holdout_score
    assert set(r.holdout_trades_by_symbol) == {"ES", "NQ", "MGC"}
    assert r.regimes["up"]["bars"] + r.regimes["down"]["bars"] + r.regimes["unlabeled_bars"] == len(r.best.holdout)


def test_minute_noise_is_not_an_edge():
    r = search.run_search(orb_hypothesis(), "orb", window(DAYS), series_for(DAYS, 0.0), prior_trials=0, now=NOW)
    assert r.verdict.label == "DO NOT PASTE"


def test_the_random_benchmark_keeps_the_trade_count():
    days = DAYS[:200]
    data = orb.prepare(["ES"], [minute_bars("ES", market(days, 1, 0.00015))], window(days), COSTS)
    params = {"orb_minutes": 15, "target_r": 2.0, "exit": "target"}
    trades = orb.trades_for(data, params)
    hold = 150
    held = sum(1 for t in trades if t.day >= hold)
    for p in orb.random_pools(data, trades, params, hold, np.random.default_rng(1), 5):
        assert int((p[hold:] != 0).sum()) == held
        assert (p[:hold] == 0).all()


@pytest.mark.parametrize(
    "over,needle",
    [
        ({"baseline": "buy-and-hold"}, "session-hold"),
        ({"params": [{"name": "orb_minutes", "choices": [15]}, {"name": "target_r", "min": 1, "max": 2, "step": 1},
                     {"name": "exit", "choices": ["moon"]}]}, "exit must be one of"),
        ({"params": [{"name": "orb_minutes", "choices": [0]}, {"name": "target_r", "min": 1, "max": 2, "step": 1},
                     {"name": "exit", "choices": ["target"]}]}, "orb_minutes"),
        ({"costs": {"maker_bps": 40, "taker_bps": 80, "slippage_bps": 5}}, "per contract"),
        ({"symbols": ["ES", "NQ", "CL"]}, "isn't one"),
    ],
)
def test_an_orb_search_that_cant_run_as_registered_says_why(over, needle):
    h = orb_hypothesis(**over)
    days = DAYS[:60]
    series = series_for(days, 0.0)
    if len(h.symbols) == 3 and h.symbols[-1] == "CL":
        series[-1] = minute_bars("GC", market(days, 3, 0.0, n=310, base=3000.0))
    with pytest.raises(search.SearchError, match=needle):
        search.run_search(h, "orb", window(days), series, prior_trials=0, now=NOW)


def test_trend_band_refuses_the_intraday_baseline(hypothesis):
    from conftest import bars, random_walk
    h = hypothesis(baseline="session-hold")
    s = [bars(x, random_walk(400, seed=1)) for x in h.symbols]
    with pytest.raises(search.SearchError, match="buy-and-hold or trend-200d-5pct"):
        search.run_search(h, "trend-band", parse_spec("raw/binance/spot/spot-history 1d 2018-01-01..2019-02-04"),
                          s, prior_trials=0, now=NOW)


def test_a_window_is_searched_only_once_its_data_is_complete():
    days = DAYS[:200]
    h = orb_hypothesis()
    late = parse_spec(f"raw/cme/test 1m {days[0]}..{days[-1] + timedelta(30)}")
    with pytest.raises(search.SearchError, match="searched once it is complete"):
        search.run_search(h, "orb", late, series_for(days, 0.0), prior_trials=0, now=NOW)


def test_coverage_counts_missing_weekdays_but_not_holidays():
    days = weekdays(date(2026, 11, 2), 20)  # includes Thanksgiving week (11-26 is skipped)
    sess = {d: orb_session() for d in days}
    del sess[days[5]]  # one ordinary weekday missing
    data = orb.prepare(["ES"], [minute_bars("ES", sess)], window(days), COSTS)
    assert data.sessions[0].coverage == pytest.approx(19 / 20)


# ── The lake ──────────────────────────────────────────────────────────────────


def write_month(folder, root, rows):
    p = folder / root / "1m"
    p.mkdir(parents=True, exist_ok=True)
    with gzip.open(p / f"{root}-1m-2026-10.csv.gz", "wt", newline="") as f:
        w = csv.writer(f)
        w.writerow(["open_time_ms", "open", "high", "low", "close", "volume"])
        w.writerows(rows)


def test_two_folders_join_and_the_first_wins_a_whole_day(tmp_path):
    t = session_start_ms(date(2026, 10, 20))
    nxt = session_start_ms(date(2026, 10, 21))
    write_month(tmp_path / "cme/cache", "ES", [(t, 1, 2, 0.5, 1.5, 10), (t + 60_000, 1, 2, 0.5, 1.5, 10)])
    # The live folder's 10-20 bars (one overlapping, one the cache lacks) are
    # ignored: that day is the cache's. Its 10-21 bar is used.
    write_month(tmp_path / "cme/live", "ES", [(t + 60_000, 9, 9, 9, 9, 1), (t + 120_000, 3, 4, 2, 3, 7),
                                              (nxt, 5, 6, 4, 5, 2)])
    with (tmp_path / "cme/cache/ES/rolls.csv").open("w") as f:
        f.write("day,from_contract,to_contract,gap_points,detected_by\n2026-10-16,ESZ6,ESH7,60,volume\n")
    with (tmp_path / "cme/live/ES/roll_flips.csv").open("w") as f:
        f.write("day_utc,jumps,threshold_points\n2026-10-15,5,29.4\n")
    spec = parse_spec("raw/cme/cache+raw/cme/live 1m 2026-10-01..2026-10-31")
    assert spec.paths == ["cme/cache", "cme/live"]
    b = load_cme_minutes(tmp_path, spec, "ES")
    assert list(b.close) == [1.5, 1.5, 5.0]
    assert b.roll_days == {date(2026, 10, 16), date(2026, 10, 15)}
    with pytest.raises(DatasetError, match="1-minute"):
        load_cme_minutes(tmp_path, parse_spec("raw/cme/cache 5m 2026-10-01..2026-10-31"), "ES")
    with pytest.raises(DatasetError, match="one folder"):
        load_bars(tmp_path, spec, "ES")


def test_vendor_flagged_degraded_days_are_not_traded(tmp_path):
    folder = tmp_path / "cme" / "databento" / "ES"
    (folder / "1m").mkdir(parents=True)
    with gzip.open(folder / "1m" / "ES-1m-2026-10-01.csv.gz", "wt") as f:
        f.write(f"open_time_ms,open,high,low,close,volume\n{session_start_ms(date(2026, 10, 1))},1,1,1,1,1\n")
    (folder / "excluded_days.csv").write_text("day_utc,reason\n2026-10-01,degraded (databento)\n")
    b = load_cme_minutes(tmp_path, parse_spec("raw/cme/databento 1m 2026-09-30..2026-10-02"), "ES")
    assert date(2026, 10, 1) in b.roll_days

