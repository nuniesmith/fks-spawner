"""A perpetual pays funding: reading it, binning it into bars, charging it.

Measured on KuCoin 2020-26, funding cost the live trend rule 12-19 CAGR points
a year (it is long when longs are crowded). A search that leaves funding out
prices a different instrument, so the reader refuses a perpetual whose
funding is missing or has a hole, and every run on a perpetual pays it.
"""

import gzip
import zipfile
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pytest
from conftest import T0, bars, day_ms, days, regimes

from edge_search import config, engine, search
from edge_search.dataset import (
    DatasetError,
    align,
    align_funding,
    is_perpetual,
    load_bars,
    parse_spec,
)

H = 3_600_000
DAY = 86_400_000
NOW = datetime(2026, 10, 27, 9, 0, tzinfo=timezone.utc)
N = 3200


@pytest.fixture(autouse=True)
def fewer_random_strategies(monkeypatch):
    monkeypatch.setitem(config.THRESHOLDS, "random_strategies", 200)


def write_csv_gz(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    text = header + "\n" + "".join(",".join(map(str, r)) + "\n" for r in rows)
    path.write_bytes(gzip.compress(text.encode(), mtime=0))


def kucoin_month(lake, inst, symbol, d0, n, rate=0.0001, funding_every=8 * H, funding_until=None):
    """n daily candles from d0, and funding every `funding_every` through the
    last candle's close (or `funding_until`), as the KuCoin recorder writes them."""
    base = lake / "kucoin" / "futures" / inst / symbol
    d = days(d0, n)
    write_csv_gz(base / "klines" / "1d" / f"{symbol}-1d-{d0:%Y-%m}.csv.gz",
                 "open_time_ms,open,high,low,close,volume,turnover",
                 [[day_ms(x), 100, 101, 99, 100 + i, 5, 500] for i, x in enumerate(d)])
    end = funding_until if funding_until is not None else day_ms(d[-1]) + DAY
    times = range(day_ms(d0) + funding_every, end + 1, funding_every)
    write_csv_gz(base / "fundingRate" / f"{symbol}-fundingRate-{d0:%Y-%m}.csv.gz",
                 "funding_time_ms,funding_rate", [[t, rate] for t in times])
    return d


# ── reading ───────────────────────────────────────────────────────────────────


def test_kucoin_files_are_read_with_their_funding(tmp_path):
    d = kucoin_month(tmp_path, "k", "XBTUSDTM", date(2026, 9, 1), 30)
    # a daily file overlapping the month must not double count
    write_csv_gz(tmp_path / "kucoin/futures/k/XBTUSDTM/klines/1d/XBTUSDTM-1d-2026-09-30.csv.gz",
                 "open_time_ms,open,high,low,close,volume,turnover", [[day_ms(d[-1]), 100, 101, 99, 129, 5, 500]])
    spec = parse_spec("raw/kucoin/futures/k 1d 2026-09-01..2026-09-30")
    assert is_perpetual(spec)
    b = load_bars(tmp_path, spec, "XBTUSDTM")
    assert list(b.open_ms) == [day_ms(x) for x in d] and list(b.close) == [100 + i for i in range(30)]
    # every settlement after the first bar's open, through the last close
    assert len(b.funding_ms) == 90 and b.funding_ms[0] == day_ms(d[0]) + 8 * H
    assert b.funding_ms[-1] == day_ms(d[-1]) + DAY and set(b.funding_rate) == {0.0001}


def test_spot_has_no_funding(tmp_path):
    write_csv_gz(tmp_path / "kucoin/spot/s/BTCUSDT/klines/1d/BTCUSDT-1d-2026-09.csv.gz",
                 "open_time_ms,open,high,low,close,volume,turnover",
                 [[day_ms(x), 1, 2, 0.5, 1.5, 5, 7] for x in days(date(2026, 9, 1), 30)])
    spec = parse_spec("raw/kucoin/spot/s 1d 2026-09-01..2026-09-30")
    assert not is_perpetual(spec)
    assert load_bars(tmp_path, spec, "BTCUSDT").funding_ms is None


def test_a_perpetual_without_funding_is_an_error(tmp_path):
    kucoin_month(tmp_path, "k", "XBTUSDTM", date(2026, 9, 1), 30)
    for f in (tmp_path / "kucoin/futures/k/XBTUSDTM/fundingRate").iterdir():
        f.unlink()
    spec = parse_spec("raw/kucoin/futures/k 1d 2026-09-01..2026-09-30")
    with pytest.raises(DatasetError, match="pays funding"):
        load_bars(tmp_path, spec, "XBTUSDTM")


def test_funding_that_stops_early_is_an_error_not_a_free_ride(tmp_path):
    d0 = date(2026, 9, 1)
    kucoin_month(tmp_path, "k", "XBTUSDTM", d0, 30, funding_until=day_ms(d0) + 27 * DAY)
    spec = parse_spec("raw/kucoin/futures/k 1d 2026-09-01..2026-09-30")
    with pytest.raises(DatasetError, match="no funding settlement from 2026-09-28 00:00 to 2026-10-01 00:00"):
        load_bars(tmp_path, spec, "XBTUSDTM")


def test_four_hour_funding_and_a_schedule_change_fit(tmp_path):
    # KuCoin moved from 04/12/20 to 00/08/16 UTC in 2025: a 4h step, not a hole
    d0 = date(2025, 6, 1)
    kucoin_month(tmp_path, "k", "SOLUSDTM", d0, 30, funding_every=4 * H)
    b = load_bars(tmp_path, parse_spec("raw/kucoin/futures/k 1d 2025-06-01..2025-06-30"), "SOLUSDTM")
    assert len(b.funding_ms) == 180


def test_binance_funding_settles_on_the_second(tmp_path):
    # the archive's calc_time runs a few ms past the hour; usdm zips: calc_time, interval, rate
    base = tmp_path / "binance/usdm/u/BTCUSDT"
    d = days(date(2026, 9, 1), 3)
    folder = base / "klines" / "1d"
    folder.mkdir(parents=True)
    with zipfile.ZipFile(folder / "BTCUSDT-1d-2026-09.zip", "w") as z:
        z.writestr("BTCUSDT-1d-2026-09.csv", "open_time,open,high,low,close,volume\n"
                   + "".join(f"{day_ms(x)},1,2,0.5,1.5,5\n" for x in d))
    (base / "fundingRate").mkdir()
    rows = [f"{day_ms(d[0]) + k * 8 * H + 5},8,0.0002" for k in range(1, 10)]
    with zipfile.ZipFile(base / "fundingRate" / "BTCUSDT-fundingRate-2026-09.zip", "w") as z:
        z.writestr("BTCUSDT-fundingRate-2026-09.csv", "calc_time,funding_interval_hours,last_funding_rate\n"
                   + "\n".join(rows) + "\n")
    b = load_bars(tmp_path, parse_spec("raw/binance/usdm/u 1d 2026-09-01..2026-09-03"), "BTCUSDT")
    assert list(b.funding_ms) == [day_ms(d[0]) + k * 8 * H for k in range(1, 10)]
    times, _ = align([b], DAY)
    f = align_funding([b], times, DAY)
    # 08:00, 16:00 and the next 00:00 belong to each day: three per day
    assert f.tolist() == [[pytest.approx(0.0006)] * 3]


# ── binning ───────────────────────────────────────────────────────────────────


def test_a_settlement_belongs_to_the_bar_whose_open_close_holds_it():
    b = bars("X", [100, 100, 100])
    b.funding_ms = np.array([T0, T0 + 8 * H, T0 + DAY, T0 + DAY + 1, T0 + 3 * DAY + 1], dtype=np.int64)
    b.funding_rate = np.array([1.0, 2.0, 4.0, 8.0, 16.0])
    times, _ = align([b], DAY)
    f = align_funding([b], times, DAY)
    # T0 is bar 0's open: it closes the bar before (none here). 8h and the close
    # (T0 + DAY) are bar 0's; T0 + DAY + 1 ms is bar 1's; past the data is dropped.
    assert f.tolist() == [[6.0, 8.0, 0.0]]
    assert align_funding([bars("Y", [1, 2])], times, DAY) is None


# ── charging ──────────────────────────────────────────────────────────────────


def test_a_held_bar_pays_its_funding_and_a_negative_rate_is_received():
    pos = np.array([[0.0, 1.0, 1.0, 0.0, 0.0]])
    rets = np.zeros((1, 5))
    rets[0, 0] = np.nan
    funding = np.array([[0.01, 0.01, 0.01, -0.02, 0.01]])
    net = engine.run(pos, rets, 0.0, funding).net
    # held on bars 2 and 3 (decided on the closes of 1 and 2)
    assert net[0, 1:].tolist() == [0.0, -0.01, 0.02, 0.0]
    assert np.isnan(net[0, 0])
    assert engine.run(pos, rets, 0.0).net[0, 1:].tolist() == [0.0, 0.0, 0.0, 0.0]
    assert engine.funding_paid(pos, funding, 1.0) == pytest.approx((0.01 - 0.02) / 2)


def test_holding_a_perpetual_pays_funding_every_bar():
    rets = np.zeros((1, 4))
    rets[0, 0] = np.nan
    funding = np.full((1, 4), 0.001)
    pool = engine.buy_and_hold(rets, 0.0, 1, funding).pool
    assert pool.tolist() == pytest.approx([0.0, 0.0, -0.001, -0.001])


# ── the search ────────────────────────────────────────────────────────────────


def perp(symbol, closes, rate):
    b = bars(symbol, closes)
    b.funding_ms = T0 + 8 * H * np.arange(1, 3 * len(closes) + 1, dtype=np.int64)
    b.funding_rate = np.full(len(b.funding_ms), rate)
    return b


def test_a_search_on_a_perpetual_pays_funding_everywhere_and_says_how_much(hypothesis):
    h = hypothesis(params=[{"name": "lookback", "choices": [200]}, {"name": "band", "choices": [0.05]}],
                   dataset="raw/kucoin/futures/k 1d 2018-01-01..2026-10-07")
    spec = parse_spec(h.dataset)
    closes = [regimes(N, seed=40 + i) for i in range(len(h.symbols))]
    free = search.run_search(h, "trend-band", spec, [perp(s, c, 0.0) for s, c in zip(h.symbols, closes)],
                             prior_trials=0, now=NOW)
    rate = 0.0002  # per 8h: ~21.9% a year
    paid = search.run_search(h, "trend-band", spec, [perp(s, c, rate) for s, c in zip(h.symbols, closes)],
                             prior_trials=0, now=NOW)
    # the selection and the baseline (holding the perpetual) both pay it
    assert paid.holdout_score < free.holdout_score
    assert paid.baseline_holdout_score < free.baseline_holdout_score
    f = paid.funding
    assert f["applied"] is True
    assert f["mean_rate_per_year"] == {s: pytest.approx(rate * 3 * 365, rel=0.01) for s in h.symbols}
    assert f["selection_paid_per_year_in_market"] == pytest.approx(rate * 3 * 365, rel=0.01)
    assert f["holdout_paid_per_year_in_market"] == pytest.approx(rate * 3 * 365, rel=0.01)
    # with funding at zero the numbers are a spot search's
    spot = search.run_search(h, "trend-band", parse_spec("raw/binance/spot/s 1d 2018-01-01..2026-10-07"),
                             [bars(s, c) for s, c in zip(h.symbols, closes)], prior_trials=0, now=NOW)
    assert free.holdout_score == pytest.approx(spot.holdout_score)
    assert spot.funding is None and free.funding["selection_paid_per_year_in_market"] == 0.0


def test_the_holdout_pool_differs_by_exactly_the_funding_held(hypothesis):
    h = hypothesis(symbols=["AAAUSDT"], params=[{"name": "lookback", "choices": [200]}, {"name": "band", "choices": [0.05]}],
                   dataset="raw/kucoin/futures/k 1d 2018-01-01..2026-10-07")
    spec = parse_spec(h.dataset)
    c = regimes(N, seed=77)
    free = search.run_search(h, "trend-band", spec, [perp("AAAUSDT", c, 0.0)], prior_trials=0, now=NOW)
    paid = search.run_search(h, "trend-band", spec, [perp("AAAUSDT", c, 0.0001)], prior_trials=0, now=NOW)
    # funding moves no position (it doesn't feed the rule): each held bar pays 3 x 0.0001
    diff = np.asarray(free.best.holdout) - np.asarray(paid.best.holdout)
    assert set(np.round(diff, 12)) <= {0.0, 0.0003}
    assert (diff > 0).any()
