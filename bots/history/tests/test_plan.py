from datetime import date

from history import plan

TODAY = date(2026, 10, 27)


def test_months_and_days():
    assert plan.months(date(2026, 8, 1), TODAY) == [date(2026, 8, 1), date(2026, 9, 1)]
    assert plan.months(date(2025, 12, 1), date(2026, 2, 3)) == [date(2025, 12, 1), date(2026, 1, 1)]
    assert plan.days(date(2026, 10, 25), date(2026, 10, 26)) == [date(2026, 10, 25), date(2026, 10, 26)]
    assert plan.days(date(2026, 10, 27), date(2026, 10, 26)) == []


def test_klines_are_complete_months_plus_a_daily_window(make_cfg):
    cfg = make_cfg(HISTORY_INTERVALS="1d")
    ts = plan.targets(cfg, TODAY)
    monthly = [t for t in ts if t.period == "monthly"]
    daily = [t for t in ts if t.period == "daily"]
    assert [t.stamp for t in monthly] == ["2026-08", "2026-09"]
    # 35 days back from today, up to yesterday (the archive lags a day)
    assert daily[0].stamp == "2026-09-22" and daily[-1].stamp == "2026-10-26"
    assert len(daily) == 35
    t = monthly[0]
    assert t.filename == "BTCUSDT-1d-2026-08.zip"
    assert t.url(cfg) == "https://archive.test/data/spot/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2026-08.zip"
    assert t.path(cfg) == cfg.data_dir / "BTCUSDT" / "klines" / "1d" / "BTCUSDT-1d-2026-08.zip"


def test_futures_funding_is_monthly_and_metrics_daily_from_the_start(make_cfg):
    cfg = make_cfg(HISTORY_MARKET="usdm", HISTORY_DATASETS="fundingRate,metrics", HISTORY_START="2026-10")
    ts = plan.targets(cfg, TODAY)
    funding = [t for t in ts if t.dataset == "fundingRate"]
    metrics = [t for t in ts if t.dataset == "metrics"]
    assert funding == []  # October is not complete yet
    assert [t.stamp for t in metrics][:2] == ["2026-10-01", "2026-10-02"]
    assert len(metrics) == 26
    m = metrics[0]
    assert m.url(cfg) == "https://archive.test/data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-10-01.zip"
    assert m.path(cfg) == cfg.data_dir / "BTCUSDT" / "metrics" / "BTCUSDT-metrics-2026-10-01.zip"


def test_a_period_ends_on_its_last_day():
    assert plan.Target("klines", "BTCUSDT", "1d", "monthly", date(2026, 2, 1)).last_day == date(2026, 2, 28)
    assert plan.Target("klines", "BTCUSDT", "1d", "monthly", date(2025, 12, 1)).last_day == date(2025, 12, 31)
    assert plan.Target("metrics", "BTCUSDT", None, "daily", date(2026, 10, 3)).last_day == date(2026, 10, 3)
