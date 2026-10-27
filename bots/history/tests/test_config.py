from datetime import date

import pytest

from history import config


def test_defaults(make_cfg):
    cfg = make_cfg()
    assert (cfg.market, cfg.symbols, cfg.datasets, cfg.intervals) == ("spot", ("BTCUSDT",), ("klines",), ("1h", "1d"))
    assert cfg.start == date(2026, 8, 1)
    assert (cfg.poll_hours, cfg.concurrency, cfg.port) == (6.0, 4, 9091)
    assert cfg.market_path == "spot"


def test_futures_offer_funding_and_metrics(make_cfg):
    cfg = make_cfg(HISTORY_MARKET="usdm", HISTORY_DATASETS="klines,fundingRate,metrics", HISTORY_SYMBOLS="btcusdt, ETHUSDT")
    assert cfg.market_path == "futures/um"
    assert cfg.symbols == ("BTCUSDT", "ETHUSDT")
    assert cfg.datasets == ("klines", "fundingRate", "metrics")


def test_no_klines_means_no_intervals(make_cfg):
    assert make_cfg(HISTORY_MARKET="usdm", HISTORY_DATASETS="fundingRate").intervals == ()


@pytest.mark.parametrize(
    "env, message",
    [
        ({"HISTORY_MARKET": "um"}, "HISTORY_MARKET must be one of"),
        ({"HISTORY_SYMBOLS": ""}, "HISTORY_SYMBOLS is required"),
        ({"HISTORY_SYMBOLS": "BTC-USDT"}, "aren't a venue symbol"),
        ({"HISTORY_DATASETS": "fundingRate"}, "which spot doesn't publish"),
        ({"HISTORY_INTERVALS": "7m"}, "HISTORY_INTERVALS has ['7m']"),
        ({"HISTORY_START": "2016-12"}, "2017-01 or later"),
        ({"HISTORY_START": "2026-13"}, "must be a month"),
        ({"HISTORY_CONCURRENCY": "20"}, "between 1 and 8"),
        ({"HISTORY_POLL_HOURS": "0"}, "between 0.25 and 48"),
    ],
)
def test_bad_settings_say_what_is_wrong(make_cfg, env, message):
    with pytest.raises(config.ConfigError, match=message.replace("[", r"\[").replace("]", r"\]")):
        make_cfg(**env)
