import pytest

from recorder import config


def test_defaults_record_everything_the_venue_offers(make_cfg):
    cfg = make_cfg()
    assert cfg.venue.name == "binance-usdm"
    assert cfg.symbols == ("BTCUSDT",)
    assert cfg.streams == ("forceOrder", "markPrice@1s", "bookTicker")
    assert cfg.rest == ("openInterest", "premiumIndex")
    assert cfg.rest_seconds == 300
    assert cfg.backfill_oi is False
    assert cfg.port == 9091


def test_streams_are_split_by_connection_group(make_cfg):
    cfg = make_cfg(RECORDER_SYMBOLS="btcusdt, ETHUSDT")
    assert cfg.symbols == ("BTCUSDT", "ETHUSDT")
    # the /market vs /public split the btc recorder found the hard way
    assert cfg.subscriptions() == {
        "market": [
            "btcusdt@forceOrder", "ethusdt@forceOrder",
            "btcusdt@markPrice@1s", "ethusdt@markPrice@1s",
        ],
        "public": ["btcusdt@bookTicker", "ethusdt@bookTicker"],
    }


def test_duplicates_and_blanks_are_dropped(make_cfg):
    cfg = make_cfg(RECORDER_SYMBOLS="BTCUSDT,,btcusdt , ETHUSDT", RECORDER_STREAMS="bookTicker,bookTicker")
    assert cfg.symbols == ("BTCUSDT", "ETHUSDT")
    assert cfg.streams == ("bookTicker",)


@pytest.mark.parametrize(
    "env, message",
    [
        ({"RECORDER_SYMBOLS": ""}, "RECORDER_SYMBOLS is required"),
        ({"RECORDER_SYMBOLS": "BTC-USDT"}, "aren't a venue symbol"),
        ({"RECORDER_VENUE": "kraken"}, "RECORDER_VENUE 'kraken'"),
        ({"RECORDER_STREAMS": "depth20@100ms"}, "doesn't offer: ['depth20@100ms']"),
        ({"RECORDER_REST": "fundingRate"}, "doesn't offer: ['fundingRate']"),
        ({"RECORDER_STREAMS": "", "RECORDER_REST": ""}, "nothing to record"),
        ({"RECORDER_REST_SECONDS": "30"}, "at least 60"),
        ({"RECORDER_REST_SECONDS": "5m"}, "whole number"),
        ({"RECORDER_PORT": "x"}, "port number"),
    ],
)
def test_bad_settings_say_what_is_wrong(make_cfg, env, message):
    with pytest.raises(config.ConfigError, match=message.replace("[", r"\[").replace("]", r"\]")):
        make_cfg(**env)


def test_an_empty_stream_list_means_rest_only(make_cfg):
    cfg = make_cfg(RECORDER_STREAMS="")
    assert cfg.streams == ()
    assert cfg.subscriptions() == {}


def test_stream_dir_drops_the_rate_suffix():
    assert config.stream_dir("markPrice@1s") == "markPrice"
    assert config.stream_dir("forceOrder") == "forceOrder"
