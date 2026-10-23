import asyncio

from aiohttp.test_utils import TestClient, TestServer

from recorder import core, server


def call(rec, path):
    async def go():
        async with TestClient(TestServer(server.make_app(rec))) as client:
            r = await client.get(path)
            body = await (r.json() if path != "/metrics" else r.text())
            return r.status, body

    return asyncio.run(go())


def test_health_follows_the_heartbeat(make_cfg):
    rec = core.Recorder(make_cfg(FKS_BOT_ID="rec-1"))
    rec.status.write()
    status, body = call(rec, "/health")
    assert status == 200 and body["ok"] is True and body["bot_id"] == "rec-1"
    rec.status.updated_at_ms -= server.HEALTH_STALE_MS + 1000
    status, body = call(rec, "/health")
    assert status == 503 and body["ok"] is False


def test_status_serves_the_heartbeat(make_cfg):
    rec = core.Recorder(make_cfg(RECORDER_SYMBOLS="BTCUSDT,ETHUSDT"))
    status, body = call(rec, "/status")
    assert status == 200
    assert body["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert set(body["groups"]) == {"market", "public"}
    assert "ethusdt@bookTicker" in body["streams"]


def test_metrics_carry_the_recorder_series(make_cfg):
    rec = core.Recorder(make_cfg())
    status, text = call(rec, "/metrics")
    assert status == 200
    for line in [
        "fks_recorder_up 1",
        'fks_recorder_messages_total{symbol="BTCUSDT",stream="forceOrder"} 0',
        'fks_recorder_bytes_written_total{symbol="BTCUSDT",stream="bookTicker"} 0',
        'fks_recorder_connected{group="market"} 0',
        'fks_recorder_reconnects_total{group="public"} 0',
        'fks_recorder_gap_seconds_total{group="market"} 0.0',
        'fks_recorder_rest_snapshots_total{symbol="BTCUSDT"} 0',
        'fks_recorder_rest_errors_total{symbol="BTCUSDT"} 0',
    ]:
        assert line in text.splitlines(), line
    # no age until the stream has said something
    assert "fks_recorder_last_message_age_seconds{" not in text


def test_message_age_is_measured_from_the_last_message(make_cfg):
    rec = core.Recorder(make_cfg())
    s = rec.status.streams["btcusdt@bookTicker"]
    s["last_recv_ts_ms"] = 1_000_000
    text = server.render_metrics(rec, at_ms=1_002_500)
    assert 'fks_recorder_last_message_age_seconds{symbol="BTCUSDT",stream="bookTicker"} 2.5' in text.splitlines()
