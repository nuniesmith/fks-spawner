import asyncio

from aiohttp.test_utils import TestClient, TestServer

from history import core, server


def test_metrics_carry_files_by_state(make_cfg, archive, cycle):
    cfg = make_cfg(HISTORY_INTERVALS="1d", HISTORY_START="2026-09")
    a = archive
    a.publish("https://archive.test/data/spot/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2026-09.zip", b"x")
    status = cycle(cfg, a)
    text = server.render_metrics(status)
    assert 'fks_history_files{dataset="klines",symbol="BTCUSDT",interval="1d",state="present"} 1' in text
    assert 'fks_history_files{dataset="klines",symbol="BTCUSDT",interval="1d",state="pending"} 35' in text
    assert 'fks_history_bytes{dataset="klines",symbol="BTCUSDT",interval="1d"} 1' in text
    assert "fks_history_last_success_timestamp_seconds" in text


def test_health_follows_the_heartbeat(make_cfg):
    cfg = make_cfg()
    status = core.Status(cfg)

    async def go():
        app = server.make_app(status)
        async with TestClient(TestServer(app)) as client:
            ok = await client.get("/health")
            status.updated_at_ms -= server.HEALTH_STALE_MS + 1
            stale = await client.get("/health")
            body = await (await client.get("/status")).json()
            return ok.status, stale.status, body

    ok, stale, body = asyncio.run(go())
    assert (ok, stale) == (200, 503)
    assert body["kind"] == "history" and body["market"] == "spot"
