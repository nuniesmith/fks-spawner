import asyncio
import json
import time

from recorder import core


def wall_ms() -> int:
    return int(time.time() * 1000)


def msg(stream: str, data: dict, recv_ms: int | None = None):
    return ("msg", recv_ms or wall_ms(), json.dumps({"stream": stream, "data": data}))


def drive(rec, group, steps, settle=0.3):
    """Run one connection group's drain task over `steps`: queue items, or
    numbers (seconds to sleep). Returns how often it asked to recycle."""

    async def go():
        loop = asyncio.get_running_loop()
        bridge = core.StreamBridge(loop, group, "wss://unused", rec.cfg.subscriptions()[group])
        closes = []
        bridge.request_close = lambda: closes.append(wall_ms())
        stop = asyncio.Event()
        task = asyncio.create_task(
            core.drain_task(bridge, rec.stream_writers, rec.status, rec.gaps, rec.group_states[group], stop)
        )
        for step in steps:
            if isinstance(step, (int, float)):
                await asyncio.sleep(step)
            else:
                bridge.queue.put_nowait(step)
        await asyncio.sleep(settle)
        stop.set()
        await task
        for w in rec.all_writers():
            w.close()
        return closes

    return asyncio.run(go())


def gaps(rec):
    out = []
    for f in sorted((rec.cfg.data_dir / "_gaps").glob("*.jsonl")):
        out += [json.loads(line) for line in f.read_text().splitlines()]
    return out


def test_messages_land_in_their_stream_files_in_the_envelope(make_cfg):
    rec = core.Recorder(make_cfg())
    t = wall_ms()
    drive(rec, "market", [
        ("event", "connected", t),
        msg("btcusdt@forceOrder", {"o": {"p": "1"}}, t + 1),
        msg("btcusdt@markPrice@1s", {"p": "2"}, t + 2),
    ])
    day = core.utc_day(t)
    force = (rec.cfg.data_dir / "BTCUSDT" / "forceOrder" / f"{day}.jsonl").read_text().splitlines()
    assert [json.loads(x) for x in force] == [
        {"recv_ts_ms": t + 1, "stream": "btcusdt@forceOrder", "data": {"o": {"p": "1"}}}
    ]
    assert (rec.cfg.data_dir / "BTCUSDT" / "markPrice" / f"{day}.jsonl").exists()
    s = rec.status.streams["btcusdt@forceOrder"]
    assert s["count"] == 1 and s["last_recv_ts_ms"] == t + 1 and s["bytes"] == len(force[0]) + 1
    assert rec.status.groups["market"]["connected"] is True


def test_a_market_wide_stream_is_filed_under_ALL(make_cfg):
    rec = core.Recorder(make_cfg(RECORDER_SYMBOLS="", RECORDER_MARKET_STREAMS="forceOrder@arr", RECORDER_REST=""))
    t = wall_ms()
    liquidation = {"e": "forceOrder", "o": {"s": "ETHUSDT", "S": "SELL", "q": "1.5"}}
    drive(rec, "market", [("event", "connected", t), msg("!forceOrder@arr", liquidation, t + 1)])
    day = core.utc_day(t)
    lines = (rec.cfg.data_dir / "ALL" / "forceOrder" / f"{day}.jsonl").read_text().splitlines()
    assert [json.loads(x) for x in lines] == [
        {"recv_ts_ms": t + 1, "stream": "!forceOrder@arr", "data": liquidation}
    ]
    s = rec.status.streams["!forceOrder@arr"]
    assert (s["symbol"], s["stream"], s["count"]) == ("ALL", "forceOrder@arr", 1)
    # no symbols: no REST snapshots to write
    assert rec.snapshot_writers == {}


def test_unknown_and_undecodable_messages_are_dropped(make_cfg):
    rec = core.Recorder(make_cfg())
    drive(rec, "market", [
        ("event", "connected", wall_ms()),
        msg("ethusdt@forceOrder", {"x": 1}),  # not subscribed here
        ("msg", wall_ms(), "not json"),
    ])
    assert all(s["count"] == 0 for s in rec.status.streams.values())


def test_the_first_connect_closes_the_startup_gap(make_cfg):
    rec = core.Recorder(make_cfg())
    start = rec.status.started_at_ms
    drive(rec, "market", [("event", "connected", start + 1500)])
    assert gaps(rec) == [{"group": "market", "start_ms": start, "end_ms": start + 1500, "reason": "startup"}]


def test_downtime_since_the_previous_runs_heartbeat_is_a_gap(make_cfg):
    cfg = make_cfg()
    earlier = core.Recorder(cfg)
    earlier.status.write()
    last_alive = earlier.status.updated_at_ms
    time.sleep(0.05)
    rec = core.Recorder(cfg)  # the next run, same DATA_DIR
    connect = rec.status.started_at_ms + 2000
    drive(rec, "public", [("event", "connected", connect)])
    assert gaps(rec) == [{"group": "public", "start_ms": last_alive, "end_ms": connect, "reason": "recorder_down"}]


def test_a_disconnect_is_a_gap_until_the_reconnect(make_cfg):
    rec = core.Recorder(make_cfg())
    t = wall_ms()
    drive(rec, "public", [
        ("event", "connected", t),
        ("event", "disconnected", t + 10_000),
        ("event", "connected", t + 13_500),
    ])
    reasons = [(g["reason"], g["end_ms"] - g["start_ms"]) for g in gaps(rec) if g["reason"] != "startup"]
    assert reasons == [("disconnect", 3500)]
    group = rec.status.groups["public"]
    assert group["disconnects"] == 1
    assert group["gap_ms_total"] >= 3500


def test_a_stalled_connection_is_recycled_and_its_gap_starts_at_the_last_message(make_cfg, monkeypatch):
    monkeypatch.setattr(core, "CONNECTION_SILENCE", 0.2)
    monkeypatch.setattr(core, "POLL_SECONDS", 0.05)
    rec = core.Recorder(make_cfg())
    t = wall_ms()
    closes = drive(rec, "market", [
        ("event", "connected", t),
        msg("btcusdt@markPrice@1s", {"p": "1"}, t + 100),
        0.6,  # nothing more arrives: the connection goes stale
        ("event", "disconnected", t + 900),
        ("event", "connected", t + 1200),
    ], settle=0.1)
    assert closes, "the stalled connection should have been recycled"
    stall = [g for g in gaps(rec) if g["reason"] == "stall"]
    assert stall == [{"group": "market", "start_ms": t + 100, "end_ms": t + 1200, "reason": "stall"}]


def test_one_quiet_stream_is_neither_a_gap_nor_a_recycle(make_cfg, monkeypatch):
    monkeypatch.setattr(core, "CONNECTION_SILENCE", 0.5)
    monkeypatch.setattr(core, "POLL_SECONDS", 0.05)  # check often while it flows
    rec = core.Recorder(make_cfg())
    t = wall_ms()
    steps = [("event", "connected", t)]
    for i in range(8):  # markPrice keeps flowing; forceOrder says nothing
        steps += [msg("btcusdt@markPrice@1s", {"i": i}), 0.1]
    closes = drive(rec, "market", steps)
    assert closes == []
    # (a startup gap may be recorded or not: on a fast machine the first
    # connect lands in the same millisecond, and a zero-length gap isn't one)
    assert [g["reason"] for g in gaps(rec) if g["reason"] != "startup"] == []


def test_previous_heartbeat_ignores_a_missing_or_broken_file(tmp_path):
    assert core.previous_heartbeat_ms(tmp_path / "nope.json") is None
    bad = tmp_path / "status.json"
    bad.write_text("{not json")
    assert core.previous_heartbeat_ms(bad) is None


def test_a_healthy_connection_that_drops_reconnects_at_once():
    # 10-27 01:23 UTC: hours of healthy bookTicker, then a venue-side drop.
    # The backoff had grown from earlier drops; a healthy connection resets it.
    delay, nxt = core.reconnect_backoff(32.0, connected_for=2 * 3600)
    assert delay == core.RECONNECT_FIRST
    assert nxt == 2 * core.RECONNECT_FIRST


def test_repeated_failures_back_off_to_the_cap():
    backoff, delays = core.RECONNECT_FIRST, []
    for _ in range(8):  # the venue keeps refusing: never opens
        delay, backoff = core.reconnect_backoff(backoff, connected_for=None)
        delays.append(delay)
    assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]


def test_a_short_lived_connection_continues_the_backoff():
    # Opened, then dropped within seconds: still flapping, keep backing off.
    delay, nxt = core.reconnect_backoff(8.0, connected_for=5.0)
    assert (delay, nxt) == (8.0, 16.0)
