"""Records raw public market data from a venue's websocket streams and REST
snapshots, append-only, one file per symbol, stream and UTC day.

Ported from the btc repo's recorder (BTCUSDT on oryx since 2026-10-21) and
generalized: symbols, streams and REST snapshots come from the environment
(config.py), files go under DATA_DIR, and `:9091` serves /health, /metrics
and /status (server.py). The two Binance traps it was proven against stay:

  - The gateway answers the websocket upgrade with `Connection: close` in
    its 101 response, which strict clients reject. websocket-client is
    pinned and ONE narrow patch drops its `connection` header check; status
    101 and Sec-WebSocket-Accept are still verified, and the patch refuses
    to run if the library's internals changed (apply_handshake_patch).
  - Futures streams are split across /market and /public base paths, and
    the legacy combined base delivers nothing for market streams, so each
    base path gets its own connection (config.BINANCE_USDM.ws_bases).

Files, under DATA_DIR:
  <SYMBOL>/<stream>/YYYY-MM-DD.jsonl   {"recv_ts_ms", "stream", "data"} per
                                       message: the raw frame plus our receive
                                       time; gzipped once the UTC day closes
  <SYMBOL>/snapshots/YYYY-MM-DD.jsonl  REST snapshots, same envelope
  _gaps/YYYY-MM-DD.jsonl               {"group", "start_ms", "end_ms",
                                       "reason"} for every stretch a
                                       connection recorded nothing: missing
                                       data is never mistaken for a quiet
                                       market
  _state/status.json                   the heartbeat /status serves
"""

from __future__ import annotations

import asyncio
import csv
import gzip
import io
import json
import logging
import random
import shutil
import signal
import threading
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import websocket
import websocket._handshake as _ws_handshake

from .config import Config, stream_dir

log = logging.getLogger("recorder")

# A connection that delivers nothing on any of its streams for this long is
# a zombie and gets recycled (websocket ping/pong is the primary dead-peer
# check; this is the backstop). One stream's silence is never acted on:
# forceOrder prints only when liquidations happen and can be quiet for
# hours, so recycling on it would punch gaps in the healthy streams. It is
# logged (first after 10 minutes, then hourly) and visible in /status.
CONNECTION_SILENCE = 300
SILENCE_WARN_FIRST = 600
SILENCE_WARN_REPEAT = 3600
# The heartbeat is written at least this often, so the next start knows when
# this run was last alive.
HEARTBEAT_SECONDS = 10
# How long a drain waits for a message before checking its connection's
# health; also how soon it notices it should stop.
POLL_SECONDS = 1.0


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def apply_handshake_patch() -> None:
    """Drop websocket-client's `Connection: upgrade` response check (see the
    module docstring). Refuses to run if the pinned library changed shape."""
    expected = {"upgrade": "websocket", "connection": "upgrade"}
    current = getattr(_ws_handshake, "_HEADERS_TO_CHECK", None)
    if current == {"upgrade": "websocket"}:
        return  # already applied in this process
    if current != expected:
        raise RuntimeError(
            f"websocket-client _HEADERS_TO_CHECK is {current!r}, expected "
            f"{expected!r}; refusing to run with an unverified handshake patch"
        )
    _ws_handshake._HEADERS_TO_CHECK = {"upgrade": "websocket"}
    log.info("handshake patch applied: websocket-client Connection check relaxed")


def gzip_file(path: Path) -> None:
    gz_path = path.with_suffix(path.suffix + ".gz")
    with open(path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out, length=1024 * 1024)
    path.unlink()
    log.info("gzipped %s", gz_path)


class DayWriter:
    """Appends JSONL lines to <dir>/YYYY-MM-DD.jsonl, rotating at 00:00 UTC
    and gzipping the closed day.

    `file_day` writes a record into that day's file instead (the open-interest
    backfill does), never disturbing the open append handle.
    """

    def __init__(self, directory: Path):
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)
        self.day: str | None = None
        self.path: Path | None = None
        self.fh = None
        self.buffer: list[str] = []
        self.lines_written = 0
        self.bytes_written = 0

    def _open(self, day: str) -> None:
        self.day = day
        self.path = self.dir / f"{day}.jsonl"
        self.fh = open(self.path, "a", encoding="utf-8")
        log.info("opened %s", self.path)

    def write(self, line: str, ts_ms: int, file_day: str | None = None) -> None:
        self.lines_written += 1
        self.bytes_written += len(line) + 1
        if file_day is not None and file_day != utc_day(ts_ms):
            with open(self.dir / f"{file_day}.jsonl", "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
            return
        day = utc_day(ts_ms)
        if day != self.day:
            self._rotate(day)
        self.buffer.append(line)
        if len(self.buffer) >= 500:
            self.flush()

    def _rotate(self, new_day: str) -> None:
        old_path = self.path
        self.flush()
        if self.fh is not None:
            self.fh.close()
            self.fh = None
        self._open(new_day)
        if old_path is not None and old_path.exists():
            asyncio.get_running_loop().run_in_executor(None, gzip_file, old_path)

    def flush(self) -> None:
        if self.buffer and self.fh is not None:
            self.fh.write("\n".join(self.buffer) + "\n")
            self.fh.flush()
            self.buffer.clear()

    def close(self) -> None:
        self.flush()
        if self.fh is not None:
            self.fh.close()
            self.fh = None


class Status:
    """What /status and /metrics report, persisted to _state/status.json.
    Mutated only on the event loop."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.started_at_ms = now_ms()
        subs = cfg.subscriptions()
        self.groups: dict[str, dict] = {
            group: {
                "connected": False,
                "last_connect_ts_ms": None,
                "last_disconnect_ts_ms": None,
                "disconnects": 0,
                "gap_ms_total": 0,
            }
            for group in subs
        }
        self.streams: dict[str, dict] = {}
        for group, names in subs.items():
            for name in names:
                symbol, stream = name.split("@", 1)
                self.streams[name] = {
                    "symbol": symbol.upper(),
                    "stream": stream,
                    "group": group,
                    "count": 0,
                    "bytes": 0,
                    "last_recv_ts_ms": None,
                }
        self.rest: dict[str, dict] = {
            symbol: {"count": 0, "errors": 0, "last_poll_ts_ms": None, "last_error": None}
            for symbol in cfg.symbols
        } if cfg.rest else {}
        self.oi_backfill: dict[str, dict] = {
            symbol: {"days_done": [], "last_run_ts_ms": None, "last_error": None}
            for symbol in cfg.symbols
        } if cfg.backfill_oi else {}
        self.updated_at_ms = self.started_at_ms
        self.dirty = True

    def mark_dirty(self) -> None:
        self.dirty = True

    def snapshot(self) -> dict:
        return {
            "bot_id": self.cfg.bot_id,
            "venue": self.cfg.venue.name,
            "symbols": list(self.cfg.symbols),
            "started_at_ms": self.started_at_ms,
            "updated_at_ms": self.updated_at_ms,
            "groups": self.groups,
            "streams": self.streams,
            "rest_snapshots": self.rest,
            "oi_backfill": self.oi_backfill,
        }

    @property
    def path(self) -> Path:
        return self.cfg.data_dir / "_state" / "status.json"

    def write(self) -> None:
        self.updated_at_ms = now_ms()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.snapshot(), indent=1), encoding="utf-8")
        tmp.replace(self.path)
        self.dirty = False

    async def flusher(self, stop: asyncio.Event) -> None:
        last = 0.0
        while not stop.is_set():
            if self.dirty or time.monotonic() - last >= HEARTBEAT_SECONDS:
                self.write()
                last = time.monotonic()
            try:
                await asyncio.wait_for(stop.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass


def previous_heartbeat_ms(status_path: Path) -> int | None:
    """When the previous run last wrote its heartbeat, if there was one."""
    try:
        return int(json.loads(status_path.read_text(encoding="utf-8"))["updated_at_ms"])
    except (OSError, ValueError, KeyError, TypeError):
        return None


class GapLedger:
    """Appends one line per stretch a connection group recorded nothing:
    a disconnect, a stalled connection, the recorder being down, or the
    seconds it takes to connect at startup. Gaps are rare, so each line is
    written and flushed at once."""

    def __init__(self, data_dir: Path, status: Status):
        self.dir = data_dir / "_gaps"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.status = status

    def record(self, group: str, start_ms: int, end_ms: int, reason: str) -> None:
        if end_ms <= start_ms:
            return
        line = json.dumps(
            {"group": group, "start_ms": start_ms, "end_ms": end_ms, "reason": reason},
            separators=(",", ":"),
        )
        with open(self.dir / f"{utc_day(start_ms)}.jsonl", "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        if group in self.status.groups:
            self.status.groups[group]["gap_ms_total"] += end_ms - start_ms
            self.status.mark_dirty()
        log.info("gap %s %s: %.1fs", group, reason, (end_ms - start_ms) / 1000)


# ---------------------------------------------------------------------------
# Websocket: one combined connection per venue base path, each in its own
# blocking thread (websocket-client), bridged to the event loop by a queue.
# Queue items: ("msg", recv_ts_ms, raw_text) | ("event", kind, ts_ms)
# ---------------------------------------------------------------------------


class StreamBridge:
    def __init__(self, loop: asyncio.AbstractEventLoop, group: str, base_url: str, streams):
        self.loop = loop
        self.group = group
        self.base_url = base_url
        self.streams = list(streams)
        self.queue: asyncio.Queue = asyncio.Queue()
        self.stop_flag = threading.Event()
        self.app: websocket.WebSocketApp | None = None

    def push(self, item) -> None:
        self.loop.call_soon_threadsafe(self.queue.put_nowait, item)

    def request_close(self) -> None:
        app = self.app
        if app is not None:
            try:
                app.close()
            except Exception:
                pass


def _proxy_kwargs(proxy: str | None) -> dict:
    if not proxy:
        return {}
    p = urlparse(proxy)
    kwargs: dict = {"proxy_type": "http"}
    if p.hostname:
        kwargs["http_proxy_host"] = p.hostname
    if p.port:
        kwargs["http_proxy_port"] = p.port
    if p.username:
        kwargs["http_proxy_auth"] = (p.username, p.password or "")
    return kwargs


def ws_thread(bridge: StreamBridge, proxy: str | None) -> None:
    url = f"{bridge.base_url}?streams={'/'.join(bridge.streams)}"
    proxy_kwargs = _proxy_kwargs(proxy)
    backoff = 1.0
    while not bridge.stop_flag.is_set():

        def on_open(_app):
            bridge.push(("event", "connected", now_ms()))

        def on_message(_app, message):
            bridge.push(("msg", now_ms(), message))

        def on_error(_app, error):
            log.warning("%s ws error: %r", bridge.group, error)

        def on_close(_app, code, reason):
            bridge.push(("event", "disconnected", now_ms()))
            log.info("%s ws closed (code=%s reason=%s)", bridge.group, code, reason)

        app = websocket.WebSocketApp(
            url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close
        )
        bridge.app = app
        log.info("connecting %s (%d streams)", bridge.group, len(bridge.streams))
        try:
            app.run_forever(ping_interval=60, ping_timeout=20, **proxy_kwargs)
        except Exception as exc:
            log.warning("%s run_forever raised: %r", bridge.group, exc)
            bridge.push(("event", "disconnected", now_ms()))
        bridge.app = None
        if bridge.stop_flag.is_set():
            break
        delay = min(backoff, 60.0) * random.uniform(0.8, 1.2)
        log.info("%s reconnecting in %.1fs", bridge.group, delay)
        bridge.stop_flag.wait(timeout=delay)
        backoff = min(backoff * 2, 60.0)


class GroupState:
    """The gap bookkeeping for one connection group: when its current gap
    began (None while it records) and why."""

    def __init__(self, gap_start_ms: int, reason: str):
        self.gap_start_ms: int | None = gap_start_ms
        self.reason = reason
        self.connected = False


async def drain_task(
    bridge: StreamBridge,
    writers: dict[str, DayWriter],
    status: Status,
    gaps: GapLedger,
    group_state: GroupState,
    stop: asyncio.Event,
) -> None:
    """Consumes one connection's queue on the event loop: writes each message
    to its stream's file, keeps that connection's status, records its gaps,
    and recycles it when every one of its streams has gone silent."""
    last_msg_at = {s: time.monotonic() for s in bridge.streams}
    last_any_msg_at = time.monotonic()
    last_any_msg_ms = now_ms()
    next_silence_warn = {s: time.monotonic() + SILENCE_WARN_FIRST for s in bridge.streams}
    group = status.groups[bridge.group]
    while True:
        # Once stopping, write what's already queued, then leave: no health
        # checks, so shutting down never "recycles" a healthy connection.
        if stop.is_set() and bridge.queue.empty():
            break
        try:
            item = await asyncio.wait_for(bridge.queue.get(), timeout=POLL_SECONDS)
        except asyncio.TimeoutError:
            item = None
        if item is not None:
            if item[0] == "event":
                _, kind, ts_ms = item
                up = kind == "connected"
                if up != group_state.connected:
                    group_state.connected = up
                    group["connected"] = up
                    if up:
                        group["last_connect_ts_ms"] = ts_ms
                        if group_state.gap_start_ms is not None:
                            gaps.record(bridge.group, group_state.gap_start_ms, ts_ms, group_state.reason)
                            group_state.gap_start_ms = None
                        for name in bridge.streams:
                            last_msg_at[name] = time.monotonic()
                        last_any_msg_at = time.monotonic()
                        last_any_msg_ms = ts_ms
                    else:
                        group["disconnects"] += 1
                        group["last_disconnect_ts_ms"] = ts_ms
                        if group_state.gap_start_ms is None:  # a stall already started it
                            group_state.gap_start_ms = ts_ms
                            group_state.reason = "disconnect"
                    log.info("%s %s", bridge.group, kind)
                    status.mark_dirty()
            else:
                _, recv_ms, raw = item
                try:
                    wrapper = json.loads(raw)
                    stream = wrapper["stream"]
                    data = wrapper["data"]
                except (ValueError, TypeError, KeyError):
                    log.warning("undecodable combined message on %s, dropped", bridge.group)
                    continue
                writer = writers.get(stream)
                if writer is None:
                    log.warning("message for unconfigured stream %r, dropped", stream)
                    continue
                line = json.dumps(
                    {"recv_ts_ms": recv_ms, "stream": stream, "data": data},
                    separators=(",", ":"),
                )
                writer.write(line, recv_ms)
                state = status.streams[stream]
                state["count"] += 1
                state["bytes"] += len(line) + 1
                state["last_recv_ts_ms"] = recv_ms
                last_msg_at[stream] = time.monotonic()
                last_any_msg_at = last_msg_at[stream]
                last_any_msg_ms = recv_ms
                if state["count"] % 50 == 0:
                    status.mark_dirty()
        if stop.is_set() or not group_state.connected:
            continue
        now = time.monotonic()
        if now - last_any_msg_at > CONNECTION_SILENCE:
            log.warning(
                "no data on any %s stream for >%ss; recycling the connection",
                bridge.group,
                CONNECTION_SILENCE,
            )
            if group_state.gap_start_ms is None:
                group_state.gap_start_ms = last_any_msg_ms
                group_state.reason = "stall"
            bridge.request_close()
            last_any_msg_at = now
        else:
            for name in bridge.streams:
                silent_for = now - last_msg_at[name]
                if silent_for > SILENCE_WARN_FIRST and now >= next_silence_warn[name]:
                    log.warning(
                        "%s has delivered nothing for %.0f min on this connection "
                        "(visible in /status; not recycling: other streams are healthy)",
                        name,
                        silent_for / 60,
                    )
                    next_silence_warn[name] = now + SILENCE_WARN_REPEAT


# ---------------------------------------------------------------------------
# REST snapshots and the Binance Vision open-interest backfill (aiohttp)
# ---------------------------------------------------------------------------


async def rest_task(
    cfg: Config,
    session: aiohttp.ClientSession,
    writers: dict[str, DayWriter],
    status: Status,
    stop: asyncio.Event,
) -> None:
    """Every RECORDER_REST_SECONDS (first at startup), one record per symbol
    holding each requested snapshot. A failed poll is logged and counted;
    nothing is written for it."""
    while not stop.is_set():
        for symbol in cfg.symbols:
            state = status.rest[symbol]
            try:
                data = {}
                for kind in cfg.rest:
                    path, key = cfg.venue.rest[kind]
                    async with session.get(
                        f"{cfg.venue.rest_base}{path}", params={"symbol": symbol}, proxy=cfg.proxy
                    ) as r:
                        r.raise_for_status()
                        data[key] = await r.json()
                recv_ms = now_ms()
                line = json.dumps(
                    {"recv_ts_ms": recv_ms, "stream": "rest:snapshots", "data": data},
                    separators=(",", ":"),
                )
                writers[symbol].write(line, recv_ms)
                state["count"] += 1
                state["last_poll_ts_ms"] = recv_ms
                state["last_error"] = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                state["errors"] += 1
                state["last_error"] = repr(exc)
                log.warning("REST snapshot for %s failed: %r", symbol, exc)
            status.mark_dirty()
        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg.rest_seconds)
        except asyncio.TimeoutError:
            pass


async def backfill_day(
    cfg: Config, session: aiohttp.ClientSession, writer: DayWriter, symbol: str, day: str
) -> int:
    """One day's Binance Vision metrics rows (5-minute open interest and
    more), appended to that day's snapshots file."""
    url = cfg.venue.vision_metrics.format(symbol=symbol, day=day)
    async with session.get(url, proxy=cfg.proxy) as r:
        r.raise_for_status()
        blob = await r.read()
    rows = 0
    recv_ms = now_ms()
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        with zf.open(zf.namelist()[0]) as raw:
            for row in csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8")):
                line = json.dumps(
                    {
                        "recv_ts_ms": recv_ms,
                        "stream": "rest:snapshots",
                        "data": {"source": "binance-vision-metrics", **row},
                    },
                    separators=(",", ":"),
                )
                writer.write(line, recv_ms, file_day=day)
                rows += 1
    return rows


async def backfill_task(
    cfg: Config,
    session: aiohttp.ClientSession,
    writers: dict[str, DayWriter],
    status: Status,
    stop: asyncio.Event,
) -> None:
    """Scan a week back for Vision metrics days not yet backfilled, then
    re-scan every 6 hours (files appear about a day late)."""
    done: dict[str, set[str]] = {}
    for symbol in cfg.symbols:
        path = writers[symbol].dir / ".oi-backfill-done.json"
        try:
            done[symbol] = set(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            done[symbol] = set()
        status.oi_backfill[symbol]["days_done"] = sorted(done[symbol])
    status.mark_dirty()
    while not stop.is_set():
        today = datetime.now(timezone.utc).date()
        for symbol in cfg.symbols:
            state = status.oi_backfill[symbol]
            for back in range(7, 0, -1):
                day = (today - timedelta(days=back)).strftime("%Y-%m-%d")
                if day in done[symbol] or stop.is_set():
                    continue
                try:
                    n = await backfill_day(cfg, session, writers[symbol], symbol, day)
                    done[symbol].add(day)
                    (writers[symbol].dir / ".oi-backfill-done.json").write_text(
                        json.dumps(sorted(done[symbol])), encoding="utf-8"
                    )
                    state["days_done"] = sorted(done[symbol])
                    state["last_error"] = None
                    log.info("OI backfill %s %s: %d rows", symbol, day, n)
                except Exception as exc:
                    state["last_error"] = f"{day}: {exc!r}"
                    log.info("OI backfill %s %s not available yet: %r", symbol, day, exc)
                state["last_run_ts_ms"] = now_ms()
                status.mark_dirty()
        try:
            await asyncio.wait_for(stop.wait(), timeout=6 * 3600)
        except asyncio.TimeoutError:
            pass


async def flush_loop(writers: list[DayWriter], stop: asyncio.Event) -> None:
    while not stop.is_set():
        for w in writers:
            w.flush()
        try:
            await asyncio.wait_for(stop.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass


class Recorder:
    """Everything one run holds: writers, status, gap ledger, connections."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        cfg.data_dir.mkdir(parents=True, exist_ok=True)
        self.status = Status(cfg)
        self.previous_heartbeat_ms = previous_heartbeat_ms(self.status.path)
        self.gaps = GapLedger(cfg.data_dir, self.status)
        self.stream_writers: dict[str, DayWriter] = {}
        for name, state in self.status.streams.items():
            directory = cfg.data_dir / state["symbol"] / stream_dir(state["stream"])
            self.stream_writers[name] = DayWriter(directory)
        self.snapshot_writers: dict[str, DayWriter] = (
            {s: DayWriter(cfg.data_dir / s / "snapshots") for s in cfg.symbols}
            if cfg.rest or cfg.backfill_oi
            else {}
        )
        # The gap each group is in until its first connect: since the
        # previous run's last heartbeat if there was one, else since now.
        start = self.status.started_at_ms
        prev = self.previous_heartbeat_ms
        self.group_states = {
            group: GroupState(prev, "recorder_down") if prev is not None and prev < start
            else GroupState(start, "startup")
            for group in self.status.groups
        }

    def all_writers(self) -> list[DayWriter]:
        return list(self.stream_writers.values()) + list(self.snapshot_writers.values())


async def run(cfg: Config, serve=None) -> None:
    """Record until SIGTERM/SIGINT. `serve(recorder, stop)` is the HTTP
    server coroutine (server.serve), passed in so tests can run without it."""
    apply_handshake_patch()
    rec = Recorder(cfg)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    log.info(
        "recorder %s starting: %s %s, streams %s, REST %s every %ss, data %s, proxy %s",
        cfg.bot_id, cfg.venue.name, ",".join(cfg.symbols), ",".join(cfg.streams) or "-",
        ",".join(cfg.rest) or "-", cfg.rest_seconds, cfg.data_dir, "set" if cfg.proxy else "none",
    )
    bridges = []
    for group, names in cfg.subscriptions().items():
        bridge = StreamBridge(loop, group, cfg.venue.ws_bases[group], names)
        threading.Thread(target=ws_thread, args=(bridge, cfg.proxy), name=f"ws-{group}", daemon=True).start()
        bridges.append(bridge)

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        tasks = [
            asyncio.create_task(
                drain_task(b, rec.stream_writers, rec.status, rec.gaps, rec.group_states[b.group], stop),
                name=f"drain-{b.group}",
            )
            for b in bridges
        ]
        if cfg.rest:
            tasks.append(asyncio.create_task(rest_task(cfg, session, rec.snapshot_writers, rec.status, stop), name="rest"))
        if cfg.backfill_oi:
            tasks.append(asyncio.create_task(backfill_task(cfg, session, rec.snapshot_writers, rec.status, stop), name="oi-backfill"))
        tasks.append(asyncio.create_task(flush_loop(rec.all_writers(), stop), name="flush"))
        tasks.append(asyncio.create_task(rec.status.flusher(stop), name="status"))
        if serve is not None:
            tasks.append(asyncio.create_task(serve(rec, stop), name="http"))

        await stop.wait()
        log.info("stopping: closing connections and files")
        for b in bridges:
            b.stop_flag.set()
            b.request_close()
        await asyncio.sleep(1.5)  # let the drains empty their queues
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    for w in rec.all_writers():
        w.close()
    rec.status.write()  # the last heartbeat: the next start's gap begins here
    log.info("recorder stopped cleanly")
