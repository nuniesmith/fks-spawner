"""The `:9091` contract every FKS workload serves (EDGE_FACTORY_PLAN §5.2):

  /health   200 while the heartbeat is fresh, else 503
  /metrics  Prometheus text: fks_recorder_* series (the spawner's file-SD
            makes Prometheus scrape it automatically)
  /status   the heartbeat as JSON (the same as _state/status.json)
"""

from __future__ import annotations

import asyncio
import math

from aiohttp import web

from .core import Recorder, now_ms

# /health fails when the heartbeat (written at least every 10 s) is older.
HEALTH_STALE_MS = 60_000


def _labels(**kv: str) -> str:
    def esc(v: str) -> str:
        return v.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')

    return "{" + ",".join(f'{k}="{esc(v)}"' for k, v in kv.items()) + "}"


def render_metrics(rec: Recorder, at_ms: int | None = None) -> str:
    at_ms = now_ms() if at_ms is None else at_ms
    st = rec.status
    out: list[str] = []

    def family(name: str, kind: str, help_text: str, samples: list[tuple[str, float]]) -> None:
        out.append(f"# HELP {name} {help_text}")
        out.append(f"# TYPE {name} {kind}")
        for labels, value in samples:
            if isinstance(value, float) and not math.isfinite(value):
                continue
            out.append(f"{name}{labels} {value}")

    def stream_labels(s: dict) -> str:
        return _labels(symbol=s["symbol"], stream=s["stream"])

    family("fks_recorder_up", "gauge", "1 while the recorder runs.", [("", 1)])
    family(
        "fks_recorder_uptime_seconds", "gauge", "Seconds since this run started.",
        [("", round((at_ms - st.started_at_ms) / 1000, 3))],
    )
    family(
        "fks_recorder_messages_total", "counter", "Messages recorded, by symbol and stream.",
        [(stream_labels(s), s["count"]) for s in st.streams.values()],
    )
    family(
        "fks_recorder_bytes_written_total", "counter", "Bytes written, by symbol and stream.",
        [(stream_labels(s), s["bytes"]) for s in st.streams.values()],
    )
    family(
        "fks_recorder_last_message_age_seconds", "gauge",
        "Seconds since the stream's last message; absent until the first one.",
        [
            (stream_labels(s), round((at_ms - s["last_recv_ts_ms"]) / 1000, 3))
            for s in st.streams.values()
            if s["last_recv_ts_ms"] is not None
        ],
    )
    family(
        "fks_recorder_connected", "gauge", "1 while the connection group is connected.",
        [(_labels(group=g), 1 if s["connected"] else 0) for g, s in st.groups.items()],
    )
    family(
        "fks_recorder_reconnects_total", "counter", "Disconnects, by connection group.",
        [(_labels(group=g), s["disconnects"]) for g, s in st.groups.items()],
    )
    family(
        "fks_recorder_gap_seconds_total", "counter",
        "Seconds recorded in the gap ledger, by connection group.",
        [(_labels(group=g), round(s["gap_ms_total"] / 1000, 3)) for g, s in st.groups.items()],
    )
    family(
        "fks_recorder_rest_snapshots_total", "counter", "REST snapshot polls that succeeded.",
        [(_labels(symbol=sym), s["count"]) for sym, s in st.rest.items()],
    )
    family(
        "fks_recorder_rest_errors_total", "counter", "REST snapshot polls that failed.",
        [(_labels(symbol=sym), s["errors"]) for sym, s in st.rest.items()],
    )
    return "\n".join(out) + "\n"


def make_app(rec: Recorder) -> web.Application:
    async def health(_request: web.Request) -> web.Response:
        age = now_ms() - rec.status.updated_at_ms
        ok = age <= HEALTH_STALE_MS
        return web.json_response(
            {"ok": ok, "bot_id": rec.cfg.bot_id, "heartbeat_age_ms": age},
            status=200 if ok else 503,
        )

    async def metrics(_request: web.Request) -> web.Response:
        return web.Response(text=render_metrics(rec), content_type="text/plain", charset="utf-8")

    async def status(_request: web.Request) -> web.Response:
        return web.json_response(rec.status.snapshot())

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/metrics", metrics)
    app.router.add_get("/status", status)
    return app


async def serve(rec: Recorder, stop: asyncio.Event) -> None:
    runner = web.AppRunner(make_app(rec), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", rec.cfg.port)
    await site.start()
    try:
        await stop.wait()
    finally:
        await runner.cleanup()
