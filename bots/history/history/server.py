"""The `:9091` contract every FKS workload serves:

  /health   200 while the heartbeat (written every 10 s) is fresh, else 503
  /metrics  Prometheus text: fks_history_* (the spawner's file-SD makes
            Prometheus scrape it automatically)
  /status   the heartbeat as JSON (the same as _state/status.json)
"""

from __future__ import annotations

import asyncio

from aiohttp import web

from .core import Status, now_ms

HEALTH_STALE_MS = 60_000

FILE_STATES = ("present", "pending", "not_published", "errors")


def _labels(**kv: str) -> str:
    def esc(v: str) -> str:
        return v.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')

    return "{" + ",".join(f'{k}="{esc(v)}"' for k, v in kv.items()) + "}"


def render_metrics(status: Status) -> str:
    out: list[str] = []

    def family(name: str, kind: str, help_text: str, samples: list[tuple[str, float]]) -> None:
        out.append(f"# HELP {name} {help_text}")
        out.append(f"# TYPE {name} {kind}")
        out.extend(f"{name}{labels} {value}" for labels, value in samples)

    def series_labels(s: dict, **extra: str) -> str:
        return _labels(dataset=s["dataset"], symbol=s["symbol"], interval=s["interval"] or "-", **extra)

    family("fks_history_up", "gauge", "1 while the history recorder runs.", [("", 1)])
    family(
        "fks_history_files", "gauge",
        "Archive files the lake should hold, by state: present (verified), pending "
        "(recent, not published yet), not_published (old 404), errors (retried next cycle).",
        [
            (series_labels(s, state=state), s[state])
            for s in status.series.values()
            for state in FILE_STATES
        ],
    )
    family(
        "fks_history_bytes", "gauge", "Bytes of verified archive files on disk.",
        [(series_labels(s), s["bytes"]) for s in status.series.values()],
    )
    family("fks_history_downloads_total", "counter", "Files downloaded and verified, this run.",
           [("", status.downloads_total)])
    family("fks_history_errors_total", "counter", "Download errors, this run.",
           [("", status.errors_total)])
    family("fks_history_cycle_running", "gauge", "1 while a check for new files runs.",
           [("", 1 if status.cycle["running"] else 0)])
    if status.last_success_ms is not None:
        family(
            "fks_history_last_success_timestamp_seconds", "gauge",
            "When the last check finished with no errors.",
            [("", round(status.last_success_ms / 1000, 3))],
        )
    return "\n".join(out) + "\n"


def make_app(status: Status) -> web.Application:
    async def health(_request: web.Request) -> web.Response:
        age = now_ms() - status.updated_at_ms
        ok = age <= HEALTH_STALE_MS
        return web.json_response(
            {"ok": ok, "bot_id": status.cfg.bot_id, "heartbeat_age_ms": age},
            status=200 if ok else 503,
        )

    async def metrics(_request: web.Request) -> web.Response:
        return web.Response(text=render_metrics(status), content_type="text/plain", charset="utf-8")

    async def status_handler(_request: web.Request) -> web.Response:
        return web.json_response(status.snapshot())

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/metrics", metrics)
    app.router.add_get("/status", status_handler)
    return app


async def serve(status: Status, stop: asyncio.Event) -> None:
    runner = web.AppRunner(make_app(status), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", status.cfg.port)
    await site.start()
    try:
        await stop.wait()
    finally:
        await runner.cleanup()
