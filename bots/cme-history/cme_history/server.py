"""The `:9091` contract every FKS workload serves:

  /health   200 while the heartbeat (every 10 s) is fresh, else 503
  /metrics  Prometheus text: fks_cme_history_*
  /status   the recorder's state as JSON
"""

from __future__ import annotations

import json
import threading
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .core import Status, now_ms

HEALTH_STALE_MS = 60_000


def render_metrics(status: Status) -> str:
    out = [
        "# HELP fks_cme_history_up 1 while the CME history recorder runs.",
        "# TYPE fks_cme_history_up gauge",
        "fks_cme_history_up 1",
        "# HELP fks_cme_history_last_day_timestamp_seconds The last UTC day recorded, per root (its midnight).",
        "# TYPE fks_cme_history_last_day_timestamp_seconds gauge",
    ]
    for root, s in sorted(status.roots.items()):
        if s.get("last_day"):
            d = date.fromisoformat(s["last_day"])
            ts = datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()
            out.append(f'fks_cme_history_last_day_timestamp_seconds{{root="{root}"}} {ts:.0f}')
    out += [
        "# HELP fks_cme_history_errors_total Failed cycles for a root, this run.",
        "# TYPE fks_cme_history_errors_total counter",
        f"fks_cme_history_errors_total {status.errors_total}",
        "# HELP fks_cme_history_requests_total Requests made to Massive, this run.",
        "# TYPE fks_cme_history_requests_total counter",
        f"fks_cme_history_requests_total {status.requests}",
    ]
    if status.last_success_ms is not None:
        out += [
            "# HELP fks_cme_history_last_success_timestamp_seconds When the last cycle finished with no errors.",
            "# TYPE fks_cme_history_last_success_timestamp_seconds gauge",
            f"fks_cme_history_last_success_timestamp_seconds {status.last_success_ms / 1000:.3f}",
        ]
    return "\n".join(out) + "\n"


def make_handler(status: Status):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, body: bytes, kind: str) -> None:
            self.send_response(code)
            self.send_header("content-type", kind)
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                age = now_ms() - status.updated_at_ms
                ok = age <= HEALTH_STALE_MS
                body = json.dumps({"ok": ok, "bot_id": status.cfg.bot_id, "heartbeat_age_ms": age}).encode()
                self._send(200 if ok else 503, body, "application/json")
            elif self.path == "/metrics":
                self._send(200, render_metrics(status).encode(), "text/plain; charset=utf-8")
            elif self.path == "/status":
                self._send(200, json.dumps(status.snapshot()).encode(), "application/json")
            else:
                self._send(404, b'{"error":"not_found"}', "application/json")

        def log_message(self, *_args) -> None:
            pass

    return Handler


def serve(status: Status, port: int) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer(("0.0.0.0", port), make_handler(status))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd
