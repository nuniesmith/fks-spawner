"""The `:9091` contract every FKS workload serves, while the search runs:

  /health   200 while running
  /metrics  fks_search_* (Prometheus scrapes it through the spawner's file-SD)
  /status   the progress and, once done, the verdict
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Status:
    def __init__(self, bot_id: str, hypothesis_id: int):
        self.lock = threading.Lock()
        self.doc = {
            "kind": "job",
            "job": "edge-search",
            "bot_id": bot_id,
            "hypothesis_id": hypothesis_id,
            "phase": "starting",
            "trials_done": 0,
            "trials_total": 0,
            "verdict": None,
            "error": None,
        }

    def update(self, **kv) -> None:
        with self.lock:
            self.doc.update(kv)

    def snapshot(self) -> dict:
        with self.lock:
            return dict(self.doc)


def render_metrics(doc: dict) -> str:
    lines = [
        "# HELP fks_search_trials_done Grid points scored so far.",
        "# TYPE fks_search_trials_done gauge",
        f"fks_search_trials_done {doc['trials_done']}",
        "# HELP fks_search_trials_total Grid points this search scores.",
        "# TYPE fks_search_trials_total gauge",
        f"fks_search_trials_total {doc['trials_total']}",
        "# HELP fks_search_up 1 while the search runs.",
        "# TYPE fks_search_up gauge",
        "fks_search_up 1",
    ]
    return "\n".join(lines) + "\n"


def serve(status: Status, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):  # quiet
            pass

        def do_GET(self):
            doc = status.snapshot()
            if self.path == "/health":
                body, ctype = json.dumps({"ok": True}).encode(), "application/json"
            elif self.path == "/metrics":
                body, ctype = render_metrics(doc).encode(), "text/plain; version=0.0.4"
            elif self.path == "/status":
                body, ctype = json.dumps(doc).encode(), "application/json"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("content-type", ctype)
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True, name="status").start()
    return server
