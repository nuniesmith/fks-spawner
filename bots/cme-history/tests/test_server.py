import json
import urllib.request

from cme_history import core, server


def test_the_9091_contract(cfg):
    status = core.Status(cfg)
    status.roots["ES"] = {"last_day": "2026-09-17", "contract": "ESZ6", "error": None}
    status.last_success_ms = 1_790_000_000_000
    text = server.render_metrics(status)
    assert 'fks_cme_history_last_day_timestamp_seconds{root="ES"} 1789603200' in text
    assert "fks_cme_history_up 1" in text
    httpd = server.serve(status, 0)
    port = httpd.server_address[1]
    try:
        health = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/health").read())
        assert health["ok"] is True
        st = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/status").read())
        assert st["roots"]["ES"]["contract"] == "ESZ6"
    finally:
        httpd.shutdown()
