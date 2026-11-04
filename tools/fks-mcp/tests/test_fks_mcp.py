import asyncio
import json
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fks_mcp.client import FKS, Config, FKSError, family_for  # noqa: E402
from fks_mcp.server import build  # noqa: E402

LADDER = {
    "edges": [{"edge_id": "orb-cme", "display_name": "ORB", "edge_type": "rule", "status": "research"}],
    "hypotheses": [
        {"id": 1, "edge_id": "orb-cme", "idea": "orb", "status": "registered",
         "dataset": "raw/cme/databento+raw/cme/massive/cme 1m 2014-09-02..2024-09-06"},
        {"id": 2, "edge_id": "trend", "idea": "band", "status": "registered", "dataset": "raw/kraken/btc 1d 2020-01-01..2025-01-01"},
        {"id": 3, "edge_id": "trend", "idea": "old", "status": "withdrawn", "dataset": "raw/kraken/btc 1d"},
    ],
    "experiments": [{"id": 7, "hypothesis_id": 1, "verdict": "DO NOT PASTE", "verdict_reasons": ["U5"],
                     "holdout_score": 0.84, "selection_score": -0.8, "n_trials": 50, "holdout_trades": 300,
                     "selected_params": {"orb_minutes": 60}, "finished_at": "2026-11-03"}],
    "deployments": [],
    "effort": [],
    "health": {},
}


class Fake:
    def __init__(self):
        self.calls = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content) if req.content else None
        self.calls.append((req.method, req.url.path, body, req.headers.get("authorization")))
        p = req.url.path
        if p.endswith("/spawner/ladder"):
            return httpx.Response(200, json=LADDER)
        if p.endswith("/spawner/instances") and req.method == "GET":
            return httpx.Response(200, json={"instances": [
                {"name": "edge-search-h1", "template": "job.edge-search", "data_dir": "/srv/fks-lake/jobs/edge-search-h1",
                 "container": {"id": "abc123", "state": "exited", "status": "Exited (0)"}},
                {"name": "btc-recorder", "template": "job.recorder", "data_dir": "/srv/fks-lake/raw/kraken/btc", "params": {}}]})
        if p.endswith("/spawner/instances") and req.method == "POST":
            if body["params"]["hypothesis_id"] == 99:
                return httpx.Response(409, json={"errors": ["instance exists"]})
            return httpx.Response(201, json={"ok": True})
        if p.endswith("/spawner/container/abc123/logs"):
            return httpx.Response(200, text="event: log\ndata: line one\n\nevent: log\ndata: verdict DO NOT PASTE\n\n",
                                  headers={"content-type": "text/event-stream"})
        if p.endswith("/spawner/edges"):
            return httpx.Response(400, json={"error": "unknown edge_type 'janus' (supported: adaptive, rule)"})
        if p.endswith("/hypotheses"):
            return httpx.Response(201, json={"hypothesis": {"id": 4, **body}})
        return httpx.Response(404, json={"error": "not found"})


@pytest.fixture
def fake():
    return Fake()


@pytest.fixture
def fks(fake):
    return FKS(Config("https://x/api/v1", "fks_pat_test"), transport=httpx.MockTransport(fake))


def test_token_must_be_a_pat():
    with pytest.raises(FKSError):
        Config.from_env({"FKS_TOKEN": "session-cookie"})
    assert Config.from_env({"FKS_TOKEN": "fks_pat_a"}).url.endswith("/api/v1")


def test_family_inferred_from_dataset():
    assert family_for("raw/cme/databento+raw/cme/massive/cme 1m 2014..2024") == "orb"
    assert family_for("raw/kraken/btc 1d") == "trend-band"


def test_start_search_posts_the_instance(fks, fake):
    out = fks.start_search(1)
    assert out["family"] == "orb"
    method, path, body, auth = fake.calls[-1]
    assert (method, path) == ("POST", "/api/v1/spawner/instances")
    assert body == {"template": "job.edge-search", "name": "edge-search-h1",
                    "params": {"hypothesis_id": 1, "family": "orb"}}
    assert auth == "Bearer fks_pat_test"


def test_withdrawn_hypothesis_is_not_searched(fks, fake):
    with pytest.raises(FKSError, match="withdrawn"):
        fks.start_search(3)
    assert all(c[0] == "GET" for c in fake.calls)


def test_server_errors_come_back_in_its_words(fks):
    with pytest.raises(FKSError, match="supported: adaptive, rule"):
        fks.create_edge("x", "X", "janus", [])


def test_search_status_reads_the_log_tail(fks):
    s = fks.search_status(1)
    assert s["state"] == "exited"
    assert s["log_tail"] == ["line one", "verdict DO NOT PASTE"]
    assert s["experiments"][0]["verdict"] == "DO NOT PASTE"


def test_datasets_lists_recorded_folders(fks):
    d = fks.datasets()
    assert [r["path"] for r in d["recorded_by_instances"]] == ["raw/kraken/btc"]


def test_tools_are_only_the_safe_ones(fks):
    server = build(fks)
    names = {t.name for t in asyncio.run(server.list_tools())}
    assert names == {"ladder", "hypothesis_get", "experiment_get", "datasets", "alerts",
                     "create_edge", "register_hypothesis", "start_search", "search_status"}


def test_tool_call_returns_refusals_as_text(fks):
    server = build(fks)
    res = asyncio.run(server.call_tool("create_edge", {"edge_id": "x", "name": "X", "edge_type": "janus", "assets": []}))
    text = json.dumps(res, default=lambda o: getattr(o, "__dict__", str(o)))
    assert "FKS refused" in text and "adaptive" in text


def test_register_hypothesis_tool(fks, fake):
    server = build(fks)
    asyncio.run(server.call_tool("register_hypothesis", {"edge_id": "orb-cme", "hypothesis": {"idea": "x"}}))
    assert fake.calls[-1][1] == "/api/v1/spawner/edges/orb-cme/hypotheses"
