"""The FKS API v1 as an agent needs it: a few calls, plain answers.

Every call goes to fks-web's API (`/api/v1/...`) with a personal access
token, so the platform's own rules apply unchanged: the token's role
(operator at most: tokens are never admin), the route registry, the
database's ladder (frozen hypotheses, results that can't be rewritten,
no skipped rung). This module adds nothing those rules don't allow; it
only shapes the answers to be short.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any

import httpx

DEFAULT_URL = "https://oryx.tailfef10.ts.net/api/v1"

#: The lake folders a futures hypothesis can name (fks-web ladder.ts).
CME_FOLDERS = [
    {"path": "raw/cme/databento+raw/cme/massive/cme", "interval": "1m", "from": "2014-09-02",
     "to": "2024-09-06", "note": "Databento decade (every contract), then the live recorder. "
     "The orb family has been searched on it (experiment 1, rejected)."},
    {"path": "raw/cme/massive-cache+raw/cme/massive/cme", "interval": "1m", "from": "2024-09-09",
     "to": None, "note": "Massive cache, then the live recorder. Searched by earlier orb work, and "
     "2026-04-01..2026-09-11 is the futures program's SEALED holdout: don't train on it."},
]


class FKSError(RuntimeError):
    """The platform refused or failed; the message is its own words."""


@dataclass
class Config:
    url: str
    token: str

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        env = dict(os.environ) if env is None else env
        token = env.get("FKS_TOKEN", "").strip()
        if not token.startswith("fks_pat_"):
            raise FKSError("FKS_TOKEN must be a personal access token (fks_pat_…): Settings -> API tokens")
        return cls(env.get("FKS_URL", DEFAULT_URL).rstrip("/"), token)


class FKS:
    def __init__(self, cfg: Config, transport: httpx.BaseTransport | None = None, timeout: float = 30.0):
        self.cfg = cfg
        self.http = httpx.Client(base_url=cfg.url, transport=transport, timeout=timeout,
                                 headers={"Authorization": f"Bearer {cfg.token}",
                                          "User-Agent": "fks-mcp/1"})

    # ── HTTP ────────────────────────────────────────────────────────────────
    def _check(self, r: httpx.Response) -> Any:
        try:
            body = r.json()
        except ValueError:
            body = {"error": r.text[:300]}
        if r.status_code >= 400:
            if isinstance(body, dict):
                errs = body.get("errors") or [body.get("detail") or body.get("error") or body.get("message")]
                raise FKSError(f"HTTP {r.status_code}: " + "; ".join(str(e) for e in errs if e))
            raise FKSError(f"HTTP {r.status_code}: {body}")
        return body

    def get(self, path: str) -> Any:
        return self._check(self.http.get(path))

    def post(self, path: str, body: dict) -> Any:
        return self._check(self.http.post(path, json=body))

    # ── The ladder ──────────────────────────────────────────────────────────
    def ladder(self) -> dict:
        return self.get("/spawner/ladder")

    def ladder_summary(self) -> dict:
        b = self.ladder()
        searched = {e.get("hypothesis_id") for e in b.get("experiments", [])}
        return {
            "edges": [{"edge_id": e.get("edge_id"), "name": e.get("display_name"), "type": e.get("edge_type"),
                       "status": e.get("status")} for e in b.get("edges", [])],
            "hypotheses": [{"id": h["id"], "edge_id": h["edge_id"], "idea": h["idea"], "dataset": h["dataset"],
                            "status": h.get("status"), "searched": h["id"] in searched}
                           for h in b.get("hypotheses", [])],
            "experiments": [_experiment_brief(e) for e in b.get("experiments", [])],
            "deployments": [{"id": d["id"], "experiment_id": d["experiment_id"], "stage": d["stage"],
                             "symbol": d["symbol"], "account_id": d["account_id"],
                             "health": (b.get("health") or {}).get(str(d["id"]))}
                            for d in b.get("deployments", [])],
            "trials_per_family": [{"family": x.get("config_fingerprint"), "symbol": x.get("symbol"),
                                   "searches": x.get("experiments"), "trials": x.get("trials_total")}
                                  for x in b.get("effort", [])],
        }

    def hypothesis(self, hypothesis_id: int) -> dict:
        for h in self.ladder().get("hypotheses", []):
            if h["id"] == hypothesis_id:
                return h
        raise FKSError(f"no hypothesis {hypothesis_id}")

    def experiment(self, experiment_id: int) -> dict:
        for e in self.ladder().get("experiments", []):
            if e["id"] == experiment_id:
                return e
        raise FKSError(f"no experiment {experiment_id}")

    def create_edge(self, edge_id: str, name: str, edge_type: str, assets: list[str]) -> dict:
        return self.post("/spawner/edges", {"edge_id": edge_id, "display_name": name, "edge_type": edge_type,
                                            "asset_scope": assets, "status": "research"})

    def register_hypothesis(self, edge_id: str, hypothesis: dict) -> dict:
        res = self.post(f"/spawner/edges/{edge_id}/hypotheses", hypothesis)
        return res.get("hypothesis", res)

    # ── Searches ────────────────────────────────────────────────────────────
    def start_search(self, hypothesis_id: int, family: str | None = None) -> dict:
        h = self.hypothesis(hypothesis_id)
        if h.get("status") == "withdrawn":
            raise FKSError(f"hypothesis {hypothesis_id} is withdrawn")
        fam = family or family_for(h["dataset"])
        name = f"edge-search-h{hypothesis_id}"
        res = self.post("/spawner/instances", {"template": "job.edge-search", "name": name,
                                               "params": {"hypothesis_id": hypothesis_id, "family": fam}})
        return {"instance": name, "family": fam, "started": True, "spawner": res}

    def search_status(self, hypothesis_id: int, log_lines: int = 40) -> dict:
        name = f"edge-search-h{hypothesis_id}"
        inst = next((i for i in self.get("/spawner/instances").get("instances", []) if i.get("name") == name), None)
        exps = [_experiment_brief(e) for e in self.ladder().get("experiments", [])
                if e.get("hypothesis_id") == hypothesis_id]
        container = (inst or {}).get("container") or {}
        out: dict[str, Any] = {"instance": name, "exists": inst is not None,
                               "state": container.get("state") or ("no container" if inst else None),
                               "status": container.get("status"), "experiments": exps}
        if container.get("id"):
            out["log_tail"] = self.logs(container["id"], log_lines)
        return out

    def logs(self, container: str, tail: int = 40, wait_s: float = 4.0) -> list[str]:
        """The container's last lines (the SSE stream, read briefly then closed)."""
        lines: list[str] = []
        deadline = time.monotonic() + wait_s
        try:
            with self.http.stream("GET", f"/spawner/container/{container}/logs", params={"tail": tail},
                                  timeout=wait_s + 2) as r:
                if r.status_code >= 400:
                    return [f"(logs unavailable: HTTP {r.status_code})"]
                for raw in r.iter_lines():
                    if raw.startswith("data:"):
                        lines.append(raw[5:].lstrip())
                    if time.monotonic() > deadline or len(lines) >= tail:
                        break
        except httpx.ReadTimeout:
            pass  # a running container's stream stays open: what arrived is the tail
        except httpx.HTTPError as e:
            lines.append(f"(log stream: {e})")
        return lines[-tail:]

    # ── The lake and the platform ───────────────────────────────────────────
    def datasets(self) -> dict:
        recorded = []
        for i in self.get("/spawner/instances").get("instances", []):
            d = i.get("data_dir") or ""
            if "/raw/" in d:
                recorded.append({"path": "raw/" + d.split("/raw/", 1)[1], "instance": i.get("name"),
                                 "template": i.get("template"), "params": i.get("params")})
        return {"cme_futures": CME_FOLDERS, "recorded_by_instances": recorded,
                "dataset_format": "'<lake folder>[+<folder>] <interval> <from>..<to>', e.g. "
                                  "'raw/cme/databento+raw/cme/massive/cme 1m 2014-09-02..2024-09-06'"}

    def alerts(self) -> Any:
        return self.get("/alerts/inbox")


def family_for(dataset: str) -> str:
    """The search family a dataset implies: CME futures are orb."""
    folder = dataset.strip().split()[0] if dataset.strip() else ""
    return "orb" if folder.startswith("raw/cme/") else "trend-band"


def _experiment_brief(e: dict) -> dict:
    return {"id": e["id"], "hypothesis_id": e.get("hypothesis_id"), "verdict": e.get("verdict"),
            "holdout_score": e.get("holdout_score"), "selection_score": e.get("selection_score"),
            "holdout_trades": e.get("holdout_trades"), "trials": e.get("n_trials"),
            "selected_params": e.get("selected_params"), "reasons": e.get("verdict_reasons"),
            "finished_at": e.get("finished_at")}


def dumps(x: Any) -> str:
    return json.dumps(x, indent=1, default=str)
