"""The FKS platform as MCP tools: research the edge ladder, nothing more.

Reads: the ladder, one hypothesis, one experiment, datasets, a search's
status, alerts. Writes: an edge, a hypothesis (frozen once registered), a
search. Nothing here deletes, promotes a rung, arms live trading or touches
a key: those stay in the web UI, behind a person.
"""

from __future__ import annotations

from typing import Any

from mcp.server.mcpserver import MCPServer

from .client import FKS, Config, FKSError, dumps

INSTRUCTIONS = """FKS edge research. The process: create an edge (the idea's home), register
a hypothesis on it (FROZEN at registration: idea, dataset, params grid, costs,
holdout share can never change; a new idea is a new hypothesis), then start a
search; the search writes one experiment with a verdict (USABLE or DO NOT
PASTE) against the U1-U13 clauses. Every search spends the trial budget, so
register only ideas worth testing, and never re-search a rejected idea with
tweaked params to get a pass: that is the overfitting the ladder exists to stop.
Report verdicts as they are; don't call DO NOT PASTE "almost passed"."""


def build(fks: FKS | None = None) -> MCPServer:
    server = MCPServer("fks", instructions=INSTRUCTIONS)
    holder: dict[str, FKS] = {}

    def api() -> FKS:
        if "c" not in holder:
            holder["c"] = fks or FKS(Config.from_env())
        return holder["c"]

    def run(fn, *a, **k) -> str:
        try:
            return dumps(fn(*a, **k))
        except FKSError as e:
            return f"FKS refused: {e}"

    @server.tool(description="The edge ladder in brief: edges, hypotheses (and whether searched), "
                             "experiments with verdicts, deployments with health, trials spent per family.")
    def ladder() -> str:
        return run(lambda: api().ladder_summary())

    @server.tool(description="One hypothesis in full (its frozen spec: idea, dataset, params grid, costs).")
    def hypothesis_get(hypothesis_id: int) -> str:
        return run(lambda: api().hypothesis(hypothesis_id))

    @server.tool(description="One experiment in full: verdict, the reasons (which U-clauses failed), "
                             "scores, selected params, holdout sessions.")
    def experiment_get(experiment_id: int) -> str:
        return run(lambda: api().experiment(experiment_id))

    @server.tool(description="The lake folders a hypothesis's `dataset` can name, with their date ranges, "
                             "and the dataset string format.")
    def datasets() -> str:
        return run(lambda: api().datasets())

    @server.tool(description="Open alerts on the platform.")
    def alerts() -> str:
        return run(lambda: api().alerts())

    @server.tool(description="Create an edge (status research). edge_id: lowercase slug, e.g. 'orb-cme'. "
                             "edge_type: 'rule' (a fixed strategy) or 'adaptive' (janus). assets: symbols, e.g. ['MES','MNQ'].")
    def create_edge(edge_id: str, name: str, edge_type: str, assets: list[str]) -> str:
        return run(lambda: api().create_edge(edge_id, name, edge_type, assets))

    @server.tool(description="Register a hypothesis on an edge. It is FROZEN once registered. Fields: "
                             "idea (20-1000 chars), symbols (list, e.g. ['BTCUSDT'] or ['MES','MNQ']), dataset "
                             "('<folder> <interval> <from>..<to>', see datasets), holdout_share (e.g. 0.25), "
                             "params (list of {name, choices} or {name, min, max, step}; trend-band tunes "
                             "lookback (bars) and band (fraction), orb tunes orb_minutes, target_r, exit; at "
                             "most 5000 grid points), baseline ('buy-and-hold' or 'trend-200d-5pct' for "
                             "trend-band, 'session-hold' for orb), metric ('net return after fees', 'sharpe "
                             "after fees' or 'return over max drawdown after fees'), costs ({maker_bps, "
                             "taker_bps, slippage_bps} for crypto, e.g. 40/80/5 = Kraken tier 1; "
                             "{commission_usd_rt, slippage_ticks, contracts, account_usd, trail_usd} for "
                             "futures), evidence ('trades', the default, or 'low-frequency').")
    def register_hypothesis(edge_id: str, hypothesis: dict[str, Any]) -> str:
        return run(lambda: api().register_hypothesis(edge_id, hypothesis))

    @server.tool(description="Start the search for a hypothesis (one job, instance edge-search-h<id>). "
                             "family is inferred from the dataset (raw/cme -> orb, else trend-band) unless "
                             "given. It runs for minutes to hours; poll search_status.")
    def start_search(hypothesis_id: int, family: str | None = None) -> str:
        return run(lambda: api().start_search(hypothesis_id, family))

    @server.tool(description="A search's progress: its container state, the last log lines, and the "
                             "experiment(s) it wrote (with verdicts) once finished.")
    def search_status(hypothesis_id: int, log_lines: int = 40) -> str:
        return run(lambda: api().search_status(hypothesis_id, max(1, min(log_lines, 200))))

    return server


def main() -> None:
    build().run("stdio")
