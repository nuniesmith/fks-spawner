# fks-mcp

The FKS edge ladder as MCP tools, so an agent (Claude Code, Claude Desktop) can
do edge research through the platform's API v1 with a personal access token.

| Tool | What it does |
|---|---|
| `ladder` | edges, hypotheses (searched or not), experiments with verdicts, deployments with health, trials spent |
| `hypothesis_get` / `experiment_get` | one record in full |
| `datasets` | the lake folders a hypothesis can name, and the dataset string format |
| `alerts` | open alerts |
| `create_edge` | a new edge, status research |
| `register_hypothesis` | a hypothesis, **frozen at registration** |
| `start_search` | the `job.edge-search` instance `edge-search-h<id>` (family inferred: `raw/cme/…` → orb) |
| `search_status` | container state, the last log lines, the experiment once written |

Nothing deletes, promotes a rung, arms live trading or touches a key: those
stay in the web UI. The token's role caps the rest (tokens are never admin).

## Setup

1. Web UI → Settings → API tokens → create an **operator** token (`fks_pat_…`).
2. Install and register (Claude Code, user scope):

```sh
python3 -m venv ~/.local/share/fks-mcp && ~/.local/share/fks-mcp/bin/pip install -r tools/fks-mcp/requirements.txt
claude mcp add fks --scope user \
  -e FKS_TOKEN=fks_pat_… -e PYTHONPATH="$PWD/tools/fks-mcp" \
  -- ~/.local/share/fks-mcp/bin/python -m fks_mcp
```

Run it from the fks-spawner checkout. The token lands in `~/.claude.json`;
revoke it in the web UI if that file leaks.

`FKS_URL` defaults to `https://oryx.tailfef10.ts.net/api/v1` (tailnet only).
