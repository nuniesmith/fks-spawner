"""Edge-search job configuration: the environment the spawner sets, and the
verdict thresholds.

The spawner runs this from the `job.edge-search` template (a one-shot job
instance, fks-spawner #69):

  EDGE_HYPOTHESIS_ID  the registered hypothesis to test; required
  EDGE_FAMILY         the strategy family to search (default trend-band)
  FKS_DB_URL          the scoped research database (fks_backtest role),
                      added by the spawner at spawn; required
  LAKE_DIR            the lake, read-only (default /lake)
  DATA_DIR            this job's own folder for artifacts (default /data)
  FKS_BOT_ID          set by the spawner; names this run in logs
  STATUS_PORT         /health, /metrics, /status while it runs (default 9091)

THRESHOLDS are EDGE_PROCESS_2026-10-26 §9's proposals (†), NOT confirmed
yet. They live here, in one place, and every experiment records
THRESHOLDS["version"] in its fingerprint, so a verdict can always be traced
to the rules it was judged by. Changing one is a code change plus a note in
EDGE_PROCESS §9, never a quiet edit.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

THRESHOLDS = {
    # EDGE_PROCESS §9 proposals, 2026-10-26. Bump the version on any change.
    "version": "edge-process-2026-10-26-proposed",
    # U1: share of the window's bars present, per symbol.
    "min_coverage": 0.99,
    # U4: holdout trades, pooled over the symbols.
    "min_holdout_trades": 30,
    # U5: the holdout keeps at least this share of the selection score.
    "overfit_retention": 0.5,
    # U7: the deflated Sharpe ratio, as a probability.
    "dsr_confidence": 0.95,
    # U8: share of random strategies (same trades, holding, costs) beaten.
    "random_beat_share": 0.95,
    "random_strategies": 1000,
    # The walk-forward over the training part.
    "folds": 4,
}

# The metrics a hypothesis may name (EDGE_PROCESS §2.5), all net of costs.
METRICS = {
    "net return after fees": "cagr",
    "sharpe after fees": "sharpe",
    "return over max drawdown after fees": "calmar",
}

# Baselines a hypothesis may name (EDGE_PROCESS §2.4).
BASELINES = ("buy-and-hold", "trend-200d-5pct")

# The most trials one search may run: a typo'd step can't ask for millions.
MAX_TRIALS = 5000


class ConfigError(ValueError):
    """A setting the job can't run with; the message says which."""


@dataclass(frozen=True)
class Config:
    hypothesis_id: int
    family: str
    db_url: str
    lake_dir: Path
    data_dir: Path
    bot_id: str
    port: int


def load(env: Mapping[str, str] | None = None) -> Config:
    env = os.environ if env is None else env
    raw = env.get("EDGE_HYPOTHESIS_ID", "").strip()
    if not raw.isdigit() or int(raw) < 1:
        raise ConfigError(f"EDGE_HYPOTHESIS_ID must be a hypothesis id, not {raw!r}")
    db_url = env.get("FKS_DB_URL", "").strip()
    if not db_url:
        raise ConfigError("FKS_DB_URL is required: the spawner adds it for a research job")
    try:
        port = int(env.get("STATUS_PORT", "9091"))
    except ValueError:
        raise ConfigError("STATUS_PORT must be a number") from None
    return Config(
        hypothesis_id=int(raw),
        family=env.get("EDGE_FAMILY", "trend-band").strip() or "trend-band",
        db_url=db_url,
        lake_dir=Path(env.get("LAKE_DIR", "/lake")),
        data_dir=Path(env.get("DATA_DIR", "/data")),
        bot_id=env.get("FKS_BOT_ID", "local"),
        port=port,
    )
