"""Run one search and write its experiment (EDGE_PROCESS step 3-4).

Exit codes: 0 the experiment row is written (USABLE or DO NOT PASTE: both
are results); 2 the search couldn't run as registered (nothing written,
nothing counted: the message says why); 1 anything else.

Artifacts, in DATA_DIR/<study_name>/ (the job's own lake folder):
  trials.csv     every grid point: params, fold scores, selection, trades
  holdout.csv    bar time, the selection's and the baseline's pool return
  verdict.json   the row as written, the clauses, coverage, thresholds
"""

from __future__ import annotations

import csv
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import psycopg

from . import db
from .config import THRESHOLDS, ConfigError, load
from .dataset import DatasetError, load_bars, parse_spec
from .search import Result, SearchError, run_search
from .server import Status, serve

log = logging.getLogger("edge-search")


def experiment_row(h, spec, result: Result, study: str, finished: datetime) -> dict:
    return {
        "study_name": study,
        "config_fingerprint": result.fingerprint,
        "symbol": "+".join(h.symbols),
        "bar_minutes": spec.bar_minutes,
        "hypothesis": h.idea,
        "registered_at": h.registered_at,
        "hypothesis_id": h.id,
        "tuned_params": result.names,
        "n_trials": result.n_trials,
        "train_days": result.train_days,
        "test_days": result.test_days,
        "folds": result.folds,
        "holdout_folds": 1,
        "selection_score": result.selection_score,
        "holdout_score": result.holdout_score,
        "baseline_holdout_score": result.baseline_holdout_score,
        "overfit_gap": result.selection_score - result.holdout_score,
        "selection_trades": result.selection_trades,
        "holdout_trades": result.holdout_trades,
        "verdict": result.verdict.label,
        "verdict_reasons": result.verdict.reasons,
        "finished_at": finished,
    }


def write_artifacts(folder: Path, result: Result, row: dict) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / "trials.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([*result.names, *[f"fold{i + 1}" for i in range(result.folds)], "selection", "trades"])
        for t in result.trials:
            w.writerow([*(t.params[n] for n in result.names), *t.folds, t.selection, t.trades])
    with (folder / "holdout.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["open_time_ms", "selection", "baseline"])
        base = result.baseline_pool[result.holdout_start:]
        for t, a, b in zip(result.times[result.holdout_start:], result.best.holdout, base):
            w.writerow([int(t), f"{a:.8f}", f"{b:.8f}"])
    doc = {
        **{k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()},
        "best_params": result.best.params,
        "baseline_selection_score": result.baseline_selection_score,
        "coverage": result.coverage,
        "trials_total_for_edge": result.trials_total,
        "deflated_sharpe": result.dsr,
        "random_beaten_share": result.random_share,
        "clauses": [{"id": c.id, "ok": c.ok, "text": c.text} for c in result.verdict.clauses],
        "thresholds": THRESHOLDS,
    }
    (folder / "verdict.json").write_text(json.dumps(doc, indent=2, default=float))


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S%z")
    try:
        cfg = load()
    except ConfigError as e:
        log.error("%s", e)
        return 2
    status = Status(cfg.bot_id, cfg.hypothesis_id)
    serve(status, cfg.port)
    try:
        with psycopg.connect(cfg.db_url) as conn:
            h = db.load_hypothesis(conn, cfg.hypothesis_id)
            spec = parse_spec(h.dataset)
            log.info("hypothesis %d (%s): %s", h.id, h.edge_id, h.idea)
            status.update(phase="reading the dataset")
            series = [load_bars(cfg.lake_dir, spec, s) for s in h.symbols]
            for b in series:
                log.info("%s: %d %s bars from %s", b.symbol, len(b.open_ms), spec.interval,
                         datetime.fromtimestamp(b.open_ms[0] / 1000, timezone.utc).date())
            prior = db.prior_trials(conn, h.edge_id)
            status.update(phase="searching")
            result = run_search(
                h, cfg.family, spec, series, prior, datetime.now(timezone.utc),
                progress=lambda done, total: status.update(trials_done=done, trials_total=total),
            )
            finished = datetime.now(timezone.utc)
            study = f"{h.edge_id}-h{h.id}-{finished:%Y%m%dT%H%M%SZ}"
            row = experiment_row(h, spec, result, study, finished)
            write_artifacts(cfg.data_dir / study, result, row)
            new_id = db.write_experiment(conn, row)
    except (SearchError, DatasetError) as e:
        log.error("not searched: %s", e)
        status.update(phase="refused", error=str(e))
        return 2
    except psycopg.Error as e:
        log.error("database: %s", e)
        status.update(phase="failed", error=str(e))
        return 1
    status.update(phase="done", verdict=result.verdict.label)
    log.info("experiment %d %s: %s", new_id, study, result.verdict.label)
    for c in result.verdict.clauses:
        log.info("  %s %s %s", c.id, "ok  " if c.ok else "FAIL", c.text)
    log.info("best %s: selection %.4g, holdout %.4g (baseline %.4g); %d trials, N for the edge %d",
             result.best.params, result.selection_score, result.holdout_score,
             result.baseline_holdout_score, result.n_trials, result.trials_total)
    return 0


def run() -> None:
    np.seterr(all="ignore")
    sys.exit(main())
