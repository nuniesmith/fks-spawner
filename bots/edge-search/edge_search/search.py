"""One search: a hypothesis, its dataset and its grid, to scores and a verdict.

Pure (no database, no network, no clock but `now`), so the whole judgement is
tested on synthetic data. core.py reads the hypothesis and the lake, calls
`run_search`, and writes the result.

The order is the process's (EDGE_PROCESS §3-4):
  1. the window splits by time: training first, the holdout (the last
     `holdout_share`) after it, untouched until step 4;
  2. every grid point is scored on the training part's folds (walk-forward;
     the family has no fitted state, so a fold's score is the parameters'
     score on that stretch), and every one counts as a trial;
  3. the best mean fold score is the selection;
  4. the selection, its baseline and its neighbours are scored on the
     holdout once;
  5. the clauses U1-U9 decide the verdict.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

import numpy as np

from . import engine
from .config import BASELINES, MAX_TRIALS, METRICS, THRESHOLDS
from .dataset import Bars, DatasetSpec, align, coverage
from .verdict import (
    Verdict,
    boundary_params,
    deflated_sharpe,
    expected_max_sharpe,
    neighbours,
    random_positions,
)


class SearchError(ValueError):
    """The search can't run as registered; nothing is written, nothing counted."""


@dataclass
class Hypothesis:
    id: int
    edge_id: str
    idea: str
    symbols: list[str]
    dataset: str
    holdout_share: float
    params: list[dict]
    baseline: str
    metric: str
    costs: dict
    registered_at: datetime


@dataclass
class Trial:
    params: dict
    folds: list[float]
    selection: float
    sharpe_per_bar: float
    trades: int
    # Only the holdout part is kept (memory); nothing selects on it: the
    # neighbour check (U6) reads it after the selection is made.
    holdout: np.ndarray


@dataclass
class Result:
    family: str
    names: list[str]
    values: list[list]
    trials: list[Trial]
    best: Trial
    selection_score: float
    holdout_score: float
    baseline_selection_score: float
    baseline_holdout_score: float
    selection_trades: int
    holdout_trades: int
    train_days: int
    test_days: int
    folds: int
    coverage: dict[str, float]
    n_trials: int
    trials_total: int
    dsr: float
    random_share: float
    verdict: Verdict
    fingerprint: str
    times: np.ndarray
    holdout_start: int
    baseline_pool: np.ndarray


def metric_of(h: Hypothesis) -> str:
    key = h.metric.strip().lower()
    if key not in METRICS:
        raise SearchError(f"metric {h.metric!r} isn't one this job scores: {list(METRICS)}")
    return METRICS[key]


def fingerprint(family: str, h: Hypothesis, spec: DatasetSpec, names: list[str], values: list[list]) -> str:
    """The problem being searched: two experiments with the same fingerprint
    searched the same thing; different ones are not comparable."""
    blob = json.dumps(
        {
            "family": family,
            "dataset": [spec.path, spec.interval, spec.start.isoformat(), spec.end.isoformat()],
            "symbols": sorted(h.symbols),
            "holdout_share": h.holdout_share,
            "folds": THRESHOLDS["folds"],
            "space": dict(zip(names, values)),
            "costs": h.costs,
            "metric": metric_of(h),
            "baseline": h.baseline,
            "thresholds": THRESHOLDS["version"],
        },
        sort_keys=True,
    )
    return f"{family}:" + hashlib.sha256(blob.encode()).hexdigest()[:16]


def run_search(
    h: Hypothesis,
    family: str,
    spec: DatasetSpec,
    series: list[Bars],
    prior_trials: int,
    now: datetime,
    seed: int = 20261026,
    progress=None,
) -> Result:
    metric = metric_of(h)
    if h.baseline not in BASELINES:
        raise SearchError(f"baseline {h.baseline!r} isn't one this job computes: {list(BASELINES)}")
    try:
        names, values, combos = engine.grid(family, h.params)
    except engine.FamilyError as e:
        raise SearchError(str(e)) from None
    if len(combos) > MAX_TRIALS:
        raise SearchError(f"{len(combos)} grid points is more than {MAX_TRIALS}; widen the steps")

    bar_ms = spec.bar_ms
    bars_per_year = 365 * 86_400_000 / bar_ms
    times, closes = align(series, bar_ms)
    closes = engine.forward_fill(closes)
    rets = engine.returns(closes)
    n = len(times)
    cost = (float(h.costs.get("taker_bps", 0)) + float(h.costs.get("slippage_bps", 0))) / 10_000

    # 1. The split, by time.
    t0, t1 = int(times[0]), int(times[-1]) + bar_ms
    split_ms = t0 + (1.0 - h.holdout_share) * (t1 - t0)
    hold = int(np.searchsorted(times, split_ms))
    baseline_lookback = round(200 * 86_400_000 / bar_ms)
    warm = max(
        max(int(c.get("lookback", 0)) for c in combos),
        baseline_lookback if h.baseline == "trend-200d-5pct" else 0,
    )
    k = int(THRESHOLDS["folds"])
    if hold - warm < 2 * k or n - hold < 2:
        raise SearchError(
            f"not enough data: {hold - warm} training bars after a {warm}-bar warm-up, "
            f"{n - hold} holdout bars"
        )
    spans = engine.folds(warm, hold, k)

    # 2-3. Every grid point on the training folds; the best mean is the selection.
    trials: list[Trial] = []
    for done, params in enumerate(combos, start=1):
        pos = engine.positions(family, closes, params)
        run = engine.run(pos, rets, cost)
        fold_scores = [engine.score(metric, run.pool[a:b], bars_per_year) for a, b in spans]
        train = run.pool[warm:hold]
        sd = float(np.std(train, ddof=1)) if len(train) > 1 else 0.0
        trials.append(
            Trial(
                params=params,
                folds=fold_scores,
                selection=float(np.mean(fold_scores)),
                sharpe_per_bar=float(np.mean(train)) / sd if sd > 0 else 0.0,
                trades=engine.trades(pos, warm, hold),
                holdout=run.pool[hold:].copy(),
            )
        )
        if progress is not None:
            progress(done, len(combos))
    best = max(trials, key=lambda t: t.selection)
    best_pos = engine.positions(family, closes, best.params)

    # The baseline, on the same bars and costs.
    if h.baseline == "buy-and-hold":
        base_train = engine.buy_and_hold(rets, cost, warm).pool
        base_hold = engine.buy_and_hold(rets, cost, hold).pool
    else:
        base = engine.run(engine.trend_band_positions(closes, baseline_lookback, 0.05), rets, cost).pool
        base_train = base_hold = base
    base_folds = [engine.score(metric, base_train[a:b], bars_per_year) for a, b in spans]
    baseline_selection = float(np.mean(base_folds))

    # 4. The holdout, once.
    holdout = best.holdout
    holdout_score = engine.score(metric, holdout, bars_per_year)
    baseline_holdout = engine.score(metric, base_hold[hold:], bars_per_year)
    holdout_trades = engine.trades(best_pos, hold, n)

    # 5. The clauses.
    v = Verdict()
    cov = {b.symbol: coverage(b, spec) for b in series}
    low = {s: c for s, c in cov.items() if c < THRESHOLDS["min_coverage"]}
    v.add("U1", not low, "dataset complete" if not low else
          "coverage under {:.0%}: {}".format(THRESHOLDS["min_coverage"],
                                             ", ".join(f"{s} {c:.2%}" for s, c in low.items())))
    v.add("U2", h.registered_at < now,
          f"registered {h.registered_at:%Y-%m-%d %H:%M} UTC, before the search")
    v.add("U3", holdout_score > baseline_holdout,
          f"holdout {holdout_score:.4g} vs {h.baseline} {baseline_holdout:.4g}")
    v.add("U4", holdout_trades >= THRESHOLDS["min_holdout_trades"],
          f"{holdout_trades} holdout trades over {len(series)} symbols "
          f"(need {THRESHOLDS['min_holdout_trades']})")
    retention = THRESHOLDS["overfit_retention"]
    keeps = (best.selection > baseline_selection and holdout_score > baseline_holdout
             and holdout_score >= retention * best.selection)
    v.add("U5", keeps,
          f"holdout {holdout_score:.4g} vs selection {best.selection:.4g} (keep {retention:.0%}); "
          f"baselines {baseline_selection:.4g} / {baseline_holdout:.4g}")
    near = neighbours(best.params, names, values)
    if not near:
        v.add("U6", True, "one-point grid: nothing was selected")
    else:
        by_params = {json.dumps(t.params, sort_keys=True): t for t in trials}
        weak = []
        for p in near:
            score = engine.score(metric, by_params[json.dumps(p, sort_keys=True)].holdout, bars_per_year)
            if score <= baseline_holdout:
                weak.append(f"{p} {score:.4g}")
        v.add("U6", not weak, f"all {len(near)} neighbours beat the baseline" if not weak
              else f"{len(weak)} of {len(near)} neighbours don't beat the baseline: " + "; ".join(weak))
    trials_total = prior_trials + len(trials)
    sr0 = expected_max_sharpe(np.array([t.sharpe_per_bar for t in trials]), trials_total)
    dsr = deflated_sharpe(holdout, sr0)
    v.add("U7", dsr >= THRESHOLDS["dsr_confidence"],
          f"deflated Sharpe {dsr:.3f} after {trials_total} trials for the edge "
          f"(need {THRESHOLDS['dsr_confidence']})")
    rng = np.random.default_rng(seed)
    beaten = 0
    draws = int(THRESHOLDS["random_strategies"])
    for _ in range(draws):
        rp = random_positions(best_pos, hold, n, rng)
        if holdout_score > engine.score(metric, engine.run(rp, rets, cost).pool[hold:], bars_per_year):
            beaten += 1
    share = beaten / draws if draws else 0.0
    v.add("U8", share >= THRESHOLDS["random_beat_share"],
          f"beats {share:.1%} of {draws} random strategies (need {THRESHOLDS['random_beat_share']:.0%})")
    edge = boundary_params(best.params, names, values)
    v.add("U9", not edge, "no parameter on its range's edge" if not edge
          else "on the range's edge: " + ", ".join(edge))

    return Result(
        family=family,
        names=names,
        values=values,
        trials=trials,
        best=best,
        selection_score=best.selection,
        holdout_score=holdout_score,
        baseline_selection_score=baseline_selection,
        baseline_holdout_score=baseline_holdout,
        selection_trades=best.trades,
        holdout_trades=holdout_trades,
        train_days=round((times[hold] - times[warm]) / 86_400_000),
        test_days=round((t1 - times[hold]) / 86_400_000),
        folds=len(spans),
        coverage=cov,
        n_trials=len(trials),
        trials_total=trials_total,
        dsr=dsr,
        random_share=share,
        verdict=v,
        fingerprint=fingerprint(family, h, spec, names, values),
        times=times,
        holdout_start=hold,
        baseline_pool=base_hold,
    )
