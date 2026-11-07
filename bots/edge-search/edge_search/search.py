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
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import datetime

import numpy as np

from . import engine, orb
from .config import BASELINES, EVIDENCE_PATHS, MAX_TRIALS, METRICS, THRESHOLDS
from .dataset import Bars, DatasetSpec, MinuteBars, align, align_funding, coverage
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
    # fks 028: declared at registration and frozen. 'trades' = U4 as written.
    evidence: str = "trades"


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
    evidence: str = "trades"
    holdout_trades_by_symbol: dict | None = None
    regimes: dict | None = None
    #: A perpetual's funding, as applied: each symbol's mean rate and what
    #: the selection paid, per year in the market. None for spot and sessions.
    funding: dict | None = None


def regimes(closes: np.ndarray, hold: int, n: int, bar_ms: int, metric: str,
            bars_per_year: float, selection: np.ndarray, baseline: np.ndarray,
            lookback_bars: int | None = None) -> dict:
    """The holdout split by market regime, for the record (not a clause).

    Holdout bar t earns the return of a position decided at t-1, so its regime
    is known at t-1 too: "up" when the coins' mean trailing return over
    `regime_lookback_days`, from t-1-L to t-1, is positive; "down" otherwise.
    Coins not listed yet are left out of the mean; a bar with no coin that
    has L bars of history is "unlabeled". Each regime is scored with the
    hypothesis's metric, for the selection and the baseline alike.
    """
    days = THRESHOLDS["regime_lookback_days"]
    lb = lookback_bars if lookback_bars is not None else max(1, round(days * 86_400_000 / bar_ms))
    labels: list[str] = []
    for t in range(hold, n):
        a, b = t - 1, t - 1 - lb
        if b < 0:
            labels.append("unlabeled")
            continue
        with np.errstate(divide="ignore", invalid="ignore"):
            r = closes[:, a] / closes[:, b] - 1.0
        r = r[np.isfinite(r)]
        labels.append("unlabeled" if r.size == 0 else ("up" if float(r.mean()) > 0 else "down"))
    lab = np.array(labels)
    out: dict = {"lookback_days": days}
    for name in ("up", "down"):
        mask = lab == name
        k = int(mask.sum())
        out[name] = {
            "bars": k,
            "selection": engine.score(metric, selection[mask], bars_per_year) if k > 1 else None,
            "baseline": engine.score(metric, baseline[mask], bars_per_year) if k > 1 else None,
        }
    out["unlabeled_bars"] = int((lab == "unlabeled").sum())
    return out


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
            "evidence": h.evidence,
        },
        sort_keys=True,
    )
    return f"{family}:" + hashlib.sha256(blob.encode()).hexdigest()[:16]


#: A registered window must be complete before it is searched: the data has
#: to reach this close to its end (weekends and holidays on a session grid).
END_SLACK_MS = 4 * 86_400_000


@dataclass
class Evaluation:
    """One parameter set over the whole grid."""

    pool: np.ndarray  # the pool's net return per grid bar
    entries: np.ndarray  # [symbol, bar] entries
    state: object  # the family's own record, for its random benchmark


@dataclass
class Problem:
    """What a family hands the judge: its grid, its runs, its baseline and
    its random benchmark. Everything after this is the same for every family."""

    times: np.ndarray
    bar_ms: int
    bars_per_year: float
    warm: int
    t1: int  # the data's exclusive end, ms
    coverage: dict[str, float]
    closes: np.ndarray  # [symbol, bar], for the regime split
    regime_lookback_bars: int
    evaluate: Callable[[dict], Evaluation]
    baseline_pools: Callable[[int, int], tuple[np.ndarray, np.ndarray]]
    random_pools: Callable[[Evaluation, dict, int, np.random.Generator, int], Iterable[np.ndarray]]
    #: The same run with every cost multiplied (U10's stress).
    evaluate_costs: Callable[[dict, float], Evaluation] | None = None
    #: The account a prop firm would judge, from bar `hold` on (U13); None
    #: when the family trades a cash account.
    account: Callable[[Evaluation, int], dict] | None = None
    #: A perpetual's funding[symbol, bar], already in every run; None for
    #: spot and sessions. Kept to report what the selection paid.
    funding: np.ndarray | None = None


def _position_problem(family: str, h: Hypothesis, spec: DatasetSpec, series: list[Bars],
                      combos: list[dict]) -> Problem:
    if h.baseline not in ("buy-and-hold", "trend-200d-5pct"):
        raise SearchError(f"{family} is judged against buy-and-hold or trend-200d-5pct, not {h.baseline!r}")
    bar_ms = spec.bar_ms
    times, closes = align(series, bar_ms)
    closes = engine.forward_fill(closes)
    rets = engine.returns(closes)
    # A perpetual's funding is measured, not estimated: it is in every run
    # (selection, baselines, random benchmark) and is not stressed by U10.
    funding = align_funding(series, times, bar_ms)
    cost = (float(h.costs.get("taker_bps", 0)) + float(h.costs.get("slippage_bps", 0))) / 10_000
    baseline_lookback = round(200 * 86_400_000 / bar_ms)
    warm = max(
        max(int(c.get("lookback", 0)) for c in combos),
        baseline_lookback if h.baseline == "trend-200d-5pct" else 0,
    )

    def evaluate_costs(params: dict, mult: float) -> Evaluation:
        pos = engine.positions(family, closes, params)
        run = engine.run(pos, rets, cost * mult, funding)
        entries = np.zeros_like(pos)
        entries[:, 1:] = pos[:, 1:] > pos[:, :-1]
        return Evaluation(run.pool, entries, pos)

    def evaluate(params: dict) -> Evaluation:
        return evaluate_costs(params, 1.0)

    def baseline_pools(warm_: int, hold: int) -> tuple[np.ndarray, np.ndarray]:
        if h.baseline == "buy-and-hold":
            return (engine.buy_and_hold(rets, cost, warm_, funding).pool,
                    engine.buy_and_hold(rets, cost, hold, funding).pool)
        base = engine.run(engine.trend_band_positions(closes, baseline_lookback, 0.05), rets, cost, funding).pool
        return base, base

    def random(ev: Evaluation, params: dict, hold: int, rng: np.random.Generator, draws: int):
        n = len(times)
        for _ in range(draws):
            yield engine.run(random_positions(ev.state, hold, n, rng), rets, cost, funding).pool

    return Problem(
        times=times, bar_ms=bar_ms, bars_per_year=365 * 86_400_000 / bar_ms, warm=warm,
        t1=int(times[-1]) + bar_ms, coverage={b.symbol: coverage(b, spec) for b in series},
        closes=closes, regime_lookback_bars=max(1, round(THRESHOLDS["regime_lookback_days"] * 86_400_000 / bar_ms)),
        evaluate=evaluate, baseline_pools=baseline_pools, random_pools=random,
        evaluate_costs=evaluate_costs, funding=funding,
    )


def _orb_problem(h: Hypothesis, spec: DatasetSpec, series: list[MinuteBars], combos: list[dict]) -> Problem:
    if h.baseline != orb.BASELINE:
        raise SearchError(
            f"orb is judged against {orb.BASELINE!r} (long each session, open to close: the "
            f"intraday drift an intraday rule could be riding), not {h.baseline!r}"
        )
    try:
        orb.check_params(combos)
        data = orb.prepare(h.symbols, series, spec, h.costs)
    except orb.OrbError as e:
        raise SearchError(str(e)) from None
    base = orb.session_hold(data)

    def evaluate_costs(params: dict, mult: float) -> Evaluation:
        d = data if mult == 1.0 else replace(data, commission=data.commission * mult,
                                             slip_ticks=data.slip_ticks * mult)
        try:
            trades = orb.trades_for(d, params)
        except orb.OrbError as e:
            raise SearchError(str(e)) from None
        net, entries = orb.daily(d, trades)
        return Evaluation(orb.pool(net), entries, trades)

    def evaluate(params: dict) -> Evaluation:
        return evaluate_costs(params, 1.0)

    acct = account_terms(h.costs)

    def account(ev: Evaluation, hold: int) -> dict:
        if acct is None:
            return {"declared": False}
        return {"declared": True, **acct, **orb.account_path(data, ev.state, hold, **acct)}

    return Problem(
        times=data.times, bar_ms=86_400_000, bars_per_year=orb.SESSIONS_PER_YEAR, warm=0,
        t1=int(data.times[-1]) + 86_400_000, coverage=orb.coverage(data),
        closes=orb.session_closes(data),
        regime_lookback_bars=orb.regime_lookback_sessions(THRESHOLDS["regime_lookback_days"]),
        evaluate=evaluate, baseline_pools=lambda warm_, hold: (base, base),
        random_pools=lambda ev, params, hold, rng, draws: orb.random_pools(data, ev.state, params, hold, rng, draws),
        evaluate_costs=evaluate_costs, account=account,
    )


def account_terms(costs: dict) -> dict | None:
    """The prop account a futures hypothesis declares (U13): contracts per
    symbol, the starting balance and the trailing drawdown, all or none."""
    keys = ("contracts", "account_usd", "trail_usd")
    present = [k for k in keys if costs.get(k) is not None]
    if not present:
        return None
    if len(present) != len(keys):
        raise SearchError(f"the account is contracts, account_usd and trail_usd together; got {present}")
    return {"contracts": int(costs["contracts"]), "account_usd": float(costs["account_usd"]),
            "trail_usd": float(costs["trail_usd"])}


def day_sums(times: np.ndarray, pool: np.ndarray) -> np.ndarray:
    """The pool's net return summed per UTC day (bars may be shorter than a day)."""
    days = (np.asarray(times, dtype=np.int64) // 86_400_000)
    if not len(days):
        return np.zeros(0)
    _, idx = np.unique(days, return_inverse=True)
    return np.bincount(idx, weights=pool)


def run_search(
    h: Hypothesis,
    family: str,
    spec: DatasetSpec,
    series: list,
    prior_trials: int,
    now: datetime,
    seed: int = 20261026,
    progress=None,
) -> Result:
    metric = metric_of(h)
    if h.evidence not in EVIDENCE_PATHS:
        raise SearchError(f"evidence {h.evidence!r} isn't one this job judges: {list(EVIDENCE_PATHS)}")
    if h.baseline not in BASELINES:
        raise SearchError(f"baseline {h.baseline!r} isn't one this job computes: {list(BASELINES)}")
    try:
        names, values, combos = engine.grid(family, h.params)
    except engine.FamilyError as e:
        raise SearchError(str(e)) from None
    if len(combos) > MAX_TRIALS:
        raise SearchError(f"{len(combos)} grid points is more than {MAX_TRIALS}; widen the steps")

    problem = (_orb_problem(h, spec, series, combos) if family == "orb"
               else _position_problem(family, h, spec, series, combos))
    times, bars_per_year, warm = problem.times, problem.bars_per_year, problem.warm
    n = len(times)

    # A forward holdout: the split follows the data, so a window searched
    # before it is complete would move its holdout into data already seen.
    if problem.t1 < spec.end_ms - END_SLACK_MS:
        last = datetime.fromtimestamp((problem.t1 - 1) / 1000, tz=now.tzinfo).date()
        raise SearchError(
            f"the data ends {last}, before the registered window's end {spec.end}: "
            "a window is searched once it is complete"
        )

    # 1. The split, by time.
    t0, t1 = int(times[0]), problem.t1
    split_ms = t0 + (1.0 - h.holdout_share) * (t1 - t0)
    hold = int(np.searchsorted(times, split_ms))
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
        ev = problem.evaluate(params)
        fold_scores = [engine.score(metric, ev.pool[a:b], bars_per_year) for a, b in spans]
        train = ev.pool[warm:hold]
        sd = float(np.std(train, ddof=1)) if len(train) > 1 else 0.0
        trials.append(
            Trial(
                params=params,
                folds=fold_scores,
                selection=float(np.mean(fold_scores)),
                sharpe_per_bar=float(np.mean(train)) / sd if sd > 0 else 0.0,
                trades=int(ev.entries[:, warm:hold].sum()),
                holdout=ev.pool[hold:].copy(),
            )
        )
        if progress is not None:
            progress(done, len(combos))
    best = max(trials, key=lambda t: t.selection)
    best_ev = problem.evaluate(best.params)

    # The baseline, on the same bars and costs.
    base_train, base_hold = problem.baseline_pools(warm, hold)
    base_folds = [engine.score(metric, base_train[a:b], bars_per_year) for a, b in spans]
    baseline_selection = float(np.mean(base_folds))

    # 4. The holdout, once.
    holdout = best.holdout
    holdout_score = engine.score(metric, holdout, bars_per_year)
    baseline_holdout = engine.score(metric, base_hold[hold:], bars_per_year)
    holdout_trades = int(best_ev.entries[:, hold:n].sum())

    # 5. The clauses.
    v = Verdict()
    cov = problem.coverage
    low = {s: c for s, c in cov.items() if c < THRESHOLDS["min_coverage"]}
    v.add("U1", not low, "dataset complete" if not low else
          "coverage under {:.0%}: {}".format(THRESHOLDS["min_coverage"],
                                             ", ".join(f"{s} {c:.2%}" for s, c in low.items())))
    v.add("U2", h.registered_at < now,
          f"registered {h.registered_at:%Y-%m-%d %H:%M} UTC, before the search")
    v.add("U3", holdout_score > baseline_holdout,
          f"holdout {holdout_score:.4g} vs {h.baseline} {baseline_holdout:.4g}")
    per_symbol = {s: int(best_ev.entries[i, hold:n].sum()) for i, s in enumerate(h.symbols)}
    if h.evidence == "low-frequency":
        # EDGE_PROCESS §3–4: a low-frequency edge can't reach 30 trades on any
        # history; it must instead hold with ONE parameter set across many
        # coins, every one of which trades in the holdout.
        need_t = THRESHOLDS["low_frequency_min_holdout_trades"]
        need_s = THRESHOLDS["low_frequency_min_symbols"]
        traded = [s for s, c in per_symbol.items() if c > 0]
        v.add("U4", holdout_trades >= need_t and len(traded) >= need_s and len(traded) == len(per_symbol),
              f"low-frequency path: {holdout_trades} holdout trades pooled (need {need_t}); "
              f"{len(traded)} of {len(per_symbol)} symbols traded (need {need_s}, and all of them): "
              + ", ".join(f"{s} {c}" for s, c in per_symbol.items()))
    else:
        v.add("U4", holdout_trades >= THRESHOLDS["min_holdout_trades"],
              f"{holdout_trades} holdout trades over {len(per_symbol)} symbols "
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
    draws = int(THRESHOLDS["random_strategies"])
    beaten = sum(
        1 for rp in problem.random_pools(best_ev, best.params, hold, rng, draws)
        if holdout_score > engine.score(metric, rp[hold:], bars_per_year)
    )
    share = beaten / draws if draws else 0.0
    v.add("U8", share >= THRESHOLDS["random_beat_share"],
          f"beats {share:.1%} of {draws} random strategies (need {THRESHOLDS['random_beat_share']:.0%})")
    edge = boundary_params(best.params, names, values)
    v.add("U9", not edge, "no parameter on its range's edge" if not edge
          else "on the range's edge: " + ", ".join(edge))

    # The futures program's bars (2026-11-02), on the same holdout.
    mult = float(THRESHOLDS["cost_stress"])
    stressed = (problem.evaluate_costs(best.params, mult).pool[hold:]
                if problem.evaluate_costs is not None else holdout)
    m1, m2 = float(np.mean(holdout)), float(np.mean(stressed))
    v.add("U10", m1 > 0 and m2 > 0,
          f"holdout mean net return per bar {m1:.3g} at 1x costs, {m2:.3g} at {mult:g}x (both must be above 0)")
    half = len(holdout) // 2
    first, second = float(np.sum(holdout[:half])), float(np.sum(holdout[half:]))
    v.add("U11", first > 0 and second > 0,
          f"holdout halves {first:.4g} / {second:.4g} (both must be above 0)")
    k_days = int(THRESHOLDS["concentration_days"])
    by_day = day_sums(times[hold:], holdout)
    rest = float(np.sum(np.sort(by_day)[:-k_days])) if len(by_day) > k_days else 0.0
    v.add("U12", rest > 0,
          f"holdout without its best {k_days} days {rest:.4g} (of {float(np.sum(by_day)):.4g}; must stay above 0)")
    if problem.account is None:
        v.add("U13", True, "a cash account: no trailing drawdown to survive")
    else:
        acc = problem.account(best_ev, hold)
        if not acc["declared"]:
            v.add("U13", False, "no prop account declared (contracts, account_usd, trail_usd): a futures "
                                "edge is judged on the account it would trade")
        else:
            v.add("U13", not acc["breached"],
                  f"{acc['contracts']} contract(s) per symbol on ${acc['account_usd']:,.0f} with a "
                  f"${acc['trail_usd']:,.0f} trail from the holdout's start: "
                  + (f"BREACHED {acc['breach_day']}" if acc["breached"] else "no breach")
                  + f"; worst headroom ${acc['worst_headroom']:,.2f} ({acc['worst_day']}), "
                  f"final ${acc['final_balance']:,.2f}")

    regime_split = regimes(problem.closes, hold, n, problem.bar_ms, metric, bars_per_year,
                           holdout, base_hold[hold:], lookback_bars=problem.regime_lookback_bars)

    funding_summary = None
    if problem.funding is not None:
        f, pos = problem.funding, best_ev.state
        listed = ~np.isnan(problem.closes)
        funding_summary = {
            "applied": True,
            "mean_rate_per_year": {
                s: float(f[i, listed[i]].sum() / max(int(listed[i].sum()), 1) * bars_per_year)
                for i, s in enumerate(h.symbols)
            },
            "selection_paid_per_year_in_market": engine.funding_paid(pos, f, bars_per_year),
            "holdout_paid_per_year_in_market": engine.funding_paid(pos[:, hold - 1:], f[:, hold - 1:], bars_per_year),
        }

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
        evidence=h.evidence,
        holdout_trades_by_symbol=per_symbol,
        regimes=regime_split,
        funding=funding_summary,
    )
