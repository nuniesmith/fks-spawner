"""The USABLE verdict (EDGE_PROCESS_2026-10-26 §3-4): every clause must hold,
and each failed one is written into `verdict_reasons` in words.

  U1 dataset gate: coverage of the window, per symbol
  U2 registered_at < finished_at (the database refuses otherwise, too)
  U3 holdout score, net of fees, above the baseline's on the same holdout
  U4 holdout trades, pooled, at least THRESHOLDS["min_holdout_trades"]
  U5 the holdout keeps THRESHOLDS["overfit_retention"] of the selection
     score, both above their baselines
  U6 the winner's grid neighbours also beat the baseline on the holdout
  U7 deflated Sharpe ratio ≥ THRESHOLDS["dsr_confidence"], given every trial
     ever run for the edge
  U8 beats THRESHOLDS["random_beat_share"] of random strategies with the same
     trades, holding period and costs, on the holdout
  U9 no tuned parameter on its range's edge (a dead zone attracting the
     sampler)

DO NOT PASTE is a result, not a failure: it is kept and counts toward N.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from statistics import NormalDist

import numpy as np

EULER_GAMMA = 0.5772156649015329
_N = NormalDist()


@dataclass
class Clause:
    id: str
    ok: bool
    text: str


@dataclass
class Verdict:
    clauses: list[Clause] = field(default_factory=list)

    def add(self, id: str, ok: bool, text: str) -> None:
        self.clauses.append(Clause(id, ok, text))

    @property
    def usable(self) -> bool:
        return bool(self.clauses) and all(c.ok for c in self.clauses)

    @property
    def label(self) -> str:
        return "USABLE" if self.usable else "DO NOT PASTE"

    @property
    def reasons(self) -> list[str]:
        return [f"{c.id}: {c.text}" for c in self.clauses if not c.ok]


def expected_max_sharpe(trial_sharpes: np.ndarray, n_trials: int) -> float:
    """SR0: the Sharpe the best of `n_trials` zero-edge draws is expected to
    reach, from the spread of the trials' Sharpes (Bailey & López de Prado,
    2014, the deflated Sharpe ratio)."""
    if n_trials < 2 or len(trial_sharpes) < 2:
        return 0.0
    sd = float(np.std(trial_sharpes, ddof=1))
    a = _N.inv_cdf(1.0 - 1.0 / n_trials)
    b = _N.inv_cdf(1.0 - 1.0 / (n_trials * math.e))
    return sd * ((1.0 - EULER_GAMMA) * a + EULER_GAMMA * b)


def deflated_sharpe(r: np.ndarray, sr0: float) -> float:
    """P(true Sharpe > sr0) for per-bar returns `r`, allowing for their skew
    and fat tails (the probabilistic Sharpe ratio at the threshold sr0)."""
    t = len(r)
    if t < 3:
        return 0.0
    sd = float(np.std(r, ddof=1))
    if sd == 0:
        return 0.0
    sr = float(np.mean(r)) / sd
    z = (r - np.mean(r)) / np.std(r)
    skew = float(np.mean(z**3))
    kurt = float(np.mean(z**4))
    denom = 1.0 - skew * sr + (kurt - 1.0) / 4.0 * sr * sr
    if denom <= 0:
        return 0.0
    return _N.cdf((sr - sr0) * math.sqrt(t - 1) / math.sqrt(denom))


def random_positions(pos: np.ndarray, a: int, b: int, rng: np.random.Generator) -> np.ndarray:
    """Per symbol, as many long spells inside [a, b) as the strategy had,
    each as long as its average spell, at random places that never overlap
    (an overlap would merge two spells and quietly give the random strategy
    fewer trades than the one it is compared with)."""
    out = np.zeros_like(pos)
    n = b - a
    for i, row in enumerate(pos):
        seg = row[a:b]
        if n < 2:
            continue
        entries = int((seg[1:] > seg[:-1]).sum()) + (1 if seg[0] > 0 else 0)
        held = int(seg.sum())
        if entries == 0 or held == 0:
            continue
        length = max(1, round(held / entries))
        if entries * length > n:
            length = max(1, n // entries)
        free = n - entries * length
        # Stars and bars: sorted cut points spread the free bars into gaps.
        cuts = np.sort(rng.integers(0, free + 1, size=entries))
        for k, c in enumerate(cuts):
            start = a + int(c) + k * length
            out[i, start:start + length] = 1.0
    return out


def boundary_params(best: dict, names: list[str], values: list[list]) -> list[str]:
    """Numeric parameters whose winning value is the range's min or max."""
    hits = []
    for name, vals in zip(names, values):
        if len(vals) < 2 or not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in vals):
            continue
        if best[name] in (min(vals), max(vals)):
            hits.append(f"{name}={best[name]}")
    return hits


def neighbours(best: dict, names: list[str], values: list[list]) -> list[dict]:
    """The winner with one parameter moved one grid step either way."""
    out = []
    for name, vals in zip(names, values):
        i = vals.index(best[name])
        for j in (i - 1, i + 1):
            if 0 <= j < len(vals):
                out.append({**best, name: vals[j]})
    return out
