"""The backtest engine: one strategy family, scored net of costs on a pool.

Positions are decided on a bar's close and earn the next bar's return; a
change of position pays `cost` (taker + slippage, per side) on the bar it
happens. A pool is the equal-weight average of the symbols that have a bar
at that time, so a coin that lists late joins the pool when it lists.

Families
--------
trend-band (the live rule's family, EDGE_PROCESS §2.4's incumbent):
    long while close > SMA(lookback) × (1 + band), flat once
    close < SMA(lookback) × (1 − band), unchanged in between (hysteresis).
    Params: lookback (bars), band (fraction).
"""

from __future__ import annotations

import math
import warnings
from dataclasses import dataclass

import numpy as np

FAMILIES = {
    "trend-band": ("lookback", "band"),
}


class FamilyError(ValueError):
    """The hypothesis's parameters don't fit the family; the message says how."""


def forward_fill(closes: np.ndarray) -> np.ndarray:
    """Holes after a symbol's first bar take the last close (a zero return
    during the hole); before its first bar stays NaN (not listed yet)."""
    out = closes.copy()
    for row in out:
        valid = ~np.isnan(row)
        if not valid.any():
            continue
        idx = np.where(valid, np.arange(len(row)), 0)
        np.maximum.accumulate(idx, out=idx)
        first = int(np.argmax(valid))
        row[first:] = row[idx[first:]]
    return out


def returns(closes: np.ndarray) -> np.ndarray:
    """Bar-to-bar returns; NaN before a symbol lists and on its first bar."""
    r = np.full_like(closes, np.nan)
    r[:, 1:] = closes[:, 1:] / closes[:, :-1] - 1.0
    return r


def sma(row: np.ndarray, n: int) -> np.ndarray:
    out = np.full_like(row, np.nan)
    valid = ~np.isnan(row)
    if not valid.any() or n < 1:
        return out
    first = int(np.argmax(valid))
    seg = row[first:]
    if len(seg) < n:
        return out
    c = np.cumsum(np.insert(seg, 0, 0.0))
    out[first + n - 1:] = (c[n:] - c[:-n]) / n
    return out


def trend_band_positions(closes: np.ndarray, lookback: int, band: float) -> np.ndarray:
    """0/1 positions per symbol and bar; 0 until the SMA has `lookback` bars."""
    pos = np.zeros_like(closes)
    for i, row in enumerate(closes):
        m = sma(row, lookback)
        up = row > m * (1.0 + band)
        down = row < m * (1.0 - band)
        state = 0.0
        p = pos[i]
        for t in range(len(row)):
            if up[t]:
                state = 1.0
            elif down[t]:
                state = 0.0
            elif math.isnan(m[t]):
                state = 0.0
            p[t] = state
    return pos


def positions(family: str, closes: np.ndarray, params: dict) -> np.ndarray:
    if family == "trend-band":
        return trend_band_positions(closes, int(params["lookback"]), float(params["band"]))
    raise FamilyError(f"unknown family {family!r}; this job knows {sorted(FAMILIES)}")


@dataclass
class Run:
    """One parameter set over the whole series: net returns per symbol and
    bar, the pool's net return per bar, and the positions."""

    net: np.ndarray  # [symbol, bar]
    pool: np.ndarray  # [bar]
    pos: np.ndarray  # [symbol, bar]


def run(pos: np.ndarray, rets: np.ndarray, cost: float) -> Run:
    held = np.zeros_like(pos)
    held[:, 1:] = pos[:, :-1]
    change = np.zeros_like(pos)
    change[:, 1:] = np.abs(pos[:, 1:] - pos[:, :-1])
    change[:, 0] = np.abs(pos[:, 0])
    net = held * np.nan_to_num(rets) - change * cost
    net[np.isnan(rets)] = np.nan
    # Bars where no symbol has listed yet average nothing: 0, not a warning.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        pool = np.nanmean(net, axis=0)
    pool = np.nan_to_num(pool)
    return Run(net=net, pool=pool, pos=pos)


def buy_and_hold(rets: np.ndarray, cost: float, start: int) -> Run:
    """Long every symbol from `start` (one entry cost there, or when it lists)."""
    pos = np.where(np.isnan(rets), 0.0, 1.0)
    pos[:, :start] = 0.0
    return run(pos, rets, cost)


# ── Scores over a span of bars [a, b) ─────────────────────────────────────────


def cagr(r: np.ndarray, bars_per_year: float) -> float:
    if len(r) == 0:
        return 0.0
    growth = float(np.sum(np.log1p(np.maximum(r, -0.999999))))
    return math.expm1(growth * bars_per_year / len(r))


def sharpe(r: np.ndarray, bars_per_year: float) -> float:
    if len(r) < 2:
        return 0.0
    sd = float(np.std(r, ddof=1))
    return 0.0 if sd == 0 else float(np.mean(r)) / sd * math.sqrt(bars_per_year)


def max_drawdown(r: np.ndarray) -> float:
    if len(r) == 0:
        return 0.0
    equity = np.exp(np.cumsum(np.log1p(np.maximum(r, -0.999999))))
    peak = np.maximum.accumulate(np.concatenate([[1.0], equity]))[1:]
    return float(np.max(1.0 - equity / peak))


def score(metric: str, r: np.ndarray, bars_per_year: float) -> float:
    if metric == "cagr":
        return cagr(r, bars_per_year)
    if metric == "sharpe":
        return sharpe(r, bars_per_year)
    if metric == "calmar":
        dd = max_drawdown(r)
        return cagr(r, bars_per_year) / max(dd, 1e-6)
    raise ValueError(f"unknown metric {metric!r}")


def trades(pos: np.ndarray, a: int, b: int) -> int:
    """Entries (0 → long) inside [a, b), over all symbols."""
    if b <= a:
        return 0
    seg = pos[:, max(a - 1, 0):b]
    entries = (seg[:, 1:] > seg[:, :-1]).sum() if seg.shape[1] > 1 else 0
    return int(entries)


def folds(a: int, b: int, k: int) -> list[tuple[int, int]]:
    """k consecutive spans covering [a, b)."""
    edges = np.linspace(a, b, k + 1).round().astype(int)
    return [(int(edges[i]), int(edges[i + 1])) for i in range(k) if edges[i + 1] > edges[i]]


# ── The grid ──────────────────────────────────────────────────────────────────


def grid_values(spec: dict) -> list:
    """A ParamSpec (the hypothesis's) as its grid: choices, or min..max by step."""
    if spec.get("choices") is not None:
        return list(spec["choices"])
    lo, hi, step = spec.get("min"), spec.get("max"), spec.get("step")
    if lo is None or hi is None or step is None or step <= 0:
        raise FamilyError(f"{spec.get('name')}: a search needs min, max and a positive step")
    n = int(math.floor((hi - lo) / step + 1e-9)) + 1
    vals = [lo + i * step for i in range(n)]
    # keep integers integers, and floats free of 0.30000000000000004
    if all(float(v).is_integer() for v in (lo, hi, step)):
        return [int(round(v)) for v in vals]
    return [round(v, 10) for v in vals]


def grid(family: str, params: list[dict]) -> tuple[list[str], list[list], list[dict]]:
    """(names, values per name, every combination) for a family's search."""
    need = FAMILIES.get(family)
    if need is None:
        raise FamilyError(f"unknown family {family!r}; this job knows {sorted(FAMILIES)}")
    by_name = {p.get("name"): p for p in params}
    missing = [n for n in need if n not in by_name]
    extra = [n for n in by_name if n not in need]
    if missing or extra:
        raise FamilyError(
            f"{family} tunes {list(need)}; the hypothesis has {sorted(by_name)}"
            + (f" (missing {missing})" if missing else "")
            + (f" (not used: {extra})" if extra else "")
        )
    values = [grid_values(by_name[n]) for n in need]
    combos: list[dict] = [{}]
    for name, vals in zip(need, values):
        combos = [{**c, name: v} for c in combos for v in vals]
    return list(need), values, combos
