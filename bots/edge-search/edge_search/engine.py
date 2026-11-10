"""The backtest engine: one strategy family, scored net of costs on a pool.

Positions are decided on a bar's close and earn the next bar's return; a
change of position pays `cost` (taker + slippage, per side) on the bar it
happens. On a perpetual, a position held through a bar also pays that bar's
funding (dataset.align_funding: the settlements inside its (open, close]),
and receives it when the rate is negative. A pool is the equal-weight
average of the symbols that have a bar at that time, so a coin that lists
late joins the pool when it lists.

Families
--------
trend-band (the live rule's family, EDGE_PROCESS §2.4's incumbent):
    long while close > SMA(lookback) × (1 + band), flat once
    close < SMA(lookback) × (1 − band), unchanged in between (hysteresis).
    Params: lookback (bars), band (fraction).
trend-vol (the same signal, volatility-managed; Moreira & Muir 2017):
    while trend-band is long, hold min(1, target_vol / realised vol), the
    vol being the annualised standard deviation of the last `vol_lookback`
    log returns, known at the deciding close. Entries and exits act at once;
    a resize only when the new size differs from the held one by more than
    `rebalance` (each change pays its cost on the change's size). Params:
    lookback, band, vol_lookback (bars), target_vol (a year's, a fraction),
    rebalance (a fraction of full size).
"""

from __future__ import annotations

import math
import warnings
from dataclasses import dataclass

import numpy as np

FAMILIES = {
    "trend-band": ("lookback", "band"),
    # A session family (edge_search/orb.py): judged on a session grid, not
    # through `positions`.
    "orb": ("orb_minutes", "target_r", "exit"),
    # A hedged pair (edge_search/carry.py): long spot, short the perpetual.
    "carry": ("lookback", "enter_apr", "exit_apr", "leverage"),
    # trend-band's signal, sized by volatility.
    "trend-vol": ("lookback", "band", "vol_lookback", "target_vol", "rebalance"),
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


def realised_vol(closes: np.ndarray, n: int, bars_per_year: float) -> np.ndarray:
    """Annualised standard deviation of each bar's last n log returns, NaN
    until a symbol has n of them. Known at that bar's close."""
    out = np.full_like(closes, np.nan)
    lr = np.full_like(closes, np.nan)
    with np.errstate(divide="ignore", invalid="ignore"):
        lr[:, 1:] = np.log(closes[:, 1:] / closes[:, :-1])
    for i, row in enumerate(lr):
        valid = ~np.isnan(row)
        if valid.sum() < n:
            continue
        first = int(np.argmax(valid))
        seg = row[first:]
        c1 = np.cumsum(np.insert(np.nan_to_num(seg), 0, 0.0))
        c2 = np.cumsum(np.insert(np.nan_to_num(seg) ** 2, 0, 0.0))
        for t in range(n - 1, len(seg)):
            s1 = c1[t + 1] - c1[t + 1 - n]
            s2 = c2[t + 1] - c2[t + 1 - n]
            var = (s2 - s1 * s1 / n) / (n - 1) if n > 1 else 0.0
            out[i, first + t] = math.sqrt(max(var, 0.0) * bars_per_year)
    return out


def vol_sized(signal: np.ndarray, vol: np.ndarray, target_vol: float, rebalance: float) -> np.ndarray:
    """Fractional positions: `signal` (0/1) sized to min(1, target/vol), held
    until the target moves by more than `rebalance`. No position while the
    vol isn't known yet."""
    out = np.zeros_like(signal)
    for i in range(signal.shape[0]):
        held = 0.0
        for t in range(signal.shape[1]):
            v = vol[i, t]
            if signal[i, t] <= 0 or np.isnan(v):
                held = 0.0
            else:
                want = min(1.0, target_vol / v) if v > 0 else 1.0
                if held == 0.0 or abs(want - held) > rebalance:
                    held = want
            out[i, t] = held
    return out


def check_vol_params(combos: list[dict]) -> None:
    """trend-vol's grid must make sense before a trial is spent."""
    for c in combos:
        if int(c["vol_lookback"]) < 2:
            raise FamilyError("vol_lookback must be at least 2 bars")
        if not float(c["target_vol"]) > 0:
            raise FamilyError("target_vol must be above 0 (a year's volatility, e.g. 0.6)")
        if not 0.0 <= float(c["rebalance"]) < 1.0:
            raise FamilyError("rebalance must be at least 0 and below 1 (a fraction of full size)")


def positions(family: str, closes: np.ndarray, params: dict, bars_per_year: float = 365.0) -> np.ndarray:
    if family == "trend-band":
        return trend_band_positions(closes, int(params["lookback"]), float(params["band"]))
    if family == "trend-vol":
        signal = trend_band_positions(closes, int(params["lookback"]), float(params["band"]))
        vol = realised_vol(closes, int(params["vol_lookback"]), bars_per_year)
        return vol_sized(signal, vol, float(params["target_vol"]), float(params["rebalance"]))
    if family == "orb":
        raise FamilyError("orb trades sessions, not bar positions (edge_search/orb.py)")
    if family == "carry":
        raise FamilyError("carry trades a hedged pair, not bar positions (edge_search/carry.py)")
    raise FamilyError(f"unknown family {family!r}; this job knows {sorted(FAMILIES)}")


@dataclass
class Run:
    """One parameter set over the whole series: net returns per symbol and
    bar, the pool's net return per bar, and the positions."""

    net: np.ndarray  # [symbol, bar]
    pool: np.ndarray  # [bar]
    pos: np.ndarray  # [symbol, bar]


def run(pos: np.ndarray, rets: np.ndarray, cost: float, funding: np.ndarray | None = None) -> Run:
    held = np.zeros_like(pos)
    held[:, 1:] = pos[:, :-1]
    change = np.zeros_like(pos)
    change[:, 1:] = np.abs(pos[:, 1:] - pos[:, :-1])
    change[:, 0] = np.abs(pos[:, 0])
    net = held * np.nan_to_num(rets) - change * cost
    if funding is not None:
        # a long pays a positive rate, a short receives it
        net = net - held * funding
    net[np.isnan(rets)] = np.nan
    # Bars where no symbol has listed yet average nothing: 0, not a warning.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        pool = np.nanmean(net, axis=0)
    pool = np.nan_to_num(pool)
    return Run(net=net, pool=pool, pos=pos)


def buy_and_hold(rets: np.ndarray, cost: float, start: int, funding: np.ndarray | None = None) -> Run:
    """Long every symbol from `start` (one entry cost there, or when it lists);
    on a perpetual, paying its funding all the way."""
    pos = np.where(np.isnan(rets), 0.0, 1.0)
    pos[:, :start] = 0.0
    return run(pos, rets, cost, funding)


def funding_paid(pos: np.ndarray, funding: np.ndarray, bars_per_year: float) -> float:
    """What holding these positions paid in funding, per year in the market
    (a fraction of the position; negative when it was received)."""
    held = np.zeros_like(pos)
    held[:, 1:] = pos[:, :-1]
    in_market = float(np.sum(np.abs(held)))
    return float(np.sum(held * funding)) / in_market * bars_per_year if in_market else 0.0


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


def entries_of(pos: np.ndarray) -> np.ndarray:
    """1 where a symbol goes from flat to a position: an entry. A resize of a
    held (fractional) position is not one."""
    out = np.zeros_like(pos)
    out[:, 1:] = (pos[:, 1:] > 0) & (pos[:, :-1] <= 0)
    out[:, 0] = pos[:, 0] > 0
    return out


def trades(pos: np.ndarray, a: int, b: int) -> int:
    """Entries (flat → long) inside [a, b), over all symbols."""
    if b <= a:
        return 0
    seg = pos[:, max(a - 1, 0):b]
    entries = ((seg[:, 1:] > 0) & (seg[:, :-1] <= 0)).sum() if seg.shape[1] > 1 else 0
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
