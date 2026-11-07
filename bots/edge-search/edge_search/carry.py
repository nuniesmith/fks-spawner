"""The carry family: long spot, short the perpetual, the same coin amount.

Price moves cancel; what's left is the funding the short receives (or pays,
when the rate is negative), the basis moving between the two legs, and fees
on both legs. Per bar held, per unit of notional:

    spot return - perpetual return + the bar's funding

returned on the capital the pair ties up: the spot leg in full plus the
perpetual's margin, 1 + 1/leverage per unit of notional. The leverage is
declared, frozen with the hypothesis, never searched (one choice).

The rule (what the search tunes): the trailing funding rate over `lookback`
bars, a year's worth, enters the pair at or above `enter_apr` and leaves it
below `exit_apr` (hysteresis; every exit threshold at or below every entry
threshold). Decided on a bar's close, held through the next bar.

LIQUIDATION. The short is an isolated position with margin 1/leverage of its
notional at entry; the exchange takes it when the price has risen
1/leverage - MAINT_MARGIN above the entry (its maintenance margin is all
that is left). It is checked on every bar held against the perpetual's
HIGH, so an intraday wick counts. On that bar the short closes at that price
(at the open, if the bar opens beyond it), the remaining maintenance margin
is lost, the spot leg is sold at the close (one leg's fee), and the bar's
funding isn't received. A wick that retraces by the close is the loss: the
short paid the top, the spot sold lower. The rule may enter again on that
close. The baseline (always-carry) and the random benchmark run the same
liquidation, on the same bars.

Not modelled: the venue failing while it holds both legs, margin top-ups
from the spot leg (it sits in another account), moving money between the
two accounts, borrowing, and the perpetual-spot basis inside a bar.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .dataset import CarryPair, align, align_funding, coverage
from .engine import FamilyError, forward_fill, returns

PARAMS = ("lookback", "enter_apr", "exit_apr", "leverage")
BASELINE = "always-carry"
#: The share of notional an exchange keeps when it liquidates: KuCoin's and
#: Binance's maintenance margin for BTC/ETH at small size is 0.4-0.5%; 1%
#: errs toward liquidating sooner and losing more.
MAINT_MARGIN = 0.01


class CarryError(ValueError):
    """The hypothesis can't run as the carry family; the message says why."""


@dataclass
class CarryData:
    symbols: list[str]
    times: np.ndarray  # [bar] open times, ms
    bar_ms: int
    perp_open: np.ndarray  # [symbol, bar]; NaN where the perpetual has no bar
    perp_high: np.ndarray
    perp_close: np.ndarray  # forward-filled after listing
    spot_close: np.ndarray  # forward-filled after listing
    r_perp: np.ndarray
    r_spot: np.ndarray
    funding: np.ndarray  # [symbol, bar]: each bar's settlements
    live: np.ndarray  # [symbol, bar]: both legs listed
    coverage: dict[str, float]

    @property
    def bars_per_year(self) -> float:
        return 365 * 86_400_000 / self.bar_ms


def check_params(combos: list[dict]) -> float:
    """The declared leverage. Refuses a grid that would search the leverage
    or let a pair exit above where it enters."""
    levs = sorted({float(c["leverage"]) for c in combos})
    if len(levs) != 1:
        raise CarryError(f"leverage is declared, not searched: give one choice (the grid has {levs})")
    lev = levs[0]
    if not 1.0 <= lev <= 20.0:
        raise CarryError(f"leverage {lev:g} is outside 1..20")
    if 1.0 / lev <= MAINT_MARGIN:
        raise CarryError(f"at {lev:g}x the margin is gone at entry (maintenance {MAINT_MARGIN:.0%})")
    enters = [float(c["enter_apr"]) for c in combos]
    exits = [float(c["exit_apr"]) for c in combos]
    if max(exits) > min(enters):
        raise CarryError(
            f"every exit threshold must be at or below every entry threshold: exit_apr up to "
            f"{max(exits):g} against enter_apr from {min(enters):g}"
        )
    if min(int(c["lookback"]) for c in combos) < 1:
        raise CarryError("lookback must be at least 1 bar")
    return lev


def prepare(pairs: list[CarryPair], bar_ms: int, spec) -> CarryData:
    perps = [p.perp for p in pairs]
    spots = [p.spot for p in pairs]
    times, pc = align(perps + spots, bar_ms)
    n = len(pairs)
    perp_close_raw, spot_close_raw = pc[:n], pc[n:]
    first = int(times[0])
    perp_open = np.full_like(perp_close_raw, np.nan)
    perp_high = np.full_like(perp_close_raw, np.nan)
    for i, b in enumerate(perps):
        idx = (b.open_ms - first) // bar_ms
        perp_open[i, idx] = b.open
        perp_high[i, idx] = b.high if b.high is not None else np.maximum(b.open, b.close)
    perp_close = forward_fill(perp_close_raw)
    spot_close = forward_fill(spot_close_raw)
    live = ~np.isnan(perp_close) & ~np.isnan(spot_close)
    funding = align_funding(perps, times, bar_ms)
    if funding is None:
        raise CarryError("the perpetual leg has no funding: is the first folder a perpetual's?")
    cov = {p.symbol: min(coverage(p.perp, spec), coverage(p.spot, spec)) for p in pairs}
    return CarryData(
        symbols=[p.symbol for p in pairs], times=times, bar_ms=bar_ms,
        perp_open=perp_open, perp_high=perp_high, perp_close=perp_close, spot_close=spot_close,
        r_perp=returns(perp_close), r_spot=returns(spot_close), funding=funding, live=live, coverage=cov,
    )


def trailing_apr(d: CarryData, lookback: int) -> np.ndarray:
    """Each bar's mean funding over the last `lookback` live bars, a year's
    worth; NaN until a symbol has that many. Known at the bar's close: its
    settlements are all inside it."""
    out = np.full(d.funding.shape, np.nan)
    for i in range(len(d.symbols)):
        live = d.live[i]
        f = np.where(live, d.funding[i], 0.0)
        c = np.cumsum(np.insert(f, 0, 0.0))
        k = np.cumsum(np.insert(live.astype(int), 0, 0))
        for t in np.flatnonzero(live):
            lo = t + 1 - lookback
            if lo < 0 or k[t + 1] - k[lo] < lookback:
                continue
            out[i, t] = (c[t + 1] - c[lo]) / lookback * d.bars_per_year
    return out


def wanted(d: CarryData, lookback: int, enter_apr: float, exit_apr: float) -> np.ndarray:
    """The rule's position (1 = in the pair), decided on each bar's close."""
    apr = trailing_apr(d, lookback)
    out = np.zeros(apr.shape)
    for i in range(len(d.symbols)):
        state = 0.0
        for t in range(apr.shape[1]):
            a = apr[i, t]
            if np.isnan(a):
                state = 0.0
            elif a >= enter_apr:
                state = 1.0
            elif a < exit_apr:
                state = 0.0
            out[i, t] = state
    return out


@dataclass
class Sim:
    net: np.ndarray  # [symbol, bar]: return on capital, NaN before both legs list
    pool: np.ndarray  # [bar]
    pos: np.ndarray  # [symbol, bar]: held after each bar's close (liquidations included)
    liquidated: np.ndarray  # [symbol, bar]: 1 on a bar the short was liquidated
    received: np.ndarray  # [symbol, bar]: funding received on bars held (negative = paid)


def simulate(d: CarryData, want: np.ndarray, leg_cost: float, leverage: float) -> Sim:
    cap = 1.0 + 1.0 / leverage
    rise = 1.0 / leverage - MAINT_MARGIN
    shape = want.shape
    net = np.full(shape, np.nan)
    pos = np.zeros(shape)
    liq = np.zeros(shape)
    received = np.zeros(shape)
    for i in range(shape[0]):
        held, entry = 0.0, np.nan
        for t in range(shape[1]):
            if not d.live[i, t]:
                continue
            pnl = 0.0
            if held and t > 0 and not np.isnan(d.r_spot[i, t]) and not np.isnan(d.r_perp[i, t]):
                level = entry * (1.0 + rise)
                hi, op, prev = d.perp_high[i, t], d.perp_open[i, t], d.perp_close[i, t - 1]
                if not np.isnan(hi) and hi >= level:
                    fill = max(level, op) if not np.isnan(op) else level
                    pnl = d.r_spot[i, t] - (fill / prev - 1.0) - MAINT_MARGIN - leg_cost
                    liq[i, t] = 1.0
                    held, entry = 0.0, np.nan
                else:
                    pnl = d.r_spot[i, t] - d.r_perp[i, t] + d.funding[i, t]
                    received[i, t] = d.funding[i, t]
            w = want[i, t]
            if w != held:
                pnl -= 2.0 * leg_cost  # both legs: buy spot and sell the perpetual, or the reverse
                held = w
                entry = d.perp_close[i, t] if w else np.nan
            net[i, t] = pnl / cap
            pos[i, t] = held
    with np.errstate(all="ignore"):
        counts = np.sum(~np.isnan(net), axis=0)
        pool = np.where(counts > 0, np.nansum(net, axis=0) / np.maximum(counts, 1), 0.0)
    return Sim(net=net, pool=pool, pos=pos, liquidated=liq, received=received)


def always(d: CarryData, start: int) -> np.ndarray:
    """always-carry: in the pair on every live bar's close from `start` on."""
    out = d.live.astype(float)
    out[:, :start] = 0.0
    return out


def report(d: CarryData, sim: Sim, hold: int, leverage: float) -> dict:
    held_bars = np.zeros_like(sim.pos)
    held_bars[:, 1:] = sim.pos[:, :-1]
    bpy = d.bars_per_year

    def per_year(a: int) -> float:
        n = float(np.sum(held_bars[:, a:]))
        return float(np.sum(sim.received[:, a:])) / n * bpy if n else 0.0

    return {
        "leverage": leverage,
        "maint_margin": MAINT_MARGIN,
        "capital_per_notional": 1.0 + 1.0 / leverage,
        "funding_received_per_year_in_pair": per_year(0),
        "holdout_funding_received_per_year_in_pair": per_year(hold),
        "liquidations": int(sim.liquidated.sum()),
        "holdout_liquidations": int(sim.liquidated[:, hold:].sum()),
        "mean_funding_apr": {
            s: float(d.funding[i, d.live[i]].sum() / max(int(d.live[i].sum()), 1) * bpy)
            for i, s in enumerate(d.symbols)
        },
    }


def params_of(p: dict) -> tuple[int, float, float]:
    try:
        return int(p["lookback"]), float(p["enter_apr"]), float(p["exit_apr"])
    except (KeyError, TypeError, ValueError) as e:
        raise FamilyError(f"carry tunes {list(PARAMS)}: {e}") from None
