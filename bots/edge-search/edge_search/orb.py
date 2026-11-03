"""The `orb` family: an opening-range breakout on CME futures, one trade per
session, judged on a session grid (one bar per trading day).

The rules are fixed here, so they are part of every `orb` hypothesis:

  session     ES/MES/NQ/MNQ 09:30-16:00, GC/MGC 08:20-13:30, New York time
              (DST-aware). The 08:20 / 09:30 anchors are the ones the
              2026-09-03 Rithmic check confirmed against exchange bars.
  range       high and low of the session's first `orb_minutes` minutes; all
              of them must have a bar
  entry       the first 1-minute CLOSE beyond the range, either side, at that
              close plus slippage; at most one trade per session
  stop        the other side of the range
  exit        `target`: a limit at entry ± target_r × risk, filled only when a
              bar trades a tick through it. `trail`: touching that level moves
              the stop to 1R below the best price since (so at least
              target_r − 1 R is kept), trailing; held to the close otherwise.
              Either way: still open at the session's last bar → out at its
              close (market, slipped)
  fills       a stop that the bar OPENS through fills at the open; a bar that
              touches both the stop and the target is the stop (pessimistic);
              stop and close exits pay slippage, a target limit doesn't
  no trade    roll days and days the stitched series interleaves two
              contracts (the lake's rolls.csv / roll_flips.csv), US exchange
              holidays, and sessions with fewer than 98% of their minutes
  costs       commission per contract round turn ($) and slippage in ticks per
              side, from the hypothesis; the bps fields must be 0
  return      the trade's P&L over the notional at entry (one contract), so
              roots with different contract sizes pool as equal weights

The baseline is `session-hold`: long from each session's first bar to its
last, every tradeable session, with the same costs. It is the intraday drift
an intraday rule could be riding. (A 24-hour buy-and-hold isn't offered: the
lake's series are unadjusted, so every roll would show as a return.)

U8's random strategies keep each symbol's holdout trade count and the trades'
holding times, at random sessions (one per session), random minutes after
the opening range, and a random side, paying the same costs.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np

from .dataset import DatasetSpec, MinuteBars

ET = ZoneInfo("America/New_York")
PARAMS = ("orb_minutes", "target_r", "exit")
EXITS = ("target", "trail")
BASELINE = "session-hold"
COMPLETE_SHARE = 0.98  # a session is tradeable with at least this share of its minutes
PRESENT_SHARE = 0.5  # and counts as present (U1) with this much: early closes are present
SESSIONS_PER_YEAR = 252


class OrbError(ValueError):
    """The hypothesis can't be searched by this family; the message says why."""


@dataclass(frozen=True)
class Contract:
    root: str  # the lake folder's series this contract trades
    tick: float
    point_value: float  # $ per point
    start: time  # New York time
    end: time


_EQ = (time(9, 30), time(16, 0))
_GOLD = (time(8, 20), time(13, 30))
CONTRACTS = {
    "ES": Contract("ES", 0.25, 50.0, *_EQ),
    "MES": Contract("ES", 0.25, 5.0, *_EQ),
    "NQ": Contract("NQ", 0.25, 20.0, *_EQ),
    "MNQ": Contract("NQ", 0.25, 2.0, *_EQ),
    "GC": Contract("GC", 0.10, 100.0, *_GOLD),
    # Micro gold trades on gold's prices, like MES/MNQ on ES/NQ: the same
    # underlying and contract months, one tenth the size. The Databento
    # decade (2014-24) has no MGC series at all, and GC's is the liquid one.
    "MGC": Contract("GC", 0.10, 10.0, *_GOLD),
}


def contract(symbol: str) -> Contract:
    try:
        return CONTRACTS[symbol]
    except KeyError:
        raise OrbError(f"orb trades {sorted(CONTRACTS)}; {symbol!r} isn't one") from None


# ── US exchange holidays (full closures; CME equity and COMEX follow them) ────


def _easter(y: int) -> date:
    a, b, c = y % 19, y // 100, y % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l_ = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l_) // 451
    month = (h + l_ - 7 * m + 114) // 31
    return date(y, month, (h + l_ - 7 * m + 114) % 31 + 1)


def _nth(y: int, month: int, weekday: int, n: int) -> date:
    """The n-th `weekday` (Mon = 0) of a month; n = -1 is the last."""
    if n > 0:
        d = date(y, month, 1)
        return d + timedelta((weekday - d.weekday()) % 7 + 7 * (n - 1))
    d = (date(y, month + 1, 1) if month < 12 else date(y + 1, 1, 1)) - timedelta(1)
    return d - timedelta((d.weekday() - weekday) % 7)


def _observed(d: date) -> date:
    return d - timedelta(1) if d.weekday() == 5 else d + timedelta(1) if d.weekday() == 6 else d


#: One-off market closures (Hurricane Sandy, two state funerals).
SPECIAL_CLOSURES = {date(2012, 10, 29), date(2012, 10, 30), date(2018, 12, 5), date(2025, 1, 9)}


def us_holidays(y: int) -> set[date]:
    out = {
        _nth(y, 1, 0, 3),  # Martin Luther King Jr.
        _nth(y, 2, 0, 3),  # Washington's Birthday
        _easter(y) - timedelta(2),  # Good Friday
        _nth(y, 5, 0, -1),  # Memorial Day
        _observed(date(y, 7, 4)),
        _nth(y, 9, 0, 1),  # Labor Day
        _nth(y, 11, 3, 4),  # Thanksgiving
        _observed(date(y, 12, 25)),
    }
    new_year = date(y, 1, 1)
    if new_year.weekday() == 6:
        out.add(date(y, 1, 2))
    elif new_year.weekday() != 5:  # a Saturday New Year isn't moved to Friday
        out.add(new_year)
    if y >= 2022:
        out.add(_observed(date(y, 6, 19)))  # Juneteenth
    return out | {d for d in SPECIAL_CLOSURES if d.year == y}


# ── Sessions ──────────────────────────────────────────────────────────────────


@dataclass
class Sessions:
    """One symbol's sessions as [session, minute] arrays (NaN = no bar)."""

    symbol: str
    k: Contract
    dates: list[date]
    o: np.ndarray
    h: np.ndarray
    l: np.ndarray
    c: np.ndarray
    present: np.ndarray  # [session] at least PRESENT_SHARE of the minutes
    complete: np.ndarray  # [session] tradeable: COMPLETE_SHARE, no roll, no holiday
    coverage: float  # U1: present sessions over the weekdays expected (not holidays)


def _minutes(k: Contract) -> int:
    return (k.end.hour * 60 + k.end.minute) - (k.start.hour * 60 + k.start.minute)


def build_sessions(symbol: str, bars: MinuteBars, spec: DatasetSpec) -> Sessions:
    k = contract(symbol)
    n_min = _minutes(k)
    holidays: set[date] = set()
    for y in range(spec.start.year, spec.end.year + 1):
        holidays |= us_holidays(y)
    rows: list[tuple[date, np.ndarray, np.ndarray]] = []
    d = spec.start
    while d <= spec.end:
        if d.weekday() < 5:
            s_ms = int(datetime.combine(d, k.start, tzinfo=ET).timestamp() * 1000)
            a = int(np.searchsorted(bars.open_ms, s_ms, "left"))
            b = int(np.searchsorted(bars.open_ms, s_ms + n_min * 60_000, "left"))
            if b > a:
                rows.append((d, np.arange(a, b), (bars.open_ms[a:b] - s_ms) // 60_000))
        d += timedelta(1)
    n = len(rows)
    shape = (n, n_min)
    o, h, lo, c = (np.full(shape, np.nan) for _ in range(4))
    for i, (_, idx, minute) in enumerate(rows):
        o[i, minute], h[i, minute], lo[i, minute], c[i, minute] = (
            bars.open[idx], bars.high[idx], bars.low[idx], bars.close[idx])
    dates = [r[0] for r in rows]
    have = (~np.isnan(c)).sum(axis=1) if n else np.zeros(0)
    present = have >= PRESENT_SHARE * n_min
    blocked = np.array([d in bars.roll_days or d in holidays for d in dates], dtype=bool)
    complete = (have >= COMPLETE_SHARE * n_min) & ~blocked
    first = next((d for d, p in zip(dates, present) if p), spec.start)
    expected = [first + timedelta(j) for j in range((spec.end - first).days + 1)]
    expected = {e for e in expected if e.weekday() < 5 and e not in holidays}
    got = sum(1 for d, p in zip(dates, present) if p and d in expected)
    cov = min(1.0, got / len(expected)) if expected else 0.0
    return Sessions(symbol, k, dates, o, h, lo, c, present, complete, cov)


# ── One trade's exit ──────────────────────────────────────────────────────────


def _first(mask: np.ndarray) -> int | None:
    idx = np.flatnonzero(mask)
    return int(idx[0]) if idx.size else None


def _stop_fill(open_px: float, level: float, side: int, slip: float) -> float:
    """A resting stop: through it at the open fills at the open; else at the level."""
    if side > 0:
        return (open_px if open_px <= level else level) - slip
    return (open_px if open_px >= level else level) + slip


def exit_trade(s: Sessions, i: int, e: int, side: int, entry: float, stop: float, risk: float,
               target_r: float, mode: str, slip: float) -> tuple[int, float]:
    """(exit minute, exit price) for a position opened at the close of minute
    `e` of session `i`. Bars from e + 1 on; the stop resting during a bar is
    the one set before it."""
    o, h, lo, c = s.o[i, e + 1:], s.h[i, e + 1:], s.l[i, e + 1:], s.c[i, e + 1:]
    valid = np.flatnonzero(~np.isnan(c))
    if valid.size == 0:
        return e, float(s.c[i, e]) - side * slip
    last = int(valid[-1])
    target = entry + side * target_r * risk
    tick = s.k.tick
    with np.errstate(invalid="ignore"):
        stop_hit = lo <= stop if side > 0 else h >= stop
        if mode == "target":
            reach = h >= target + tick if side > 0 else lo <= target - tick
        else:
            reach = h >= target if side > 0 else lo <= target
    hs, ht = _first(stop_hit), _first(reach)
    if hs is not None and (ht is None or hs <= ht):
        return e + 1 + hs, _stop_fill(float(o[hs]), stop, side, slip)
    if ht is None:
        return e + 1 + last, float(c[last]) - side * slip
    if mode == "target":
        return e + 1 + ht, target
    # trail: from the bar after the touch, the stop sits 1R behind the best
    # price since the touch (inclusive), never below target - 1R.
    if side > 0:
        best = np.maximum.accumulate(np.where(np.isnan(h[ht:]), -np.inf, h[ht:]))
        trail = best[:-1] - risk
        with np.errstate(invalid="ignore"):
            hit = lo[ht + 1:] <= trail
    else:
        best = np.minimum.accumulate(np.where(np.isnan(lo[ht:]), np.inf, lo[ht:]))
        trail = best[:-1] + risk
        with np.errstate(invalid="ignore"):
            hit = h[ht + 1:] >= trail
    q = _first(hit)
    if q is None:
        return e + 1 + last, float(c[last]) - side * slip
    j = ht + 1 + q
    return e + 1 + j, _stop_fill(float(o[j]), float(trail[q]), side, slip)


# ── The problem the search judges ─────────────────────────────────────────────


@dataclass
class Trade:
    sym: int
    day: int  # grid index
    session: int  # index into the symbol's Sessions
    entry_min: int
    exit_min: int
    side: int
    ret: float
    # One contract, in dollars, net of commission and slippage: the trade's
    # result, and its worst unrealized point (the account path, U13).
    pnl_usd: float = 0.0
    mae_usd: float = 0.0


@dataclass
class OrbData:
    sessions: list[Sessions]
    times: np.ndarray  # the grid: each session date's UTC midnight, ms
    day_of: list[np.ndarray]  # per symbol, its sessions' grid indices
    on_grid: np.ndarray  # [symbol, day] the symbol had a session that day
    commission: float  # $ per contract round turn
    slip_ticks: float


def costs_of(costs: dict) -> tuple[float, float]:
    loose = [k for k in ("maker_bps", "taker_bps", "slippage_bps") if float(costs.get(k, 0) or 0) != 0]
    if loose:
        raise OrbError(
            "orb costs are per contract: commission_usd_rt and slippage_ticks, with "
            f"{', '.join(loose)} at 0 (they would be charged on the notional as well)"
        )
    try:
        commission = float(costs["commission_usd_rt"])
        slip = float(costs["slippage_ticks"])
    except (KeyError, TypeError, ValueError):
        raise OrbError("an orb hypothesis states commission_usd_rt ($ per contract, round turn) "
                       "and slippage_ticks (per side) in its costs") from None
    if commission < 0 or slip < 0:
        raise OrbError("commission_usd_rt and slippage_ticks can't be negative")
    return commission, slip


def prepare(symbols: list[str], series: list[MinuteBars], spec: DatasetSpec, costs: dict) -> OrbData:
    commission, slip = costs_of(costs)
    sessions = [build_sessions(s, b, spec) for s, b in zip(symbols, series)]
    days = sorted({d for s in sessions for d in s.dates})
    if not days:
        raise OrbError("no session in the window has a bar")
    index = {d: j for j, d in enumerate(days)}
    times = np.array(
        [int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000) for d in days],
        dtype=np.int64,
    )
    day_of = [np.array([index[d] for d in s.dates], dtype=np.int64) for s in sessions]
    on_grid = np.zeros((len(sessions), len(days)), dtype=bool)
    for i, j in enumerate(day_of):
        on_grid[i, j] = True
    return OrbData(sessions, times, day_of, on_grid, commission, slip)


def check_params(combos: list[dict]) -> None:
    for p in combos:
        m, r, x = p["orb_minutes"], p["target_r"], p["exit"]
        if not isinstance(m, int) or isinstance(m, bool) or m < 1:
            raise OrbError(f"orb_minutes must be whole minutes ≥ 1, not {m!r}")
        if not isinstance(r, (int, float)) or isinstance(r, bool) or r <= 0:
            raise OrbError(f"target_r must be a number above 0, not {r!r}")
        if x not in EXITS:
            raise OrbError(f"exit must be one of {list(EXITS)}, not {x!r}")


def trades_for(data: OrbData, params: dict) -> list[Trade]:
    m, target_r, mode = int(params["orb_minutes"]), float(params["target_r"]), params["exit"]
    out: list[Trade] = []
    for si, s in enumerate(data.sessions):
        n_min = s.c.shape[1]
        if m >= n_min - 1:
            raise OrbError(f"orb_minutes {m} leaves no time to trade in {s.symbol}'s {n_min}-minute session")
        if not len(s.dates):
            continue
        slip = data.slip_ticks * s.k.tick
        window = ~np.isnan(s.c[:, :m]).any(axis=1)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            hi = np.nanmax(s.h[:, :m], axis=1)
            lo = np.nanmin(s.l[:, :m], axis=1)
        for i in np.flatnonzero(s.complete & window):
            seg = s.c[i, m:n_min - 1]
            with np.errstate(invalid="ignore"):
                up, dn = seg > hi[i], seg < lo[i]
            j = _first(up | dn)
            if j is None:
                continue
            e = m + j
            side = 1 if up[j] else -1
            entry = float(s.c[i, e]) + side * slip
            stop = float(lo[i]) if side > 0 else float(hi[i])
            risk = abs(entry - stop)
            if risk <= 0:
                continue
            x, px = exit_trade(s, int(i), e, side, entry, stop, risk, target_r, mode, slip)
            pv = s.k.point_value
            ret = side * (px - entry) / entry - data.commission / (entry * pv)
            pnl = side * (px - entry) * pv - data.commission
            # The worst price between entry and exit (inclusive of the exit
            # bar), against the position: what the account saw open.
            if x > e:
                worst = np.nanmin(s.l[i, e + 1:x + 1]) if side > 0 else np.nanmax(s.h[i, e + 1:x + 1])
                adverse = min(0.0, side * (float(worst) - entry) * pv)
            else:
                adverse = 0.0
            mae = min(adverse - data.commission, pnl)
            out.append(Trade(si, int(data.day_of[si][i]), int(i), e, x, side, ret, pnl, mae))
    return out


def daily(data: OrbData, trades: list[Trade]) -> tuple[np.ndarray, np.ndarray]:
    """(net[symbol, day], entries[symbol, day]): 0 on a session day without a
    trade, NaN on a day the symbol has no session."""
    net = np.where(data.on_grid, 0.0, np.nan)
    entries = np.zeros(data.on_grid.shape)
    for t in trades:
        net[t.sym, t.day] = t.ret
        entries[t.sym, t.day] = 1.0
    return net, entries


def pool(net: np.ndarray) -> np.ndarray:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        return np.nan_to_num(np.nanmean(net, axis=0))


def session_hold(data: OrbData) -> np.ndarray:
    """The baseline's pool: long each tradeable session from its first bar's
    open to its last bar's close, with the same costs."""
    net = np.where(data.on_grid, 0.0, np.nan)
    for si, s in enumerate(data.sessions):
        slip = data.slip_ticks * s.k.tick
        for i in np.flatnonzero(s.complete):
            ok = np.flatnonzero(~np.isnan(s.c[i]))
            entry = float(s.o[i, ok[0]]) + slip
            exit_ = float(s.c[i, ok[-1]]) - slip
            net[si, data.day_of[si][i]] = (exit_ - entry) / entry - data.commission / (entry * s.k.point_value)
    return pool(net)


def session_closes(data: OrbData) -> np.ndarray:
    """[symbol, day] each session's last close, for the regime split."""
    out = np.full(data.on_grid.shape, np.nan)
    for si, s in enumerate(data.sessions):
        for i in range(len(s.dates)):
            ok = np.flatnonzero(~np.isnan(s.c[i]))
            if ok.size:
                out[si, data.day_of[si][i]] = s.c[i, ok[-1]]
    return out


def _ffill(row: np.ndarray) -> np.ndarray:
    valid = ~np.isnan(row)
    idx = np.where(valid, np.arange(len(row)), 0)
    np.maximum.accumulate(idx, out=idx)
    return row[idx]


def random_pools(data: OrbData, trades: list[Trade], params: dict, hold: int,
                 rng: np.random.Generator, draws: int):
    """Yield the pool of `draws` random strategies with the strategy's holdout
    trades: per symbol the same count and holding times, at random tradeable
    holdout sessions (one each), random minutes after the opening range, and
    random sides, paying the same costs."""
    m = int(params["orb_minutes"])
    held = [t for t in trades if t.day >= hold]
    plans = []
    for si, s in enumerate(data.sessions):
        mine = [t for t in held if t.sym == si]
        if not mine:
            plans.append(None)
            continue
        cand = np.array([i for i in np.flatnonzero(s.complete) if data.day_of[si][i] >= hold], dtype=np.int64)
        closes = np.vstack([_ffill(s.c[i]) for i in cand]) if cand.size else np.zeros((0, s.c.shape[1]))
        lengths = np.array([max(1, t.exit_min - t.entry_min) for t in mine], dtype=np.int64)
        plans.append((cand, closes, lengths))
    for _ in range(draws):
        net = np.where(data.on_grid, 0.0, np.nan)
        for si, plan in enumerate(plans):
            if plan is None:
                continue
            cand, closes, lengths = plan
            s = data.sessions[si]
            n_min = s.c.shape[1]
            k = min(len(lengths), len(cand))
            if k == 0:
                continue
            pick = rng.choice(len(cand), size=k, replace=False)
            ln = np.minimum(rng.permutation(lengths)[:k], n_min - 1 - m)
            ln = np.maximum(ln, 1)
            start = m + (rng.random(k) * (n_min - m - ln)).astype(np.int64)
            side = np.where(rng.random(k) < 0.5, 1.0, -1.0)
            slip = data.slip_ticks * s.k.tick
            entry = closes[pick, start] + side * slip
            exit_ = closes[pick, start + ln] - side * slip
            ret = side * (exit_ - entry) / entry - data.commission / (entry * s.k.point_value)
            days = data.day_of[si][cand[pick]]
            net[si, days] = np.where(np.isfinite(ret), ret, 0.0)
        yield pool(net)


def regime_lookback_sessions(days: int) -> int:
    return max(1, round(days * SESSIONS_PER_YEAR / 365))


def coverage(data: OrbData) -> dict[str, float]:
    return {s.symbol: s.coverage for s in data.sessions}


# ── The account a prop firm judges (U13) ──────────────────────────────────────


def account_path(data: OrbData, trades: list[Trade], start: int, contracts: int,
                 account_usd: float, trail_usd: float) -> dict:
    """A fresh account from grid day `start` on, trading `contracts` of each
    symbol, against a trailing floor: account_usd - trail_usd at first,
    rising with the best end-of-day balance and stopping at account_usd
    (TPT's rule). Breached the first time a day's worst point (the balance
    before it plus every trade's worst excursion that day, as if all at
    once: conservative) or its close is at or under the floor."""
    days = len(data.times)
    pnl = np.zeros(days)
    worst = np.zeros(days)
    for t in trades:
        if t.day >= start:
            pnl[t.day] += t.pnl_usd * contracts
            worst[t.day] += t.mae_usd * contracts
    balance = peak = float(account_usd)
    floor = account_usd - trail_usd
    headroom, breach, low_day = float("inf"), None, None
    for j in range(start, days):
        low = balance + worst[j]
        room = min(low, balance + pnl[j]) - floor
        if room < headroom:
            headroom, low_day = room, j
        if room <= 0 and breach is None:
            breach = j
        balance += pnl[j]
        peak = max(peak, balance)
        floor = min(account_usd, peak - trail_usd)
    day = lambda j: None if j is None else str(np.datetime64(int(data.times[j]), "ms").astype("datetime64[D]"))  # noqa: E731
    return {"breached": breach is not None, "breach_day": day(breach), "worst_headroom": headroom,
            "worst_day": day(low_day), "final_balance": balance, "floor_locked": floor >= account_usd}
