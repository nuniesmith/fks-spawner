"""The signal runner (EDGE_PROCESS §6): an orb deployment's sessions as they
happen, told to the trader in time to act.

On a prop account a human places every order (fks 018 refuses
execution='auto'), so "live" for an orb edge is this: a paper instance of
`bot.edge-signal`, bound to one deployment, on Massive's real-time futures
feed. Every minute bar, it re-runs the SEARCHED code (orb.trades_for and
orb.exit_trade, unchanged) on the session so far and says what changed:

  range   the opening range is set: its high and low, where entry would be;
  entry   a 1-minute close beyond the range: side, price, stop, target;
  exit    the stop, the target, the trailing stop, or the session's close.

Each one is written to the signal ledger (fks 031) BEFORE it is sent, once
per deployment, session, symbol and kind, so a restart never repeats one,
then posted to the deployment's Discord channel (Settings -> Notifications,
the template's `channel` field) and marked delivered. While the deployment
is at forward the messages say PAPER; once it is live they say LIVE.

THE SESSION SO FAR. The search's code reads a whole session. To ask it about
part of one without changing it, the runner gives it the bars so far plus
one placeholder bar after the last: flat at the last close. That bar can't
trigger a stop, a target or a trail that the last real bar didn't already
(a close beyond a level means the bar's range crossed it), so an exit the
code places ON the placeholder means "still open". After the session's end
the placeholder is dropped and the code's close-out is the exit.

WHAT IT CAN'T KNOW YET. The search skips a session with under 98% of its
minutes; live, that is only known at the close, so the runner trades the
session and says so in the exit if it ended short. What IS known in advance
isn't traded, like the search: roll days (the contract changes), US
exchange holidays, and the early closes that cut a session short (the day
after Thanksgiving and Christmas Eve, and July 3 when it's a Monday to
Thursday: CME's equity products stop at 13:15 ET, metals at 13:45 ET, so
the 16:00 ET equity session is short and the 13:30 ET gold one isn't).

LATE. A signal is meant to be acted on: one said more than 2 minutes after
its bar closed (a runner started, or restarted, mid-session finds the
morning already decided) is recorded, marked LATE in the message and the
ledger's reason, and says not to chase it.

RETIRED MID-SESSION. A deployment retired while a trade is open (by the
monitor or by hand) keeps its runner watching that trade until it exits,
saying only exits (fks 032 admits an exit for an entry already said); then
the runner goes idle.

WHAT IT REPORTS. fks_signal_error is 1 when the last step failed, when
Massive refused the key, or when a channel was named but its webhook didn't
arrive (a misspelled or deleted channel: signals would only be logged);
fks_signal_feed_stale is 1 inside a session that is being traded while no
bar has come for 3 minutes. Both have alert rules (fks
prometheus/alerts/edge-ladder.yml).

THE CONTRACT. Each root follows the contract with the most volume on the
previous weekday, among the three nearest (the CME recorder's rule), read
from Massive's REST API at the start of each day; a day whose contract
differs from the previous day's is a roll day.

Settings (environment):
  EDGE_RUNNER=signal            selects this program in the image
  EDGE_DEPLOYMENT_ID            the deployment; required, forward or live
  FKS_DB_URL                    the research role, set by the spawner
  MASSIVE_API_KEY               Settings -> API keys, provider `massive`
                                (a real-time futures plan: Futures Advanced)
  SIGNAL_DISCORD_WEBHOOK_URL    from Settings -> Notifications; without it
                                the signals are recorded and logged only
  SIGNAL_CHANNEL                that channel's name (the template sets both),
                                so a channel that didn't resolve is an error
  FKS_CODE_REVISION             baked into the image
  MASSIVE_WS_URL                default wss://socket.massive.com/futures
  MASSIVE_BASE_URL              default https://api.massive.com
  MASSIVE_PER_MINUTE            REST requests per minute (default 5)
  FKS_BOT_ID, STATUS_PORT       as the forward runner
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal as signals
import sys
import time as clock
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
from typing import Callable, Mapping

import numpy as np

from . import orb
from .dataset import DatasetSpec, MinuteBars

log = logging.getLogger("edge-signal")

ET = orb.ET
RUN_STAGES = ("forward", "live")
#: A signal said this long after its bar closed is LATE (module docs).
LATE_MS = 120_000
#: Inside a traded session, this long without a bar is a stale feed.
FEED_STALE_S = 180
#: How long to wait before asking Massive again after it refused the key.
REFUSED_RETRY_S = 1800
WS_URL = "wss://socket.massive.com/futures"
BASE_URL = "https://api.massive.com"
USER_AGENT = "fks-edge-signal/1 (+https://github.com/nuniesmith/fks-spawner)"


class SignalError(ValueError):
    """The runner can't run; the message says why."""


# ── Pure: what the searched code says about the session so far ──────────────


@dataclass(frozen=True)
class Event:
    kind: str  # range | entry | exit
    symbol: str
    day: date
    bar_ms: int  # the bar it was decided on (start, UTC ms)
    side: int | None = None
    price: float | None = None
    stop: float | None = None
    target: float | None = None
    reason: str = ""


@dataclass
class Session:
    """What evaluate() saw: the events so far, and whether a trade is open."""

    events: list[Event]
    open_trade: bool = False
    over: bool = False


def session_bounds(symbol: str, day: date) -> tuple[int, int]:
    """The session's first bar and its end (UTC ms), New York time."""
    k = orb.contract(symbol)
    s = int(datetime.combine(day, k.start, tzinfo=ET).timestamp() * 1000)
    e = int(datetime.combine(day, k.end, tzinfo=ET).timestamp() * 1000)
    return s, e


def _bars(root: str, rows: list[tuple]) -> MinuteBars:
    a = np.array(sorted(rows), dtype=float).reshape(-1, 6) if rows else np.zeros((0, 6))
    return MinuteBars(root=root, open_ms=a[:, 0].astype(np.int64), open=a[:, 1], high=a[:, 2],
                      low=a[:, 3], close=a[:, 4], volume=a[:, 5], roll_days=set())


def evaluate(symbol: str, day: date, rows: list[tuple], params: dict, costs: dict,
             now_ms: int) -> Session:
    """The session so far, through orb's own code. `rows` are today's
    1-minute bars (open_ms, o, h, l, c, v) of the contract followed."""
    k = orb.contract(symbol)
    start_ms, end_ms = session_bounds(symbol, day)
    rows = [r for r in rows if start_ms <= r[0] < end_ms]
    if not rows:
        return Session([])
    spec = DatasetSpec(path="live", interval="1m", start=day, end=day)
    s = orb.build_sessions(symbol, _bars(k.root, rows), spec)
    if not s.dates:
        return Session([])
    n_min = s.c.shape[1]
    over = now_ms >= end_ms
    valid = np.flatnonzero(~np.isnan(s.c[0]))
    last = int(valid[-1])
    m, target_r, mode = int(params["orb_minutes"]), float(params["target_r"]), params["exit"]
    # The range needs a real bar in each of its minutes (the search's rule),
    # checked before the placeholder could stand in for one.
    if np.isnan(s.c[0, :m]).any():
        return Session([], over=over)
    placeholder = None
    if not over and last + 1 < n_min:
        placeholder = last + 1
        for arr in (s.o, s.h, s.l, s.c):
            arr[0, placeholder] = s.c[0, last]
    s.complete[:] = True  # 98% is only known at the close (module docs)
    commission, slip_ticks = orb.costs_of(costs)
    slip = slip_ticks * k.tick
    data = orb.OrbData([s], np.zeros(1, dtype=np.int64), [np.zeros(1, dtype=np.int64)],
                       np.ones((1, 1), dtype=bool), commission, slip_ticks)
    bar = lambda minute: start_ms + int(minute) * 60_000  # noqa: E731
    events: list[Event] = []
    hi, lo = float(np.nanmax(s.h[0, :m])), float(np.nanmin(s.l[0, :m]))
    events.append(Event("range", symbol, day, bar(m - 1), price=None, stop=None, target=None,
                        reason=f"{m}-minute range {_px(lo, k)}–{_px(hi, k)}: an entry is a "
                               f"1-minute close beyond it"))
    trades = orb.trades_for(data, params)
    if not trades:
        return Session(events, over=over)
    t = trades[0]
    side, e = t.side, t.entry_min
    entry = float(s.c[0, e]) + side * slip
    stop = lo if side > 0 else hi
    risk = abs(entry - stop)
    target = entry + side * target_r * risk
    how = "target limit" if mode == "target" else "trail from the target"
    events.append(Event("entry", symbol, day, bar(e), side, entry, stop, target,
                        reason=f"close {_px(float(s.c[0, e]), k)} beyond the range; "
                               f"{target_r:g}R {how}"))
    x, px = orb.exit_trade(s, 0, e, side, entry, stop, risk, target_r, mode, slip)
    if placeholder is not None and x == placeholder:
        return Session(events, open_trade=True, over=False)
    events.append(Event("exit", symbol, day, bar(x), side, float(px), stop, target,
                        reason=_exit_reason(s, e, x, side, float(px), stop, target, mode, slip,
                                            over and x == last, k, n_min, len(valid))))
    return Session(events, over=over)


def _exit_reason(s, e, x, side, px, stop, target, mode, slip, closed, k, n_min, have) -> str:
    short = "" if have >= orb.COMPLETE_SHARE * n_min else (
        f" (the session had {have} of {n_min} minutes: the search wouldn't have traded it)")
    if closed and px == float(s.c[0, x]) - side * slip:
        return "the session's close" + short
    if mode == "target" and px == target:
        return "the target" + short
    hi = s.h[0, e + 1:x]
    lo = s.l[0, e + 1:x]
    with np.errstate(invalid="ignore"):
        touched = (np.nanmax(hi) >= target) if side > 0 and hi.size else (
            (np.nanmin(lo) <= target) if lo.size else False)
    if mode == "trail" and touched:
        return "the trailing stop" + short
    return "the stop" + short


def _px(v: float, k: orb.Contract) -> str:
    """A price to the contract's tick: 0.25 -> two decimals, 0.10 -> one."""
    decimals = len(f"{k.tick:g}".partition(".")[2])
    return f"{v:.{decimals}f}"


def message(ev: Event, contract: str, stage: str) -> str:
    """The Discord line for one event: what to do, in one glance."""
    tag = "LIVE" if stage == "live" else "PAPER"
    k = orb.contract(ev.symbol)
    at = datetime.fromtimestamp(ev.bar_ms / 1000, timezone.utc).astimezone(ET).strftime("%H:%M ET")
    if ev.kind == "range":
        return f"[{tag}] {ev.symbol} ({contract}) {ev.reason} (bar {at})"
    side = "LONG" if ev.side and ev.side > 0 else "SHORT"
    if ev.kind == "entry":
        return (f"[{tag}] {ev.symbol} ({contract}) ENTER {side} ~{_px(ev.price, k)} "
                f"stop {_px(ev.stop, k)} target {_px(ev.target, k)}: {ev.reason} (bar {at})")
    return f"[{tag}] {ev.symbol} ({contract}) EXIT {side} ~{_px(ev.price, k)}: {ev.reason} (bar {at})"


# ── Pure: which contract a root follows today ───────────────────────────────


def choose(order: list[str], prev_volume: Mapping[str, float]) -> str | None:
    """The most-traded of `order` (nearest first) on the previous weekday;
    the nearest when nothing traded."""
    if not order:
        return None
    best = max(order, key=lambda t: (prev_volume.get(t, 0.0), -order.index(t)))
    return best if prev_volume.get(best, 0.0) > 0 else order[0]


#: When CME stops trading on an early-close day (New York time), by root.
EARLY_CLOSE = {"ES": time(13, 15), "NQ": time(13, 15), "GC": time(13, 45), "MGC": time(13, 45)}


def early_close_days(y: int) -> set[date]:
    """CME's US early closes in year `y` (verify against CME's holiday
    calendar each year): the day after Thanksgiving, Christmas Eve on a
    weekday, and July 3 when it falls Monday to Thursday (on a Friday it is
    the observed holiday)."""
    thanksgiving = orb._nth(y, 11, 3, 4)
    out = {thanksgiving + timedelta(1)}
    for d in (date(y, 12, 24), date(y, 7, 3)):
        if d.weekday() < (5 if d.month == 12 else 4):
            out.add(d)
    return out


def early_close(symbol: str, day: date) -> time | None:
    """The time `symbol`'s market stops on `day` if it's an early close."""
    if day not in early_close_days(day.year):
        return None
    return EARLY_CLOSE.get(orb.contract(symbol).root)


def previous_weekday(d: date) -> date:
    d -= timedelta(1)
    while d.weekday() >= 5:
        d -= timedelta(1)
    return d


# ── Massive (REST for the day's setup, the socket for its bars) ─────────────


class Massive:
    def __init__(self, key: str, base: str = BASE_URL, per_minute: int = 5,
                 opener: Callable = urllib.request.urlopen):
        self._key, self._base, self._open = key, base.rstrip("/"), opener
        self._gap = 60.0 / max(1, per_minute)
        self._last = 0.0

    def _get(self, path: str, params: dict) -> dict:
        wait = self._last + self._gap - clock.monotonic()
        if wait > 0:
            clock.sleep(wait)
        self._last = clock.monotonic()
        url = f"{self._base}{path}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {self._key}",
                                                   "User-Agent": USER_AGENT})
        with self._open(req, timeout=60) as resp:
            body = json.load(resp)
        if str(body.get("status", "")).upper() == "ERROR":
            raise SignalError(f"massive {path}: {body.get('error') or body.get('message')}")
        return body

    def nearest(self, product: str, day: date, n: int = 3) -> list[str]:
        body = self._get("/futures/v1/contracts", {"product_code": product, "date": day.isoformat(),
                                                   "active": "true", "type": "single", "limit": 1000})
        rows = {r["ticker"]: r for r in body.get("results") or [] if r.get("ticker")}

        def expiry(r):
            return str(r.get("last_trade_date") or r.get("settlement_date") or "9999-12-31")[:10]

        live = [r for r in rows.values() if expiry(r) >= day.isoformat()]
        live.sort(key=lambda r: (expiry(r), r["ticker"]))
        return [r["ticker"] for r in live[:n]]

    def minutes(self, ticker: str, day: date) -> list[tuple]:
        lo = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)
        body = self._get(f"/futures/v1/aggs/{ticker}", {
            "resolution": "1min", "limit": 50000, "sort": "window_start.asc",
            "window_start.gte": day.isoformat(), "window_start.lte": (day + timedelta(1)).isoformat()})
        out = []
        for r in body.get("results") or []:
            ms = int(r["window_start"]) // 1_000_000
            if lo <= ms < lo + 86_400_000:
                out.append((ms, float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]),
                            float(r.get("volume") or 0)))
        return out


def socket_bar(msg: dict) -> tuple[str, tuple] | None:
    """An AM message as (ticker, row); None for anything else."""
    if msg.get("ev") != "AM":
        return None
    return msg["sym"], (int(msg["s"]), float(msg["o"]), float(msg["h"]), float(msg["l"]),
                        float(msg["c"]), float(msg.get("v") or 0))


# ── The runner ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Config:
    deployment_id: int
    db_url: str
    massive_key: str
    webhook: str | None
    code_version: str
    ws_url: str
    base_url: str
    per_minute: int
    bot_id: str
    port: int
    channel: str | None = None


def load_config(env: Mapping[str, str] | None = None) -> Config:
    env = os.environ if env is None else env
    raw = env.get("EDGE_DEPLOYMENT_ID", "").strip()
    if not raw.isdigit() or int(raw) < 1:
        raise SignalError(f"EDGE_DEPLOYMENT_ID must be a deployment id, not {raw!r}")
    need = {"FKS_DB_URL": "the spawner adds it for a bot with the research database",
            "MASSIVE_API_KEY": "store it in Settings -> API keys as provider massive",
            "FKS_CODE_REVISION": "every signal names the code that produced it"}
    for var, why in need.items():
        if not env.get(var, "").strip():
            raise SignalError(f"{var} is missing: {why}")
    try:
        per_minute = int(env.get("MASSIVE_PER_MINUTE", "5"))
        port = int(env.get("STATUS_PORT", "9091"))
    except ValueError as e:
        raise SignalError(f"a number setting: {e}") from None
    return Config(int(raw), env["FKS_DB_URL"].strip(), env["MASSIVE_API_KEY"].strip(),
                  env.get("SIGNAL_DISCORD_WEBHOOK_URL", "").strip() or None,
                  env["FKS_CODE_REVISION"].strip(), env.get("MASSIVE_WS_URL", WS_URL),
                  env.get("MASSIVE_BASE_URL", BASE_URL), per_minute,
                  env.get("FKS_BOT_ID", "edge-signal"), port,
                  env.get("SIGNAL_CHANNEL", "").strip() or None)


def config_problem(cfg: Config) -> str | None:
    """Why signals would go nowhere although a channel was named."""
    if cfg.channel and not cfg.webhook:
        return (f"channel {cfg.channel!r} was named but its webhook didn't arrive (is it in Settings "
                f"-> Notifications?): signals are recorded and logged, not sent")
    return None


def post_discord(url: str, text: str, opener: Callable = urllib.request.urlopen) -> bool:
    req = urllib.request.Request(url, data=json.dumps({"content": text[:1900]}).encode(),
                                 headers={"Content-Type": "application/json", "User-Agent": USER_AGENT})
    try:
        with opener(req, timeout=20):
            return True
    except (urllib.error.URLError, OSError) as e:
        log.warning("discord: %s", e)
        return False


@dataclass
class Day:
    """One trading day's setup: the contract per symbol, and why not to trade."""

    day: date
    contracts: dict[str, str]  # symbol -> ticker
    blocked: dict[str, str]  # symbol -> reason (roll day, holiday)
    rows: dict[str, dict[int, tuple]] = field(default_factory=dict)  # ticker -> bars by minute


def plan_day(m: Massive, symbols: list[str], day: date) -> Day:
    """Today's contract per symbol (by the previous weekday's volume), and
    whether today is a roll day for it (the search doesn't trade those)."""
    contracts, blocked = {}, {}
    holidays = orb.us_holidays(day.year)
    prev = previous_weekday(day)
    before = previous_weekday(prev)
    by_root: dict[str, tuple[str | None, str | None]] = {}
    for sym in symbols:
        root = orb.contract(sym).root
        if root not in by_root:
            order = m.nearest(root, day)
            vol_prev = {t: sum(r[5] for r in m.minutes(t, prev)) for t in order}
            today = choose(order, vol_prev)
            order_y = m.nearest(root, prev)
            vol_before = {t: sum(r[5] for r in m.minutes(t, before)) for t in order_y}
            by_root[root] = (today, choose(order_y, vol_before))
        today, yesterday = by_root[root]
        if today is None:
            blocked[sym] = f"no {root} contract listed for {day}"
            continue
        contracts[sym] = today
        cut = early_close(sym, day)
        if day in holidays:
            blocked[sym] = "a US exchange holiday"
        elif cut is not None and cut < orb.contract(sym).end:
            blocked[sym] = (f"an early close (trading stops {cut:%H:%M} ET, before the session's "
                            f"{orb.contract(sym).end:%H:%M}): the search doesn't trade short sessions")
        elif yesterday and yesterday != today:
            blocked[sym] = f"a roll day ({yesterday} -> {today}): the search doesn't trade those"
    return Day(day, contracts, blocked)


def record(conn, cfg: Config, ev: Event, contract: str) -> tuple[int, str] | None:
    """Write the signal (fks 031) unless already said; (id, stage) when new."""
    row = conn.execute(
        "INSERT INTO edge_signals (deployment_id, session_date, symbol, kind, stage, contract, side, "
        "price, stop, target, bar_at, reason, runner, code_version) "
        "VALUES (%s, %s, %s, %s, 'forward', %s, %s, %s, %s, %s, to_timestamp(%s / 1000.0), %s, %s, %s) "
        "ON CONFLICT (deployment_id, session_date, symbol, kind) DO NOTHING RETURNING id, stage",
        (cfg.deployment_id, ev.day, ev.symbol, ev.kind, contract, ev.side, ev.price, ev.stop, ev.target,
         ev.bar_ms, ev.reason, cfg.bot_id, cfg.code_version),
    ).fetchone()
    conn.commit()
    return (int(row[0]), str(row[1])) if row else None


def deliver(conn, signal_id: int) -> None:
    conn.execute("UPDATE edge_signals SET delivered_at = now() WHERE id = %s AND delivered_at IS NULL",
                 (signal_id,))
    conn.commit()


def lateness_ms(ev: Event, now_ms: int) -> int:
    """How long after its bar closed the event is being said."""
    return now_ms - (ev.bar_ms + 60_000)


def late_text(ev: Event, text: str, late_ms: int) -> str:
    """A late event's message: what it was, and that it isn't a call to act."""
    minutes = max(1, round(late_ms / 60_000))
    tag, _, rest = text.partition("] ")
    advice = {"entry": "for the record: don't chase it",
              "exit": "if you're still in, the model is out",
              "range": "for the record"}[ev.kind]
    return f"{tag}] LATE by {minutes} min: {rest}. {advice[0].upper()}{advice[1:]}."


def say(conn, cfg: Config, ev: Event, contract: str,
        send: Callable[[str], bool], now_ms: int | None = None) -> bool:
    """Record, then send, then mark delivered. False when already said, or
    when the ledger refuses it (its rules decide; the reason is logged)."""
    import psycopg

    late = lateness_ms(ev, now_ms) if now_ms is not None else 0
    if late > LATE_MS:
        ev = replace(ev, reason=f"{ev.reason} [said {round(late / 60_000)} min after its bar]")
    try:
        new = record(conn, cfg, ev, contract)
    except psycopg.errors.CheckViolation as e:
        conn.rollback()
        log.warning("%s %s %s refused by the ledger: %s", ev.symbol, ev.day, ev.kind, str(e).splitlines()[0])
        return False
    if new is None:
        return False
    signal_id, stage = new
    text = message(ev, contract, stage)
    if late > LATE_MS:
        text = late_text(ev, text, late)
    log.info("%s", text)
    if send(text):
        deliver(conn, signal_id)
    return True


def step(conn, cfg: Config, dep, today: Day, now_ms: int, send: Callable[[str], bool],
         exits_only: bool = False) -> dict:
    """Evaluate every symbol on the bars so far; say what's new. With
    `exits_only` (the deployment was retired with a trade open), only exits."""
    said = 0
    status = {}
    for sym in dep.symbols:
        if sym in today.blocked:
            status[sym] = today.blocked[sym]
            continue
        ticker = today.contracts[sym]
        rows = list(today.rows.get(ticker, {}).values())
        s = evaluate(sym, today.day, rows, dep.params, dep.hypothesis.costs, now_ms)
        for ev in s.events:
            if exits_only and ev.kind != "exit":
                continue
            said += say(conn, cfg, ev, ticker, send, now_ms)
        status[sym] = ("open" if s.open_trade else s.events[-1].kind if s.events else "waiting")
    return {"said": said, "symbols": status, "open": sum(1 for v in status.values() if v == "open")}


def feed_stale(dep, today: Day, now_ms: int, last_bar_ms: int) -> bool:
    """Inside a session being traded, and no bar for FEED_STALE_S."""
    if now_ms - last_bar_ms <= FEED_STALE_S * 1000:
        return False
    for sym in dep.symbols:
        if sym in today.blocked or sym not in today.contracts:
            continue
        start, end = session_bounds(sym, today.day)
        if start <= now_ms < end:
            return True
    return False


def render_metrics(doc: dict) -> str:
    def g(name: str, help_text: str, value) -> list[str]:
        return [f"# HELP {name} {help_text}", f"# TYPE {name} gauge", f"{name} {value}"]

    lines = g("fks_signal_up", "1 while the signal runner runs.", 1)
    lines += g("fks_signal_connected", "1 while the market-data socket is connected.",
               1 if doc.get("connected") else 0)
    lines += g("fks_signal_last_bar_age_seconds", "Seconds since the last bar arrived (NaN before one).",
               doc.get("last_bar_age", "NaN"))
    lines += g("fks_signal_said_total", "Signals said since start.", doc.get("said_total", 0))
    lines += g("fks_signal_error",
               "1 when the last step failed, Massive refused the key, or a named channel has no webhook.",
               1 if doc.get("error") or doc.get("config_error") else 0)
    lines += g("fks_signal_feed_stale", "1 inside a traded session with no bar for 3 minutes.",
               1 if doc.get("feed_stale") else 0)
    lines += g("fks_signal_open_trades", "Trades the runner is watching (entered, not yet exited).",
               doc.get("open_trades", 0))
    lines += g("fks_signal_winding_down", "1 while a retired deployment's open trade is watched to its exit.",
               1 if doc.get("winding_down") else 0)
    return "\n".join(lines) + "\n"


async def _socket(cfg: Config, tickers: list[str], on_bar: Callable[[str, tuple], None], status) -> None:
    import websockets  # the runner's own dependency

    async with websockets.connect(cfg.ws_url, ping_interval=20) as ws:
        await ws.send(json.dumps({"action": "auth", "params": cfg.massive_key}))
        await ws.send(json.dumps({"action": "subscribe", "params": ",".join(f"AM.{t}" for t in tickers)}))
        async for raw in ws:
            for msg in json.loads(raw):
                if msg.get("ev") == "status":
                    log.info("massive: %s %s", msg.get("status"), msg.get("message"))
                    if msg.get("status") == "auth_failed":
                        raise SignalError(f"massive refused the key (auth_failed: {msg.get('message')}): "
                                          f"live signals need a real-time futures plan (Futures Advanced)")
                    if msg.get("status") == "auth_success":
                        # A refusal from an earlier try no longer applies.
                        status.update(connected=True, config_error=config_problem(cfg))
                    continue
                got = socket_bar(msg)
                if got:
                    on_bar(*got)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S%z")
    try:
        cfg = load_config()
    except SignalError as e:
        log.error("%s", e)
        return 2
    import psycopg

    from .forward import ForwardError, Status, load_deployment
    from .server import serve as serve_status

    status = Status(cfg)  # type: ignore[arg-type]
    status.update(kind="bot", bot="edge-signal", bot_id=cfg.bot_id, deployment_id=cfg.deployment_id,
                  phase="starting", code_version=cfg.code_version, said_total=0, error=None,
                  config_error=config_problem(cfg))
    if config_problem(cfg):
        log.error("%s", config_problem(cfg))
    serve_status(status, cfg.port, render=render_metrics)
    massive = Massive(cfg.massive_key, cfg.base_url, cfg.per_minute)
    send = (lambda text: post_discord(cfg.webhook, text)) if cfg.webhook else (lambda text: False)
    if not cfg.webhook and not cfg.channel:
        log.warning("no channel named: signals are recorded and logged, not sent")
    stop = asyncio.Event()

    async def nap(seconds: float) -> None:
        try:
            await asyncio.wait_for(stop.wait(), seconds)
        except asyncio.TimeoutError:
            pass

    async def run_loop() -> int:
        loop = asyncio.get_running_loop()
        for sig in (signals.SIGTERM, signals.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        backoff = 5
        while not stop.is_set():
            try:
                with psycopg.connect(cfg.db_url) as conn:
                    dep = load_deployment(conn, cfg.deployment_id)
                    dep.check()
                    if dep.stage not in RUN_STAGES:
                        status.update(phase=f"idle: the deployment is at {dep.stage}", error=None)
                        await nap(600)
                        continue
                    day = datetime.now(ET).date()
                    today = await asyncio.to_thread(plan_day, massive, dep.symbols, day)
                    for t in set(today.contracts.values()):
                        today.rows[t] = {r[0]: r for r in await asyncio.to_thread(massive.minutes, t, day)}
                    status.update(phase="watching", day=day.isoformat(), contracts=today.contracts,
                                  blocked=today.blocked, stage=dep.stage, error=None)
                    last_bar = [clock.time()]

                    def on_bar(ticker: str, row: tuple) -> None:
                        if ticker in today.rows:
                            today.rows[ticker][row[0]] = row
                            last_bar[0] = clock.time()

                    sock = asyncio.create_task(_socket(cfg, sorted(set(today.contracts.values())), on_bar,
                                                       status))
                    winding = False
                    try:
                        while not stop.is_set() and datetime.now(ET).date() == day:
                            if sock.done():
                                sock.result()  # raises the socket's error
                                raise ConnectionError("the market-data socket closed")
                            now_ms = int(clock.time() * 1000)
                            out = step(conn, cfg, dep, today, now_ms, send, exits_only=winding)
                            snap = status.snapshot()
                            status.update(last_step=out, said_total=snap.get("said_total", 0) + out["said"],
                                          last_bar_age=round(clock.time() - last_bar[0], 1), error=None,
                                          open_trades=out["open"], winding_down=winding,
                                          feed_stale=feed_stale(dep, today, now_ms, int(last_bar[0] * 1000)))
                            if winding and out["open"] == 0:
                                break
                            stage = load_deployment(conn, cfg.deployment_id).stage
                            if stage not in RUN_STAGES and not winding:
                                if out["open"] == 0:
                                    break
                                # Retired with a trade open: watch it to its exit.
                                log.warning("deployment %d is now %s with %d trade(s) open: saying "
                                            "their exits only", dep.id, stage, out["open"])
                                winding = True
                            await nap(15)
                    finally:
                        sock.cancel()
                        status.update(winding_down=False, feed_stale=False)
                    backoff = 5
            except ForwardError as e:  # what this deployment is can't be signalled
                log.error("%s", e)
                status.update(phase="refused", error=str(e))
                return 2
            except Exception as e:  # the feed, the database: retried, slower each time
                if "auth_failed" in str(e):
                    # Stay up and say why (an exit would be a crash loop under
                    # the restart policy); ask again now and then, in case the
                    # plan changes.
                    log.error("%s (asking again in %ss)", e, REFUSED_RETRY_S)
                    status.update(phase="refused", config_error=str(e), connected=False)
                    await nap(REFUSED_RETRY_S)
                    continue
                log.error("signal runner: %s (retrying in %ss)", e, backoff)
                status.update(phase="error", error=str(e), connected=False)
                await nap(backoff)
                backoff = min(300, backoff * 2)
        return 0

    return asyncio.run(run_loop())


def run() -> None:
    np.seterr(all="ignore")
    sys.exit(main())
