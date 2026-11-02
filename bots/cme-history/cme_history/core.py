"""Records each root's CME 1-minute bars from Massive, one UTC day at a time,
stitched by volume (stitch.py), into the lake (store.py), and keeps it current.

A day is recorded once it is over (two hours past UTC midnight, so the last
bars have settled). Saturdays have no Globex session and are skipped; a day
with no bars at all (a holiday) is passed without a file. Per day and root:
one request per candidate contract (the three nearest expiries), plus a
contracts listing at most weekly. At Massive's free 5 requests a minute,
four roots cost about 2.5 minutes a day.

A failure stops that root for this cycle (state is saved after every day,
so nothing is redone or skipped) and is retried at the next poll.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from . import stitch
from .config import Config, ConfigError, load
from .massive import Massive, MassiveError
from .store import Lake

log = logging.getLogger("cme-history")

SETTLE = timedelta(hours=2)
CANDIDATES_MAX_AGE_DAYS = 7


def now_ms() -> int:
    return int(time.time() * 1000)


def last_complete_day(now: datetime) -> date:
    return (now - SETTLE).date() - timedelta(days=1)


def previous_weekday(day: date) -> date:
    d = day - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


@dataclass
class Status:
    cfg: Config
    roots: dict = field(default_factory=dict)
    errors_total: int = 0
    last_success_ms: int | None = None
    updated_at_ms: int = field(default_factory=now_ms)
    requests: int = 0

    def snapshot(self) -> dict:
        return {
            "bot_id": self.cfg.bot_id,
            "roots": self.roots,
            "errors_total": self.errors_total,
            "requests": self.requests,
            "last_success_ms": self.last_success_ms,
            "updated_at_ms": self.updated_at_ms,
        }


class Recorder:
    def __init__(self, cfg: Config, client, lake: Lake, status: Status):
        self.cfg, self.client, self.lake, self.status = cfg, client, lake, status

    def _candidates(self, root: str, day: date, st: dict) -> list[str]:
        cached = st.get("candidates")
        if cached:
            as_of = date.fromisoformat(cached["as_of"])
            nearest = cached["expiries"].get(cached["tickers"][0]) if cached["tickers"] else None
            fresh = (day - as_of).days < CANDIDATES_MAX_AGE_DAYS and day >= as_of
            if fresh and not (nearest and date.fromisoformat(nearest) < day):
                return list(cached["tickers"])
        listed = self.client.contracts(root, day)
        tickers = stitch.candidates(listed, day)
        exp = {c["ticker"]: (stitch.expiry(c).isoformat() if stitch.expiry(c) else None) for c in listed}
        st["candidates"] = {"as_of": day.isoformat(), "tickers": tickers,
                            "expiries": {t: exp.get(t) for t in tickers}}
        return tickers

    def _volumes(self, root: str, day: date, st: dict) -> dict[str, float]:
        order = self._candidates(root, day, st)
        return {t: stitch.volume(self.client.bars(t, day)) for t in order}

    def run_root(self, root: str, now: datetime) -> int:
        st = self.lake.state(root)
        day = date.fromisoformat(st["last_day"]) + timedelta(1) if st["last_day"] else self.cfg.start
        end = last_complete_day(now)
        written = 0
        if day <= end and not st.get("prev_volume"):
            # Seed the first choice with the weekday before the start: the
            # rule only ever looks back.
            st["prev_volume"] = self._volumes(root, previous_weekday(day), st)
        while day <= end:
            if day.weekday() != 5:
                order = self._candidates(root, day, st)
                rows = {t: self.client.bars(t, day) for t in order}
                for t, r in rows.items():
                    if r:
                        self.lake.write_bars(self.lake.contract_path(root, t, day), r, dataset="contract_bars",
                                             symbol=root, contract=t)
                choice = stitch.choose(st.get("contract"), order, st.get("prev_volume") or {})
                if choice and rows.get(choice):
                    prev = st.get("contract")
                    # The day's file and its roll row go together: a day redone
                    # after a crash finds its file and appends nothing twice.
                    if self.lake.write_bars(self.lake.day_path(root, day), rows[choice], dataset="bars",
                                            symbol=root, contract=choice):
                        written += 1
                        if prev and choice != prev:
                            self.lake.append_roll(root, day, prev, choice,
                                                  stitch.roll_gap(rows.get(prev) or [], rows[choice]))
                            log.info("%s rolls %s -> %s on %s", root, prev, choice, day)
                    st["contract"] = choice
                vols = {t: stitch.volume(r) for t, r in rows.items()}
                if day.weekday() < 5 and any(v > 0 for v in vols.values()):
                    st["prev_volume"] = vols
            st["last_day"] = day.isoformat()
            self.lake.save_state(root, st)
            self.status.roots[root] = {"last_day": st["last_day"], "contract": st.get("contract"), "error": None}
            self.status.updated_at_ms = now_ms()
            day += timedelta(1)
        return written

    def cycle(self, now: datetime) -> bool:
        ok = True
        for root in self.cfg.roots:
            try:
                n = self.run_root(root, now)
                if n:
                    log.info("%s: %d new day(s)", root, n)
            except (MassiveError, OSError, KeyError, ValueError) as e:
                ok = False
                self.status.errors_total += 1
                self.status.roots.setdefault(root, {})["error"] = str(e)
                log.error("%s: %s (retried next cycle)", root, e)
        self.status.requests = getattr(self.client, "requests_made", 0)
        if ok:
            self.status.last_success_ms = now_ms()
        return ok


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S%z")
    try:
        cfg = load()
    except ConfigError as e:
        log.error("%s", e)
        return 2
    from .server import serve

    status = Status(cfg)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    serve(status, cfg.port)

    def beat() -> None:
        while not stop.wait(10):
            status.updated_at_ms = now_ms()

    threading.Thread(target=beat, daemon=True).start()
    rec = Recorder(cfg, Massive(cfg.api_key, per_minute=cfg.rpm, base_url=cfg.base_url), Lake(cfg.data_dir), status)
    log.info("recording %s from %s into %s", ",".join(cfg.roots), cfg.start, cfg.data_dir)
    while not stop.is_set():
        rec.cycle(datetime.now(timezone.utc))
        stop.wait(cfg.poll_hours * 3600)
    return 0
