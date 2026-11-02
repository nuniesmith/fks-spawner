"""The forward runner (EDGE_PROCESS §5): a deployment's sessions, as they
happen, through the exact code that searched it.

A paper instance of the `bot.edge-forward` template, bound to one deployment
(EDGE_DEPLOYMENT_ID). It runs from the search job's own image, so the family
code that trades forward is the code that was searched, and the image's
revision is recorded with every session. Each poll it:

  1. reads the deployment, its experiment and its hypothesis (the research
     role: fks 025, 029);
  2. for every weekday after the later of the day the deployment entered
     forward and the last session recorded, up to yesterday (UTC): when the
     lake holds that day's file for every root, it replays the session with
     the deployment's frozen params_snapshot through the family (orb.py),
     exactly as the search would have;
  3. records it in the forward ledger (edge_forward_sessions). The database
     adds it to forward_sessions_observed, and refuses a session it can't
     admit: one before the forward start, not over yet, or more than 10†
     days late (a gap stays a gap);
  4. reads the deployment's health (fks 030, edge_deployment_health): the
     forward band and the CUSUM against the experiment's holdout. A breach
     is the database's to act on: it retires the deployment in the same
     transaction as the session that breached, and the runner goes idle.

It keeps replaying once the deployment is live (fks 030): the model's
sessions are what the monitor watches for decay.

A day whose files aren't in the lake yet stops the scan until the next poll
(the recorder writes days in order); a US exchange holiday, or a day whose
session has no bars, is passed without a row. Days older than the late
limit are passed as gaps.

What this does NOT check, and §5 asks for: execution matching the model
(fill vs touch, fees, rejected orders). That needs live quotes and an order
path; the forward ledger checks the MODEL on data that didn't exist when the
search ran. The live rung's readiness check is where execution is tested.

Settings (environment):
  EDGE_RUNNER=forward     selects this program in the image
  EDGE_DEPLOYMENT_ID      the deployment to run; required
  FKS_DB_URL              the research role, set by the spawner; required
  LAKE_DIR                the lake, read-only (default /lake)
  FKS_CODE_REVISION       the image's git revision, baked at build; required
  FORWARD_POLL_MINUTES    minutes between scans (default 60)
  FKS_BOT_ID, STATUS_PORT as the job
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping

import numpy as np

from . import orb
from .dataset import DatasetError, DatasetSpec, load_cme_minutes, parse_spec
from .search import Hypothesis

log = logging.getLogger("edge-forward")

#: How late a session may still be recorded: fks 029's edge_forward_late_days().
LATE_DAYS = 10
FAMILIES = ("orb",)
#: The stages whose sessions are recorded (fks 030: through live).
RUN_STAGES = ("forward", "live")


class ForwardError(ValueError):
    """The deployment can't be run forward; the message says why."""


@dataclass(frozen=True)
class Config:
    deployment_id: int
    db_url: str
    lake_dir: Path
    code_version: str
    poll_minutes: float
    bot_id: str
    port: int


def load_config(env: Mapping[str, str] | None = None) -> Config:
    env = os.environ if env is None else env
    raw = env.get("EDGE_DEPLOYMENT_ID", "").strip()
    if not raw.isdigit() or int(raw) < 1:
        raise ForwardError(f"EDGE_DEPLOYMENT_ID must be a deployment id, not {raw!r}")
    db_url = env.get("FKS_DB_URL", "").strip()
    if not db_url:
        raise ForwardError("FKS_DB_URL is required: the spawner adds it for a bot with the research database")
    code = env.get("FKS_CODE_REVISION", "").strip()
    if not code:
        raise ForwardError("FKS_CODE_REVISION is missing: every forward session names the code that ran it")
    try:
        poll = float(env.get("FORWARD_POLL_MINUTES", "60"))
        port = int(env.get("STATUS_PORT", "9091"))
    except ValueError as e:
        raise ForwardError(f"a number setting: {e}") from None
    if not 1 <= poll <= 24 * 60:
        raise ForwardError("FORWARD_POLL_MINUTES must be between 1 and 1440")
    return Config(int(raw), db_url, Path(env.get("LAKE_DIR", "/lake")), code, poll,
                  env.get("FKS_BOT_ID", "edge-forward"), port)


@dataclass
class Deployment:
    id: int
    experiment_id: int
    symbol: str
    stage: str
    params: dict
    since: datetime
    required: int | None
    observed: int
    family: str
    hypothesis: Hypothesis
    retired_reason: str | None = None

    @property
    def symbols(self) -> list[str]:
        return self.symbol.split("+")

    def check(self) -> None:
        """Refuse what this runner can't replay faithfully."""
        if self.family not in FAMILIES:
            raise ForwardError(f"the forward runner replays {list(FAMILIES)}; experiment "
                               f"{self.experiment_id} searched {self.family!r}")
        unknown = [s for s in self.symbols if s not in self.hypothesis.symbols]
        if unknown:
            raise ForwardError(f"deployment {self.id} trades {unknown}, which hypothesis "
                               f"{self.hypothesis.id} didn't search ({self.hypothesis.symbols})")
        try:
            orb.check_params([self.params])
            for s in self.symbols:
                orb.contract(s)
            orb.costs_of(self.hypothesis.costs)
        except orb.OrbError as e:
            raise ForwardError(str(e)) from None


@dataclass
class Observation:
    day: date
    traded: bool
    trade: dict | None
    session_return: float
    data_ref: list[dict]


@dataclass
class Outcome:
    """`observe`'s answer for one day: a session, or why there is none."""

    kind: str  # "session" | "wait" | "none"
    observation: Observation | None = None
    reason: str = ""


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def day_files(lake: Path, spec: DatasetSpec, root: str, day: date) -> list[Path]:
    """The day's file for `root` in the dataset's folders (the recorder's
    daily files): every copy, the first folder first."""
    name = f"{root}-1m-{day.isoformat()}.csv.gz"
    return [p for part in spec.paths if (p := lake / part / root / "1m" / name).is_file()]


def observe(dep: Deployment, lake: Path, day: date) -> Outcome:
    """One session of `day` for the deployment, replayed by the family with
    the deployment's frozen parameters, or why there is none (yet)."""
    if day.weekday() >= 5:
        return Outcome("none", reason="a weekend")
    if day in orb.us_holidays(day.year):
        return Outcome("none", reason="a US exchange holiday")
    h = dep.hypothesis
    spec = parse_spec(h.dataset)
    roots = [orb.contract(s).root for s in dep.symbols]
    refs: list[dict] = []
    for root in dict.fromkeys(roots):
        files = day_files(lake, spec, root, day)
        if not files:
            return Outcome("wait", reason=f"no {root} file for {day} in the lake yet")
        # The first folder wins a day (dataset.load_cme_minutes): that's the file read.
        refs.append({"path": str(files[0].relative_to(lake)), "sha256": _sha256(files[0])})
    one_day = DatasetSpec(path=spec.path, interval=spec.interval, start=day, end=day)
    try:
        series = [load_cme_minutes(lake, one_day, r) for r in roots]
        data = orb.prepare(dep.symbols, series, one_day, h.costs)
    except (DatasetError, orb.OrbError) as e:
        return Outcome("none", reason=f"no session: {e}")
    if not any(len(s.dates) and s.present.any() for s in data.sessions):
        return Outcome("none", reason="no session with bars")
    trades = orb.trades_for(data, dep.params)
    net, _ = orb.daily(data, trades)
    ret = float(orb.pool(net)[0]) if net.shape[1] else 0.0
    traded = bool(trades)
    trade = None
    if traded:
        trade = {"trades": [
            {"symbol": data.sessions[t.sym].symbol, "side": t.side, "entry_minute": t.entry_min,
             "exit_minute": t.exit_min, "return": t.ret}
            for t in trades
        ]}
    else:
        ret = 0.0  # a flat session's return is exactly 0 (the ledger checks it)
    if not np.isfinite(ret):
        return Outcome("none", reason="a non-finite return; not recorded")
    return Outcome("session", Observation(day, traded, trade, ret, refs))


def days_to_scan(dep: Deployment, last_recorded: date | None, today: date) -> list[date]:
    """Every day after the forward start and the last session recorded, up
    to yesterday (UTC)."""
    start = (dep.since.astimezone(timezone.utc).date() + timedelta(1))
    if last_recorded is not None:
        start = max(start, last_recorded + timedelta(1))
    return [start + timedelta(i) for i in range((today - start).days)]


@dataclass
class Status:
    cfg: Config
    doc: dict = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def update(self, **kv) -> None:
        with self.lock:
            self.doc.update(kv)

    def snapshot(self) -> dict:
        with self.lock:
            return dict(self.doc)


def render_metrics(doc: dict) -> str:
    def g(name: str, help_text: str, value) -> list[str]:
        return [f"# HELP {name} {help_text}", f"# TYPE {name} gauge", f"{name} {value}"]

    scan_out = doc.get("last_scan") or {}
    lines = g("fks_forward_up", "1 while the forward runner runs.", 1)
    lines += g("fks_forward_sessions_observed", "The deployment's forward_sessions_observed (the ledger).",
               doc.get("observed") or 0)
    lines += g("fks_forward_sessions_required", "The deployment's forward_sessions_required.",
               doc.get("required") or 0)
    lines += g("fks_forward_last_scan_recorded", "Sessions the last scan recorded.", scan_out.get("recorded", 0))
    lines += g("fks_forward_last_scan_gaps", "Days the last scan passed as gaps (too late to record).",
               scan_out.get("gaps", 0))
    lines += g("fks_forward_error", "1 when the last scan failed.", 1 if doc.get("error") else 0)
    lines += g("fks_forward_retired", "1 once the deployment is retired (by the monitor or by hand).",
               1 if doc.get("stage") == "retired" else 0)
    health = doc.get("health") or {}
    if health.get("applies"):
        band, cusum = health.get("band") or {}, health.get("cusum") or {}

        def num(v):
            return "NaN" if v is None else v

        lines += g("fks_forward_band_mean", "Mean of the forward window's sessions so far.", num(band.get("mean")))
        lines += g("fks_forward_band_floor", "The band's floor for the declared window (the holdout's 5th pct).",
                   num(band.get("floor")))
        lines += g("fks_forward_band_failed", "1 when the forward window failed the band.",
                   1 if band.get("verdict") == "fail" else 0)
        lines += g("fks_forward_cusum", "The monitor's CUSUM; it alarms above fks_forward_cusum_h.",
                   num(cusum.get("value")))
        lines += g("fks_forward_cusum_h", "The CUSUM's alarm threshold.", num(cusum.get("h")))
    return "\n".join(lines) + "\n"


def scan(dep: Deployment, lake: Path, today: date, last_recorded: date | None,
         record: Callable[[Observation], bool]) -> dict:
    """Observe and record every day that can be, in order. `record` returns
    False when the ledger refused the row (the database's rules decide)."""
    recorded, gaps, refused = 0, 0, 0
    waiting = None
    for day in days_to_scan(dep, last_recorded, today):
        out = observe(dep, lake, day)
        if out.kind == "none":
            continue
        if out.kind == "wait":
            if (today - day).days > LATE_DAYS:
                gaps += 1  # too late to record now: a gap stays a gap
                continue
            waiting = out.reason
            break
        if (today - day).days > LATE_DAYS:
            gaps += 1
            continue
        if record(out.observation):
            recorded += 1
        else:
            refused += 1
    return {"recorded": recorded, "gaps": gaps, "refused": refused, "waiting_for": waiting}


# ── The database (the research role) ────────────────────────────────────────


def load_deployment(conn, deployment_id: int) -> Deployment:
    from . import db

    row = conn.execute(
        "SELECT d.id, d.experiment_id, d.symbol, d.stage, d.params_snapshot, d.since, "
        "       d.forward_sessions_required, d.forward_sessions_observed, "
        "       e.config_fingerprint, e.hypothesis_id, d.retired_reason "
        "FROM edge_deployments d JOIN edge_experiments e ON e.id = d.experiment_id "
        "WHERE d.id = %s",
        (deployment_id,),
    ).fetchone()
    if row is None:
        raise ForwardError(f"no deployment {deployment_id}")
    if row[9] is None:
        raise ForwardError(f"experiment {row[1]} has no hypothesis: nothing to replay it with")
    try:
        h = db.load_hypothesis(conn, int(row[9]))
    except Exception as e:  # SearchError: withdrawn, missing
        raise ForwardError(str(e)) from None
    params = row[4] if isinstance(row[4], dict) else json.loads(row[4])
    return Deployment(id=int(row[0]), experiment_id=int(row[1]), symbol=row[2], stage=row[3],
                      params=params, since=row[5], required=row[6], observed=int(row[7]),
                      family=str(row[8]).split(":", 1)[0], hypothesis=h, retired_reason=row[10])


def load_health(conn, deployment_id: int) -> dict | None:
    """edge_deployment_health (fks 030), or None before 030 is applied."""
    from . import db

    if not db.has_function(conn, "edge_deployment_health"):
        return None
    (doc,) = conn.execute("SELECT edge_deployment_health(%s)", (deployment_id,)).fetchone()
    return json.loads(doc) if isinstance(doc, str) else doc


def last_recorded(conn, deployment_id: int) -> date | None:
    (d,) = conn.execute(
        "SELECT max(session_date) FROM edge_forward_sessions WHERE deployment_id = %s",
        (deployment_id,),
    ).fetchone()
    return d


def make_recorder(conn, dep: Deployment, cfg: Config):
    import psycopg

    def record(o: Observation) -> bool:
        try:
            conn.execute(
                "INSERT INTO edge_forward_sessions (deployment_id, session_date, symbol, traded, "
                "trade, session_return, data_ref, runner, code_version) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (dep.id, o.day, dep.symbol, o.traded, json.dumps(o.trade) if o.trade else None,
                 o.session_return, json.dumps(o.data_ref), cfg.bot_id, cfg.code_version),
            )
            conn.commit()
            log.info("deployment %d: %s %s, return %.6f", dep.id, o.day,
                     "traded" if o.traded else "flat", o.session_return)
            return True
        except psycopg.errors.CheckViolation as e:
            conn.rollback()
            log.warning("deployment %d: %s refused by the ledger: %s", dep.id, o.day,
                        str(e).splitlines()[0])
            return False
        except psycopg.errors.UniqueViolation:
            conn.rollback()
            return False

    return record


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S%z")
    try:
        cfg = load_config()
    except ForwardError as e:
        log.error("%s", e)
        return 2
    import psycopg

    from .server import serve as serve_status

    status = Status(cfg)
    status.update(kind="bot", bot="edge-forward", bot_id=cfg.bot_id, deployment_id=cfg.deployment_id,
                  phase="starting", code_version=cfg.code_version, error=None)
    serve_status(status, cfg.port, render=render_metrics)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        try:
            with psycopg.connect(cfg.db_url) as conn:
                dep = load_deployment(conn, cfg.deployment_id)
                dep.check()
                status.update(stage=dep.stage, symbol=dep.symbol, family=dep.family,
                              required=dep.required, observed=dep.observed)
                if dep.stage not in RUN_STAGES:
                    status.update(phase=f"idle: the deployment is at {dep.stage}", error=None,
                                  health=load_health(conn, dep.id))
                else:
                    today = datetime.now(timezone.utc).date()
                    out = scan(dep, cfg.lake_dir, today, last_recorded(conn, dep.id),
                               make_recorder(conn, dep, cfg))
                    dep = load_deployment(conn, cfg.deployment_id)
                    if dep.stage == "retired":
                        log.warning("deployment %d was retired: %s", dep.id, dep.retired_reason)
                    status.update(phase="watching" if dep.stage in RUN_STAGES
                                  else f"idle: the deployment is at {dep.stage}",
                                  stage=dep.stage, observed=dep.observed, last_scan=out,
                                  health=load_health(conn, dep.id), error=None,
                                  scanned_at=datetime.now(timezone.utc).isoformat(timespec="seconds"))
        except ForwardError as e:
            log.error("%s", e)
            status.update(phase="refused", error=str(e))
            return 2
        except Exception as e:  # the database or the lake: retried at the next poll
            log.error("scan failed: %s (retried next poll)", e)
            status.update(phase="error", error=str(e))
        stop.wait(cfg.poll_minutes * 60)
    return 0


def run() -> None:
    np.seterr(all="ignore")
    sys.exit(main())
