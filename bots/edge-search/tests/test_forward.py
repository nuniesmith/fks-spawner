"""The forward runner (edge_search/forward.py): the search's code, one day at a time."""

import csv
import gzip
import hashlib
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pytest
import test_orb as T

from edge_search import core, forward, orb
from edge_search.dataset import parse_spec

PARAMS = {"orb_minutes": 15, "target_r": 2.0, "exit": "trail"}


def write_recorder_days(folder, bars):
    """A MinuteBars as the CME recorder writes it: one file per UTC day."""
    by_day = {}
    for i, t in enumerate(bars.open_ms):
        d = datetime.fromtimestamp(t / 1000, timezone.utc).date()
        by_day.setdefault(d, []).append((int(t), bars.open[i], bars.high[i], bars.low[i], bars.close[i], 1.0))
    out = folder / bars.root / "1m"
    out.mkdir(parents=True, exist_ok=True)
    for d, rows in by_day.items():
        with gzip.open(out / f"{bars.root}-1m-{d.isoformat()}.csv.gz", "wt", newline="") as f:
            w = csv.writer(f)
            w.writerow(["open_time_ms", "open", "high", "low", "close", "volume"])
            w.writerows(rows)
    return sorted(by_day)


DAYS = T.weekdays(date(2026, 11, 2), 30)


@pytest.fixture
def lake(tmp_path):
    folder = tmp_path / "cme" / "massive" / "cme"
    for b in T.series_for(DAYS, 0.00015):
        write_recorder_days(folder, b)
    return tmp_path


def deployment(**over):
    h = T.orb_hypothesis(symbols=["ES", "NQ", "MGC"],
                         dataset=f"raw/cme/massive/cme 1m {DAYS[0]}..{DAYS[-1]}")
    base = dict(id=3, experiment_id=9, symbol="ES+NQ+MGC", stage="forward", params=PARAMS,
                since=datetime.combine(DAYS[0] - timedelta(3), datetime.min.time(), timezone.utc),
                required=20, observed=0, family="orb", hypothesis=h)
    base.update(over)
    return forward.Deployment(**base)


def test_a_replayed_day_is_the_searched_day(lake):
    """Parity: each session replayed alone equals the same session in the
    search's own run over the whole window (orb.trades_for + orb.daily)."""
    dep = deployment()
    spec = parse_spec(dep.hypothesis.dataset)
    from edge_search.dataset import load_cme_minutes

    series = [load_cme_minutes(lake, spec, orb.contract(s).root) for s in dep.symbols]
    data = orb.prepare(dep.symbols, series, spec, dep.hypothesis.costs)
    net, _ = orb.daily(data, orb.trades_for(data, PARAMS))
    searched = dict(zip([datetime.fromtimestamp(t / 1000, timezone.utc).date() for t in data.times], orb.pool(net)))
    checked = 0
    for day in DAYS:
        out = forward.observe(dep, lake, day)
        assert out.kind == "session", (day, out.reason)
        assert out.observation.session_return == pytest.approx(searched[day], abs=1e-15), day
        checked += 1
    assert checked == len(DAYS)


def test_an_observation_names_the_files_it_read(lake):
    dep = deployment()
    out = forward.observe(dep, lake, DAYS[2])
    refs = out.observation.data_ref
    assert [r["path"].split("/")[3] for r in refs] == ["ES", "NQ", "GC"]
    p = lake / refs[0]["path"]
    assert refs[0]["sha256"] == hashlib.sha256(p.read_bytes()).hexdigest()
    trades = out.observation.trade["trades"]
    assert {t["symbol"] for t in trades} <= {"ES", "NQ", "MGC"}


def test_weekends_holidays_and_missing_days(lake):
    dep = deployment()
    assert forward.observe(dep, lake, date(2026, 11, 7)).kind == "none"  # Saturday
    assert forward.observe(dep, lake, date(2026, 11, 26)).reason == "a US exchange holiday"
    out = forward.observe(dep, lake, DAYS[-1] + timedelta(7))
    assert out.kind == "wait" and "file" in out.reason


def test_a_flat_session_returns_exactly_zero(tmp_path):
    folder = tmp_path / "cme" / "massive" / "cme"
    day = date(2026, 11, 4)
    for root, n in (("ES", 390), ("NQ", 390), ("GC", 310)):
        write_recorder_days(folder, T.minute_bars(root, {day: T.orb_session(n=n)}))
    out = forward.observe(deployment(), tmp_path, day)
    assert out.kind == "session" and not out.observation.traded
    assert out.observation.session_return == 0.0 and out.observation.trade is None


def test_scan_records_in_order_and_waits_for_the_lake(lake):
    dep = deployment()
    seen = []
    today = DAYS[-1] + timedelta(4)
    out = forward.scan(dep, lake, today, None, lambda o: seen.append(o.day) or True)
    recent = [d for d in DAYS if (today - d).days <= forward.LATE_DAYS]
    assert seen == recent and out["recorded"] == len(recent)
    assert out["gaps"] == len(DAYS) - len(recent)  # older than 10 days: gaps, never backfilled
    # Resume after the last recorded day; a missing day stops the scan.
    out = forward.scan(dep, lake, today + timedelta(7), DAYS[-1], lambda o: True)
    assert out["recorded"] == 0 and "file" in (out["waiting_for"] or "")


def test_a_refusal_by_the_ledger_is_counted_not_hidden(lake):
    dep = deployment()
    today = DAYS[5] + timedelta(1)
    out = forward.scan(dep, lake, today, None, lambda o: False)
    assert out["refused"] == len([d for d in DAYS if d < today]) and out["recorded"] == 0


def test_days_start_after_the_forward_start():
    dep = deployment(since=datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc))
    days = forward.days_to_scan(dep, None, date(2026, 11, 14))
    assert days == [date(2026, 11, 11), date(2026, 11, 12), date(2026, 11, 13)]
    assert forward.days_to_scan(dep, date(2026, 11, 12), date(2026, 11, 14)) == [date(2026, 11, 13)]


@pytest.mark.parametrize("over,needle", [
    ({"family": "trend-band"}, "replays"),
    ({"symbol": "ES+CL"}, "didn't search"),
    ({"params": {"orb_minutes": 15, "target_r": 2.0, "exit": "moon"}}, "exit must be"),
])
def test_what_the_runner_cant_replay_is_refused(over, needle):
    with pytest.raises(forward.ForwardError, match=needle):
        deployment(**over).check()


def test_config_needs_the_deployment_the_database_and_the_code_version():
    base = {"EDGE_DEPLOYMENT_ID": "3", "FKS_DB_URL": "postgres://x", "FKS_CODE_REVISION": "abc123"}
    c = forward.load_config(base)
    assert (c.deployment_id, c.code_version, c.poll_minutes) == (3, "abc123", 60.0)
    for key, needle in (("EDGE_DEPLOYMENT_ID", "deployment id"), ("FKS_DB_URL", "FKS_DB_URL"),
                        ("FKS_CODE_REVISION", "names the code")):
        with pytest.raises(forward.ForwardError, match=needle):
            forward.load_config({**base, key: ""})


def test_metrics_render_from_the_status():
    text = forward.render_metrics({"observed": 4, "required": 20, "last_scan": {"recorded": 1, "gaps": 0}})
    assert "fks_forward_sessions_observed 4" in text and "fks_forward_sessions_required 20" in text


def test_metrics_carry_the_monitor():
    health = {"applies": True, "band": {"verdict": "fail", "mean": -0.002, "floor": -0.0005},
              "cusum": {"value": 3.5, "h": 7.25}}
    text = forward.render_metrics({"stage": "retired", "health": health})
    for line in ("fks_forward_retired 1", "fks_forward_band_failed 1", "fks_forward_band_floor -0.0005",
                 "fks_forward_cusum 3.5", "fks_forward_cusum_h 7.25"):
        assert line in text
    # A pending window has no mean yet; a low-frequency edge has no P&L monitor.
    pending = {"applies": True, "band": {"verdict": "pending", "mean": None, "floor": -0.0005},
               "cusum": {"value": 0, "h": 7.25}}
    assert "fks_forward_band_mean NaN" in forward.render_metrics({"stage": "forward", "health": pending})
    lf = forward.render_metrics({"stage": "forward", "health": {"applies": False, "why": "low-frequency"}})
    assert "fks_forward_cusum" not in lf and "fks_forward_retired 0" in lf


def test_the_search_records_its_selection():
    h = T.orb_hypothesis()
    r = __import__("edge_search.search", fromlist=["run_search"]).run_search(
        h, "orb", T.window(T.DAYS), T.series_for(T.DAYS, 0.00015), prior_trials=0, now=T.NOW)
    row = core.experiment_row(h, T.window(T.DAYS), r, "s", datetime.now(timezone.utc))
    assert row["selected_params"] == r.best.params and set(row["selected_params"]) == set(orb.PARAMS)
    assert np.isfinite(row["holdout_score"])
    # fks 030: the selection's holdout sessions, the monitor's reference.
    assert row["holdout_returns"] == [float(x) for x in r.best.holdout]
    assert len(row["holdout_returns"]) == len(r.best.holdout) > 0


def test_an_unusable_holdout_series_is_not_recorded():
    h = T.orb_hypothesis()
    r = __import__("edge_search.search", fromlist=["run_search"]).run_search(
        h, "orb", T.window(T.DAYS), T.series_for(T.DAYS, 0.00015), prior_trials=0, now=T.NOW)
    r.best.holdout = np.array([0.001, np.nan])
    assert core.experiment_row(h, T.window(T.DAYS), r, "s", T.NOW)["holdout_returns"] is None
    r.best.holdout = np.array([])
    assert core.experiment_row(h, T.window(T.DAYS), r, "s", T.NOW)["holdout_returns"] is None
