import csv
import gzip
import json
from datetime import date, datetime, timezone

from conftest import FakeMassive

from cme_history import core
from cme_history.store import Lake

ES = [
    {"ticker": "ESU6", "last_trade_date": "2026-09-18"},
    {"ticker": "ESZ6", "last_trade_date": "2026-12-18"},
    {"ticker": "ESH7", "last_trade_date": "2027-03-19"},
]


def d(day: int) -> date:
    return date(2026, 9, day)


def volumes():
    v = {}
    for day in range(11, 19):  # Fri 11 .. Fri 18
        # ESU6 leads until Tue 15; from Wed 16 on, ESZ6 trades more.
        u, z = (900, 100) if day <= 15 else (100, 900)
        v[("ESU6", d(day))] = u
        v[("ESZ6", d(day))] = z
        v[("ESH7", d(day))] = 5
    return v


def read_rows(path):
    with gzip.open(path, "rt", newline="") as f:
        return list(csv.reader(f))


def test_a_week_with_a_roll(cfg, tmp_path):
    client = FakeMassive({"ES": ES}, volumes(), price={"ESU6": 5000.0, "ESZ6": 5060.0, "ESH7": 5110.0})
    status = core.Status(cfg)
    rec = core.Recorder(cfg, client, Lake(tmp_path), status)
    now = datetime(2026, 9, 18, 3, 0, tzinfo=timezone.utc)  # Thu 17 is the last complete day
    assert rec.cycle(now)
    days = sorted(p.name for p in (tmp_path / "ES" / "1m").iterdir())
    assert days == [f"ES-1m-2026-09-{n}.csv.gz" for n in (14, 15, 16, 17)]
    # Chosen on the previous weekday's volume: ESZ6 leads from Wed 16, so it
    # is followed from Thu 17 (no look-ahead), and the roll is logged once.
    man = [json.loads(line) for line in (tmp_path / "_manifest.jsonl").read_text().splitlines()]
    by_day = {m["path"]: m["contract"] for m in man if m["dataset"] == "bars"}
    assert by_day["ES/1m/ES-1m-2026-09-16.csv.gz"] == "ESU6"
    assert by_day["ES/1m/ES-1m-2026-09-17.csv.gz"] == "ESZ6"
    rolls = list(csv.reader((tmp_path / "ES" / "rolls.csv").open()))
    assert rolls == [["day", "from_contract", "to_contract", "gap_points", "detected_by"],
                     ["2026-09-17", "ESU6", "ESZ6", "60.0", "volume"]]
    rows = read_rows(tmp_path / "ES" / "1m" / "ES-1m-2026-09-17.csv.gz")
    assert rows[0] == ["open_time_ms", "open", "high", "low", "close", "volume"] and float(rows[1][4]) == 5060.0
    # Every candidate's own bars are kept, for audit.
    assert (tmp_path / "ES" / "contracts" / "ESH7" / "ESH7-1m-2026-09-14.csv.gz").exists()
    assert status.roots["ES"]["last_day"] == "2026-09-17"


def test_saturday_is_skipped_and_a_holiday_writes_nothing(cfg, tmp_path):
    v = volumes()
    for t in ("ESU6", "ESZ6", "ESH7"):
        v.pop((t, d(14)))  # Monday 14: no bars at all
    client = FakeMassive({"ES": ES}, v)
    rec = core.Recorder(cfg, client, Lake(tmp_path), core.Status(cfg))
    rec.cycle(datetime(2026, 9, 16, 3, 0, tzinfo=timezone.utc))  # up to Tue 15
    assert sorted(p.name for p in (tmp_path / "ES" / "1m").iterdir()) == ["ES-1m-2026-09-15.csv.gz"]
    # The seed day (Fri 11) and Tue 15 decided, not the empty Monday.
    state = Lake(tmp_path).state("ES")
    assert state["prev_volume"]["ESU6"] == 900 and state["last_day"] == "2026-09-15"
    sat = [c for c in client.bar_calls if c[1] == date(2026, 9, 19)]
    assert sat == []


def test_resume_after_a_failure_redoes_only_the_failed_day(cfg, tmp_path):
    client = FakeMassive({"ES": ES}, volumes(), fail_on={("ESZ6", d(16))})
    status = core.Status(cfg)
    rec = core.Recorder(cfg, client, Lake(tmp_path), status)
    assert not rec.cycle(datetime(2026, 9, 18, 3, 0, tzinfo=timezone.utc))
    assert Lake(tmp_path).state("ES")["last_day"] == "2026-09-15"
    assert status.errors_total == 1 and "scripted" in status.roots["ES"]["error"]
    ok = FakeMassive({"ES": ES}, volumes())
    rec2 = core.Recorder(cfg, ok, Lake(tmp_path), core.Status(cfg))
    assert rec2.cycle(datetime(2026, 9, 18, 3, 0, tzinfo=timezone.utc))
    assert {c[1] for c in ok.bar_calls} == {d(16), d(17)}
    rolls = (tmp_path / "ES" / "rolls.csv").read_text().splitlines()
    assert len(rolls) == 2  # the header and one roll


def test_a_day_is_recorded_once_it_is_over():
    assert core.last_complete_day(datetime(2026, 9, 18, 1, 0, tzinfo=timezone.utc)) == date(2026, 9, 16)
    assert core.last_complete_day(datetime(2026, 9, 18, 3, 0, tzinfo=timezone.utc)) == date(2026, 9, 17)
    assert core.previous_weekday(date(2026, 9, 14)) == date(2026, 9, 11)


def test_contracts_are_listed_at_most_weekly(cfg, tmp_path):
    client = FakeMassive({"ES": ES}, volumes())
    rec = core.Recorder(cfg, client, Lake(tmp_path), core.Status(cfg))
    rec.cycle(datetime(2026, 9, 18, 3, 0, tzinfo=timezone.utc))
    listings = client.requests_made - len(client.bar_calls)
    assert listings == 1


def test_files_are_never_rewritten(cfg, tmp_path):
    lake = Lake(tmp_path)
    p = lake.day_path("ES", d(14))
    assert lake.write_bars(p, [(1, 1.0, 1.0, 1.0, 1.0, 1.0)], dataset="bars", symbol="ES", contract="ESU6")
    assert not lake.write_bars(p, [(2, 2.0, 2.0, 2.0, 2.0, 2.0)], dataset="bars", symbol="ES", contract="ESU6")
    assert len(read_rows(p)) == 2
