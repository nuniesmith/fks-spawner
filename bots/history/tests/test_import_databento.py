"""tools/import_databento.py: a Databento purchase into the lake's CME layout."""

import csv
import gzip
import hashlib
import importlib.util
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "import_databento", Path(__file__).resolve().parent.parent / "tools" / "import_databento.py")
db = importlib.util.module_from_spec(_spec)
sys.modules["import_databento"] = db
_spec.loader.exec_module(db)


def ms(d: date, hh: int, mm: int = 0) -> int:
    return int(datetime(d.year, d.month, d.day, hh, mm, tzinfo=UTC).timestamp() * 1000)


def bar(t: int, px: float, v: float):
    return (t, px, px + 0.5, px - 0.5, px + 0.25, v)


def test_a_one_digit_year_is_the_next_one_ending_in_it():
    assert db.contract_year(4, 2014) == 2014
    assert db.contract_year(5, 2014) == 2015
    assert db.contract_year(9, 2014) == 2019  # a deferred gold month
    assert db.contract_year(0, 2019) == 2020
    k = db.outright("GCZ9", "GC", date(2014, 9, 2))
    assert (k.code, k.year) == ("GCZ19", 2019)
    assert db.outright("ESM5-ESH6", "ES", date(2015, 1, 2)) is None  # a spread
    assert db.outright("NQM9", "ES", date(2019, 1, 2)) is None  # another root
    assert db.outright("NQQ9", "NQ", date(2019, 1, 2)).month == 8


def test_spreads_are_dropped_the_roll_uses_weekdays_and_the_lake_gets_one_contract_a_day(tmp_path):
    fri, sun, mon, tue = date(2024, 3, 8), date(2024, 3, 10), date(2024, 3, 11), date(2024, 3, 12)
    stream = [
        # Friday: the old contract trades most.
        ("ESH4", bar(ms(fri, 15), 5000, 900)), ("ESM4", bar(ms(fri, 15), 5050, 100)),
        ("ESH4-ESM4", bar(ms(fri, 15), -50, 40)),
        # Sunday evening's hour: the new one trades more, but a Sunday isn't evidence.
        ("ESH4", bar(ms(sun, 23), 5001, 10)), ("ESM4", bar(ms(sun, 23), 5051, 500)),
        # Monday follows Friday (still ESH4), and trades the new one most.
        ("ESH4", bar(ms(mon, 15), 5002, 100)), ("ESM4", bar(ms(mon, 15), 5052, 900)),
        # Tuesday rolls on Monday's volume.
        ("ESH4", bar(ms(tue, 15), 5003, 50)), ("ESM4", bar(ms(tue, 15), 5053, 900)),
    ]
    out = tmp_path / "lake"
    out.mkdir()
    s = db.import_root(out, "ES", [Path("ES_2024.dbn.zst")], {mon: "degraded (databento)"},
                       read=lambda f: iter(stream))
    assert s["skipped"] == {"spread": 1} and s["rolls"] == 1
    def first_close(d):
        with gzip.open(out / "ES" / "1m" / f"ES-1m-{d}.csv.gz", "rt") as f:
            return float(list(csv.reader(f))[1][4])
    assert [first_close(d) for d in (fri, sun, mon, tue)] == [5000.25, 5001.25, 5002.25, 5053.25]
    with (out / "ES" / "rolls.csv").open() as f:
        assert list(csv.DictReader(f)) == [{"day": "2024-03-12", "from_contract": "ESH24", "to_contract": "ESM24",
                                            "gap_points": "50.0", "detected_by": "volume"}]
    assert (out / "ES" / "excluded_days.csv").read_text() == "day_utc,reason\n2024-03-11,degraded (databento)\n"


def test_two_different_bars_in_one_minute_are_refused(tmp_path):
    d = date(2024, 3, 8)
    stream = [("ESH4", bar(ms(d, 15), 5000, 9)), ("ESH4", bar(ms(d, 15), 5001, 9))]
    with pytest.raises(db.ImportError_, match="two different bars"):
        db.import_root(tmp_path, "ES", [Path("x")], {}, read=lambda f: iter(stream))


def test_the_freeze_decides_what_is_imported(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    f = src / "ES_2015.dbn.zst"
    f.write_bytes(b"not really dbn")
    good = hashlib.sha256(f.read_bytes()).hexdigest()
    freeze = tmp_path / "fullverify.log"
    freeze.write_text(f"FILES: 1\nFILE ES_2015.dbn.zst bytes=14 sha256={good} records=0\n")
    assert db.verify([f], db.read_freeze(freeze)) == {f: good}
    freeze.write_text(f"FILE ES_2015.dbn.zst bytes=14 sha256={'0' * 64} records=0\n")
    with pytest.raises(db.ImportError_, match="doesn't match the freeze"):
        db.run(tmp_path / "lake", freeze, [src], {})
    assert not (tmp_path / "lake" / "ES").exists()  # nothing imported
    freeze.write_text("no hashes here\n")
    with pytest.raises(db.ImportError_, match="not a hash freeze"):
        db.read_freeze(freeze)


def test_excluded_days_parse():
    assert db.parse_exclude("2014-09-22:degraded,2017-11-13") == {
        date(2014, 9, 22): "degraded", date(2017, 11, 13): "excluded"}
