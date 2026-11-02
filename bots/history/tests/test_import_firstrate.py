"""tools/import_firstrate.py: bought contract files into the lake's CME layout."""

import csv
import gzip
import importlib.util
import json
import sys
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "import_firstrate", Path(__file__).resolve().parent.parent / "tools" / "import_firstrate.py")
fr = importlib.util.module_from_spec(_spec)
sys.modules["import_firstrate"] = fr  # dataclasses look their module up
_spec.loader.exec_module(fr)


def bars(day: date, start_et: str, n: int, price: float, volume: float) -> list[str]:
    """n one-minute bars from an Eastern wall time on `day`."""
    t0 = datetime.fromisoformat(f"{day.isoformat()} {start_et}")
    out = []
    for i in range(n):
        t = t0 + timedelta(minutes=i)
        p = price + i * 0.25
        out.append(f"{t:%Y-%m-%d %H:%M:%S},{p},{p + 0.5},{p - 0.5},{p + 0.25},{volume}")
    return out


def write(folder: Path, name: str, lines: list[str]) -> Path:
    p = folder / name
    p.write_text("\n".join(lines) + "\n")
    return p


def lake_days(out: Path, root: str) -> dict[str, list[list[str]]]:
    days = {}
    for f in sorted((out / root / "1m").glob("*.csv.gz")):
        with gzip.open(f, "rt") as fh:
            days[f.name] = list(csv.reader(fh))
    return days


def test_eastern_time_becomes_utc_milliseconds_at_the_bar_start():
    # 09:30 EDT = 13:30 UTC; 09:30 EST = 14:30 UTC.
    assert fr.utc_ms("2024-06-03 09:30:00", 0) == int(datetime(2024, 6, 3, 13, 30, tzinfo=UTC).timestamp() * 1000)
    assert fr.utc_ms("2024-12-02 09:30:00", 0) == int(datetime(2024, 12, 2, 14, 30, tzinfo=UTC).timestamp() * 1000)
    with pytest.raises(fr.ImportError_, match="DST gap"):
        fr.utc_ms("2024-03-10 02:30:00", 0)


def test_the_repeated_fall_back_hour_is_read_in_order():
    rows = fr.parse("ES_Z24_1min.txt", "\n".join([
        "2024-11-03 01:30:00,1,2,0.5,1.5,10",
        "2024-11-03 01:30:00,2,3,1.5,2.5,10",  # the second 01:30 (EST)
    ]))
    assert [r[0] for r in rows][1] - rows[0][0] == 3_600_000


def test_impossible_bars_and_conflicting_minutes_are_refused():
    with pytest.raises(fr.ImportError_, match="impossible bar"):
        fr.parse("ES_Z24_1min.txt", "2024-12-02 09:30:00,10,9,8,9.5,1")
    with pytest.raises(fr.ImportError_, match="columns"):
        fr.parse("ES_Z24_1min.txt", "2024-12-02 09:30:00,10,11,9,10")
    with pytest.raises(fr.ImportError_, match="two different bars"):
        fr.parse("ES_Z24_1min.txt", "2024-12-02 09:30:00,10,11,9,10,1\n2024-12-02 09:30:00,10,12,9,10,1")
    with pytest.raises(fr.ImportError_, match="contract file name"):
        fr.contract_of("ES_continuous_1min.txt")
    # An exact duplicate row is just dropped.
    assert len(fr.parse("ES_Z24_1min.txt", "2024-12-02 09:30:00,10,11,9,10,1\n2024-12-02 09:30:00,10,11,9,10,1")) == 1


def test_a_roll_follows_the_previous_days_volume_and_never_goes_back(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    mon, tue, wed, thu = (date(2024, 12, 9) + timedelta(d) for d in range(4))
    # Monday the old contract trades most; Tuesday the new one does; Wednesday
    # the old one trades most AGAIN (a thin day), but the series never goes back.
    write(src, "ES_Z24_1min.txt", [*bars(mon, "09:30:00", 5, 6000, 900), *bars(tue, "09:30:00", 5, 6001, 100),
                                   *bars(wed, "09:30:00", 5, 6002, 5000), *bars(thu, "09:30:00", 5, 6003, 1)])
    write(src, "ES_H25_1min.txt", [*bars(mon, "09:30:00", 5, 6050, 100), *bars(tue, "09:30:00", 5, 6051, 900),
                                   *bars(wed, "09:30:00", 5, 6052, 10), *bars(thu, "09:30:00", 5, 6053, 900)])
    out = tmp_path / "lake"
    summary = fr.run(out, [src])
    assert summary["ES"] == {"contracts": 2, "days": 4, "rolls": 1, "first": "2024-12-09", "last": "2024-12-12"}
    days = lake_days(out, "ES")
    first_price = {name[-17:-7]: float(rows[1][1]) for name, rows in days.items()}
    # Mon: Z24 (no previous day: nearest month). Tue: still Z24 (Monday's volume).
    # Wed: H25 (Tuesday's volume). Thu: H25 (Wednesday's Z24 volume can't pull it back).
    assert first_price == {"2024-12-09": 6000, "2024-12-10": 6001, "2024-12-11": 6052, "2024-12-12": 6053}
    with (out / "ES" / "rolls.csv").open() as f:
        rolls = list(csv.DictReader(f))
    assert rolls == [{"day": "2024-12-11", "from_contract": "ESZ24", "to_contract": "ESH25",
                      "gap_points": "50.0", "detected_by": "volume"}]
    assert days["ES-1m-2024-12-09.csv.gz"][0] == fr.HEADER


def test_zips_are_read_the_manifest_names_every_file_and_the_lake_is_append_only(tmp_path):
    z = tmp_path / "es.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("ES_1min/ES_Z24_1min.txt", "\n".join(bars(date(2024, 12, 9), "09:30:00", 3, 6000, 5)))
        zf.writestr("readme.txt", "ignored")
    out = tmp_path / "lake"
    fr.run(out, [z])
    lines = [json.loads(x) for x in (out / "_manifest.jsonl").read_text().splitlines()]
    assert lines[0]["source_file"] == str(z) and len(lines[0]["sha256"]) == 64
    assert lines[1]["path"] == "ES/1m/ES-1m-2024-12-09.csv.gz" and lines[1]["contract"] == "ESZ24"
    with pytest.raises(fr.ImportError_, match="append-only"):
        fr.run(out, [z])


def test_the_search_loads_what_was_imported(tmp_path):
    """The lake folder reads through the edge search's own CME loader."""
    es = Path(__file__).resolve().parents[2] / "edge-search"
    if not (es / "edge_search" / "dataset.py").exists():
        pytest.skip("edge-search isn't beside this bot")
    pytest.importorskip("numpy")  # the search's own dependency, not this bot's
    sys.path.insert(0, str(es))
    from edge_search.dataset import load_cme_minutes, parse_spec

    src = tmp_path / "src"
    src.mkdir()
    write(src, "ES_Z24_1min.txt", bars(date(2024, 12, 9), "09:30:00", 390, 6000, 50))
    out = tmp_path / "lake" / "raw" / "cme" / "firstrate"
    fr.run(out, [src])
    m = load_cme_minutes(tmp_path / "lake" / "raw", parse_spec("raw/cme/firstrate 1m 2024-12-09..2024-12-10"), "ES")
    assert len(m.open_ms) == 390
    assert m.open_ms[0] == int(datetime(2024, 12, 9, 14, 30, tzinfo=UTC).timestamp() * 1000)
