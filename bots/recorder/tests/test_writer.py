import asyncio
import gzip
from datetime import datetime, timezone

from recorder.core import DayWriter, utc_day


def ms(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp() * 1000)


def test_lines_go_to_the_utc_day_file(tmp_path):
    w = DayWriter(tmp_path / "BTCUSDT" / "forceOrder")

    async def go():
        w.write('{"a":1}', ms("2026-10-21T23:59:59"))
        w.close()

    asyncio.run(go())
    assert (tmp_path / "BTCUSDT" / "forceOrder" / "2026-10-21.jsonl").read_text() == '{"a":1}\n'
    assert w.lines_written == 1
    assert w.bytes_written == len('{"a":1}') + 1


def test_rotation_at_midnight_gzips_the_closed_day(tmp_path):
    d = tmp_path / "s"
    w = DayWriter(d)

    async def go():
        w.write("one", ms("2026-10-21T23:59:59"))
        w.write("two", ms("2026-10-22T00:00:01"))
        await asyncio.sleep(0.2)  # the gzip runs in an executor
        w.close()

    asyncio.run(go())
    assert not (d / "2026-10-21.jsonl").exists()
    with gzip.open(d / "2026-10-21.jsonl.gz", "rt") as fh:
        assert fh.read() == "one\n"
    assert (d / "2026-10-22.jsonl").read_text() == "two\n"


def test_a_backfilled_day_never_disturbs_the_open_file(tmp_path):
    w = DayWriter(tmp_path / "snapshots")

    async def go():
        w.write("live", ms("2026-10-22T10:00:00"))
        w.write("old", ms("2026-10-22T10:00:00"), file_day="2026-10-15")
        w.close()

    asyncio.run(go())
    assert (tmp_path / "snapshots" / "2026-10-15.jsonl").read_text() == "old\n"
    assert (tmp_path / "snapshots" / "2026-10-22.jsonl").read_text() == "live\n"


def test_utc_day():
    assert utc_day(ms("2026-10-22T00:00:00")) == "2026-10-22"
    assert utc_day(ms("2026-10-21T23:59:59")) == "2026-10-21"
