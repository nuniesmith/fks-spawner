import hashlib
import json

from conftest import TODAY
from history import core

SEP = "https://archive.test/data/spot/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2026-09.zip"


def test_downloads_are_verified_recorded_and_counted(make_cfg, archive, cycle):
    cfg = make_cfg(HISTORY_INTERVALS="1d", HISTORY_START="2026-09")
    a = archive
    sep = SEP
    a.publish(sep, b"1756684800000,108000,110000,107000,109000,123\n")
    status = cycle(cfg, a)
    path = cfg.data_dir / "BTCUSDT" / "klines" / "1d" / "BTCUSDT-1d-2026-09.zip"
    assert path.read_bytes() == a.files[sep]
    lines = [json.loads(x) for x in (cfg.data_dir / "_manifest.jsonl").read_text().splitlines()]
    assert len(lines) == 1 and lines[0]["path"] == "BTCUSDT/klines/1d/BTCUSDT-1d-2026-09.zip"
    assert lines[0]["sha256"] == hashlib.sha256(a.files[sep]).hexdigest()
    s = status.series["klines/BTCUSDT/1d"]
    # 1 monthly present; the 35 daily files are recent, so "not yet"
    assert (s["present"], s["pending"], s["not_published"], s["errors"]) == (1, 35, 0, 0)
    assert s["first"] == s["last"] == "2026-09"
    assert status.last_success_ms is not None


def test_a_checksum_mismatch_writes_nothing_and_is_retried(make_cfg, archive, cycle):
    cfg = make_cfg(HISTORY_INTERVALS="1d", HISTORY_START="2026-09")
    a = archive
    sep = SEP
    a.publish(sep, b"tampered", checksum="0" * 64)
    status = cycle(cfg, a)
    folder = cfg.data_dir / "BTCUSDT" / "klines" / "1d"
    assert not folder.exists() or list(folder.iterdir()) == []  # no file, no .part
    assert status.series["klines/BTCUSDT/1d"]["errors"] == 1
    assert status.last_success_ms is None
    assert "sha256" in status.recent_errors[0]["error"]


def test_an_old_404_is_never_and_skipped_next_cycle_a_recent_one_is_retried(make_cfg, archive, cycle):
    cfg = make_cfg(HISTORY_INTERVALS="1d", HISTORY_START="2026-07")
    a = archive
    first = cycle(cfg, a)
    s = first.series["klines/BTCUSDT/1d"]
    # July and August ended over 45 days ago: never published (before the
    # symbol listed, say). September ended 27 days ago: not yet.
    assert (s["not_published"], s["pending"]) == (2, 1 + 35)
    a.calls.clear()
    second = cycle(cfg, a)
    assert not any("2026-07.zip" in c or "2026-08.zip" in c for c in a.calls)
    assert second.series["klines/BTCUSDT/1d"]["not_published"] == 2
    assert any("2026-09.zip" in c for c in a.calls)  # recent: asked again
    assert any("2026-10-26" in c for c in a.calls)


def test_a_present_file_is_not_fetched_again(make_cfg, archive, cycle):
    cfg = make_cfg(HISTORY_INTERVALS="1d", HISTORY_START="2026-09")
    a = archive
    sep = SEP
    a.publish(sep, b"x")
    cycle(cfg, a)
    a.calls.clear()
    status = cycle(cfg, a)
    assert sep not in a.calls and sep + ".CHECKSUM" not in a.calls
    assert status.series["klines/BTCUSDT/1d"]["present"] == 1


def test_a_server_error_is_an_error_not_a_404(make_cfg, archive, cycle):
    cfg = make_cfg(HISTORY_INTERVALS="1d", HISTORY_START="2026-09")
    a = archive
    sep = SEP
    a.publish(sep, b"x")
    a.bad.add(sep + ".CHECKSUM")
    status = cycle(cfg, a)
    s = status.series["klines/BTCUSDT/1d"]
    assert (s["errors"], s["not_published"]) == (1, 0)


def test_parse_checksum():
    digest = "a" * 64
    assert core.parse_checksum(f"{digest}  X.zip\n", "X.zip") == digest
    assert core.parse_checksum(f"{digest}  Y.zip\n", "X.zip") is None
    assert core.parse_checksum("not a checksum", "X.zip") is None


def test_counts_stay_until_the_next_check_finishes(make_cfg, archive, cycle):
    """A gauge that dipped to zero while a check ran would reset every alert
    that holds on it, so the last finished check's counts stay up."""
    import asyncio

    cfg = make_cfg(HISTORY_INTERVALS="1d", HISTORY_START="2026-09")
    archive.publish(SEP, b"x")
    archive.bad.add(SEP.replace("2026-09", "2026-10-01").replace("monthly", "daily") + ".CHECKSUM")
    status = cycle(cfg, archive)
    assert status.series["klines/BTCUSDT/1d"]["errors"] == 1
    seen: list[int] = []

    async def slow(url: str):
        seen.append(status.series["klines/BTCUSDT/1d"]["errors"])
        return await archive.fetch(url)

    asyncio.run(core.run_cycle(cfg, slow, status, core.NotPublished(cfg.data_dir),
                               core.Manifest(cfg.data_dir), TODAY))
    assert seen and set(seen) == {1}  # the old counts, all through the check
    assert status.series["klines/BTCUSDT/1d"]["errors"] == 1
    assert (status.cycle["planned"], status.cycle["checked"]) == (36, 36)
