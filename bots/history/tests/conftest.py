import asyncio
import hashlib
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from history import config, core  # noqa: E402

TODAY = date(2026, 10, 27)


@pytest.fixture
def make_cfg(tmp_path):
    """A Config for a temp DATA_DIR; keyword args override env values."""

    def make(**env):
        base = {
            "HISTORY_MARKET": "spot",
            "HISTORY_SYMBOLS": "BTCUSDT",
            "HISTORY_START": "2026-08",
            "HISTORY_BASE_URL": "https://archive.test",
            "DATA_DIR": str(tmp_path / "data"),
        }
        base.update(env)
        return config.load(base)

    return make


class Archive:
    """A fake data.binance.vision: files by URL; everything else is a 404."""

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.calls: list[str] = []
        self.bad: set[str] = set()

    def publish(self, url: str, data: bytes, checksum: str | None = None):
        name = url.rsplit("/", 1)[1]
        self.files[url] = data
        digest = checksum or hashlib.sha256(data).hexdigest()
        self.files[url + ".CHECKSUM"] = f"{digest}  {name}\n".encode()

    async def fetch(self, url: str):
        self.calls.append(url)
        if url in self.bad:
            return 503, b""
        if url in self.files:
            return 200, self.files[url]
        return 404, b""


@pytest.fixture
def archive():
    return Archive()


@pytest.fixture
def cycle():
    """Runs one check as of TODAY, with fresh state objects read from disk
    (as a restarted process would), and returns its Status."""

    def run(cfg, archive, today=TODAY):
        cfg.data_dir.mkdir(parents=True, exist_ok=True)
        status = core.Status(cfg)
        memo = core.NotPublished(cfg.data_dir)
        manifest = core.Manifest(cfg.data_dir)
        asyncio.run(core.run_cycle(cfg, archive.fetch, status, memo, manifest, today))
        return status

    return run
