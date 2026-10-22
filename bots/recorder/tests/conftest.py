import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from recorder import config  # noqa: E402


@pytest.fixture
def make_cfg(tmp_path):
    """A Config for a temp DATA_DIR; keyword args override env values."""

    def make(**env):
        base = {"RECORDER_SYMBOLS": "BTCUSDT", "DATA_DIR": str(tmp_path / "data")}
        base.update({k: v for k, v in env.items()})
        return config.load(base)

    return make
