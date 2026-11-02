from datetime import date

import pytest

from cme_history.config import ConfigError, load

BASE = {"CME_ROOTS": "es, nq,GC", "MASSIVE_API_KEY": "k"}


def test_defaults():
    c = load(BASE)
    assert c.roots == ("ES", "NQ", "GC") and c.start == date(2026, 9, 14) and c.rpm == 5 and c.poll_hours == 6


@pytest.mark.parametrize("over,needle", [
    ({"CME_ROOTS": ""}, "CME_ROOTS is required"),
    ({"CME_ROOTS": "ES,ES"}, "twice"),
    ({"CME_ROOTS": "E-S"}, "product roots"),
    ({"MASSIVE_API_KEY": ""}, "massive"),
    ({"CME_START": "2026-13-01"}, "YYYY-MM-DD"),
    ({"CME_POLL_HOURS": "0"}, "between"),
])
def test_refusals(over, needle):
    with pytest.raises(ConfigError, match=needle):
        load({**BASE, **over})
