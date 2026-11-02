from datetime import date

from cme_history import stitch

CONTRACTS = [
    {"ticker": "ESZ6", "last_trade_date": "2026-12-18"},
    {"ticker": "ESU6", "last_trade_date": "2026-09-18"},
    {"ticker": "ESH7", "last_trade_date": "2027-03-19"},
    {"ticker": "ESM7", "last_trade_date": "2027-06-18"},
]


def test_candidates_are_the_nearest_live_expiries():
    assert stitch.candidates(CONTRACTS, date(2026, 9, 14)) == ["ESU6", "ESZ6", "ESH7"]
    assert stitch.candidates(CONTRACTS, date(2026, 9, 21)) == ["ESZ6", "ESH7", "ESM7"]


def test_the_most_traded_contract_yesterday_wins_and_never_backwards():
    order = ["ESU6", "ESZ6", "ESH7"]
    assert stitch.choose(None, order, {"ESU6": 900, "ESZ6": 100}) == "ESU6"
    assert stitch.choose("ESU6", order, {"ESU6": 400, "ESZ6": 600}) == "ESZ6"
    # Back on the old contract's volume: never backwards.
    assert stitch.choose("ESZ6", order, {"ESU6": 900, "ESZ6": 100}) == "ESZ6"
    # No volume yet: stay, or take the nearest.
    assert stitch.choose("ESZ6", order, {}) == "ESZ6"
    assert stitch.choose(None, order, {}) == "ESU6"
    # A tie goes to the nearer expiry.
    assert stitch.choose(None, order, {"ESU6": 5, "ESZ6": 5}) == "ESU6"
    assert stitch.choose("ESU6", [], {}) is None


def test_the_roll_gap_is_new_minus_old_at_the_first_shared_minute():
    old = [(1, 0, 0, 0, 5000.0, 1), (2, 0, 0, 0, 5001.0, 1)]
    new = [(2, 0, 0, 0, 5061.5, 1), (3, 0, 0, 0, 5062.0, 1)]
    assert stitch.roll_gap(old, new) == 60.5
    assert stitch.roll_gap(old, [(9, 0, 0, 0, 1.0, 1)]) is None
