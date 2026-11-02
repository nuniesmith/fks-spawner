"""Which contract a root's series follows on a day: by volume, never back.

  * The day's contract is chosen on the PREVIOUS weekday's volume (known
    before the day starts: no look-ahead), among the contracts not expired.
  * Never backwards: once on a contract, only it and later expiries are
    candidates. The futures repo's cache rolled on expiry and on volume both,
    and on some roll days interleaved two contracts minute by minute
    (raw/cme/massive-cache roll_flips.csv); one contract per day, chosen
    once, can't.
  * Without any volume yet (a holiday-only history), stay on the current
    contract, or take the nearest expiry.
"""

from __future__ import annotations

from datetime import date

from .massive import Row


def expiry(contract: dict) -> date | None:
    for key in ("last_trade_date", "settlement_date"):
        v = contract.get(key)
        if v:
            try:
                return date.fromisoformat(str(v)[:10])
            except ValueError:
                continue
    return None


def candidates(contracts: list[dict], day: date, n: int = 3) -> list[str]:
    """The `n` nearest contracts still trading on `day`, nearest first."""
    live = [c for c in contracts if (e := expiry(c)) is None or e >= day]
    live.sort(key=lambda c: (expiry(c) or date.max, c["ticker"]))
    return [c["ticker"] for c in live[:n]]


def choose(current: str | None, order: list[str], prev_volume: dict[str, float]) -> str | None:
    if not order:
        return None
    pool = order[order.index(current):] if current in order else order
    best = max(pool, key=lambda t: (prev_volume.get(t, 0.0), -pool.index(t)))
    if prev_volume.get(best, 0.0) <= 0:
        return current if current in pool else pool[0]
    return best


def volume(rows: list[Row]) -> float:
    return float(sum(r[5] for r in rows))


def roll_gap(old: list[Row], new: list[Row]) -> float | None:
    """The new contract's close minus the old one's, at the first minute
    both traded on the roll day (positive in contango)."""
    old_close = {r[0]: r[4] for r in old}
    for r in new:
        if r[0] in old_close:
            return round(r[4] - old_close[r[0]], 6)
    return None
