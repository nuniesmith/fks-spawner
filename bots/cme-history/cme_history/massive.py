"""Massive (massive.com) per-contract futures bars, rate-limit aware.

Ported from ~/github/futures src/optimizer/providers/massive.py (pandas-free).
The two traps it is built around:

  * An error arrives as HTTP 200: exceeding the rate limit returns a normal
    200 whose BODY says {"status": "ERROR", ...}. A client that checks only
    the status code reads that as success and records zero bars, a silent
    partial. `_get` reads the body and raises.
  * The free tier allows 5 requests a minute. That is the normal operating
    rate, so requests are paced BEFORE sending, not retried after.

Timestamps are Unix nanoseconds, UTC: no local-time guessing.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Callable

import requests

log = logging.getLogger("cme-history")

MAX_LIMIT = 50_000

#: One bar: open time (ms, UTC), open, high, low, close, volume.
Row = tuple[int, float, float, float, float, float]


class MassiveError(RuntimeError):
    """The API reported a failure, whatever the HTTP status said."""


class RateLimiter:
    """Never more than `per_minute` requests in any minute; no bursts."""

    def __init__(self, per_minute: int, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic):
        self.per_minute = per_minute
        self._interval = 60.0 / per_minute if per_minute > 0 else 0.0
        self._last: float | None = None
        self._sleep, self._clock = sleep, clock

    def wait(self) -> None:
        if self._interval <= 0:
            return
        now = self._clock()
        if self._last is not None and now - self._last < self._interval:
            self._sleep(self._interval - (now - self._last))
        self._last = self._clock()


class Massive:
    def __init__(self, api_key: str, *, per_minute: int = 5, base_url: str = "https://api.massive.com",
                 session: requests.Session | None = None, max_retries: int = 3,
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic):
        if not api_key.strip():
            raise MassiveError("no Massive API key")
        self._key = api_key.strip()
        self._base = base_url.rstrip("/")
        self._session = session or requests.Session()
        self._limiter = RateLimiter(per_minute, sleep, clock)
        self._max_retries = max_retries
        self._sleep = sleep
        self.requests_made = 0

    def _get(self, url: str, params: dict | None = None) -> dict:
        for attempt in range(self._max_retries + 1):
            self._limiter.wait()
            resp = self._session.get(url, params=params, timeout=60,
                                     headers={"Authorization": f"Bearer {self._key}"})
            self.requests_made += 1
            try:
                body = resp.json()
            except ValueError:
                raise MassiveError(f"non-JSON response (HTTP {resp.status_code}) from {url}") from None
            status = str(body.get("status", "")).upper()
            if resp.status_code == 429 or status == "ERROR":
                message = str(body.get("error") or body.get("message") or "")
                limited = resp.status_code == 429 or "per minute" in message.lower()
                if limited and attempt < self._max_retries:
                    wait = 60.0 / max(1, self._limiter.per_minute) * (attempt + 2)
                    log.warning("rate limited (attempt %d/%d), waiting %.0fs", attempt + 1, self._max_retries, wait)
                    self._sleep(wait)
                    continue
                raise MassiveError(f"{url}: {message or body}")
            if resp.status_code >= 400:
                raise MassiveError(f"HTTP {resp.status_code} from {url}: {body}")
            return body
        raise MassiveError(f"{url}: still rate limited after {self._max_retries} retries")

    def contracts(self, product: str, day: date) -> list[dict]:
        """The single contracts listed for `product` on `day` (point in time:
        the endpoint returns one row per contract per day, so the date is
        always sent), one row per ticker."""
        body = self._get(f"{self._base}/futures/v1/contracts", params={
            "product_code": product, "date": day.isoformat(), "active": "true",
            "type": "single", "limit": 1000,
        })
        seen: dict[str, dict] = {}
        for row in body.get("results") or []:
            t = row.get("ticker")
            if t and t not in seen:
                seen[t] = row
        return list(seen.values())

    def bars(self, ticker: str, day: date) -> list[Row]:
        """Every 1-minute bar of `ticker` that opens on UTC `day`."""
        lo = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)
        hi = lo + 86_400_000
        url: str | None = f"{self._base}/futures/v1/aggs/{ticker}"
        params: dict | None = {
            "resolution": "1min", "limit": MAX_LIMIT, "sort": "window_start.asc",
            "window_start.gte": day.isoformat(), "window_start.lte": (day + timedelta(1)).isoformat(),
        }
        rows: dict[int, Row] = {}
        pages = 0
        while url and pages < 50:
            body = self._get(url, params=params)
            batch = body.get("results") or []
            for r in batch:
                ms = int(r["window_start"]) // 1_000_000
                if lo <= ms < hi:
                    rows[ms] = (ms, float(r["open"]), float(r["high"]), float(r["low"]),
                                float(r["close"]), float(r.get("volume") or 0))
            pages += 1
            url = body.get("next_url")
            params = None  # next_url carries the cursor and the filters
            if not batch:
                break
        return [rows[k] for k in sorted(rows)]
