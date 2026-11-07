"""KuCoin's public market data, for the history recorder's kucoin-* markets.

Binance publishes files with checksums; KuCoin only answers API queries. This
module turns one plan.Target (a month or a day of one series) into rows by
paging the API, and writes them as the lake's file: a gzipped CSV with a
header, rows in time order, values exactly as the API sent them.

Endpoints (public, no key). Each one pages differently, and the spot one also
orders its columns differently; all three traps cost a silent loss of rows:

  futures candles  GET {futures}/api/v1/kline/query
                   ?symbol=XBTUSDTM&granularity=<minutes>&from=<ms>&to=<ms>
                   rows [t_ms, open, high, low, close, volume, turnover],
                   OLDEST first, at most 200 per answer: page FORWARD.
  futures funding  GET {futures}/api/v1/contract/funding-rates
                   ?symbol=XBTUSDTM&from=<ms>&to=<ms>
                   [{symbol, fundingRate, timepoint}], NEWEST first, at most
                   100 per answer, so a wide window silently drops its START:
                   page BACKWARD.
  spot candles     GET {spot}/api/v1/market/candles
                   ?type=1day&symbol=BTC-USDT&startAt=<s>&endAt=<s>
                   rows [t_s, open, CLOSE, HIGH, LOW, volume, turnover] as
                   strings, NEWEST first, at most 1500 per answer: page
                   BACKWARD, and reorder the columns.

Written files (under DATA_DIR, like the Binance archive's but .csv.gz):
  <SYMBOL>/klines/<interval>/<SYMBOL>-<interval>-<YYYY-MM | YYYY-MM-DD>.csv.gz
      open_time_ms,open,high,low,close,volume,turnover
  <SYMBOL>/fundingRate/<SYMBOL>-fundingRate-<YYYY-MM | YYYY-MM-DD>.csv.gz
      funding_time_ms,funding_rate

A file is written once and never changed: plan.py only asks for periods that
have closed. Its manifest line records what the API covered (rows, first and
last time, and for candles how many slots are missing after the first row),
so a reader can tell a listing month from a gap. Zero rows is "missing", the
same answer as the archive's 404. A candle period that closed less than
SETTLE_MS ago and lacks its last slot is also "missing" (asked again next
check), so a lagging API is never frozen into the lake as a gap.

The gzip header carries no timestamp, so the same rows always give the same
bytes and the same sha256.

RATE LIMITS. KuCoin's public API allows about 2,000 request-weight per 30 s
per IP, and every KuCoin instance on a host shares that IP. Requests are
paced (MIN_INTERVAL_S apart per process, env KUCOIN_MIN_INTERVAL_S), and an
HTTP 429 or 5xx is retried after a growing pause (1, 2, 4 ... 32 s) before
it counts as an error. Found 2026-11-07: two instances with 16 symbols each,
unpaced, had 2,388 of 5,616 first-pass requests refused with 429 (nothing
wrong was written: a refused page fails its whole period).
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import os
import weakref
from datetime import datetime, timezone

from .plan import Target

FUTURES_URL = "https://api-futures.kucoin.com"
SPOT_URL = "https://api.kucoin.com"

#: interval -> (futures granularity in minutes, spot candle type, ms)
INTERVALS = {
    "1h": (60, "1hour", 3_600_000),
    "4h": (240, "4hour", 14_400_000),
    "1d": (1440, "1day", 86_400_000),
}
#: Spot pairs are asked for as BTCUSDT (the spawner's symbol format has no
#: dash) and sent to KuCoin as BTC-USDT.
SPOT_QUOTES = ("USDT", "USDC")

FUTURES_KLINE_CAP = 200
FUNDING_CAP = 100
SPOT_KLINE_CAP = 1500
SETTLE_MS = 2 * 86_400_000
#: More pages than any one period needs means the API is not advancing.
MAX_PAGES = 400
#: Seconds between requests from one process: 5 a second keeps two
#: instances on one IP well inside KuCoin's public limit.
MIN_INTERVAL_S = float(os.environ.get("KUCOIN_MIN_INTERVAL_S", "0.2"))
#: Pauses before retrying a 429 or a 5xx; then it counts as an error.
RETRY_DELAYS_S = (1, 2, 4, 8, 16, 32)

# One pacer per event loop (tests run many loops; a lock belongs to one).
_pacers: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, tuple[asyncio.Lock, list[float]]]" = (
    weakref.WeakKeyDictionary()
)


async def _pace() -> None:
    if MIN_INTERVAL_S <= 0:
        return
    loop = asyncio.get_running_loop()
    lock, last = _pacers.setdefault(loop, (asyncio.Lock(), [0.0]))
    async with lock:
        wait = last[0] + MIN_INTERVAL_S - loop.time()
        if wait > 0:
            await asyncio.sleep(wait)
        last[0] = loop.time()

KLINE_HEADER = "open_time_ms,open,high,low,close,volume,turnover"
FUNDING_HEADER = "funding_time_ms,funding_rate"


class KucoinError(RuntimeError):
    """An answer the recorder can't use; retried next check."""


def spot_pair(symbol: str) -> str | None:
    """BTCUSDT -> BTC-USDT, or None if the quote isn't one we take."""
    for q in SPOT_QUOTES:
        if symbol.endswith(q) and len(symbol) > len(q):
            return f"{symbol[: -len(q)]}-{q}"
    return None


def period_ms(t: Target) -> tuple[int, int]:
    """[start, end) of the target's period, in UTC milliseconds."""
    start = datetime(t.day.year, t.day.month, t.day.day, tzinfo=timezone.utc)
    last = t.last_day
    end = datetime(last.year, last.month, last.day, tzinfo=timezone.utc).timestamp() * 1000 + 86_400_000
    return int(start.timestamp() * 1000), int(end)


async def _get(fetch, url: str):
    for delay in (*RETRY_DELAYS_S, None):
        await _pace()
        status, body = await fetch(url)
        doc = None
        if status == 200:
            try:
                # Numbers stay the strings KuCoin sent: nothing is reformatted.
                doc = json.loads(body, parse_float=str, parse_int=str)
            except ValueError:
                raise KucoinError("KuCoin's answer is not JSON") from None
        # A rate limit comes as HTTP 429, or as code 429000 in a 200.
        limited = status == 429 or status >= 500 or (doc is not None and str(doc.get("code")) == "429000")
        if limited and delay is not None:
            await asyncio.sleep(delay)
            continue
        break
    if status != 200:
        raise KucoinError(f"KuCoin answered HTTP {status}")
    if str(doc.get("code")) != "200000":
        raise KucoinError(f"KuCoin code {doc.get('code')}: {str(doc.get('msg'))[:120]}")
    return doc.get("data")


async def futures_klines(fetch, base: str, symbol: str, interval: str, start: int, end: int) -> list[list[str]]:
    gran, _, step = INTERVALS[interval]
    rows: dict[int, list[str]] = {}
    frm = start
    for _ in range(MAX_PAGES):
        page = await _get(fetch, f"{base}/api/v1/kline/query?symbol={symbol}&granularity={gran}&from={frm}&to={end - 1}") or []
        last = None
        for r in page:
            t = int(r[0])
            if start <= t < end:
                rows[t] = [str(t)] + [str(v) for v in r[1:7]]
            last = t if last is None else max(last, t)
        if len(page) < FUTURES_KLINE_CAP or last is None or last + step >= end:
            return [rows[t] for t in sorted(rows)]
        if last + step <= frm:
            raise KucoinError("futures candles stopped advancing")
        frm = last + step
    raise KucoinError("futures candles: too many pages")


async def futures_funding(fetch, base: str, symbol: str, start: int, end: int) -> list[list[str]]:
    rows: dict[int, str] = {}
    to = end - 1
    for _ in range(MAX_PAGES):
        page = await _get(fetch, f"{base}/api/v1/contract/funding-rates?symbol={symbol}&from={start}&to={to}") or []
        oldest = None
        for r in page:
            t = int(r["timepoint"])
            if start <= t < end:
                rows[t] = str(r["fundingRate"])
            oldest = t if oldest is None else min(oldest, t)
        if len(page) < FUNDING_CAP or oldest is None or oldest <= start:
            return [[str(t), rows[t]] for t in sorted(rows)]
        if oldest - 1 >= to:
            raise KucoinError("funding history stopped advancing")
        to = oldest - 1
    raise KucoinError("funding history: too many pages")


async def spot_klines(fetch, base: str, symbol: str, interval: str, start: int, end: int) -> list[list[str]]:
    _, kind, _ = INTERVALS[interval]
    pair = spot_pair(symbol)
    if pair is None:
        raise KucoinError(f"{symbol} is not a {'/'.join(SPOT_QUOTES)} pair")
    rows: dict[int, list[str]] = {}
    end_s = (end - 1) // 1000
    start_s = start // 1000
    for _ in range(MAX_PAGES):
        page = await _get(fetch, f"{base}/api/v1/market/candles?type={kind}&symbol={pair}&startAt={start_s}&endAt={end_s}") or []
        oldest = None
        for r in page:
            t = int(r[0]) * 1000
            if start <= t < end:
                # KuCoin spot: [time, open, CLOSE, HIGH, LOW, volume, turnover]
                rows[t] = [str(t), r[1], r[3], r[4], r[2], r[5], r[6]]
            oldest = t if oldest is None else min(oldest, t)
        if len(page) < SPOT_KLINE_CAP or oldest is None or oldest <= start:
            return [rows[t] for t in sorted(rows)]
        nxt = oldest // 1000 - 1
        if nxt >= end_s:
            raise KucoinError("spot candles stopped advancing")
        end_s = nxt
    raise KucoinError("spot candles: too many pages")


def to_csv_gz(header: str, rows: list[list[str]]) -> bytes:
    text = header + "\n" + "".join(",".join(r) + "\n" for r in rows)
    return gzip.compress(text.encode("utf-8"), mtime=0)


def coverage(t: Target, rows: list[list[str]], start: int, end: int) -> dict:
    out = {"rows": len(rows), "first_ms": int(rows[0][0]), "last_ms": int(rows[-1][0])}
    if t.dataset == "klines":
        step = INTERVALS[t.interval][2]
        out["missing_after_first"] = (end - out["first_ms"]) // step - len(rows)
        out["missing_at_end"] = (end - step - out["last_ms"]) // step
    else:
        times = [int(r[0]) for r in rows]
        out["max_gap_ms"] = max((b - a for a, b in zip(times, times[1:])), default=0)
    return out


async def fetch_target(cfg, t: Target, fetch, manifest, now_ms: int) -> tuple[str, int]:
    """("present" | "downloaded" | "missing", bytes on disk); raises
    KucoinError (retried next check) on an answer it can't use."""
    path = t.path(cfg)
    if path.exists():
        return "present", path.stat().st_size
    start, end = period_ms(t)
    if t.dataset == "fundingRate":
        rows, header = await futures_funding(fetch, cfg.base_url, t.symbol, start, end), FUNDING_HEADER
        url = f"{cfg.base_url}/api/v1/contract/funding-rates?symbol={t.symbol}"
    elif cfg.market == "kucoin-futures":
        rows, header = await futures_klines(fetch, cfg.base_url, t.symbol, t.interval, start, end), KLINE_HEADER
        url = f"{cfg.base_url}/api/v1/kline/query?symbol={t.symbol}&granularity={INTERVALS[t.interval][0]}"
    else:
        rows, header = await spot_klines(fetch, cfg.base_url, t.symbol, t.interval, start, end), KLINE_HEADER
        url = f"{cfg.base_url}/api/v1/market/candles?type={INTERVALS[t.interval][1]}&symbol={spot_pair(t.symbol)}"
    if not rows:
        return "missing", 0
    cov = coverage(t, rows, start, end)
    if cov.get("missing_at_end", 0) > 0 and now_ms - end < SETTLE_MS:
        return "missing", 0  # just closed and not all there yet: ask again
    data = to_csv_gz(header, rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    with part.open("wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(part, path)
    manifest.append({
        "path": str(path.relative_to(cfg.data_dir)),
        "url": url,
        "sha256": hashlib.sha256(data).hexdigest(),
        "bytes": len(data),
        "dataset": t.dataset,
        "symbol": t.symbol,
        "interval": t.interval,
        "period": t.period,
        "date": t.stamp,
        "source": "api",
        **cov,
        "fetched_at_ms": now_ms,
    })
    return "downloaded", len(data)
