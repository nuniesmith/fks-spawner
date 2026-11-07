"""KuCoin markets: the API's paging and column traps, and the lake files.

FakeKucoin answers like the real API (checked against it on 2026-11-06):
futures candles oldest-first, at most 200; funding newest-first, at most 100;
spot candles newest-first, at most 1500, with columns [t, open, CLOSE, HIGH,
LOW, volume, turnover]. A recorder that ignores any of these loses rows
without an error, which is what these tests catch.
"""

import asyncio
import gzip
import json
from datetime import date, datetime, timezone
from urllib.parse import parse_qs, urlparse

import pytest

from conftest import TODAY
from history import config, core, kucoin, plan

H = 3_600_000
D = 86_400_000


def ms(y, m, d, h=0):
    return int(datetime(y, m, d, h, tzinfo=timezone.utc).timestamp() * 1000)


class FakeKucoin:
    def __init__(self):
        self.fut: dict[tuple[str, int], dict[int, list]] = {}
        self.funding: dict[str, dict[int, float]] = {}
        self.spot: dict[tuple[str, str], dict[int, list]] = {}
        self.calls: list[str] = []
        self.code = "200000"
        self.refuse = 0  # answer this many requests with HTTP 429 first
        self.refuse_in_body = 0  # ... or with code 429000 in a 200

    def futures_candles(self, symbol, gran, start, end, step):
        rows = self.fut.setdefault((symbol, gran), {})
        for t in range(start, end, step):
            p = 100.0 + (t - start) / step
            rows[t] = [t, p, p + 2, p - 1, p + 1, 10, 1000.5]

    def funding_every(self, symbol, start, end, step, rate=0.0001):
        f = self.funding.setdefault(symbol, {})
        for t in range(start, end, step):
            f[t] = rate

    def spot_candles(self, pair, kind, start, end, step):
        rows = self.spot.setdefault((pair, kind), {})
        for t in range(start, end, step):
            # [time_s, open, CLOSE, HIGH, LOW, volume, turnover] as strings
            rows[t // 1000] = [str(t // 1000), "1", "4", "5", "0.5", "10", "100"]

    async def fetch(self, url):
        self.calls.append(url)
        u = urlparse(url)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if self.refuse:
            self.refuse -= 1
            return 429, b""
        if self.refuse_in_body:
            self.refuse_in_body -= 1
            return 200, json.dumps({"code": "429000", "msg": "Too many requests"}).encode()
        if self.code != "200000":
            return 200, json.dumps({"code": self.code, "msg": "Too many requests"}).encode()
        if u.path == "/api/v1/kline/query":
            rows = self.fut.get((q["symbol"], int(q["granularity"])), {})
            sel = [rows[t] for t in sorted(rows) if int(q["from"]) <= t <= int(q["to"])][:200]
        elif u.path == "/api/v1/contract/funding-rates":
            f = self.funding.get(q["symbol"], {})
            sel = [{"symbol": q["symbol"], "fundingRate": f[t], "timepoint": t}
                   for t in sorted(f, reverse=True) if int(q["from"]) <= t <= int(q["to"])][:100]
        elif u.path == "/api/v1/market/candles":
            rows = self.spot.get((q["symbol"], q["type"]), {})
            sel = [rows[t] for t in sorted(rows, reverse=True) if int(q["startAt"]) <= t <= int(q["endAt"])][:1500]
        else:
            return 404, b""
        return 200, json.dumps({"code": "200000", "data": sel}).encode()


@pytest.fixture
def kc():
    return FakeKucoin()


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    """Pacing and back-off are real seconds; the tests that check them set their own."""
    monkeypatch.setattr(kucoin, "MIN_INTERVAL_S", 0.0)
    monkeypatch.setattr(kucoin, "RETRY_DELAYS_S", (0, 0, 0, 0, 0, 0))


@pytest.fixture
def kcfg(make_cfg):
    def make(**env):
        base = {"HISTORY_MARKET": "kucoin-futures", "HISTORY_SYMBOLS": "XBTUSDTM",
                "HISTORY_BASE_URL": "https://kucoin.test", "HISTORY_START": "2026-09"}
        base.update(env)
        return make_cfg(**base)
    return make


def target(cfg, dataset, period, day, interval=None):
    return plan.Target(dataset, cfg.symbols[0], interval, period, day, cfg.file_ext)


def fetch(cfg, kc, t, now=None):
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    m = core.Manifest(cfg.data_dir)
    now = now if now is not None else ms(2026, 11, 6)
    return asyncio.run(kucoin.fetch_target(cfg, t, kc.fetch, m, now))


def read_csv(path):
    lines = gzip.decompress(path.read_bytes()).decode().splitlines()
    return lines[0], [r.split(",") for r in lines[1:]]


def manifest(cfg):
    return [json.loads(x) for x in (cfg.data_dir / "_manifest.jsonl").read_text().splitlines()]


def test_futures_candles_page_forward_past_the_200_cap(kcfg, kc):
    cfg = kcfg(HISTORY_INTERVALS="1h")
    kc.futures_candles("XBTUSDTM", 60, ms(2026, 9, 1), ms(2026, 10, 1), H)
    t = target(cfg, "klines", "monthly", date(2026, 9, 1), "1h")
    assert fetch(cfg, kc, t)[0] == "downloaded"
    header, rows = read_csv(t.path(cfg))
    assert header == "open_time_ms,open,high,low,close,volume,turnover"
    assert len(rows) == 720 and rows[0][0] == str(ms(2026, 9, 1)) and rows[-1][0] == str(ms(2026, 9, 30, 23))
    assert [int(r[0]) for r in rows] == sorted(int(r[0]) for r in rows)
    assert len(kc.calls) == 4  # 200 + 200 + 200 + 120
    assert t.path(cfg).name == "XBTUSDTM-1h-2026-09.csv.gz"
    m = manifest(cfg)[0]
    assert (m["rows"], m["missing_after_first"], m["missing_at_end"], m["source"]) == (720, 0, 0, "api")


def test_funding_pages_backward_so_the_start_of_the_month_is_kept(kcfg, kc):
    cfg = kcfg(HISTORY_DATASETS="fundingRate")
    # 4-hourly funding: 180 settlements, more than one 100-row answer
    kc.funding_every("XBTUSDTM", ms(2026, 9, 1), ms(2026, 10, 1), 4 * H)
    t = target(cfg, "fundingRate", "monthly", date(2026, 9, 1))
    assert fetch(cfg, kc, t)[0] == "downloaded"
    header, rows = read_csv(t.path(cfg))
    assert header == "funding_time_ms,funding_rate"
    assert len(rows) == 180
    # one answer would have been the NEWEST 100 and silently lost these
    assert rows[0] == [str(ms(2026, 9, 1)), "0.0001"]
    assert manifest(cfg)[0]["max_gap_ms"] == 4 * H


def test_spot_pairs_are_dashed_and_columns_reordered(kcfg, kc):
    cfg = kcfg(HISTORY_MARKET="kucoin-spot", HISTORY_SYMBOLS="BTCUSDT", HISTORY_INTERVALS="1d")
    kc.spot_candles("BTC-USDT", "1day", ms(2026, 9, 1), ms(2026, 10, 1), D)
    t = target(cfg, "klines", "monthly", date(2026, 9, 1), "1d")
    assert fetch(cfg, kc, t)[0] == "downloaded"
    assert "symbol=BTC-USDT" in kc.calls[0] and "type=1day" in kc.calls[0]
    _, rows = read_csv(t.path(cfg))
    assert len(rows) == 30 and rows[0][0] == str(ms(2026, 9, 1))
    # KuCoin sent [t, open=1, CLOSE=4, HIGH=5, LOW=0.5, ...]: the file is o,h,l,c
    assert rows[0][1:] == ["1", "5", "0.5", "4", "10", "100"]


def test_spot_pages_backward_past_the_1500_cap(kcfg, kc):
    cfg = kcfg(HISTORY_MARKET="kucoin-spot", HISTORY_SYMBOLS="ETHUSDC", HISTORY_INTERVALS="1h")
    kc.spot_candles("ETH-USDC", "1hour", ms(2026, 7, 1), ms(2026, 10, 1), H)
    rows = asyncio.run(kucoin.spot_klines(kc.fetch, cfg.base_url, "ETHUSDC", "1h", ms(2026, 7, 1), ms(2026, 10, 1)))
    assert len(rows) == 2208 and rows[0][0] == str(ms(2026, 7, 1))
    assert len(kc.calls) == 2


def test_a_just_closed_day_missing_its_last_candle_waits(kcfg, kc):
    cfg = kcfg(HISTORY_INTERVALS="1h")
    kc.futures_candles("XBTUSDTM", 60, ms(2026, 10, 26), ms(2026, 10, 26, 23), H)  # 23 of 24
    t = target(cfg, "klines", "daily", date(2026, 10, 26), "1h")
    assert fetch(cfg, kc, t, now=ms(2026, 10, 27, 6))[0] == "missing"
    assert not t.path(cfg).exists()
    # long after, the API won't improve: written, with the gap on record
    assert fetch(cfg, kc, t, now=ms(2026, 11, 6))[0] == "downloaded"
    assert manifest(cfg)[0]["missing_at_end"] == 1


def test_a_listing_month_is_written_with_its_coverage(kcfg, kc):
    cfg = kcfg(HISTORY_INTERVALS="1d")
    kc.futures_candles("XBTUSDTM", 1440, ms(2026, 9, 10), ms(2026, 10, 1), D)
    t = target(cfg, "klines", "monthly", date(2026, 9, 1), "1d")
    assert fetch(cfg, kc, t)[0] == "downloaded"
    m = manifest(cfg)[0]
    assert (m["rows"], m["first_ms"], m["missing_after_first"]) == (21, ms(2026, 9, 10), 0)


def test_no_rows_is_missing_like_a_404(kcfg, kc, cycle):
    cfg = kcfg(HISTORY_DATASETS="fundingRate", HISTORY_START="2026-08")
    status = cycle(cfg, kc)
    s = status.series["fundingRate/XBTUSDTM/-"]
    # 2 months older than 45 days -> never (Aug); Sep + 35 days -> not yet
    assert (s["present"], s["not_published"], s["pending"], s["errors"]) == (0, 1, 36, 0)
    assert not list(cfg.data_dir.glob("XBTUSDTM/**/*.csv.gz"))


def test_a_rate_limit_is_retried_and_the_period_still_lands(kcfg, kc):
    cfg = kcfg(HISTORY_DATASETS="fundingRate")
    kc.funding_every("XBTUSDTM", ms(2026, 9, 1), ms(2026, 10, 1), 8 * H)
    kc.refuse, kc.refuse_in_body = 2, 1  # 429, 429, then code 429000, then the data
    t = target(cfg, "fundingRate", "monthly", date(2026, 9, 1))
    assert fetch(cfg, kc, t)[0] == "downloaded"
    assert len(kc.calls) == 4 and len(read_csv(t.path(cfg))[1]) == 90


def test_a_rate_limit_that_never_lifts_is_an_error_and_writes_nothing(kcfg, kc, cycle):
    cfg = kcfg(HISTORY_DATASETS="fundingRate")
    kc.funding_every("XBTUSDTM", ms(2026, 9, 1), ms(2026, 10, 27), 8 * H)
    kc.refuse = 10 ** 6
    status = cycle(cfg, kc)
    assert status.cycle["errors"] > 0 and "HTTP 429" in status.recent_errors[0]["error"]
    # every attempt: the first try plus one per back-off step
    assert len(kc.calls) == status.cycle["planned"] * (len(kucoin.RETRY_DELAYS_S) + 1)
    assert not list(cfg.data_dir.glob("XBTUSDTM/**/*.csv.gz"))


def test_requests_are_paced(monkeypatch):
    monkeypatch.setattr(kucoin, "MIN_INTERVAL_S", 0.05)

    async def three():
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        for _ in range(3):
            await kucoin._pace()
        return loop.time() - t0

    assert asyncio.run(three()) >= 0.1  # the 2nd and 3rd wait 0.05 s each


def test_an_error_code_is_an_error_and_writes_nothing(kcfg, kc, cycle):
    cfg = kcfg(HISTORY_DATASETS="fundingRate")
    kc.funding_every("XBTUSDTM", ms(2026, 9, 1), ms(2026, 10, 27), 8 * H)
    kc.code = "429000"
    status = cycle(cfg, kc)
    assert status.cycle["errors"] > 0 and status.last_success_ms is None
    assert "429000" in status.recent_errors[0]["error"]
    assert not list(cfg.data_dir.glob("XBTUSDTM/**/*.csv.gz"))


def test_a_full_check_writes_months_and_days_and_skips_them_next_time(kcfg, kc, cycle):
    cfg = kcfg(HISTORY_INTERVALS="1d", HISTORY_DATASETS="klines,fundingRate")
    kc.futures_candles("XBTUSDTM", 1440, ms(2026, 9, 1), ms(2026, 10, 27), D)
    kc.funding_every("XBTUSDTM", ms(2026, 9, 1), ms(2026, 10, 27), 8 * H)
    status = cycle(cfg, kc, today=date(2026, 10, 29))  # 2 days after the data ends: settled
    k = status.series["klines/XBTUSDTM/1d"]
    f = status.series["fundingRate/XBTUSDTM/-"]
    assert (k["present"], k["errors"]) == (1 + 33, 0)  # Sep, and Sep 24..Oct 26 (Oct 27-28 have no rows)
    assert (f["present"], f["errors"]) == (1 + 33, 0)
    calls = len(kc.calls)
    cycle(cfg, kc, today=date(2026, 10, 29))
    assert len(kc.calls) - calls == 4  # only Oct 27-28, for both datasets, are asked again


def test_files_are_byte_stable():
    rows = [["1", "2"], ["3", "4"]]
    assert kucoin.to_csv_gz("a,b", rows) == kucoin.to_csv_gz("a,b", rows)


def test_config_for_kucoin(make_cfg):
    cfg = make_cfg(HISTORY_MARKET="kucoin-futures", HISTORY_SYMBOLS="XBTUSDTM,ETHUSDTM",
                   HISTORY_DATASETS="klines,fundingRate", HISTORY_BASE_URL="")
    assert cfg.base_url == "https://api-futures.kucoin.com" and cfg.file_ext == "csv.gz"
    assert cfg.periods("fundingRate") == ("monthly", "daily")
    spot = make_cfg(HISTORY_MARKET="kucoin-spot", HISTORY_SYMBOLS="BTCUSDT", HISTORY_BASE_URL="")
    assert spot.base_url == "https://api.kucoin.com"
    with pytest.raises(config.ConfigError, match="USDT/USDC pairs"):
        make_cfg(HISTORY_MARKET="kucoin-spot", HISTORY_SYMBOLS="BTCEUR")
    with pytest.raises(config.ConfigError, match="offered"):
        make_cfg(HISTORY_MARKET="kucoin-futures", HISTORY_SYMBOLS="XBTUSDTM", HISTORY_INTERVALS="1m")
    with pytest.raises(config.ConfigError, match="doesn't publish"):
        make_cfg(HISTORY_MARKET="kucoin-spot", HISTORY_SYMBOLS="BTCUSDT", HISTORY_DATASETS="fundingRate")


def test_kucoin_plan_keeps_funding_daily_too(make_cfg):
    cfg = make_cfg(HISTORY_MARKET="kucoin-futures", HISTORY_SYMBOLS="XBTUSDTM",
                   HISTORY_DATASETS="fundingRate", HISTORY_START="2026-09")
    ts = plan.targets(cfg, TODAY)
    assert [t.stamp for t in ts if t.period == "monthly"] == ["2026-09"]
    assert len([t for t in ts if t.period == "daily"]) == 35
    assert ts[0].filename.endswith(".csv.gz")


def test_binance_files_are_unchanged(make_cfg):
    cfg = make_cfg(HISTORY_INTERVALS="1d")
    t = next(t for t in plan.targets(cfg, TODAY) if t.period == "monthly")
    assert t.filename == "BTCUSDT-1d-2026-08.zip" and cfg.file_ext == "zip"
