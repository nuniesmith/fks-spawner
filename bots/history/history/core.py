"""Downloads the archive files plan.py asks for into the lake, verified, and
keeps them current.

Each file is fetched together with its `.CHECKSUM`; the data is written to
`<name>.part` and renamed into place only when its sha256 matches, so a file
present in the lake is always a complete, verified copy and a crash leaves at
most a `.part` behind. Every download is appended to `_manifest.jsonl`, the
dataset's lineage: which bytes, from where, with which checksum.

A 404 is an answer, not an error: the archive has no such file. Within
RECENT_DAYS of the period's end it means "not yet" and is retried every cycle;
older,
it means "never" (a month before the symbol listed, say) and is remembered in
`_state/not_published.json`, retried weekly in case the archive back-fills.

Files, under DATA_DIR (the instance's lake folder):
  <SYMBOL>/<dataset>/[<interval>/]<SYMBOL>-<interval|dataset>-<date>.zip
  _manifest.jsonl               one line per verified download
  _state/status.json            the heartbeat /status serves
  _state/not_published.json     404s old enough to mean "never"
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import signal
import time
from collections import deque
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable

import aiohttp

from .config import Config, load
from .plan import Target, targets

log = logging.getLogger("history")

RECENT_DAYS = 45
NOT_PUBLISHED_RETRY_SECONDS = 7 * 86400
HEARTBEAT_SECONDS = 10

# (HTTP status, body). A non-200 that is not 404 is an error worth retrying.
Fetch = Callable[[str], Awaitable[tuple[int, bytes]]]


class ChecksumMismatch(Exception):
    pass


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def parse_checksum(text: str, filename: str) -> str | None:
    """`<sha256>  <filename>` -> the sha256, if the line names this file."""
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1] == filename and len(parts[0]) == 64:
            return parts[0].lower()
    return None


def write_json_atomic(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)


class NotPublished:
    """404s old enough to mean the archive will never have the file."""

    def __init__(self, data_dir: Path):
        self.path = data_dir / "_state" / "not_published.json"
        try:
            self.seen: dict[str, int] = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            self.seen = {}

    def skip(self, filename: str, at_ms: int) -> bool:
        seen = self.seen.get(filename)
        return seen is not None and at_ms - seen < NOT_PUBLISHED_RETRY_SECONDS * 1000

    def add(self, filename: str, at_ms: int) -> None:
        self.seen[filename] = at_ms

    def save(self) -> None:
        write_json_atomic(self.path, self.seen)


class Manifest:
    """Append-only: one JSON line per verified download."""

    def __init__(self, data_dir: Path):
        self.path = data_dir / "_manifest.jsonl"

    def append(self, entry: dict) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, separators=(",", ":")) + "\n")


def _series_key(t: Target) -> str:
    return "/".join(t.series)


class Status:
    """What /status and /metrics report, persisted to _state/status.json."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.started_at_ms = now_ms()
        self.updated_at_ms = self.started_at_ms
        self.series: dict[str, dict] = {}
        self.cycle: dict = {
            "running": False,
            "started_at_ms": None,
            "finished_at_ms": None,
            "next_at_ms": None,
            "downloaded": 0,
            "errors": 0,
        }
        self.last_success_ms: int | None = None
        self.downloads_total = 0
        self.errors_total = 0
        self.recent_errors: deque[dict] = deque(maxlen=20)

    @property
    def path(self) -> Path:
        return self.cfg.data_dir / "_state" / "status.json"

    def reset_series(self, plan: list[Target]) -> None:
        series: dict[str, dict] = {}
        for t in plan:
            s = series.setdefault(_series_key(t), {
                "dataset": t.dataset, "symbol": t.symbol, "interval": t.interval,
                "expected": 0, "present": 0, "pending": 0, "not_published": 0, "errors": 0,
                "bytes": 0, "first": None, "last": None,
            })
            s["expected"] += 1
        self.series = series

    def count(self, t: Target, state: str, size: int = 0) -> None:
        s = self.series[_series_key(t)]
        s[state] += 1
        if state == "present":
            s["bytes"] += size
            s["first"] = t.stamp if s["first"] is None or t.stamp < s["first"] else s["first"]
            s["last"] = t.stamp if s["last"] is None or t.stamp > s["last"] else s["last"]

    def snapshot(self) -> dict:
        return {
            "bot_id": self.cfg.bot_id,
            "kind": "history",
            "market": self.cfg.market,
            "symbols": list(self.cfg.symbols),
            "datasets": list(self.cfg.datasets),
            "intervals": list(self.cfg.intervals),
            "start": self.cfg.start.strftime("%Y-%m"),
            "started_at_ms": self.started_at_ms,
            "updated_at_ms": self.updated_at_ms,
            "cycle": dict(self.cycle),
            "last_success_ms": self.last_success_ms,
            "downloads_total": self.downloads_total,
            "errors_total": self.errors_total,
            "series": self.series,
            "recent_errors": list(self.recent_errors),
        }

    def write(self) -> None:
        self.updated_at_ms = now_ms()
        write_json_atomic(self.path, self.snapshot())


async def fetch_one(cfg: Config, t: Target, fetch: Fetch, manifest: Manifest) -> tuple[str, int]:
    """("present" | "downloaded" | "missing", bytes on disk). Raises on an
    error worth retrying: a non-200/404 answer, a bad checksum file, or data
    that does not match its checksum (then nothing is written)."""
    path = t.path(cfg)
    if path.exists():
        return "present", path.stat().st_size
    url = t.url(cfg)
    status, body = await fetch(url + ".CHECKSUM")
    if status == 404:
        return "missing", 0
    if status != 200:
        raise RuntimeError(f"CHECKSUM answered {status}")
    want = parse_checksum(body.decode("utf-8", "replace"), t.filename)
    if want is None:
        raise RuntimeError("CHECKSUM does not name this file")
    status, data = await fetch(url)
    if status == 404:
        return "missing", 0
    if status != 200:
        raise RuntimeError(f"archive answered {status}")
    got = hashlib.sha256(data).hexdigest()
    if got != want:
        raise ChecksumMismatch(f"sha256 {got[:12]}… != published {want[:12]}…")
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
        "sha256": got,
        "bytes": len(data),
        "dataset": t.dataset,
        "symbol": t.symbol,
        "interval": t.interval,
        "period": t.period,
        "date": t.stamp,
        "fetched_at_ms": now_ms(),
    })
    return "downloaded", len(data)


async def run_cycle(cfg: Config, fetch: Fetch, status: Status, memo: NotPublished,
                    manifest: Manifest, today: date) -> None:
    plan = targets(cfg, today)
    status.reset_series(plan)
    started = now_ms()
    status.cycle.update(running=True, started_at_ms=started, downloaded=0, errors=0)
    sem = asyncio.Semaphore(cfg.concurrency)

    async def one(t: Target) -> None:
        if memo.skip(t.filename, started):
            status.count(t, "not_published")
            return
        async with sem:
            try:
                state, size = await fetch_one(cfg, t, fetch, manifest)
            except Exception as exc:  # retried next cycle
                status.count(t, "errors")
                status.cycle["errors"] += 1
                status.errors_total += 1
                status.recent_errors.append({"file": t.filename, "error": str(exc)[:200], "at_ms": now_ms()})
                log.warning("%s: %s", t.filename, exc)
                return
        if state == "missing":
            if (today - t.last_day).days > RECENT_DAYS:
                memo.add(t.filename, started)
                status.count(t, "not_published")
            else:
                status.count(t, "pending")
            return
        if state == "downloaded":
            status.cycle["downloaded"] += 1
            status.downloads_total += 1
            log.info("downloaded %s (%d bytes)", t.filename, size)
        status.count(t, "present", size)

    await asyncio.gather(*(one(t) for t in plan))
    memo.save()
    finished = now_ms()
    status.cycle.update(running=False, finished_at_ms=finished)
    if status.cycle["errors"] == 0:
        status.last_success_ms = finished
    status.write()
    log.info(
        "cycle done: %d files planned, %d downloaded, %d errors",
        len(plan), status.cycle["downloaded"], status.cycle["errors"],
    )


async def heartbeat(status: Status, stop: asyncio.Event) -> None:
    while not stop.is_set():
        status.write()
        try:
            await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_SECONDS)
        except asyncio.TimeoutError:
            pass


async def run(cfg: Config, fetch: Fetch | None = None, serve=None, stop: asyncio.Event | None = None,
              today: Callable[[], date] = utc_today) -> Status:
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    status = Status(cfg)
    memo = NotPublished(cfg.data_dir)
    manifest = Manifest(cfg.data_dir)
    log.info(
        "history %s starting: %s %s, datasets %s, intervals %s, from %s, every %sh, data %s",
        cfg.bot_id, cfg.market, ",".join(cfg.symbols), ",".join(cfg.datasets),
        ",".join(cfg.intervals) or "-", cfg.start.strftime("%Y-%m"), cfg.poll_hours, cfg.data_dir,
    )
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=300),
        headers={"user-agent": "fks-history-recorder"},
    ) as session:
        if fetch is None:
            async def fetch(url: str) -> tuple[int, bytes]:
                async with session.get(url) as resp:
                    return resp.status, (await resp.read() if resp.status == 200 else b"")

        tasks = [asyncio.create_task(heartbeat(status, stop), name="heartbeat")]
        if serve is not None:
            tasks.append(asyncio.create_task(serve(status, stop), name="http"))
        while not stop.is_set():
            await run_cycle(cfg, fetch, status, memo, manifest, today())
            status.cycle["next_at_ms"] = now_ms() + int(cfg.poll_hours * 3600 * 1000)
            try:
                await asyncio.wait_for(stop.wait(), timeout=cfg.poll_hours * 3600)
            except asyncio.TimeoutError:
                pass
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    status.write()
    return status


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    cfg = load()
    from .server import serve

    asyncio.run(run(cfg, serve=serve))
