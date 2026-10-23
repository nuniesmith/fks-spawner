"""Recorder configuration, read from the environment.

The spawner sets these from a recorder template's form (EDGE_FACTORY_PLAN
§5.2); the same names work by hand with `docker run -e ...`.

  RECORDER_VENUE         venue id; only `binance-usdm` so far
  RECORDER_SYMBOLS       required, comma-separated, e.g. BTCUSDT,ETHUSDT
  RECORDER_STREAMS       websocket streams (default: all the venue offers)
  RECORDER_REST          REST snapshot kinds (default: all the venue offers)
  RECORDER_REST_SECONDS  seconds between REST snapshots (default 300, min 60)
  RECORDER_BACKFILL_OI   1 = also backfill open interest from Binance Vision
                         daily metrics files, as the btc recorder did when its
                         REST access was geo-blocked (default 0)
  DATA_DIR               where files go (default /data)
  RECORDER_PORT          the /health, /metrics, /status port (default 9091)
  FKS_BOT_ID             set by the spawner; names this instance in logs
  HTTPS_PROXY            optional egress proxy
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


class ConfigError(ValueError):
    """A setting the recorder can't run with; the message says which."""


@dataclass(frozen=True)
class Venue:
    name: str
    rest_base: str
    # connection group -> combined-stream websocket base URL
    ws_bases: dict[str, str]
    # stream, as named in RECORDER_STREAMS -> the connection group it needs
    streams: dict[str, str]
    # REST snapshot kind -> (path, key in the recorded snapshot)
    rest: dict[str, tuple[str, str]]
    # daily metrics zip, formatted with {symbol} and {day}
    vision_metrics: str | None = None


# Binance split its futures websocket streams across /public, /market and
# /private base paths, and the legacy combined base acknowledges a market
# subscription but never delivers a message (found by the btc recorder on
# 2026-10-21), so each base path gets its own connection.
BINANCE_USDM = Venue(
    name="binance-usdm",
    rest_base="https://fapi.binance.com",
    ws_bases={
        "market": "wss://fstream.binance.com/market/stream",
        "public": "wss://fstream.binance.com/public/stream",
    },
    streams={
        "forceOrder": "market",
        "markPrice@1s": "market",
        "bookTicker": "public",
    },
    rest={
        "openInterest": ("/fapi/v1/openInterest", "open_interest"),
        "premiumIndex": ("/fapi/v1/premiumIndex", "premium_index"),
    },
    vision_metrics=(
        "https://data.binance.vision/data/futures/um/daily/metrics/"
        "{symbol}/{symbol}-metrics-{day}.zip"
    ),
)

VENUES = {v.name: v for v in (BINANCE_USDM,)}

SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,20}$")


@dataclass(frozen=True)
class Config:
    venue: Venue
    symbols: tuple[str, ...]
    streams: tuple[str, ...]
    rest: tuple[str, ...]
    rest_seconds: int
    backfill_oi: bool
    data_dir: Path
    port: int
    bot_id: str
    proxy: str | None

    def subscriptions(self) -> dict[str, list[str]]:
        """Connection group -> the stream names to subscribe on it, as the
        venue spells them (e.g. `btcusdt@forceOrder`)."""
        groups: dict[str, list[str]] = {}
        for stream in self.streams:
            group = self.venue.streams[stream]
            for symbol in self.symbols:
                groups.setdefault(group, []).append(f"{symbol.lower()}@{stream}")
        return groups


def stream_dir(stream: str) -> str:
    """The folder a stream's files go in: `markPrice@1s` -> `markPrice`."""
    return stream.split("@", 1)[0]


def _list(raw: str | None) -> list[str]:
    if raw is None:
        return []
    out: list[str] = []
    for part in raw.split(","):
        part = part.strip()
        if part and part not in out:
            out.append(part)
    return out


def load(env: Mapping[str, str] | None = None) -> Config:
    env = os.environ if env is None else env

    venue_name = env.get("RECORDER_VENUE", "binance-usdm").strip()
    venue = VENUES.get(venue_name)
    if venue is None:
        raise ConfigError(f"RECORDER_VENUE {venue_name!r} is not one of {sorted(VENUES)}")

    symbols = [s.upper() for s in _list(env.get("RECORDER_SYMBOLS"))]
    if not symbols:
        raise ConfigError("RECORDER_SYMBOLS is required, e.g. BTCUSDT")
    bad = [s for s in symbols if not SYMBOL_RE.match(s)]
    if bad:
        raise ConfigError(f"RECORDER_SYMBOLS has symbols that aren't a venue symbol: {bad}")
    symbols = list(dict.fromkeys(symbols))

    if "RECORDER_STREAMS" in env:
        streams = _list(env["RECORDER_STREAMS"])
    else:
        streams = list(venue.streams)
    unknown = [s for s in streams if s not in venue.streams]
    if unknown:
        raise ConfigError(
            f"RECORDER_STREAMS has streams {venue.name} doesn't offer: {unknown} "
            f"(offered: {list(venue.streams)})"
        )

    if "RECORDER_REST" in env:
        rest = _list(env["RECORDER_REST"])
    else:
        rest = list(venue.rest)
    unknown = [r for r in rest if r not in venue.rest]
    if unknown:
        raise ConfigError(
            f"RECORDER_REST has kinds {venue.name} doesn't offer: {unknown} "
            f"(offered: {list(venue.rest)})"
        )

    if not streams and not rest:
        raise ConfigError("nothing to record: RECORDER_STREAMS and RECORDER_REST are both empty")

    try:
        rest_seconds = int(env.get("RECORDER_REST_SECONDS", "300"))
    except ValueError:
        raise ConfigError("RECORDER_REST_SECONDS must be a whole number of seconds") from None
    if rest_seconds < 60:
        raise ConfigError("RECORDER_REST_SECONDS must be at least 60")

    backfill = env.get("RECORDER_BACKFILL_OI", "0").strip().lower() in ("1", "true", "yes")
    if backfill and venue.vision_metrics is None:
        raise ConfigError(f"RECORDER_BACKFILL_OI isn't available for {venue.name}")

    try:
        port = int(env.get("RECORDER_PORT", "9091"))
    except ValueError:
        raise ConfigError("RECORDER_PORT must be a port number") from None

    proxy = env.get("HTTPS_PROXY") or env.get("https_proxy") or None

    return Config(
        venue=venue,
        symbols=tuple(symbols),
        streams=tuple(streams),
        rest=tuple(rest),
        rest_seconds=rest_seconds,
        backfill_oi=backfill,
        data_dir=Path(env.get("DATA_DIR", "/data")),
        port=port,
        bot_id=env.get("FKS_BOT_ID", "local"),
        proxy=proxy,
    )
