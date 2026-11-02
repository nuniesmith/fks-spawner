"""CME history recorder configuration, read from the environment.

The spawner sets these from the `history.cme-massive` template's form, and
injects MASSIVE_API_KEY from Settings' stored `massive` credential (a data
vendor's key: templates.rs DATA_VENDOR_SECRETS). The same names work by hand.

  CME_ROOTS         comma-separated product roots: ES,NQ,GC,MGC; required
  CME_START         the first UTC day to record, YYYY-MM-DD. Default
                    2026-09-14: the Monday after the lake's imported cache
                    (raw/cme/massive-cache) ends, so no day is in both.
  CME_POLL_HOURS    hours between checks for new days (default 6)
  MASSIVE_API_KEY   required
  MASSIVE_RPM       requests a minute (default 5, Massive's free tier; 0 = unpaced)
  MASSIVE_BASE_URL  default https://api.massive.com
  DATA_DIR          where files go (default /data, the instance's lake folder)
  RECORDER_PORT     the /health, /metrics, /status port (default 9091)
  FKS_BOT_ID        set by the spawner; names this instance in logs
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Mapping

ROOT_RE = re.compile(r"^[A-Z0-9]{1,5}$")
DEFAULT_START = date(2026, 9, 14)


class ConfigError(ValueError):
    """A setting the recorder can't run with; the message says which."""


@dataclass(frozen=True)
class Config:
    roots: tuple[str, ...]
    start: date
    poll_hours: float
    api_key: str
    rpm: int
    base_url: str
    data_dir: Path
    port: int
    bot_id: str


def load(env: Mapping[str, str] | None = None) -> Config:
    env = os.environ if env is None else env
    roots = tuple(r.strip().upper() for r in env.get("CME_ROOTS", "").split(",") if r.strip())
    if not roots:
        raise ConfigError("CME_ROOTS is required, e.g. ES,NQ,GC,MGC")
    bad = [r for r in roots if not ROOT_RE.match(r)]
    if bad:
        raise ConfigError(f"CME_ROOTS: {bad} aren't product roots (A-Z and 0-9, up to 5)")
    if len(set(roots)) != len(roots):
        raise ConfigError("CME_ROOTS names a root twice")
    raw = env.get("CME_START", "").strip()
    try:
        start = date.fromisoformat(raw) if raw else DEFAULT_START
    except ValueError:
        raise ConfigError(f"CME_START must be YYYY-MM-DD, not {raw!r}") from None
    try:
        poll = float(env.get("CME_POLL_HOURS", "6"))
        rpm = int(env.get("MASSIVE_RPM", "5"))
        port = int(env.get("RECORDER_PORT", "9091"))
    except ValueError as e:
        raise ConfigError(f"a number setting: {e}") from None
    if not 0.25 <= poll <= 48:
        raise ConfigError("CME_POLL_HOURS must be between 0.25 and 48")
    if rpm < 0:
        raise ConfigError("MASSIVE_RPM can't be negative")
    key = env.get("MASSIVE_API_KEY", "").strip()
    if not key:
        raise ConfigError(
            "MASSIVE_API_KEY is required: store a 'massive' credential in Settings; "
            "the history.cme-massive template injects it"
        )
    return Config(
        roots=roots,
        start=start,
        poll_hours=poll,
        api_key=key,
        rpm=rpm,
        base_url=env.get("MASSIVE_BASE_URL", "https://api.massive.com").rstrip("/"),
        data_dir=Path(env.get("DATA_DIR", "/data")),
        port=port,
        bot_id=env.get("FKS_BOT_ID", "cme-history"),
    )
