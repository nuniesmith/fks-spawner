"""python -m recorder: read the config from the environment and record."""

from __future__ import annotations

import asyncio
import logging
import sys

from . import config, core, server


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stdout,
    )
    try:
        cfg = config.load()
    except config.ConfigError as exc:
        logging.getLogger("recorder").error("can't start: %s", exc)
        return 2
    asyncio.run(core.run(cfg, serve=server.serve))
    return 0


if __name__ == "__main__":
    sys.exit(main())
