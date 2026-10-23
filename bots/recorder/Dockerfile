# =============================================================================
# fks-bot-recorder: raw public market data, append-only, with a gap ledger
# =============================================================================
# Built from the fks-spawner repo root:
#   docker build -f bots/recorder/Dockerfile -t fks-bot-recorder .
# CI publishes ghcr.io/nuniesmith/fks-bot-recorder (.github/workflows/recorder.yml);
# the spawner runs it from a recorder template. See README.md.
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# uid 1000, non-root. The spawner gives each instance its own data folder,
# owned by this uid, mounted at /data. Without a mount, /data is in the
# container and goes with it.
RUN useradd -u 1000 -m -s /usr/sbin/nologin recorder \
    && mkdir -p /data && chown recorder:recorder /data

WORKDIR /app
COPY bots/recorder/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY bots/recorder/recorder ./recorder

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data
USER recorder
EXPOSE 9091
HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:9091/health', timeout=5).status == 200 else 1)"
CMD ["python", "-m", "recorder"]
