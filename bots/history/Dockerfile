# =============================================================================
# fks-bot-history: Binance's public archive (data.binance.vision), verified
# =============================================================================
# Built from the fks-spawner repo root:
#   docker build -f bots/history/Dockerfile -t fks-bot-history .
# CI publishes ghcr.io/nuniesmith/fks-bot-history (.github/workflows/history.yml);
# the spawner runs it from a history template. See README.md.
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# uid 1000, non-root, like the recorder: the spawner gives each instance its
# own data folder, owned by this uid, mounted at /data.
RUN useradd -u 1000 -m -s /usr/sbin/nologin history \
    && mkdir -p /data && chown history:history /data

WORKDIR /app
COPY bots/history/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY bots/history/history ./history

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data
USER history
EXPOSE 9091
HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:9091/health', timeout=5).status == 200 else 1)"
CMD ["python", "-m", "history"]
