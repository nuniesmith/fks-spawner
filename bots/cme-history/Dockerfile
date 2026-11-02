# =============================================================================
# fks-bot-cme-history: CME futures 1-minute bars from Massive, stitched by
# volume, into the lake
# =============================================================================
# Built from the fks-spawner repo root:
#   docker build -f bots/cme-history/Dockerfile -t fks-bot-cme-history .
# CI publishes ghcr.io/nuniesmith/fks-bot-cme-history (.github/workflows/
# cme-history.yml); the spawner runs it from the history.cme-massive template.
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# uid 1000, non-root, like the other recorders: the spawner gives each
# instance its own data folder, owned by this uid, mounted at /data.
RUN useradd -u 1000 -m -s /usr/sbin/nologin cme \
    && mkdir -p /data && chown cme:cme /data

WORKDIR /app
COPY bots/cme-history/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY bots/cme-history/cme_history ./cme_history

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data
USER cme
EXPOSE 9091
HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:9091/health', timeout=5).status == 200 else 1)"
CMD ["python", "-m", "cme_history"]
