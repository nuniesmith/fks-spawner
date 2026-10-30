# =============================================================================
# fks-bot-edge-search: one search of a registered hypothesis, and its verdict
# =============================================================================
# Built from the fks-spawner repo root:
#   docker build -f bots/edge-search/Dockerfile -t fks-bot-edge-search .
# CI publishes ghcr.io/nuniesmith/fks-bot-edge-search (.github/workflows/
# edge-search.yml); the spawner runs it from the job.edge-search template as a
# one-shot job instance. See README.md.
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# uid 1000 like the other bots: the spawner owns the job's folder as 1000.
RUN useradd -u 1000 -m -s /usr/sbin/nologin search \
    && mkdir -p /data /lake && chown search:search /data

WORKDIR /app
COPY bots/edge-search/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY bots/edge-search/edge_search ./edge_search

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    LAKE_DIR=/lake
USER search
EXPOSE 9091
CMD ["python", "-m", "edge_search"]
