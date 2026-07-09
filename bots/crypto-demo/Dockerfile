# =============================================================================
# crypto-demo — a working rustrade bot over crypto pairs (paper mode)
#
# Builds the standalone bot crate at `bots/crypto-demo/`, which consumes the
# published rustrade-framework + indicators-ta + exchange-apiws from crates.io,
# then ships a slim runtime image exposing :9091/metrics with the `fks_bot_*`
# series — spawnable by the FKS spawner exactly like fks-bot-example.
#
# The image is tagged `fks-bot-crypto-demo` so it matches the spawner's
# `ALLOWED_IMAGE_PREFIX` (default `fks-bot-`) and can be launched from `/bots`.
#
# Build (from repo root):
#   docker build -f infrastructure/docker/services/crypto-demo/Dockerfile \
#                -t fks-bot-crypto-demo:latest .
#
# Run (paper, live KuCoin data):
#   docker run --rm -p 9091:9091 fks-bot-crypto-demo:latest
#
# Run offline (synthetic data, no network):
#   docker run --rm -e DEMO_SOURCE=synthetic -p 9091:9091 fks-bot-crypto-demo:latest
#
# Spawn via the FKS spawner's `/api/spawner/spawn`:
#   { "image": "fks-bot-crypto-demo:latest", "mode": "paper",
#     "env": { "DEMO_SYMBOLS": "XBTUSDTM,ETHUSDTM" } }
# =============================================================================

ARG RUST_VERSION=1.94.1
ARG BUILDER_BASE=debian:bookworm-slim
ARG RUNTIME_BASE=debian:bookworm-slim

# ─── Builder ─────────────────────────────────────────────────────────────────
FROM ${BUILDER_BASE} AS builder

ARG RUST_VERSION
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl ca-certificates pkg-config libssl-dev g++ make \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --no-modify-path --profile minimal \
              --default-toolchain ${RUST_VERSION} && \
    chmod -R a+w ${RUSTUP_HOME} ${CARGO_HOME}

WORKDIR /build
# crypto-demo consumes the published crates from crates.io AND has a path
# dependency on the sibling bots/rustrade-exchange-apiws crate (the live KuCoin
# order path). Copy BOTH, preserving the sibling layout its Cargo.toml expects:
#   rustrade-exchange-apiws = { path = "../rustrade-exchange-apiws" }
# Build context is the repo root.
COPY bots/crypto-demo/             ./crypto-demo/
COPY bots/rustrade-exchange-apiws/ ./rustrade-exchange-apiws/

WORKDIR /build/crypto-demo
RUN cargo build --release --locked && \
    strip /build/crypto-demo/target/release/crypto-demo

# ─── Runtime ─────────────────────────────────────────────────────────────────
FROM ${RUNTIME_BASE}

ARG APP_USER=appuser

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    ( apt-get install -y --no-install-recommends libssl3 2>/dev/null || \
      apt-get install -y --no-install-recommends libssl3t64 ) && \
    rm -rf /var/lib/apt/lists/*

# Prefer IPv4 in getaddrinfo. The fks Docker network has no IPv6, but some
# venues are IPv6-first in DNS (e.g. api-futures.kucoin.com is dual-stack) —
# the default RFC 6724 ordering then makes reqwest attempt an unroutable IPv6
# address and fail with "error sending request for url". Bumping the
# IPv4-mapped precedence makes IPv4 win. (This is Debian's documented
# "prefer IPv4" toggle from the stock /etc/gai.conf.)
RUN printf 'precedence ::ffff:0:0/96  100\n' > /etc/gai.conf

RUN useradd -u 1000 -m -s /bin/bash ${APP_USER}

WORKDIR /app
COPY --from=builder /build/crypto-demo/target/release/crypto-demo /app/crypto-demo

USER ${APP_USER}

ENV RUST_LOG=info,crypto_demo=info \
    BOT_METRICS_PORT=9091 \
    DEMO_SOURCE=kucoin \
    DEMO_SYMBOLS=XBTUSDTM,ETHUSDTM,SOLUSDTM

EXPOSE 9091

# Single-process container; the bot owns its own SIGTERM handling.
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=10s \
    CMD curl -fs http://localhost:9091/health || exit 1

CMD ["/app/crypto-demo"]
