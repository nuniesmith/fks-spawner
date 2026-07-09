# =============================================================================
# fks-bot-example — reference bot image for the FKS spawner
#
# Builds the standalone bot crate at `bots/fks-bot-example/`, which consumes the
# published `rustrade-framework` (+ friends) from crates.io, then ships a slim
# runtime image that exposes :9091/metrics with the documented `fks_bot_*` series.
#
# Build (from repo root):
#   docker build -f infrastructure/docker/services/fks-bot-example/Dockerfile \
#                -t fks-bot-example:latest .
#
# Spawn via the FKS spawner's `/api/spawner/spawn`:
#   {
#     "image": "fks-bot-example:latest",
#     "mode":  "paper",
#     "env":   { "BOT_SYMBOL": "BTCUSDT" }
#   }
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
# Copy just the standalone bot crate; cargo fetches rustrade-framework and the
# rest from crates.io during the build. Build context is the repo root.
COPY bots/fks-bot-example/ .

RUN cargo build --release && \
    strip /build/target/release/fks-bot-example

# ─── Runtime ─────────────────────────────────────────────────────────────────
FROM ${RUNTIME_BASE}

ARG APP_USER=appuser

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    ( apt-get install -y --no-install-recommends libssl3 2>/dev/null || \
      apt-get install -y --no-install-recommends libssl3t64 ) && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -u 1000 -m -s /bin/bash ${APP_USER}

WORKDIR /app
COPY --from=builder /build/target/release/fks-bot-example /app/fks-bot-example

USER ${APP_USER}

ENV RUST_LOG=info,fks_bot_example=debug \
    BOT_METRICS_PORT=9091 \
    BOT_SYMBOL=BTCUSDT

EXPOSE 9091

# Single-process container; the bot owns its own SIGTERM handling so we
# don't need tini.
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=5s \
    CMD curl -fs http://localhost:9091/health || exit 1

CMD ["/app/fks-bot-example"]
