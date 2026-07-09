# syntax=docker/dockerfile:1
#
# fks-bot-crypto-spot — multi-exchange spot portfolio rebalancer.
#
# Migrated from the `crypto` repo's Dockerfile `spot` target; the image
# contract is unchanged (FKSRUNBOOK.md §4; spawner contract: image name prefix
# `fks-bot-`, env-only config, no volume mounts, metrics on :9091).
#
# Build context = the FKS-SPAWNER REPO ROOT (the crate path-deps
# ../../crates/crypto-bot-core, so both must be in the context — same
# crates/ + bots/ layout as the original fks tree):
#
#   docker build -f bots/spot-portfolio/Dockerfile -t fks-bot-crypto-spot:latest .
#
# The TOML is baked from this crate's example unless the operator's tuned
# (gitignored) `spot-portfolio.toml` sits next to it in `bots/spot-portfolio/`
# (NO secrets in the TOMLs — keys arrive as env, injected by the spawner's
# encrypted secret store).
# ⚠️ The baked TOML must keep `live = false` (dry-run): going live must be an
# explicit, deliberate override — never the default of whatever someone spawns
# from the UI (FKSRUNBOOK.md §6). The build FAILS if `live = true` would bake.

ARG RUST_VERSION=1.94.1
ARG DEBIAN_RELEASE=bookworm

# ──────────────────────────────────────────────────────────────────────────
# Builder — both bins (spot-portfolio + spot-optimize) in one pass; only
# spot-portfolio ships in the runtime image.
# ──────────────────────────────────────────────────────────────────────────
FROM rust:${RUST_VERSION}-slim-${DEBIAN_RELEASE} AS builder

# TLS is rustls/ring end-to-end (exchange-apiws REST + WS), so the only build
# deps are git + ca-certificates to fetch the first-party git dependencies. No
# OpenSSL, no cmake — the rust:slim base already has the C toolchain ring needs.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# The path-dep crate + the bot crate, laid out exactly as in the repo so the
# crate's `path = "../../crates/crypto-bot-core"` resolves. Cargo.lock pins
# the exact git revisions of the deps so the build is reproducible.
COPY crates/crypto-bot-core/Cargo.toml crates/crypto-bot-core/
COPY crates/crypto-bot-core/src crates/crypto-bot-core/src
COPY bots/spot-portfolio/Cargo.toml bots/spot-portfolio/Cargo.lock bots/spot-portfolio/
COPY bots/spot-portfolio/src bots/spot-portfolio/src

WORKDIR /build/bots/spot-portfolio

# BuildKit cache mounts keep the cargo registry/git clones and the target dir
# warm across builds. The release binaries are copied OUT of the cache-mounted
# target dir within the same RUN so they survive into the image.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/bots/spot-portfolio/target \
    cargo build --release --locked \
        --bin spot-portfolio --bin spot-optimize && \
    cp target/release/spot-portfolio /usr/local/bin/spot-portfolio && \
    cp target/release/spot-optimize  /usr/local/bin/spot-optimize

# ──────────────────────────────────────────────────────────────────────────
# Runtime base — slim, non-root. No inbound ports except the :9091 status
# server (FKS bot contract); outbound HTTPS/WSS to the venues only.
# ──────────────────────────────────────────────────────────────────────────
FROM debian:${DEBIAN_RELEASE}-slim AS runtime-base

# ca-certificates: validate the venues' TLS against the system trust store
# (rustls uses it for both REST and WS). No libssl — rustls/ring is static.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -u 10001 -m -s /usr/sbin/nologin appuser

USER appuser
# State files (journals) default to the CWD — point it at appuser's writable
# home.
WORKDIR /home/appuser

ENV RUST_LOG=info \
    RUST_BACKTRACE=1

# ──────────────────────────────────────────────────────────────────────────
# fks-bot-crypto-spot — the (default) final stage
#   Keys via env (spawner secrets injection): KRAKEN_API_KEY/SECRET,
#   KUCOIN_API_KEY/SECRET/PASSPHRASE, CRYPTOCOM_API_KEY/SECRET.
#   Baked config is DRY-RUN (live = false): reads real balances with keys
#   present, computes rebalances, places nothing.
# ──────────────────────────────────────────────────────────────────────────
FROM runtime-base AS spot

COPY --from=builder /usr/local/bin/spot-portfolio /usr/local/bin/spot-portfolio
# Prefer the operator's tuned (gitignored) TOML when it exists in the build
# context; fall back to the example. The wildcard makes the COPY succeed
# either way, and the real config overwrites the example when present —
# net-worth only counts cash + TARGETED assets (src/spot/rebalance.rs::plan),
# so the tuned target baskets are what make the number match reality.
# ⚠️ Whichever lands, `live` in it MUST stay false for this image (§6).
COPY --chown=root:root bots/spot-portfolio/spot-portfolio.example.toml bots/spot-portfolio/spot-portfolio*.toml* /tmp/cfg/
USER root
RUN mkdir -p /etc/crypto && \
    if [ -f /tmp/cfg/spot-portfolio.toml ]; then \
        cp /tmp/cfg/spot-portfolio.toml /etc/crypto/spot-portfolio.toml && \
        echo "baked OPERATOR spot config"; \
    else \
        cp /tmp/cfg/spot-portfolio.example.toml /etc/crypto/spot-portfolio.toml && \
        echo "baked EXAMPLE spot config"; \
    fi && \
    if grep -qE '^\s*live\s*=\s*true' /etc/crypto/spot-portfolio.toml; then \
        echo "ERROR: live=true in the baked spot config — this image must be dry-run (runbook §6)" >&2; \
        exit 1; \
    fi && \
    rm -rf /tmp/cfg
USER appuser

ENV SPOT_PORTFOLIO_CONFIG=/etc/crypto/spot-portfolio.toml \
    BOT_STATUS_PORT=9091

ENTRYPOINT ["/usr/local/bin/spot-portfolio"]
