//! Multi-exchange spot **portfolio** bot — entry point.
//!
//! Holds %-target baskets plus a cash reserve and rebalances on drift across
//! Kraken → Crypto.com → more. Config is a TOML file (default
//! `spot-portfolio.toml`, or pass a path / set `SPOT_PORTFOLIO_CONFIG`); API
//! keys come from the environment. Shares this repo with the KuCoin futures dip
//! bot for now.

use anyhow::{Context, Result};

use crypto_bot_core::status;
use spot_portfolio::spot::config::PortfolioConfig;
use spot_portfolio::spot::portfolio::Engine;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let path = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("SPOT_PORTFOLIO_CONFIG").ok())
        .unwrap_or_else(|| "spot-portfolio.toml".to_string());

    let mut cfg = PortfolioConfig::load(&path).with_context(|| format!("loading config {path}"))?;

    // Deliberate live override. The container image bakes `live = false`
    // (the Dockerfile build guard rejects a baked `live = true`), so going
    // live is an explicit, auditable, reversible act: set SPOT_LIVE=1 in the
    // spawn request env. Reverting is respawning without it. Never flips a
    // config that was already live to dry-run — this only ever ARMS live.
    let spot_live_env = std::env::var("SPOT_LIVE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // ── The declared mode has to agree with the capability being used ────────
    //
    // `FKS_BOT_MODE` is what the spawner stamps on the container, what the
    // `fks.mode` label says, and what the `live_flip` notification keys off.
    // Until now this process never read it: `SPOT_LIVE=1` armed real orders
    // regardless, so a container labelled `paper` could trade live and the
    // alert that exists to announce that would report "paper".
    //
    // A label nobody checks is not a boundary, it is a comment. This makes the
    // two agree or refuses to start.
    //
    // An UNSET mode is refused too, and that is the deliberate part. The
    // spawner always sets it, so unset means "not launched by the platform" —
    // and treating absence as permission is the exact shape that makes every
    // other check bypassable by removing a variable. The escape hatch is to
    // state the capability you are using: `FKS_BOT_MODE=live`.
    if spot_live_env {
        match std::env::var("FKS_BOT_MODE").ok().as_deref() {
            Some("live") => {}
            Some(other) => {
                anyhow::bail!(
                    "REFUSING TO START: SPOT_LIVE=1 arms real orders, but this process is \
                     declared FKS_BOT_MODE={other}. The label and the capability must agree — \
                     a container labelled '{other}' that places real orders is invisible to \
                     the live_flip alert and to anyone reading `docker ps`. Either spawn it \
                     with mode=live, or drop SPOT_LIVE."
                );
            }
            None => {
                anyhow::bail!(
                    "REFUSING TO START: SPOT_LIVE=1 arms real orders but FKS_BOT_MODE is unset, \
                     so nothing declares what this process is. Absence is not permission. Set \
                     FKS_BOT_MODE=live to state the capability being used."
                );
            }
        }
    }

    if spot_live_env && !cfg.live {
        tracing::warn!(
            "SPOT_LIVE=1 — arming LIVE trading (real orders on real balances). \
             Config baked live=false; env override in effect."
        );
        cfg.live = true;
    }

    // Status/metrics server (FKS bot contract): /health, /metrics, /status on
    // BOT_STATUS_PORT (default 9091). Venue snapshots are pushed each cycle.
    let st = status::init("spot-portfolio", "spot", cfg.exchanges.len());
    st.set_mode(if cfg.live { "live" } else { "dry-run" });
    status::serve(st);

    let engine = Engine::build(cfg).await?;
    engine.run().await
}
