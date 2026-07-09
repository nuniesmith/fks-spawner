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
