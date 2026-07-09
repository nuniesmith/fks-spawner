//! `fks-bot-example` — reference bot image for the FKS spawner.
//!
//! Provides a minimal, runnable container that the spawner can launch end
//! to end:
//!   1. Boots a `rustrade::Bot` with a `HeartbeatBrain` and `MockExchange`.
//!   2. Spawns a synthetic ticker publisher so the brain has events.
//!   3. Exposes `:9091/metrics` with the documented `fks_bot_*` series so
//!      Prometheus' `fks-bots` `file_sd_configs` job can scrape it.
//!   4. Honours the standard `FKS_BOT_ID` / `FKS_BOT_MODE` env vars the
//!      spawner injects, and surfaces them in logs + metrics labels.
//!
//! Operators don't run this directly. The spawner builds it as the Docker
//! image `fks-bot-example:latest`, then `POST /spawn` with that image
//! brings up an instance.
//!
//! ```bash
//! # Local smoke test (no Docker)
//! cargo run -p fks-bot-example
//!
//! # In another terminal:
//! curl -s http://localhost:9091/metrics | grep fks_bot_
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use rustrade::{Bot, BotConfig, Brain, ExchangeClient};
use tokio_util::sync::CancellationToken;
use tracing::info;

mod brain;
mod metrics;
mod mock_exchange;
mod server;
mod ticker;

use crate::brain::{HeartbeatBrain, HeartbeatConfig};
use crate::mock_exchange::MockExchange;
use crate::ticker::TickerConfig;

/// Read a positive integer env var, falling back to `default`.
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustrade::logging::init_tracing();

    // ── Identity injected by the spawner ─────────────────────────────────
    // `FKS_BOT_ID` and `FKS_BOT_MODE` are forced env vars the spawner
    // injects on every spawn. Default values let the binary run outside
    // the spawner for local smoke tests.
    let bot_id = std::env::var("FKS_BOT_ID").unwrap_or_else(|_| "local-dev".to_string());
    let mode = std::env::var("FKS_BOT_MODE").unwrap_or_else(|_| "paper".to_string());
    let symbol = std::env::var("BOT_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let metrics_port = std::env::var("BOT_METRICS_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(9091);

    info!(bot_id = %bot_id, mode = %mode, symbol = %symbol, metrics_port, "fks-bot-example starting");

    // ── Build the bot ────────────────────────────────────────────────────
    let exchange: Arc<dyn ExchangeClient> = Arc::new(MockExchange);
    let heartbeat_cfg = HeartbeatConfig {
        signal_every: env_u64("BOT_SIGNAL_EVERY", 6),
        trade_every: env_u64("BOT_TRADE_EVERY", 20),
        pnl_mean: 1.0,
        pnl_std: 5.0,
    };
    let brains: Vec<Arc<dyn Brain>> = vec![Arc::new(HeartbeatBrain::new(
        format!("heartbeat-{bot_id}"),
        heartbeat_cfg,
    ))];

    let config = BotConfig::builder()
        .name(format!("fks-bot-example-{bot_id}"))
        .symbol(symbol.clone())
        .shutdown_timeout(Duration::from_secs(10))
        .build()?;

    let bot = Bot::new(config, exchange, brains)?;
    let bus = bot.market_data_bus().clone();

    // Our auxiliary services (metrics server + ticker) share one cancel
    // token. The framework owns its own SIGTERM/Ctrl-C handling, so we cancel
    // this token once `run_until_shutdown` returns to tear them down in step.
    let aux_cancel = CancellationToken::new();

    // ── Background services ──────────────────────────────────────────────

    // Uptime ticker.
    let start = Arc::new(Instant::now());
    tokio::spawn(metrics::uptime_loop(Arc::clone(&start)));

    // Metrics HTTP server.
    {
        let cancel = aux_cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = server::run(metrics_port, cancel).await {
                tracing::error!(error = %e, "metrics server crashed");
            }
        });
    }

    // Synthetic ticker — feeds the brain so the metric counters tick up.
    {
        let cancel = aux_cancel.clone();
        let cfg = TickerConfig {
            symbol: symbol.clone(),
            ..Default::default()
        };
        tokio::spawn(ticker::publish(bus, cfg, cancel));
    }

    // ── Run until shutdown ───────────────────────────────────────────────
    // Blocks until SIGTERM / Ctrl-C; the framework drives its own shutdown.
    let result = bot.run_until_shutdown().await;

    // Bot is done — stop our auxiliary services too.
    aux_cancel.cancel();

    info!("fks-bot-example exited");
    result
}
