//! `crypto-demo` — a working rustrade bot over crypto pairs.
//!
//! Wires the full published FKS stack together and paper-trades over time:
//!
//! ```text
//!   exchange-apiws (KuCoin Futures klines)
//!        │  CandleSource::poll
//!        ▼
//!   rustrade CandlePollerService ──► MarketDataBus ──► EmaCrossBrain
//!        (one per symbol)                               (indicators-ta)
//!                                                          │ Decision
//!                                                          ▼
//!   rustrade ExecutionService (risk gate: sizing + session PnL + breaker)
//!                                                          ▼
//!                                                   MockExchange (paper)
//! ```
//!
//! Safe to leave running for days: PAPER mode only (no real orders), live
//! KuCoin market data by default, synthetic fallback when offline. Exposes
//! `:9091/metrics` with the `fks_bot_*` series so the FKS spawner / Prometheus
//! can scrape it exactly like `fks-bot-example`.
//!
//! ```bash
//! cargo run -p crypto-demo                                  # live XBT/ETH/SOL, paper
//! DEMO_SYMBOLS=XBTUSDTM,ETHUSDTM cargo run -p crypto-demo   # pick pairs
//! DEMO_SOURCE=synthetic cargo run -p crypto-demo            # offline
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use rustrade::{Bot, BotConfig, Brain, ExchangeClient, SizingConfig};
use rustrade::{CircuitBreakerConfig, JsonFileStore, PortfolioRiskConfig, SessionPnlConfig};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

mod brain;
mod exchange;
mod janus_brain;
mod metrics;
mod mock_exchange;
mod paper;
mod server;
mod source;

use crate::brain::{EmaCrossBrain, EmaCrossConfig};
use crate::janus_brain::{JanusBrain, JanusBrainConfig};

/// Leverage used in two places that must agree: the `SizingConfig` (how
/// positions are sized) and the live exchange adapter (the per-order leverage
/// KuCoin receives).
const DEMO_LEVERAGE: u32 = 5;

/// Read a comma-separated env list, falling back to `default`.
fn env_list(key: &str, default: &[&str]) -> Vec<String> {
    std::env::var(key)
        .ok()
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.iter().map(|s| s.to_string()).collect())
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustrade::logging::init_tracing();

    // exchange-apiws compiles reqwest with `rustls-no-provider`, and cargo
    // feature-unification applies that to every reqwest client in this binary.
    // Its own client constructors install the ring provider, but the paper
    // MockExchange path never calls them — so JanusBrain's plain-HTTP
    // reqwest::Client panics at build time ("No rustls crypto provider is
    // configured") unless we install one first. Idempotent.
    exchange_apiws::ensure_crypto_provider();

    // ── Config from env ───────────────────────────────────────────────────
    let bot_id = std::env::var("FKS_BOT_ID").unwrap_or_else(|_| "crypto-demo".into());
    // KuCoin Futures perpetual symbols (XBT = BTC on KuCoin). Override with
    // DEMO_SYMBOLS=... ; use spot-style names if you point at a spot source.
    let symbols = env_list("DEMO_SYMBOLS", &["XBTUSDTM", "ETHUSDTM", "SOLUSDTM"]);
    let poll_secs = env_u64("DEMO_POLL_SECS", 60);
    let interval_secs = env_u64("DEMO_CANDLE_SECS", 60); // 1m candles
    let warmup = env_u64("DEMO_WARMUP_CANDLES", 100) as usize;
    let metrics_port = std::env::var("BOT_METRICS_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(9091);

    info!(
        bot_id = %bot_id,
        symbols = ?symbols,
        poll_secs,
        source = %std::env::var("DEMO_SOURCE").unwrap_or_else(|_| "kucoin".into()),
        "crypto-demo starting (PAPER mode)"
    );

    // ── Market data: one CandleSource shared across symbols ────────────────
    let candle_source = source::build_source(&symbols);
    info!(source = candle_source.name(), "market data source selected");

    // ── Strategy + paper exchange ──────────────────────────────────────────
    // DEMO_BRAIN selects who decides:
    //   "ema-cross" (default) — decide locally from indicators-ta
    //   "janus"               — POST features to janus and act on its signal
    let brain_kind = std::env::var("DEMO_BRAIN").unwrap_or_else(|_| "ema-cross".into());
    let brain: Arc<dyn Brain> = if brain_kind.eq_ignore_ascii_case("janus") {
        // POST /api/v1/signals/generate lives on janus's FORWARD service:
        // natively that's :8180; inside the FKS compose network
        // http://fks_janus:8180; from the host against the compose stack
        // http://localhost:7001. (:8080 is the api service — it 404s.)
        let janus_url =
            std::env::var("JANUS_HTTP_URL").unwrap_or_else(|_| "http://localhost:8180".into());
        info!(janus_url = %janus_url, "using JanusBrain — signals delegated to janus");
        Arc::new(JanusBrain::new(
            format!("janus-{bot_id}"),
            JanusBrainConfig::default(),
            janus_url,
        ))
    } else {
        info!("using EmaCrossBrain — local indicators-ta strategy");
        Arc::new(EmaCrossBrain::new(
            format!("ema-cross-{bot_id}"),
            EmaCrossConfig::default(),
        ))
    };
    // Paper MockExchange by default; DEMO_EXCHANGE=kucoin routes to the live
    // KuCoin Futures adapter (rustrade-exchange-apiws) — see src/exchange.rs.
    let selected = exchange::build_exchange(&symbols, DEMO_LEVERAGE).await;
    let exchange: Arc<dyn ExchangeClient> = selected.exchange;
    let fill_source = selected.fills;
    // On the live path we get REAL fills; the paper PnL simulator must then be
    // disabled so it doesn't double-count against the real fill routing.
    let real_fills = fill_source.is_some();

    // ── Bot config: multi-symbol + risk gates ─────────────────────────────
    let mut config_builder = BotConfig::builder()
        .name(format!("crypto-demo-{bot_id}"))
        .symbols(symbols.iter().cloned())
        .shutdown_timeout(Duration::from_secs(10))
        // Paper sizing. The MockExchange reports contract_value = 1.0, so
        // notional (margin × leverage) must exceed the asset price to size
        // ≥ 1 contract. BTC ≈ 65k ⇒ 50k margin × 5x = 250k notional ⇒ a few
        // contracts. Against a real KuCoin adapter (XBTUSDTM contract_value
        // = 0.001 BTC) far smaller margin would suffice — tune per deployment.
        .sizing_config(SizingConfig {
            // Default 50k suits the paper MockExchange (contract_value = 1.0).
            // For a real venue adapter (KuCoin XBTUSDTM contract_value = 0.001
            // BTC) set DEMO_MARGIN_PER_TRADE_USD tiny: e.g. 7 at DEMO_LEVERAGE=1
            // sizes ~1 SOLUSDTM contract (~$6.74). DEMO_MAX_CONTRACTS is a hard
            // ceiling on contracts/trade regardless of the margin×leverage math.
            margin_per_trade: env_u64("DEMO_MARGIN_PER_TRADE_USD", 50_000) as f64,
            leverage: DEMO_LEVERAGE,
            max_contracts: env_u64("DEMO_MAX_CONTRACTS", 100) as u32,
        })
        // Stop the session if paper PnL drops past this (per UTC day).
        .session_pnl_config(SessionPnlConfig::default())
        // Trip after repeated losses, cool down, resume.
        .circuit_breaker_config(CircuitBreakerConfig::default())
        // Account-wide risk (rustrade 0.3): a daily-loss halt across all symbols,
        // a concurrency cap, and a gross-notional cap — on top of the per-symbol
        // gates above. Defaults are paper-safe and roomy so the demo runs; tune
        // via DEMO_MAX_DAILY_LOSS_USD / DEMO_MAX_POSITIONS / DEMO_MAX_GROSS_EXPOSURE_USD.
        .portfolio_config(PortfolioRiskConfig {
            max_daily_loss: -(env_u64("DEMO_MAX_DAILY_LOSS_USD", 5_000) as f64),
            max_concurrent_positions: env_u64("DEMO_MAX_POSITIONS", symbols.len() as u64) as u32,
            max_gross_exposure: env_u64("DEMO_MAX_GROSS_EXPOSURE_USD", 5_000_000) as f64,
        });

    // Per-asset-class risk (rustrade 0.3 `class_risk`). The multi-venue exchange
    // returns presets for each class it trades; the framework resolves a symbol's
    // class from `instrument_spec` and applies the matching rules (per-symbol →
    // per-class → default). Empty for single-venue modes — one class, so the
    // bot-wide config above already fits.
    for (class, cfg) in &selected.class_risk {
        config_builder = config_builder.class_risk(*class, cfg.clone());
    }
    if !selected.class_risk.is_empty() {
        info!(
            classes = selected.class_risk.len(),
            "per-asset-class risk presets applied (class_risk) — CryptoPerp vs CryptoSpot diverge"
        );
    }

    let config = config_builder.build()?;

    let mut bot = Bot::new(config, exchange, vec![brain])?;

    // Real fills (KuCoin private WS trigger + /recentFills) on the live path.
    // This also turns on the framework's SL/TP bracket + OCO handling, which it
    // gates on a fill source being present.
    if let Some(fills) = fill_source {
        bot = bot.with_fill_source(fills);
        info!("real fill source wired (live adapter) — paper PnL simulator disabled");
    }

    // Durable risk state (rustrade 0.3 JsonFileStore). Opt-in: set DEMO_STATE_FILE
    // to a path and per-symbol risk (session-PnL halt + circuit breaker) survives
    // a restart instead of resetting; the account daily-loss halt re-derives via
    // the sweep. Unset ⇒ in-memory (the prior behaviour).
    if let Ok(path) = std::env::var("DEMO_STATE_FILE") {
        match JsonFileStore::open(&path).await {
            Ok(store) => {
                bot = bot.with_state_store(Arc::new(store));
                info!(path = %path, "durable risk state enabled (JsonFileStore)");
            }
            Err(e) => {
                warn!(error = %e, path = %path, "state store unavailable — risk state stays in-memory");
            }
        }
    }

    // Attach one supervised candle poller per symbol. The poller calls
    // CandleSource::poll on `poll_cadence`, diffs newly-closed candles, and
    // publishes them to the bus as MarketDataEvent::Candle.
    let interval = Duration::from_secs(interval_secs);
    let cadence = Duration::from_secs(poll_secs);
    for sym in &symbols {
        bot = bot.with_candle_poller(
            Arc::clone(&candle_source),
            sym.clone(),
            interval,
            cadence,
            warmup,
        );
    }

    let handle = bot.handle();

    // ── Auxiliary services (metrics HTTP + uptime), torn down with the bot ──
    let aux_cancel = CancellationToken::new();

    // Paper-PnL tracker: signals → simulated round trips → metrics + risk PnL.
    // Skipped on the live path, where the framework's FillRoutingService records
    // PnL from the real fill source instead (running both would double-count).
    if real_fills {
        info!("live fills active — skipping paper PnL simulator");
    } else {
        paper::spawn(
            handle.clone(),
            bot.market_data_bus().clone(),
            bot.signal_bus().clone(),
            aux_cancel.clone(),
        );
    }

    let start = Arc::new(Instant::now());
    tokio::spawn(metrics::uptime_loop(Arc::clone(&start)));

    {
        let cancel = aux_cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = server::run(metrics_port, cancel).await {
                tracing::error!(error = %e, "metrics server crashed");
            }
        });
    }

    // Periodic health + PnL snapshot to the log so a long run is observable.
    {
        let h = handle.clone();
        let cancel = aux_cancel.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(300));
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tick.tick() => {
                        let health = h.health().await;
                        info!(
                            healthy = health.healthy,
                            "crypto-demo heartbeat — see :{}/metrics for fks_bot_* series",
                            metrics_port,
                        );
                    }
                }
            }
        });
    }

    info!(port = metrics_port, "metrics live at /metrics and /health");

    // ── Run until SIGTERM / Ctrl-C ─────────────────────────────────────────
    let result = bot.run_until_shutdown().await;
    aux_cancel.cancel();
    info!("crypto-demo exited");
    result
}
