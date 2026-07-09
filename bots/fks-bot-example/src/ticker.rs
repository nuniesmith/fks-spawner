//! Synthetic ticker publisher.
//!
//! In a real bot this is replaced by a feed handler subscribed to the
//! exchange. Here we just generate plausible BTC-shaped tickers on a fixed
//! interval so the brain has events to react to. Random walk centered on
//! a configurable price; bid/ask spread fixed at 0.01%.
//!
//! Cancellation: the loop checks the supervisor's cancel token between
//! ticks so the publisher exits immediately on shutdown.

use std::time::Duration;

use chrono::Utc;
use rand::RngExt;
use rustrade::{MarketDataEvent, Price, Symbol, Tick, Volume};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// Configuration for the synthetic ticker.
pub struct TickerConfig {
    /// Symbol the ticker reports.
    pub symbol: String,
    /// Initial price; the random walk starts from here.
    pub initial_price: f64,
    /// Per-tick price stdev (in absolute price units).
    pub volatility: f64,
    /// How often to emit a tick.
    pub interval: Duration,
}

impl Default for TickerConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            initial_price: 50_000.0,
            volatility: 25.0,
            interval: Duration::from_secs(1),
        }
    }
}

/// Publish synthetic tickers to the given bus until `cancel` fires.
pub async fn publish(
    bus: rustrade::MarketDataBus,
    config: TickerConfig,
    cancel: CancellationToken,
) {
    info!(
        symbol = %config.symbol,
        initial_price = config.initial_price,
        interval_ms = config.interval.as_millis() as u64,
        "synthetic ticker publisher started"
    );

    let mut price = config.initial_price;
    let mut ticker = tokio::time::interval(config.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("synthetic ticker publisher cancelled");
                return;
            }
            _ = ticker.tick() => {
                // Random walk step.
                let mut rng = rand::rng();
                let step: f64 = rng.random::<f64>() - 0.5;
                price += 2.0 * step * config.volatility;
                if price <= 0.0 {
                    price = config.initial_price; // pathological reset
                }

                let spread = price * 0.0001;
                let tick = Tick {
                    symbol: config.symbol.clone().into(),
                    timestamp: Utc::now(),
                    bid: Price(price - spread / 2.0),
                    ask: Price(price + spread / 2.0),
                    bid_size: Volume(1.0),
                    ask_size: Volume(1.0),
                    last_price: Some(Price(price)),
                    last_size: Some(Volume(0.1)),
                };
                let n = bus.publish(MarketDataEvent::Ticker {
                    exchange: "synthetic".into(),
                    symbol: Symbol::from(config.symbol.clone()),
                    tick,
                });
                debug!(price = price, subscribers = n, "ticker published");
            }
        }
    }
}
