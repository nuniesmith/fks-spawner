//! Market-data sources implementing rustrade's [`CandleSource`].
//!
//! - [`KucoinCandleSource`] — polls KuCoin Futures klines through
//!   `exchange-apiws`. Public market data, no API key required.
//! - `KrakenCandleSource` (from `rustrade-exchange-apiws`) — Kraken public OHLC,
//!   used for Kraken spot symbols.
//! - [`SyntheticCandleSource`] — a random-walk generator used when
//!   `DEMO_SOURCE=synthetic` (or when an exchange is unreachable, e.g. CI /
//!   sandboxes that block exchange endpoints). Keeps the demo runnable offline.
//!
//! [`build_source`] aligns the market-data feed with the trading venue: the
//! `kraken` and `multi` exchange modes get Kraken (and, for `multi`, per-symbol
//! routed via `RoutingCandleSource`) candles instead of always polling KuCoin.
//!
//! rustrade's `CandlePollerService` calls `poll(...)` on a cadence, diffs the
//! returned candles against what it has already seen, and publishes each newly
//! closed candle to the bot's `MarketDataBus` as `MarketDataEvent::Candle`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use exchange_apiws::{Credentials, KuCoinClient, KucoinEnv};
use rustrade::{Candle, CandleSource, Result, Symbol};
use rustrade_exchange_apiws::{KrakenCandleSource, RoutingCandleSource};
use tracing::{debug, warn};

/// Map a rustrade poll `interval` to a KuCoin granularity string (minutes).
fn kucoin_granularity(interval: Duration) -> &'static str {
    match interval.as_secs() {
        0..=60 => "1",
        61..=300 => "5",
        301..=900 => "15",
        901..=1800 => "30",
        1801..=3600 => "60",
        3601..=14400 => "240",
        _ => "1440",
    }
}

// ── Live KuCoin source ────────────────────────────────────────────────────────

/// Polls KuCoin Futures klines via `exchange-apiws`. No credentials needed for
/// public market data — we pass empty `Credentials` (klines are unauthenticated).
pub struct KucoinCandleSource {
    client: KuCoinClient,
}

impl KucoinCandleSource {
    /// Build a public-data client against KuCoin Futures.
    pub fn new() -> anyhow::Result<Self> {
        // Empty creds: the kline endpoint is public. Real trading would supply
        // KC_KEY / KC_SECRET / KC_PASSPHRASE, but this demo never places orders.
        let creds = Credentials::new(
            std::env::var("KC_KEY").unwrap_or_default(),
            std::env::var("KC_SECRET").unwrap_or_default(),
            std::env::var("KC_PASSPHRASE").unwrap_or_default(),
        );
        let client = KuCoinClient::new(creds, KucoinEnv::LiveFutures)
            .map_err(|e| anyhow::anyhow!("kucoin client: {e}"))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl CandleSource for KucoinCandleSource {
    fn name(&self) -> &str {
        "kucoin"
    }

    async fn poll(&self, symbol: &Symbol, interval: Duration, limit: usize) -> Result<Vec<Candle>> {
        let gran = kucoin_granularity(interval);
        let raw = self
            .client
            .fetch_klines(symbol.0.as_str(), limit, gran)
            .await
            .map_err(|e| rustrade::Error::Exchange(format!("kucoin fetch_klines: {e}")))?;

        // exchange_apiws::Candle and rustrade::Candle share the same field
        // layout but are distinct types — convert at the boundary.
        let candles = raw
            .into_iter()
            .map(|c| Candle {
                time: c.time,
                open: c.open,
                high: c.high,
                low: c.low,
                close: c.close,
                volume: c.volume,
            })
            .collect::<Vec<_>>();

        debug!(symbol = %symbol.0, granularity = gran, n = candles.len(), "kucoin klines polled");
        Ok(candles)
    }
}

// ── Synthetic offline source ──────────────────────────────────────────────────

/// Random-walk candle generator. Produces fresh "closed" candles on each poll
/// so the demo runs with no network. Each symbol walks from its own seed price.
pub struct SyntheticCandleSource {
    state: Mutex<Vec<(String, f64, i64)>>, // (symbol, last_close, last_time_ms)
}

impl SyntheticCandleSource {
    /// Seed each symbol at a plausible starting price.
    pub fn new(symbols: &[String]) -> Self {
        let seed = |s: &str| -> f64 {
            match s {
                x if x.starts_with("XBT") || x.starts_with("BTC") => 65_000.0,
                x if x.starts_with("ETH") => 3_200.0,
                x if x.starts_with("SOL") => 150.0,
                _ => 100.0,
            }
        };
        let now = chrono::Utc::now().timestamp_millis();
        let state = symbols.iter().map(|s| (s.clone(), seed(s), now)).collect();
        Self {
            state: Mutex::new(state),
        }
    }
}

#[async_trait]
impl CandleSource for SyntheticCandleSource {
    fn name(&self) -> &str {
        "synthetic"
    }

    async fn poll(&self, symbol: &Symbol, interval: Duration, limit: usize) -> Result<Vec<Candle>> {
        let step_ms = interval.as_millis() as i64;
        let mut guard = self.state.lock().unwrap();
        let entry = guard
            .iter_mut()
            .find(|(s, _, _)| *s == symbol.0)
            .ok_or_else(|| rustrade::Error::Exchange(format!("unknown symbol {}", symbol.0)))?;

        // Generate `limit` candles forward from the stored state so the poller
        // always has a full window; advance the state so the next poll
        // continues the walk (and the poller sees new closed bars).
        let mut price = entry.1;
        let mut t = entry.2;
        let mut out = Vec::with_capacity(limit);
        for _ in 0..limit {
            let drift: f64 = rand::random::<f64>() - 0.5; // [-0.5, 0.5)
            let vol = price * 0.002; // 0.2% per-bar vol
            let open = price;
            let close = (price + 2.0 * drift * vol).max(0.01);
            let high = open.max(close) + rand::random::<f64>() * vol;
            let low = open.min(close) - rand::random::<f64>() * vol;
            out.push(Candle {
                time: t,
                open,
                high,
                low,
                close,
                volume: 10.0 + rand::random::<f64>() * 5.0,
            });
            price = close;
            t += step_ms.max(1);
        }
        entry.1 = price;
        entry.2 = t;
        Ok(out)
    }
}

/// Build the market-data source. `DEMO_SOURCE` forces a specific source
/// (`synthetic` / `kucoin` / `kraken`); unset, it follows the trading venue
/// (`DEMO_EXCHANGE`) so candles come from where orders go — KuCoin for
/// `kucoin`/`mock`, Kraken for `kraken`, and per-symbol routed for `multi`.
/// Any real source falls back to the synthetic generator if it can't be built.
pub fn build_source(symbols: &[String]) -> Arc<dyn CandleSource> {
    match std::env::var("DEMO_SOURCE")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .as_deref()
    {
        Some("synthetic") => Arc::new(SyntheticCandleSource::new(symbols)),
        Some("kucoin") => kucoin_or_synthetic(symbols),
        Some("kraken") => kraken_or_synthetic(symbols),
        // Unset/unknown: align market data with the trading venue.
        _ => match std::env::var("DEMO_EXCHANGE")
            .unwrap_or_else(|_| "mock".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "kraken" => kraken_or_synthetic(symbols),
            "multi" | "kucoin+kraken" => multi_source(symbols),
            _ => kucoin_or_synthetic(symbols),
        },
    }
}

fn kucoin_or_synthetic(symbols: &[String]) -> Arc<dyn CandleSource> {
    match KucoinCandleSource::new() {
        Ok(src) => Arc::new(src),
        Err(e) => {
            warn!(error = %e, "kucoin source unavailable — falling back to synthetic");
            Arc::new(SyntheticCandleSource::new(symbols))
        }
    }
}

fn kraken_or_synthetic(symbols: &[String]) -> Arc<dyn CandleSource> {
    match KrakenCandleSource::new() {
        Ok(src) => Arc::new(src),
        Err(e) => {
            warn!(error = %e, "kraken source unavailable — falling back to synthetic");
            Arc::new(SyntheticCandleSource::new(symbols))
        }
    }
}

/// Multi-venue market data: KuCoin klines for KuCoin symbols, Kraken OHLC for
/// Kraken symbols (the same split [`crate::exchange::split_venues`] uses for
/// order routing), with the synthetic generator as the fallback for any source
/// that can't be built and for unmapped symbols.
fn multi_source(symbols: &[String]) -> Arc<dyn CandleSource> {
    let (kucoin_syms, kraken_syms) = crate::exchange::split_venues(symbols);
    let synthetic: Arc<dyn CandleSource> = Arc::new(SyntheticCandleSource::new(symbols));
    let kucoin: Arc<dyn CandleSource> = match KucoinCandleSource::new() {
        Ok(s) => Arc::new(s),
        Err(e) => {
            warn!(error = %e, "multi: kucoin source unavailable — synthetic for its symbols");
            Arc::clone(&synthetic)
        }
    };
    let kraken: Arc<dyn CandleSource> = match KrakenCandleSource::new() {
        Ok(s) => Arc::new(s),
        Err(e) => {
            warn!(error = %e, "multi: kraken source unavailable — synthetic for its symbols");
            Arc::clone(&synthetic)
        }
    };
    Arc::new(
        RoutingCandleSource::builder()
            .route(kucoin_syms, kucoin)
            .route(kraken_syms, kraken)
            .default_source(synthetic)
            .build(),
    )
}
