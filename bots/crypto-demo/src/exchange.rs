//! Exchange selection for the demo.
//!
//! The default is [`MockExchange`] — **paper only**, it places no real orders
//! and reports `contract_value = 1.0`. This keeps the demo safe to leave
//! running anywhere (the "no autonomous execution" default of the FKS stack).
//!
//! Set `DEMO_EXCHANGE=kucoin` to route orders through the **live** KuCoin
//! Futures adapter ([`rustrade_exchange_apiws::KucoinExchangeAdapter`]), the
//! Track-1 bridge to `exchange-apiws`'s signed REST. That path requires
//! `KC_KEY` / `KC_SECRET` / `KC_PASSPHRASE` and is real trading — point those
//! at a sandbox/sub-account to paper-trade the identical code path. If the
//! adapter can't be built (missing creds, network), the demo logs loudly and
//! falls back to the paper `MockExchange` rather than trading on broken state.
//!
//! Other `DEMO_EXCHANGE` values:
//!
//! - `kraken` — live **Kraken spot** ([`KrakenSpotAdapter`], `CryptoSpot`,
//!   long-only). Needs `KRAKEN_API_KEY` / `KRAKEN_API_SECRET`.
//! - `multi` (or `kucoin+kraken`) — **both** venues at once behind a
//!   [`RoutingExchange`], so per-asset-class risk (`class_risk`) actually
//!   diverges: KuCoin perps resolve to `CryptoPerp` rules (5×), Kraken spot to
//!   `CryptoSpot` rules (1×). Symbols are split by venue (KuCoin perps end in
//!   `M`, e.g. `XBTUSDTM`; the rest go to Kraken — or set `DEMO_KUCOIN_SYMBOLS`
//!   / `DEMO_KRAKEN_SYMBOLS` explicitly). Fills from both venues are merged.
//!
//! When a live adapter is selected, its [`FillSource`] is wired too so the bot
//! consumes **real fills** (which also enables the framework's bracket/OCO
//! handling). The demo's paper PnL simulator is disabled in that mode to avoid
//! double-counting (see `main.rs`).

use std::sync::Arc;
use std::time::Duration;

use rustrade::{AssetClass, ExchangeClient, FillSource, RiskConfig};
use rustrade_exchange_apiws::{
    CompositeFillSource, KrakenFillSource, KrakenSpotAdapter, KucoinExchangeAdapter,
    KucoinFillSource, RoutingExchange,
};
use tracing::{error, info, warn};

use crate::mock_exchange::MockExchange;

/// The selected exchange plus, when live, its real-fill source and any
/// per-asset-class risk presets to apply.
pub struct Selected {
    /// Where orders go (paper `MockExchange` or a live adapter / router).
    pub exchange: Arc<dyn ExchangeClient>,
    /// Real fills from the exchange, present only on the live path. When
    /// `Some`, the caller must skip the paper PnL simulator (they'd
    /// double-count) — and the framework turns on bracket/OCO handling.
    pub fills: Option<Arc<dyn FillSource>>,
    /// Per-asset-class risk presets (`class_risk`) to apply on the bot config.
    /// Empty for single-venue modes — the bot trades one asset class, so the
    /// bot-wide config already fits. The multi-venue mode populates it so
    /// `CryptoPerp` and `CryptoSpot` symbols resolve to divergent rules.
    pub class_risk: Vec<(AssetClass, RiskConfig)>,
}

impl Selected {
    fn paper() -> Self {
        info!("exchange: MockExchange (paper — places no real orders)");
        Self {
            exchange: Arc::new(MockExchange),
            fills: None,
            class_risk: Vec::new(),
        }
    }
}

/// Build the configured exchange (+ fill source).
///
/// `leverage` should match the bot's `SizingConfig.leverage` so the per-order
/// leverage the adapter sends to KuCoin agrees with how positions were sized.
pub async fn build_exchange(symbols: &[String], leverage: u32) -> Selected {
    let want = std::env::var("DEMO_EXCHANGE").unwrap_or_else(|_| "mock".into());
    match want.to_ascii_lowercase().as_str() {
        "kucoin" => build_kucoin(symbols, leverage).await,
        "kraken" => build_kraken(symbols),
        "multi" | "kucoin+kraken" => build_multi(symbols, leverage).await,
        _ => Selected::paper(),
    }
}

/// KuCoin Futures (live): adapter + real fills via the private WS + /recentFills.
async fn build_kucoin(symbols: &[String], leverage: u32) -> Selected {
    warn!(
        "DEMO_EXCHANGE=kucoin — routing orders through LIVE KuCoin Futures. \
         Confirm KC_KEY/KC_SECRET/KC_PASSPHRASE target the account you intend to trade \
         (use a sandbox/sub-account to paper-trade this exact path)."
    );

    let syms: Vec<&str> = symbols.iter().map(String::as_str).collect();
    match KucoinExchangeAdapter::from_env(leverage, &syms).await {
        Ok(adapter) => {
            // Reuse the adapter's signed client for the fill source (same creds,
            // KuCoin Futures). The source streams real executions via the private
            // tradeOrders WS + /recentFills.
            let fills = KucoinFillSource::connect(
                adapter.client().clone(),
                exchange_apiws::KucoinEnv::LiveFutures,
                symbols.to_vec(),
                Duration::from_secs(5),
            );
            info!(
                leverage,
                symbols = syms.len(),
                "exchange: KuCoin Futures adapter (LIVE) + real fill source — \
                 contract multipliers fetched, brackets enabled"
            );
            Selected {
                exchange: Arc::new(adapter),
                fills: Some(Arc::new(fills)),
                class_risk: Vec::new(),
            }
        }
        Err(e) => {
            error!(
                error = %e,
                "kucoin adapter unavailable — FALLING BACK to MockExchange (paper)"
            );
            Selected::paper()
        }
    }
}

/// Kraken **spot** (live): adapter + real fills via TradesHistory polling.
///
/// Spot is long-only with no leverage and `AssetClass::CryptoSpot`. Use Kraken
/// pair names in `DEMO_SYMBOLS` (e.g. `XBTUSD`) and tune the demo's sizing for
/// spot (a `contract_value` of 1.0 means margin × leverage is the notional).
fn build_kraken(symbols: &[String]) -> Selected {
    warn!(
        "DEMO_EXCHANGE=kraken — routing orders through LIVE Kraken spot. \
         Confirm KRAKEN_API_KEY/KRAKEN_API_SECRET target the account you intend to trade."
    );
    let base_assets = kraken_base_assets(symbols);
    let refs: Vec<(&str, &str)> = base_assets
        .iter()
        .map(|(s, c)| (s.as_str(), c.as_str()))
        .collect();
    match KrakenSpotAdapter::from_env(&refs) {
        Ok(adapter) => {
            // Kraken has no private own-trades WS through exchange-apiws, so real
            // fills come from polling TradesHistory. Fee is in the quote (USD).
            let fills = KrakenFillSource::connect_default(adapter.client().clone(), "USD");
            info!(
                symbols = symbols.len(),
                base_assets = refs.len(),
                "exchange: Kraken spot adapter (LIVE) + real fill source (CryptoSpot)"
            );
            Selected {
                exchange: Arc::new(adapter),
                fills: Some(Arc::new(fills)),
                class_risk: Vec::new(),
            }
        }
        Err(e) => {
            error!(error = %e, "kraken adapter unavailable — FALLING BACK to MockExchange (paper)");
            Selected::paper()
        }
    }
}

/// Multi-venue (live): KuCoin Futures **and** Kraken spot behind a
/// [`RoutingExchange`], so per-asset-class risk diverges. Symbols are split by
/// venue (see [`split_venues`]); fills from both are merged into one stream.
/// Requires BOTH venues' credentials — if either adapter can't be built, falls
/// back to paper rather than trading half the book.
async fn build_multi(symbols: &[String], leverage: u32) -> Selected {
    warn!(
        "DEMO_EXCHANGE=multi — routing orders across LIVE KuCoin Futures + Kraken spot. \
         Confirm KC_* and KRAKEN_API_* target the accounts you intend to trade."
    );
    let (kucoin_syms, kraken_syms) = split_venues(symbols);
    if kucoin_syms.is_empty() || kraken_syms.is_empty() {
        error!(
            kucoin = kucoin_syms.len(),
            kraken = kraken_syms.len(),
            "multi needs symbols on BOTH venues (KuCoin perps end in 'M'; or set \
             DEMO_KUCOIN_SYMBOLS / DEMO_KRAKEN_SYMBOLS) — FALLING BACK to MockExchange (paper)"
        );
        return Selected::paper();
    }

    // KuCoin adapter (fetches contract multipliers for its symbols).
    let kucoin_refs: Vec<&str> = kucoin_syms.iter().map(String::as_str).collect();
    let kucoin = match KucoinExchangeAdapter::from_env(leverage, &kucoin_refs).await {
        Ok(a) => a,
        Err(e) => {
            error!(error = %e, "multi: kucoin adapter unavailable — FALLING BACK to paper");
            return Selected::paper();
        }
    };

    // Kraken spot adapter (base-asset codes for balance/position lookups).
    let kraken_base = kraken_base_assets(&kraken_syms);
    let kraken_refs: Vec<(&str, &str)> = kraken_base
        .iter()
        .map(|(s, c)| (s.as_str(), c.as_str()))
        .collect();
    let kraken = match KrakenSpotAdapter::from_env(&kraken_refs) {
        Ok(a) => a,
        Err(e) => {
            error!(error = %e, "multi: kraken adapter unavailable — FALLING BACK to paper");
            return Selected::paper();
        }
    };

    // Real fills per venue (reuse each adapter's signed client), merged so the
    // framework sees a single FillSource.
    let kucoin_fills = KucoinFillSource::connect(
        kucoin.client().clone(),
        exchange_apiws::KucoinEnv::LiveFutures,
        kucoin_syms.clone(),
        Duration::from_secs(5),
    );
    let kraken_fills = KrakenFillSource::connect_default(kraken.client().clone(), "USD");
    let merged: Arc<dyn FillSource> = Arc::new(CompositeFillSource::new(vec![
        Arc::new(kucoin_fills) as Arc<dyn FillSource>,
        Arc::new(kraken_fills) as Arc<dyn FillSource>,
    ]));

    // Compose the two adapters into one symbol-routed ExchangeClient.
    let kucoin_arc: Arc<dyn ExchangeClient> = Arc::new(kucoin);
    let kraken_arc: Arc<dyn ExchangeClient> = Arc::new(kraken);
    let routed = match RoutingExchange::builder()
        .route(kucoin_syms.iter().cloned(), kucoin_arc)
        .route(kraken_syms.iter().cloned(), kraken_arc)
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "multi: routing build failed — FALLING BACK to paper");
            return Selected::paper();
        }
    };

    info!(
        kucoin = kucoin_syms.len(),
        kraken = kraken_syms.len(),
        leverage,
        "exchange: multi-venue RoutingExchange (LIVE) — KuCoin perps (CryptoPerp) + \
         Kraken spot (CryptoSpot, 1×), real fills merged; per-asset-class risk active"
    );
    Selected {
        exchange: Arc::new(routed),
        fills: Some(merged),
        // The whole point of this mode: divergent risk per asset class.
        class_risk: vec![
            (AssetClass::CryptoPerp, RiskConfig::crypto_perp()),
            (AssetClass::CryptoSpot, RiskConfig::crypto_spot()),
        ],
    }
}

/// Split the bot's symbols into `(kucoin, kraken)`. Explicit
/// `DEMO_KUCOIN_SYMBOLS` / `DEMO_KRAKEN_SYMBOLS` (comma lists) win; otherwise
/// infer by KuCoin's perpetual suffix — symbols ending in `M` (e.g. `XBTUSDTM`)
/// are KuCoin Futures, the rest are Kraken spot. When set explicitly,
/// `DEMO_SYMBOLS` should be the union (every bot symbol needs a venue).
pub(crate) fn split_venues(symbols: &[String]) -> (Vec<String>, Vec<String>) {
    let explicit_kucoin = std::env::var("DEMO_KUCOIN_SYMBOLS").ok();
    let explicit_kraken = std::env::var("DEMO_KRAKEN_SYMBOLS").ok();
    if explicit_kucoin.is_some() || explicit_kraken.is_some() {
        let parse = |v: Option<String>| -> Vec<String> {
            v.map(|s| {
                s.split(',')
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .collect()
            })
            .unwrap_or_default()
        };
        return (parse(explicit_kucoin), parse(explicit_kraken));
    }
    let mut kucoin = Vec::new();
    let mut kraken = Vec::new();
    for s in symbols {
        if s.ends_with('M') {
            kucoin.push(s.clone());
        } else {
            kraken.push(s.clone());
        }
    }
    (kucoin, kraken)
}

/// Resolve `symbol → Kraken base-asset code` from `DEMO_KRAKEN_BASE_ASSETS`
/// (`"XBTUSD:XXBT,ETHUSD:XETH"`) or a built-in default for common USD pairs.
/// Unmapped symbols report flat positions (the adapter warns).
fn kraken_base_assets(symbols: &[String]) -> Vec<(String, String)> {
    if let Ok(env) = std::env::var("DEMO_KRAKEN_BASE_ASSETS") {
        return env
            .split(',')
            .filter_map(|entry| entry.split_once(':'))
            .map(|(s, c)| (s.trim().to_string(), c.trim().to_string()))
            .collect();
    }
    // Best-effort defaults: Kraken uses legacy X-prefixed codes for BTC/ETH.
    let code_for = |sym: &str| -> Option<&'static str> {
        match sym {
            s if s.starts_with("XBT") => Some("XXBT"),
            s if s.starts_with("ETH") => Some("XETH"),
            s if s.starts_with("SOL") => Some("SOL"),
            s if s.starts_with("ADA") => Some("ADA"),
            s if s.starts_with("DOT") => Some("DOT"),
            _ => None,
        }
    };
    symbols
        .iter()
        .filter_map(|s| code_for(s).map(|c| (s.clone(), c.to_string())))
        .collect()
}
