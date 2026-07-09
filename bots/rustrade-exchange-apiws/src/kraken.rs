//! [`KrakenSpotAdapter`] — a [`rustrade::ExchangeClient`] over Kraken **spot**,
//! via [`exchange-apiws`]'s signed `KrakenPrivateClient`.
//!
//! Spot is a different shape from KuCoin Futures, and the adapter models it
//! honestly:
//!
//! - **Long-only, no leverage.** You hold the base asset; there are no
//!   contracts and no shorts. `contract_value` is `1.0` and the instrument is
//!   [`AssetClass::CryptoSpot`], so the framework's per-asset-class risk picks
//!   the spot rules.
//! - **A "position" is your balance.** [`get_position`](ExchangeClient::get_position)
//!   returns the base-asset balance as a (non-negative) qty; closing it is a
//!   market **sell** of that balance.
//! - **Orders are in base-asset units** (e.g. `0.5` BTC), not contracts.
//!   Market and limit only — Kraken's stop / IOC / FOK / post-only aren't on
//!   this surface, so the adapter rejects them and advertises them as
//!   unsupported.
//!
//! # Asset codes
//!
//! Kraken keys balances by its own asset codes (`XXBT`, `XETH`, `ZUSD`, `SOL`,
//! …), which aren't derivable from a pair string. So `get_position` needs a
//! `symbol → base-asset-code` map, supplied at construction — e.g.
//! `("XBTUSD", "XXBT")`. The trading `pair` is the symbol itself (use Kraken's
//! pair names, e.g. `XBTUSD`).
//!
//! # Example
//!
//! ```no_run
//! use rustrade_exchange_apiws::KrakenSpotAdapter;
//! # async fn demo() -> rustrade::Result<()> {
//! // KRAKEN_API_KEY / KRAKEN_API_SECRET from the env; map each pair to its
//! // Kraken base-asset code for balance/position lookups.
//! let exchange = KrakenSpotAdapter::from_env(&[("XBTUSD", "XXBT"), ("ETHUSD", "XETH")])?;
//! # let _ = exchange;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustrade::{
    AssetClass, Capability, ExchangeClient, Fill, FillSource, InstrumentSpec, OpenOrder, Order,
    OrderKind, OrderStatus, Position, Price, Result, Side, Symbol, Volume,
};
use tokio::sync::{Mutex as AsyncMutex, OnceCell, mpsc, watch};
use tracing::{debug, info, warn};

use exchange_apiws::kraken::{KrakenAssetPair, KrakenTradeHistoryEntry};
use exchange_apiws::{KrakenCredentials, KrakenPrivateClient, KrakenRestClient};

use crate::ex;

// ── Pure mapping helpers (unit-tested, no network) ───────────────────────────

/// framework [`Side`] → Kraken `"buy"` / `"sell"`.
fn kraken_side(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

/// framework [`OrderKind`] → Kraken `ordertype`. Spot here supports market and
/// limit only; stop / IOC / FOK / post-only aren't on this REST surface.
fn kraken_order_type(kind: OrderKind) -> Result<&'static str> {
    match kind {
        OrderKind::Market => Ok("market"),
        OrderKind::Limit => Ok("limit"),
        OrderKind::Ioc | OrderKind::Fok | OrderKind::PostOnly => Err(rustrade::Error::exchange(
            format!("Kraken spot adapter supports market/limit only, not {kind:?}"),
        )),
    }
}

/// Format an amount as a plain decimal string (no scientific notation, trailing
/// zeros trimmed) — Kraken accepts decimal `volume`/`price` strings.
fn fmt_amount(v: f64) -> String {
    let s = format!("{v:.8}");
    // Trim trailing zeros, then a trailing dot.
    let s = s.trim_end_matches('0');
    s.trim_end_matches('.').to_string()
}

/// Parse a Kraken balance map entry into an `f64` (missing / unparseable ⇒ 0).
fn balance_of(balances: &HashMap<String, String>, asset: &str) -> f64 {
    balances
        .get(asset)
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0)
}

/// Kraken order `status` string → framework [`OrderStatus`].
fn order_status(status: &str, filled: f64, size: f64) -> OrderStatus {
    match status {
        "pending" => OrderStatus::Pending,
        "open" => {
            if filled > 0.0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Open
            }
        }
        "closed" => OrderStatus::Filled,
        "canceled" | "expired" => {
            // A cancel after a partial fill still leaves some filled.
            if filled >= size && size > 0.0 {
                OrderStatus::Filled
            } else {
                OrderStatus::Cancelled
            }
        }
        _ => OrderStatus::Open,
    }
}

/// Kraken `opentm` (fractional Unix **seconds**) → `DateTime<Utc>`.
fn secs_to_dt(secs: f64) -> Option<DateTime<Utc>> {
    let whole = secs.trunc() as i64;
    let nanos = ((secs.fract()) * 1e9) as u32;
    DateTime::<Utc>::from_timestamp(whole, nanos)
}

/// Kraken advertises price/lot precision as a decimal-place count; convert it to
/// the corresponding increment (e.g. `2 → 0.01`, `8 → 1e-8`, `0 → 1.0`).
fn decimals_to_increment(decimals: u32) -> f64 {
    10f64.powi(-(decimals as i32))
}

/// Build an [`InstrumentSpec`] from a Kraken [`KrakenAssetPair`]: spot,
/// `contract_value` 1.0, tick/lot from the pair's decimal precision, and
/// `min_notional` from the pair's minimum order *cost* (`costmin`, the quote
/// notional). A pair with no `costmin` ⇒ `min_notional` 0.0 — unconstrained,
/// the same permissive default as before the field was exposed.
fn spec_from_pair(p: &KrakenAssetPair) -> InstrumentSpec {
    InstrumentSpec {
        asset_class: AssetClass::CryptoSpot,
        contract_value: 1.0,
        tick_size: decimals_to_increment(p.pair_decimals),
        lot_size: decimals_to_increment(p.lot_decimals),
        min_notional: p.costmin_f64().unwrap_or(0.0),
    }
}

// ── Adapter ──────────────────────────────────────────────────────────────────

/// A [`rustrade::ExchangeClient`] for Kraken **spot**. See this module's documentation.
#[derive(Clone)]
pub struct KrakenSpotAdapter {
    client: KrakenPrivateClient,
    /// symbol → Kraken base-asset code (for balance → position lookups).
    base_assets: HashMap<String, String>,
    /// Lazily-fetched per-pair instrument specs (Kraken **altname** → spec),
    /// derived from the public `AssetPairs` endpoint so live orders round to
    /// Kraken's real tick/lot precision instead of a blind 8 dp. `Arc<OnceCell>`
    /// so clones share a single fetch; populated on the first async touch
    /// (`place_order` / [`prime_instrument_specs`](Self::prime_instrument_specs)).
    specs: Arc<OnceCell<HashMap<String, InstrumentSpec>>>,
}

impl KrakenSpotAdapter {
    /// Wrap an existing [`KrakenPrivateClient`]. Register base-asset codes with
    /// [`with_base_asset`](Self::with_base_asset) before relying on
    /// [`get_position`](ExchangeClient::get_position).
    #[must_use]
    pub fn new(client: KrakenPrivateClient) -> Self {
        Self {
            client,
            base_assets: HashMap::new(),
            specs: Arc::new(OnceCell::new()),
        }
    }

    /// Register the Kraken base-asset code for a `symbol` (builder style) — e.g.
    /// `("XBTUSD", "XXBT")`. Needed so positions read the right balance.
    #[must_use]
    pub fn with_base_asset(
        mut self,
        symbol: impl Into<String>,
        asset_code: impl Into<String>,
    ) -> Self {
        self.base_assets.insert(symbol.into(), asset_code.into());
        self
    }

    /// Build from explicit credentials, registering each `(symbol, base-asset
    /// code)`.
    ///
    /// # Errors
    /// Fails if the HTTP client can't be built.
    pub fn connect(creds: KrakenCredentials, base_assets: &[(&str, &str)]) -> Result<Self> {
        let client = KrakenPrivateClient::new(creds).map_err(ex)?;
        let mut adapter = Self::new(client);
        for (sym, code) in base_assets {
            adapter = adapter.with_base_asset(*sym, *code);
        }
        Ok(adapter)
    }

    /// Build from `KRAKEN_API_KEY` / `KRAKEN_API_SECRET`, registering each
    /// `(symbol, base-asset code)`.
    ///
    /// # Errors
    /// Fails if a credential env var is missing or the client can't be built.
    pub fn from_env(base_assets: &[(&str, &str)]) -> Result<Self> {
        let creds = KrakenCredentials::from_env().map_err(ex)?;
        Self::connect(creds, base_assets)
    }

    /// Borrow the underlying signed client.
    #[must_use]
    pub fn client(&self) -> &KrakenPrivateClient {
        &self.client
    }

    /// The per-pair instrument specs, fetched once from Kraken's **public**
    /// `AssetPairs` endpoint and cached for the process lifetime. Mirrors the
    /// crypto spot bot's `lot_decimals_for`: on any failure (public client can't
    /// be built, or the fetch errors) it caches an empty map, so specs degrade
    /// to a permissive spot default rather than blocking orders. Keyed by Kraken
    /// **altname** (e.g. `"XBTUSD"`), matching the pair names orders use.
    async fn cached_specs(&self) -> &HashMap<String, InstrumentSpec> {
        self.specs
            .get_or_init(|| async {
                let public = match KrakenRestClient::new() {
                    Ok(c) => c,
                    Err(e) => {
                        warn!(error = %e, "Kraken: public client build failed — instrument specs unavailable");
                        return HashMap::new();
                    }
                };
                match public.get_asset_pairs(None).await {
                    Ok(pairs) => pairs
                        .values()
                        .map(|p| (p.altname.clone(), spec_from_pair(p)))
                        .collect(),
                    Err(e) => {
                        warn!(error = %e, "Kraken: AssetPairs fetch failed — instrument specs default to permissive spot");
                        HashMap::new()
                    }
                }
            })
            .await
    }

    /// Cached [`InstrumentSpec`] for `symbol` if the spec cache is already warm,
    /// else `None`. Sync — safe to call from [`ExchangeClient::instrument_spec`].
    fn cached_spec(&self, symbol: &str) -> Option<InstrumentSpec> {
        self.specs.get().and_then(|m| m.get(symbol).copied())
    }

    /// Warm the instrument-spec cache from Kraken's public `AssetPairs` so
    /// [`instrument_spec`](ExchangeClient::instrument_spec) reports real
    /// tick/lot precision from the very first order. Idempotent and safe to call
    /// at startup. Returns the number of pairs cached (`0` if the fetch failed —
    /// specs then fall back to a permissive spot default).
    pub async fn prime_instrument_specs(&self) -> usize {
        self.cached_specs().await.len()
    }
}

#[async_trait]
impl ExchangeClient for KrakenSpotAdapter {
    fn name(&self) -> &str {
        "kraken"
    }

    async fn place_order(&self, order: &Order) -> Result<String> {
        if order.stop.is_some() {
            return Err(rustrade::Error::exchange(
                "Kraken spot adapter does not support stop attachments",
            ));
        }
        let order_type = kraken_order_type(order.kind)?;
        // Round to Kraken's advertised precision (lazily fetched + cached). The
        // framework snaps limit prices via `instrument_spec`, but never rounds
        // order volume — so we round volume down to the pair's lot here. This is
        // the live-order money path: it must not submit more precision than
        // Kraken accepts. Unknown pair / cold cache ⇒ a no-op (raw value).
        let spec = self
            .cached_specs()
            .await
            .get(order.symbol.as_str())
            .copied();
        let volume =
            fmt_amount(spec.map_or(order.size.value(), |s| s.round_qty_down(order.size.value())));
        let price = if order.kind == OrderKind::Limit {
            order
                .limit_price
                .map(|p| fmt_amount(spec.map_or(p.value(), |s| s.round_price(p.value()))))
        } else {
            None
        };
        let resp = self
            .client
            .place_order(
                order.symbol.as_str(),
                kraken_side(order.side),
                order_type,
                &volume,
                price.as_deref(),
            )
            .await
            .map_err(ex)?;
        resp.txid
            .into_iter()
            .next()
            .ok_or_else(|| rustrade::Error::exchange("Kraken AddOrder returned no txid"))
    }

    async fn cancel_all(&self, symbol: &Symbol) -> Result<usize> {
        let open = self.client.get_open_orders().await.map_err(ex)?.open;
        let mut cancelled = 0;
        for (txid, order) in open {
            let pair_matches = order
                .descr
                .as_ref()
                .is_some_and(|d| d.pair == symbol.as_str());
            if pair_matches && self.client.cancel_order(&txid).await.is_ok() {
                cancelled += 1;
            }
        }
        Ok(cancelled)
    }

    async fn close_position(&self, symbol: &Symbol, position: &Position) -> Result<String> {
        if position.is_flat() {
            return Err(rustrade::Error::exchange(format!(
                "close_position: {} holds no balance",
                symbol.as_str()
            )));
        }
        // Spot is long-only: closing a holding is a market SELL of the balance,
        // rounded DOWN to the pair's lot so Kraken accepts the precision and we
        // never try to sell more than we hold. No-op when the spec is unknown.
        let spec = self.cached_specs().await.get(symbol.as_str()).copied();
        let volume =
            fmt_amount(spec.map_or(position.qty.abs(), |s| s.round_qty_down(position.qty.abs())));
        let resp = self
            .client
            .place_order(symbol.as_str(), "sell", "market", &volume, None)
            .await
            .map_err(ex)?;
        resp.txid
            .into_iter()
            .next()
            .ok_or_else(|| rustrade::Error::exchange("Kraken AddOrder returned no txid"))
    }

    async fn get_position(&self, symbol: &Symbol) -> Result<Position> {
        let Some(asset) = self.base_assets.get(symbol.as_str()) else {
            // Without the base-asset code we can't read the holding; treat as flat.
            tracing::warn!(
                symbol = %symbol,
                "no Kraken base-asset code registered — reporting flat (use with_base_asset)"
            );
            return Ok(Position::FLAT);
        };
        let balances = self.client.get_balance().await.map_err(ex)?;
        let qty = balance_of(&balances, asset);
        Ok(Position {
            qty,
            // Spot has no exchange-side entry price (would need cost-basis tracking).
            entry_price: None,
            unrealised_pnl: 0.0,
        })
    }

    async fn get_balance(&self, currency: &str) -> Result<f64> {
        let balances = self.client.get_balance().await.map_err(ex)?;
        Ok(balance_of(&balances, currency))
    }

    fn supports(&self, capability: Capability) -> bool {
        // Spot via the REST AddOrder surface: only resting-order tracking.
        matches!(capability, Capability::OrderTracking)
    }

    fn contract_value(&self, _symbol: &Symbol) -> f64 {
        1.0
    }

    fn instrument_spec(&self, symbol: &Symbol) -> InstrumentSpec {
        // Real tick/lot from Kraken's AssetPairs once the cache is warm (primed
        // at startup or after the first order); a permissive spot default while
        // still cold, so behaviour is safe before the async fetch runs. This is
        // sync (trait contract), so it can only READ the cache — the fetch is
        // driven from the async order path / `prime_instrument_specs`. A warm
        // spec carries the pair's real `min_notional` (from `costmin`); the
        // cold default below stays 0.0 (unconstrained) until the fetch runs.
        self.cached_spec(symbol.as_str()).unwrap_or(InstrumentSpec {
            asset_class: AssetClass::CryptoSpot,
            contract_value: 1.0,
            tick_size: 0.0,
            lot_size: 0.0,
            min_notional: 0.0,
        })
    }

    async fn get_open_orders(&self, symbol: &Symbol) -> Result<Vec<OpenOrder>> {
        let open = self.client.get_open_orders().await.map_err(ex)?.open;
        let mut out = Vec::new();
        for (txid, order) in open {
            let Some(descr) = &order.descr else { continue };
            if descr.pair != symbol.as_str() {
                continue;
            }
            let size = order.vol.parse::<f64>().unwrap_or(0.0);
            let filled = order.vol_exec.parse::<f64>().unwrap_or(0.0);
            out.push(OpenOrder {
                order_id: txid,
                client_id: None,
                symbol: symbol.clone(),
                side: if descr.side == "sell" {
                    Side::Sell
                } else {
                    Side::Buy
                },
                kind: if descr.ordertype == "limit" {
                    OrderKind::Limit
                } else {
                    OrderKind::Market
                },
                limit_price: descr
                    .price
                    .parse::<f64>()
                    .ok()
                    .filter(|p| *p > 0.0)
                    .map(Price),
                size: Volume(size),
                filled: Volume(filled),
                status: order_status(&order.status, filled, size),
                created_at: order.opentm.and_then(secs_to_dt),
            });
        }
        Ok(out)
    }

    async fn cancel_order(&self, _symbol: &Symbol, order_id: &str) -> Result<bool> {
        let resp = self.client.cancel_order(order_id).await.map_err(ex)?;
        Ok(resp.count > 0)
    }
}

// ── Fill source ──────────────────────────────────────────────────────────────

/// Default poll cadence for the Kraken fill source.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Bounded dedup memory (Kraken's TradesHistory first page is ~50 trades).
const SEEN_CAPACITY: usize = 10_000;

/// A [`rustrade::FillSource`] that streams the account's **real** Kraken trades
/// into the bot by polling `/0/private/TradesHistory`, deduped by trade id.
///
/// Kraken doesn't expose a private own-trades WS through `exchange-apiws`, so
/// this is poll-based (unlike [`KucoinFillSource`](crate::KucoinFillSource)'s
/// WS trigger). Trades are in **base-asset units** with the fee in quote
/// currency. Startup is baselined so pre-existing history isn't replayed.
pub struct KrakenFillSource {
    rx: AsyncMutex<mpsc::UnboundedReceiver<Fill>>,
    _shutdown: watch::Sender<bool>,
}

impl KrakenFillSource {
    /// Connect a fill source polling every `poll_interval`. `fee_currency` labels
    /// the fee (Kraken charges it in the quote currency, which `TradesHistory`
    /// doesn't name per row) — e.g. `"USD"`. Spawns the driver on the current
    /// Tokio runtime; the baseline snapshot happens inside the task.
    #[must_use]
    pub fn connect(
        client: KrakenPrivateClient,
        fee_currency: impl Into<String>,
        poll_interval: Duration,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<Fill>();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        tokio::spawn(drive_fills(
            client,
            fee_currency.into(),
            poll_interval.max(Duration::from_secs(1)),
            tx,
            shutdown_rx,
        ));
        Self {
            rx: AsyncMutex::new(rx),
            _shutdown: shutdown_tx,
        }
    }

    /// Connect with the default poll cadence (5 s).
    #[must_use]
    pub fn connect_default(client: KrakenPrivateClient, fee_currency: impl Into<String>) -> Self {
        Self::connect(client, fee_currency, DEFAULT_POLL_INTERVAL)
    }
}

#[async_trait]
impl FillSource for KrakenFillSource {
    async fn next_fill(&self) -> Option<Fill> {
        self.rx.lock().await.recv().await
    }
}

/// Bounded FIFO set of already-emitted trade ids.
struct Seen {
    set: HashSet<String>,
    order: VecDeque<String>,
}

impl Seen {
    fn new() -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
        }
    }
    /// Record `id`; returns `true` if it was not seen before.
    fn insert(&mut self, id: &str) -> bool {
        if self.set.contains(id) {
            return false;
        }
        if self.order.len() >= SEEN_CAPACITY
            && let Some(old) = self.order.pop_front()
        {
            self.set.remove(&old);
        }
        self.set.insert(id.to_string());
        self.order.push_back(id.to_string());
        true
    }
}

/// Convert a Kraken trade-history entry into a framework [`Fill`].
fn trade_to_fill(e: &KrakenTradeHistoryEntry, fee_ccy: &str) -> Fill {
    Fill {
        symbol: Symbol::from(e.pair.as_str()),
        order_id: e.ordertxid.clone(),
        client_id: None,
        side: if e.side == "sell" {
            Side::Sell
        } else {
            Side::Buy
        },
        price: Price(e.price.parse::<f64>().unwrap_or(0.0)),
        size: Volume(e.vol.parse::<f64>().unwrap_or(0.0)),
        fee: e.fee.parse::<f64>().unwrap_or(0.0),
        fee_currency: fee_ccy.to_string(),
        timestamp: secs_to_dt(e.time).unwrap_or_else(Utc::now),
    }
}

/// Driver: baseline, then poll TradesHistory on a cadence, emitting new trades.
async fn drive_fills(
    client: KrakenPrivateClient,
    fee_ccy: String,
    poll_interval: Duration,
    tx: mpsc::UnboundedSender<Fill>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut seen = Seen::new();
    // Baseline: record existing trades without replaying them.
    hydrate_fills(&client, &fee_ccy, &mut seen, true, &tx).await;
    info!("kraken fill source baselined; polling TradesHistory");

    let mut tick = tokio::time::interval(poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = tick.tick() => hydrate_fills(&client, &fee_ccy, &mut seen, false, &tx).await,
        }
    }
    debug!("kraken fill source driver stopped");
}

/// Fetch TradesHistory and emit trades not yet seen (baseline = record only).
async fn hydrate_fills(
    client: &KrakenPrivateClient,
    fee_ccy: &str,
    seen: &mut Seen,
    baseline: bool,
    tx: &mpsc::UnboundedSender<Fill>,
) {
    let history = match client.get_trades_history().await {
        Ok(h) => h,
        Err(e) => {
            warn!(error = %e, "kraken TradesHistory poll failed");
            return;
        }
    };
    for (trade_id, entry) in &history.trades {
        let is_new = seen.insert(trade_id);
        if is_new && !baseline && tx.send(trade_to_fill(entry, fee_ccy)).is_err() {
            return; // receiver gone
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_maps() {
        assert_eq!(kraken_side(Side::Buy), "buy");
        assert_eq!(kraken_side(Side::Sell), "sell");
    }

    #[test]
    fn order_type_market_limit_only() {
        assert_eq!(kraken_order_type(OrderKind::Market).unwrap(), "market");
        assert_eq!(kraken_order_type(OrderKind::Limit).unwrap(), "limit");
        assert!(kraken_order_type(OrderKind::Ioc).is_err());
        assert!(kraken_order_type(OrderKind::Fok).is_err());
        assert!(kraken_order_type(OrderKind::PostOnly).is_err());
    }

    #[test]
    fn amount_is_plain_decimal_trimmed() {
        assert_eq!(fmt_amount(0.5), "0.5");
        assert_eq!(fmt_amount(1000.0), "1000");
        assert_eq!(fmt_amount(0.001), "0.001");
        assert_eq!(fmt_amount(1.23456789), "1.23456789");
        // No scientific notation for small values.
        assert_eq!(fmt_amount(0.00000001), "0.00000001");
    }

    #[test]
    fn balance_lookup_parses_or_defaults_zero() {
        let mut b = HashMap::new();
        b.insert("XXBT".to_string(), "0.75".to_string());
        b.insert("ZUSD".to_string(), "1234.5".to_string());
        assert!((balance_of(&b, "XXBT") - 0.75).abs() < 1e-9);
        assert!((balance_of(&b, "ZUSD") - 1234.5).abs() < 1e-9);
        assert_eq!(balance_of(&b, "MISSING"), 0.0);
    }

    #[test]
    fn status_maps() {
        assert_eq!(order_status("open", 0.0, 1.0), OrderStatus::Open);
        assert_eq!(order_status("open", 0.5, 1.0), OrderStatus::PartiallyFilled);
        assert_eq!(order_status("closed", 1.0, 1.0), OrderStatus::Filled);
        assert_eq!(order_status("canceled", 0.0, 1.0), OrderStatus::Cancelled);
        assert_eq!(order_status("pending", 0.0, 1.0), OrderStatus::Pending);
    }

    #[test]
    fn instrument_spec_is_spot() {
        let a = KrakenSpotAdapter::new(
            KrakenPrivateClient::new(KrakenCredentials::new("k", "c2VjcmV0")).unwrap(),
        )
        .with_base_asset("XBTUSD", "XXBT");
        let spec = a.instrument_spec(&Symbol::from("XBTUSD"));
        assert_eq!(spec.asset_class, AssetClass::CryptoSpot);
        assert_eq!(spec.contract_value, 1.0);
        // Cold cache (no network): permissive defaults — tick/lot/min unconstrained.
        assert_eq!(spec.tick_size, 0.0);
        assert_eq!(spec.lot_size, 0.0);
        assert_eq!(spec.min_notional, 0.0);
        assert_eq!(a.name(), "kraken");
        assert!(a.supports(Capability::OrderTracking));
        assert!(!a.supports(Capability::StopOrders));
    }

    #[test]
    fn decimals_to_increment_maps_precision() {
        assert_eq!(decimals_to_increment(0), 1.0);
        assert!((decimals_to_increment(1) - 0.1).abs() < 1e-12);
        assert!((decimals_to_increment(2) - 0.01).abs() < 1e-12);
        assert!((decimals_to_increment(8) - 1e-8).abs() < 1e-15);
    }

    #[test]
    fn instrument_spec_uses_cached_pair_precision() {
        let a = KrakenSpotAdapter::new(
            KrakenPrivateClient::new(KrakenCredentials::new("k", "c2VjcmV0")).unwrap(),
        )
        .with_base_asset("XBTUSD", "XXBT");
        // Seed the cache exactly as the async AssetPairs fetch would: XBTUSD with
        // 1-dp price precision and 8-dp lot precision.
        let mut specs = HashMap::new();
        specs.insert(
            "XBTUSD".to_string(),
            InstrumentSpec {
                asset_class: AssetClass::CryptoSpot,
                contract_value: 1.0,
                tick_size: decimals_to_increment(1),
                lot_size: decimals_to_increment(8),
                min_notional: 0.0,
            },
        );
        a.specs.set(specs).expect("cache starts empty");

        let spec = a.instrument_spec(&Symbol::from("XBTUSD"));
        assert!((spec.tick_size - 0.1).abs() < 1e-12);
        assert!((spec.lot_size - 1e-8).abs() < 1e-15);
        assert_eq!(spec.min_notional, 0.0);

        // Unknown pair → permissive spot default (cache miss).
        let miss = a.instrument_spec(&Symbol::from("NOPE"));
        assert_eq!(miss.tick_size, 0.0);
        assert_eq!(miss.lot_size, 0.0);
        assert_eq!(miss.asset_class, AssetClass::CryptoSpot);
    }

    fn asset_pair(costmin: Option<&str>) -> KrakenAssetPair {
        KrakenAssetPair {
            altname: "XBTUSD".into(),
            wsname: None,
            base: "XXBT".into(),
            quote: "ZUSD".into(),
            pair_decimals: 1,
            lot_decimals: 8,
            lot_multiplier: 1,
            ordermin: Some("0.00005".into()),
            costmin: costmin.map(Into::into),
            cost_decimals: Some(5),
            status: Some("online".into()),
        }
    }

    #[test]
    fn spec_min_notional_from_costmin() {
        // A pair advertising a min order cost → min_notional carries it, while
        // tick/lot still derive from the pair's decimal precision.
        let spec = spec_from_pair(&asset_pair(Some("0.5")));
        assert!((spec.min_notional - 0.5).abs() < 1e-12);
        assert!((spec.tick_size - 0.1).abs() < 1e-12);
        assert!((spec.lot_size - 1e-8).abs() < 1e-15);
        assert_eq!(spec.asset_class, AssetClass::CryptoSpot);

        // No costmin (absent / unparseable) → 0.0: unconstrained, the permissive
        // default that held before the field was exposed.
        assert_eq!(spec_from_pair(&asset_pair(None)).min_notional, 0.0);
        assert_eq!(spec_from_pair(&asset_pair(Some("nope"))).min_notional, 0.0);
    }

    #[test]
    fn secs_to_dt_roundtrips() {
        let dt = secs_to_dt(1_700_000_000.5).expect("valid");
        assert_eq!(dt.timestamp(), 1_700_000_000);
    }

    fn trade(side: &str, price: &str, vol: &str) -> KrakenTradeHistoryEntry {
        KrakenTradeHistoryEntry {
            ordertxid: "O123-ABC".into(),
            postxid: String::new(),
            pair: "XBTUSD".into(),
            time: 1_700_000_000.0,
            side: side.into(),
            ordertype: "market".into(),
            price: price.into(),
            cost: "0".into(),
            fee: "0.42".into(),
            vol: vol.into(),
            margin: String::new(),
            misc: String::new(),
        }
    }

    #[test]
    fn trade_converts_to_fill() {
        let f = trade_to_fill(&trade("sell", "65000.0", "0.25"), "USD");
        assert_eq!(f.symbol, Symbol::from("XBTUSD"));
        assert_eq!(f.order_id, "O123-ABC");
        assert_eq!(f.side, Side::Sell);
        assert_eq!(f.price, Price(65000.0));
        assert_eq!(f.size, Volume(0.25));
        assert!((f.fee - 0.42).abs() < 1e-9);
        assert_eq!(f.fee_currency, "USD");
    }

    #[test]
    fn seen_dedupes_and_evicts() {
        let mut seen = Seen::new();
        assert!(seen.insert("t1"));
        assert!(!seen.insert("t1"));
        assert!(seen.insert("t2"));
    }
}
