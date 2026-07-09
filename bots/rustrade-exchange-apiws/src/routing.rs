//! [`RoutingExchange`] — one [`rustrade::ExchangeClient`] that fans each call
//! out to a **per-symbol** venue, plus [`CompositeFillSource`] to merge those
//! venues' fills into a single stream.
//!
//! # Why
//!
//! A `rustrade::Bot` holds **one** `ExchangeClient`, but per-asset-class risk
//! ([`BotConfigBuilder::class_risk`](rustrade::BotConfig)) only earns its keep
//! when one bot trades **more than one** asset class at once — e.g. KuCoin
//! perpetuals (`CryptoPerp`, 5×) *and* Kraken spot (`CryptoSpot`, 1×). The
//! framework resolves a symbol's [`AssetClass`](rustrade::AssetClass) from
//! [`ExchangeClient::instrument_spec`] at startup, then applies the matching
//! class preset. So if a single `ExchangeClient` returns the *right* venue's
//! `instrument_spec` for each symbol, `resolve_risk` (per-symbol → per-class →
//! default) picks the right rules automatically — no framework change needed.
//!
//! `RoutingExchange` is exactly that client: a thin dispatcher keyed by symbol.
//! Every call carrying a symbol ([`place_order`](ExchangeClient::place_order)
//! routes on `order.symbol`) goes to that symbol's venue; the venue's own
//! `instrument_spec` / `contract_value` / capabilities flow back through.
//!
//! # Global calls (no symbol)
//!
//! Two trait methods aren't per-symbol, so the router answers conservatively:
//!
//! - [`supports`](ExchangeClient::supports) → the **intersection**: a capability
//!   is advertised only if *every* venue supports it. (A bracket placed on a
//!   symbol whose venue lacks stops would just fail, so the safe global answer
//!   is "only what all venues can do".)
//! - [`get_balance`](ExchangeClient::get_balance) → the **sum** across venues
//!   for the given currency (a venue that doesn't hold it, or errors, counts 0
//!   and is logged). Pass a currency code each venue recognises.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use rustrade_exchange_apiws::{KrakenSpotAdapter, KucoinExchangeAdapter, RoutingExchange};
//! # async fn demo() -> rustrade::Result<()> {
//! let kucoin = Arc::new(KucoinExchangeAdapter::from_env(5, &["XBTUSDTM"]).await?);
//! let kraken = Arc::new(KrakenSpotAdapter::from_env(&[("XBTUSD", "XXBT")])?);
//!
//! let exchange = Arc::new(
//!     RoutingExchange::builder()
//!         .route(["XBTUSDTM"], kucoin) // → CryptoPerp risk
//!         .route(["XBTUSD"], kraken)   // → CryptoSpot risk
//!         .build()?,
//! );
//! // hand `exchange` to `Bot::new` with
//! // `.class_risk(AssetClass::CryptoPerp, RiskConfig::crypto_perp())`
//! // `.class_risk(AssetClass::CryptoSpot, RiskConfig::crypto_spot())`
//! # let _ = exchange;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rustrade::{
    Candle, CandleSource, Capability, Error, ExchangeClient, Fill, FillSource, InstrumentSpec,
    OpenOrder, Order, Position, Result, Symbol,
};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tracing::warn;

// ── RoutingExchange ───────────────────────────────────────────────────────────

/// A [`rustrade::ExchangeClient`] that dispatches each call to a per-symbol
/// venue. See this module's documentation.
pub struct RoutingExchange {
    /// symbol → the venue that trades it.
    routes: HashMap<Symbol, Arc<dyn ExchangeClient>>,
    /// Distinct venues (one per [`RoutingExchangeBuilder::route`] call), for the
    /// symbol-less global calls (`supports` intersection, `get_balance` sum).
    venues: Vec<Arc<dyn ExchangeClient>>,
    /// Composed identifier, e.g. `routing(kucoin+kraken)`.
    name: String,
}

impl RoutingExchange {
    /// Start building a router. Add venues with
    /// [`route`](RoutingExchangeBuilder::route).
    #[must_use]
    pub fn builder() -> RoutingExchangeBuilder {
        RoutingExchangeBuilder::default()
    }

    /// The venue for `symbol`, or an `Exchange` error if none is registered —
    /// an unmapped symbol is a routing misconfiguration and should be loud.
    fn venue_for(&self, symbol: &Symbol) -> Result<&Arc<dyn ExchangeClient>> {
        self.routes.get(symbol).ok_or_else(|| {
            Error::exchange(format!(
                "RoutingExchange: no venue registered for symbol {symbol}"
            ))
        })
    }
}

#[async_trait]
impl ExchangeClient for RoutingExchange {
    fn name(&self) -> &str {
        &self.name
    }

    async fn place_order(&self, order: &Order) -> Result<String> {
        self.venue_for(&order.symbol)?.place_order(order).await
    }

    async fn cancel_all(&self, symbol: &Symbol) -> Result<usize> {
        self.venue_for(symbol)?.cancel_all(symbol).await
    }

    async fn close_position(&self, symbol: &Symbol, position: &Position) -> Result<String> {
        self.venue_for(symbol)?
            .close_position(symbol, position)
            .await
    }

    async fn get_position(&self, symbol: &Symbol) -> Result<Position> {
        self.venue_for(symbol)?.get_position(symbol).await
    }

    async fn get_balance(&self, currency: &str) -> Result<f64> {
        // Balance is an account property of each venue; aggregate across them.
        // A venue that doesn't recognise `currency` returns 0; an erroring venue
        // contributes 0 and is logged rather than failing the whole call.
        let mut total = 0.0;
        for venue in &self.venues {
            match venue.get_balance(currency).await {
                Ok(b) => total += b,
                Err(e) => warn!(
                    venue = venue.name(),
                    currency,
                    error = %e,
                    "RoutingExchange get_balance: venue failed; counting 0"
                ),
            }
        }
        Ok(total)
    }

    fn supports(&self, capability: Capability) -> bool {
        // No symbol here, so the only sound global answer is the intersection:
        // advertise a capability only if every venue supports it.
        !self.venues.is_empty() && self.venues.iter().all(|v| v.supports(capability))
    }

    fn contract_value(&self, symbol: &Symbol) -> f64 {
        match self.routes.get(symbol) {
            Some(v) => v.contract_value(symbol),
            None => {
                warn!(%symbol, "RoutingExchange contract_value: unmapped symbol; defaulting to 1.0");
                1.0
            }
        }
    }

    fn instrument_spec(&self, symbol: &Symbol) -> InstrumentSpec {
        // The crux: each symbol's spec (and thus its AssetClass) comes from its
        // own venue, so per-asset-class risk resolves correctly across venues.
        match self.routes.get(symbol) {
            Some(v) => v.instrument_spec(symbol),
            None => {
                warn!(%symbol, "RoutingExchange instrument_spec: unmapped symbol; defaulting");
                InstrumentSpec::from_contract_value(1.0)
            }
        }
    }

    async fn get_open_orders(&self, symbol: &Symbol) -> Result<Vec<OpenOrder>> {
        self.venue_for(symbol)?.get_open_orders(symbol).await
    }

    async fn cancel_order(&self, symbol: &Symbol, order_id: &str) -> Result<bool> {
        self.venue_for(symbol)?.cancel_order(symbol, order_id).await
    }
}

/// Builder for [`RoutingExchange`]. One [`route`](Self::route) call registers
/// one venue for a set of symbols.
#[derive(Default)]
pub struct RoutingExchangeBuilder {
    routes: HashMap<Symbol, Arc<dyn ExchangeClient>>,
    venues: Vec<Arc<dyn ExchangeClient>>,
    /// First symbol routed to two venues (a build error), if any.
    duplicate: Option<Symbol>,
}

impl RoutingExchangeBuilder {
    /// Route every symbol in `symbols` to `venue`. Repeated calls add more
    /// venues; routing the **same symbol** to two venues is rejected at
    /// [`build`](Self::build).
    #[must_use]
    pub fn route<I, S>(mut self, symbols: I, venue: Arc<dyn ExchangeClient>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Symbol>,
    {
        self.venues.push(Arc::clone(&venue));
        for s in symbols {
            let sym = s.into();
            if self
                .routes
                .insert(sym.clone(), Arc::clone(&venue))
                .is_some()
            {
                self.duplicate.get_or_insert(sym);
            }
        }
        self
    }

    /// Validate and build.
    ///
    /// # Errors
    /// Fails if no symbols were routed, or if a symbol was routed to more than
    /// one venue (an ambiguous route).
    pub fn build(self) -> Result<RoutingExchange> {
        if let Some(dup) = self.duplicate {
            return Err(Error::exchange(format!(
                "RoutingExchange: symbol {dup} routed to more than one venue"
            )));
        }
        if self.routes.is_empty() {
            return Err(Error::exchange(
                "RoutingExchange: no symbols routed — add at least one route()",
            ));
        }
        let name = format!(
            "routing({})",
            self.venues
                .iter()
                .map(|v| v.name().to_string())
                .collect::<Vec<_>>()
                .join("+")
        );
        Ok(RoutingExchange {
            routes: self.routes,
            venues: self.venues,
            name,
        })
    }
}

// ── CompositeFillSource ───────────────────────────────────────────────────────

/// A [`rustrade::FillSource`] that merges several sources into one stream — the
/// fill-side companion to [`RoutingExchange`].
///
/// At construction it spawns one forwarder task per inner source that pumps that
/// source's [`next_fill`](FillSource::next_fill) into a shared channel; this
/// type's `next_fill` then yields from the merged channel. When every inner
/// source ends, the channel closes and `next_fill` returns `None`.
pub struct CompositeFillSource {
    rx: AsyncMutex<mpsc::UnboundedReceiver<Fill>>,
}

impl CompositeFillSource {
    /// Merge `sources`. Spawns the forwarders on the current Tokio runtime.
    #[must_use]
    pub fn new(sources: Vec<Arc<dyn FillSource>>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<Fill>();
        for source in sources {
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Some(fill) = source.next_fill().await {
                    if tx.send(fill).is_err() {
                        break; // merged receiver gone
                    }
                }
            });
        }
        // Drop our own sender: once every forwarder ends (all sources drained),
        // the last clone drops, the channel closes, and `next_fill` → None.
        drop(tx);
        Self {
            rx: AsyncMutex::new(rx),
        }
    }
}

#[async_trait]
impl FillSource for CompositeFillSource {
    async fn next_fill(&self) -> Option<Fill> {
        self.rx.lock().await.recv().await
    }
}

// ── RoutingCandleSource ───────────────────────────────────────────────────────

/// A [`rustrade::CandleSource`] that dispatches each `poll` to a per-symbol
/// source — the market-data companion to [`RoutingExchange`], so a multi-venue
/// bot pulls each symbol's candles from its **own** venue. An optional default
/// source handles any symbol without an explicit route.
pub struct RoutingCandleSource {
    routes: HashMap<Symbol, Arc<dyn CandleSource>>,
    default: Option<Arc<dyn CandleSource>>,
    name: String,
}

impl RoutingCandleSource {
    /// Start building a routing candle source.
    #[must_use]
    pub fn builder() -> RoutingCandleSourceBuilder {
        RoutingCandleSourceBuilder::default()
    }
}

#[async_trait]
impl CandleSource for RoutingCandleSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn poll(&self, symbol: &Symbol, interval: Duration, limit: usize) -> Result<Vec<Candle>> {
        match self.routes.get(symbol).or(self.default.as_ref()) {
            Some(src) => src.poll(symbol, interval, limit).await,
            None => Err(Error::exchange(format!(
                "RoutingCandleSource: no source for symbol {symbol}"
            ))),
        }
    }
}

/// Builder for [`RoutingCandleSource`].
#[derive(Default)]
pub struct RoutingCandleSourceBuilder {
    routes: HashMap<Symbol, Arc<dyn CandleSource>>,
    default: Option<Arc<dyn CandleSource>>,
    names: Vec<String>,
}

impl RoutingCandleSourceBuilder {
    /// Route every symbol in `symbols` to `source`.
    #[must_use]
    pub fn route<I, S>(mut self, symbols: I, source: Arc<dyn CandleSource>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Symbol>,
    {
        self.names.push(source.name().to_string());
        for s in symbols {
            self.routes.insert(s.into(), Arc::clone(&source));
        }
        self
    }

    /// Set the fallback source for symbols without an explicit route.
    #[must_use]
    pub fn default_source(mut self, source: Arc<dyn CandleSource>) -> Self {
        self.default = Some(source);
        self
    }

    /// Build the routing source.
    #[must_use]
    pub fn build(self) -> RoutingCandleSource {
        let name = format!("routing-candles({})", self.names.join("+"));
        RoutingCandleSource {
            routes: self.routes,
            default: self.default,
            name,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rustrade::{AssetClass, Price, Side, Volume};

    /// A no-network test venue with a fixed asset class / balance / capability
    /// and a counter of `place_order` calls.
    struct MockVenue {
        name: String,
        class: AssetClass,
        balance: f64,
        stops: bool,
        orders: AtomicUsize,
    }

    impl MockVenue {
        fn new(name: &str, class: AssetClass) -> Self {
            Self {
                name: name.into(),
                class,
                balance: 0.0,
                stops: false,
                orders: AtomicUsize::new(0),
            }
        }
        fn with_balance(mut self, b: f64) -> Self {
            self.balance = b;
            self
        }
        fn with_stops(mut self, s: bool) -> Self {
            self.stops = s;
            self
        }
    }

    #[async_trait]
    impl ExchangeClient for MockVenue {
        fn name(&self) -> &str {
            &self.name
        }
        async fn place_order(&self, _o: &Order) -> Result<String> {
            self.orders.fetch_add(1, Ordering::SeqCst);
            Ok(format!("{}-order", self.name))
        }
        async fn cancel_all(&self, _s: &Symbol) -> Result<usize> {
            Ok(0)
        }
        async fn close_position(&self, _s: &Symbol, _p: &Position) -> Result<String> {
            Ok(format!("{}-close", self.name))
        }
        async fn get_position(&self, _s: &Symbol) -> Result<Position> {
            Ok(Position::FLAT)
        }
        async fn get_balance(&self, _c: &str) -> Result<f64> {
            Ok(self.balance)
        }
        fn supports(&self, c: Capability) -> bool {
            matches!(c, Capability::OrderTracking)
                || (self.stops && matches!(c, Capability::StopOrders))
        }
        fn instrument_spec(&self, _s: &Symbol) -> InstrumentSpec {
            InstrumentSpec {
                asset_class: self.class,
                contract_value: 1.0,
                tick_size: 0.0,
                lot_size: 0.0,
                min_notional: 0.0,
            }
        }
    }

    fn router() -> (RoutingExchange, Arc<MockVenue>, Arc<MockVenue>) {
        let perp = Arc::new(
            MockVenue::new("kucoin", AssetClass::CryptoPerp)
                .with_balance(100.0)
                .with_stops(true),
        );
        let spot = Arc::new(MockVenue::new("kraken", AssetClass::CryptoSpot).with_balance(40.0));
        let ex = RoutingExchange::builder()
            .route(
                ["XBTUSDTM", "ETHUSDTM"],
                perp.clone() as Arc<dyn ExchangeClient>,
            )
            .route(["XBTUSD"], spot.clone() as Arc<dyn ExchangeClient>)
            .build()
            .expect("valid routes");
        (ex, perp, spot)
    }

    #[test]
    fn instrument_spec_resolves_per_venue_class() {
        let (ex, ..) = router();
        // This is what makes class_risk diverge: each symbol's class comes from
        // its own venue.
        assert_eq!(
            ex.instrument_spec(&Symbol::from("XBTUSDTM")).asset_class,
            AssetClass::CryptoPerp
        );
        assert_eq!(
            ex.instrument_spec(&Symbol::from("XBTUSD")).asset_class,
            AssetClass::CryptoSpot
        );
        // Unmapped symbol → benign default (perp), not a panic.
        assert_eq!(
            ex.instrument_spec(&Symbol::from("NOPE")).asset_class,
            AssetClass::CryptoPerp
        );
        assert_eq!(ex.name(), "routing(kucoin+kraken)");
    }

    #[test]
    fn supports_is_the_intersection() {
        let (ex, ..) = router();
        // Both venues advertise OrderTracking → intersection yes.
        assert!(ex.supports(Capability::OrderTracking));
        // Only the perp venue has stops → intersection no.
        assert!(!ex.supports(Capability::StopOrders));
    }

    #[tokio::test]
    async fn get_balance_sums_across_venues() {
        let (ex, ..) = router();
        assert!((ex.get_balance("USD").await.unwrap() - 140.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn place_order_routes_to_the_symbols_venue() {
        let (ex, perp, spot) = router();
        ex.place_order(&Order::market("XBTUSDTM", Side::Buy, Volume(1.0)))
            .await
            .unwrap();
        ex.place_order(&Order::market("XBTUSD", Side::Buy, Volume(1.0)))
            .await
            .unwrap();
        ex.place_order(&Order::market("ETHUSDTM", Side::Sell, Volume(2.0)))
            .await
            .unwrap();
        assert_eq!(perp.orders.load(Ordering::SeqCst), 2); // both perp symbols
        assert_eq!(spot.orders.load(Ordering::SeqCst), 1); // the one spot symbol
    }

    #[tokio::test]
    async fn unmapped_symbol_order_errors() {
        let (ex, ..) = router();
        assert!(
            ex.place_order(&Order::market("DOGEUSD", Side::Buy, Volume(1.0)))
                .await
                .is_err()
        );
    }

    #[test]
    fn build_rejects_empty_and_duplicate_routes() {
        // No routes.
        assert!(RoutingExchange::builder().build().is_err());
        // Same symbol on two venues.
        let a = Arc::new(MockVenue::new("a", AssetClass::CryptoPerp)) as Arc<dyn ExchangeClient>;
        let b = Arc::new(MockVenue::new("b", AssetClass::CryptoSpot)) as Arc<dyn ExchangeClient>;
        assert!(
            RoutingExchange::builder()
                .route(["XBTUSD"], a)
                .route(["XBTUSD"], b)
                .build()
                .is_err()
        );
    }

    /// A `FillSource` that yields a fixed list of fills once, then `None`.
    struct VecFills {
        rx: AsyncMutex<mpsc::UnboundedReceiver<Fill>>,
    }
    impl VecFills {
        fn arc(fills: Vec<Fill>) -> Arc<dyn FillSource> {
            let (tx, rx) = mpsc::unbounded_channel();
            for f in fills {
                let _ = tx.send(f);
            }
            // tx drops here → channel closes after the queued fills drain.
            Arc::new(Self {
                rx: AsyncMutex::new(rx),
            })
        }
    }
    #[async_trait]
    impl FillSource for VecFills {
        async fn next_fill(&self) -> Option<Fill> {
            self.rx.lock().await.recv().await
        }
    }

    fn fill(sym: &str) -> Fill {
        Fill {
            symbol: Symbol::from(sym),
            order_id: format!("{sym}-1"),
            client_id: None,
            side: Side::Buy,
            price: Price(1.0),
            size: Volume(1.0),
            fee: 0.0,
            fee_currency: "USD".into(),
            timestamp: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn composite_merges_all_sources() {
        let src_a = VecFills::arc(vec![fill("XBTUSDTM"), fill("ETHUSDTM")]);
        let src_b = VecFills::arc(vec![fill("XBTUSD")]);
        let merged = CompositeFillSource::new(vec![src_a, src_b]);

        let mut syms = Vec::new();
        while let Some(f) = merged.next_fill().await {
            syms.push(f.symbol.as_str().to_string());
        }
        syms.sort();
        assert_eq!(syms, vec!["ETHUSDTM", "XBTUSD", "XBTUSDTM"]);
    }

    #[tokio::test]
    async fn composite_with_no_sources_ends_immediately() {
        let merged = CompositeFillSource::new(vec![]);
        assert!(merged.next_fill().await.is_none());
    }

    /// A `CandleSource` that tags each candle's `volume` with a fixed id, so a
    /// test can tell which source served a poll.
    struct TaggedCandles {
        name: String,
        tag: f64,
    }
    #[async_trait]
    impl CandleSource for TaggedCandles {
        fn name(&self) -> &str {
            &self.name
        }
        async fn poll(
            &self,
            _symbol: &Symbol,
            _interval: Duration,
            _limit: usize,
        ) -> Result<Vec<Candle>> {
            Ok(vec![Candle {
                time: 0,
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
                volume: self.tag,
            }])
        }
    }

    fn tagged(name: &str, tag: f64) -> Arc<dyn CandleSource> {
        Arc::new(TaggedCandles {
            name: name.into(),
            tag,
        })
    }

    #[tokio::test]
    async fn candle_routing_dispatches_per_symbol_with_default() {
        let src = RoutingCandleSource::builder()
            .route(["XBTUSDTM"], tagged("kucoin", 1.0))
            .route(["XBTUSD"], tagged("kraken", 2.0))
            .default_source(tagged("synthetic", 9.0))
            .build();
        let i = Duration::from_secs(60);
        let vol = |c: Vec<Candle>| c[0].volume;
        // Each symbol's poll is served by its routed source; unmapped → default.
        assert!((vol(src.poll(&Symbol::from("XBTUSDTM"), i, 1).await.unwrap()) - 1.0).abs() < 1e-9);
        assert!((vol(src.poll(&Symbol::from("XBTUSD"), i, 1).await.unwrap()) - 2.0).abs() < 1e-9);
        assert!((vol(src.poll(&Symbol::from("DOGEUSD"), i, 1).await.unwrap()) - 9.0).abs() < 1e-9);
        assert_eq!(src.name(), "routing-candles(kucoin+kraken)");
    }

    #[tokio::test]
    async fn candle_routing_errors_on_unmapped_without_default() {
        let src = RoutingCandleSource::builder()
            .route(["XBTUSD"], tagged("kraken", 2.0))
            .build();
        assert!(
            src.poll(&Symbol::from("NOPE"), Duration::from_secs(60), 1)
                .await
                .is_err()
        );
    }
}
