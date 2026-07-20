//! [`rustrade`] [`ExchangeClient`] adapters backed by `exchange-apiws`'s signed
//! REST surfaces:
//!
//! - [`KucoinExchangeAdapter`] — **KuCoin Futures** (contracts, leverage,
//!   brackets), plus [`KucoinFillSource`] for real fills → `AssetClass::CryptoPerp`.
//! - [`KrakenSpotAdapter`] — **Kraken spot** (long-only, base-asset units,
//!   `position` = balance), plus [`KrakenFillSource`] for real fills →
//!   `AssetClass::CryptoSpot`.
//! - [`RoutingExchange`] — composes the above into **one** `ExchangeClient` that
//!   dispatches per symbol, so a single bot can trade both venues at once and
//!   per-asset-class risk (`class_risk`) actually diverges; [`CompositeFillSource`]
//!   merges their fills.
//! - [`KrakenCandleSource`] / [`RoutingCandleSource`] — the market-data side:
//!   Kraken public OHLC candles, plus a per-symbol candle router so each venue's
//!   symbols are fed their own candles.
//!
//! The `rustrade` framework stays exchange-agnostic: it speaks in `Order`,
//! `Position`, `Capability`. `exchange-apiws` speaks each venue's HTTP API.
//! These adapters are the bridge — the thing that turns a framework `Order`
//! into a real order on a real exchange (point them at sandbox/test
//! credentials to paper-trade the exact same path).
//!
//! It is **Track 1** of `docs/MULTI_ASSET_BRAIN_ROADMAP.md`: until now every
//! bot under `bots/` traded against `MockExchange`, so nothing actually
//! executed through the framework.
//!
//! # What it maps
//!
//! | framework call | KuCoin (via exchange-apiws) |
//! |---|---|
//! | `place_order(Order)` plain | `place_order` (market / limit / IOC / FOK) |
//! | `place_order(Order)` with `stop` + `reduce_only` | `place_stop_order` (a bracket leg) |
//! | `place_order(Order)` with `stop`, not reduce-only | `place_order` (entry) **+** a reduce-only `place_stop_order` (protection) |
//! | `close_position` | `close_position` (market, signed qty) |
//! | `get_position` / `get_balance` | `get_position` / `get_balance` |
//! | `cancel_all` | `cancel_all_orders` + `cancel_all_stop_orders` |
//! | `get_open_orders` / `cancel_order` | `get_open_orders` / `cancel_order` |
//! | `contract_value` | cached `get_contract().multiplier` |
//!
//! # Capabilities (advertised truthfully)
//!
//! `StopOrders`, `ReduceOnly`, `Ioc`, `Fok`, `OrderTracking` → **yes**.
//! `PostOnly` → **no** (the exchange-apiws `place_order` exposes no post-only
//! flag). `PublicFeed` / `PrivateFeed` → **no** on the *adapter object* itself
//! (it's trading-only); real fills are delivered by the companion
//! [`KucoinFillSource`], and market data by a bot's own candle source.
//!
//! # Real fills
//!
//! [`KucoinFillSource`] is a [`rustrade::FillSource`] that streams the
//! exchange's actual executions (price / size / fee) into the bot, replacing
//! paper-simulated fills — and, because the framework gates bracket/OCO
//! handling on a fill source being present, it's what turns on real SL/TP
//! management. See its docs for the WS-trigger + `/recentFills` design.
//!
//! # Leverage & sizing
//!
//! KuCoin takes leverage *per order*, so it's a field on the adapter
//! ([`KucoinExchangeAdapter::leverage`]). Order sizes are **contracts** (whole
//! `u32`): the risk layer's `PositionSizer` produces a contract count using
//! [`ExchangeClient::contract_value`], which this adapter resolves from each
//! symbol's KuCoin contract `multiplier` (e.g. `XBTUSDTM` = 0.001 BTC). Pass
//! the symbols you trade to [`KucoinExchangeAdapter::connect`] so those
//! multipliers are fetched once up front.
//!
//! # Example
//!
//! ```no_run
//! use rustrade_exchange_apiws::KucoinExchangeAdapter;
//! use exchange_apiws::KucoinEnv;
//!
//! # async fn demo() -> rustrade::Result<()> {
//! // Credentials from KC_KEY / KC_SECRET / KC_PASSPHRASE, 5x leverage,
//! // pre-fetching contract multipliers for the symbols we'll trade.
//! let exchange = KucoinExchangeAdapter::from_env(5, &["XBTUSDTM", "ETHUSDTM"]).await?;
//!
//! // `exchange` is now an `Arc`-able `dyn ExchangeClient` for `Bot::new(...)`.
//! # let _ = (exchange, KucoinEnv::LiveFutures);
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustrade::{
    AssetClass, Capability, Error, ExchangeClient, InstrumentSpec, OpenOrder, Order, OrderKind,
    OrderStatus, Position, Price, Result, Side, StopAttachment, StopKind, Symbol, Volume,
};
use uuid::Uuid;

use exchange_apiws::rest::orders::{CancelledOrders, OrderDetail, StopOrderDetail, SubmittedOrder};
use exchange_apiws::{
    Credentials, ErrorClass, KuCoinClient, KucoinEnv, OrderType as EaOrderType, Side as EaSide,
    TimeInForce as EaTif,
};

mod fills;
pub use fills::KucoinFillSource;

mod recovery;
pub use recovery::RecoveryPolicy;

mod kraken;
pub use kraken::{KrakenFillSource, KrakenSpotAdapter};

mod routing;
pub use routing::{
    CompositeFillSource, RoutingCandleSource, RoutingCandleSourceBuilder, RoutingExchange,
    RoutingExchangeBuilder,
};

mod candles;
pub use candles::KrakenCandleSource;

// ── Error glue ───────────────────────────────────────────────────────────────

/// Map any `exchange-apiws` error into a framework [`Error::Exchange`].
pub(crate) fn ex<E: std::fmt::Display>(e: E) -> Error {
    Error::exchange(e.to_string())
}

// ── Pure mapping helpers (unit-tested, no network) ───────────────────────────

/// framework [`Side`] → exchange-apiws side.
fn ea_side(side: Side) -> EaSide {
    match side {
        Side::Buy => EaSide::Buy,
        Side::Sell => EaSide::Sell,
    }
}

/// framework [`OrderKind`] → (KuCoin order type, optional time-in-force).
///
/// `Market`/`Limit` use KuCoin's default GTC; `Ioc`/`Fok` are limit orders
/// with the matching TIF (and therefore require a limit price). `PostOnly`
/// has no representation on the exchange-apiws `place_order` surface, so it is
/// rejected — consistent with advertising `Capability::PostOnly = false`.
fn order_kind_to_type(kind: OrderKind) -> Result<(EaOrderType, Option<EaTif>)> {
    Ok(match kind {
        OrderKind::Market => (EaOrderType::Market, None),
        OrderKind::Limit => (EaOrderType::Limit, None),
        OrderKind::Ioc => (EaOrderType::Limit, Some(EaTif::IOC)),
        OrderKind::Fok => (EaOrderType::Limit, Some(EaTif::FOK)),
        OrderKind::PostOnly => {
            return Err(Error::exchange(
                "OrderKind::PostOnly is unsupported: the exchange-apiws KuCoin \
                 place_order surface exposes no post-only flag (Capability::PostOnly = false)",
            ));
        }
    })
}

/// KuCoin stop *trigger direction* (`"up"` / `"down"`) for a protective order
/// whose `closing_side` would flatten the position.
///
/// `"down"` fires when the price falls to the trigger, `"up"` when it rises.
/// Derived purely from the closing side and stop kind — no mark price needed:
///
/// - **stop-loss** (`StopMarket`/`StopLimit`): a sell-to-close (long stop)
///   sits *below* the market → `"down"`; a buy-to-close (short stop) sits
///   *above* → `"up"`.
/// - **take-profit**: a sell-to-close (long TP) sits *above* → `"up"`; a
///   buy-to-close (short TP) sits *below* → `"down"`.
fn stop_trigger_direction(closing_side: Side, kind: StopKind) -> Result<&'static str> {
    Ok(match (closing_side, kind) {
        (Side::Sell, StopKind::StopMarket | StopKind::StopLimit { .. }) => "down",
        (Side::Buy, StopKind::StopMarket | StopKind::StopLimit { .. }) => "up",
        (Side::Sell, StopKind::TakeProfit) => "up",
        (Side::Buy, StopKind::TakeProfit) => "down",
        (_, StopKind::TrailingStop { .. }) => {
            return Err(Error::exchange(
                "StopKind::TrailingStop is unsupported by the exchange-apiws KuCoin surface",
            ));
        }
    })
}

/// The triggered order's limit price, if the stop kind carries one
/// (`None` ⇒ the trigger fires a market order).
fn stop_limit_price(kind: StopKind) -> Result<Option<f64>> {
    Ok(match kind {
        StopKind::StopLimit { limit_price } => Some(limit_price.value()),
        StopKind::StopMarket | StopKind::TakeProfit => None,
        StopKind::TrailingStop { .. } => {
            return Err(Error::exchange(
                "StopKind::TrailingStop is unsupported by the exchange-apiws KuCoin surface",
            ));
        }
    })
}

/// framework [`Volume`] → whole KuCoin contracts. Rounds to nearest and
/// rejects anything that lands below one contract (KuCoin's minimum).
fn to_contracts(size: Volume) -> Result<u32> {
    let rounded = size.value().round();
    if !rounded.is_finite() || rounded < 1.0 {
        return Err(Error::exchange(format!(
            "order size {} rounds to {rounded} contracts; KuCoin requires at least 1 whole contract",
            size.value()
        )));
    }
    Ok(rounded as u32)
}

/// Count the orders a KuCoin cancel endpoint actually cancelled (`0` when
/// nothing matched).
fn count_cancelled(resp: &CancelledOrders) -> usize {
    resp.cancelled_order_ids.len()
}

/// Milliseconds since the epoch → `DateTime<Utc>` (None on overflow).
pub(crate) fn ms_to_dt(ms: i64) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(ms)
}

// ── Adapter ──────────────────────────────────────────────────────────────────

/// A [`rustrade::ExchangeClient`] that executes through KuCoin Futures.
///
/// Cheaply cloneable wrapper around an `exchange-apiws` [`KuCoinClient`] plus
/// the per-order leverage and a cache of per-symbol contract multipliers. See
/// the [crate] docs for the full mapping and capability table.
#[derive(Clone)]
pub struct KucoinExchangeAdapter {
    client: KuCoinClient,
    leverage: u32,
    /// symbol → base-asset units per contract (`get_contract().multiplier`).
    contract_values: HashMap<String, f64>,
    /// Bounded retry/reconcile policy for ambiguous-fill recovery (A.7).
    recovery: RecoveryPolicy,
}

impl KucoinExchangeAdapter {
    /// Wrap an existing [`KuCoinClient`]. No contract multipliers are known yet
    /// — [`contract_value`](ExchangeClient::contract_value) returns the spot
    /// default of `1.0` until you register them via
    /// [`with_contract_value`](Self::with_contract_value) or
    /// [`fetch_contract_values`](Self::fetch_contract_values). Leverage is
    /// clamped to a minimum of `1`.
    #[must_use]
    pub fn new(client: KuCoinClient, leverage: u32) -> Self {
        Self {
            client,
            leverage: leverage.max(1),
            contract_values: HashMap::new(),
            recovery: RecoveryPolicy::default(),
        }
    }

    /// Override the bounded ambiguous-fill [`RecoveryPolicy`] (builder style).
    /// The default is a few seconds of tightly-bounded recovery; tests use a
    /// near-zero backoff to stay fast.
    #[must_use]
    pub fn with_recovery_policy(mut self, policy: RecoveryPolicy) -> Self {
        self.recovery = policy;
        self
    }

    /// Register a known contract multiplier for `symbol` (builder style).
    /// Useful in tests and when you'd rather hard-code multipliers than fetch
    /// them.
    #[must_use]
    pub fn with_contract_value(mut self, symbol: impl Into<String>, multiplier: f64) -> Self {
        self.contract_values.insert(symbol.into(), multiplier);
        self
    }

    /// Fetch and cache the contract multiplier for each of `symbols` from
    /// KuCoin's `/contracts/{symbol}` endpoint.
    ///
    /// # Errors
    /// Surfaces any REST error, or rejects a symbol whose contract reports no
    /// multiplier (sizing would otherwise be silently wrong).
    pub async fn fetch_contract_values<I, S>(&mut self, symbols: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for s in symbols {
            let sym = s.as_ref();
            let contract = self.client.get_contract(sym).await.map_err(ex)?;
            let multiplier = contract.multiplier.ok_or_else(|| {
                Error::exchange(format!(
                    "contract {sym} reports no multiplier — cannot size positions for it"
                ))
            })?;
            self.contract_values.insert(sym.to_string(), multiplier);
        }
        Ok(())
    }

    /// Build from explicit credentials + environment, pre-fetching the
    /// contract multipliers for `symbols`.
    ///
    /// # Errors
    /// Fails if the HTTP client can't be built or any contract fetch fails.
    pub async fn connect(
        creds: Credentials,
        env: KucoinEnv,
        leverage: u32,
        symbols: &[&str],
    ) -> Result<Self> {
        let client = KuCoinClient::new(creds, env).map_err(ex)?;
        let mut adapter = Self::new(client, leverage);
        adapter
            .fetch_contract_values(symbols.iter().copied())
            .await?;
        Ok(adapter)
    }

    /// Build from `KC_KEY` / `KC_SECRET` / `KC_PASSPHRASE` against KuCoin
    /// **live futures**, pre-fetching multipliers for `symbols`.
    ///
    /// # Errors
    /// Fails if a credential env var is missing or a contract fetch fails.
    pub async fn from_env(leverage: u32, symbols: &[&str]) -> Result<Self> {
        let creds = Credentials::from_env().map_err(ex)?;
        Self::connect(creds, KucoinEnv::LiveFutures, leverage, symbols).await
    }

    /// The per-order leverage this adapter sends to KuCoin.
    #[must_use]
    pub fn leverage(&self) -> u32 {
        self.leverage
    }

    /// Borrow the underlying signed client (for surface this adapter doesn't
    /// expose — funding history, margin mode, transfers, …).
    #[must_use]
    pub fn client(&self) -> &KuCoinClient {
        &self.client
    }

    /// Place a KuCoin stop/trigger order. `side` is the order's own side (the
    /// side that would flatten the position it protects); the trigger
    /// direction and triggered order type are derived from `stop`.
    async fn place_trigger(
        &self,
        symbol: &str,
        side: Side,
        size: u32,
        stop: StopAttachment,
        reduce_only: bool,
    ) -> Result<String> {
        let direction = stop_trigger_direction(side, stop.kind)?;
        let limit = stop_limit_price(stop.kind)?;
        let ea = ea_side(side);
        let trigger = stop.trigger_price.value();
        let leverage = self.leverage;
        let client = &self.client;
        // An untriggered stop lives in KuCoin's SEPARATE stopOrders bucket, which
        // `byClientOid` on the regular-orders endpoint cannot see. Carry the
        // stop's own attributes so an ambiguous submit is reconciled against
        // that bucket (not misread as never-reached → double protective order).
        let resolve = ResolveTarget::Stop(StopResolve {
            symbol: symbol.to_string(),
            side: match side {
                Side::Buy => "buy",
                Side::Sell => "sell",
            },
            size,
            direction,
            stop_price: trigger,
            reduce_only,
        });
        self.submit_with_recovery("stop", symbol, resolve, |oid| async move {
            client
                .place_stop_order_with_client_oid(
                    &oid,
                    symbol,
                    ea,
                    size,
                    leverage,
                    trigger,
                    direction,
                    limit,
                    reduce_only,
                )
                .await
        })
        .await
    }

    /// Place a plain (non-trigger) order and return its id.
    async fn place_plain(&self, order: &Order, size: u32) -> Result<String> {
        let (order_type, tif) = order_kind_to_type(order.kind)?;
        // Only send a price on limit-style orders; market orders omit it.
        let price = if order_type == EaOrderType::Limit {
            order.limit_price.map(Price::value)
        } else {
            None
        };
        let symbol = order.symbol.as_str();
        let side = ea_side(order.side);
        let reduce_only = order.reduce_only;
        let leverage = self.leverage;
        let client = &self.client;
        self.submit_with_recovery("entry", symbol, ResolveTarget::Regular, |oid| async move {
            client
                .place_order_with_client_oid(
                    &oid,
                    symbol,
                    side,
                    size,
                    leverage,
                    order_type,
                    price,
                    tif,
                    reduce_only,
                    None,
                )
                .await
        })
        .await
    }

    /// Submit an order with bounded **ambiguous-fill recovery** (A.7).
    ///
    /// One `clientOid` is minted per logical submit and **reused** across every
    /// retry and re-query (the idempotency key), so no path double-submits. The
    /// `submit` closure places the order given that `clientOid`; on error the
    /// outcome is triaged with [`ExchangeError::classify_submit`]:
    ///
    /// - [`ErrorClass::Fatal`] → surface immediately (no retry).
    /// - [`ErrorClass::Retriable`] → bounded backoff retry (same `clientOid`).
    /// - [`ErrorClass::Ambiguous`] → resolve the *true* state via
    ///   [`get_order_by_client_oid`](KuCoinClient::get_order_by_client_oid) and
    ///   reconcile (adopt a landed order / re-place a never-reached one / surface
    ///   a venue-side failure / retry on a transient resolve error).
    ///
    /// Never assumes filled-or-unfilled; never loops unbounded. On success it
    /// returns the venue order id (the *real* one when an ambiguous submit is
    /// discovered to have landed) — which the caller feeds into the bot's
    /// existing venue-truth fill-confirmation path exactly as a clean placement.
    async fn submit_with_recovery<F, Fut>(
        &self,
        what: &'static str,
        symbol: &str,
        resolve: ResolveTarget,
        submit: F,
    ) -> Result<String>
    where
        F: Fn(String) -> Fut,
        Fut: std::future::Future<Output = exchange_apiws::Result<SubmittedOrder>>,
    {
        let client_oid = Uuid::new_v4().to_string();
        let max = self.recovery.max_attempts.max(1);
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let last = attempt >= max;
            match submit(client_oid.clone()).await {
                Ok(s) => {
                    if attempt > 1 {
                        tracing::warn!(
                            target: "order_recovery",
                            what, symbol, client_oid, order_id = %s.order_id, attempt,
                            "order-submit recovered: placed on retry (clientOid reused)"
                        );
                    }
                    return Ok(s.order_id);
                }
                Err(e) => match e.classify_submit(&client_oid) {
                    ErrorClass::Fatal => {
                        tracing::error!(
                            target: "order_recovery",
                            what, symbol, client_oid, attempt, error = %e,
                            "order-submit FATAL — surfacing, no retry"
                        );
                        return Err(ex(e));
                    }
                    ErrorClass::Retriable => {
                        if last {
                            tracing::error!(
                                target: "order_recovery",
                                what, symbol, client_oid, attempt, error = %e,
                                "order-submit retriable but attempts exhausted — surfacing"
                            );
                            return Err(ex(e));
                        }
                        let wait = self.recovery.backoff_after(attempt);
                        tracing::warn!(
                            target: "order_recovery",
                            what, symbol, client_oid, attempt, error = %e,
                            wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                            "order-submit retriable — backing off, reusing clientOid"
                        );
                        tokio::time::sleep(wait).await;
                    }
                    ErrorClass::Ambiguous { .. } => {
                        tracing::warn!(
                            target: "order_recovery",
                            what, symbol, client_oid, attempt, error = %e,
                            "order-submit AMBIGUOUS — resolving true state via byClientOid"
                        );
                        match self.resolve(&resolve, &client_oid).await {
                            Resolved::Landed(order_id) => {
                                tracing::warn!(
                                    target: "order_recovery",
                                    what, symbol, client_oid, order_id = %order_id, attempt,
                                    "ambiguous submit RESOLVED: order landed at venue — adopting real order id"
                                );
                                return Ok(order_id);
                            }
                            Resolved::NeverReached => {
                                if last {
                                    return Err(Error::exchange(format!(
                                        "{what} {symbol}: ambiguous submit never reached the venue \
                                         and attempts exhausted (clientOid={client_oid})"
                                    )));
                                }
                                let wait = self.recovery.backoff_after(attempt);
                                tracing::warn!(
                                    target: "order_recovery",
                                    what, symbol, client_oid, attempt,
                                    wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                                    "ambiguous submit RESOLVED: never reached the engine — re-placing SAME clientOid"
                                );
                                tokio::time::sleep(wait).await;
                            }
                            Resolved::Unknown => {
                                if last {
                                    return Err(Error::exchange(format!(
                                        "{what} {symbol}: submit outcome remained UNKNOWN after \
                                         {attempt} attempts — operator must reconcile clientOid={client_oid}"
                                    )));
                                }
                                let wait = self.recovery.backoff_after(attempt);
                                tracing::warn!(
                                    target: "order_recovery",
                                    what, symbol, client_oid, attempt,
                                    wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                                    "ambiguous submit resolve transient/UNKNOWN — retrying (clientOid reused, idempotent)"
                                );
                                tokio::time::sleep(wait).await;
                            }
                            Resolved::Failed(reason) => {
                                tracing::error!(
                                    target: "order_recovery",
                                    what, symbol, client_oid, attempt, reason,
                                    "ambiguous submit RESOLVED: order failed at venue (no position) — surfacing"
                                );
                                return Err(Error::exchange(format!("{what} {symbol}: {reason}")));
                            }
                        }
                    }
                    // `ErrorClass` is `#[non_exhaustive]`: an unknown future
                    // class is surfaced conservatively rather than silently
                    // dropped or retried.
                    _ => return Err(ex(e)),
                },
            }
        }
    }

    /// Re-query an ambiguous submit and classify its true state, routing to the
    /// venue bucket that actually holds the order.
    async fn resolve(&self, target: &ResolveTarget, client_oid: &str) -> Resolved {
        match target {
            ResolveTarget::Regular => self.resolve_client_oid(client_oid).await,
            ResolveTarget::Stop(want) => self.resolve_stop_client_oid(want, client_oid).await,
        }
    }

    /// Re-query an ambiguous **regular** submit's `clientOid` (the
    /// `/api/v1/orders` bucket) and classify its true state.
    async fn resolve_client_oid(&self, client_oid: &str) -> Resolved {
        match self.client.get_order_by_client_oid(client_oid).await {
            Ok(d) => classify_order_detail(client_oid, d),
            // The venue has no record of this clientOid → the order never
            // reached the engine → safe to treat as unsubmitted and re-place.
            Err(e) if recovery::is_order_not_found(&e) => Resolved::NeverReached,
            // Transient query failure → outcome still unknown; retry.
            Err(e) if e.is_retriable() => Resolved::Unknown,
            // Fatal query failure (auth/params) → surface.
            Err(e) => Resolved::Failed(format!("byClientOid resolve failed fatally: {e}")),
        }
    }

    /// Re-query an ambiguous **stop** submit and classify its true state.
    ///
    /// A stop can be in either of two venue buckets, so both are checked:
    /// 1. If it already **triggered**, it became a regular order under the same
    ///    `clientOid` — visible via `byClientOid` on `/api/v1/orders`.
    /// 2. While **untriggered**, KuCoin holds it in a *separate* bucket
    ///    (`/api/v1/stopOrders`) that `byClientOid` cannot see — so a not-found
    ///    there is **expected**, not proof the order never reached the engine.
    ///
    /// Only after the order is absent from **both** buckets do we conclude
    /// `NeverReached` (safe to re-place). Because `StopOrderDetail` carries no
    /// `clientOid`, the stop bucket is matched on the order's own attributes and
    /// a match is adopted **only when it is unique** — an ambiguous (0-vs-many)
    /// bucket is reported `Unknown` rather than guessing a wrong order id.
    async fn resolve_stop_client_oid(&self, want: &StopResolve, client_oid: &str) -> Resolved {
        // (1) Did it trigger into the regular bucket?
        match self.client.get_order_by_client_oid(client_oid).await {
            Ok(d) => return classify_order_detail(client_oid, d),
            // Absent from the regular bucket — expected while untriggered; fall
            // through to the stop bucket rather than concluding NeverReached.
            Err(e) if recovery::is_order_not_found(&e) => {}
            Err(e) if e.is_retriable() => return Resolved::Unknown,
            Err(e) => {
                return Resolved::Failed(format!("stop byClientOid resolve failed fatally: {e}"));
            }
        }
        // (2) Is it resting untriggered in the separate stop bucket?
        match self.client.get_open_stop_orders(&want.symbol).await {
            Ok(items) => {
                let mut hits = items.into_iter().filter(|it| want.matches(it));
                match (hits.next(), hits.next()) {
                    // Exactly one matching untriggered stop → it landed; adopt it.
                    (Some(hit), None) => Resolved::Landed(hit.id),
                    // Absent from BOTH buckets → the stop never reached the
                    // engine → safe to re-place the same clientOid.
                    (None, _) => Resolved::NeverReached,
                    // Multiple indistinguishable matches → cannot safely pick one
                    // (guessing risks adopting the wrong order id); surface/retry.
                    (Some(_), Some(_)) => Resolved::Unknown,
                }
            }
            Err(e) if e.is_retriable() => Resolved::Unknown,
            Err(e) => Resolved::Failed(format!("stop-bucket resolve failed fatally: {e}")),
        }
    }
}

/// Classify a resolved [`OrderDetail`] (regular bucket) into a [`Resolved`].
///
/// A fully-filled order, one still resting/active, **or one that is terminal but
/// partially filled** all mean the submit LANDED with real (possibly leveraged)
/// exposure — adopt the real venue id and let the caller's fill-confirmation
/// path reconcile the actual fill from venue truth. Only a terminal order with
/// **zero** fill is a true no-position outcome (the `clientOid` is consumed, so a
/// re-place is futile) and surfaces for the caller to re-decide.
fn classify_order_detail(client_oid: &str, d: OrderDetail) -> Resolved {
    let filled = d.filled_size.unwrap_or(0);
    if d.is_filled() || d.is_active() {
        Resolved::Landed(d.id)
    } else if filled > 0 {
        // Terminal (done/cancelled) yet PARTIALLY filled: a live position exists
        // even though the order left the book. Adopting it as landed prevents an
        // untracked partial position being reported as "no position established".
        tracing::warn!(
            target: "order_recovery",
            client_oid, order_id = %d.id, filled, size = d.size,
            "ambiguous submit RESOLVED: terminal but PARTIALLY filled — adopting partial position"
        );
        Resolved::Landed(d.id)
    } else {
        Resolved::Failed(format!(
            "clientOid {client_oid} resolved terminal-unfilled at the venue \
             (status={}, cancelExist={:?}) — no position established",
            d.status, d.cancel_exist
        ))
    }
}

/// Which venue bucket an ambiguous submit must be reconciled against.
enum ResolveTarget {
    /// A regular order (market / limit / close) — the `/api/v1/orders` bucket,
    /// reconciled by `byClientOid`.
    Regular,
    /// A stop/trigger order — may be in the regular bucket (if triggered) or the
    /// separate untriggered-stop bucket; matched by [`StopResolve`].
    Stop(StopResolve),
}

/// The identifying attributes of a stop submit, used to find it in the
/// `/api/v1/stopOrders` bucket (which carries no `clientOid` to match on).
struct StopResolve {
    symbol: String,
    /// Order side as KuCoin reports it — `"buy"` / `"sell"`.
    side: &'static str,
    size: u32,
    /// Trigger direction — `"up"` / `"down"`.
    direction: &'static str,
    stop_price: f64,
    reduce_only: bool,
}

impl StopResolve {
    /// Whether an untriggered stop in the venue bucket is *this* submit. Every
    /// discriminating attribute must agree; the trigger price is compared with a
    /// tight relative tolerance to absorb float round-tripping without matching a
    /// neighbouring stop at a different price.
    fn matches(&self, it: &StopOrderDetail) -> bool {
        it.symbol == self.symbol
            && it.side == self.side
            && it.size == self.size
            && it.stop.as_deref() == Some(self.direction)
            && it.reduce_only.unwrap_or(false) == self.reduce_only
            && it
                .stop_price
                .is_some_and(|p| (p - self.stop_price).abs() <= 1e-6 * self.stop_price.abs().max(1.0))
    }
}

/// Outcome of resolving an ambiguous submit's `clientOid` against the venue.
enum Resolved {
    /// The order reached the engine and lives (filled or resting) — adopt this
    /// real venue order id.
    Landed(String),
    /// The order never reached the engine — safe to re-place the same clientOid.
    NeverReached,
    /// The order reached the engine but established no position (cancelled /
    /// expired), or the resolve failed fatally — surface; do not blindly retry.
    Failed(String),
    /// The resolve query failed transiently — the true outcome is still unknown.
    Unknown,
}

#[async_trait]
impl ExchangeClient for KucoinExchangeAdapter {
    fn name(&self) -> &str {
        "kucoin"
    }

    async fn place_order(&self, order: &Order) -> Result<String> {
        let symbol = order.symbol.as_str();
        let size = to_contracts(order.size)?;

        match order.stop {
            // (A) A reduce-only protective leg *is* a trigger order — place the
            // stop only, on the order's own (closing) side. This is how the
            // framework's bracket builder emits SL / TP legs.
            Some(stop) if order.reduce_only => {
                self.place_trigger(symbol, order.side, size, stop, true)
                    .await
            }
            // (B) An entry order that carries attached protection: place the
            // entry, then a separate reduce-only trigger on the opposite side.
            // KuCoin can't attach a stop to an entry atomically, so this is two
            // calls; if the protective leg fails the entry still stands (logged
            // loudly so the position isn't silently unprotected).
            Some(stop) => {
                let entry_id = self.place_plain(order, size).await?;
                if let Err(e) = self
                    .place_trigger(symbol, order.side.opposite(), size, stop, true)
                    .await
                {
                    tracing::error!(
                        symbol,
                        entry_id = %entry_id,
                        error = %e,
                        "entry order placed but its protective stop failed — position is UNPROTECTED"
                    );
                }
                Ok(entry_id)
            }
            // (C) A plain order.
            None => self.place_plain(order, size).await,
        }
    }

    async fn cancel_all(&self, symbol: &Symbol) -> Result<usize> {
        let sym = symbol.as_str();
        // Regular open orders and the separate stop-order bucket.
        let orders = self.client.cancel_all_orders(sym).await.map_err(ex)?;
        let stops = self.client.cancel_all_stop_orders(sym).await.map_err(ex)?;
        Ok(count_cancelled(&orders) + count_cancelled(&stops))
    }

    async fn close_position(&self, symbol: &Symbol, position: &Position) -> Result<String> {
        if position.is_flat() {
            return Err(Error::exchange(format!(
                "close_position: {} is already flat",
                symbol.as_str()
            )));
        }
        // KuCoin wants signed contracts: positive = long (it sells to close).
        let qty = position.qty.round() as i32;
        let sym = symbol.as_str();
        let leverage = self.leverage;
        let client = &self.client;
        self.submit_with_recovery("close", sym, ResolveTarget::Regular, |oid| async move {
            client
                .close_position_with_client_oid(&oid, sym, qty, leverage)
                .await
        })
        .await
    }

    async fn get_position(&self, symbol: &Symbol) -> Result<Position> {
        let info = self
            .client
            .get_position(symbol.as_str())
            .await
            .map_err(ex)?;
        Ok(Position {
            qty: info.current_qty,
            entry_price: info.avg_entry_price,
            unrealised_pnl: info.unrealised_pnl.unwrap_or(0.0),
        })
    }

    async fn get_balance(&self, currency: &str) -> Result<f64> {
        self.client.get_balance(currency).await.map_err(ex)
    }

    fn supports(&self, capability: Capability) -> bool {
        matches!(
            capability,
            Capability::StopOrders
                | Capability::ReduceOnly
                | Capability::Ioc
                | Capability::Fok
                | Capability::OrderTracking
        )
        // PostOnly: no post-only flag on the place_order surface.
        // PublicFeed / PrivateFeed: trading-only adapter; feeds are wired separately.
    }

    fn contract_value(&self, symbol: &Symbol) -> f64 {
        self.contract_values
            .get(symbol.as_str())
            .copied()
            .unwrap_or(1.0)
    }

    fn instrument_spec(&self, symbol: &Symbol) -> InstrumentSpec {
        // KuCoin Futures perpetuals: whole-contract lots, the cached contract
        // multiplier as the contract value, and `CryptoPerp` so per-asset-class
        // risk rules resolve correctly. Tick size / min-notional aren't cached
        // yet, so they stay unconstrained (a future enhancement can populate
        // them from the contract metadata).
        InstrumentSpec {
            asset_class: AssetClass::CryptoPerp,
            contract_value: self.contract_value(symbol),
            tick_size: 0.0,
            lot_size: 1.0,
            min_notional: 0.0,
        }
    }

    async fn get_open_orders(&self, symbol: &Symbol) -> Result<Vec<OpenOrder>> {
        let details = self
            .client
            .get_open_orders(symbol.as_str())
            .await
            .map_err(ex)?;
        let orders = details
            .into_iter()
            .map(|d| {
                let filled = d.filled_size.unwrap_or(0);
                let status = if d.is_active() {
                    if filled > 0 {
                        OrderStatus::PartiallyFilled
                    } else {
                        OrderStatus::Open
                    }
                } else if d.is_filled() {
                    OrderStatus::Filled
                } else {
                    OrderStatus::Cancelled
                };
                OpenOrder {
                    order_id: d.id,
                    client_id: None,
                    symbol: symbol.clone(),
                    side: if d.side == "sell" {
                        Side::Sell
                    } else {
                        Side::Buy
                    },
                    kind: if d.order_type == "limit" {
                        OrderKind::Limit
                    } else {
                        OrderKind::Market
                    },
                    limit_price: d.price.map(Price),
                    size: Volume(f64::from(d.size)),
                    filled: Volume(f64::from(filled)),
                    status,
                    created_at: d.created_at.and_then(ms_to_dt),
                }
            })
            .collect();
        Ok(orders)
    }

    async fn cancel_order(&self, _symbol: &Symbol, order_id: &str) -> Result<bool> {
        // KuCoin returns `{ cancelledOrderIds: [...] }`; non-empty ⇒ cancelled.
        // An already-gone order surfaces as a REST error, which we propagate.
        let resp = self.client.cancel_order(order_id).await.map_err(ex)?;
        Ok(count_cancelled(&resp) > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adapter() -> KucoinExchangeAdapter {
        // No network: `with_base_url`/`new` just builds an HTTP client.
        let client = KuCoinClient::new(Credentials::new("k", "s", "p"), KucoinEnv::LiveFutures)
            .expect("client builds");
        KucoinExchangeAdapter::new(client, 5).with_contract_value("XBTUSDTM", 0.001)
    }

    #[test]
    fn side_maps() {
        assert_eq!(ea_side(Side::Buy), EaSide::Buy);
        assert_eq!(ea_side(Side::Sell), EaSide::Sell);
    }

    #[test]
    fn order_kind_maps() {
        assert_eq!(
            order_kind_to_type(OrderKind::Market).unwrap(),
            (EaOrderType::Market, None)
        );
        assert_eq!(
            order_kind_to_type(OrderKind::Limit).unwrap(),
            (EaOrderType::Limit, None)
        );
        assert_eq!(
            order_kind_to_type(OrderKind::Ioc).unwrap(),
            (EaOrderType::Limit, Some(EaTif::IOC))
        );
        assert_eq!(
            order_kind_to_type(OrderKind::Fok).unwrap(),
            (EaOrderType::Limit, Some(EaTif::FOK))
        );
        assert!(order_kind_to_type(OrderKind::PostOnly).is_err());
    }

    #[test]
    fn stop_direction_is_mark_price_independent() {
        // Closing a long (sell): stop below ⇒ down, TP above ⇒ up.
        assert_eq!(
            stop_trigger_direction(Side::Sell, StopKind::StopMarket).unwrap(),
            "down"
        );
        assert_eq!(
            stop_trigger_direction(Side::Sell, StopKind::TakeProfit).unwrap(),
            "up"
        );
        // Closing a short (buy): stop above ⇒ up, TP below ⇒ down.
        assert_eq!(
            stop_trigger_direction(Side::Buy, StopKind::StopMarket).unwrap(),
            "up"
        );
        assert_eq!(
            stop_trigger_direction(Side::Buy, StopKind::TakeProfit).unwrap(),
            "down"
        );
        // Stop-limit follows the stop-loss direction.
        assert_eq!(
            stop_trigger_direction(
                Side::Sell,
                StopKind::StopLimit {
                    limit_price: Price(1.0)
                }
            )
            .unwrap(),
            "down"
        );
        // Trailing stops are unsupported.
        assert!(
            stop_trigger_direction(
                Side::Sell,
                StopKind::TrailingStop {
                    trail_distance: Price(1.0)
                }
            )
            .is_err()
        );
    }

    #[test]
    fn stop_limit_price_only_for_stop_limit() {
        assert_eq!(stop_limit_price(StopKind::StopMarket).unwrap(), None);
        assert_eq!(stop_limit_price(StopKind::TakeProfit).unwrap(), None);
        assert_eq!(
            stop_limit_price(StopKind::StopLimit {
                limit_price: Price(42.5)
            })
            .unwrap(),
            Some(42.5)
        );
        assert!(
            stop_limit_price(StopKind::TrailingStop {
                trail_distance: Price(1.0)
            })
            .is_err()
        );
    }

    #[test]
    fn contracts_round_and_floor_at_one() {
        assert_eq!(to_contracts(Volume(1.0)).unwrap(), 1);
        assert_eq!(to_contracts(Volume(2.4)).unwrap(), 2);
        assert_eq!(to_contracts(Volume(2.6)).unwrap(), 3);
        assert!(to_contracts(Volume(0.4)).is_err());
        assert!(to_contracts(Volume(0.0)).is_err());
        assert!(to_contracts(Volume(f64::NAN)).is_err());
    }

    #[test]
    fn count_cancelled_parses_response() {
        let de = |v: serde_json::Value| -> CancelledOrders {
            serde_json::from_value(v).expect("valid cancel response")
        };
        assert_eq!(
            count_cancelled(&de(
                serde_json::json!({ "cancelledOrderIds": ["a", "b", "c"] })
            )),
            3
        );
        // Absent field defaults to an empty list ⇒ 0.
        assert_eq!(count_cancelled(&de(serde_json::json!({}))), 0);
        // A malformed (non-array) field is now a deserialization error.
        assert!(
            serde_json::from_value::<CancelledOrders>(
                serde_json::json!({ "cancelledOrderIds": 7 })
            )
            .is_err()
        );
    }

    #[test]
    fn capabilities_are_truthful() {
        let a = adapter();
        for yes in [
            Capability::StopOrders,
            Capability::ReduceOnly,
            Capability::Ioc,
            Capability::Fok,
            Capability::OrderTracking,
        ] {
            assert!(a.supports(yes), "expected support for {yes:?}");
        }
        for no in [
            Capability::PostOnly,
            Capability::PublicFeed,
            Capability::PrivateFeed,
        ] {
            assert!(!a.supports(no), "expected NO support for {no:?}");
        }
    }

    #[test]
    fn contract_value_uses_cache_then_spot_default() {
        let a = adapter();
        assert_eq!(a.contract_value(&Symbol::from("XBTUSDTM")), 0.001);
        // Unknown symbol falls back to the spot default.
        assert_eq!(a.contract_value(&Symbol::from("UNKNOWN")), 1.0);
    }

    #[test]
    fn instrument_spec_is_crypto_perp_whole_contract() {
        let a = adapter();
        let spec = a.instrument_spec(&Symbol::from("XBTUSDTM"));
        assert_eq!(spec.asset_class, AssetClass::CryptoPerp);
        assert_eq!(spec.contract_value, 0.001); // from the cache
        assert_eq!(spec.lot_size, 1.0); // whole contracts
    }

    #[test]
    fn name_and_leverage() {
        let a = adapter();
        assert_eq!(a.name(), "kucoin");
        assert_eq!(a.leverage(), 5);
        // Leverage floors at 1.
        let client =
            KuCoinClient::new(Credentials::new("k", "s", "p"), KucoinEnv::LiveFutures).unwrap();
        assert_eq!(KucoinExchangeAdapter::new(client, 0).leverage(), 1);
    }

    #[test]
    fn ms_to_dt_roundtrips() {
        let dt = ms_to_dt(1_700_000_000_000).expect("valid ms");
        assert_eq!(dt.timestamp_millis(), 1_700_000_000_000);
    }
}
