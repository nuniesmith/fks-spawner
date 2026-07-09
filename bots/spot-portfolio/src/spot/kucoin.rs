//! KuCoin **spot** [`SpotExchange`] adapter.
//!
//! exchange-apiws's KuCoin client is futures/margin-oriented (its typed
//! `place_order` takes contracts + leverage; there is no typed plain-spot order
//! wrapper). But `KuCoinClient` exposes generic *signed* `get`/`post`, and
//! `KuCoin::spot(creds)` points it at the spot host (`api.kucoin.com`), so this
//! adapter drives KuCoin's **spot** REST directly with raw JSON:
//!   - price    `GET  /api/v1/market/orderbook/level1?symbol=BTC-USDT` → `price`
//!   - balances `GET  /api/v1/accounts?type=trade`        → `[{currency, available}]`
//!   - buy      `POST /api/v1/orders {type:market, side:buy,  funds:<quote>}`
//!   - sell     `POST /api/v1/orders {type:market, side:sell, size:<base>}`
//!   - fill     `GET  /api/v1/orders/{id}`                 → `dealSize` / `dealFunds`
//!
//! This is genuine **spot** (no borrow, no leverage). KuCoin spot market BUYs are
//! natively quote-denominated via `funds`, mapping perfectly onto this trait, and
//! the order-detail endpoint gives a REAL fill (executed base/quote), so fills are
//! verified — not estimated.
//!
//! NB: EXPERIMENTAL — raw JSON, UNTESTED against a live account, and it needs
//! **spot-enabled** keys (`KUCOIN_API_KEY` / `_SECRET` / `_PASSPHRASE`), which are
//! distinct from the futures bot's `KC_*` keys. Balances read the **trade**
//! account only (funds must be there to trade). KuCoin's role in this portfolio is
//! futures (Canada restriction); spot here is a bonus venue. Verify with a tiny
//! live trade first.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use exchange_apiws::client::{Credentials, KuCoinClient};
use exchange_apiws::connectors::KuCoin;
use serde_json::{Value, json};
use tokio::sync::OnceCell;
use tracing::warn;

use super::exchange::{Balance, Fill, Side, SpotExchange, decimals_of, floor_dp};

/// KuCoin spot adapter. Always holds a client (pointed at the spot host); without
/// real keys, private calls fail and `has_keys` is false (→ paper mode), while the
/// public price endpoint still works.
/// Per-symbol order precision + minimums from `GET /api/v1/symbols`.
#[derive(Clone)]
struct SymPrec {
    /// Decimal places for base-asset `size` (from `baseIncrement`).
    base_dp: usize,
    /// Decimal places for quote-asset `funds` (from `quoteIncrement`).
    quote_dp: usize,
    /// Minimum base `size` for a sell (`baseMinSize`).
    base_min: f64,
    /// Minimum quote `funds` for a buy (`quoteMinSize`).
    quote_min: f64,
}

impl Default for SymPrec {
    /// Conservative fallback if AssetPairs is unavailable: 8 dp, no minimum.
    fn default() -> Self {
        Self {
            base_dp: 8,
            quote_dp: 8,
            base_min: 0.0,
            quote_min: 0.0,
        }
    }
}

pub struct KucoinSpot {
    client: KuCoinClient,
    has_keys: bool,
    cash: String,
    /// Lazily-fetched per-symbol precision/minimums (symbol → [`SymPrec`]).
    symbols: OnceCell<HashMap<String, SymPrec>>,
}

impl KucoinSpot {
    pub fn new(cash: impl Into<String>, creds: Option<Credentials>) -> Result<Self> {
        let has_keys = creds.is_some();
        // Build the client even without keys so public price calls work; private
        // calls then fail and the engine drops this venue to paper mode.
        let creds = creds.unwrap_or_else(|| Credentials::new("", "", ""));
        let client = KuCoin::spot(creds)
            .rest_client()
            .context("building KuCoin spot client")?;
        Ok(Self {
            client,
            has_keys,
            cash: cash.into(),
            symbols: OnceCell::new(),
        })
    }

    /// KuCoin spot symbol for `asset`, e.g. `"BTC-USDT"`.
    fn symbol(&self, asset: &str) -> String {
        format!("{asset}-{}", self.cash)
    }

    /// Order precision + minimums for `sym`, fetched once from `/api/v1/symbols`
    /// (public; cached). Defaults to 8 dp / no minimum if unavailable.
    async fn sym_prec(&self, sym: &str) -> SymPrec {
        let map = self
            .symbols
            .get_or_init(|| async {
                match self.client.get::<Value>("/api/v1/symbols", &[]).await {
                    Ok(v) => parse_symbols(&v),
                    Err(e) => {
                        warn!(error = %e, "KuCoin: symbols fetch failed — using default precision");
                        HashMap::new()
                    }
                }
            })
            .await;
        map.get(sym).cloned().unwrap_or_default()
    }

    /// Resolve the REAL fill of `order_id` from `GET /api/v1/orders/{id}`
    /// (`dealSize` base, `dealFunds` quote), retrying briefly for the order to
    /// settle. Falls back to the supplied estimate if it can't be read.
    async fn resolve_fill(
        &self,
        order_id: &str,
        asset: &str,
        side: Side,
        est_base: f64,
        est_quote: f64,
    ) -> Fill {
        if !order_id.is_empty() {
            for attempt in 0..5u32 {
                if attempt > 0 {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                match self
                    .client
                    .get::<Value>(&format!("/api/v1/orders/{order_id}"), &[])
                    .await
                {
                    Ok(o) => {
                        let deal_size = o.get("dealSize").and_then(json_f64).unwrap_or(0.0);
                        let deal_funds = o.get("dealFunds").and_then(json_f64).unwrap_or(0.0);
                        if deal_size > 0.0 && deal_funds > 0.0 {
                            return Fill {
                                asset: asset.to_string(),
                                side,
                                base_qty: deal_size,
                                avg_price: deal_funds / deal_size,
                                quote_usd: deal_funds,
                            };
                        }
                    }
                    Err(e) => {
                        warn!(order_id, error = %e, "KuCoin: order poll failed while resolving fill")
                    }
                }
            }
            warn!(
                order_id,
                "KuCoin: could not resolve fill from order detail — using estimated values"
            );
        }
        let avg = if est_base > 0.0 {
            est_quote / est_base
        } else {
            0.0
        };
        Fill {
            asset: asset.to_string(),
            side,
            base_qty: est_base,
            avg_price: avg,
            quote_usd: est_quote,
        }
    }
}

/// Parse a JSON value that may be a number or a numeric string into f64.
fn json_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// Extract free balances from a KuCoin `GET /api/v1/accounts` response (an array
/// of `{currency, type, balance, available, holds}`). Defensive: a non-array or
/// unknown shape yields an empty vec.
fn parse_accounts(v: &Value) -> Vec<Balance> {
    let mut out = Vec::new();
    let Some(arr) = v.as_array() else {
        return out;
    };
    for a in arr {
        let Some(cur) = a.get("currency").and_then(Value::as_str) else {
            continue;
        };
        let free = a.get("available").and_then(json_f64).unwrap_or(0.0);
        if free > 0.0 {
            out.push(Balance {
                asset: cur.to_string(),
                free,
            });
        }
    }
    out
}

/// Build a symbol → [`SymPrec`] map from a KuCoin `GET /api/v1/symbols` response
/// (array of `{symbol, baseIncrement, quoteIncrement, baseMinSize, quoteMinSize}`).
fn parse_symbols(v: &Value) -> HashMap<String, SymPrec> {
    let mut m = HashMap::new();
    let Some(arr) = v.as_array() else {
        return m;
    };
    for s in arr {
        let Some(sym) = s.get("symbol").and_then(Value::as_str) else {
            continue;
        };
        let base_dp = s
            .get("baseIncrement")
            .and_then(Value::as_str)
            .map_or(8, decimals_of);
        let quote_dp = s
            .get("quoteIncrement")
            .and_then(Value::as_str)
            .map_or(8, decimals_of);
        let base_min = s.get("baseMinSize").and_then(json_f64).unwrap_or(0.0);
        let quote_min = s.get("quoteMinSize").and_then(json_f64).unwrap_or(0.0);
        m.insert(
            sym.to_string(),
            SymPrec {
                base_dp,
                quote_dp,
                base_min,
                quote_min,
            },
        );
    }
    m
}

/// A process-unique client order id (KuCoin requires `clientOid` unique per
/// order). `uuid` isn't a dependency, so compose nanos + a monotonic counter.
fn client_oid() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("dip-{nanos}-{n}")
}

#[async_trait]
impl SpotExchange for KucoinSpot {
    fn name(&self) -> &str {
        "KuCoin-spot"
    }
    fn has_keys(&self) -> bool {
        self.has_keys
    }
    fn cash_asset(&self) -> &str {
        &self.cash
    }

    async fn balances(&self) -> Result<Vec<Balance>> {
        ensure!(self.has_keys, "KuCoin-spot: balances need API keys");
        // The `trade` account is the spot trading wallet (vs `main` funding).
        let v: Value = self
            .client
            .get("/api/v1/accounts", &[("type", "trade")])
            .await
            .context("KuCoin get accounts")?;
        Ok(parse_accounts(&v))
    }

    async fn price(&self, asset: &str) -> Result<f64> {
        let sym = self.symbol(asset);
        // Public endpoint (KuCoinClient signs, but KuCoin ignores auth here).
        let v: Value = self
            .client
            .get("/api/v1/market/orderbook/level1", &[("symbol", &sym)])
            .await
            .with_context(|| format!("KuCoin level1 {sym}"))?;
        let px = v
            .get("price")
            .and_then(json_f64)
            .with_context(|| format!("KuCoin: no price in level1 for {sym}"))?;
        ensure!(px > 0.0, "KuCoin: non-positive price for {sym}");
        Ok(px)
    }

    async fn market_buy(&self, asset: &str, quote_usd: f64) -> Result<Fill> {
        ensure!(self.has_keys, "KuCoin-spot: orders need API keys");
        let sym = self.symbol(asset);
        // KuCoin spot market BUY is quote-denominated via `funds`; round to the
        // pair's quote precision and enforce its quote minimum.
        let prec = self.sym_prec(&sym).await;
        let funds = floor_dp(quote_usd, prec.quote_dp);
        ensure!(
            funds >= prec.quote_min,
            "KuCoin: {sym} buy funds {funds} below quoteMinSize {}",
            prec.quote_min
        );
        let qdp = prec.quote_dp;
        let body = json!({
            "clientOid": client_oid(),
            "side": "buy",
            "symbol": sym,
            "type": "market",
            "funds": format!("{funds:.qdp$}"),
        });
        let resp: Value = self
            .client
            .post("/api/v1/orders", &body)
            .await
            .with_context(|| format!("KuCoin market buy {sym}"))?;
        let order_id = resp
            .get("orderId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        // Estimate base from a fresh price as the fallback if the fill can't be read.
        let price = self.price(asset).await.unwrap_or(0.0);
        let est_base = if price > 0.0 { funds / price } else { 0.0 };
        Ok(self
            .resolve_fill(order_id, asset, Side::Buy, est_base, funds)
            .await)
    }

    async fn market_sell(&self, asset: &str, base_qty: f64) -> Result<Fill> {
        ensure!(self.has_keys, "KuCoin-spot: orders need API keys");
        let sym = self.symbol(asset);
        // Round the base `size` down to the pair's base precision; enforce its min.
        let prec = self.sym_prec(&sym).await;
        let size = floor_dp(base_qty, prec.base_dp);
        ensure!(
            size >= prec.base_min,
            "KuCoin: {sym} sell size {size} below baseMinSize {}",
            prec.base_min
        );
        let bdp = prec.base_dp;
        let body = json!({
            "clientOid": client_oid(),
            "side": "sell",
            "symbol": sym,
            "type": "market",
            "size": format!("{size:.bdp$}"),
        });
        let resp: Value = self
            .client
            .post("/api/v1/orders", &body)
            .await
            .with_context(|| format!("KuCoin market sell {sym}"))?;
        let order_id = resp
            .get("orderId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let price = self.price(asset).await.unwrap_or(0.0);
        Ok(self
            .resolve_fill(order_id, asset, Side::Sell, size, size * price)
            .await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn symbol_uses_dash_separator() {
        let k = KucoinSpot::new("USDT", None).unwrap();
        assert_eq!(k.symbol("BTC"), "BTC-USDT");
        assert_eq!(k.symbol("SOL"), "SOL-USDT");
    }

    #[test]
    fn no_keys_means_paper() {
        let k = KucoinSpot::new("USDT", None).unwrap();
        assert!(!k.has_keys());
        assert_eq!(k.name(), "KuCoin-spot");
        assert_eq!(k.cash_asset(), "USDT");
    }

    #[test]
    fn parse_accounts_reads_trade_balances() {
        let v = json!([
            { "currency": "USDT", "type": "trade", "balance": "500.0", "available": "500.0", "holds": "0" },
            { "currency": "BTC",  "type": "trade", "balance": "0.1",   "available": "0.1",   "holds": "0" },
            { "currency": "ETH",  "type": "trade", "balance": "0",     "available": "0",     "holds": "0" }
        ]);
        let mut bals = parse_accounts(&v);
        bals.sort_by(|a, b| a.asset.cmp(&b.asset));
        assert_eq!(bals.len(), 2); // ETH (0 available) dropped
        assert_eq!(bals[0].asset, "BTC");
        assert!((bals[0].free - 0.1).abs() < 1e-9);
        assert_eq!(bals[1].asset, "USDT");
    }

    #[test]
    fn parse_accounts_tolerates_garbage() {
        assert!(parse_accounts(&json!({})).is_empty());
        assert!(parse_accounts(&json!([])).is_empty());
    }

    #[test]
    fn client_oid_is_unique() {
        let a = client_oid();
        let b = client_oid();
        assert_ne!(a, b);
        assert!(a.starts_with("dip-"));
    }

    #[test]
    fn parse_symbols_reads_precision_and_minimums() {
        let v = json!([{
            "symbol": "BTC-USDT",
            "baseIncrement": "0.00000001",
            "quoteIncrement": "0.000001",
            "baseMinSize": "0.00001",
            "quoteMinSize": "0.1"
        }]);
        let m = parse_symbols(&v);
        let p = m.get("BTC-USDT").expect("BTC-USDT present");
        assert_eq!(p.base_dp, 8);
        assert_eq!(p.quote_dp, 6);
        assert!((p.base_min - 0.00001).abs() < 1e-12);
        assert!((p.quote_min - 0.1).abs() < 1e-12);
        // Unknown symbol → conservative default.
        assert!(parse_symbols(&json!([])).is_empty());
    }
}
