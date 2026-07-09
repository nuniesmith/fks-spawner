//! Crypto.com [`SpotExchange`] adapter.
//!
//! Wraps exchange-apiws's `CryptocomRestClient` (public ticker) +
//! `CryptocomPrivateClient` (get-account-summary + create-order). Crypto.com
//! returns raw JSON for balances/orders, so those are parsed defensively; the
//! typed public ticker drives `price`. Instrument names are `"{ASSET}_{CASH}"`
//! (e.g. `BTC_USDT`), so the configured cash currency selects the quote pair.
//!
//! NB: only `price` (public) is exercised by the paper dry-run. Balances +
//! orders should be verified against a real Crypto.com account before trusting
//! them live (the JSON shapes are parsed defensively but untested live here).

use std::collections::HashMap;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use exchange_apiws::cryptocom::{
    CryptocomCredentials, CryptocomPrivateClient, CryptocomRestClient,
};
use serde_json::Value;
use tokio::sync::OnceCell;
use tracing::warn;

use super::exchange::{Balance, Fill, Side, SpotExchange, fmt_floor};

pub struct CryptocomSpot {
    public: CryptocomRestClient,
    private: Option<CryptocomPrivateClient>,
    cash: String,
    /// Lazily-fetched per-instrument base-quantity precision (symbol →
    /// `quantity_decimals`), so order sizes are rounded to what Crypto.com accepts.
    qty_decimals: OnceCell<HashMap<String, u32>>,
}

impl CryptocomSpot {
    pub fn new(cash: impl Into<String>, creds: Option<CryptocomCredentials>) -> Result<Self> {
        let public = CryptocomRestClient::new().context("building Crypto.com public client")?;
        let private = match creds {
            Some(c) => {
                Some(CryptocomPrivateClient::new(c).context("building Crypto.com private client")?)
            }
            None => None,
        };
        Ok(Self {
            public,
            private,
            cash: cash.into(),
            qty_decimals: OnceCell::new(),
        })
    }

    /// Crypto.com instrument name for `asset`, e.g. `"BTC_USDT"`.
    fn instrument(&self, asset: &str) -> String {
        format!("{asset}_{}", self.cash)
    }

    /// Base-quantity decimal precision for `inst`, fetched once from
    /// `get-instruments` and cached. Defaults to 8 dp if unavailable.
    async fn qty_dp(&self, inst: &str) -> usize {
        let map = self
            .qty_decimals
            .get_or_init(|| async {
                match self.public.get_instruments().await {
                    Ok(insts) => insts
                        .into_iter()
                        .filter_map(|i| i.quantity_decimals.map(|d| (i.symbol, d)))
                        .collect(),
                    Err(e) => {
                        warn!(error = %e, "Crypto.com: instruments fetch failed — defaulting qty precision to 8 dp");
                        HashMap::new()
                    }
                }
            })
            .await;
        map.get(inst).map_or(8, |d| *d as usize)
    }
}

/// Parse a JSON value that may be a number or a numeric string into f64.
fn json_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

#[async_trait]
impl SpotExchange for CryptocomSpot {
    fn name(&self) -> &str {
        "Crypto.com"
    }
    fn has_keys(&self) -> bool {
        self.private.is_some()
    }
    fn cash_asset(&self) -> &str {
        &self.cash
    }

    async fn balances(&self) -> Result<Vec<Balance>> {
        let pc = self
            .private
            .as_ref()
            .context("Crypto.com: balances need API keys")?;
        let resp = pc
            .get_user_balance()
            .await
            .context("Crypto.com get_user_balance")?;
        // user-balance shape: result.data[0].position_balances[] of
        // {instrument_name, quantity, reserved_qty, ...}. The `result` envelope is
        // already unwrapped, so `data` may sit at the top level or under `result`.
        let data = resp
            .get("result")
            .and_then(|r| r.get("data"))
            .or_else(|| resp.get("data"))
            .and_then(Value::as_array)
            .context("Crypto.com: no `data` in user-balance response")?;
        let mut out = Vec::new();
        for acct in data {
            let Some(positions) = acct.get("position_balances").and_then(Value::as_array) else {
                continue;
            };
            for p in positions {
                let Some(inst) = p.get("instrument_name").and_then(Value::as_str) else {
                    continue;
                };
                // quantity = total holding; subtract reserved (in open orders) for
                // the free/tradeable amount.
                let qty = p.get("quantity").and_then(json_f64).unwrap_or(0.0);
                let reserved = p.get("reserved_qty").and_then(json_f64).unwrap_or(0.0);
                let free = (qty - reserved).max(0.0);
                if free > 0.0 {
                    out.push(Balance {
                        asset: inst.to_string(),
                        free,
                    });
                }
            }
        }
        Ok(out)
    }

    async fn price(&self, asset: &str) -> Result<f64> {
        let inst = self.instrument(asset);
        let tickers = self
            .public
            .get_ticker(Some(&inst))
            .await
            .context("Crypto.com get_ticker")?;
        let t = tickers
            .into_iter()
            .find(|t| t.instrument == inst)
            .with_context(|| format!("Crypto.com: {inst} not in ticker response"))?;
        t.last_price
            .with_context(|| format!("Crypto.com: {inst} ticker has no last price"))?
            .parse::<f64>()
            .context("Crypto.com: parsing last price")
    }

    async fn market_buy(&self, asset: &str, quote_usd: f64) -> Result<Fill> {
        let pc = self
            .private
            .as_ref()
            .context("Crypto.com: orders need API keys")?;
        let inst = self.instrument(asset);
        let price = self.price(asset).await?;
        ensure!(price > 0.0, "Crypto.com: non-positive price for {asset}");
        // Market BUY is sized in base qty here; round to the instrument's precision.
        let dp = self.qty_dp(&inst).await;
        let qty = fmt_floor(quote_usd / price, dp);
        pc.place_order(&inst, "BUY", "MARKET", &qty, None)
            .await
            .with_context(|| format!("Crypto.com market buy {inst}"))?;
        let base: f64 = qty.parse().unwrap_or(0.0);
        Ok(Fill {
            asset: asset.to_string(),
            side: Side::Buy,
            base_qty: base,
            avg_price: price,
            quote_usd: base * price,
        })
    }

    async fn market_sell(&self, asset: &str, base_qty: f64) -> Result<Fill> {
        let pc = self
            .private
            .as_ref()
            .context("Crypto.com: orders need API keys")?;
        let inst = self.instrument(asset);
        let price = self.price(asset).await?; // for the fill report only
        let dp = self.qty_dp(&inst).await;
        let qty = fmt_floor(base_qty, dp);
        pc.place_order(&inst, "SELL", "MARKET", &qty, None)
            .await
            .with_context(|| format!("Crypto.com market sell {inst}"))?;
        let base: f64 = qty.parse().unwrap_or(base_qty);
        Ok(Fill {
            asset: asset.to_string(),
            side: Side::Sell,
            base_qty: base,
            avg_price: price,
            quote_usd: base * price,
        })
    }
}
