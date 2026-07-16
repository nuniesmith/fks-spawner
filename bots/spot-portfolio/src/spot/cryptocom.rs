//! Crypto.com [`SpotExchange`] adapter.
//!
//! Wraps exchange-apiws's `CryptocomRestClient` (public ticker) +
//! `CryptocomPrivateClient` (user-balance + create-order). Balances come back
//! typed (`CryptocomUserBalance` as of exchange-apiws 0.10); the typed public
//! ticker drives `price`; orders return raw JSON and are parsed defensively.
//! Instrument names are `"{ASSET}_{CASH}"` (e.g. `BTC_USDT`), so the configured
//! cash currency selects the quote pair.
//!
//! NB: only `price` (public) is exercised by the paper dry-run. Balances +
//! orders should be verified against a real Crypto.com account before trusting
//! them live (untested against a live account here).

use std::collections::HashMap;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use exchange_apiws::cryptocom::{
    CryptocomCredentials, CryptocomPrivateClient, CryptocomRestClient, CryptocomUserBalance,
};
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

/// Flatten Crypto.com per-account `position_balances` into free/tradeable
/// [`Balance`]s: `free = max(quantity - reserved_qty, 0)`, skipping unnamed
/// assets and zero-free holdings. Pure over the typed `get_user_balance`
/// response so the mapping can be unit-tested without a live account.
fn free_balances(accounts: &[CryptocomUserBalance]) -> Vec<Balance> {
    let mut out = Vec::new();
    for acct in accounts {
        for p in &acct.position_balances {
            if p.instrument_name.is_empty() {
                continue;
            }
            // quantity = total holding; subtract reserved (in open orders) for
            // the free/tradeable amount.
            let qty = p.quantity_f64();
            let reserved = p
                .reserved_qty
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let free = (qty - reserved).max(0.0);
            if free > 0.0 {
                out.push(Balance {
                    asset: p.instrument_name.clone(),
                    free,
                });
            }
        }
    }
    out
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
        // Typed as of exchange-apiws 0.10: `get_user_balance` returns one
        // `CryptocomUserBalance` per account (the endpoint's `data[]`, typically
        // a single element), each carrying a `position_balances[]` per-asset
        // breakdown of {instrument_name, quantity, reserved_qty, ...}. The
        // `result`/`data` envelope is unwrapped by the client.
        let accounts = pc
            .get_user_balance()
            .await
            .context("Crypto.com get_user_balance")?;
        Ok(free_balances(&accounts))
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

#[cfg(test)]
mod tests {
    use super::*;

    // Representative `data[0]` snapshot from `private/user-balance` (the shape the
    // live rebalancer reads for Crypto.com holdings). Two named positions plus a
    // zero-free and an unnamed entry that must be dropped.
    fn snapshot() -> Vec<CryptocomUserBalance> {
        let raw = r#"[{
            "instrument_name": "USD",
            "position_balances": [
                { "instrument_name": "BTC", "quantity": "0.5",  "reserved_qty": "0.1" },
                { "instrument_name": "USD", "quantity": "100.0" },
                { "instrument_name": "ETH", "quantity": "2.0",  "reserved_qty": "2.0" },
                { "instrument_name": "",    "quantity": "9.9" }
            ]
        }]"#;
        serde_json::from_str(raw).expect("deserialize user-balance snapshot")
    }

    #[test]
    fn free_balances_subtracts_reserved_and_drops_empty() {
        let out = free_balances(&snapshot());
        // BTC: 0.5 - 0.1 = 0.4 free; USD: 100.0 (no reserved). ETH nets to 0 and
        // the unnamed asset is skipped, so neither appears.
        assert_eq!(out.len(), 2);
        let btc = out.iter().find(|b| b.asset == "BTC").expect("BTC present");
        assert!((btc.free - 0.4).abs() < 1e-9);
        let usd = out.iter().find(|b| b.asset == "USD").expect("USD present");
        assert!((usd.free - 100.0).abs() < 1e-9);
        assert!(
            out.iter().all(|b| b.asset != "ETH"),
            "fully-reserved ETH dropped"
        );
        assert!(
            out.iter().all(|b| !b.asset.is_empty()),
            "unnamed asset dropped"
        );
    }

    #[test]
    fn free_balances_tolerates_numeric_wire_amounts() {
        // The `flex` deserializer accepts JSON numbers as well as strings.
        let accounts: Vec<CryptocomUserBalance> = serde_json::from_str(
            r#"[{"position_balances":[{"instrument_name":"SOL","quantity":3.5,"reserved_qty":0.5}]}]"#,
        )
        .expect("deserialize numeric amounts");
        let out = free_balances(&accounts);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].asset, "SOL");
        assert!((out[0].free - 3.0).abs() < 1e-9);
    }

    #[test]
    fn free_balances_empty_when_no_accounts() {
        assert!(free_balances(&[]).is_empty());
    }
}
