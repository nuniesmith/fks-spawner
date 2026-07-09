//! Kraken [`SpotExchange`] adapter.
//!
//! Wraps `exchange-apiws`'s `KrakenRestClient` (public ticker) +
//! `KrakenPrivateClient` (balances + AddOrder), centralizing Kraken's XBT/ZUSD
//! code quirks behind clean asset names — the same calls the standalone Kraken
//! bot uses, now behind the common trait. Long-only, market orders, never
//! withdraws.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tokio::sync::OnceCell;
use tracing::warn;

use exchange_apiws::kraken::{KrakenCredentials, KrakenPrivateClient, KrakenRestClient};

use super::exchange::{Balance, Fill, Side, SpotExchange};

/// Kraken-specific codes for one asset.
struct Codes {
    name: &'static str,
    /// Pair altname for Ticker + AddOrder, e.g. `"XBTUSD"`.
    pair: &'static str,
    /// Balance-map keys to try in order (older assets carry an X prefix).
    balance_keys: &'static [&'static str],
}

/// Known assets. Extend as the basket grows. (BTC's Kraken code is **XBT**;
/// balances come back keyed `XXBT`/`XETH`/`SOL`, USD as `ZUSD`.)
const ASSETS: &[Codes] = &[
    Codes {
        name: "BTC",
        pair: "XBTUSD",
        balance_keys: &["XXBT", "XBT"],
    },
    Codes {
        name: "ETH",
        pair: "ETHUSD",
        balance_keys: &["XETH", "ETH"],
    },
    Codes {
        name: "SOL",
        pair: "SOLUSD",
        balance_keys: &["SOL"],
    },
];
const USD_KEYS: &[&str] = &["ZUSD", "USD"];

fn codes(asset: &str) -> Option<&'static Codes> {
    ASSETS.iter().find(|a| a.name == asset)
}

/// Read a balance (f64) from Kraken's `{code: amount-string}` map, trying each key.
fn balance_of(map: &HashMap<String, String>, keys: &[&str]) -> f64 {
    keys.iter()
        .find_map(|k| map.get(*k).and_then(|s| s.parse::<f64>().ok()))
        .unwrap_or(0.0)
}

/// Kraken spot adapter. With no credentials it can still read prices (public),
/// but balances/orders return an error — used for price-only / paper mode.
pub struct KrakenSpot {
    public: KrakenRestClient,
    private: Option<KrakenPrivateClient>,
    cash: String,
    /// Lazily-fetched per-pair lot precision (altname → lot_decimals), so order
    /// volumes are rounded to what Kraken accepts rather than a blind 8 dp.
    lot_decimals: OnceCell<HashMap<String, u32>>,
}

impl KrakenSpot {
    pub fn new(cash: impl Into<String>, creds: Option<KrakenCredentials>) -> Result<Self> {
        let public = KrakenRestClient::new().context("building Kraken public client")?;
        let private = match creds {
            Some(c) => Some(KrakenPrivateClient::new(c).context("building Kraken private client")?),
            None => None,
        };
        Ok(Self {
            public,
            private,
            cash: cash.into(),
            lot_decimals: OnceCell::new(),
        })
    }

    fn pair_for(&self, asset: &str) -> Result<&'static str> {
        codes(asset)
            .map(|a| a.pair)
            .with_context(|| format!("Kraken: unknown asset {asset} — add it to the code table"))
    }

    /// Lot-size decimal precision for `pair` (e.g. `"XBTUSD"`), fetched once from
    /// Kraken's AssetPairs and cached. Defaults to 8 dp if unavailable.
    async fn lot_decimals_for(&self, pair: &str) -> usize {
        let map = self
            .lot_decimals
            .get_or_init(|| async {
                match self.public.get_asset_pairs(None).await {
                    Ok(pairs) => pairs.values().map(|p| (p.altname.clone(), p.lot_decimals)).collect(),
                    Err(e) => {
                        warn!(error = %e, "Kraken: AssetPairs fetch failed — defaulting lot precision to 8 dp");
                        HashMap::new()
                    }
                }
            })
            .await;
        map.get(pair).map_or(8, |d| *d as usize)
    }
}

#[async_trait]
impl SpotExchange for KrakenSpot {
    fn name(&self) -> &str {
        "Kraken"
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
            .context("Kraken: balances need API keys")?;
        let map = pc.get_balance().await.context("Kraken get_balance")?;
        let mut out: Vec<Balance> = ASSETS
            .iter()
            .filter_map(|a| {
                let free = balance_of(&map, a.balance_keys);
                (free > 0.0).then(|| Balance {
                    asset: a.name.to_string(),
                    free,
                })
            })
            .collect();
        out.push(Balance {
            asset: self.cash.clone(),
            free: balance_of(&map, USD_KEYS),
        });
        Ok(out)
    }

    async fn price(&self, asset: &str) -> Result<f64> {
        let pair = self.pair_for(asset)?;
        let map = self
            .public
            .get_ticker(pair)
            .await
            .context("Kraken get_ticker")?;
        let t = map
            .values()
            .next()
            .context("Kraken: empty ticker response")?;
        t.c.first()
            .context("Kraken: ticker has no last price")?
            .parse::<f64>()
            .context("Kraken: parsing last price")
    }

    async fn market_buy(&self, asset: &str, quote_usd: f64) -> Result<Fill> {
        let pc = self
            .private
            .as_ref()
            .context("Kraken: orders need API keys")?;
        let pair = self.pair_for(asset)?;
        // Price the buy fresh at execution time → base volume for AddOrder.
        let price = self.price(asset).await?;
        ensure!(price > 0.0, "Kraken: non-positive price for {asset}");
        let volume = quote_usd / price;
        let prec = self.lot_decimals_for(pair).await;
        let vol = format!("{volume:.prec$}");
        let resp = pc
            .place_order(pair, "buy", "market", &vol, None)
            .await
            .with_context(|| format!("Kraken market buy {asset}"))?;
        let txid = resp.txid.first().map(String::as_str).unwrap_or_default();
        Ok(resolve_fill(pc, txid, asset, Side::Buy, volume, price).await)
    }

    async fn market_sell(&self, asset: &str, base_qty: f64) -> Result<Fill> {
        let pc = self
            .private
            .as_ref()
            .context("Kraken: orders need API keys")?;
        let pair = self.pair_for(asset)?;
        let price = self.price(asset).await?; // request-price fallback only
        let prec = self.lot_decimals_for(pair).await;
        let vol = format!("{base_qty:.prec$}");
        let resp = pc
            .place_order(pair, "sell", "market", &vol, None)
            .await
            .with_context(|| format!("Kraken market sell {asset}"))?;
        let txid = resp.txid.first().map(String::as_str).unwrap_or_default();
        Ok(resolve_fill(pc, txid, asset, Side::Sell, base_qty, price).await)
    }
}

/// Resolve the REAL fill of order `txid` from closed-orders, retrying briefly for
/// the record to settle (market orders fill ~instantly but can lag the
/// closed-orders feed). Returns the executed volume + volume-weighted avg price
/// (`cost / vol_exec`). Falls back to the requested qty/price if it can't resolve
/// (e.g. no txid, or the order isn't closed yet) — logged loudly.
async fn resolve_fill(
    pc: &KrakenPrivateClient,
    txid: &str,
    asset: &str,
    side: Side,
    req_qty: f64,
    req_price: f64,
) -> Fill {
    if !txid.is_empty() {
        for attempt in 0..5u32 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            match pc.get_closed_orders().await {
                Ok(co) => {
                    if let Some(o) = co.closed.get(txid) {
                        let vol_exec = o.vol_exec.parse::<f64>().unwrap_or(0.0);
                        if o.status == "closed" && vol_exec > 0.0 {
                            let cost = o.cost.parse::<f64>().unwrap_or(vol_exec * req_price);
                            return Fill {
                                asset: asset.to_string(),
                                side,
                                base_qty: vol_exec,
                                avg_price: cost / vol_exec,
                                quote_usd: cost,
                            };
                        }
                    }
                }
                Err(e) => {
                    warn!(txid, error = %e, "Kraken: closed-orders poll failed while resolving fill")
                }
            }
        }
        warn!(
            txid,
            "Kraken: could not resolve fill from closed-orders — using requested values"
        );
    }
    Fill {
        asset: asset.to_string(),
        side,
        base_qty: req_qty,
        avg_price: req_price,
        quote_usd: req_qty * req_price,
    }
}
