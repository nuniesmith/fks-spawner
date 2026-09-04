//! The normalized spot-exchange interface.
//!
//! `exchange-apiws` exposes a *bespoke* REST trading client per venue (different
//! method names, different balance/order types, no shared trait). This module
//! defines the single shape the portfolio engine needs — `balances`, `price`,
//! `market_buy`, `market_sell` — and each venue gets a thin adapter implementing
//! it (see `kraken`, `cryptocom`). Prices could also come from the unified WS
//! `DataMessage` feed, but a REST `price()` keeps the rebalancer synchronous and
//! simple.

use anyhow::Result;
use async_trait::async_trait;

/// Trade side (shared by the rebalancer and the exchange adapters).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

/// A free balance of one asset on a venue.
#[derive(Clone, Debug, PartialEq)]
pub struct Balance {
    pub asset: String,
    pub free: f64,
}

/// The realized result of a market order.
#[derive(Clone, Debug, PartialEq)]
pub struct Fill {
    pub asset: String,
    pub side: Side,
    /// Base-asset quantity actually filled.
    pub base_qty: f64,
    /// Volume-weighted average fill price, in the cash currency.
    pub avg_price: f64,
    /// Cash (quote) value moved = `base_qty * avg_price`.
    pub quote_usd: f64,
    /// Whether the numbers above came from BROKER CONFIRMATION (`true`) or are
    /// the values we asked for / estimated because confirmation could not be
    /// resolved (`false`).
    ///
    /// WHY THIS FIELD EXISTS. Every adapter has a path where the closed-order
    /// lookup fails and it returns the REQUESTED quantity and the price fetched
    /// BEFORE submission, shaped exactly like a real fill. Downstream that is
    /// consumed as an execution: it is journalled as one, it seeds
    /// reconciliation expectations, and it is what the slippage check measures.
    ///
    /// That last one is the trap. Slippage is `planned vs filled`, and on an
    /// unresolved fill those are THE SAME NUMBER — so the check that exists to
    /// catch a bad fill reports exactly 0.00% precisely when the fill is least
    /// trustworthy. A fabricated fill is not merely unverified, it is actively
    /// self-certifying.
    ///
    /// This flag does not fix that. It makes it VISIBLE, so an unconfirmed
    /// execution can be seen in the journal and alerted on, rather than being
    /// indistinguishable from a confirmed one. The real repair is an order
    /// lifecycle with an explicit unknown state; this is the detection half.
    pub resolved: bool,
}

/// A normalized spot-trading interface over one exchange. Each venue's bespoke
/// `exchange-apiws` client is wrapped in an adapter implementing this trait, so
/// the portfolio engine can treat every venue uniformly.
#[async_trait]
pub trait SpotExchange: Send + Sync {
    /// Display name, e.g. `"Kraken"`.
    fn name(&self) -> &str;

    /// Whether private API keys are present (real balances + orders available).
    /// `false` → the engine runs this venue in paper mode (simulated, no orders).
    fn has_keys(&self) -> bool;

    /// The cash/quote currency this venue settles in (e.g. `"USD"`, `"USDT"`,
    /// `"USDC"`) — the reserve is held here.
    fn cash_asset(&self) -> &str;

    /// Free balances for every non-zero asset, including the cash currency.
    async fn balances(&self) -> Result<Vec<Balance>>;

    /// Last price of `asset` quoted in the cash currency.
    async fn price(&self, asset: &str) -> Result<f64>;

    /// Market-buy `quote_usd` worth of `asset`.
    async fn market_buy(&self, asset: &str, quote_usd: f64) -> Result<Fill>;

    /// Market-sell `base_qty` of `asset`.
    async fn market_sell(&self, asset: &str, base_qty: f64) -> Result<Fill>;
}

// ── Order-precision helpers (shared by the venue adapters) ──────────────────

/// Decimal places implied by a power-of-ten increment string, e.g. `"0.000001"`
/// → 6, `"0.1"` → 1, `"1"` → 0. Venue size/price increments are powers of ten.
pub(crate) fn decimals_of(increment: &str) -> usize {
    increment
        .split_once('.')
        .map_or(0, |(_, frac)| frac.trim_end_matches('0').len())
}

/// Floor `value` to `dp` decimal places. Order quantities are floored (never
/// rounded up) so a rounded qty can't exceed the available balance or overshoot
/// the venue's allowed precision. A tiny epsilon absorbs binary-float error
/// (e.g. 0.29 → 28.999999/100) without crossing a real tick boundary.
pub(crate) fn floor_dp(value: f64, dp: usize) -> f64 {
    let f = 10f64.powi(dp as i32);
    ((value * f) + 1e-6).floor() / f
}

/// `value` floored to `dp` places, as a fixed-precision string for an order body.
pub(crate) fn fmt_floor(value: f64, dp: usize) -> String {
    format!("{:.dp$}", floor_dp(value, dp))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_of_counts_increment_precision() {
        assert_eq!(decimals_of("0.00000001"), 8);
        assert_eq!(decimals_of("0.000001"), 6);
        assert_eq!(decimals_of("0.1"), 1);
        assert_eq!(decimals_of("1"), 0);
        assert_eq!(decimals_of("0.0010"), 3); // trailing zeros ignored
    }

    #[test]
    fn fmt_floor_never_rounds_up() {
        assert_eq!(fmt_floor(0.015_678_9, 8), "0.01567890");
        assert_eq!(fmt_floor(0.012_399_999, 4), "0.0123"); // floored, not 0.0124
        assert_eq!(fmt_floor(375.0, 6), "375.000000");
        assert_eq!(fmt_floor(1.999, 0), "1");
    }

    #[test]
    fn floor_dp_tolerates_float_error() {
        // 0.29 is 0.28999999.. in binary; must still floor to 0.29 at 2 dp.
        assert_eq!(fmt_floor(0.29, 2), "0.29");
        assert_eq!(fmt_floor(0.07, 2), "0.07");
    }
}
