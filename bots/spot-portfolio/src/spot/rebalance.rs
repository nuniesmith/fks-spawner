//! Pure threshold-rebalancing logic **with a cash reserve** — no I/O, fully
//! unit-testable. Ported from the standalone Kraken bot and extended.
//!
//! The original was always 100% invested (implicit cash target = 0). Here we
//! keep `reserve_pct` of total value in the cash currency and spread the rest
//! across the target basket. Two consequences that the planned system wants:
//! - **Dry powder:** you always hold `reserve_pct` in USDC/USDT/USD.
//! - **Deposit handling for free:** a USD deposit lifts cash above the reserve,
//!   which pushes every asset *under* its target → the next check deploys the
//!   new cash into the basket (keeping the reserve intact).
//! - **No wedging on a small book:** a genuinely breached target whose corrective
//!   buy lands just under `min_trade_usd` is *seeded* at the floor rather than
//!   skipped as dust, so a venue can't sit forever "triggered" but unable to act
//!   (e.g. a 20% slice worth ~$4.98 on a ~$33 book, held at zero).
//!
//! Trading only on a relative-drift breach (not on a schedule) keeps it
//! fee-efficient. Sells are emitted before buys so freed cash funds the buys.

use super::exchange::Side;

/// A target weight for one asset (weights are normalized; they need not sum to 1).
#[derive(Clone, Debug)]
pub struct Target {
    pub name: String,
    pub weight: f64,
}

/// Current holding of one asset (value = qty × price, in the cash currency).
#[derive(Clone, Debug)]
pub struct Holding {
    pub name: String,
    pub qty: f64,
    pub price: f64,
}

impl Holding {
    fn value(&self) -> f64 {
        self.qty * self.price
    }
}

/// One rebalancing trade to execute.
#[derive(Clone, Debug, PartialEq)]
pub struct Trade {
    pub name: String,
    pub side: Side,
    /// Quote (cash) amount to move.
    pub usd: f64,
    /// Base-asset volume = `usd / price`.
    pub volume: f64,
    pub price: f64,
}

/// The outcome of one rebalance check.
#[derive(Clone, Debug, Default)]
pub struct Plan {
    /// Total portfolio value (all holdings + cash), in the cash currency.
    pub total_value: f64,
    /// The investable portion = `total_value * (1 - reserve_pct)`.
    pub investable: f64,
    /// Cash on hand at the time of the check.
    pub cash: f64,
    /// Largest relative drift `|w − t| / t` across the basket (t = target weight
    /// as a fraction of total, i.e. already scaled by `1 - reserve_pct`).
    pub max_drift: f64,
    /// True when `max_drift` exceeded the band. (Trades can still be empty if the
    /// only breach is a sub-minimum dust delta — callers treat empty as a no-op.)
    pub triggered: bool,
    /// Sells first, then buys (so freed cash funds the buys).
    pub trades: Vec<Trade>,
    /// Current weight per asset, for logging.
    pub weights: Vec<(String, f64)>,
}

/// Compute the rebalance plan.
///
/// - `reserve_pct` (0.0–1.0): fraction of total value to keep in cash; the rest
///   is spread across `targets` by normalized weight.
/// - `band`: relative drift threshold (0.25 = rebalance when any asset is ±25%
///   off its target weight).
/// - `min_trade_usd`: skip dust trades below the venue minimum.
pub fn plan(
    holdings: &[Holding],
    cash: f64,
    targets: &[Target],
    reserve_pct: f64,
    band: f64,
    min_trade_usd: f64,
) -> Plan {
    let total: f64 = holdings.iter().map(Holding::value).sum::<f64>() + cash.max(0.0);
    let reserve = reserve_pct.clamp(0.0, 1.0);
    let investable = total * (1.0 - reserve);
    let mut p = Plan {
        total_value: total,
        investable,
        cash,
        ..Default::default()
    };
    let wsum: f64 = targets.iter().map(|t| t.weight).sum();
    if total <= 0.0 || wsum <= 0.0 {
        return p;
    }

    // Per-asset current value + target value (target weight is scaled by the
    // invested fraction so the leftover naturally settles at the cash reserve).
    let mut rows: Vec<(String, f64, f64, f64)> = Vec::new(); // (name, cur_val, target_val, price)
    for t in targets {
        let target_w = (t.weight / wsum) * (1.0 - reserve); // as a fraction of TOTAL
        let h = holdings.iter().find(|h| h.name == t.name);
        let price = h.map_or(0.0, |h| h.price);
        let cur_val = h.map_or(0.0, Holding::value);
        let cur_w = cur_val / total;
        if target_w > 0.0 {
            p.max_drift = p.max_drift.max((cur_w - target_w).abs() / target_w);
        }
        p.weights.push((t.name.clone(), cur_w));
        rows.push((t.name.clone(), cur_val, target_w * total, price));
    }

    p.triggered = p.max_drift > band;
    if !p.triggered {
        return p;
    }

    // Sells (overweight) first, then buys (underweight), so freed cash funds buys.
    let mut buys = Vec::new();
    for (name, cur_val, target_val, price) in rows {
        if price <= 0.0 {
            continue;
        }
        let delta = target_val - cur_val; // + = buy, − = sell
        if delta < 0.0 {
            // Sell: skip sub-minimum dust.
            let usd = -delta;
            if usd <= min_trade_usd {
                continue;
            }
            p.trades.push(Trade {
                name,
                side: Side::Sell,
                usd,
                volume: usd / price,
                price,
            });
        } else {
            // Buy. Normally skip sub-minimum dust — but if this target is genuinely
            // BREACHED (its own drift exceeds the band) and the corrective only lands
            // below the floor because the book is small, SEED it at the floor so the
            // venue can't wedge: a real under-allocation (e.g. a 20% slice that sits
            // just under min_trade_usd, held at zero) would otherwise be skipped every
            // cycle while the venue stays perpetually "triggered" but unable to act.
            // Guarded by `target_val ≥ ½·min` so rounding up can't grossly overshoot a
            // genuinely tiny target.
            let mut usd = delta;
            if usd <= min_trade_usd {
                let drift = if target_val > 0.0 {
                    (cur_val - target_val).abs() / target_val
                } else {
                    0.0
                };
                if drift > band && target_val >= 0.5 * min_trade_usd {
                    usd = min_trade_usd;
                } else {
                    continue;
                }
            }
            buys.push(Trade {
                name,
                side: Side::Buy,
                usd,
                volume: usd / price,
                price,
            });
        }
    }
    p.trades.extend(buys);
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(name: &str, qty: f64, price: f64) -> Holding {
        Holding {
            name: name.into(),
            qty,
            price,
        }
    }
    fn targets() -> Vec<Target> {
        vec![
            Target {
                name: "BTC".into(),
                weight: 0.5,
            },
            Target {
                name: "ETH".into(),
                weight: 0.3,
            },
            Target {
                name: "SOL".into(),
                weight: 0.2,
            },
        ]
    }

    #[test]
    fn fully_invested_reserve_zero_matches_legacy() {
        // reserve 0 → exactly 50/30/20 on a $1000 book does not trigger.
        let hs = [
            h("BTC", 5.0, 100.0),
            h("ETH", 3.0, 100.0),
            h("SOL", 2.0, 100.0),
        ];
        let p = plan(&hs, 0.0, &targets(), 0.0, 0.25, 1.0);
        assert!((p.total_value - 1000.0).abs() < 1e-9);
        assert!(p.max_drift < 1e-9);
        assert!(!p.triggered);
    }

    #[test]
    fn cash_reserve_is_left_undeployed() {
        // $1000 fresh cash, 20% reserve → deploy $800 across 50/30/20, keep $200.
        let hs = [
            h("BTC", 0.0, 100.0),
            h("ETH", 0.0, 100.0),
            h("SOL", 0.0, 100.0),
        ];
        let p = plan(&hs, 1000.0, &targets(), 0.20, 0.25, 1.0);
        assert!(p.triggered);
        assert!(p.trades.iter().all(|t| t.side == Side::Buy));
        let spent: f64 = p.trades.iter().map(|t| t.usd).sum();
        assert!(
            (spent - 800.0).abs() < 1e-6,
            "should deploy 80% = $800, spent {spent}"
        );
        let btc = p.trades.iter().find(|t| t.name == "BTC").unwrap();
        assert!((btc.volume - 4.0).abs() < 1e-9); // $400 / $100
    }

    #[test]
    fn at_the_reserve_target_nothing_triggers() {
        // 80% invested 50/30/20 ($400/$240/$160) + $200 cash = the reserve target.
        let hs = [
            h("BTC", 4.0, 100.0),
            h("ETH", 2.4, 100.0),
            h("SOL", 1.6, 100.0),
        ];
        let p = plan(&hs, 200.0, &targets(), 0.20, 0.25, 1.0);
        assert!((p.total_value - 1000.0).abs() < 1e-9);
        assert!(
            p.max_drift < 1e-9,
            "at reserve target drift should be ~0, got {}",
            p.max_drift
        );
        assert!(!p.triggered);
    }

    #[test]
    fn a_deposit_pushes_assets_under_target_and_deploys() {
        // Balanced (80% invested) then a $500 USD deposit lands (cash 200→700).
        // Assets now under target → buy to restore the basket, keeping 20% cash.
        let hs = [
            h("BTC", 4.0, 100.0),
            h("ETH", 2.4, 100.0),
            h("SOL", 1.6, 100.0),
        ];
        let p = plan(&hs, 700.0, &targets(), 0.20, 0.25, 1.0);
        assert!(p.triggered);
        assert!(p.trades.iter().all(|t| t.side == Side::Buy));
        // new total 1500, invest 1200, BTC target 600, holds 400 → buy $200.
        let btc = p.trades.iter().find(|t| t.name == "BTC").unwrap();
        assert!((btc.usd - 200.0).abs() < 1e-6, "BTC buy {}", btc.usd);
    }

    #[test]
    fn overweight_asset_sells_before_buys() {
        let hs = [
            h("BTC", 8.0, 100.0),
            h("ETH", 1.0, 100.0),
            h("SOL", 1.0, 100.0),
        ];
        let p = plan(&hs, 0.0, &targets(), 0.0, 0.25, 1.0);
        assert!(p.triggered);
        assert_eq!(p.trades[0].side, Side::Sell);
        assert_eq!(p.trades[0].name, "BTC");
        assert!(p.trades[1..].iter().all(|t| t.side == Side::Buy));
    }

    #[test]
    fn breached_target_just_below_min_is_seeded_not_skipped() {
        // The Crypto.com SOL wedge: a 20% target whose invested slice (~$4.98 on a
        // ~$33 book) lands just under the $5 floor and is held at zero. The old dust
        // filter skipped it forever; now a breached target is seeded at the floor.
        let targets = vec![
            Target {
                name: "BTC".into(),
                weight: 0.40,
            },
            Target {
                name: "ETH".into(),
                weight: 0.25,
            },
            Target {
                name: "SOL".into(),
                weight: 0.20,
            },
            Target {
                name: "CRO".into(),
                weight: 0.15,
            },
        ];
        // Prices = 1 so value == qty. Invested $24.90 → 9.96/6.225/3.735, SOL 0; the
        // undeployed SOL slice sits in cash (above the $8.30 reserve target).
        let hs = [
            h("BTC", 9.96, 1.0),
            h("ETH", 6.225, 1.0),
            h("CRO", 3.735, 1.0),
            h("SOL", 0.0, 1.0),
        ];
        let p = plan(&hs, 13.28, &targets, 0.25, 0.25, 5.0);
        assert!(p.triggered, "SOL at 0 vs its target → breached");
        let sol = p
            .trades
            .iter()
            .find(|t| t.name == "SOL")
            .expect("SOL must be seeded, not skipped");
        assert_eq!(sol.side, Side::Buy);
        assert!(
            (sol.usd - 5.0).abs() < 1e-9,
            "seeded at the $5 floor, got {}",
            sol.usd
        );
        // The on-target majors are left alone (their deltas are ~0, well under min).
        assert!(
            p.trades.iter().all(|t| t.name == "SOL"),
            "only the breached slice trades"
        );
    }

    #[test]
    fn breached_but_target_below_half_min_is_not_oversized() {
        // A target so small ($2) that seeding at the $5 floor would 2.5× overshoot →
        // leave it skipped rather than grossly distort the basket.
        let targets = vec![
            Target {
                name: "BIG".into(),
                weight: 0.9,
            },
            Target {
                name: "TINY".into(),
                weight: 0.1,
            },
        ];
        // total $20, reserve 0: BIG target $18 (on it), TINY target $2 (held 0).
        let hs = [h("BIG", 18.0, 1.0), h("TINY", 0.0, 1.0)];
        let p = plan(&hs, 2.0, &targets, 0.0, 0.25, 5.0);
        assert!(p.triggered, "TINY at 0 vs $2 target → breached");
        assert!(
            p.trades.iter().all(|t| t.name != "TINY"),
            "a $2 target must not be oversized up to the $5 floor"
        );
    }
}
