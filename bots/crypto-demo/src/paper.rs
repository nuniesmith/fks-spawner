//! Paper position book — turns the brain's signals into simulated round-trip
//! PnL so a long-running demo produces the full `fks_bot_*` metric series and
//! actually exercises the framework's risk layer.
//!
//! It runs as one background task that:
//!   1. Subscribes to the bot's `MarketDataBus` to track each symbol's latest
//!      close price (candles carry the price; signals don't).
//!   2. Subscribes to the `SignalBus`. On a confirmed Buy/Sell that flips the
//!      paper position, it realises PnL on the closed leg, records it to the
//!      Prometheus metrics (`metrics::record_trade`), and feeds it to the
//!      framework via `BotHandle::record_trade_outcome` so `SessionPnl` and the
//!      `CircuitBreaker` engage exactly as they would in live trading.
//!
//! This is paper-only bookkeeping (the `MockExchange` places no real orders),
//! deliberately simple: fixed notional, flat→long→short flips, mark-to-flip PnL.

use std::collections::HashMap;

use rustrade::{BotHandle, MarketDataBus, MarketDataEvent, SignalBus, SignalType, Symbol};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::metrics;

/// One open paper position: +1 long / -1 short / 0 flat, with entry price.
#[derive(Clone, Copy, Default)]
struct PaperPos {
    side: i8,
    entry: f64,
}

/// Notional (quote currency) per paper trade — PnL = notional × return.
const PAPER_NOTIONAL: f64 = 100.0;

/// Spawn the paper-PnL tracker. Returns immediately; the work runs until
/// `cancel` fires.
pub fn spawn(
    handle: BotHandle,
    market: MarketDataBus,
    signals: SignalBus,
    cancel: CancellationToken,
) {
    let mut md_rx = market.subscribe();
    let mut sig_rx = signals.subscribe();

    tokio::spawn(async move {
        let mut last_price: HashMap<String, f64> = HashMap::new();
        let mut positions: HashMap<String, PaperPos> = HashMap::new();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,

                // Track latest close per symbol from the candle stream.
                Ok(ev) = md_rx.recv() => {
                    if let MarketDataEvent::Candle { symbol, candle, .. } = &ev {
                        last_price.insert(symbol.0.clone(), candle.close);
                    }
                }

                // Turn confirmed Buy/Sell signals into paper position flips.
                Ok(sig) = sig_rx.recv() => {
                    let want = match sig.kind {
                        SignalType::Buy => 1i8,
                        SignalType::Sell => -1i8,
                        _ => continue, // Hold / Close: nothing to flip here
                    };
                    let Some(&price) = last_price.get(&sig.symbol) else {
                        continue; // no price seen yet
                    };

                    let pos = positions.entry(sig.symbol.clone()).or_default();
                    if pos.side == want {
                        continue; // already on this side
                    }

                    // Realise PnL on the leg we're closing (if any).
                    if pos.side != 0 && pos.entry > 0.0 {
                        let ret = (price - pos.entry) / pos.entry * f64::from(pos.side);
                        let pnl = ret * PAPER_NOTIONAL;

                        metrics::record_trade(pnl);
                        // Feed the framework's risk layer (SessionPnl + breaker).
                        // Fee modelled as 0 for the demo.
                        handle
                            .record_trade_outcome(&Symbol::from(sig.symbol.as_str()), pnl, 0.0)
                            .await;

                        info!(
                            target: "pnl",
                            symbol = %sig.symbol,
                            closed_side = pos.side,
                            entry = pos.entry,
                            exit = price,
                            pnl = format!("{pnl:.2}"),
                            "paper trade closed",
                        );
                    }

                    // Open the new leg at the current mark.
                    pos.side = want;
                    pos.entry = price;
                }
            }
        }

        info!("paper PnL tracker stopped");
    });
}
