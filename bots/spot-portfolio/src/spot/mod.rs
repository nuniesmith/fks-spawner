//! Multi-exchange spot **portfolio** engine: %-target allocations plus a
//! stablecoin/fiat **cash reserve**, rebalanced on drift (and, later, on new
//! deposits), across any venue that implements [`exchange::SpotExchange`].
//!
//! Built to generalize the standalone Kraken rebalancer: the pure
//! [`rebalance`] logic is exchange-agnostic; each venue is wrapped in a thin
//! adapter behind the [`exchange::SpotExchange`] trait. Roadmap: Kraken first,
//! then Crypto.com, then any other `exchange-apiws` spot venue.

pub mod backtest;
pub mod config;
pub mod cryptocom;
pub mod exchange;
pub mod kraken;
pub mod kucoin;
pub mod portfolio;
pub mod rebalance;
pub mod signals;
