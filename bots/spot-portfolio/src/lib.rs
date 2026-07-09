//! Multi-exchange spot **portfolio** engine.
//!
//! %-target allocations plus a stablecoin/fiat **cash reserve**, rebalanced on
//! drift across any venue that implements [`spot::exchange::SpotExchange`]
//! (Kraken → Crypto.com → …). The pure [`spot::rebalance`] logic is
//! exchange-agnostic; each venue is a thin adapter behind the trait.
//!
//! The generic bot plumbing (Discord alerts, JSONL journal, the FKS
//! status/metrics HTTP server) lives in the shared `crypto-bot-core` crate; this
//! crate holds only the spot edge. Consumed by the `spot-portfolio` and
//! `spot-optimize` binaries.

pub mod spot;
