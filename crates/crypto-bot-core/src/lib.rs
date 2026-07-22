//! Shared, edge-free scaffolding for the crypto trading bots.
//!
//! These modules are the generic bot infrastructure that BOTH the spot-portfolio
//! bot and the futures/funding edges import — extracted verbatim from the
//! original `crypto` repo, where both bots shared them via `use crate::…`:
//!
//! - [`alerts`]  — fire-and-forget Discord webhook notifications ([`alerts::Alerter`]).
//! - [`events`]  — fire-and-forget spawner `POST /events` ingest for platform
//!   `risk_halt` events ([`events::EventClient`]).
//! - [`journal`] — append-only JSONL trade journal ([`journal::Journal`]).
//! - [`status`]  — the FKS bot HTTP contract: `/health`, `/metrics`, `/status`,
//!   plus the process-global [`status::StatusState`] every update site pokes.
//!
//! There is deliberately no trading edge here — only the plumbing every bot
//! needs. The strategy logic lives in the consuming bot crates (`spot-portfolio`
//! in this repo; the futures/funding edges in the private `fks-state` repo).

pub mod alerts;
pub mod events;
pub mod journal;
pub mod status;
