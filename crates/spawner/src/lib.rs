// =============================================================================
// lib.rs — public surface of the `spawner` crate
//
// Originally the spawner was a pure-binary crate. We promote it to a
// hybrid `lib + bin` so that integration tests under `tests/` can:
//
//   use spawner::{api, auth, config, docker_client, models};
//
// `src/main.rs` shrinks to a thin shim that wires these modules into a
// runnable binary; everything testable lives here.
// =============================================================================

pub mod api;
pub mod auth;
pub mod btc_watch;
pub mod config;
#[cfg(feature = "db")]
pub mod db;
pub mod docker_client;
pub mod edge_decay;
pub mod edges;
pub mod error;
pub mod metrics;
pub mod models;
pub mod net_worth;
pub mod notifications;
pub mod prometheus_sd;
pub mod rithmic_sampler;
#[cfg(feature = "db")]
pub mod secrets_crypto;
pub mod treasury;
