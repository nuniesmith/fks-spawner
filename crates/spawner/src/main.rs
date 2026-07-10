// =============================================================================
// main.rs — FKS Bot Spawner entry point
//
// Starts the Axum HTTP server (default :8090) and a background task for
// periodic auto-prune of stopped/dead bot containers.
//
// Environment variables:
//   SPAWNER_HOST              bind address (default: 0.0.0.0)
//   SPAWNER_PORT              bind port    (default: 8090)
//   ALLOWED_IMAGE_PREFIX      image whitelist prefix (default: fks-bot-)
//   MAX_CONCURRENT_BOTS       hard cap on running bots (default: 20)
//   ALLOWED_NETWORK           Docker network for spawned containers (default: fks_network)
//   DEFAULT_CPU_LIMIT         fractional cores (default: 1.0)
//   DEFAULT_MEMORY_LIMIT_MB   memory cap in MiB (default: 512)
//   PROMETHEUS_SD_PATH        path for SD file (default: /prometheus-sd/bots.json)
//   BOT_METRICS_PORT          port bots expose /metrics on (default: 9091)
//   PRUNE_AFTER_SECS          seconds before a stopped bot is pruned (default: 300)
//   PRUNE_INTERVAL_SECS       seconds between prune sweeps (default: 60)
//   NET_WORTH_SAMPLE_INTERVAL_SECS  seconds between net-worth samples (default: 300; DB only)
//   BTC_WATCH_XPUB            account xpub for the cold-BTC watcher (read-only; derives addresses)
//   BTC_WATCH_ADDRESSES       comma-separated BTC addresses to watch (alternative/addition to xpub)
//   BTC_WATCH_GAP             receive+change addresses derived per branch (default: 20)
//   BTC_WATCH_INTERVAL_SECS   seconds between cold-BTC ticks (default: 3600; DB only)
//   BTC_WATCH_ACCOUNT_ID      account id for the cold-BTC snapshot rows (default: btc-cold)
//   ESPLORA_API_BASE          Esplora API base for balances (default: https://blockstream.info/api)
//   RITHMIC_SAMPLER_URL       rithmic-connector base URL for the balance sampler (read-only; off unless set)
//   RITHMIC_SAMPLE_INTERVAL_SECS  seconds between rithmic balance samples (default: 300; DB only)
//   RUST_LOG                  log level (default: info,spawner=debug)
// =============================================================================

use std::sync::Arc;
use std::time::Duration;

use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use spawner::api::{AppState, build_router};
use spawner::config::Config;
#[cfg(feature = "db")]
use spawner::db;
use spawner::docker_client::{DockerClient, DockerOps};
use spawner::{metrics, prometheus_sd};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Logging ───────────────────────────────────────────────────────────────
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,spawner=debug")),
        )
        .with(fmt::layer().with_target(true))
        .init();

    // ── Config ────────────────────────────────────────────────────────────────
    let config = Arc::new(Config::from_env());

    info!(
        host = %config.host,
        port = %config.port,
        allowed_image_prefix = %config.allowed_image_prefix,
        max_concurrent_bots = %config.max_concurrent_bots,
        allowed_network = %config.allowed_network,
        "FKS Bot Spawner starting"
    );

    // ── Docker client ─────────────────────────────────────────────────────────
    let docker: Arc<dyn DockerOps> = Arc::new(DockerClient::new(config.clone())?);
    info!("connected to Docker daemon");

    // ── Postgres (optional) ───────────────────────────────────────────────────
    #[cfg(feature = "db")]
    let store = {
        let s = db::BotRunStore::try_connect(&config.database_url).await;
        if let Some(s) = &s {
            // Probe schema; failure is logged inside the helper, not fatal.
            let _ = s.check_schema().await;
        }
        s
    };

    // ── Initial SD file write ──────────────────────────────────────────────────
    prometheus_sd::update_sd_file(docker.as_ref(), &config).await;

    // ── Background: auto-prune task ────────────────────────────────────────────────
    {
        let docker_prune: Arc<dyn DockerOps> = docker.clone();
        let config_prune = config.clone();
        // The prune sweep emits a best-effort bot_removed notification per
        // sweep (a count summary — auto_prune returns a count, not ids).
        #[cfg(feature = "db")]
        let store_prune = store.clone();
        tokio::spawn(async move {
            let interval = Duration::from_secs(config_prune.prune_interval_secs);
            loop {
                tokio::time::sleep(interval).await;
                match docker_prune.auto_prune().await {
                    Ok(n) if n > 0 => {
                        metrics::PRUNE_TOTAL.inc_by(n as f64);
                        prometheus_sd::update_sd_file(docker_prune.as_ref(), &config_prune).await;
                        // Notify configured channels (best-effort, detached).
                        #[cfg(feature = "db")]
                        if config_prune.notify_enabled
                            && let Some(store) = store_prune.clone()
                        {
                            use spawner::notifications::{
                                NotificationDispatcher, NotificationEvent,
                            };
                            tokio::spawn(async move {
                                NotificationDispatcher::new(store)
                                    .dispatch(NotificationEvent::pruned(n))
                                    .await;
                            });
                        }
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "auto-prune error"),
                }
            }
        });
    }

    // ── Background: net-worth sampler task ─────────────────────────────────────
    // Polls each running bot's /status endpoint on an interval and appends
    // net_worth_snapshots rows. DB-only (nothing to write to without Postgres)
    // and best-effort (a bot that doesn't expose net worth is skipped, never
    // fatal). See crate::net_worth for the contract.
    #[cfg(feature = "db")]
    if let Some(store_sampler) = store.clone() {
        let docker_sampler: Arc<dyn DockerOps> = docker.clone();
        let config_sampler = config.clone();
        tokio::spawn(async move {
            spawner::net_worth::run_sampler(docker_sampler, config_sampler, store_sampler).await;
        });
        info!(
            interval_secs = %config.net_worth_sample_interval_secs,
            "net-worth sampler started"
        );
    }

    // ── Background: cold-BTC on-chain watcher (read-only, source=onchain) ───────
    // Derives addresses from a public xpub and/or reads an explicit address
    // list, sums their confirmed on-chain balance via Esplora, prices BTC→USD
    // off Kraken, and writes one onchain net-worth snapshot per tick. DB-only,
    // best-effort, and OFF unless BTC_WATCH_XPUB/BTC_WATCH_ADDRESSES is set.
    // Holds no keys — it can only READ. See crate::btc_watch.
    #[cfg(feature = "db")]
    if config.btc_watch.enabled() {
        if let Some(store_btc) = store.clone() {
            let config_btc = config.clone();
            tokio::spawn(async move {
                spawner::btc_watch::run_watcher(config_btc, store_btc).await;
            });
            info!(
                interval_secs = config.btc_watch.interval_secs,
                gap = config.btc_watch.gap,
                has_xpub = config.btc_watch.xpub.is_some(),
                explicit_addresses = config.btc_watch.addresses.len(),
                "cold-BTC watcher ENABLED"
            );
        } else {
            info!(
                "cold-BTC watcher configured but DB is disabled — not starting (nothing to write to)"
            );
        }
    } else {
        info!(
            "cold-BTC watcher disabled (set BTC_WATCH_XPUB and/or BTC_WATCH_ADDRESSES to enable)"
        );
    }

    // ── Background: Rithmic account-balance sampler (read-only, source=rithmic) ─
    // Polls the rithmic-connector's read-only /positions endpoint for the
    // account balance and writes one rithmic net-worth snapshot per tick.
    // DB-only, best-effort, and OFF unless RITHMIC_SAMPLER_URL is set (the
    // connector is gated on live creds and usually down in dev). See
    // crate::rithmic_sampler.
    #[cfg(feature = "db")]
    if config.rithmic_sampler.enabled() {
        if let Some(store_rithmic) = store.clone() {
            let config_rithmic = config.rithmic_sampler.clone();
            tokio::spawn(async move {
                spawner::rithmic_sampler::run_sampler(config_rithmic, store_rithmic).await;
            });
            info!(
                interval_secs = config.rithmic_sampler.interval_secs,
                "rithmic balance sampler ENABLED"
            );
        } else {
            info!(
                "rithmic sampler configured but DB is disabled — not starting (nothing to write to)"
            );
        }
    } else {
        info!("rithmic balance sampler disabled (set RITHMIC_SAMPLER_URL to enable)");
    }

    // ── HTTP server ───────────────────────────────────────────────────────────
    #[cfg(feature = "db")]
    let state = AppState {
        docker,
        config: config.clone(),
        store,
    };
    #[cfg(not(feature = "db"))]
    let state = AppState {
        docker,
        config: config.clone(),
    };
    let app = build_router(state);
    let bind_addr = config.bind_addr();

    info!(addr = %bind_addr, "spawner HTTP server listening");

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
