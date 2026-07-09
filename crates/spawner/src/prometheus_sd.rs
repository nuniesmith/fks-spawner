// =============================================================================
// prometheus_sd.rs — Prometheus file-based service discovery (file_sd_configs)
//
// After every spawn/stop, this module rewrites /prometheus-sd/bots.json
// in the Prometheus SD format so Prometheus automatically picks up new bots
// without a config reload.
//
// Each bot container must expose port 9091 with a /metrics endpoint.
// The bot_metrics_port is configurable via BOT_METRICS_PORT env var.
//
// SD file format:
//   [
//     {
//       "targets": ["container_name:9091"],
//       "labels": {
//         "job": "fks-bots",
//         "bot_id": "abc-123",
//         "mode": "paper",
//         "__meta_fks_image": "fks-bot-arbitrage:latest"
//       }
//     }
//   ]
// =============================================================================

use serde::Serialize;
use std::collections::HashMap;
use std::{path::Path, sync::Arc};
use tokio::fs;
use tracing::{debug, info, warn};

use crate::{config::Config, docker_client::DockerOps, error::SpawnerResult};

#[derive(Debug, Serialize)]
struct SdTarget {
    targets: Vec<String>,
    labels: HashMap<String, String>,
}

/// Rewrite the Prometheus SD JSON file from the live list of bot containers.
/// Call this after any spawn/stop/remove operation.
pub async fn update_sd_file(docker: &dyn DockerOps, config: &Arc<Config>) {
    match build_sd_targets(docker, config).await {
        Ok(targets) => {
            if let Err(e) = write_sd_file(&config.prometheus_sd_path, &targets).await {
                warn!(error = %e, path = %config.prometheus_sd_path, "failed to write Prometheus SD file");
            } else {
                info!(
                    path = %config.prometheus_sd_path,
                    targets = targets.len(),
                    "Prometheus SD file updated"
                );
            }
        }
        Err(e) => warn!(error = %e, "failed to list bots for SD file update"),
    }
}

async fn build_sd_targets(
    docker: &dyn DockerOps,
    config: &Arc<Config>,
) -> SpawnerResult<Vec<SdTarget>> {
    let bots = docker.list_bots().await?;
    let running: Vec<_> = bots.into_iter().filter(|b| b.state == "running").collect();

    debug!(
        count = running.len(),
        "building SD targets for running bots"
    );

    let targets = running
        .into_iter()
        .map(|bot| {
            let target = format!("{}:{}", bot.name, config.bot_metrics_port);
            let mut labels = HashMap::new();
            labels.insert("job".to_string(), "fks-bots".to_string());
            labels.insert("bot_id".to_string(), bot.bot_id.clone());
            labels.insert("mode".to_string(), bot.mode.clone());
            labels.insert("container_name".to_string(), bot.name.clone());
            labels.insert("__meta_fks_image".to_string(), bot.image.clone());
            SdTarget {
                targets: vec![target],
                labels,
            }
        })
        .collect();

    Ok(targets)
}

async fn write_sd_file(path: &str, targets: &[SdTarget]) -> SpawnerResult<()> {
    // Ensure the parent directory exists.
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent).await?;
    }

    let json = serde_json::to_string_pretty(targets)?;

    // Write atomically: write to a temp file then rename.
    let tmp_path = format!("{}.tmp", path);
    fs::write(&tmp_path, &json).await?;
    fs::rename(&tmp_path, path).await?;

    Ok(())
}
