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
//         "bot_id": "abc-123",
//         "mode": "paper",
//         "container_name": "fks-bot-abc-123",
//         "__meta_fks_image": "fks-bot-arbitrage:latest"
//       }
//     }
//   ]
//
// NOTE: the SD file deliberately does NOT set a `job` label. A `job` label in
// file_sd overrides each scrape config's `job_name` for every job that reads
// the file — so injecting `job: "fks-bots"` here made the `fks-bots-risk` job's
// series ALSO carry `job="fks-bots"`, breaking arm-day `job="fks-bots-risk"`
// queries and mislabelling a :9092 exporter-bind failure as the :9091
// LiveBotMetricsDown alert (M6). With no `job` label, each job's series carry
// their own `job_name` (`fks-bots` / `fks-bots-risk`). Both jobs' relabel_configs
// consume only `__meta_fks_image`/`bot_id`/`mode`/`container_name`, never `job`.
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
        .iter()
        .map(|bot| sd_target(bot, config.bot_metrics_port))
        .collect();

    Ok(targets)
}

/// Build one Prometheus SD target from a running bot container. Emits the
/// metadata labels both scrape jobs relabel (`bot_id`/`mode`/`container_name`/
/// `__meta_fks_image`) and deliberately NO `job` label — see the module header.
fn sd_target(bot: &crate::models::ContainerInfo, metrics_port: u16) -> SdTarget {
    let mut labels = HashMap::new();
    labels.insert("bot_id".to_string(), bot.bot_id.clone());
    labels.insert("mode".to_string(), bot.mode.clone());
    labels.insert("container_name".to_string(), bot.name.clone());
    labels.insert("__meta_fks_image".to_string(), bot.image.clone());
    SdTarget {
        targets: vec![format!("{}:{}", bot.name, metrics_port)],
        labels,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ContainerInfo;

    fn bot(name: &str, bot_id: &str, mode: &str, image: &str) -> ContainerInfo {
        ContainerInfo {
            id: "short".into(),
            id_full: "full".into(),
            name: name.into(),
            image: image.into(),
            status: "Up 1m".into(),
            state: "running".into(),
            bot_id: bot_id.into(),
            mode: mode.into(),
            created_at: None,
            started_at: None,
            finished_at: None,
            labels: HashMap::new(),
            cpu_percent: None,
            memory_bytes: None,
            memory_limit_bytes: None,
            exit_code: None,
        }
    }

    #[test]
    fn sd_target_sets_no_job_label_so_each_scrape_job_keeps_its_own_job_name() {
        // M6: a `job` label in file_sd overrides BOTH scrape jobs' job_name — so
        // it must be absent, letting fks-bots / fks-bots-risk each carry their
        // own job label.
        let t = sd_target(
            &bot("fks-bot-abc", "abc-123", "live", "fks-bot-crypto-funding"),
            9091,
        );
        assert!(
            !t.labels.contains_key("job"),
            "the SD writer must NOT set a job label (it would override job_name)"
        );
    }

    #[test]
    fn sd_target_emits_the_relabel_metadata_and_target_address() {
        let t = sd_target(
            &bot(
                "fks-bot-abc",
                "abc-123",
                "paper",
                "fks-bot-arbitrage:latest",
            ),
            9091,
        );
        assert_eq!(t.targets, vec!["fks-bot-abc:9091".to_string()]);
        assert_eq!(t.labels.get("bot_id").unwrap(), "abc-123");
        assert_eq!(t.labels.get("mode").unwrap(), "paper");
        assert_eq!(t.labels.get("container_name").unwrap(), "fks-bot-abc");
        assert_eq!(
            t.labels.get("__meta_fks_image").unwrap(),
            "fks-bot-arbitrage:latest"
        );
        // Exactly the four metadata keys the prometheus relabel_configs consume.
        let mut keys: Vec<&str> = t.labels.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["__meta_fks_image", "bot_id", "container_name", "mode"]
        );
    }

    #[test]
    fn sd_target_honors_a_custom_metrics_port() {
        let t = sd_target(&bot("fks-bot-x", "x", "live", "img"), 9092);
        assert_eq!(t.targets, vec!["fks-bot-x:9092".to_string()]);
    }
}
