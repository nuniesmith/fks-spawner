// =============================================================================
// docker_client.rs — Docker SDK wrapper for FKS Bot Spawner
//
// Wraps bollard to provide:
//   spawn()        — create + start a bot container with safety guards
//   stop()         — graceful stop
//   restart()      — restart
//   remove()       — force remove
//   inspect()      — container details as ContainerInfo
//   list_bots()    — all containers with fks.bot=true label
//   stream_logs()  — streaming log output (used for SSE endpoint)
//   auto_prune()   — remove exited bot containers older than threshold
// =============================================================================

use std::{collections::HashMap, pin::Pin, sync::Arc};

use async_trait::async_trait;
use bollard::{
    Docker,
    container::LogOutput,
    models::{ContainerCreateBody, EndpointSettings, HostConfig, NetworkingConfig},
    query_parameters::{
        CreateContainerOptionsBuilder, ListContainersOptionsBuilder, LogsOptionsBuilder,
        RemoveContainerOptionsBuilder, RestartContainerOptionsBuilder, StatsOptionsBuilder,
        StopContainerOptionsBuilder,
    },
};
use chrono::{DateTime, Datelike, Utc};
use futures_util::StreamExt;
use tokio_stream::Stream;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    config::Config,
    error::{SpawnerError, SpawnerResult},
    models::{ContainerInfo, ContainerStats, SpawnRequest, SpawnResponse},
};

// ──────────────────────────────────────────────────────────────────────────────
// DockerOps — abstraction over the Docker daemon for handler dispatch
// ──────────────────────────────────────────────────────────────────────────────
//
// Trait surface mirrors the public API of [`DockerClient`] so handlers can
// depend on `Arc<dyn DockerOps>` instead of the concrete type. Tests inject
// a `MockDockerClient` that maintains in-memory state without a real
// daemon.
//
// `stream_logs` returns a boxed stream so the trait is object-safe; the
// concrete `DockerClient` impl wraps its `async_stream` in `Box::pin`.

/// Heap-allocated stream of log lines. Used by `stream_logs` so the trait
/// stays object-safe.
pub type LogStream = Pin<Box<dyn Stream<Item = String> + Send + 'static>>;

/// Operations the spawner's HTTP handlers perform against the Docker
/// daemon. Implemented by [`DockerClient`] for production and by a mock
/// in `tests/integration.rs` for handler-level tests.
#[async_trait]
pub trait DockerOps: Send + Sync + 'static {
    /// Create + start a new bot container.
    async fn spawn(&self, req: SpawnRequest) -> SpawnerResult<SpawnResponse>;

    /// Graceful stop with a 30s timeout.
    async fn stop(&self, id: &str) -> SpawnerResult<()>;

    /// Restart with a 10s graceful stop timeout.
    async fn restart(&self, id: &str) -> SpawnerResult<()>;

    /// Force remove (works on running containers too).
    async fn remove(&self, id: &str) -> SpawnerResult<()>;

    /// Inspect a container; `Err(SpawnerError::NotFound)` if missing.
    async fn inspect(&self, id: &str) -> SpawnerResult<ContainerInfo>;

    /// All containers carrying the `fks.bot=true` label.
    async fn list_bots(&self) -> SpawnerResult<Vec<ContainerInfo>>;

    /// One-shot live resource stats (CPU% + memory) for a single container.
    async fn stats(&self, id: &str) -> SpawnerResult<ContainerStats>;

    /// Returns a stream of log lines for `id`, optionally tailing the
    /// last `tail` lines first.
    fn stream_logs(&self, id: &str, tail: Option<String>) -> LogStream;

    /// Remove exited/dead bot containers older than the configured
    /// `prune_after_secs`. Returns the number removed.
    async fn auto_prune(&self) -> SpawnerResult<usize>;
}

// ─────────────────────────────────────────────────────────────────────────────
// DockerClient
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct DockerClient {
    docker: Docker,
    config: Arc<Config>,
}

/// Max number of caller-supplied env vars / labels accepted on a spawn request.
const MAX_SPAWN_ENV_VARS: usize = 100;
const MAX_SPAWN_LABELS: usize = 50;

/// Validate a caller-supplied identifier (`bot_id`, `mode`) that ends up in the
/// container name and labels. Allows the Docker container-name charset
/// (`[A-Za-z0-9._-]`, 1..=max_len) so a token holder can't inject control
/// characters, forge a colliding container name, or smuggle separators into a
/// label query.
fn validate_identifier(value: &str, field: &str, max_len: usize) -> SpawnerResult<()> {
    if value.is_empty() || value.len() > max_len {
        return Err(SpawnerError::InvalidRequest(format!(
            "{field} must be 1..={max_len} characters (got {})",
            value.len()
        )));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err(SpawnerError::InvalidRequest(format!(
            "{field} may only contain ASCII letters, digits, '.', '_' or '-'"
        )));
    }
    Ok(())
}

/// Count the containers that occupy a slot under `MAX_CONCURRENT_BOTS`: only
/// RUNNING ones. Exited/dead one-shot containers (finished backtests waiting
/// for auto_prune) must NOT hold cap slots — counting them let a burst of
/// completed `bt-*` containers wedge every subsequent spawn until the prune
/// sweep caught up. The cap is documented as a bound on *simultaneously
/// running* bots (config.rs, CLAUDE.md); this is the single definition of
/// "running" the cap uses, shared by [`DockerClient::spawn`] and the
/// pre-insert cap check in `api::run_edge_backtest_handler`. Kept pure so
/// it's unit-tested without a Docker daemon.
pub fn count_running_bots(bots: &[ContainerInfo]) -> usize {
    bots.iter().filter(|b| b.state == "running").count()
}

/// Remove any existing bot container named `container_name` so a fresh one can
/// take that identity — with NO window where two containers share it. The
/// atomic core of `POST /configs/{name}/respawn`:
///
///   1. inspect by name; if it doesn't exist, return `Ok(None)` (respawn stays
///      idempotent when the bot was never started / was already stopped+removed);
///   2. graceful stop (best-effort — an already-exited container isn't an
///      error; the forced remove below finishes the job regardless);
///   3. force-remove, AWAITED — this MUST complete before the caller spawns, so
///      there is never a moment with two live containers and never a silently
///      skipped removal. A remove failure propagates (the caller must NOT
///      spawn).
///
/// Reuses the same `DockerOps::stop` / `DockerOps::remove` paths that
/// `POST /container/{id}/stop` and `DELETE /container/{id}` use — no Docker
/// calls are reimplemented. Returns the removed container's id (for the respawn
/// response) or `None` when nothing was there.
pub async fn remove_existing_bot(
    docker: &dyn DockerOps,
    container_name: &str,
) -> SpawnerResult<Option<String>> {
    match docker.inspect(container_name).await {
        Ok(info) => {
            // Graceful SIGTERM + the existing 30s timeout. Best-effort: a
            // container that is already stopped (or races to exit) must not
            // block the forced removal that actually guarantees it is gone.
            if let Err(e) = docker.stop(container_name).await {
                warn!(
                    container = %container_name,
                    error = %e,
                    "graceful stop before respawn failed; forcing removal anyway"
                );
            }
            // Force-remove MUST complete before the caller spawns.
            docker.remove(container_name).await?;
            Ok(Some(info.id))
        }
        // No such container — nothing to tear down; respawn proceeds cleanly.
        Err(SpawnerError::NotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Validate the operator-controlled fields of a spawn request before any Docker
/// call: identifier charset (`bot_id`/`mode`), resource-limit bounds, and
/// env/label cardinality. The image-prefix + concurrency guards stay in
/// [`DockerClient::spawn`]; this rejects malformed or abusive *input* — forged
/// names, absurd CPU/RAM requests that could starve the host, oversized
/// env/label maps. Kept pure so it's unit-tested without a Docker daemon.
fn validate_spawn_request(req: &SpawnRequest, max_cpu: f64, max_mem_mb: i64) -> SpawnerResult<()> {
    // An empty/omitted bot_id is replaced by a generated UUID in `spawn`, so
    // only validate a caller-supplied non-empty one.
    if let Some(bot_id) = req.bot_id.as_deref()
        && !bot_id.is_empty()
    {
        validate_identifier(bot_id, "bot_id", 64)?;
    }
    validate_identifier(&req.mode, "mode", 32)?;

    if let Some(cpu) = req.cpu_limit
        && !(cpu.is_finite() && cpu > 0.0 && cpu <= max_cpu)
    {
        return Err(SpawnerError::InvalidRequest(format!(
            "cpu_limit must be in (0, {max_cpu}] cores (got {cpu})"
        )));
    }
    if let Some(mem) = req.memory_limit_mb
        && !(mem > 0 && mem <= max_mem_mb)
    {
        return Err(SpawnerError::InvalidRequest(format!(
            "memory_limit_mb must be in 1..={max_mem_mb} (got {mem})"
        )));
    }
    if req.env.len() > MAX_SPAWN_ENV_VARS {
        return Err(SpawnerError::InvalidRequest(format!(
            "too many env vars ({} > {MAX_SPAWN_ENV_VARS})",
            req.env.len()
        )));
    }
    if req.labels.len() > MAX_SPAWN_LABELS {
        return Err(SpawnerError::InvalidRequest(format!(
            "too many labels ({} > {MAX_SPAWN_LABELS})",
            req.labels.len()
        )));
    }
    Ok(())
}

impl DockerClient {
    /// Connect to the Docker daemon via the Unix socket (default path).
    pub fn new(config: Arc<Config>) -> SpawnerResult<Self> {
        let docker = Docker::connect_with_unix_defaults().map_err(SpawnerError::Docker)?;
        Ok(Self { docker, config })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // spawn — create + start a bot container
    // ─────────────────────────────────────────────────────────────────────────

    pub async fn spawn(&self, req: SpawnRequest) -> SpawnerResult<SpawnResponse> {
        // ── Safety guard: image prefix ────────────────────────────────────────
        if !req.image.starts_with(&self.config.allowed_image_prefix) {
            return Err(SpawnerError::InvalidImage(req.image));
        }

        // ── Safety guard: validate operator-controlled input ──────────────────
        validate_spawn_request(&req, self.config.max_cpu_limit, self.config.max_memory_mb)?;

        // ── Safety guard: concurrent bot cap ──────────────────────────────────
        // Only RUNNING containers occupy slots (list_bots returns ALL states,
        // including exited one-shot backtests awaiting auto_prune — see
        // count_running_bots).
        let running = count_running_bots(&self.list_bots().await?);
        if running >= self.config.max_concurrent_bots {
            return Err(SpawnerError::TooManyBots(running));
        }

        // ── Bot identity ──────────────────────────────────────────────────────
        let bot_id = req
            .bot_id
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let container_name = format!("fks-bot-{}", bot_id);
        let now = Utc::now();

        // ── Labels ────────────────────────────────────────────────────────────
        let mut labels: HashMap<String, String> = req.labels.clone();
        labels.insert("fks.bot".into(), "true".into());
        labels.insert("fks.mode".into(), req.mode.clone());
        labels.insert("fks.bot_id".into(), bot_id.clone());
        labels.insert("fks.created_by".into(), "spawner".into());
        labels.insert("fks.created_at".into(), now.to_rfc3339());
        labels.insert("fks.image".into(), req.image.clone());

        // ── Environment ───────────────────────────────────────────────────────
        let env: Vec<String> = req
            .env
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .chain(std::iter::once(format!("FKS_BOT_ID={}", bot_id)))
            .chain(std::iter::once(format!("FKS_BOT_MODE={}", req.mode)))
            .collect();

        // ── Resource limits ───────────────────────────────────────────────────
        let cpu_quota = req.cpu_limit.unwrap_or(self.config.default_cpu_limit);
        // Docker CPU quota: period=100_000µs, quota = cores × period
        let cpu_quota_us = (cpu_quota * 100_000.0) as i64;

        let memory_bytes = req
            .memory_limit_mb
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(self.config.default_memory_bytes);

        // ── Networking ────────────────────────────────────────────────────────
        let mut endpoints: HashMap<String, EndpointSettings> = HashMap::new();
        endpoints.insert(
            self.config.allowed_network.clone(),
            EndpointSettings::default(),
        );

        // ── Host config (hardened — see build_bot_host_config) ─────────────────
        let host_config =
            build_bot_host_config(memory_bytes, cpu_quota_us, self.config.default_cpu_shares);

        let networking_config = NetworkingConfig {
            endpoints_config: Some(endpoints),
        };

        // ── Container config ──────────────────────────────────────────────────
        let mut container_cfg = ContainerCreateBody {
            image: Some(req.image.clone()),
            env: Some(env),
            labels: Some(labels.clone()),
            host_config: Some(host_config),
            networking_config: Some(networking_config),
            ..Default::default()
        };

        if let Some(cmd) = req.cmd {
            container_cfg.cmd = Some(cmd);
        }
        if let Some(ep) = req.entrypoint {
            container_cfg.entrypoint = Some(ep);
        }

        // ── Create ────────────────────────────────────────────────────────────
        info!(
            container_name = %container_name,
            image = %req.image,
            mode = %req.mode,
            bot_id = %bot_id,
            "spawning bot container"
        );

        let create_opts = CreateContainerOptionsBuilder::new()
            .name(&container_name)
            .build();

        let created = self
            .docker
            .create_container(Some(create_opts), container_cfg)
            .await
            .map_err(SpawnerError::Docker)?;

        let container_id = created.id.clone();

        // ── Start ─────────────────────────────────────────────────────────────
        self.docker
            .start_container(&container_id, None::<bollard::query_parameters::StartContainerOptions>)
            .await
            .map_err(|e| {
                warn!(container_id = %container_id, error = %e, "failed to start container — will try to remove");
                SpawnerError::Docker(e)
            })?;

        info!(container_id = %&container_id[..12], "bot container started");

        Ok(SpawnResponse {
            container_id: container_id[..12].to_string(),
            container_name,
            bot_id,
            image: req.image,
            mode: req.mode,
            started_at: Utc::now(),
        })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // stop
    // ─────────────────────────────────────────────────────────────────────────

    pub async fn stop(&self, id: &str) -> SpawnerResult<()> {
        debug!(container = %id, "stopping bot container");
        self.docker
            .stop_container(id, Some(StopContainerOptionsBuilder::new().t(30).build()))
            .await
            .map_err(SpawnerError::Docker)?;
        info!(container = %id, "bot container stopped");
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // restart
    // ─────────────────────────────────────────────────────────────────────────

    pub async fn restart(&self, id: &str) -> SpawnerResult<()> {
        debug!(container = %id, "restarting bot container");
        self.docker
            .restart_container(
                id,
                Some(RestartContainerOptionsBuilder::new().t(10).build()),
            )
            .await
            .map_err(SpawnerError::Docker)?;
        info!(container = %id, "bot container restarted");
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // remove
    // ─────────────────────────────────────────────────────────────────────────

    pub async fn remove(&self, id: &str) -> SpawnerResult<()> {
        debug!(container = %id, "removing bot container");
        self.docker
            .remove_container(
                id,
                Some(RemoveContainerOptionsBuilder::new().force(true).build()),
            )
            .await
            .map_err(SpawnerError::Docker)?;
        info!(container = %id, "bot container removed");
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // status / inspect
    // ─────────────────────────────────────────────────────────────────────────

    pub async fn inspect(&self, id: &str) -> SpawnerResult<ContainerInfo> {
        let data = self
            .docker
            .inspect_container(
                id,
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await
            .map_err(|e| match e {
                bollard::errors::Error::DockerResponseServerError {
                    status_code: 404, ..
                } => SpawnerError::NotFound(id.to_string()),
                other => SpawnerError::Docker(other),
            })?;

        Ok(container_info_from_inspect(data))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // list_bots — all containers with the fks.bot=true label
    // ─────────────────────────────────────────────────────────────────────────

    pub async fn list_bots(&self) -> SpawnerResult<Vec<ContainerInfo>> {
        let filters: HashMap<String, Vec<String>> =
            HashMap::from([("label".to_string(), vec!["fks.bot=true".to_string()])]);

        let opts = ListContainersOptionsBuilder::new()
            .all(true)
            .filters(&filters)
            .build();

        let summaries = self
            .docker
            .list_containers(Some(opts))
            .await
            .map_err(SpawnerError::Docker)?;

        let infos = summaries
            .into_iter()
            .map(container_info_from_summary)
            .collect();

        Ok(infos)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // stats — one-shot CPU + memory usage for a single container
    // ─────────────────────────────────────────────────────────────────────────

    pub async fn stats(&self, id: &str) -> SpawnerResult<ContainerStats> {
        // stream=false + one_shot=false ⇒ Docker collects two samples ~1s apart
        // and returns a single reading with `precpu_stats` populated, which is
        // what the CPU-percent delta needs.
        let opts = StatsOptionsBuilder::new()
            .stream(false)
            .one_shot(false)
            .build();

        let mut stream = self.docker.stats(id, Some(opts));
        match stream.next().await {
            Some(Ok(resp)) => Ok(stats_from_response(resp)),
            Some(Err(e)) => Err(match e {
                bollard::errors::Error::DockerResponseServerError {
                    status_code: 404, ..
                } => SpawnerError::NotFound(id.to_string()),
                other => SpawnerError::Docker(other),
            }),
            None => Err(SpawnerError::Other(format!("no stats returned for {id}"))),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // stream_logs — returns an async Stream of log line strings
    // ─────────────────────────────────────────────────────────────────────────

    pub fn stream_logs(
        &self,
        id: &str,
        tail: Option<String>,
    ) -> impl Stream<Item = String> + 'static + use<> {
        let docker = self.docker.clone();
        let id = id.to_string();
        let tail_str = tail.unwrap_or_else(|| "100".to_string());

        async_stream::stream! {
            let opts = LogsOptionsBuilder::new()
                .follow(true)
                .stdout(true)
                .stderr(true)
                .timestamps(true)
                .tail(&tail_str)
                .build();

            let mut log_stream = docker.logs(&id, Some(opts));

            while let Some(item) = log_stream.next().await {
                match item {
                    Ok(LogOutput::StdOut { message }) |
                    Ok(LogOutput::StdErr { message }) => {
                        let line = String::from_utf8_lossy(&message).to_string();
                        yield line;
                    }
                    Ok(LogOutput::Console { message }) => {
                        let line = String::from_utf8_lossy(&message).to_string();
                        yield line;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!(container = %id, error = %e, "log stream error");
                        break;
                    }
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // auto_prune — remove exited bot containers older than `prune_after_secs`
    // ─────────────────────────────────────────────────────────────────────────

    pub async fn auto_prune(&self) -> SpawnerResult<usize> {
        let filters: HashMap<String, Vec<String>> = HashMap::from([
            ("label".to_string(), vec!["fks.bot=true".to_string()]),
            (
                "status".to_string(),
                vec!["exited".to_string(), "dead".to_string()],
            ),
        ]);

        let opts = ListContainersOptionsBuilder::new()
            .all(true)
            .filters(&filters)
            .build();
        let stopped = self
            .docker
            .list_containers(Some(opts))
            .await
            .map_err(SpawnerError::Docker)?;

        let threshold = chrono::Duration::seconds(self.config.prune_after_secs);
        let cutoff = Utc::now() - threshold;

        let mut pruned = 0usize;

        for c in stopped {
            let id = c.id.as_deref().unwrap_or("");
            if id.is_empty() {
                continue;
            }

            // Use the Created timestamp from the summary as a proxy for
            // "finished_at" (good enough for prune purposes).
            let created_ts = c.created.unwrap_or(0);
            let created_at = DateTime::from_timestamp(created_ts, 0).unwrap_or(Utc::now());

            if created_at < cutoff {
                match self.remove(id).await {
                    Ok(_) => {
                        info!(container = %&id[..12.min(id.len())], "auto-pruned stopped bot container");
                        pruned += 1;
                    }
                    Err(e) => warn!(container = %id, error = %e, "auto-prune remove failed"),
                }
            }
        }

        if pruned > 0 {
            info!(count = pruned, "auto-prune complete");
        }

        Ok(pruned)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// DockerOps trait impl — thin delegating wrapper around the inherent
// methods so handlers can dispatch via `Arc<dyn DockerOps>`.
// ──────────────────────────────────────────────────────────────────────────────

#[async_trait]
impl DockerOps for DockerClient {
    async fn spawn(&self, req: SpawnRequest) -> SpawnerResult<SpawnResponse> {
        DockerClient::spawn(self, req).await
    }

    async fn stop(&self, id: &str) -> SpawnerResult<()> {
        DockerClient::stop(self, id).await
    }

    async fn restart(&self, id: &str) -> SpawnerResult<()> {
        DockerClient::restart(self, id).await
    }

    async fn remove(&self, id: &str) -> SpawnerResult<()> {
        DockerClient::remove(self, id).await
    }

    async fn inspect(&self, id: &str) -> SpawnerResult<ContainerInfo> {
        DockerClient::inspect(self, id).await
    }

    async fn list_bots(&self) -> SpawnerResult<Vec<ContainerInfo>> {
        DockerClient::list_bots(self).await
    }

    async fn stats(&self, id: &str) -> SpawnerResult<ContainerStats> {
        DockerClient::stats(self, id).await
    }

    fn stream_logs(&self, id: &str, tail: Option<String>) -> LogStream {
        Box::pin(DockerClient::stream_logs(self, id, tail))
    }

    async fn auto_prune(&self) -> SpawnerResult<usize> {
        DockerClient::auto_prune(self).await
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers — convert bollard types to ContainerInfo
// ──────────────────────────────────────────────────────────────────────────────

fn container_info_from_summary(s: bollard::models::ContainerSummary) -> ContainerInfo {
    let id_full = s.id.clone().unwrap_or_default();
    let id = id_full[..12.min(id_full.len())].to_string();

    let name = s
        .names
        .as_ref()
        .and_then(|n| n.first())
        .map(|n| n.trim_start_matches('/').to_string())
        .unwrap_or_else(|| id.clone());

    let labels = s.labels.clone().unwrap_or_default();
    let bot_id = labels.get("fks.bot_id").cloned().unwrap_or_default();
    let mode = labels.get("fks.mode").cloned().unwrap_or_default();

    let created_at = s.created.and_then(|ts| DateTime::from_timestamp(ts, 0));

    ContainerInfo {
        id,
        id_full,
        name,
        image: s.image.clone().unwrap_or_default(),
        status: s.status.clone().unwrap_or_default(),
        state: s.state.as_ref().map(|e| e.to_string()).unwrap_or_default(),
        bot_id,
        mode,
        created_at,
        started_at: None,
        finished_at: None,
        labels,
        cpu_percent: None,
        memory_bytes: None,
        memory_limit_bytes: None,
    }
}

fn container_info_from_inspect(d: bollard::models::ContainerInspectResponse) -> ContainerInfo {
    let id_full = d.id.clone().unwrap_or_default();
    let id = id_full[..12.min(id_full.len())].to_string();

    let name = d
        .name
        .as_ref()
        .map(|n| n.trim_start_matches('/').to_string())
        .unwrap_or_else(|| id.clone());

    let labels = d
        .config
        .as_ref()
        .and_then(|c| c.labels.clone())
        .unwrap_or_default();

    let bot_id = labels.get("fks.bot_id").cloned().unwrap_or_default();
    let mode = labels.get("fks.mode").cloned().unwrap_or_default();

    let state = d.state.as_ref();
    let status = state
        .and_then(|s| s.status.as_ref())
        .map(|s| s.to_string())
        .unwrap_or_default();
    let state_str = status.clone();

    let parse_dt = |s: Option<&String>| -> Option<DateTime<Utc>> {
        s.and_then(|ts| {
            // Docker returns times like "0001-01-01T00:00:00Z" for "never"
            let dt = DateTime::parse_from_rfc3339(ts).ok()?;
            let utc = dt.with_timezone(&Utc);
            if utc.year() < 2000 { None } else { Some(utc) }
        })
    };

    let started_at = state.and_then(|s| parse_dt(s.started_at.as_ref()));
    let finished_at = state.and_then(|s| parse_dt(s.finished_at.as_ref()));

    let created_at = d
        .created
        .as_ref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc));

    ContainerInfo {
        id,
        id_full,
        name,
        image: d
            .config
            .as_ref()
            .and_then(|c| c.image.clone())
            .unwrap_or_default(),
        status,
        state: state_str,
        bot_id,
        mode,
        created_at,
        started_at,
        finished_at,
        labels,
        cpu_percent: None,
        memory_bytes: None,
        memory_limit_bytes: None,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Stats math — pure helpers (unit-tested; no daemon required)
// ──────────────────────────────────────────────────────────────────────────────

/// Docker's CPU-percent formula:
///   (cpu_delta / system_delta) × online_cpus × 100
/// Returns `None` when the deltas aren't usable (zero system delta, no online
/// cpus, or a counter reset where the previous reading exceeds the current).
fn compute_cpu_percent(
    cpu_total: u64,
    precpu_total: u64,
    system: u64,
    presystem: u64,
    online: u64,
) -> Option<f64> {
    let cpu_delta = cpu_total.checked_sub(precpu_total)? as f64;
    let system_delta = system.checked_sub(presystem)? as f64;
    if system_delta <= 0.0 || online == 0 {
        return None;
    }
    Some((cpu_delta / system_delta) * online as f64 * 100.0)
}

/// Resident memory = total usage minus reclaimable page cache, saturating at 0.
fn mem_used_bytes(usage: u64, cache: u64) -> i64 {
    usage.saturating_sub(cache) as i64
}

/// Build the hardened `HostConfig` every spawned bot runs under.
///
/// Security contract (also stated in `crates/spawner/CLAUDE.md`): unconditional
/// `cap_drop: ALL` + `no-new-privileges:true`, swap disabled (`memory_swap ==
/// memory`, so a bot can't escape its RAM cap into swap), and the json-file log
/// driver capped at 50 MB × 3. Bots only need outbound TCP + a high (>1024)
/// metrics port — neither requires a Linux capability — so dropping every
/// capability is safe with no `cap_add`. Extracted as a pure fn so the posture
/// is unit-tested without a Docker daemon (the spawn path itself is exercised
/// via `MockDockerClient`, which doesn't build a `HostConfig`).
fn build_bot_host_config(memory_bytes: i64, cpu_quota_us: i64, cpu_shares: i64) -> HostConfig {
    HostConfig {
        memory: Some(memory_bytes),
        memory_swap: Some(memory_bytes), // disable swap
        cpu_period: Some(100_000),
        cpu_quota: Some(cpu_quota_us),
        cpu_shares: Some(cpu_shares),
        // Log config: json-file driver with 50 MB cap, 3 rotations
        log_config: Some(bollard::models::HostConfigLogConfig {
            typ: Some("json-file".to_string()),
            config: Some(HashMap::from([
                ("max-size".to_string(), "50m".to_string()),
                ("max-file".to_string(), "3".to_string()),
            ])),
        }),
        // Security: drop ALL Linux capabilities + block privilege escalation.
        cap_drop: Some(vec!["ALL".to_string()]),
        security_opt: Some(vec!["no-new-privileges:true".to_string()]),
        ..Default::default()
    }
}

/// Reduce a Docker stats frame to the CPU%/memory figures we surface.
fn stats_from_response(r: bollard::models::ContainerStatsResponse) -> ContainerStats {
    let cpu = r.cpu_stats.as_ref();
    let precpu = r.precpu_stats.as_ref();

    let cpu_total = cpu
        .and_then(|c| c.cpu_usage.as_ref())
        .and_then(|u| u.total_usage);
    let precpu_total = precpu
        .and_then(|c| c.cpu_usage.as_ref())
        .and_then(|u| u.total_usage);
    let system = cpu.and_then(|c| c.system_cpu_usage);
    let presystem = precpu.and_then(|c| c.system_cpu_usage);
    let online = cpu
        .and_then(|c| c.online_cpus)
        .map(u64::from)
        .or_else(|| {
            cpu.and_then(|c| c.cpu_usage.as_ref())
                .and_then(|u| u.percpu_usage.as_ref())
                .map(|v| v.len() as u64)
        })
        .unwrap_or(0);

    let cpu_percent = match (cpu_total, precpu_total, system, presystem) {
        (Some(ct), Some(pt), Some(s), Some(ps)) => compute_cpu_percent(ct, pt, s, ps, online),
        _ => None,
    };

    let mem = r.memory_stats.as_ref();
    // cgroup v2 exposes reclaimable cache as `inactive_file`; v1 as `cache`.
    let cache = mem
        .and_then(|m| m.stats.as_ref())
        .and_then(|s| s.get("inactive_file").or_else(|| s.get("cache")).copied())
        .unwrap_or(0);
    let memory_bytes = mem
        .and_then(|m| m.usage)
        .map(|usage| mem_used_bytes(usage, cache));
    let memory_limit_bytes = mem.and_then(|m| m.limit).map(|l| l as i64);

    ContainerStats {
        cpu_percent,
        memory_bytes,
        memory_limit_bytes,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests — exercise the pure stats math (the daemon-dependent path is covered by
// the MockDockerClient in tests/integration.rs).
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::{
        ContainerInfo, SpawnRequest, SpawnerError, build_bot_host_config, compute_cpu_percent,
        count_running_bots, mem_used_bytes, validate_spawn_request,
    };

    /// Minimal ContainerInfo in the given Docker state, for cap-count tests.
    fn bot_in_state(state: &str) -> ContainerInfo {
        ContainerInfo {
            id: "abc123def456".to_string(),
            id_full: "abc123def456".to_string(),
            name: "fks-bot-x".to_string(),
            image: "fks-bot-example:latest".to_string(),
            status: String::new(),
            state: state.to_string(),
            bot_id: "x".to_string(),
            mode: "paper".to_string(),
            created_at: None,
            started_at: None,
            finished_at: None,
            labels: std::collections::HashMap::new(),
            cpu_percent: None,
            memory_bytes: None,
            memory_limit_bytes: None,
        }
    }

    #[test]
    fn cap_count_ignores_exited_and_dead_containers() {
        // The MAX_CONCURRENT_BOTS regression: list_bots(.all(true)) returns
        // exited one-shot backtest containers too, and counting them let
        // finished bt-* runs wedge every spawn until auto_prune caught up.
        // Only "running" occupies a slot.
        let bots: Vec<ContainerInfo> = ["running", "exited", "exited", "dead", "created", "paused"]
            .iter()
            .map(|s| bot_in_state(s))
            .collect();
        assert_eq!(count_running_bots(&bots), 1);
    }

    #[test]
    fn cap_count_all_running_counts_all() {
        let bots: Vec<ContainerInfo> = (0..3).map(|_| bot_in_state("running")).collect();
        assert_eq!(count_running_bots(&bots), 3);
    }

    #[test]
    fn cap_count_empty_is_zero() {
        assert_eq!(count_running_bots(&[]), 0);
    }

    #[test]
    fn cpu_percent_basic_delta() {
        // cpu_delta=100, system_delta=1000, 4 cpus → 100/1000 × 4 × 100 = 40%
        assert_eq!(compute_cpu_percent(1100, 1000, 5000, 4000, 4), Some(40.0));
    }

    #[test]
    fn cpu_percent_idle_is_zero() {
        // No CPU movement but a positive system delta → 0%, not None.
        assert_eq!(compute_cpu_percent(1000, 1000, 5000, 4000, 4), Some(0.0));
    }

    #[test]
    fn cpu_percent_zero_system_delta_is_none() {
        assert_eq!(compute_cpu_percent(1100, 1000, 4000, 4000, 4), None);
    }

    #[test]
    fn cpu_percent_no_online_cpus_is_none() {
        assert_eq!(compute_cpu_percent(1100, 1000, 5000, 4000, 0), None);
    }

    #[test]
    fn cpu_percent_counter_reset_is_none() {
        // Previous reading exceeds current (counter reset) → checked_sub fails.
        assert_eq!(compute_cpu_percent(900, 1000, 5000, 4000, 4), None);
    }

    #[test]
    fn mem_used_subtracts_cache() {
        assert_eq!(
            mem_used_bytes(100 * 1024 * 1024, 30 * 1024 * 1024),
            70 * 1024 * 1024
        );
    }

    #[test]
    fn mem_used_saturates_when_cache_exceeds_usage() {
        assert_eq!(mem_used_bytes(10, 50), 0);
    }

    #[test]
    fn bot_host_config_enforces_security_contract() {
        let hc = build_bot_host_config(512 * 1024 * 1024, 100_000, 1024);
        // The hardening that was missing: drop every Linux capability.
        assert_eq!(hc.cap_drop, Some(vec!["ALL".to_string()]));
        // No capabilities are added back — bots need none.
        assert!(
            hc.cap_add.is_none(),
            "bots must not be granted capabilities"
        );
        // Block setuid / privilege escalation.
        assert_eq!(
            hc.security_opt,
            Some(vec!["no-new-privileges:true".to_string()])
        );
        // Never privileged.
        assert_ne!(hc.privileged, Some(true));
        // Swap disabled (memory_swap == memory) so the RAM cap can't be escaped.
        assert_eq!(hc.memory, Some(512 * 1024 * 1024));
        assert_eq!(hc.memory_swap, hc.memory);
        // CPU limits propagate.
        assert_eq!(hc.cpu_quota, Some(100_000));
        assert_eq!(hc.cpu_shares, Some(1024));
    }

    fn valid_spawn_req() -> SpawnRequest {
        SpawnRequest {
            image: "fks-bot-example:latest".to_string(),
            bot_id: Some("bot-1".to_string()),
            mode: "paper".to_string(),
            env: std::collections::HashMap::new(),
            labels: std::collections::HashMap::new(),
            cpu_limit: None,
            memory_limit_mb: None,
            cmd: None,
            entrypoint: None,
            secrets: vec![],
        }
    }

    #[test]
    fn valid_spawn_request_passes() {
        assert!(validate_spawn_request(&valid_spawn_req(), 8.0, 16384).is_ok());
    }

    #[test]
    fn empty_or_absent_bot_id_is_allowed() {
        // Replaced by a generated UUID in spawn(), so validation skips it.
        let mut r = valid_spawn_req();
        r.bot_id = None;
        assert!(validate_spawn_request(&r, 8.0, 16384).is_ok());
        r.bot_id = Some(String::new());
        assert!(validate_spawn_request(&r, 8.0, 16384).is_ok());
    }

    #[test]
    fn rejects_bad_bot_id_and_mode() {
        let long = "x".repeat(65);
        for bad in ["bad id", "../etc", "a/b", "name;rm", "naïve", long.as_str()] {
            let mut r = valid_spawn_req();
            r.bot_id = Some(bad.to_string());
            assert!(
                matches!(
                    validate_spawn_request(&r, 8.0, 16384),
                    Err(SpawnerError::InvalidRequest(_))
                ),
                "bot_id {bad:?} should be rejected"
            );
        }
        let mut r = valid_spawn_req();
        r.mode = "paper trading!".to_string();
        assert!(matches!(
            validate_spawn_request(&r, 8.0, 16384),
            Err(SpawnerError::InvalidRequest(_))
        ));
    }

    #[test]
    fn rejects_out_of_range_resources() {
        for cpu in [0.0, -1.0, f64::NAN, f64::INFINITY, 9.0] {
            let mut r = valid_spawn_req();
            r.cpu_limit = Some(cpu);
            assert!(
                matches!(
                    validate_spawn_request(&r, 8.0, 16384),
                    Err(SpawnerError::InvalidRequest(_))
                ),
                "cpu_limit {cpu} should be rejected"
            );
        }
        for mem in [0_i64, -1, 16385, i64::MAX] {
            let mut r = valid_spawn_req();
            r.memory_limit_mb = Some(mem);
            assert!(
                matches!(
                    validate_spawn_request(&r, 8.0, 16384),
                    Err(SpawnerError::InvalidRequest(_))
                ),
                "memory_limit_mb {mem} should be rejected"
            );
        }
        // In-range values pass.
        let mut r = valid_spawn_req();
        r.cpu_limit = Some(2.0);
        r.memory_limit_mb = Some(2048);
        assert!(validate_spawn_request(&r, 8.0, 16384).is_ok());
    }

    #[test]
    fn rejects_oversized_env_and_labels() {
        let mut r = valid_spawn_req();
        r.env = (0..=super::MAX_SPAWN_ENV_VARS)
            .map(|i| (format!("K{i}"), "v".to_string()))
            .collect();
        assert!(matches!(
            validate_spawn_request(&r, 8.0, 16384),
            Err(SpawnerError::InvalidRequest(_))
        ));

        let mut r = valid_spawn_req();
        r.labels = (0..=super::MAX_SPAWN_LABELS)
            .map(|i| (format!("l{i}"), "v".to_string()))
            .collect();
        assert!(matches!(
            validate_spawn_request(&r, 8.0, 16384),
            Err(SpawnerError::InvalidRequest(_))
        ));
    }
}
