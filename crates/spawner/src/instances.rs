//! Instances: a template, the values from its form and a name, saved as a
//! config and run as a container (EDGE_FACTORY_PLAN §4).
//!
//! An instance is everything the spawner needs to rebuild its container:
//! the saved config carries a [`TemplateRef`] (template id, version, the
//! checked form values), and every (re)spawn derives the image, env,
//! resources and data folder from the template as it is NOW. A respawn after
//! a deploy that changed the template therefore runs the new image, and a
//! saved config can't point an instance at another image: if the template
//! says otherwise, the template wins.
//!
//! A recorder's files go in a host folder the spawner computes, never one a
//! caller supplies: `LAKE_ROOT/raw/<venue>/<market>/<instance>`, from the
//! template's `data` and the instance name, all checked against a tight
//! charset. The spawner creates it (it runs as uid 1000, the uid the
//! recorder image runs as, so the folder is the recorder's) and mounts only
//! that folder at `/data`. A new recorder is refused when the lake's disk
//! has less than `LAKE_MIN_FREE_GB` free; a respawn never is, because
//! refusing it would only punch a gap in a dataset that already exists.
//!
//! A JOB (a one-shot template: a search, a collector) writes to
//! `LAKE_ROOT/jobs/<instance>` at `/data`, and with `"lake": "read"` gets
//! `LAKE_ROOT/raw` READ-ONLY at `/lake`. With `"database": "research"` the
//! API adds the scoped research URL at spawn (`FKS_DB_URL`); it is never
//! saved with the instance. A job runs once: the template refuses restarts,
//! the supervisor never restarts a one-shot, and boot reconciliation never
//! re-runs one.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::config::InstancesConfig;
use crate::models::{ConfigRequest, SpawnRequest, TemplateRef};
use crate::supervisor::RestartPolicy;
use crate::templates::{
    DatabaseAccess, LakeAccess, Template, TemplateKind, TemplateSet, env_for, validate_params,
};

/// Mounted at this path in every instance that has a data folder.
pub const DATA_MOUNT: &str = "/data";
/// Where a job that reads the lake finds it (read-only).
pub const LAKE_MOUNT: &str = "/lake";

/// `POST /instances` and `POST /instances/validate`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceRequest {
    /// Template id, e.g. `recorder.binance-usdm`.
    pub template: String,
    /// The instance's name: its config name and bot id (`fks-bot-<name>`).
    pub name: String,
    /// The form's values.
    #[serde(default)]
    pub params: Value,
    /// One of the template's modes; its first by default.
    #[serde(default)]
    pub mode: Option<String>,
}

/// What an instance resolves to, before anything is created.
#[derive(Debug, Clone, Serialize)]
pub struct InstancePlan {
    pub name: String,
    pub template_id: String,
    pub template_version: u32,
    pub kind: TemplateKind,
    pub image: String,
    pub mode: String,
    pub env: BTreeMap<String, String>,
    pub params: Map<String, Value>,
    pub cpu_limit: Option<f64>,
    pub memory_mb: Option<i32>,
    pub restart_policy: Option<RestartPolicy>,
    /// The host folder mounted at `/data`, for templates that record data.
    pub data_dir: Option<PathBuf>,
    /// The lake, mounted read-only at `/lake` (jobs with `"lake": "read"`).
    pub lake_read: Option<PathBuf>,
    /// The API adds the research database URL at spawn (jobs with
    /// `"database": "research"`).
    pub research_db: bool,
}

/// Why an instance can't be planned: an unknown template (404) or input that
/// doesn't pass (400, one line per problem).
#[derive(Debug, PartialEq)]
pub enum PlanError {
    NotFound(String),
    Invalid(Vec<String>),
}

impl PlanError {
    fn invalid(msg: impl Into<String>) -> Self {
        PlanError::Invalid(vec![msg.into()])
    }
}

/// An instance name is a config name, a bot id and a folder name at once.
pub fn validate_instance_name(name: &str) -> Result<(), String> {
    let ok = (2..=40).contains(&name.len())
        && name.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if ok {
        Ok(())
    } else {
        Err(format!(
            "name {name:?} must be 2-40 lower-case letters, digits and '-', starting with a letter or digit"
        ))
    }
}

/// An instance's own folder: `LAKE_ROOT/raw/<venue>/<market>/<name>` for a
/// recorder, `LAKE_ROOT/jobs/<name>` for a job. The parts were checked when
/// the template loaded and when the name was.
pub fn lake_dir(root: &Path, template: &Template, name: &str) -> Option<PathBuf> {
    if template.kind == TemplateKind::Job {
        return Some(root.join("jobs").join(name));
    }
    let data = template.data.as_ref()?;
    Some(
        root.join("raw")
            .join(&data.venue)
            .join(&data.market)
            .join(name),
    )
}

/// Plan a new instance from a request.
pub fn plan(
    set: &TemplateSet,
    cfg: &InstancesConfig,
    req: &InstanceRequest,
) -> Result<InstancePlan, PlanError> {
    let template = set
        .get(&req.template)
        .ok_or_else(|| PlanError::NotFound(format!("template '{}'", req.template)))?;
    build(template, cfg, &req.name, req.mode.as_deref(), &req.params)
}

/// Re-derive a saved instance from its template as it is now (every respawn,
/// boot reconcile and crash restart goes through here).
pub fn plan_saved(
    set: &TemplateSet,
    cfg: &InstancesConfig,
    name: &str,
    mode: &str,
    saved: &TemplateRef,
) -> Result<InstancePlan, PlanError> {
    let template = set.get(&saved.id).ok_or_else(|| {
        PlanError::NotFound(format!(
            "template '{}' (instance '{name}' was made from it; is its file still in TEMPLATES_DIR?)",
            saved.id
        ))
    })?;
    build(template, cfg, name, Some(mode), &saved.params)
}

fn build(
    template: &Template,
    cfg: &InstancesConfig,
    name: &str,
    mode: Option<&str>,
    params: &Value,
) -> Result<InstancePlan, PlanError> {
    if template.kind == TemplateKind::Bot {
        return Err(PlanError::invalid(format!(
            "{} is a bot template; bots become instances with the forward harness \
             (EDGE_PROCESS §5), not yet",
            template.id
        )));
    }
    validate_instance_name(name).map_err(PlanError::invalid)?;
    let mode = match mode {
        Some(m) if template.modes.iter().any(|x| x == m) => m.to_string(),
        Some(m) => {
            return Err(PlanError::invalid(format!(
                "mode {m:?} isn't one of {}'s modes: {:?}",
                template.id, template.modes
            )));
        }
        None => template.modes[0].clone(),
    };
    let params = validate_params(template, params).map_err(PlanError::Invalid)?;
    let env = env_for(template, &params);
    let data_dir = lake_dir(&cfg.lake_root, template, name);
    let lake_read = (template.lake == Some(LakeAccess::Read)).then(|| cfg.lake_root.join("raw"));
    Ok(InstancePlan {
        name: name.to_string(),
        template_id: template.id.clone(),
        template_version: template.version,
        kind: template.kind,
        image: template.image.clone(),
        mode,
        env,
        params,
        cpu_limit: template.resources.cpu,
        memory_mb: template.resources.memory_mb,
        restart_policy: template.restart_policy.clone(),
        data_dir,
        lake_read,
        research_db: template.database == Some(DatabaseAccess::Research),
    })
}

impl InstancePlan {
    /// The saved config: it rebuilds this instance on every respawn.
    pub fn config_request(&self) -> ConfigRequest {
        ConfigRequest {
            name: self.name.clone(),
            image: self.image.clone(),
            mode: self.mode.clone(),
            cpu_limit: self.cpu_limit,
            memory_mb: self.memory_mb,
            env: self.env.clone().into_iter().collect(),
            secrets: Vec::new(),
            webhooks: Default::default(),
            bot_id: Some(self.name.clone()),
            account: None,
            restart_policy: self.restart_policy.clone(),
            template: Some(TemplateRef {
                id: self.template_id.clone(),
                version: self.template_version,
                params: Value::Object(self.params.clone()),
            }),
        }
    }

    /// The container to run, plus the research database URL when the template
    /// asks for it (`"database": "research"`) as `FKS_DB_URL`. Added here, at
    /// spawn, so it's never saved with the instance or shown by
    /// `/instances/validate`. Without a URL the job is refused: its result
    /// would have nowhere to go, and the spawner's own full-privilege URL is
    /// never handed out.
    pub fn spawn_request_with(&self, research_db_url: &str) -> Result<SpawnRequest, String> {
        let mut req = self.spawn_request();
        if self.research_db {
            let url = research_db_url.trim();
            if url.is_empty() {
                return Err(format!(
                    "{} needs the research database: set BACKTEST_DB_URL (the scoped \
                     fks_backtest role) on the spawner",
                    self.template_id
                ));
            }
            req.env.insert("FKS_DB_URL".to_string(), url.to_string());
        }
        Ok(req)
    }

    /// The container to run.
    pub fn spawn_request(&self) -> SpawnRequest {
        let mut labels = std::collections::HashMap::new();
        labels.insert("fks.template".to_string(), self.template_id.clone());
        labels.insert(
            "fks.template_version".to_string(),
            self.template_version.to_string(),
        );
        SpawnRequest {
            image: self.image.clone(),
            bot_id: Some(self.name.clone()),
            account: None,
            mode: self.mode.clone(),
            env: self.env.clone().into_iter().collect(),
            labels,
            cpu_limit: self.cpu_limit,
            memory_limit_mb: self.memory_mb.map(i64::from),
            cmd: None,
            entrypoint: None,
            secrets: Vec::new(),
            data_dir: self.data_dir.clone(),
            lake_read: self.lake_read.clone(),
            webhooks: Default::default(),
        }
    }
}

/// Free bytes for unprivileged use on the filesystem holding `path`.
pub fn free_bytes(path: &Path) -> std::io::Result<u64> {
    disk_space(path).map(|(free, _)| free)
}

/// (free for unprivileged use, total) bytes on the filesystem holding `path`.
#[allow(clippy::unnecessary_cast)] // the statvfs field types differ by platform
pub fn disk_space(path: &Path) -> std::io::Result<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has a NUL byte")
    })?;
    let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `c_path` is a valid NUL-terminated string and `st` points to
    // writable memory of the right size. statvfs fills it only on success,
    // which is checked before it is read.
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), st.as_mut_ptr()) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: rc == 0, so statvfs initialised `st`.
    let st = unsafe { st.assume_init() };
    let frsize = st.f_frsize as u64;
    Ok((
        (st.f_bavail as u64).saturating_mul(frsize),
        (st.f_blocks as u64).saturating_mul(frsize),
    ))
}

/// Make the instance's data folder. For a NEW instance (`min_free_gb` set)
/// the lake must have that much space free; a respawn passes `None`.
pub fn prepare_data_dir(dir: &Path, min_free_gb: Option<u64>) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| {
        format!(
            "can't create the data folder {} ({e}); is LAKE_ROOT mounted into the spawner and \
             owned by its user?",
            dir.display()
        )
    })?;
    if let Some(min_gb) = min_free_gb {
        let free = free_bytes(dir)
            .map_err(|e| format!("can't check free space at {}: {e}", dir.display()))?;
        let free_gb = free / 1_000_000_000;
        if free_gb < min_gb {
            return Err(format!(
                "only {free_gb} GB free where recordings go, under the {min_gb} GB floor \
                 (LAKE_MIN_FREE_GB); free some space before adding a recorder"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::templates::{Template, TemplateSet};
    use serde_json::json;

    fn recorder() -> Template {
        serde_json::from_value(json!({
            "id": "recorder.binance-usdm",
            "version": 3,
            "kind": "recorder",
            "display_name": "Binance USD-M futures recorder",
            "image": "ghcr.io/nuniesmith/fks-bot-recorder:latest",
            "modes": ["record"],
            "resources": {"cpu": 0.25, "memory_mb": 256},
            "restart_policy": {"enabled": true},
            "data": {"venue": "binance", "market": "usdm"},
            "fixed_env": {"RECORDER_VENUE": "binance-usdm"},
            "env": {"symbols": "RECORDER_SYMBOLS"},
            "params": {
                "type": "object",
                "required": ["symbols"],
                "properties": {
                    "symbols": {"type": "array", "items": {"type": "string", "x-format": "symbol"}, "minItems": 1}
                }
            }
        }))
        .expect("parses")
    }

    fn set() -> TemplateSet {
        let mut set = TemplateSet::default();
        let t = recorder();
        set.templates.insert(t.id.clone(), t);
        set
    }

    fn cfg() -> InstancesConfig {
        InstancesConfig {
            templates_dir: "/templates".into(),
            lake_root: "/srv/fks-lake".into(),
            lake_min_free_gb: 120,
        }
    }

    fn req(name: &str, params: Value) -> InstanceRequest {
        InstanceRequest {
            template: "recorder.binance-usdm".into(),
            name: name.into(),
            params,
            mode: None,
        }
    }

    #[test]
    fn a_recorder_instance_resolves_to_a_container_and_its_folder() {
        let p = plan(&set(), &cfg(), &req("btc", json!({"symbols": ["BTCUSDT"]}))).expect("plans");
        assert_eq!(p.mode, "record");
        assert_eq!(
            p.data_dir,
            Some(PathBuf::from("/srv/fks-lake/raw/binance/usdm/btc"))
        );
        let spawn = p.spawn_request();
        assert_eq!(spawn.bot_id.as_deref(), Some("btc"));
        assert_eq!(spawn.image, "ghcr.io/nuniesmith/fks-bot-recorder:latest");
        assert_eq!(spawn.env["RECORDER_SYMBOLS"], "BTCUSDT");
        assert_eq!(spawn.env["RECORDER_VENUE"], "binance-usdm");
        assert_eq!(spawn.labels["fks.template"], "recorder.binance-usdm");
        assert!(spawn.secrets.is_empty());
        let config = p.config_request();
        assert_eq!(config.bot_id.as_deref(), Some("btc"));
        assert_eq!(config.template.as_ref().map(|t| t.version), Some(3));
        assert!(config.restart_policy.is_some_and(|r| r.enabled));
    }

    #[test]
    fn names_that_could_escape_the_lake_are_refused() {
        for bad in ["../x", "a/b", "A", "x", "-x", "a b", "a.b", ""] {
            let err = plan(&set(), &cfg(), &req(bad, json!({"symbols": ["BTCUSDT"]}))).unwrap_err();
            assert!(
                matches!(err, PlanError::Invalid(_)),
                "{bad:?} should be refused"
            );
        }
    }

    #[test]
    fn an_unknown_template_is_not_found_and_a_bad_mode_is_invalid() {
        let mut r = req("btc", json!({"symbols": ["BTCUSDT"]}));
        r.template = "nope".into();
        assert!(matches!(
            plan(&set(), &cfg(), &r),
            Err(PlanError::NotFound(_))
        ));
        let mut r = req("btc", json!({"symbols": ["BTCUSDT"]}));
        r.mode = Some("live".into());
        let PlanError::Invalid(why) = plan(&set(), &cfg(), &r).unwrap_err() else {
            panic!("invalid")
        };
        assert!(why[0].contains("isn't one of"));
    }

    #[test]
    fn bad_params_come_back_one_line_each() {
        let PlanError::Invalid(why) =
            plan(&set(), &cfg(), &req("btc", json!({"symbols": []}))).unwrap_err()
        else {
            panic!("invalid")
        };
        assert_eq!(why, vec!["symbols: needs at least 1 item(s)".to_string()]);
    }

    #[test]
    fn bots_are_not_instances_yet() {
        let mut t = recorder();
        t.kind = TemplateKind::Bot;
        let mut s = TemplateSet::default();
        s.templates.insert(t.id.clone(), t);
        let PlanError::Invalid(why) =
            plan(&s, &cfg(), &req("btc", json!({"symbols": ["BTCUSDT"]}))).unwrap_err()
        else {
            panic!("invalid")
        };
        assert!(why[0].contains("forward harness"), "{why:?}");
    }

    fn job() -> Template {
        serde_json::from_value(json!({
            "id": "job.edge-search",
            "version": 1,
            "kind": "job",
            "display_name": "Edge search",
            "image": "ghcr.io/nuniesmith/fks-bot-edge-search:latest",
            "modes": ["backtest"],
            "lake": "read",
            "database": "research",
            "env": {"hypothesis_id": "EDGE_HYPOTHESIS_ID"},
            "params": {
                "type": "object",
                "required": ["hypothesis_id"],
                "properties": {"hypothesis_id": {"type": "integer", "minimum": 1}}
            }
        }))
        .unwrap()
    }

    #[test]
    fn a_job_writes_its_own_folder_and_reads_the_lake() {
        let mut s = TemplateSet::default();
        s.templates.insert("job.edge-search".into(), job());
        let mut r = req("search-7", json!({"hypothesis_id": 7}));
        r.template = "job.edge-search".into();
        let p = plan(&s, &cfg(), &r).expect("a job plans");
        assert_eq!(p.mode, "backtest");
        assert_eq!(
            p.data_dir,
            Some(PathBuf::from("/srv/fks-lake/jobs/search-7"))
        );
        assert_eq!(p.lake_read, Some(PathBuf::from("/srv/fks-lake/raw")));
        assert!(p.research_db);
        assert_eq!(
            p.env.get("EDGE_HYPOTHESIS_ID").map(String::as_str),
            Some("7")
        );
        let spawn = p.spawn_request();
        assert_eq!(spawn.lake_read, p.lake_read);
        assert!(
            !spawn.env.contains_key("FKS_DB_URL")
                && !p.config_request().env.contains_key("FKS_DB_URL"),
            "the database URL is added by the API at spawn, never by the plan or the saved config"
        );
        // The URL comes in at spawn, and a research job without one is refused.
        let with = p
            .spawn_request_with("postgres://fks_backtest@db/fks_db")
            .expect("spawns");
        assert_eq!(
            with.env.get("FKS_DB_URL").map(String::as_str),
            Some("postgres://fks_backtest@db/fks_db")
        );
        assert!(
            p.spawn_request_with("  ")
                .unwrap_err()
                .contains("BACKTEST_DB_URL")
        );
    }

    #[test]
    fn a_recorder_gets_no_lake_and_no_database() {
        let mut s = TemplateSet::default();
        s.templates
            .insert("recorder.binance-usdm".into(), recorder());
        let p = plan(&s, &cfg(), &req("btc", json!({"symbols": ["BTCUSDT"]}))).expect("plans");
        assert!(p.lake_read.is_none() && !p.research_db);
        let spawn = p
            .spawn_request_with("postgres://fks_backtest@db/fks_db")
            .expect("spawns");
        assert!(!spawn.env.contains_key("FKS_DB_URL"));
        assert!(spawn.lake_read.is_none());
    }

    #[test]
    fn a_saved_instance_takes_the_templates_current_image() {
        let saved = TemplateRef {
            id: "recorder.binance-usdm".into(),
            version: 1,
            params: json!({"symbols": ["ETHUSDT"]}),
        };
        let mut s = set();
        s.templates
            .get_mut("recorder.binance-usdm")
            .expect("there")
            .image = "ghcr.io/nuniesmith/fks-bot-recorder:v2".into();
        let p = plan_saved(&s, &cfg(), "eth", "record", &saved).expect("plans");
        assert_eq!(p.image, "ghcr.io/nuniesmith/fks-bot-recorder:v2");
        assert_eq!(p.template_version, 3, "the template as it is now");
        assert_eq!(p.env["RECORDER_SYMBOLS"], "ETHUSDT");
        let gone =
            plan_saved(&TemplateSet::default(), &cfg(), "eth", "record", &saved).unwrap_err();
        assert!(matches!(gone, PlanError::NotFound(m) if m.contains("still in TEMPLATES_DIR")));
    }

    #[test]
    fn the_data_folder_is_made_and_the_floor_is_enforced_only_for_new_ones() {
        let root = std::env::temp_dir().join(format!("fks-lake-{}", uuid::Uuid::new_v4()));
        let dir = root.join("raw/binance/usdm/btc");
        prepare_data_dir(&dir, Some(0)).expect("made");
        assert!(dir.is_dir());
        let err = prepare_data_dir(&dir, Some(u64::MAX / 1_000_000_000)).unwrap_err();
        assert!(err.contains("under the"), "{err}");
        prepare_data_dir(&dir, None).expect("a respawn is never refused for space");
        assert!(free_bytes(&dir).expect("statvfs") > 0);
        std::fs::remove_dir_all(&root).ok();
    }
}
