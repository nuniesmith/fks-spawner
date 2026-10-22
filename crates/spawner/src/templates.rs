//! Templates: registered, versioned descriptions of what the spawner may run
//! as an instance (EDGE_FACTORY_PLAN §4). A template names the image, the
//! modes it may run in, its resources, and a form (`params`, a small subset
//! of JSON Schema) whose values map onto the container's env. The web UI
//! renders the form from `params`; the spawner validates every submission
//! against it, because the UI's check is only a convenience.
//!
//! Templates are JSON files in `TEMPLATES_DIR` (the fks repo's `templates/`,
//! mounted read-only), read on every request: they are a handful of small
//! files, and reading them fresh means a deploy that changes one never
//! leaves a stale copy behind. A file that doesn't validate is reported by
//! `GET /templates` rather than used.
//!
//! The form schema supports exactly what the forms need, and a template
//! using anything else is refused when it loads, so a keyword is never
//! silently ignored:
//!
//! - the form: `type: "object"`, `properties`, `required`;
//! - a field: `type` (`string`, `integer`, `number`, `boolean`, `array`),
//!   `title`, `description`, `default`, `enum`, `minLength`, `maxLength`,
//!   `minimum`, `maximum`, `items` (one level), `minItems`, `maxItems`,
//!   `uniqueItems`, and `x-format: "symbol"` (A–Z and 0–9, 2–20 long);
//! - any `x-` key, for the UI (`x-backfillable`, `x-est-mb-per-day`, …).
//!
//! A submission may not carry fields the form doesn't have.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::supervisor::RestartPolicy;

/// What a template runs. Instances can be made only of recorders so far:
/// bots wait for account leases (EDGE_FACTORY_PLAN §6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TemplateKind {
    Recorder,
    Bot,
    Job,
}

/// Where a recorder's files go: `LAKE_ROOT/raw/<venue>/<market>/<instance>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataSpec {
    pub venue: String,
    pub market: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    #[serde(default)]
    pub cpu: Option<f64>,
    #[serde(default)]
    pub memory_mb: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Template {
    pub id: String,
    pub version: u32,
    pub kind: TemplateKind,
    pub display_name: String,
    #[serde(default)]
    pub description: String,
    pub image: String,
    pub modes: Vec<String>,
    #[serde(default)]
    pub secrets_allowed: Vec<String>,
    #[serde(default)]
    pub resources: Resources,
    #[serde(default)]
    pub restart_policy: Option<RestartPolicy>,
    #[serde(default)]
    pub data: Option<DataSpec>,
    /// The form: a JSON Schema subset (module docs).
    pub params: Value,
    /// Form field -> the env var it sets.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Env vars every instance of this template gets.
    #[serde(default)]
    pub fixed_env: BTreeMap<String, String>,
}

/// Env vars the spawner sets itself; a template may not set them.
const RESERVED_ENV: &[&str] = &["FKS_BOT_ID", "FKS_BOT_MODE", "DATA_DIR"];

/// Modes a recorder may never run in: it holds no keys and places nothing.
const MONEY_MODES: &[&str] = &["live", "dry-run"];

const FORM_KEYS: &[&str] = &["type", "properties", "required", "title", "description"];
const FIELD_KEYS: &[&str] = &[
    "type",
    "title",
    "description",
    "default",
    "enum",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
    "items",
    "minItems",
    "maxItems",
    "uniqueItems",
    "x-format",
];
const ITEM_KEYS: &[&str] = &["type", "enum", "minLength", "maxLength", "x-format"];

/// The templates in a folder, and what was wrong with the ones that aren't.
#[derive(Debug, Default)]
pub struct TemplateSet {
    pub templates: BTreeMap<String, Template>,
    pub errors: Vec<String>,
}

impl TemplateSet {
    /// Every `*.json` file in `dir`, validated; files are read in name order.
    pub fn load(dir: &Path, allowed_image_prefix: &str) -> TemplateSet {
        let mut set = TemplateSet::default();
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) => {
                set.errors.push(format!(
                    "can't read the templates folder {}: {e}",
                    dir.display()
                ));
                return set;
            }
        };
        let mut paths: Vec<_> = entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "json"))
            .collect();
        paths.sort();
        for path in paths {
            let file = path
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_default();
            let parsed = std::fs::read_to_string(&path)
                .map_err(|e| e.to_string())
                .and_then(|text| {
                    serde_json::from_str::<Template>(&text).map_err(|e| e.to_string())
                });
            match parsed {
                Err(e) => set.errors.push(format!("{file}: {e}")),
                Ok(t) => match validate_template(&t, allowed_image_prefix) {
                    Err(e) => set.errors.push(format!("{file}: {e}")),
                    Ok(()) if set.templates.contains_key(&t.id) => {
                        set.errors
                            .push(format!("{file}: another file already defines {}", t.id));
                    }
                    Ok(()) => {
                        set.templates.insert(t.id.clone(), t);
                    }
                },
            }
        }
        set
    }

    pub fn get(&self, id: &str) -> Option<&Template> {
        self.templates.get(id)
    }
}

fn is_slug(s: &str, max: usize, extra: &[char]) -> bool {
    let mut chars = s.chars();
    let first_ok = chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    first_ok
        && s.len() <= max
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || extra.contains(&c))
}

fn is_env_name(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().is_some_and(|c| c.is_ascii_uppercase())
        && s.len() <= 64
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// Whether a template can be used. The messages name the field.
pub fn validate_template(t: &Template, allowed_image_prefix: &str) -> Result<(), String> {
    if !is_slug(&t.id, 63, &['.']) {
        return Err(format!(
            "id {:?} must be lower-case letters, digits, '-' and '.'",
            t.id
        ));
    }
    if t.version == 0 {
        return Err("version starts at 1".into());
    }
    if t.display_name.trim().is_empty() {
        return Err("display_name is empty".into());
    }
    if !t.image.starts_with(allowed_image_prefix) {
        return Err(format!(
            "image {} isn't under the spawner's allowed prefix {allowed_image_prefix}",
            t.image
        ));
    }
    if t.modes.is_empty() {
        return Err("modes is empty".into());
    }
    if let Some(m) = t.modes.iter().find(|m| !is_slug(m, 32, &[])) {
        return Err(format!(
            "mode {m:?} must be lower-case letters, digits and '-'"
        ));
    }
    if t.kind == TemplateKind::Recorder {
        if let Some(m) = t.modes.iter().find(|m| MONEY_MODES.contains(&m.as_str())) {
            return Err(format!(
                "a recorder can't run in mode {m}: it holds no keys and trades nothing"
            ));
        }
        if !t.secrets_allowed.is_empty() {
            return Err("a recorder records public data and gets no secrets".into());
        }
        let Some(data) = &t.data else {
            return Err("a recorder needs data.venue and data.market (where its files go)".into());
        };
        for (field, value) in [("venue", &data.venue), ("market", &data.market)] {
            if !is_slug(value, 32, &[]) {
                return Err(format!(
                    "data.{field} {value:?} must be lower-case letters, digits and '-'"
                ));
            }
        }
    }
    if let Some(cpu) = t.resources.cpu
        && !(cpu > 0.0 && cpu.is_finite())
    {
        return Err("resources.cpu must be a positive number of cores".into());
    }
    if let Some(mb) = t.resources.memory_mb
        && mb < 32
    {
        return Err("resources.memory_mb must be at least 32".into());
    }
    check_form(&t.params)?;
    let fields = form_fields(&t.params);
    for (field, var) in &t.env {
        if !fields.contains_key(field.as_str()) {
            return Err(format!("env maps {field}, which the form doesn't have"));
        }
        check_env_name(var)?;
    }
    let mut set_by_form: BTreeSet<&str> = t.env.values().map(String::as_str).collect();
    for var in t.fixed_env.keys() {
        check_env_name(var)?;
        if !set_by_form.insert(var.as_str()) {
            return Err(format!("{var} is set by both env and fixed_env"));
        }
    }
    Ok(())
}

fn check_env_name(var: &str) -> Result<(), String> {
    if !is_env_name(var) {
        return Err(format!(
            "env var {var:?} must be upper-case letters, digits and '_'"
        ));
    }
    if RESERVED_ENV.contains(&var) {
        return Err(format!("{var} is set by the spawner, not a template"));
    }
    Ok(())
}

fn form_fields(form: &Value) -> Map<String, Value> {
    form.get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

fn check_keys(what: &str, obj: &Map<String, Value>, allowed: &[&str]) -> Result<(), String> {
    match obj
        .keys()
        .find(|k| !k.starts_with("x-") && !allowed.contains(&k.as_str()))
    {
        Some(k) => Err(format!("{what}: unsupported schema keyword {k:?}")),
        None => Ok(()),
    }
}

/// The form schema itself, against the supported subset.
fn check_form(form: &Value) -> Result<(), String> {
    let obj = form
        .as_object()
        .ok_or("params must be an object (the form)")?;
    check_keys("params", obj, FORM_KEYS)?;
    if obj.get("type").and_then(Value::as_str) != Some("object") {
        return Err("params.type must be \"object\"".into());
    }
    let props = obj
        .get("properties")
        .and_then(Value::as_object)
        .ok_or("params.properties must be an object")?;
    for (name, field) in props {
        if !is_slug(name, 40, &['_']) {
            return Err(format!(
                "form field {name:?} must be lower-case letters, digits, '-' and '_'"
            ));
        }
        check_field(&format!("params.{name}"), field, false)?;
    }
    if let Some(required) = obj.get("required") {
        let list = required
            .as_array()
            .ok_or("params.required must be a list")?;
        for r in list {
            let name = r.as_str().ok_or("params.required must list field names")?;
            if !props.contains_key(name) {
                return Err(format!(
                    "params.required names {name}, which the form doesn't have"
                ));
            }
        }
    }
    Ok(())
}

fn check_field(path: &str, field: &Value, is_item: bool) -> Result<(), String> {
    let obj = field
        .as_object()
        .ok_or(format!("{path} must be an object"))?;
    check_keys(path, obj, if is_item { ITEM_KEYS } else { FIELD_KEYS })?;
    let kind = obj.get("type").and_then(Value::as_str);
    let scalar = ["string", "integer", "number"];
    match kind {
        Some(k) if scalar.contains(&k) => {}
        Some("boolean") | Some("array") if !is_item => {}
        // an item may leave its type to `enum`
        None if is_item && obj.contains_key("enum") => {}
        _ => return Err(format!("{path}: unsupported or missing type {kind:?}")),
    }
    if let Some(fmt) = obj.get("x-format")
        && (fmt.as_str() != Some("symbol") || kind.is_some_and(|k| k != "string"))
    {
        return Err(format!(
            "{path}: x-format supports only \"symbol\", on a string"
        ));
    }
    if let Some(values) = obj.get("enum") {
        let list = values
            .as_array()
            .ok_or(format!("{path}.enum must be a list"))?;
        if list.is_empty() {
            return Err(format!("{path}.enum is empty"));
        }
    }
    for key in ["minLength", "maxLength", "minItems", "maxItems"] {
        if let Some(v) = obj.get(key)
            && !v.is_u64()
        {
            return Err(format!("{path}.{key} must be a whole number"));
        }
    }
    for key in ["minimum", "maximum"] {
        if let Some(v) = obj.get(key)
            && !v.is_number()
        {
            return Err(format!("{path}.{key} must be a number"));
        }
    }
    if kind == Some("array") {
        let items = obj
            .get("items")
            .ok_or(format!("{path}: an array needs items"))?;
        check_field(&format!("{path}.items"), items, true)?;
    }
    if let Some(default) = obj.get("default") {
        check_value(path, field, default).map_err(|e| format!("{path}.default: {e}"))?;
    }
    Ok(())
}

fn is_symbol(s: &str) -> bool {
    (2..=20).contains(&s.len())
        && s.chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

/// One value against its field. The message is the operator-facing reason.
fn check_value(path: &str, field: &Value, v: &Value) -> Result<(), String> {
    let kind = field.get("type").and_then(Value::as_str);
    if let Some(allowed) = field.get("enum").and_then(Value::as_array)
        && !allowed.contains(v)
    {
        return Err(format!(
            "{v} isn't one of {}",
            Value::Array(allowed.clone())
        ));
    }
    match kind {
        Some("string") | None if v.is_string() => {
            let s = v.as_str().unwrap_or_default();
            let len = s.chars().count() as u64;
            if let Some(min) = field.get("minLength").and_then(Value::as_u64)
                && len < min
            {
                return Err(format!("must be at least {min} characters"));
            }
            if let Some(max) = field.get("maxLength").and_then(Value::as_u64)
                && len > max
            {
                return Err(format!("must be at most {max} characters"));
            }
            if field.get("x-format").and_then(Value::as_str) == Some("symbol") && !is_symbol(s) {
                return Err(format!(
                    "{s:?} isn't a symbol (A-Z and 0-9, 2-20 long, e.g. BTCUSDT)"
                ));
            }
            Ok(())
        }
        None if field.get("enum").is_some() => Ok(()), // matched the enum above
        Some("integer") if v.is_i64() || v.is_u64() => check_range(field, v),
        Some("number") if v.is_number() => check_range(field, v),
        Some("boolean") if v.is_boolean() => Ok(()),
        Some("array") if v.is_array() => {
            let list = v.as_array().map(Vec::as_slice).unwrap_or_default();
            let n = list.len() as u64;
            if let Some(min) = field.get("minItems").and_then(Value::as_u64)
                && n < min
            {
                return Err(format!("needs at least {min} item(s)"));
            }
            if let Some(max) = field.get("maxItems").and_then(Value::as_u64)
                && n > max
            {
                return Err(format!("takes at most {max} items"));
            }
            if field.get("uniqueItems").and_then(Value::as_bool) == Some(true) {
                let mut seen = BTreeSet::new();
                if let Some(dup) = list.iter().find(|x| !seen.insert(x.to_string())) {
                    return Err(format!("{dup} is listed twice"));
                }
            }
            let items = field.get("items").cloned().unwrap_or(Value::Null);
            for (i, x) in list.iter().enumerate() {
                check_value(&format!("{path}[{i}]"), &items, x)
                    .map_err(|e| format!("item {}: {e}", i + 1))?;
            }
            Ok(())
        }
        _ => Err(format!(
            "expected {}",
            kind.unwrap_or("one of the listed values")
        )),
    }
}

fn check_range(field: &Value, v: &Value) -> Result<(), String> {
    let x = v.as_f64().unwrap_or(f64::NAN);
    if let Some(min) = field.get("minimum").and_then(Value::as_f64)
        && x < min
    {
        return Err(format!("must be at least {min}"));
    }
    if let Some(max) = field.get("maximum").and_then(Value::as_f64)
        && x > max
    {
        return Err(format!("must be at most {max}"));
    }
    Ok(())
}

/// A submission, checked against the form, with defaults filled in. Returns
/// every problem, one line per field, so a form can show them all at once.
pub fn validate_params(t: &Template, params: &Value) -> Result<Map<String, Value>, Vec<String>> {
    let empty = Map::new();
    let given = match params {
        Value::Null => &empty,
        Value::Object(obj) => obj,
        _ => return Err(vec!["params must be an object".into()]),
    };
    let fields = form_fields(&t.params);
    let required: BTreeSet<&str> = t
        .params
        .get("required")
        .and_then(Value::as_array)
        .map(|r| r.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let mut errors: Vec<String> = given
        .keys()
        .filter(|k| !fields.contains_key(k.as_str()))
        .map(|k| format!("{k}: the form has no such field"))
        .collect();
    let mut out = Map::new();
    for (name, field) in &fields {
        let value = match given.get(name) {
            Some(v) if !v.is_null() => v.clone(),
            _ => match field.get("default") {
                Some(d) => d.clone(),
                None if required.contains(name.as_str()) => {
                    errors.push(format!("{name}: required"));
                    continue;
                }
                None => continue,
            },
        };
        match check_value(name, field, &value) {
            Ok(()) => {
                out.insert(name.clone(), value);
            }
            Err(e) => errors.push(format!("{name}: {e}")),
        }
    }
    if errors.is_empty() {
        Ok(out)
    } else {
        Err(errors)
    }
}

/// The env an instance gets from its template and checked params: the fixed
/// env, then each mapped field (a list joined with commas).
pub fn env_for(t: &Template, params: &Map<String, Value>) -> BTreeMap<String, String> {
    let mut env = t.fixed_env.clone();
    for (field, var) in &t.env {
        let Some(v) = params.get(field) else { continue };
        let text = match v {
            Value::String(s) => s.clone(),
            Value::Array(items) => items
                .iter()
                .map(|x| {
                    x.as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| x.to_string())
                })
                .collect::<Vec<_>>()
                .join(","),
            other => other.to_string(),
        };
        env.insert(var.clone(), text);
    }
    env
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub(crate) fn recorder() -> Template {
        serde_json::from_value(json!({
            "id": "recorder.binance-usdm",
            "version": 1,
            "kind": "recorder",
            "display_name": "Binance USD-M futures recorder",
            "image": "ghcr.io/nuniesmith/fks-bot-recorder:latest",
            "modes": ["record"],
            "resources": {"cpu": 0.25, "memory_mb": 256},
            "data": {"venue": "binance", "market": "usdm"},
            "fixed_env": {"RECORDER_VENUE": "binance-usdm"},
            "env": {"symbols": "RECORDER_SYMBOLS", "streams": "RECORDER_STREAMS", "rest_seconds": "RECORDER_REST_SECONDS", "backfill_oi": "RECORDER_BACKFILL_OI"},
            "params": {
                "type": "object",
                "required": ["symbols"],
                "properties": {
                    "symbols": {"type": "array", "items": {"type": "string", "x-format": "symbol"}, "minItems": 1, "maxItems": 20, "uniqueItems": true},
                    "streams": {"type": "array", "items": {"enum": ["forceOrder", "bookTicker", "markPrice@1s"]}, "default": ["forceOrder", "markPrice@1s"], "uniqueItems": true, "x-est-mb-per-day": {"bookTicker": 335}},
                    "rest_seconds": {"type": "integer", "minimum": 60, "maximum": 3600, "default": 300},
                    "backfill_oi": {"type": "boolean", "default": false},
                    "note": {"type": "string", "maxLength": 20, "default": ""}
                }
            }
        }))
        .expect("test template parses")
    }

    const PREFIX: &str = "ghcr.io/nuniesmith/fks-bot-";

    #[test]
    fn the_recorder_template_is_valid() {
        validate_template(&recorder(), PREFIX).expect("valid");
    }

    #[test]
    fn a_template_outside_the_allowed_prefix_is_refused() {
        let mut t = recorder();
        t.image = "ghcr.io/nuniesmith/crypto:latest".into();
        assert!(
            validate_template(&t, PREFIX)
                .unwrap_err()
                .contains("allowed prefix")
        );
    }

    #[test]
    fn a_recorder_never_runs_live_or_gets_secrets() {
        let mut t = recorder();
        t.modes = vec!["record".into(), "live".into()];
        assert!(
            validate_template(&t, PREFIX)
                .unwrap_err()
                .contains("mode live")
        );
        let mut t = recorder();
        t.secrets_allowed = vec!["kraken".into()];
        assert!(
            validate_template(&t, PREFIX)
                .unwrap_err()
                .contains("no secrets")
        );
    }

    #[test]
    fn a_recorder_needs_a_safe_data_location() {
        let mut t = recorder();
        t.data = None;
        assert!(
            validate_template(&t, PREFIX)
                .unwrap_err()
                .contains("data.venue")
        );
        let mut t = recorder();
        t.data = Some(DataSpec {
            venue: "../etc".into(),
            market: "usdm".into(),
        });
        assert!(
            validate_template(&t, PREFIX)
                .unwrap_err()
                .contains("data.venue")
        );
    }

    #[test]
    fn unsupported_schema_keywords_are_refused_not_ignored() {
        let mut t = recorder();
        t.params["properties"]["symbols"]["pattern"] = json!("^[A-Z]+$");
        assert!(
            validate_template(&t, PREFIX)
                .unwrap_err()
                .contains("unsupported schema keyword \"pattern\"")
        );
        let mut t = recorder();
        t.params["properties"]["streams"]["x-anything"] = json!(1); // UI hints are fine
        validate_template(&t, PREFIX).expect("x- keys pass");
    }

    #[test]
    fn a_template_may_not_set_the_spawners_own_env() {
        let mut t = recorder();
        t.fixed_env.insert("DATA_DIR".into(), "/etc".into());
        assert!(
            validate_template(&t, PREFIX)
                .unwrap_err()
                .contains("set by the spawner")
        );
        let mut t = recorder();
        t.env.insert("missing".into(), "X".into());
        assert!(
            validate_template(&t, PREFIX)
                .unwrap_err()
                .contains("doesn't have")
        );
    }

    #[test]
    fn a_default_must_pass_its_own_field() {
        let mut t = recorder();
        t.params["properties"]["rest_seconds"]["default"] = json!(5);
        assert!(
            validate_template(&t, PREFIX)
                .unwrap_err()
                .contains("at least 60")
        );
    }

    #[test]
    fn defaults_fill_in_and_env_maps_the_fields() {
        let t = recorder();
        let p = validate_params(&t, &json!({"symbols": ["BTCUSDT", "ETHUSDT"]})).expect("valid");
        assert_eq!(p["streams"], json!(["forceOrder", "markPrice@1s"]));
        let env = env_for(&t, &p);
        assert_eq!(env["RECORDER_SYMBOLS"], "BTCUSDT,ETHUSDT");
        assert_eq!(env["RECORDER_STREAMS"], "forceOrder,markPrice@1s");
        assert_eq!(env["RECORDER_REST_SECONDS"], "300");
        assert_eq!(env["RECORDER_BACKFILL_OI"], "false");
        assert_eq!(env["RECORDER_VENUE"], "binance-usdm");
        assert!(!env.contains_key("note"), "unmapped fields set no env");
    }

    #[test]
    fn every_field_with_a_problem_gets_its_line_at_once() {
        let t = recorder();
        let errors = validate_params(
            &t,
            &json!({"symbols": ["btcusdt"], "streams": ["depth"], "rest_seconds": 10, "extra": 1}),
        )
        .unwrap_err();
        assert_eq!(errors.len(), 4, "{errors:?}");
        let text = errors.join("\n");
        assert!(text.contains("extra: the form has no such field"), "{text}");
        assert!(
            text.contains("symbols: item 1: \"btcusdt\" isn't a symbol"),
            "{text}"
        );
        assert!(
            text.contains("streams: item 1: \"depth\" isn't one of"),
            "{text}"
        );
        assert!(text.contains("rest_seconds: must be at least 60"), "{text}");
    }

    #[test]
    fn a_list_may_not_repeat_itself() {
        let errors =
            validate_params(&recorder(), &json!({"symbols": ["BTCUSDT", "BTCUSDT"]})).unwrap_err();
        assert_eq!(
            errors,
            vec!["symbols: \"BTCUSDT\" is listed twice".to_string()]
        );
    }

    #[test]
    fn a_required_field_must_be_given() {
        let t = recorder();
        assert_eq!(
            validate_params(&t, &json!({})).unwrap_err(),
            vec!["symbols: required".to_string()]
        );
        assert_eq!(
            validate_params(&t, &Value::Null).unwrap_err(),
            vec!["symbols: required".to_string()]
        );
    }

    #[test]
    fn the_folder_loader_keeps_good_files_and_reports_bad_ones() {
        let dir = std::env::temp_dir().join(format!("fks-templates-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(
            dir.join("a.json"),
            serde_json::to_string(&recorder()).expect("json"),
        )
        .expect("write");
        std::fs::write(dir.join("b.json"), "{ not json").expect("write");
        let mut dup = recorder();
        dup.display_name = "again".into();
        std::fs::write(
            dir.join("c.json"),
            serde_json::to_string(&dup).expect("json"),
        )
        .expect("write");
        std::fs::write(dir.join("notes.txt"), "ignored").expect("write");
        let set = TemplateSet::load(&dir, PREFIX);
        assert_eq!(
            set.templates.keys().collect::<Vec<_>>(),
            vec!["recorder.binance-usdm"]
        );
        assert_eq!(set.errors.len(), 2, "{:?}", set.errors);
        assert!(set.errors[0].starts_with("b.json:"));
        assert!(set.errors[1].contains("already defines"));
        std::fs::remove_dir_all(&dir).ok();
        let missing = TemplateSet::load(&dir, PREFIX);
        assert!(
            missing.templates.is_empty()
                && missing.errors[0].contains("can't read the templates folder")
        );
    }
}
