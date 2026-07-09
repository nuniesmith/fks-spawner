//! Append-only JSONL trade journal — one line per fill (entry / scale-out /
//! close), so every decision the bot acts on is recorded for later analysis,
//! reconciliation, or tax/accounting. Enabled by `DIP_JOURNAL=<path>`; a no-op
//! when unset.

use std::fs::OpenOptions;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tracing::warn;

/// Writes trade events as [JSON Lines](https://jsonlines.org) to a file.
pub struct Journal {
    path: Option<String>,
}

impl Journal {
    /// Create from an optional path (e.g. the `DIP_JOURNAL` env var). `None`
    /// disables journaling — every [`Journal::record`] call is then a no-op.
    pub fn new(path: Option<String>) -> Self {
        Self { path }
    }

    /// Append one event. A millisecond `ts` field is added automatically.
    /// Best-effort: a write failure is logged, never fatal, and never blocks a
    /// trade (the file is opened/flushed per line so a crash can't lose history).
    pub fn record(&self, mut fields: Value) {
        let Some(path) = &self.path else {
            return;
        };
        if let Value::Object(map) = &mut fields {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            map.insert("ts".into(), json!(ts));
        }
        match OpenOptions::new().create(true).append(true).open(path) {
            Ok(mut f) => {
                if let Err(e) = writeln!(f, "{fields}") {
                    warn!(error = %e, path, "journal: write failed");
                }
            }
            Err(e) => warn!(error = %e, path, "journal: could not open file"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_journal_is_a_noop() {
        // No path → no file, no panic.
        Journal::new(None).record(json!({ "event": "entry" }));
    }

    #[test]
    fn records_one_json_line_with_a_timestamp() {
        let path = std::env::temp_dir().join(format!("dip_journal_{}.jsonl", std::process::id()));
        let p = path.to_string_lossy().to_string();
        let _ = std::fs::remove_file(&path);

        let j = Journal::new(Some(p.clone()));
        j.record(json!({ "event": "close", "contracts": 3, "price": 65000.0 }));
        j.record(json!({ "event": "entry", "contracts": 1 }));

        let body = std::fs::read_to_string(&path).expect("journal file written");
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2, "one JSON line per record");
        let first: Value = serde_json::from_str(lines[0]).expect("valid JSON");
        assert_eq!(first["event"], "close");
        assert_eq!(first["contracts"], 3);
        assert!(first["ts"].as_u64().is_some(), "ts added automatically");

        let _ = std::fs::remove_file(&path);
    }
}
