// =============================================================================
// coverage.rs — what a recorder instance has actually written
//
// `GET /instances/{name}/files` answers "which days do I have, per symbol and
// stream, how big are they, and where are the holes?" straight from the lake,
// so the web UI's coverage calendar and an agent's dataset check read the
// same truth: the files themselves, not a counter that could drift from them.
//
// LAYOUT (fks-spawner bots/recorder, recorder/core.py):
//   <dir>/<SYMBOL>/<stream>/YYYY-MM-DD.jsonl      the open day
//   <dir>/<SYMBOL>/<stream>/YYYY-MM-DD.jsonl.gz   a closed day
//   <dir>/_gaps/YYYY-MM-DD.jsonl                  {"group","start_ms","end_ms","reason"}
//   <dir>/_state/status.json                      the heartbeat (not read here)
//
// While a closed day is being compressed both files exist and the .gz is
// still partial, so a day counts as closed only once its .jsonl is gone.
//
// Read-only, bounded (`days`), and blocking: the handler runs it on the
// blocking pool.
// =============================================================================

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::Serialize;

/// Upper bound on `days`, so one request cannot ask for a decade of rows.
pub const MAX_DAYS: usize = 400;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FileDay {
    pub symbol: String,
    pub stream: String,
    pub day: String,
    pub bytes: u64,
    /// The day is gzipped and the open file is gone.
    pub closed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GapDay {
    pub day: String,
    pub group: String,
    pub count: u64,
    pub seconds: f64,
    pub reasons: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Coverage {
    /// The days in the window, newest first.
    pub days: Vec<String>,
    pub files: Vec<FileDay>,
    pub gaps: Vec<GapDay>,
    /// Gap-ledger lines that did not parse (counted, never fatal).
    pub unreadable_gap_lines: u64,
    /// Over every day on disk, not only the window.
    pub first_day: Option<String>,
    pub last_day: Option<String>,
    pub total_days: usize,
    pub total_bytes: u64,
}

/// `2026-10-26.jsonl` → (`2026-10-26`, open); `2026-10-26.jsonl.gz` → closed.
pub fn parse_day_file(name: &str) -> Option<(String, bool)> {
    let (day, closed) = match name.strip_suffix(".jsonl.gz") {
        Some(day) => (day, true),
        None => (name.strip_suffix(".jsonl")?, false),
    };
    chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d").ok()?;
    Some((day.to_string(), closed))
}

/// A symbol or stream folder the recorder made: no `_` prefix (those are
/// the recorder's own bookkeeping), no hidden files.
fn is_data_folder(name: &str) -> bool {
    !name.is_empty() && !name.starts_with('_') && !name.starts_with('.')
}

/// Scan `dir` and report the newest `days` days (1..=MAX_DAYS).
pub fn scan(dir: &Path, days: usize) -> std::io::Result<Coverage> {
    let days = days.clamp(1, MAX_DAYS);

    // (symbol, stream, day) → (bytes, has_open, has_closed)
    let mut found: BTreeMap<(String, String, String), (u64, bool, bool)> = BTreeMap::new();
    for symbol in std::fs::read_dir(dir)? {
        let symbol = symbol?;
        let symbol_name = symbol.file_name().to_string_lossy().into_owned();
        if !is_data_folder(&symbol_name) || !symbol.file_type()?.is_dir() {
            continue;
        }
        for stream in std::fs::read_dir(symbol.path())? {
            let stream = stream?;
            let stream_name = stream.file_name().to_string_lossy().into_owned();
            if !is_data_folder(&stream_name) || !stream.file_type()?.is_dir() {
                continue;
            }
            for file in std::fs::read_dir(stream.path())? {
                let file = file?;
                let Some((day, closed)) = parse_day_file(&file.file_name().to_string_lossy())
                else {
                    continue;
                };
                let meta = file.metadata()?;
                if !meta.is_file() {
                    continue;
                }
                let entry = found
                    .entry((symbol_name.clone(), stream_name.clone(), day))
                    .or_insert((0, false, false));
                if closed {
                    entry.2 = true;
                    // A partial .gz beside its open file is not the day's size.
                    if !entry.1 {
                        entry.0 = meta.len();
                    }
                } else {
                    entry.1 = true;
                    entry.0 = meta.len();
                }
            }
        }
    }

    let all_days: BTreeSet<String> = found.keys().map(|(_, _, d)| d.clone()).collect();
    let mut gap_days = BTreeSet::new();
    let gaps_dir = dir.join("_gaps");
    if gaps_dir.is_dir() {
        for file in std::fs::read_dir(&gaps_dir)? {
            let file = file?;
            if let Some((day, false)) = parse_day_file(&file.file_name().to_string_lossy()) {
                gap_days.insert(day);
            }
        }
    }

    let mut window: Vec<String> = all_days.union(&gap_days).cloned().collect();
    window.sort_unstable_by(|a, b| b.cmp(a));
    window.truncate(days);
    let in_window: BTreeSet<&str> = window.iter().map(String::as_str).collect();

    let mut cov = Coverage {
        first_day: all_days.iter().next().cloned(),
        last_day: all_days.iter().next_back().cloned(),
        total_days: all_days.len(),
        total_bytes: found.values().map(|(bytes, _, _)| bytes).sum(),
        ..Coverage::default()
    };

    for ((symbol, stream, day), (bytes, open, closed)) in &found {
        if in_window.contains(day.as_str()) {
            cov.files.push(FileDay {
                symbol: symbol.clone(),
                stream: stream.clone(),
                day: day.clone(),
                bytes: *bytes,
                closed: *closed && !*open,
            });
        }
    }

    for day in &window {
        let path = gaps_dir.join(format!("{day}.jsonl"));
        let Ok(file) = std::fs::File::open(&path) else {
            continue;
        };
        let mut per_group: BTreeMap<String, GapDay> = BTreeMap::new();
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            match parse_gap_line(&line) {
                Some((group, seconds, reason)) => {
                    let g = per_group.entry(group.clone()).or_insert_with(|| GapDay {
                        day: day.clone(),
                        group,
                        count: 0,
                        seconds: 0.0,
                        reasons: BTreeMap::new(),
                    });
                    g.count += 1;
                    g.seconds += seconds;
                    *g.reasons.entry(reason).or_insert(0) += 1;
                }
                None => cov.unreadable_gap_lines += 1,
            }
        }
        cov.gaps.extend(per_group.into_values());
    }

    cov.days = window;
    Ok(cov)
}

/// `{"group":"public","start_ms":1,"end_ms":701,"reason":"startup"}` →
/// (`public`, 0.7, `startup`). None for anything malformed or negative.
fn parse_gap_line(line: &str) -> Option<(String, f64, String)> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    let group = v.get("group")?.as_str()?.to_string();
    let start = v.get("start_ms")?.as_i64()?;
    let end = v.get("end_ms")?.as_i64()?;
    if end < start {
        return None;
    }
    let reason = v
        .get("reason")
        .and_then(|r| r.as_str())
        .unwrap_or("unknown")
        .to_string();
    Some((group, (end - start) as f64 / 1000.0, reason))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn lake() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("fks-coverage-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write(dir: &Path, rel: &str, bytes: usize) {
        let path = dir.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, vec![b'x'; bytes]).unwrap();
    }

    #[test]
    fn day_file_names() {
        assert_eq!(
            parse_day_file("2026-10-26.jsonl"),
            Some(("2026-10-26".into(), false))
        );
        assert_eq!(
            parse_day_file("2026-10-25.jsonl.gz"),
            Some(("2026-10-25".into(), true))
        );
        assert_eq!(parse_day_file("2026-13-01.jsonl"), None);
        assert_eq!(parse_day_file("status.json"), None);
        assert_eq!(parse_day_file("notes.jsonl"), None);
    }

    #[test]
    fn scans_days_streams_and_gaps() {
        let dir = lake();
        write(&dir, "BTCUSDT/bookTicker/2026-10-24.jsonl.gz", 400);
        write(&dir, "BTCUSDT/bookTicker/2026-10-25.jsonl.gz", 410);
        write(&dir, "BTCUSDT/bookTicker/2026-10-26.jsonl", 9000);
        write(&dir, "BTCUSDT/forceOrder/2026-10-26.jsonl", 3);
        write(&dir, "_state/status.json", 10);
        fs::create_dir_all(dir.join("_gaps")).unwrap();
        fs::write(
            dir.join("_gaps/2026-10-26.jsonl"),
            concat!(
                r#"{"group":"public","start_ms":0,"end_ms":700,"reason":"startup"}"#,
                "\n",
                r#"{"group":"public","start_ms":5000,"end_ms":8000,"reason":"disconnect"}"#,
                "\n",
                r#"{"group":"market","start_ms":0,"end_ms":600,"reason":"startup"}"#,
                "\n",
                "not json\n",
            ),
        )
        .unwrap();

        let cov = scan(&dir, 2).unwrap();
        assert_eq!(
            cov.days,
            vec!["2026-10-26", "2026-10-25"],
            "newest first, window of 2"
        );
        assert_eq!(cov.total_days, 3);
        assert_eq!(cov.first_day.as_deref(), Some("2026-10-24"));
        assert_eq!(cov.last_day.as_deref(), Some("2026-10-26"));
        assert_eq!(cov.total_bytes, 400 + 410 + 9000 + 3);
        assert_eq!(cov.files.len(), 3, "10-24 is outside the window");
        let open = cov
            .files
            .iter()
            .find(|f| f.day == "2026-10-26" && f.stream == "bookTicker")
            .unwrap();
        assert!(!open.closed);
        assert_eq!(open.bytes, 9000);
        assert!(cov.files.iter().any(|f| f.day == "2026-10-25" && f.closed));

        assert_eq!(cov.unreadable_gap_lines, 1);
        let public = cov.gaps.iter().find(|g| g.group == "public").unwrap();
        assert_eq!(public.count, 2);
        assert!((public.seconds - 3.7).abs() < 1e-9);
        assert_eq!(public.reasons.get("disconnect"), Some(&1));
        assert!(
            !cov.files.iter().any(|f| f.symbol.starts_with('_')),
            "bookkeeping folders are skipped"
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_day_being_compressed_counts_as_open_at_its_real_size() {
        let dir = lake();
        write(&dir, "BTCUSDT/bookTicker/2026-10-26.jsonl", 9000);
        write(&dir, "BTCUSDT/bookTicker/2026-10-26.jsonl.gz", 12);
        let cov = scan(&dir, 7).unwrap();
        assert_eq!(cov.files.len(), 1);
        assert!(!cov.files[0].closed);
        assert_eq!(cov.files[0].bytes, 9000);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_empty_instance_is_empty_not_an_error() {
        let dir = lake();
        let cov = scan(&dir, 30).unwrap();
        assert!(cov.days.is_empty() && cov.files.is_empty() && cov.gaps.is_empty());
        assert_eq!(cov.first_day, None);
        fs::remove_dir_all(dir).unwrap();
    }
}
