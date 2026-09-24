//! On-disk record of finished downloads.
//!
//! The log is rewritten whenever a download finishes, which keeps it capped at
//! [`MAX_HISTORY_ENTRIES`] records and never depends on what an earlier run left behind. It
//! lives under the user's local application data, because the record is machine-specific and
//! must not roam.

use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use thiserror::Error;

const HISTORY_FORMAT_HEADER: &str = "kosmos-history-v1";
const HISTORY_FILE_NAME: &str = "history.jsonl";
const HISTORY_DIRECTORY_NAME: &str = "Kosmos Downloader";
const MAX_HISTORY_ENTRIES: usize = 1_000;

#[derive(Debug, Error)]
pub enum HistoryError {
    #[error("history I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("history format error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Never 0, which marks the active download.
    pub id: i32,
    pub url: String,
    pub filename: String,
    pub save_path: PathBuf,
    pub total_bytes: u64,
    pub completed_unix_ms: u64,
}

/// The log is UTF-8 without a byte order mark, one JSON record per line, after the format
/// header:
///
/// ```text
/// kosmos-history-v1
/// {"id":1,"url":"…","filename":"…","save_path":"…","total_bytes":1,"completed_unix_ms":1}
/// ```
#[derive(Debug)]
pub struct HistoryStore {
    path: PathBuf,
    entries: Vec<HistoryEntry>,
}

impl HistoryStore {
    pub fn load() -> Self {
        Self::load_from(default_history_path())
    }

    /// Unusable lines are skipped rather than reported: a partially written or hand-edited
    /// file must never stop the application from starting.
    pub fn load_from(path: PathBuf) -> Self {
        let entries = std::fs::read_to_string(&path)
            .map(|text| parse_history(&text, MAX_HISTORY_ENTRIES))
            .unwrap_or_default();
        Self { path, entries }
    }

    /// Retained downloads, oldest first.
    pub fn entries(&self) -> &[HistoryEntry] {
        &self.entries
    }

    fn max_id(&self) -> Option<i32> {
        self.entries.iter().map(|entry| entry.id).max()
    }

    /// Id for the next listed download, so new rows never reuse a loaded one.
    pub fn next_id(&self) -> i32 {
        self.max_id().map_or(1, |highest| highest.saturating_add(1))
    }

    pub fn path_for(&self, id: i32) -> Option<&Path> {
        self.entries
            .iter()
            .find(|entry| entry.id == id)
            .map(|entry| entry.save_path.as_path())
    }

    /// Keeps `entry` and writes the log back, keeping it at most [`MAX_HISTORY_ENTRIES`] long.
    pub fn record(&mut self, entry: HistoryEntry) -> Result<(), HistoryError> {
        self.entries.push(entry);
        if self.entries.len() > MAX_HISTORY_ENTRIES {
            self.entries.remove(0);
        }
        self.rewrite()
    }

    /// Removes the entry with `id` and rewrites the log back.
    pub fn remove(&mut self, id: i32) -> Result<Option<HistoryEntry>, HistoryError> {
        if let Some(index) = self.entries.iter().position(|entry| entry.id == id) {
            let removed = self.entries.remove(index);
            self.rewrite()?;
            Ok(Some(removed))
        } else {
            Ok(None)
        }
    }

    fn rewrite(&mut self) -> Result<(), HistoryError> {
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let mut contents = String::with_capacity(32 + self.entries.len() * 192);
        contents.push_str(HISTORY_FORMAT_HEADER);
        contents.push('\n');
        for entry in &self.entries {
            contents.push_str(&serde_json::to_string(entry)?);
            contents.push('\n');
        }

        // Replace through a temporary file so a failed write cannot truncate the log.
        let temporary = temporary_path(&self.path);
        std::fs::write(&temporary, contents.as_bytes())?;
        if let Err(error) = std::fs::rename(&temporary, &self.path) {
            // Best-effort cleanup; the rename error is the one worth reporting.
            let _ = std::fs::remove_file(&temporary);
            return Err(HistoryError::Io(error));
        }

        Ok(())
    }
}

fn parse_history(text: &str, cap: usize) -> Vec<HistoryEntry> {
    // The format has no byte order mark, but editors on Windows add one.
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut lines = text.lines();
    if lines.next() != Some(HISTORY_FORMAT_HEADER) {
        return Vec::new();
    }

    let mut entries: Vec<HistoryEntry> = Vec::new();
    for line in lines {
        let Some(entry) = decode_entry(line) else {
            continue;
        };
        // A repeated id keeps the first record, so a listed row stays unique.
        if entries.iter().all(|kept| kept.id != entry.id) {
            entries.push(entry);
        }
    }

    if entries.len() > cap {
        entries.drain(..entries.len() - cap);
    }
    entries
}

fn decode_entry(line: &str) -> Option<HistoryEntry> {
    let entry: HistoryEntry = serde_json::from_str(line).ok()?;
    // Id 0 marks the active download, and a record without a file name is unusable.
    (entry.id > 0 && !entry.filename.is_empty()).then_some(entry)
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name: OsString = path.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

fn default_history_path() -> PathBuf {
    history_directory().join(HISTORY_FILE_NAME)
}

fn history_directory() -> PathBuf {
    if let Some(directory) = environment_path("LOCALAPPDATA") {
        return directory.join(HISTORY_DIRECTORY_NAME);
    }
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(HISTORY_DIRECTORY_NAME)
}

fn environment_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// Milliseconds since the Unix epoch, or 0 when the system clock is before it.
pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Local time of `completed_unix_ms`, as shown in the Date Added column.
pub fn downloaded_label(completed_unix_ms: u64) -> String {
    format_completed_label(completed_unix_ms, chrono::Local::now())
}

fn format_completed_label(
    completed_unix_ms: u64,
    now: chrono::DateTime<chrono::Local>,
) -> String {
    use chrono::Datelike;

    let Some(completed_utc) = i64::try_from(completed_unix_ms)
        .ok()
        .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
    else {
        return "Unknown".to_string();
    };

    let local = completed_utc.with_timezone(&chrono::Local);
    let local_date = local.date_naive();
    let today = now.date_naive();

    if local_date == today {
        local.format("Today %H:%M").to_string()
    } else if Some(local_date) == today.pred_opt() {
        local.format("Yesterday %H:%M").to_string()
    } else if local.year() == now.year() {
        local.format("%b %d %H:%M").to_string()
    } else {
        local.format("%b %d, %Y").to_string()
    }
}

#[cfg(test)]
pub(crate) mod tests;
