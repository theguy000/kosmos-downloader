use super::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_TEMP_FILE: AtomicUsize = AtomicUsize::new(0);

/// History file that is removed when the test ends. Shared with the UI tests.
pub(crate) struct TempFile {
    path: PathBuf,
}

impl TempFile {
    pub(crate) fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "kosmos-history-{name}-{}-{}",
            std::process::id(),
            NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        Self { path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Reads the file back, the way the next launch does.
    pub(crate) fn store(&self) -> HistoryStore {
        HistoryStore::load_from(self.path.clone())
    }

    fn write(&self, contents: &str) {
        std::fs::write(&self.path, contents).unwrap();
    }

    fn read(&self) -> String {
        std::fs::read_to_string(&self.path).unwrap()
    }

    fn line_count(&self) -> usize {
        self.read().lines().count()
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(temporary_path(&self.path));
    }
}

fn entry(id: i32, filename: &str, total_bytes: u64) -> HistoryEntry {
    HistoryEntry {
        id,
        url: format!("https://example.com/{filename}"),
        filename: filename.to_string(),
        save_path: PathBuf::from(format!("C:\\Users\\test\\Downloads\\{filename}")),
        total_bytes,
        completed_unix_ms: 1_700_000_000_000 + id as u64,
    }
}

fn json(entry: &HistoryEntry) -> String {
    serde_json::to_string(entry).unwrap()
}

#[test]
fn load_missing_file_starts_empty() {
    let file = TempFile::new("missing");
    let store = file.store();

    assert_eq!(store.entries(), &[]);
    assert_eq!(store.max_id(), None);
    assert_eq!(store.next_id(), 1, "the first listed row never uses id 0");
}

#[test]
fn record_then_load_round_trips_every_field() {
    let file = TempFile::new("round-trip");
    let expected = entry(4, "installer.exe", 5_242_880);
    {
        let mut store = file.store();
        store.record(expected.clone()).unwrap();
    }

    let store = file.store();
    assert_eq!(store.entries(), std::slice::from_ref(&expected));
    assert_eq!(store.max_id(), Some(4));
    assert_eq!(store.next_id(), 5);
    assert_eq!(store.path_for(4), Some(expected.save_path.as_path()));
    assert_eq!(store.path_for(5), None);
    assert_eq!(file.line_count(), 2, "one header line and one record");
    assert!(file.read().starts_with(HISTORY_FORMAT_HEADER));
}

#[test]
fn fields_with_separators_and_backslashes_survive() {
    let file = TempFile::new("escaping");
    let mut expected = entry(1, "tricky.bin", 1);
    expected.url = "https://example.com/a?x=\t1&y=\\2".to_string();
    expected.filename = "tab\there\nnew\rcr\\slash.bin".to_string();
    expected.save_path = PathBuf::from("C:\\odd\tname\\file\\x.bin");

    {
        let mut store = file.store();
        store.record(expected.clone()).unwrap();
    }

    // JSON escapes the separators, so the record stays on one line.
    assert_eq!(file.line_count(), 2);
    assert_eq!(file.store().entries(), &[expected]);
}

#[test]
fn earlier_records_and_the_header_survive_a_record() {
    let file = TempFile::new("earlier-records");
    let mut store = file.store();
    store.record(entry(1, "first.bin", 1)).unwrap();
    store.record(entry(2, "second.bin", 2)).unwrap();
    store.record(entry(3, "third.bin", 3)).unwrap();

    assert_eq!(file.line_count(), 4, "one header line and three records");
    assert!(file.read().starts_with(HISTORY_FORMAT_HEADER));
    assert_eq!(file.read().matches(HISTORY_FORMAT_HEADER).count(), 1);

    let reloaded = file.store();
    assert_eq!(
        reloaded
            .entries()
            .iter()
            .map(|entry| entry.filename.as_str())
            .collect::<Vec<_>>(),
        ["first.bin", "second.bin", "third.bin"]
    );
}

#[test]
fn malformed_lines_are_skipped_without_losing_valid_ones() {
    let file = TempFile::new("malformed");
    let good = entry(7, "good.bin", 1024);
    let other = entry(8, "other.bin", 8);
    let lines = [
        json(&good),
        String::new(),
        "not a record".to_string(),
        // Missing fields.
        r#"{"id":6,"url":"https://example.com/x"}"#.to_string(),
        // An id that belongs to the active download row.
        json(&entry(0, "name.bin", 10)),
        // A record without a file name.
        json(&entry(5, "", 10)),
        json(&other),
        // Truncated by an interrupted write, with no closing brace.
        r#"{"id":9,"url":"https://example.com/x","filename":"name.bin""#.to_string(),
    ];
    file.write(&format!("{HISTORY_FORMAT_HEADER}\n{}\n", lines.join("\n")));

    let store = file.store();
    assert_eq!(
        store
            .entries()
            .iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        [7, 8],
        "usable records survive around unusable ones"
    );
}

#[test]
fn foreign_file_is_replaced_instead_of_appended_to() {
    let file = TempFile::new("foreign");
    file.write("hello\nworld\n");

    let mut store = file.store();
    assert!(
        store.entries().is_empty(),
        "an unrelated file is not a history log"
    );

    store.record(entry(1, "kept.bin", 10)).unwrap();
    assert_eq!(file.line_count(), 2);
    assert_eq!(file.store().entries(), &[entry(1, "kept.bin", 10)]);
}

#[test]
fn non_utf8_file_starts_empty() {
    let file = TempFile::new("non-utf8");
    std::fs::write(file.path(), b"kosmos-history-v1\n\xff\xfe\n").unwrap();

    let store = file.store();
    assert!(store.entries().is_empty());
    assert_eq!(store.next_id(), 1);
}

#[test]
fn crlf_history_still_parses() {
    let file = TempFile::new("crlf");
    let record = entry(2, "crlf.bin", 2048);
    file.write(&format!("{HISTORY_FORMAT_HEADER}\r\n{}\r\n", json(&record)));

    assert_eq!(file.store().entries(), &[record]);
}

#[test]
fn duplicate_ids_on_load_keep_the_first_record() {
    let file = TempFile::new("duplicate-ids");
    file.write(&format!(
        "{HISTORY_FORMAT_HEADER}\n{}\n{}\n",
        json(&entry(3, "first.bin", 10)),
        json(&entry(3, "second.bin", 20))
    ));

    let store = file.store();
    assert_eq!(store.entries().len(), 1);
    assert_eq!(store.entries()[0].filename, "first.bin");
}

#[test]
fn oldest_entries_are_dropped_at_the_cap() {
    let file = TempFile::new("cap");
    let newest = MAX_HISTORY_ENTRIES + 20;
    let mut contents = String::from(HISTORY_FORMAT_HEADER);
    for id in 1..=newest {
        contents.push('\n');
        contents.push_str(&json(&entry(id as i32, "file.bin", 10)));
    }
    file.write(&contents);

    let store = file.store();
    assert_eq!(store.entries().len(), MAX_HISTORY_ENTRIES);
    assert_eq!(store.entries()[0].id, 21, "the oldest entries are dropped");
    assert_eq!(store.entries()[MAX_HISTORY_ENTRIES - 1].id, newest as i32);
    assert_eq!(store.next_id(), newest as i32 + 1);
}

#[test]
fn recording_keeps_the_log_at_the_cap() {
    let file = TempFile::new("cap-recording");
    let mut store = file.store();

    for id in 1..=(MAX_HISTORY_ENTRIES + 2) {
        store.record(entry(id as i32, "file.bin", 10)).unwrap();
    }

    assert_eq!(store.entries().len(), MAX_HISTORY_ENTRIES);
    assert_eq!(store.entries()[0].id, 3, "the oldest entries are dropped");
    assert_eq!(
        file.line_count(),
        1 + MAX_HISTORY_ENTRIES,
        "one header line and the retained records"
    );
    assert_eq!(file.store().entries().len(), MAX_HISTORY_ENTRIES);
}

#[test]
fn rejected_records_are_ignored() {
    let file = TempFile::new("rejected");
    let bad = [
        // An id that belongs to the active download row.
        r#"{"id":-1,"url":"url","filename":"name.bin","save_path":"C:\\x","total_bytes":10,"completed_unix_ms":11}"#.to_string(),
        // A size that is not a number.
        r#"{"id":1,"url":"url","filename":"name.bin","save_path":"C:\\x","total_bytes":"abc","completed_unix_ms":11}"#.to_string(),
        // A timestamp that is not a number.
        r#"{"id":1,"url":"url","filename":"name.bin","save_path":"C:\\x","total_bytes":10,"completed_unix_ms":"11x"}"#.to_string(),
        // A record without a file name.
        json(&entry(1, "", 10)),
        // A record that is missing the completion time.
        r#"{"id":1,"url":"url","filename":"name.bin","save_path":"C:\\x","total_bytes":10}"#.to_string(),
    ];
    file.write(&format!("{HISTORY_FORMAT_HEADER}\n{}\n", bad.join("\n")));

    let store = file.store();
    assert!(store.entries().is_empty());
    assert_eq!(store.next_id(), 1);
}

#[test]
fn last_try_label_reports_local_time_and_rejects_out_of_range() {
    let label = last_try_label(1_700_000_000_000);
    assert_eq!(label.len(), "2023-11-14 22:13".len());
    assert_eq!(label.as_bytes()[4], b'-');
    assert_eq!(label.as_bytes()[10], b' ');
    assert_eq!(label.as_bytes()[13], b':');

    assert_eq!(last_try_label(u64::MAX), "Unknown");
    assert!(now_unix_ms() > 1_600_000_000_000);
}

#[cfg(windows)]
#[test]
fn default_history_path_is_under_local_app_data() {
    let path = default_history_path();

    assert_eq!(
        path.file_name().and_then(|name| name.to_str()),
        Some(HISTORY_FILE_NAME)
    );
    assert_eq!(
        path.parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str()),
        Some(HISTORY_DIRECTORY_NAME)
    );

    if let Some(local) = environment_path("LOCALAPPDATA") {
        assert!(
            path.starts_with(&local),
            "{} is not under {}",
            path.display(),
            local.display()
        );
    }
}

#[test]
fn environment_and_temporary_paths_are_derived_from_the_log_path() {
    let file = TempFile::new("paths");
    let temporary = temporary_path(file.path());
    assert_eq!(
        temporary,
        PathBuf::from(format!("{}.tmp", file.path().display())),
        "the log is replaced through a sibling temporary file"
    );
    assert_ne!(temporary, file.path());

    assert_eq!(environment_path("KOSMOS_HISTORY_TEST_UNSET"), None);
}

#[test]
fn remove_entry_updates_memory_and_disk() {
    let file = TempFile::new("remove-entry");
    let mut store = file.store();
    let first = entry(1, "first.bin", 100);
    let second = entry(2, "second.bin", 200);
    store.record(first.clone()).unwrap();
    store.record(second.clone()).unwrap();
    assert_eq!(store.entries().len(), 2);

    let removed = store.remove(1).unwrap();
    assert_eq!(removed, Some(first));
    assert_eq!(store.entries(), std::slice::from_ref(&second));

    // Check on-disk persistence
    let reloaded = file.store();
    assert_eq!(reloaded.entries(), &[second]);

    // Non-existent id returns None
    assert_eq!(store.remove(99).unwrap(), None);
}
