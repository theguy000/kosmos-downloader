use super::support::{finished_entry, install_test_platform};
use crate::engine::{DownloadSnapshot, DownloadStatus};
use crate::history::tests::TempFile;
use crate::history::{HistoryStore, downloaded_label};
use crate::ui::projection::{history_table_item, sort_items};
use crate::ui::save_settings::SaveSettings;
use crate::ui::table::HistoryTracker;
use crate::ui::view::{MainWindow, TableItem};
use slint::Model;
use std::cell::Cell;
use std::path::PathBuf;
use std::rc::Rc;

#[test]
fn column_width_defaults_and_save_callback() -> Result<(), Box<dyn std::error::Error>> {
    let _ = install_test_platform()?;
    let ui = MainWindow::new()?;

    assert_eq!(ui.get_col_filename_width(), 292.0);
    assert_eq!(ui.get_col_size_width(), 72.0);
    assert_eq!(ui.get_col_status_width(), 65.0);
    assert_eq!(ui.get_col_time_left_width(), 121.0);
    assert_eq!(ui.get_col_transfer_rate_width(), 95.0);
    assert_eq!(ui.get_col_date_added_width(), 96.0);

    let saved = Rc::new(Cell::new(false));
    let saved_clone = saved.clone();
    ui.on_save_column_widths(move || {
        saved_clone.set(true);
    });

    ui.set_col_filename_width(420.0);
    assert_eq!(ui.get_col_filename_width(), 420.0);

    ui.invoke_save_column_widths();
    assert!(saved.get());

    Ok(())
}

#[test]
fn completed_download_archiving_preserves_history() {
    let history = slint::VecModel::<TableItem>::default();
    assert_eq!(history.row_count(), 0);

    let first = finished_entry(1, "file.txt", 1024);
    history.push(history_table_item(&SaveSettings::default(), &first));
    assert_eq!(history.row_count(), 1);
    let listed = history.row_data(0).unwrap();
    assert_eq!(listed.id, 1, "Row 0 stays reserved for the active download");
    assert_eq!(listed.filename, "file.txt");
    assert_eq!(listed.file_type, "doc");
    assert_eq!(listed.size_text, "1.0 KB");
    assert_eq!(listed.status_text, "Complete");
    assert_eq!(listed.time_left_text, "--:--");
    assert_eq!(listed.transfer_rate_text, "0 KB/s");
    assert_eq!(
        listed.downloaded_text,
        downloaded_label(1_700_000_000_001),
        "The stored completion time is shown, not a fixed label"
    );
    assert_eq!(listed.size_bytes, 1024.0);

    history.push(history_table_item(
        &SaveSettings::default(),
        &finished_entry(2, "archive.ZIP", 2048),
    ));
    assert_eq!(history.row_count(), 2);
    assert_eq!(history.row_data(0).unwrap().id, 1);
    let newest = history.row_data(1).unwrap();
    assert_eq!(newest.id, 2);
    assert_eq!(newest.size_text, "2.0 KB");
    assert_eq!(
        newest.file_type, "zip",
        "Extensions match case-insensitively"
    );
}

#[test]
fn sort_items_orders_by_each_sortable_column() {
    let mut items: Vec<TableItem> = [
        ("beta.bin", 4096_u64),
        ("Alpha.bin", 1024),
        ("gamma.bin", 2048),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (name, size))| {
        let mut entry = finished_entry(index as i32 + 1, name, size);
        entry.completed_unix_ms = 1_700_000_000_000 + index as u64 * 60_000;
        history_table_item(&SaveSettings::default(), &entry)
    })
    .collect();

    sort_items(&mut items, 0, true);
    let names: Vec<&str> = items.iter().map(|item| item.filename.as_str()).collect();
    assert_eq!(names, ["Alpha.bin", "beta.bin", "gamma.bin"]);

    sort_items(&mut items, 1, true);
    assert_eq!(items[0].size_bytes, 1024.0);
    assert_eq!(items[2].size_bytes, 4096.0);
    sort_items(&mut items, 1, false);
    assert_eq!(items[0].size_bytes, 4096.0);

    sort_items(&mut items, 3, true);
    assert_eq!(items[0].id, 1);
    assert_eq!(items[2].id, 3);

    let order: Vec<i32> = items.iter().map(|item| item.id).collect();
    sort_items(&mut items, 2, true);
    sort_items(&mut items, 5, true);
    assert_eq!(items.iter().map(|item| item.id).collect::<Vec<_>>(), order);

    let mut tied: Vec<TableItem> = (1..=2)
        .map(|id| {
            history_table_item(
                &SaveSettings::default(),
                &finished_entry(id, "same.bin", 2048),
            )
        })
        .collect();
    tied.reverse();
    sort_items(&mut tied, 1, true);
    assert_eq!(tied.iter().map(|item| item.id).collect::<Vec<_>>(), [1, 2]);
}

#[test]
fn history_tracker_reports_each_finish_once() {
    let finished = DownloadSnapshot {
        session_id: 1,
        url: "https://example.com/file.bin".into(),
        filename: "file.bin".into(),
        save_path: PathBuf::from("file.bin"),
        status: DownloadStatus::Completed,
        total_bytes: Some(1024),
        downloaded_bytes: 1024,
        ..Default::default()
    };
    let next_session = DownloadSnapshot {
        session_id: 2,
        status: DownloadStatus::Connecting,
        ..Default::default()
    };

    let mut tracker = HistoryTracker::new(1);
    let listed = tracker
        .observe(&finished)
        .expect("a finish is reported right away");
    assert_eq!(listed.id, 1, "the first listed row never uses id 0");
    assert_eq!(listed.url, "https://example.com/file.bin");
    assert_eq!(listed.filename, "file.bin");
    assert_eq!(listed.save_path, PathBuf::from("file.bin"));
    assert_eq!(listed.total_bytes, 1024);
    assert!(listed.completed_unix_ms > 0, "the finish time is recorded");

    assert_eq!(
        tracker.observe(&finished),
        None,
        "repeated completed ticks are not listed again"
    );
    assert_eq!(
        tracker.observe(&next_session),
        None,
        "a later session does not re-list the same finish"
    );

    let second_finished = DownloadSnapshot {
        session_id: 2,
        filename: "next.bin".into(),
        status: DownloadStatus::Completed,
        total_bytes: Some(2048),
        downloaded_bytes: 2048,
        ..Default::default()
    };
    let listed = tracker
        .observe(&second_finished)
        .expect("the next finish is reported");
    assert_eq!(listed.id, 2, "ids increase, so no listed row is reused");
    assert_eq!(listed.filename, "next.bin");
    assert_eq!(listed.total_bytes, 2048);

    let mut tracker = HistoryTracker::new(1);
    let listed = tracker
        .observe(&DownloadSnapshot {
            session_id: 7,
            status: DownloadStatus::Completed,
            downloaded_bytes: 512,
            ..Default::default()
        })
        .expect("a finish without a known total is still listed");
    assert_eq!(listed.total_bytes, 512);

    let mut tracker = HistoryTracker::new(9);
    let listed = tracker
        .observe(&finished)
        .expect("the download is listed above the kept ids");
    assert_eq!(listed.id, 9, "a new row continues after the kept history");

    let mut tracker = HistoryTracker::new(1);
    assert_eq!(
        tracker.observe(&DownloadSnapshot {
            session_id: 5,
            status: DownloadStatus::Downloading,
            ..Default::default()
        }),
        None
    );
    assert_eq!(
        tracker.observe(&DownloadSnapshot {
            session_id: 6,
            status: DownloadStatus::Connecting,
            ..Default::default()
        }),
        None
    );

    let mut tracker = HistoryTracker::new(1);
    tracker.observe(&finished);
    assert_eq!(
        tracker.observe(&DownloadSnapshot {
            session_id: 2,
            status: DownloadStatus::Idle,
            ..Default::default()
        }),
        None
    );

    let mut tracker = HistoryTracker::new(1);
    tracker.observe(&finished);
    tracker.clear_completed();
    assert_eq!(tracker.observe(&next_session), None);
}

#[test]
fn completed_download_persists_immediately_without_next_session() {
    let file = TempFile::new("immediate-save");
    let mut store = HistoryStore::load_from(file.path().to_path_buf());
    let mut tracker = HistoryTracker::new(store.next_id());

    let finished = DownloadSnapshot {
        session_id: 1,
        url: "https://example.com/testfile.iso".into(),
        filename: "testfile.iso".into(),
        save_path: PathBuf::from("testfile.iso"),
        status: DownloadStatus::Completed,
        total_bytes: Some(1_048_576),
        downloaded_bytes: 1_048_576,
        ..Default::default()
    };

    let entry = tracker
        .observe(&finished)
        .expect("first completed observation emits entry");
    store.record(entry).unwrap();

    let reloaded = HistoryStore::load_from(file.path().to_path_buf());
    assert_eq!(reloaded.entries().len(), 1);
    assert_eq!(reloaded.entries()[0].filename, "testfile.iso");
}
