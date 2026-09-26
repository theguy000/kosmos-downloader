use super::platform::open_file;
use super::projection::{category_matches_id, sort_items};
use super::state::AppState;
use super::view::{MainWindow, TableItem};
use crate::engine::{DownloadSnapshot, DownloadStatus};
use crate::history::{HistoryEntry, now_unix_ms};
use slint::ComponentHandle;
use slint::Model;
use std::path::Path;

/// Follows the active session and reports the download it finishes.
pub(super) struct HistoryTracker {
    /// Id for the next listed download. Never 0, which marks the active download.
    next_item_id: i32,
    session_id: u64,
    completed: Option<HistoryEntry>,
}

impl HistoryTracker {
    pub(super) fn new(loaded_next_id: i32) -> Self {
        Self {
            next_item_id: loaded_next_id,
            session_id: 0,
            completed: None,
        }
    }

    pub(super) fn completed(&self) -> Option<&HistoryEntry> {
        self.completed.as_ref()
    }

    /// Records `snap` and returns the finished download.
    ///
    /// The first snapshot a session reports `Completed` yields its entry, so it can be persisted
    /// and listed right away. A later session clears the kept entry without re-listing it.
    pub(super) fn observe(&mut self, snap: &DownloadSnapshot) -> Option<HistoryEntry> {
        if snap.session_id != self.session_id {
            self.session_id = snap.session_id;
            self.completed = None;
        }

        match snap.status {
            // The first snapshot to report the finish is the one kept, so the listed row keeps
            // the id and the completion time of that first report.
            DownloadStatus::Completed if self.completed.is_none() => {
                let entry = finished_download(self.next_item_id, snap);
                self.next_item_id = self.next_item_id.saturating_add(1);
                self.completed = Some(entry.clone());
                Some(entry)
            }
            DownloadStatus::Idle => {
                self.completed = None;
                None
            }
            _ => None,
        }
    }

    /// Drops the pending download, matching a duplicate answer that replaces the file.
    pub(super) fn clear_completed(&mut self) -> Option<HistoryEntry> {
        self.completed.take()
    }
}

pub(super) fn finished_download(id: i32, snap: &DownloadSnapshot) -> HistoryEntry {
    HistoryEntry {
        id,
        url: snap.url.clone(),
        filename: snap.filename.clone(),
        save_path: snap.save_path.clone(),
        total_bytes: snap.total_bytes.unwrap_or(snap.downloaded_bytes),
        completed_unix_ms: now_unix_ms(),
    }
}

pub(super) fn update_selection_state(window: &MainWindow, selected_id: i32) {
    if selected_id <= 0 {
        window.set_history_row_selected(false);
        return;
    }
    let category = window.get_selected_category();
    let is_valid = window.get_sample_downloads().iter().any(|item| {
        item.id == selected_id && category_matches_id(category, item.category_id, true)
    });
    window.set_history_row_selected(is_valid);
}

/// Ids of the rows the table lists, top to bottom: the active download, then the listed
/// downloads the current category shows.
fn listed_row_ids(window: &MainWindow) -> Vec<i32> {
    let category = window.get_selected_category();
    let mut ids = Vec::new();
    if window.get_active_row_visible() {
        ids.push(0);
    }
    ids.extend(
        window
            .get_sample_downloads()
            .iter()
            .filter(|item| category_matches_id(category, item.category_id, true))
            .map(|item| item.id),
    );
    ids
}

/// Moves the selection by `step` listed rows, stopping at either end of the list.
pub(super) fn step_selection(window: &MainWindow, step: i32) {
    let ids = listed_row_ids(window);
    if ids.is_empty() {
        return;
    }

    let next = match ids.iter().position(|id| *id == window.get_selected_row()) {
        Some(index) => (index as i32 + step).clamp(0, ids.len() as i32 - 1) as usize,
        // Nothing is selected: the first step enters the list at the matching end.
        None if step < 0 => ids.len() - 1,
        None => 0,
    };
    let id = ids[next];
    window.set_selected_row(id);
    update_selection_state(window, id);
}

pub(super) fn remove_row_by_id(model: &slint::VecModel<TableItem>, id: i32) {
    for idx in 0..model.row_count() {
        if let Some(item) = model.row_data(idx)
            && item.id == id
        {
            model.remove(idx);
            break;
        }
    }
}

/// Reorders the model for `column`. An unknown column leaves the order alone.
pub(super) fn resort(history: &slint::VecModel<TableItem>, column: i32, ascending: bool) {
    let mut rows: Vec<TableItem> = history.iter().collect();
    sort_items(&mut rows, column, ascending);
    history.set_vec(rows);
}

/// Applies a header click: toggles the direction when the same column is clicked again,
/// restarts ascending for a new column, reorders the model, and records the state on the window.
pub(super) fn apply_sort_request(
    window: &MainWindow,
    history: &slint::VecModel<TableItem>,
    column: i32,
) {
    let ascending = window.get_sort_column() != column || !window.get_sort_ascending();
    window.set_sort_column(column);
    window.set_sort_ascending(ascending);
    resort(history, column, ascending);
}

pub(super) fn bind_table_handlers(window: &MainWindow, state: &AppState) {
    {
        let window_weak = window.as_weak();
        window.on_row_selected(move |id| {
            let Some(window) = window_weak.upgrade() else {
                return;
            };
            update_selection_state(&window, id);
        });
    }

    {
        let window_weak = window.as_weak();
        window.on_selected_category_changed(move || {
            if let Some(window) = window_weak.upgrade() {
                update_selection_state(&window, window.get_selected_row());
            }
        });
    }

    {
        let window_weak = window.as_weak();
        window.on_step_selection(move |step| {
            if let Some(window) = window_weak.upgrade() {
                step_selection(&window, step);
            }
        });
    }

    {
        let history = state.download_history.clone();
        let window_weak = window.as_weak();
        window.on_sort_requested(move |column| {
            if let Some(window) = window_weak.upgrade() {
                apply_sort_request(&window, &history, column);
            }
        });
    }

    {
        let rx = state.snapshot_rx.clone();
        let store = state.history_store.clone();
        let window_weak = window.as_weak();
        window.on_open_file(move || {
            let Some(window) = window_weak.upgrade() else {
                return;
            };
            let selected = window.get_selected_row();
            // Row 0 is the active download; every other row is a listed history entry.
            let path = if selected == 0 {
                let snap = rx.borrow();
                (snap.status == DownloadStatus::Completed).then(|| snap.save_path.clone())
            } else {
                store.borrow().path_for(selected).map(Path::to_path_buf)
            };
            if let Some(path) = path {
                open_file(&path);
            }
        });
    }
}
