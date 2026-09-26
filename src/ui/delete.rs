use super::actions::send_action;
use super::state::AppState;
use super::table::{HistoryTracker, remove_row_by_id};
use super::view::{MainWindow, TableItem};
use crate::engine::{DownloadAction, DownloadSnapshot, DownloadStatus};
use crate::history::{HistoryEntry, HistoryStore};
use slint::ComponentHandle;
use std::cell::RefCell;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DeleteTarget {
    pub(super) session_id: u64,
    pub(super) status: DownloadStatus,
    pub(super) completed_only: bool,
}

impl DeleteTarget {
    pub(super) fn displayed(snapshot: &DownloadSnapshot) -> Option<Self> {
        (!matches!(snapshot.status, DownloadStatus::Idle)).then(|| Self {
            session_id: snapshot.session_id,
            status: snapshot.status.clone(),
            completed_only: false,
        })
    }

    pub(super) fn action(&self, delete_file: bool) -> DownloadAction {
        DownloadAction::Remove {
            expected_session_id: self.session_id,
            expected_status: self.status.clone(),
            delete_file,
            completed_only: self.completed_only,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum PendingDelete {
    Active(DeleteTarget),
    History {
        id: i32,
        filename: String,
        save_path: PathBuf,
    },
}

pub(super) fn open_active_delete_confirmation(
    window: &MainWindow,
    displayed: &RefCell<Option<DeleteTarget>>,
    pending: &RefCell<Option<PendingDelete>>,
    completed_only: bool,
) {
    let Some(mut request) = displayed.borrow().clone() else {
        return;
    };
    if completed_only && !matches!(request.status, DownloadStatus::Completed) {
        return;
    }

    let target_completed = matches!(request.status, DownloadStatus::Completed);
    request.completed_only = completed_only;
    *pending.borrow_mut() = Some(PendingDelete::Active(request));
    window.set_delete_filename(window.get_active_filename());
    window.set_delete_completed_only(completed_only);
    window.set_delete_target_completed(target_completed);
    window.set_delete_file(false);
    window.set_show_delete_dialog(true);
}

pub(super) fn open_history_delete_confirmation(
    window: &MainWindow,
    pending: &RefCell<Option<PendingDelete>>,
    entry: &HistoryEntry,
) {
    *pending.borrow_mut() = Some(PendingDelete::History {
        id: entry.id,
        filename: entry.filename.clone(),
        save_path: entry.save_path.clone(),
    });
    window.set_delete_filename(entry.filename.clone().into());
    window.set_delete_completed_only(false);
    window.set_delete_target_completed(true);
    window.set_delete_file(false);
    window.set_show_delete_dialog(true);
}

pub(super) fn clear_active_completed(
    window: &MainWindow,
    tracker: &RefCell<HistoryTracker>,
) -> Option<HistoryEntry> {
    let entry = tracker.borrow_mut().clear_completed();
    if entry.is_some() {
        window.set_completed_listed(false);
    }
    entry
}

/// Removes a listed download from the log, the table, and the selection.
pub(super) fn complete_history_delete(
    window: &MainWindow,
    tracker: &RefCell<HistoryTracker>,
    store: &mut HistoryStore,
    history: &slint::VecModel<TableItem>,
    id: i32,
    failure: Option<String>,
) {
    if let Some(message) = failure {
        window.set_action_error_message(message.into());
    } else {
        if let Err(error) = store.remove(id) {
            window.set_action_error_message(
                format!("Could not save download history: {error}").into(),
            );
        }
        remove_row_by_id(history, id);
        if tracker
            .borrow()
            .completed()
            .as_ref()
            .is_some_and(|entry| entry.id == id)
        {
            clear_active_completed(window, tracker);
        }
        if window.get_selected_row() == id {
            window.set_selected_row(-1);
            window.set_history_row_selected(false);
        }
    }
    window.invoke_close_delete_dialog();
}

/// Deletes the file of a listed download off the UI thread, then reports back on the event loop.
pub(super) fn spawn_history_file_delete(
    window: slint::Weak<MainWindow>,
    id: i32,
    save_path: PathBuf,
) {
    let display_path = save_path.display().to_string();
    tokio::spawn(async move {
        let failure =
            match tokio::task::spawn_blocking(move || std::fs::remove_file(save_path)).await {
                Ok(removal) => history_delete_failure(removal),
                Err(error) => Some(std::io::Error::other(error)),
            };
        let message = failure
            .map(|error| format!("Could not delete {display_path}: {error}"))
            .unwrap_or_default();
        let failed = !message.is_empty();
        let _ = slint::invoke_from_event_loop(move || {
            if let Some(window) = window.upgrade() {
                window.invoke_history_delete_finished(id, failed, message.into());
            }
        });
    });
}

/// A file that is already gone counts as deleted; anything else is reported.
pub(super) fn history_delete_failure(removal: std::io::Result<()>) -> Option<std::io::Error> {
    match removal {
        Ok(()) => None,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => Some(error),
    }
}

pub(super) fn bind_delete_handlers(window: &MainWindow, state: &AppState) {
    {
        let displayed = state.displayed_delete_target.clone();
        let pending = state.pending_delete.clone();
        let store = state.history_store.clone();
        let window_weak = window.as_weak();
        window.on_request_delete_selected(move || {
            let Some(window) = window_weak.upgrade() else {
                return;
            };
            let selected = window.get_selected_row();
            if selected == 0 {
                open_active_delete_confirmation(&window, &displayed, &pending, false);
            } else if let Some(entry) = store.borrow().entries().iter().find(|e| e.id == selected) {
                open_history_delete_confirmation(&window, &pending, entry);
            }
        });
    }

    {
        let displayed = state.displayed_delete_target.clone();
        let pending = state.pending_delete.clone();
        let window_weak = window.as_weak();
        window.on_request_delete_completed(move || {
            if let Some(window) = window_weak.upgrade() {
                open_active_delete_confirmation(&window, &displayed, &pending, true);
            }
        });
    }

    {
        let pending = state.pending_delete.clone();
        window.on_dismiss_delete(move || {
            *pending.borrow_mut() = None;
        });
    }

    {
        let tx = state.action_tx.clone();
        let rx = state.snapshot_rx.clone();
        let pending = state.pending_delete.clone();
        let tracker = state.history_tracker.clone();
        let store = state.history_store.clone();
        let history = state.download_history.clone();
        let window_weak = window.as_weak();
        window.on_confirm_delete(move |delete_file| {
            let Some(window) = window_weak.upgrade() else {
                return;
            };
            let Some(target) = pending.borrow_mut().take() else {
                return;
            };
            match target {
                PendingDelete::Active(mut request) => {
                    let snapshot = rx.borrow();
                    if snapshot.status == DownloadStatus::Idle
                        || snapshot.session_id != request.session_id
                    {
                        window.invoke_close_delete_dialog();
                        return;
                    }
                    request.status = snapshot.status.clone();

                    if let Some(entry) = clear_active_completed(&window, &tracker) {
                        let _ = store.borrow_mut().remove(entry.id);
                        remove_row_by_id(&history, entry.id);
                    }

                    if send_action(&window, &tx, request.action(delete_file)) {
                        window.set_active_error_message("".into());
                        window.invoke_close_delete_dialog();
                    }
                }
                PendingDelete::History { id, save_path, .. } => {
                    if delete_file {
                        spawn_history_file_delete(window.as_weak(), id, save_path);
                    } else {
                        complete_history_delete(
                            &window,
                            &tracker,
                            &mut store.borrow_mut(),
                            &history,
                            id,
                            None,
                        );
                    }
                }
            }
        });
    }

    {
        let tracker = state.history_tracker.clone();
        let store = state.history_store.clone();
        let history = state.download_history.clone();
        let window_weak = window.as_weak();
        window.on_history_delete_finished(move |id, failed, message| {
            let Some(window) = window_weak.upgrade() else {
                return;
            };
            complete_history_delete(
                &window,
                &tracker,
                &mut store.borrow_mut(),
                &history,
                id,
                failed.then(|| message.to_string()),
            );
        });
    }
}
