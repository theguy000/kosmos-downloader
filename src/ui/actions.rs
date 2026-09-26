use super::delete::clear_active_completed;
use super::state::AppState;
use super::table::{remove_row_by_id, update_selection_state};
use super::view::MainWindow;
use crate::engine::{DownloadAction, DuplicateChoice};
use slint::ComponentHandle;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

pub(super) fn send_action(
    window: &MainWindow,
    tx: &mpsc::Sender<DownloadAction>,
    action: DownloadAction,
) -> bool {
    let message = match tx.try_send(action) {
        Ok(()) => "",
        Err(mpsc::error::TrySendError::Full(_)) => "Download engine is busy. Try the action again.",
        Err(mpsc::error::TrySendError::Closed(_)) => {
            "Download engine is unavailable. Restart the app."
        }
    };
    window.set_action_error_message(message.into());
    message.is_empty()
}

pub(super) fn bind_action_handlers(window: &MainWindow, state: &AppState) {
    {
        let window_weak = window.as_weak();
        window.on_browse_folder(move || {
            if let Some(window) = window_weak.upgrade() {
                let current = window.get_dest_dir_text();
                let current_str = current.as_str();
                if let Some(folder) = rfd::FileDialog::new()
                    .set_directory(current_str)
                    .pick_folder()
                {
                    window.set_dest_dir_text(folder.to_string_lossy().as_ref().into());
                }
            }
        });
    }

    {
        let window_weak = window.as_weak();
        let save_settings = state.save_settings.clone();
        window.on_add_dialog_opened(move || {
            if let Some(window) = window_weak.upgrade() {
                let url = window.get_url_text();
                let dir = save_settings.borrow().path_for_url(url.trim());
                window.set_dest_dir_text(dir.to_string_lossy().as_ref().into());
            }
        });
    }

    {
        let window_weak = window.as_weak();
        let save_settings = state.save_settings.clone();
        window.on_url_text_changed(move |new_url| {
            if let Some(window) = window_weak.upgrade() {
                let current_dest = window.get_dest_dir_text();
                let current_path = Path::new(current_dest.as_str());
                let settings = save_settings.borrow();
                let matches_known =
                    current_dest.is_empty() || settings.is_managed_path(current_path);

                if matches_known {
                    let target_dir = settings.path_for_url(new_url.trim());
                    window.set_dest_dir_text(target_dir.to_string_lossy().as_ref().into());
                }
            }
        });
    }

    {
        let tx = state.action_tx.clone();
        let window_weak = window.as_weak();
        window.on_start_download(move || {
            if let Some(window) = window_weak.upgrade() {
                let url = window.get_url_text().to_string().trim().to_string();
                let dest = window.get_dest_dir_text().to_string().trim().to_string();
                let streams = window.get_streams_count().round() as usize;

                if !url.is_empty() {
                    if send_action(
                        &window,
                        &tx,
                        DownloadAction::Start {
                            url,
                            save_path: PathBuf::from(dest),
                            num_chunks: streams.clamp(1, 16),
                        },
                    ) {
                        window.set_selected_row(0);
                        window.set_selected_category(0);
                        update_selection_state(&window, 0);
                    } else {
                        window.set_show_add_dialog(true);
                    }
                }
            }
        });
    }

    {
        let tx = state.action_tx.clone();
        let window_weak = window.as_weak();
        window.on_pause_download(move || {
            if let Some(window) = window_weak.upgrade() {
                send_action(&window, &tx, DownloadAction::Pause);
            }
        });
    }

    {
        let tx = state.action_tx.clone();
        let window_weak = window.as_weak();
        window.on_resume_download(move || {
            if let Some(window) = window_weak.upgrade() {
                send_action(&window, &tx, DownloadAction::Resume);
            }
        });
    }

    {
        let tx = state.action_tx.clone();
        let rx = state.snapshot_rx.clone();
        let window_weak = window.as_weak();
        let tracker = state.history_tracker.clone();
        let store = state.history_store.clone();
        let history = state.download_history.clone();
        window.on_resolve_duplicate(move |option, remember| {
            let Some(window) = window_weak.upgrade() else {
                return;
            };
            let snap = rx.borrow();
            let Some(ref prompt) = snap.duplicate else {
                return;
            };
            let session_id = prompt.session_id;
            let choice = match option {
                1 => DuplicateChoice::Numbered,
                2 => {
                    if let Some(entry) = clear_active_completed(&window, &tracker) {
                        let _ = store.borrow_mut().remove(entry.id);
                        remove_row_by_id(&history, entry.id);
                    }
                    DuplicateChoice::Overwrite
                }
                _ => DuplicateChoice::UseExisting,
            };
            if remember {
                send_action(
                    &window,
                    &tx,
                    DownloadAction::SetDuplicatePreference {
                        choice: Some(choice),
                    },
                );
            }
            send_action(
                &window,
                &tx,
                DownloadAction::ResolveDuplicate {
                    session_id,
                    choice: Some(choice),
                },
            );
        });
    }

    {
        let tx = state.action_tx.clone();
        let rx = state.snapshot_rx.clone();
        let window_weak = window.as_weak();
        window.on_dismiss_duplicate(move || {
            let Some(window) = window_weak.upgrade() else {
                return;
            };
            let snap = rx.borrow();
            let Some(ref prompt) = snap.duplicate else {
                return;
            };
            let session_id = prompt.session_id;
            send_action(
                &window,
                &tx,
                DownloadAction::ResolveDuplicate {
                    session_id,
                    choice: None,
                },
            );
        });
    }
}
