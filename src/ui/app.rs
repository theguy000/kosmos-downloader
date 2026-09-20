use super::platform::{default_download_directory, open_file};
use super::projection::{should_project_snapshot, update_window_state};
use super::view::MainWindow;
use crate::engine::{DownloadAction, DownloadSnapshot, DownloadStatus};
use slint::ComponentHandle;
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DeleteTarget {
    pub(super) session_id: u64,
    pub(super) status: DownloadStatus,
    pub(super) completed_only: bool,
}

impl DeleteTarget {
    fn displayed(snapshot: &DownloadSnapshot) -> Option<Self> {
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

fn open_delete_confirmation(
    window: &MainWindow,
    displayed: &RefCell<Option<DeleteTarget>>,
    pending: &RefCell<Option<DeleteTarget>>,
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
    *pending.borrow_mut() = Some(request);
    window.set_delete_filename(window.get_active_filename());
    window.set_delete_completed_only(completed_only);
    window.set_delete_target_completed(target_completed);
    window.set_delete_file(false);
    window.set_show_delete_dialog(true);
}

pub fn run_app(
    action_tx: mpsc::Sender<DownloadAction>,
    snapshot_rx: watch::Receiver<DownloadSnapshot>,
) -> Result<(), Box<dyn std::error::Error>> {
    let main_window = MainWindow::new()?;
    let displayed_delete_target = Rc::new(RefCell::new(None));
    let pending_delete = Rc::new(RefCell::new(None));

    let default_dir = default_download_directory();
    main_window.set_dest_dir_text(default_dir.to_string_lossy().into_owned().into());

    {
        let window_weak = main_window.as_weak();
        main_window.on_browse_folder(move || {
            if let Some(window) = window_weak.upgrade() {
                let current = window.get_dest_dir_text().to_string();
                if let Some(folder) = rfd::FileDialog::new().set_directory(&current).pick_folder() {
                    window.set_dest_dir_text(folder.to_string_lossy().into_owned().into());
                }
            }
        });
    }

    {
        let tx = action_tx.clone();
        let window_weak = main_window.as_weak();
        main_window.on_start_download(move || {
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
                    } else {
                        window.set_show_add_dialog(true);
                    }
                }
            }
        });
    }

    {
        let tx = action_tx.clone();
        let window_weak = main_window.as_weak();
        main_window.on_pause_download(move || {
            if let Some(window) = window_weak.upgrade() {
                send_action(&window, &tx, DownloadAction::Pause);
            }
        });
    }

    {
        let tx = action_tx.clone();
        let window_weak = main_window.as_weak();
        main_window.on_resume_download(move || {
            if let Some(window) = window_weak.upgrade() {
                send_action(&window, &tx, DownloadAction::Resume);
            }
        });
    }

    {
        let displayed = displayed_delete_target.clone();
        let pending = pending_delete.clone();
        let window_weak = main_window.as_weak();
        main_window.on_request_delete_selected(move || {
            if let Some(window) = window_weak.upgrade() {
                open_delete_confirmation(&window, &displayed, &pending, false);
            }
        });
    }

    {
        let displayed = displayed_delete_target.clone();
        let pending = pending_delete.clone();
        let window_weak = main_window.as_weak();
        main_window.on_request_delete_completed(move || {
            if let Some(window) = window_weak.upgrade() {
                open_delete_confirmation(&window, &displayed, &pending, true);
            }
        });
    }

    {
        let pending = pending_delete.clone();
        main_window.on_dismiss_delete(move || {
            *pending.borrow_mut() = None;
        });
    }

    {
        let tx = action_tx.clone();
        let rx = snapshot_rx.clone();
        let pending = pending_delete.clone();
        let window_weak = main_window.as_weak();
        main_window.on_confirm_delete(move |delete_file| {
            let Some(window) = window_weak.upgrade() else {
                return;
            };
            let Some(mut request) = pending.borrow_mut().take() else {
                return;
            };
            let snapshot = rx.borrow();
            if snapshot.status == DownloadStatus::Idle || snapshot.session_id != request.session_id
            {
                window.invoke_close_delete_dialog();
                return;
            }
            request.status = snapshot.status.clone();

            if send_action(&window, &tx, request.action(delete_file)) {
                window.set_active_error_message("".into());
                window.invoke_close_delete_dialog();
            }
        });
    }

    {
        let rx = snapshot_rx.clone();
        main_window.on_open_file(move || {
            let save_path = rx.borrow().save_path.clone();
            if save_path.exists() {
                open_file(&save_path);
            }
        });
    }

    let timer = slint::Timer::default();
    {
        let window_weak = main_window.as_weak();
        let displayed = displayed_delete_target;
        let pending = pending_delete;
        let mut rx = snapshot_rx;
        let mut initial_snapshot = true;
        timer.start(
            slint::TimerMode::Repeated,
            Duration::from_millis(80),
            move || {
                let Some(window) = window_weak.upgrade() else {
                    return;
                };

                let snap = rx.borrow_and_update();
                if should_project_snapshot(&snap, &mut initial_snapshot) {
                    let projected_target = DeleteTarget::displayed(&snap);
                    if let Some(ref mut request) = *pending.borrow_mut() {
                        if snap.status == DownloadStatus::Idle
                            || snap.session_id != request.session_id
                            || (request.completed_only
                                && !matches!(snap.status, DownloadStatus::Completed))
                        {
                            *pending.borrow_mut() = None;
                            window.invoke_close_delete_dialog();
                        } else {
                            request.status = snap.status.clone();
                            window.set_delete_target_completed(matches!(
                                snap.status,
                                DownloadStatus::Completed
                            ));
                        }
                    }
                    update_window_state(&window, &snap);
                    *displayed.borrow_mut() = projected_target;
                }
            },
        );
    }

    main_window.run()?;
    Ok(())
}

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
