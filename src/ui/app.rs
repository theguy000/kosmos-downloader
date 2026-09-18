use super::platform::{default_download_directory, open_file};
use super::projection::{should_project_snapshot, update_window_state};
use super::view::MainWindow;
use crate::engine::{DownloadAction, DownloadSnapshot};
use slint::ComponentHandle;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

pub fn run_app(
    action_tx: mpsc::Sender<DownloadAction>,
    snapshot_rx: watch::Receiver<DownloadSnapshot>,
) -> Result<(), Box<dyn std::error::Error>> {
    let main_window = MainWindow::new()?;

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
        let tx = action_tx.clone();
        let window_weak = main_window.as_weak();
        main_window.on_cancel_download(move || {
            if let Some(window) = window_weak.upgrade() {
                send_action(&window, &tx, DownloadAction::Cancel);
            }
        });
    }

    {
        let rx = snapshot_rx.clone();
        main_window.on_open_file(move || {
            let snap = rx.borrow();
            if snap.save_path.exists() {
                open_file(&snap.save_path);
            }
        });
    }

    let timer = slint::Timer::default();
    {
        let window_weak = main_window.as_weak();
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
                    update_window_state(&window, &snap);
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
