mod format;
mod platform;
mod view;

pub use self::format::{format_bytes, format_eta, format_speed};
pub use self::view::*;

use self::platform::{default_download_directory, open_file};
use crate::engine::{DownloadAction, DownloadSnapshot, DownloadStatus};
use std::path::{Path, PathBuf};
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

fn send_action(
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

fn should_project_snapshot<T>(snapshot: &watch::Ref<'_, T>, initial: &mut bool) -> bool {
    // Unlike Receiver::has_changed, this reports a final unseen value after closure.
    let should_project = *initial || snapshot.has_changed();
    *initial = false;
    should_project
}

fn update_window_state(window: &MainWindow, snap: &DownloadSnapshot) {
    let (is_downloading, is_paused, is_completed) = match &snap.status {
        DownloadStatus::Connecting => (true, false, false),
        DownloadStatus::Downloading => (true, false, false),
        DownloadStatus::Paused => (false, true, false),
        DownloadStatus::Completed => (false, false, true),
        DownloadStatus::Idle | DownloadStatus::Failed(_) => (false, false, false),
    };

    window.set_has_active_download(!matches!(snap.status, DownloadStatus::Idle));
    window.set_is_downloading(is_downloading);
    window.set_is_paused(is_paused);
    window.set_is_completed(is_completed);
    window.set_is_resumable(snap.resumable);

    window.set_active_filename(snap.filename.clone().into());
    let ext = Path::new(&snap.filename)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    let file_type = match ext.as_str() {
        "exe" | "msi" | "bat" | "cmd" => "exe",
        "zip" | "rar" | "7z" | "tar" | "gz" | "iso" => "zip",
        "mp4" | "mkv" | "avi" | "mov" | "webm" => "video",
        "mp3" | "wav" | "flac" | "aac" | "ogg" => "audio",
        _ => "doc",
    };
    window.set_active_file_type(file_type.into());

    let (badge, color, err) = match &snap.status {
        DownloadStatus::Idle => (
            "Idle",
            slint::Color::from_rgb_u8(113, 113, 122),
            String::new(),
        ),
        DownloadStatus::Connecting => (
            "Connecting",
            slint::Color::from_rgb_u8(245, 158, 11),
            String::new(),
        ),
        DownloadStatus::Downloading => (
            "Downloading",
            slint::Color::from_rgb_u8(16, 185, 129),
            String::new(),
        ),
        DownloadStatus::Paused => (
            "Stopped",
            slint::Color::from_rgb_u8(245, 158, 11),
            String::new(),
        ),
        DownloadStatus::Completed => (
            "Complete",
            slint::Color::from_rgb_u8(16, 185, 129),
            String::new(),
        ),
        DownloadStatus::Failed(msg) => (
            "Failed",
            slint::Color::from_rgb_u8(239, 68, 68),
            msg.clone(),
        ),
    };

    let progress = match snap.total_bytes {
        Some(total) if total > 0 => (snap.downloaded_bytes as f32 / total as f32).clamp(0.0, 1.0),
        _ if is_completed => 1.0,
        _ => 0.0,
    };

    let status_str = if is_downloading {
        format!("{badge} ({:.1}%)", progress * 100.0)
    } else {
        badge.to_string()
    };
    window.set_active_status(status_str.into());
    window.set_active_status_color(color);
    window.set_active_error_message(err.into());

    window.set_active_transfer_rate(format_speed(snap.speed_bytes_per_sec).into());
    let size_str = match snap.total_bytes {
        Some(total) => format!(
            "{} / {}",
            format_bytes(snap.downloaded_bytes),
            format_bytes(total)
        ),
        None => format_bytes(snap.downloaded_bytes),
    };
    window.set_active_size(size_str.into());
    window.set_active_time_left(format_eta(snap.eta_seconds).into());
}

#[cfg(test)]
mod tests {
    use tokio::sync::watch;

    #[test]
    fn snapshot_projection_handles_initial_changed_and_closed_updates() {
        let (tx, mut rx) = watch::channel(0_u8);
        let mut initial = true;

        {
            let snapshot = rx.borrow_and_update();
            assert!(super::should_project_snapshot(&snapshot, &mut initial));
            assert_eq!(*snapshot, 0);
        }
        {
            let snapshot = rx.borrow_and_update();
            assert!(!super::should_project_snapshot(&snapshot, &mut initial));
        }

        assert!(tx.send(1).is_ok());
        {
            let snapshot = rx.borrow_and_update();
            assert!(super::should_project_snapshot(&snapshot, &mut initial));
            assert_eq!(*snapshot, 1);
        }
        {
            let snapshot = rx.borrow_and_update();
            assert!(!super::should_project_snapshot(&snapshot, &mut initial));
        }

        assert!(tx.send(2).is_ok());
        drop(tx);
        assert!(rx.has_changed().is_err());
        {
            let snapshot = rx.borrow_and_update();
            assert!(super::should_project_snapshot(&snapshot, &mut initial));
            assert_eq!(*snapshot, 2);
        }
        {
            let snapshot = rx.borrow_and_update();
            assert!(!super::should_project_snapshot(&snapshot, &mut initial));
        }
    }
}
