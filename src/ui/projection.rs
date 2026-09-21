use super::format::{format_bytes, format_eta, format_speed};
use super::view::MainWindow;
use crate::engine::{DownloadSnapshot, DownloadStatus};
use std::path::Path;
use tokio::sync::watch;

pub(super) fn should_project_snapshot<T>(snapshot: &watch::Ref<'_, T>, initial: &mut bool) -> bool {
    // Unlike Receiver::has_changed, this reports a final unseen value after closure.
    let should_project = *initial || snapshot.has_changed();
    *initial = false;
    should_project
}

pub(super) fn update_window_state(window: &MainWindow, snap: &DownloadSnapshot) {
    let (is_downloading, is_paused, is_completed) = match &snap.status {
        DownloadStatus::Connecting => (true, false, false),
        DownloadStatus::Downloading => (true, false, false),
        DownloadStatus::Paused => (false, true, false),
        DownloadStatus::Completed => (false, false, true),
        DownloadStatus::Failed(_) => (false, snap.resumable, false),
        DownloadStatus::Idle => (false, false, false),
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

    if let Some(ref prompt) = snap.duplicate {
        if !window.get_show_duplicate_dialog() {
            window.set_duplicate_selected_option(0);
            window.set_duplicate_remember(false);
            window.set_show_add_dialog(false);
            window.set_show_duplicate_dialog(true);
        }
        window.set_duplicate_url(prompt.url.clone().into());
        window.set_duplicate_filename(prompt.filename.clone().into());
        window.set_duplicate_is_link(prompt.link_duplicate);
        let size_str = prompt.existing_bytes.map(format_bytes).unwrap_or_default();
        window.set_duplicate_existing_size(size_str.into());
    } else if window.get_show_duplicate_dialog() {
        window.set_show_duplicate_dialog(false);
    }
}
