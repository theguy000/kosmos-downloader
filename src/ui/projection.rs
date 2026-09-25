use super::format::{format_bytes, format_eta, format_speed};
use super::view::{MainWindow, Palette};
use crate::engine::{DownloadSnapshot, DownloadStatus};
use crate::history::{HistoryEntry, downloaded_label};
use slint::ComponentHandle;
use std::path::Path;
use tokio::sync::watch;

pub(super) fn should_project_snapshot<T>(snapshot: &watch::Ref<'_, T>, initial: &mut bool) -> bool {
    // Unlike Receiver::has_changed, this reports a final unseen value after closure.
    let should_project = *initial || snapshot.has_changed();
    *initial = false;
    should_project
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FileType {
    Executable,
    Compressed,
    Video,
    Audio,
    Document,
}

impl FileType {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Executable => "exe",
            Self::Compressed => "zip",
            Self::Video => "video",
            Self::Audio => "audio",
            Self::Document => "doc",
        }
    }
}

pub(super) fn file_type_from_filename(filename: &str) -> FileType {
    let ext = Path::new(filename)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    match ext.as_str() {
        "exe" | "msi" | "bat" | "cmd" => FileType::Executable,
        "zip" | "rar" | "7z" | "tar" | "gz" | "iso" => FileType::Compressed,
        "mp4" | "mkv" | "avi" | "mov" | "webm" => FileType::Video,
        "mp3" | "wav" | "flac" | "aac" | "ogg" => FileType::Audio,
        _ => FileType::Document,
    }
}

pub(super) fn category_matches(category: i32, file_type: FileType, completed: bool) -> bool {
    match category {
        0 => true,
        1 => file_type == FileType::Compressed,
        2 => file_type == FileType::Document,
        3 => file_type == FileType::Audio,
        4 => file_type == FileType::Executable,
        5 => file_type == FileType::Video,
        6 => !completed,
        7 => completed,
        _ => false,
    }
}

pub(super) fn history_table_item(entry: &HistoryEntry) -> super::view::TableItem {
    super::view::TableItem {
        id: entry.id,
        filename: entry.filename.clone().into(),
        file_type: file_type_from_filename(&entry.filename).as_str().into(),
        size_text: format_bytes(entry.total_bytes).into(),
        status_text: "Complete".into(),
        time_left_text: "--:--".into(),
        transfer_rate_text: "0 KB/s".into(),
        downloaded_text: downloaded_label(entry.completed_unix_ms).into(),
        size_bytes: entry.total_bytes as f32,
    }
}

/// Reorders the listed rows for a header sort. Columns 0, 1 and 3 are sortable (File Name,
/// Size, Date Added); any other column leaves the order untouched. Ties keep a stable order
/// by row id.
pub(super) fn sort_items(items: &mut [super::view::TableItem], column: i32, ascending: bool) {
    use std::cmp::Ordering;

    match column {
        0 => items.sort_by_cached_key(|a| (a.filename.to_lowercase(), a.id)),
        1 => items.sort_by(|a, b| {
            a.size_bytes
                .partial_cmp(&b.size_bytes)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        }),
        // ponytail: history IDs strictly increment with completion order; sorting by id avoids
        // adding a timestamp field to TableItem.
        3 => items.sort_by_key(|a| a.id),
        _ => return,
    }

    if !ascending {
        items.reverse();
    }
}

pub(super) fn update_window_state(window: &MainWindow, snap: &DownloadSnapshot) {
    let (is_downloading, is_paused, is_completed) = match &snap.status {
        DownloadStatus::Connecting | DownloadStatus::Downloading => (true, false, false),
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
    let file_type = file_type_from_filename(&snap.filename);
    window.set_active_file_type(file_type.as_str().into());

    let (idle_color, warning_color, success_color, danger_color) = {
        let palette = window.global::<Palette>();
        (
            palette.get_text_faint(),
            palette.get_warning(),
            palette.get_success(),
            palette.get_danger_text(),
        )
    };

    let (badge, color, err) = match &snap.status {
        DownloadStatus::Idle => ("Idle", idle_color, String::new()),
        DownloadStatus::Connecting => ("Connecting", warning_color, String::new()),
        DownloadStatus::Downloading => ("Downloading", success_color, String::new()),
        DownloadStatus::Paused => ("Stopped", warning_color, String::new()),
        DownloadStatus::Completed => ("Complete", success_color, String::new()),
        DownloadStatus::Failed(msg) => ("Failed", danger_color, msg.clone()),
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
        Some(total) if !is_completed => format!(
            "{} / {}",
            format_bytes(snap.downloaded_bytes),
            format_bytes(total)
        ),
        Some(total) => format_bytes(total),
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
