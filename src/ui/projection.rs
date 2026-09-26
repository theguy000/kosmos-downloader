use super::format::{format_bytes, format_eta, format_speed};
use super::save_settings::SaveSettings;
use super::view::{MainWindow, Palette};
use crate::engine::{DownloadSnapshot, DownloadStatus};
use crate::history::{HistoryEntry, downloaded_label};
use slint::ComponentHandle;
use tokio::sync::watch;

pub(super) fn should_project_snapshot<T>(snapshot: &watch::Ref<'_, T>, initial: &mut bool) -> bool {
    // Unlike Receiver::has_changed, this reports a final unseen value after closure.
    let should_project = *initial || snapshot.has_changed();
    *initial = false;
    should_project
}

pub(super) fn category_matches_id(filter_id: i32, category_id: i32, completed: bool) -> bool {
    match filter_id {
        1..=11 => filter_id == category_id,
        12 => !completed,
        13 => completed,
        14 | 15 => false,
        _ => true,
    }
}

pub(super) fn history_table_item(
    settings: &SaveSettings,
    entry: &HistoryEntry,
) -> super::view::TableItem {
    let file_type = settings.category_for_filename(&entry.filename);
    super::view::TableItem {
        id: entry.id,
        filename: entry.filename.clone().into(),
        file_type: file_type.as_str().into(),
        category_id: file_type.category_id(),
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

pub(super) fn update_window_state(
    window: &MainWindow,
    settings: &SaveSettings,
    snap: &DownloadSnapshot,
) {
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
    let file_type = settings.category_for_filename(&snap.filename);
    window.set_active_file_type(file_type.as_str().into());
    window.set_active_category_id(file_type.category_id());

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
