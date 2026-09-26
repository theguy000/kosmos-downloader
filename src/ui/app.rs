use super::platform::{
    default_download_directory, open_file, set_startup_enabled, startup_enabled,
};
use super::projection::{
    category_matches_id, history_table_item, should_project_snapshot, sort_items,
    update_window_state,
};
use super::save_settings::{Category, SaveSettings};
use super::table_settings::TableColumnWidths;
use super::view::{MainWindow, TableItem};
use crate::engine::{DownloadAction, DownloadSnapshot, DownloadStatus, DuplicateChoice};
use crate::history::{HistoryEntry, HistoryStore, now_unix_ms};
use slint::ComponentHandle;
use slint::Model;
use std::cell::RefCell;
use std::path::{Path, PathBuf};
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum PendingDelete {
    Active(DeleteTarget),
    History {
        id: i32,
        filename: String,
        save_path: PathBuf,
    },
}

fn open_active_delete_confirmation(
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

fn open_history_delete_confirmation(
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

fn finished_download(id: i32, snap: &DownloadSnapshot) -> HistoryEntry {
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
    use slint::Model;
    let category = window.get_selected_category();
    let is_valid = window.get_sample_downloads().iter().any(|item| {
        item.id == selected_id && category_matches_id(category, item.category_id, true)
    });
    window.set_history_row_selected(is_valid);
}

/// Ids of the rows the table lists, top to bottom: the active download, then the listed
/// downloads the current category shows.
fn listed_row_ids(window: &MainWindow) -> Vec<i32> {
    use slint::Model;
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

fn remove_row_by_id(model: &slint::VecModel<TableItem>, id: i32) {
    use slint::Model;
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
fn resort(history: &slint::VecModel<TableItem>, column: i32, ascending: bool) {
    use slint::Model;
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

/// A file that is already gone counts as deleted; anything else is reported.
pub(super) fn history_delete_failure(removal: std::io::Result<()>) -> Option<std::io::Error> {
    match removal {
        Ok(()) => None,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => Some(error),
    }
}

/// Drops the finished download the toolbar still offers and hides it from Delete Completed.
fn clear_active_completed(
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
///
/// Removing the row of the finished download the toolbar holds also stops offering it, so the
/// two delete paths agree. A file deletion that failed keeps the row and reports why, so it can
/// be tried again.
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
            .completed
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
fn spawn_history_file_delete(window: slint::Weak<MainWindow>, id: i32, save_path: PathBuf) {
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
        // The event loop is gone only once the application is shutting down.
        let _ = slint::invoke_from_event_loop(move || {
            if let Some(window) = window.upgrade() {
                window.invoke_history_delete_finished(id, failed, message.into());
            }
        });
    });
}

fn window_category_dir(window: &MainWindow, category: Category) -> slint::SharedString {
    window
        .get_options_category_dirs()
        .row_data(category.category_id() as usize)
        .unwrap_or_default()
}

fn set_window_category_dir(window: &MainWindow, category: Category, value: slint::SharedString) {
    let mut dirs: Vec<slint::SharedString> = window.get_options_category_dirs().iter().collect();
    if let Some(dir) = dirs.get_mut(category.category_id() as usize) {
        *dir = value;
    }
    window.set_options_category_dirs(slint::ModelRc::from(dirs.as_slice()));
}

fn window_category_file_types(window: &MainWindow, category: Category) -> slint::SharedString {
    window
        .get_options_category_file_types()
        .row_data(category.category_id() as usize)
        .unwrap_or_default()
}

fn set_window_category_file_types(
    window: &MainWindow,
    category: Category,
    value: slint::SharedString,
) {
    let mut types: Vec<slint::SharedString> =
        window.get_options_category_file_types().iter().collect();
    if let Some(file_types) = types.get_mut(category.category_id() as usize) {
        *file_types = value;
    }
    window.set_options_category_file_types(slint::ModelRc::from(types.as_slice()));
}

pub(super) fn update_category_defaults(
    window: &MainWindow,
    old_default: &Path,
    new_default: &Path,
) {
    for category in Category::ALL {
        if category == Category::General {
            continue;
        }
        let current_val = window_category_dir(window, category);
        let current_path = Path::new(current_val.as_str());
        let old_sub = SaveSettings::default_subfolder(old_default, category);
        let next = if current_val.is_empty() || current_path == old_sub {
            SaveSettings::default_subfolder(new_default, category)
                .to_string_lossy()
                .as_ref()
                .into()
        } else {
            current_val
        };
        set_window_category_dir(window, category, next);
    }
}

pub fn run_app(
    action_tx: &mpsc::Sender<DownloadAction>,
    snapshot_rx: watch::Receiver<DownloadSnapshot>,
) -> Result<(), Box<dyn std::error::Error>> {
    let main_window = MainWindow::new()?;
    let displayed_delete_target = Rc::new(RefCell::new(None));
    let pending_delete: Rc<RefCell<Option<PendingDelete>>> = Rc::new(RefCell::new(None));

    let save_settings = Rc::new(RefCell::new(SaveSettings::load()));
    main_window.set_dest_dir_text(
        save_settings
            .borrow()
            .default_dir
            .to_string_lossy()
            .as_ref()
            .into(),
    );
    main_window.set_startup_option_visible(cfg!(windows));
    let category_names: Vec<slint::SharedString> = Category::ALL
        .iter()
        .map(|category| category.display_name().into())
        .collect();
    main_window.set_options_category_names(Rc::new(slint::VecModel::from(category_names)).into());

    {
        let window_weak = main_window.as_weak();
        let save_settings = save_settings.clone();
        main_window.on_options_opened(move || {
            if let Some(window) = window_weak.upgrade() {
                let settings = save_settings.borrow();
                for category in Category::ALL {
                    set_window_category_dir(
                        &window,
                        category,
                        settings
                            .category_path(category)
                            .to_string_lossy()
                            .as_ref()
                            .into(),
                    );
                    if category != Category::General {
                        set_window_category_file_types(
                            &window,
                            category,
                            settings.extensions_for(category).join(", ").into(),
                        );
                    }
                }
            }

            let window_weak = window_weak.clone();
            tokio::spawn(async move {
                let enabled = tokio::task::spawn_blocking(startup_enabled)
                    .await
                    .unwrap_or_default();
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(window) = window_weak.upgrade() {
                        window.set_options_launch_on_startup(enabled);
                    }
                });
            });
        });
    }

    {
        let window_weak = main_window.as_weak();
        main_window.on_default_dir_edited(move |old_default_str, new_default_str| {
            if let Some(window) = window_weak.upgrade() {
                let new_trimmed = new_default_str.trim();
                if !new_trimmed.is_empty() {
                    update_category_defaults(
                        &window,
                        Path::new(old_default_str.trim()),
                        Path::new(new_trimmed),
                    );
                }
            }
        });
    }

    {
        let window_weak = main_window.as_weak();
        main_window.on_browse_options_folder(move |cat_idx| {
            if let Some(window) = window_weak.upgrade() {
                let category = Category::from(cat_idx);
                let current_str = window_category_dir(&window, category);
                let start_dir =
                    if !current_str.is_empty() && Path::new(current_str.as_str()).exists() {
                        PathBuf::from(current_str.as_str())
                    } else {
                        let def = window_category_dir(&window, Category::General);
                        if !def.is_empty() && Path::new(def.as_str()).exists() {
                            PathBuf::from(def.as_str())
                        } else {
                            default_download_directory()
                        }
                    };

                if let Some(folder) = rfd::FileDialog::new()
                    .set_directory(&start_dir)
                    .pick_folder()
                {
                    let folder_str: slint::SharedString = folder.to_string_lossy().as_ref().into();
                    if category == Category::General {
                        let old_default = window_category_dir(&window, Category::General);
                        set_window_category_dir(&window, Category::General, folder_str);
                        update_category_defaults(&window, Path::new(old_default.as_str()), &folder);
                    } else {
                        set_window_category_dir(&window, category, folder_str);
                    }
                }
            }
        });
    }

    {
        let window_weak = main_window.as_weak();
        main_window.on_reset_options_category_default(move |cat_idx| {
            if let Some(window) = window_weak.upgrade() {
                let category = Category::from(cat_idx);
                if category == Category::General {
                    let old_default = window_category_dir(&window, Category::General);
                    let new_default = default_download_directory();
                    set_window_category_dir(
                        &window,
                        Category::General,
                        new_default.to_string_lossy().as_ref().into(),
                    );
                    update_category_defaults(
                        &window,
                        Path::new(old_default.as_str()),
                        &new_default,
                    );
                } else {
                    let default_dir =
                        PathBuf::from(window_category_dir(&window, Category::General).as_str());
                    let subfolder = SaveSettings::default_subfolder(&default_dir, category);
                    set_window_category_dir(
                        &window,
                        category,
                        subfolder.to_string_lossy().as_ref().into(),
                    );
                }
            }
        });
    }

    let download_history = Rc::new(slint::VecModel::<TableItem>::default());
    // Downloads kept from earlier sessions are listed before the window is shown.
    let history_store = Rc::new(RefCell::new(HistoryStore::load()));
    {
        let settings = save_settings.borrow();
        download_history.set_vec(
            history_store
                .borrow()
                .entries()
                .iter()
                .map(|entry| history_table_item(&settings, entry))
                .collect::<Vec<_>>(),
        );
    }
    main_window.set_sample_downloads(download_history.clone().into());
    let history_tracker = Rc::new(RefCell::new(HistoryTracker::new(
        history_store.borrow().next_id(),
    )));
    // Default sort: Date Added, newest first.
    main_window.set_sort_column(3);
    main_window.set_sort_ascending(false);
    resort(&download_history, 3, false);

    {
        let window_weak = main_window.as_weak();
        let save_settings = save_settings.clone();
        let history_store = history_store.clone();
        let download_history = download_history.clone();
        main_window.on_commit_options(move |enabled| {
            if let Some(window) = window_weak.upgrade() {
                let default_dir_raw = window_category_dir(&window, Category::General);
                let default_dir_trimmed = default_dir_raw.trim();
                let default_dir = if default_dir_trimmed.is_empty() {
                    default_download_directory()
                } else {
                    PathBuf::from(default_dir_trimmed)
                };

                let mut new_settings = SaveSettings {
                    default_dir: default_dir.clone(),
                    ..Default::default()
                };
                for category in Category::ALL {
                    if category == Category::General {
                        continue;
                    }
                    new_settings.set_category_dir(
                        category,
                        SaveSettings::category_override(
                            &window_category_dir(&window, category),
                            &default_dir,
                            category,
                        ),
                    );
                    if let Some(extensions) = SaveSettings::file_type_override(
                        category,
                        window_category_file_types(&window, category).as_str(),
                    ) {
                        new_settings.file_types.insert(category, extensions);
                    }
                }

                *save_settings.borrow_mut() = new_settings.clone();
                window.set_dest_dir_text(default_dir.to_string_lossy().as_ref().into());

                {
                    let settings = save_settings.borrow();
                    let store = history_store.borrow();
                    download_history.set_vec(
                        store
                            .entries()
                            .iter()
                            .map(|entry| history_table_item(&settings, entry))
                            .collect::<Vec<_>>(),
                    );
                    resort(
                        &download_history,
                        window.get_sort_column(),
                        window.get_sort_ascending(),
                    );
                    update_selection_state(&window, window.get_selected_row());
                }

                let window_weak_save = window_weak.clone();
                tokio::spawn(async move {
                    let saved = tokio::task::spawn_blocking(move || new_settings.save()).await;
                    let failure = match saved {
                        Ok(result) => result.err().map(|error| error.to_string()),
                        Err(error) => Some(error.to_string()),
                    };
                    if let Some(message) = failure {
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(window) = window_weak_save.upgrade() {
                                window.set_action_error_message(
                                    format!("Could not save options: {message}").into(),
                                );
                            }
                        });
                    }
                });
            }

            let window_weak = window_weak.clone();
            tokio::spawn(async move {
                let outcome = tokio::task::spawn_blocking(move || {
                    set_startup_enabled(enabled).map_err(|error| {
                        (
                            format!("Could not update the startup setting: {error}"),
                            startup_enabled(),
                        )
                    })
                })
                .await
                .unwrap_or_else(|error| {
                    Err((
                        format!("Could not update the startup setting: {error}"),
                        enabled,
                    ))
                });
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(window) = window_weak.upgrade()
                        && let Err((message, actual)) = outcome
                    {
                        window.set_options_launch_on_startup(actual);
                        window.set_action_error_message(message.into());
                    }
                });
            });
        });
    }

    if let Some(widths) = TableColumnWidths::load() {
        main_window.set_col_filename_width(widths.filename);
        main_window.set_col_size_width(widths.size);
        main_window.set_col_status_width(widths.status);
        main_window.set_col_time_left_width(widths.time_left);
        main_window.set_col_transfer_rate_width(widths.transfer_rate);
        main_window.set_col_date_added_width(widths.date_added);
    }

    let (column_widths_tx, mut column_widths_rx) = watch::channel(None::<TableColumnWidths>);

    // One writer, so overlapping saves can never race on the same temp file. A watch channel
    // keeps only the latest widths, so a burst of releases collapses to one save.
    tokio::spawn(async move {
        while column_widths_rx.changed().await.is_ok() {
            let Some(widths) = *column_widths_rx.borrow_and_update() else {
                continue;
            };
            let saved = tokio::task::spawn_blocking(move || widths.save()).await;
            match saved {
                Ok(Ok(())) => {}
                Ok(Err(error)) => eprintln!("could not save table column widths: {error}"),
                Err(error) => eprintln!("table column width save task failed: {error}"),
            }
        }
    });

    {
        let window_weak = main_window.as_weak();
        main_window.on_save_column_widths(move || {
            if let Some(window) = window_weak.upgrade() {
                let widths = TableColumnWidths {
                    filename: window.get_col_filename_width(),
                    size: window.get_col_size_width(),
                    status: window.get_col_status_width(),
                    time_left: window.get_col_time_left_width(),
                    transfer_rate: window.get_col_transfer_rate_width(),
                    date_added: window.get_col_date_added_width(),
                };
                column_widths_tx.send_replace(Some(widths));
            }
        });
    }

    {
        let window_weak = main_window.as_weak();
        main_window.on_row_selected(move |id| {
            let Some(window) = window_weak.upgrade() else {
                return;
            };
            update_selection_state(&window, id);
        });
    }

    {
        let window_weak = main_window.as_weak();
        main_window.on_selected_category_changed(move || {
            if let Some(window) = window_weak.upgrade() {
                update_selection_state(&window, window.get_selected_row());
            }
        });
    }

    {
        let window_weak = main_window.as_weak();
        main_window.on_step_selection(move |step| {
            if let Some(window) = window_weak.upgrade() {
                step_selection(&window, step);
            }
        });
    }

    {
        let history = download_history.clone();
        let window_weak = main_window.as_weak();
        main_window.on_sort_requested(move |column| {
            if let Some(window) = window_weak.upgrade() {
                apply_sort_request(&window, &history, column);
            }
        });
    }

    {
        let window_weak = main_window.as_weak();
        main_window.on_browse_folder(move || {
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
        let window_weak = main_window.as_weak();
        let save_settings = save_settings.clone();
        main_window.on_add_dialog_opened(move || {
            if let Some(window) = window_weak.upgrade() {
                let url = window.get_url_text();
                let dir = save_settings.borrow().path_for_url(url.trim());
                window.set_dest_dir_text(dir.to_string_lossy().as_ref().into());
            }
        });
    }

    {
        let window_weak = main_window.as_weak();
        let save_settings = save_settings.clone();
        main_window.on_url_text_changed(move |new_url| {
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
                        update_selection_state(&window, 0);
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
        let store = history_store.clone();
        let window_weak = main_window.as_weak();
        main_window.on_request_delete_selected(move || {
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
        let displayed = displayed_delete_target.clone();
        let pending = pending_delete.clone();
        let window_weak = main_window.as_weak();
        main_window.on_request_delete_completed(move || {
            if let Some(window) = window_weak.upgrade() {
                open_active_delete_confirmation(&window, &displayed, &pending, true);
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
        let tracker = history_tracker.clone();
        let store = history_store.clone();
        let history = download_history.clone();
        let window_weak = main_window.as_weak();
        main_window.on_confirm_delete(move |delete_file| {
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
        let tracker = history_tracker.clone();
        let store = history_store.clone();
        let history = download_history.clone();
        let window_weak = main_window.as_weak();
        main_window.on_history_delete_finished(move |id, failed, message| {
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

    {
        let tx = action_tx.clone();
        let rx = snapshot_rx.clone();
        let window_weak = main_window.as_weak();
        let tracker = history_tracker.clone();
        let store = history_store.clone();
        let history = download_history.clone();
        main_window.on_resolve_duplicate(move |option, remember| {
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
        let tx = action_tx.clone();
        let rx = snapshot_rx.clone();
        let window_weak = main_window.as_weak();
        main_window.on_dismiss_duplicate(move || {
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

    {
        let rx = snapshot_rx.clone();
        let store = history_store.clone();
        let window_weak = main_window.as_weak();
        main_window.on_open_file(move || {
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

    let timer = slint::Timer::default();
    {
        let window_weak = main_window.as_weak();
        let displayed = displayed_delete_target;
        let pending = pending_delete;
        let tracker = history_tracker;
        let store = history_store;
        let history = download_history;
        let save_settings = save_settings.clone();
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
                    if let Some(entry) = tracker.borrow_mut().observe(&snap) {
                        if let Err(error) = store.borrow_mut().record(entry.clone()) {
                            window.set_action_error_message(
                                format!("Could not save download history: {error}").into(),
                            );
                        }
                        // The finished download is listed at once, in the active sort's position.
                        history.push(history_table_item(&save_settings.borrow(), &entry));
                        resort(
                            &history,
                            window.get_sort_column(),
                            window.get_sort_ascending(),
                        );
                        window.set_completed_listed(true);
                        // Keep the selection with the row that just moved into the list.
                        if window.get_selected_row() == 0 {
                            window.set_selected_row(entry.id);
                            update_selection_state(&window, entry.id);
                        }
                    }

                    let projected_target = DeleteTarget::displayed(&snap);
                    if let Some(ref mut target) = *pending.borrow_mut() {
                        match target {
                            PendingDelete::Active(request) => {
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
                            PendingDelete::History { .. } => {}
                        }
                    }
                    update_window_state(&window, &save_settings.borrow(), &snap);
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
