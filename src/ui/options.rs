use super::platform::{default_download_directory, set_startup_enabled, startup_enabled};
use super::projection::history_table_item;
use super::save_settings::{Category, SaveSettings};
use super::state::AppState;
use super::table::{resort, update_selection_state};
use super::view::MainWindow;
use slint::ComponentHandle;
use slint::Model;
use std::path::{Path, PathBuf};

pub(super) fn window_category_dir(window: &MainWindow, category: Category) -> slint::SharedString {
    window
        .get_options_category_dirs()
        .row_data(category.category_id() as usize)
        .unwrap_or_default()
}

pub(super) fn set_window_category_dir(
    window: &MainWindow,
    category: Category,
    value: slint::SharedString,
) {
    let mut dirs: Vec<slint::SharedString> = window.get_options_category_dirs().iter().collect();
    if let Some(dir) = dirs.get_mut(category.category_id() as usize) {
        *dir = value;
    }
    window.set_options_category_dirs(slint::ModelRc::from(dirs.as_slice()));
}

pub(super) fn window_category_file_types(
    window: &MainWindow,
    category: Category,
) -> slint::SharedString {
    window
        .get_options_category_file_types()
        .row_data(category.category_id() as usize)
        .unwrap_or_default()
}

pub(super) fn set_window_category_file_types(
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

pub(super) fn bind_options_handlers(window: &MainWindow, state: &AppState) {
    {
        let window_weak = window.as_weak();
        let save_settings = state.save_settings.clone();
        window.on_options_opened(move || {
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
        let window_weak = window.as_weak();
        window.on_default_dir_edited(move |old_default_str, new_default_str| {
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
        let window_weak = window.as_weak();
        window.on_browse_options_folder(move |cat_idx| {
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
        let window_weak = window.as_weak();
        window.on_reset_options_category_default(move |cat_idx| {
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

    {
        let window_weak = window.as_weak();
        let save_settings = state.save_settings.clone();
        let history_store = state.history_store.clone();
        let download_history = state.download_history.clone();
        window.on_commit_options(move |enabled| {
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
}
