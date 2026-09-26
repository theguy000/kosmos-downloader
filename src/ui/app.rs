use super::actions::bind_action_handlers;
use super::columns::{bind_column_handlers, init_columns, start_column_widths_worker};
use super::delete::{DeleteTarget, PendingDelete, bind_delete_handlers};
use super::options::bind_options_handlers;
use super::projection::{history_table_item, should_project_snapshot, update_window_state};
use super::save_settings::Category;
use super::state::AppState;
use super::table::{bind_table_handlers, resort, update_selection_state};
use super::view::MainWindow;
use crate::engine::{DownloadAction, DownloadSnapshot, DownloadStatus};
use slint::ComponentHandle;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

pub fn run_app(
    action_tx: &mpsc::Sender<DownloadAction>,
    snapshot_rx: watch::Receiver<DownloadSnapshot>,
) -> Result<(), Box<dyn std::error::Error>> {
    let main_window = MainWindow::new()?;
    let state = AppState::new(action_tx.clone(), snapshot_rx);

    main_window.set_dest_dir_text(
        state
            .save_settings
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
        let settings = state.save_settings.borrow();
        state.download_history.set_vec(
            state
                .history_store
                .borrow()
                .entries()
                .iter()
                .map(|entry| history_table_item(&settings, entry))
                .collect::<Vec<_>>(),
        );
    }
    main_window.set_sample_downloads(state.download_history.clone().into());

    // Default sort: Date Added, newest first.
    main_window.set_sort_column(3);
    main_window.set_sort_ascending(false);
    resort(&state.download_history, 3, false);

    let (column_widths_tx, column_widths_rx) = init_columns(&main_window);
    start_column_widths_worker(column_widths_rx);
    bind_column_handlers(&main_window, column_widths_tx);

    bind_options_handlers(&main_window, &state);
    bind_table_handlers(&main_window, &state);
    bind_action_handlers(&main_window, &state);
    bind_delete_handlers(&main_window, &state);

    let timer = slint::Timer::default();
    {
        let window_weak = main_window.as_weak();
        let state = state.clone();
        let mut rx = state.snapshot_rx.clone();
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
                    if let Some(entry) = state.history_tracker.borrow_mut().observe(&snap) {
                        if let Err(error) = state.history_store.borrow_mut().record(entry.clone()) {
                            window.set_action_error_message(
                                format!("Could not save download history: {error}").into(),
                            );
                        }
                        state
                            .download_history
                            .push(history_table_item(&state.save_settings.borrow(), &entry));
                        resort(
                            &state.download_history,
                            window.get_sort_column(),
                            window.get_sort_ascending(),
                        );
                        window.set_completed_listed(true);
                        if window.get_selected_row() == 0 {
                            window.set_selected_row(entry.id);
                            update_selection_state(&window, entry.id);
                        }
                    }

                    let projected_target = DeleteTarget::displayed(&snap);
                    if let Some(ref mut target) = *state.pending_delete.borrow_mut() {
                        match target {
                            PendingDelete::Active(request) => {
                                if snap.status == DownloadStatus::Idle
                                    || snap.session_id != request.session_id
                                    || (request.completed_only
                                        && !matches!(snap.status, DownloadStatus::Completed))
                                {
                                    *state.pending_delete.borrow_mut() = None;
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
                    update_window_state(&window, &state.save_settings.borrow(), &snap);
                    *state.displayed_delete_target.borrow_mut() = projected_target;
                }
            },
        );
    }

    main_window.run()?;
    Ok(())
}
