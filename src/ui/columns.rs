use super::table_settings::TableColumnWidths;
use super::view::MainWindow;
use slint::ComponentHandle;
use tokio::sync::watch;

pub(super) fn init_columns(
    window: &MainWindow,
) -> (
    watch::Sender<Option<TableColumnWidths>>,
    watch::Receiver<Option<TableColumnWidths>>,
) {
    if let Some(widths) = TableColumnWidths::load() {
        window.set_col_filename_width(widths.filename);
        window.set_col_size_width(widths.size);
        window.set_col_status_width(widths.status);
        window.set_col_time_left_width(widths.time_left);
        window.set_col_transfer_rate_width(widths.transfer_rate);
        window.set_col_date_added_width(widths.date_added);
    }

    watch::channel(None::<TableColumnWidths>)
}

pub(super) fn start_column_widths_worker(
    mut column_widths_rx: watch::Receiver<Option<TableColumnWidths>>,
) {
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
}

pub(super) fn bind_column_handlers(
    window: &MainWindow,
    column_widths_tx: watch::Sender<Option<TableColumnWidths>>,
) {
    let window_weak = window.as_weak();
    window.on_save_column_widths(move || {
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
