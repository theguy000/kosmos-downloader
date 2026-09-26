use super::delete::{DeleteTarget, PendingDelete};
use super::save_settings::SaveSettings;
use super::table::HistoryTracker;
use super::view::TableItem;
use crate::engine::{DownloadAction, DownloadSnapshot};
use crate::history::HistoryStore;
use std::cell::RefCell;
use std::rc::Rc;
use tokio::sync::{mpsc, watch};

#[derive(Clone)]
pub(super) struct AppState {
    pub(super) save_settings: Rc<RefCell<SaveSettings>>,
    pub(super) history_store: Rc<RefCell<HistoryStore>>,
    pub(super) history_tracker: Rc<RefCell<HistoryTracker>>,
    pub(super) download_history: Rc<slint::VecModel<TableItem>>,
    pub(super) displayed_delete_target: Rc<RefCell<Option<DeleteTarget>>>,
    pub(super) pending_delete: Rc<RefCell<Option<PendingDelete>>>,
    pub(super) action_tx: mpsc::Sender<DownloadAction>,
    pub(super) snapshot_rx: watch::Receiver<DownloadSnapshot>,
}

impl AppState {
    pub(super) fn new(
        action_tx: mpsc::Sender<DownloadAction>,
        snapshot_rx: watch::Receiver<DownloadSnapshot>,
    ) -> Self {
        let save_settings = Rc::new(RefCell::new(SaveSettings::load()));
        let history_store = Rc::new(RefCell::new(HistoryStore::load()));
        let next_id = history_store.borrow().next_id();
        let history_tracker = Rc::new(RefCell::new(HistoryTracker::new(next_id)));
        let download_history = Rc::new(slint::VecModel::<TableItem>::default());
        let displayed_delete_target = Rc::new(RefCell::new(None));
        let pending_delete = Rc::new(RefCell::new(None));

        Self {
            save_settings,
            history_store,
            history_tracker,
            download_history,
            displayed_delete_target,
            pending_delete,
            action_tx,
            snapshot_rx,
        }
    }
}
