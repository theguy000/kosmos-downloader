mod actions;
mod duplicate;
mod lifecycle;
mod metadata;
mod progress;
mod resume;
mod scheduler;
mod snapshot;
#[cfg(test)]
mod tests;

use super::model::{DownloadAction, DownloadSnapshot, DownloadStatus, DuplicateChoice};
use super::worker::{WorkerError, WorkerMsg};
use crate::client::{HttpClient, RemoteFileInfo};
use crate::storage::{Storage, StorageError};
use duplicate::PendingDuplicate;
use metadata::FetchInfoMsg;
use scheduler::{ActiveChunk, MAX_CHUNK_RETRIES};
use std::cell::LazyCell;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

#[derive(Debug, Error)]
enum CoordinatorError {
    #[error("Stream #{chunk_id} stalled after {MAX_CHUNK_RETRIES} retries")]
    Stalled { chunk_id: usize },
    #[error("Worker reported invalid download progress")]
    InvalidProgress,
    #[error("Stream #{chunk_id} error: {source}")]
    Worker {
        chunk_id: usize,
        #[source]
        source: WorkerError,
    },
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("Storage task failed: {0}")]
    StorageTask(#[from] tokio::task::JoinError),
}

pub struct DownloadEngine {
    action_tx: mpsc::Sender<DownloadAction>,
    snapshot_rx: watch::Receiver<DownloadSnapshot>,
}

impl DownloadEngine {
    pub fn new() -> Self {
        let (action_tx, action_rx) = mpsc::channel(32);
        let default_snapshot = DownloadSnapshot::default();
        let (snapshot_tx, snapshot_rx) = watch::channel(default_snapshot);

        tokio::spawn(async move {
            run_coordinator(action_rx, snapshot_tx).await;
        });

        Self {
            action_tx,
            snapshot_rx,
        }
    }

    pub fn action_tx(&self) -> mpsc::Sender<DownloadAction> {
        self.action_tx.clone()
    }

    pub fn snapshot_rx(&self) -> watch::Receiver<DownloadSnapshot> {
        self.snapshot_rx.clone()
    }
}

impl Default for DownloadEngine {
    fn default() -> Self {
        Self::new()
    }
}

struct Session {
    // Close cancellation before task handles detach during session teardown.
    cancel_tx: watch::Sender<bool>,
    client: LazyCell<HttpClient>,
    session_id: u64,
    current_url: String,
    current_filename: String,
    current_path: PathBuf,
    current_num_chunks: usize,
    status: DownloadStatus,
    file_info: Option<RemoteFileInfo>,
    active_storage: Option<Storage>,
    owns_target: bool,
    active_chunks: Vec<ActiveChunk>,
    worker_handles: Vec<JoinHandle<()>>,
    info_handle: Option<JoinHandle<()>>,
    restart_required: bool,
    content_restarts: u8,
    duplicate: Option<PendingDuplicate>,
    duplicate_preference: Option<DuplicateChoice>,
    worker_tx: mpsc::Sender<WorkerMsg>,
    worker_rx: mpsc::Receiver<WorkerMsg>,
    info_tx: mpsc::Sender<FetchInfoMsg>,
    snapshot_tx: watch::Sender<DownloadSnapshot>,
    last_tick: Instant,
    bytes_since_last_tick: u64,
    current_speed: u64,
}

impl Session {
    fn new(
        worker_tx: mpsc::Sender<WorkerMsg>,
        worker_rx: mpsc::Receiver<WorkerMsg>,
        info_tx: mpsc::Sender<FetchInfoMsg>,
        cancel_tx: watch::Sender<bool>,
        snapshot_tx: watch::Sender<DownloadSnapshot>,
    ) -> Self {
        Self {
            // Defer HTTP/TLS setup until the first download, then keep connection pooling.
            client: LazyCell::new(HttpClient::new),
            session_id: 0,
            current_url: String::new(),
            current_filename: String::new(),
            current_path: PathBuf::new(),
            current_num_chunks: 1,
            status: DownloadStatus::Idle,
            file_info: None,
            active_storage: None,
            owns_target: false,
            active_chunks: Vec::new(),
            worker_handles: Vec::new(),
            info_handle: None,
            restart_required: false,
            content_restarts: 0,
            duplicate: None,
            duplicate_preference: None,
            worker_tx,
            worker_rx,
            info_tx,
            cancel_tx,
            snapshot_tx,
            last_tick: Instant::now(),
            bytes_since_last_tick: 0,
            current_speed: 0,
        }
    }
}

// Byte samples mitigate changing sources without claiming full-file identity.
fn uses_range_workers(info: &RemoteFileInfo) -> bool {
    info.accepts_ranges && matches!(info.content_length, Some(total_size) if total_size > 0)
}

fn calculate_downloaded(chunks: &[ActiveChunk]) -> u64 {
    chunks.iter().map(|c| c.downloaded).sum()
}

async fn run_coordinator(
    mut action_rx: mpsc::Receiver<DownloadAction>,
    snapshot_tx: watch::Sender<DownloadSnapshot>,
) {
    let (worker_tx, worker_rx) = mpsc::channel(256);
    let (info_tx, mut info_rx) = mpsc::channel(8);
    let (cancel_tx, _) = watch::channel(false);
    let mut session = Session::new(worker_tx, worker_rx, info_tx, cancel_tx, snapshot_tx);
    let mut tick_interval = tokio::time::interval(Duration::from_millis(200));

    loop {
        if session.restart_required {
            session.restart_changed_content().await;
        }
        tokio::select! {
            action = action_rx.recv() => {
                let Some(action) = action else { break };
                session.handle_action(action).await;
            }
            info = info_rx.recv() => {
                if let Some(info) = info {
                    session.handle_info(info).await;
                }
            }
            message = session.worker_rx.recv() => {
                if let Some(message) = message {
                    session.handle_worker(message).await;
                }
            }
            _ = tick_interval.tick() => session.tick(),
        }
    }
}
