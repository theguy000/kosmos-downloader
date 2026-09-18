use super::chunks::{ChunkRange, calculate_chunks};
use super::model::{DownloadAction, DownloadSnapshot, DownloadStatus};
use super::worker::{
    OVERLAP_BYTES, WorkerError, WorkerMsg, spawn_chunk_worker, spawn_stream_worker,
};
use crate::client::{ClientError, HttpClient, RemoteFileInfo, is_strong_etag};
use crate::storage::{Storage, StorageError};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

enum FetchInfoKind {
    Start {
        save_path: PathBuf,
        num_chunks: usize,
    },
    Resume,
    Restart,
    Complete,
}

struct FetchInfoMsg {
    session_id: u64,
    kind: FetchInfoKind,
    result: Result<RemoteFileInfo, WorkerError>,
}

struct SavedDownload {
    info: RemoteFileInfo,
    storage: Storage,
    chunks: Vec<(ChunkRange, u64)>,
}

impl SavedDownload {
    fn new(info: &RemoteFileInfo, storage: &Storage, chunks: &[ActiveChunk]) -> Self {
        Self {
            info: info.clone(),
            storage: storage.clone(),
            chunks: chunks
                .iter()
                .map(|chunk| (chunk.range, chunk.downloaded))
                .collect(),
        }
    }

    async fn verify(
        &self,
        client: &HttpClient,
        url: &str,
        latest: &RemoteFileInfo,
    ) -> Result<(), WorkerError> {
        if !uses_range_workers(latest) || !self.info.resume_metadata_matches(latest) {
            return Err(ClientError::ContentChanged.into());
        }
        if latest.etag.as_deref().is_some_and(is_strong_etag) {
            return Ok(());
        }
        let total_size = latest
            .content_length
            .ok_or(WorkerError::InvalidRange("Missing range size"))?;
        // First/last 4 KiB per saved chunk are a consistency heuristic, not a whole-file proof.
        for &(range, downloaded) in &self.chunks {
            if downloaded == 0 {
                continue;
            }
            let end = range
                .start
                .checked_add(downloaded)
                .filter(|end| *end <= total_size && *end - 1 <= range.end)
                .ok_or(WorkerError::InvalidRange("Invalid saved chunk progress"))?;
            let length = downloaded.min(OVERLAP_BYTES);
            let mut starts = vec![range.start];
            if end - length != range.start {
                starts.push(end - length);
            }
            for start in starts {
                let storage = self.storage.clone();
                let expected = tokio::task::spawn_blocking(move || {
                    let mut bytes = vec![0; length as usize];
                    storage.read_at(start, &mut bytes)?;
                    Ok::<_, StorageError>(bytes)
                })
                .await??;
                client
                    .verify_range(url, start, &expected, total_size, latest.resume_validator())
                    .await?;
            }
        }
        Ok(())
    }
}

struct ActiveChunk {
    range: ChunkRange,
    downloaded: u64,
    is_done: bool,
    yield_tx: Option<watch::Sender<bool>>,
    split_requested: bool,
    retry_requested: bool,
    retries: u8,
    last_progress: Instant,
}

const MIN_SPLIT_BYTES: u64 = 256 * 1024;
const MAX_CHUNK_RETRIES: u8 = 3;
const MAX_CONTENT_RESTARTS: u8 = 2;
const STALL_TIMEOUT: Duration = Duration::from_secs(5);

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

impl ActiveChunk {
    fn new(range: ChunkRange) -> Self {
        Self {
            range,
            downloaded: 0,
            is_done: false,
            yield_tx: None,
            split_requested: false,
            retry_requested: false,
            retries: 0,
            last_progress: Instant::now(),
        }
    }

    fn can_retry(&self) -> bool {
        !self.is_done && self.downloaded < self.range.size() && self.retries < MAX_CHUNK_RETRIES
    }

    // pass borrowed session state rather than duplicating it in a worker context.
    #[allow(clippy::too_many_arguments)]
    fn spawn(
        &mut self,
        session_id: u64,
        url: &str,
        client: &HttpClient,
        total_size: u64,
        validator: Option<&str>,
        storage: &Storage,
        cancel_tx: &watch::Sender<bool>,
        worker_tx: &mpsc::Sender<WorkerMsg>,
    ) -> JoinHandle<()> {
        let (yield_tx, yield_rx) = watch::channel(false);
        self.yield_tx = Some(yield_tx);
        self.split_requested = false;
        self.retry_requested = false;
        self.last_progress = Instant::now();
        spawn_chunk_worker(
            session_id,
            self.range,
            self.downloaded,
            total_size,
            url.to_string(),
            client.clone(),
            storage.clone(),
            validator.map(str::to_owned),
            cancel_tx.subscribe(),
            yield_rx,
            worker_tx.clone(),
        )
    }
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

fn spawn_info_fetch(
    session_id: u64,
    kind: FetchInfoKind,
    url: String,
    client: HttpClient,
    mut cancel_rx: watch::Receiver<bool>,
    info_tx: mpsc::Sender<FetchInfoMsg>,
    saved: Option<SavedDownload>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result = tokio::select! {
            _ = cancel_rx.changed() => return,
            result = async {
                let mut retries = 0;
                loop {
                    let attempt: Result<_, WorkerError> = async {
                        let info = if matches!(kind, FetchInfoKind::Complete)
                            && let Some(saved) = &saved
                            && (!uses_range_workers(&saved.info)
                                || saved.info.etag.as_deref().is_some_and(is_strong_etag))
                        {
                            saved.info.clone()
                        } else {
                            client.fetch_info(&url).await?
                        };
                        if let Some(saved) = &saved && uses_range_workers(&saved.info) {
                            saved.verify(&client, &url, &info).await?;
                        }
                        Ok(info)
                    }.await;
                    if saved.is_some() && retries < MAX_CHUNK_RETRIES
                        && attempt.as_ref().is_err_and(|error| error.is_retryable())
                    {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                    break attempt;
                }
            } => result,
        };

        if *cancel_rx.borrow() {
            return;
        }

        tokio::select! {
            _ = cancel_rx.changed() => {}
            _ = info_tx.send(FetchInfoMsg { session_id, kind, result }) => {}
        }
    })
}

async fn wait_for_active_tasks(
    info_handle: &mut Option<JoinHandle<()>>,
    worker_handles: &mut Vec<JoinHandle<()>>,
) {
    if let Some(handle) = info_handle.take() {
        let _ = handle.await;
    }

    for handle in worker_handles.drain(..) {
        let _ = handle.await;
    }
}

// Byte samples mitigate changing sources without claiming full-file identity.
fn uses_range_workers(info: &RemoteFileInfo) -> bool {
    info.accepts_ranges && matches!(info.content_length, Some(total_size) if total_size > 0)
}

// explicit session inputs keep worker creation independent of coordinator ownership.
#[allow(clippy::too_many_arguments)]
fn spawn_download_workers(
    session_id: u64,
    url: &str,
    client: &HttpClient,
    info: &RemoteFileInfo,
    num_chunks: usize,
    storage: &Storage,
    cancel_tx: &watch::Sender<bool>,
    worker_tx: &mpsc::Sender<WorkerMsg>,
    active_chunks: &mut Vec<ActiveChunk>,
    worker_handles: &mut Vec<JoinHandle<()>>,
) {
    if uses_range_workers(info)
        && let Some(total_size) = info.content_length
    {
        *active_chunks = calculate_chunks(total_size, num_chunks)
            .into_iter()
            .map(ActiveChunk::new)
            .collect();

        for chunk in active_chunks.iter_mut() {
            worker_handles.push(chunk.spawn(
                session_id,
                url,
                client,
                total_size,
                info.resume_validator(),
                storage,
                cancel_tx,
                worker_tx,
            ));
        }
    } else {
        *active_chunks = vec![ActiveChunk::new(ChunkRange {
            id: 0,
            start: 0,
            end: info
                .content_length
                .and_then(|size| size.checked_sub(1))
                .unwrap_or(0),
        })];
        worker_handles.push(spawn_stream_worker(
            session_id,
            url.to_string(),
            info.content_length,
            client.clone(),
            storage.clone(),
            cancel_tx.subscribe(),
            worker_tx.clone(),
        ));
    }
}

fn resume_chunks_are_valid(chunks: &[ActiveChunk], total_size: u64) -> bool {
    if chunks.is_empty()
        || chunks
            .iter()
            .enumerate()
            .any(|(id, chunk)| chunk.range.id != id)
    {
        return false;
    }
    let mut ordered: Vec<_> = chunks.iter().collect();
    ordered.sort_unstable_by_key(|chunk| chunk.range.start);
    let mut next = 0;
    for chunk in ordered {
        let range = chunk.range;
        if range.start != next || range.end < range.start || range.end >= total_size {
            return false;
        }
        let size = range.end - range.start + 1;
        if chunk.downloaded > size || (chunk.is_done && chunk.downloaded != size) {
            return false;
        }
        next = range.end + 1;
    }
    next == total_size
}

fn split_remaining(chunks: &mut Vec<ActiveChunk>, index: usize) -> bool {
    let id = chunks.len();
    let chunk = &mut chunks[index];
    let remaining = chunk.range.size() - chunk.downloaded;
    if chunk.yield_tx.is_some() || chunk.is_done || remaining < 2 * MIN_SPLIT_BYTES {
        return false;
    }
    let start = chunk.range.start + chunk.downloaded + remaining / 2;
    let mut tail = ActiveChunk::new(ChunkRange {
        id,
        start,
        end: chunk.range.end,
    });
    // Splitting must not reset the retry budget of repeatedly failing bytes.
    tail.retries = chunk.retries;
    chunk.range.end = start - 1;
    chunks.push(tail);
    true
}

// borrow the existing session state; no second scheduler context to keep in sync.
#[allow(clippy::too_many_arguments)]
fn rebalance_workers(
    chunks: &mut Vec<ActiveChunk>,
    worker_handles: &mut Vec<JoinHandle<()>>,
    max_workers: usize,
    session_id: u64,
    url: &str,
    client: &HttpClient,
    info: &RemoteFileInfo,
    storage: &Storage,
    cancel_tx: &watch::Sender<bool>,
    worker_tx: &mpsc::Sender<WorkerMsg>,
) -> Result<(), CoordinatorError> {
    let Some(total_size) = info.content_length else {
        return Ok(());
    };
    if !uses_range_workers(info) {
        return Ok(());
    }
    let validator = info.resume_validator();
    let max_workers = max_workers.max(1);
    let mut unfinished = chunks.iter().filter(|chunk| !chunk.is_done).count();
    for index in 0..chunks.len() {
        if chunks[index].split_requested
            && chunks[index].yield_tx.is_none()
            && unfinished < max_workers
            && split_remaining(chunks, index)
        {
            unfinished += 1;
        }
    }
    worker_handles.retain(|handle| !handle.is_finished());
    for chunk in chunks
        .iter_mut()
        .filter(|chunk| !chunk.is_done && chunk.yield_tx.is_none())
    {
        worker_handles.push(chunk.spawn(
            session_id, url, client, total_size, validator, storage, cancel_tx, worker_tx,
        ));
    }

    // idle connections split the largest remaining range; no throughput model needed.
    if validator.is_some()
        && unfinished < max_workers
        && !chunks.iter().any(|chunk| chunk.split_requested)
        && let Some(chunk) = chunks
            .iter_mut()
            .filter(|chunk| {
                !chunk.is_done
                    && !chunk.retry_requested
                    && chunk.range.size() - chunk.downloaded >= 2 * MIN_SPLIT_BYTES
            })
            .max_by_key(|chunk| chunk.range.size() - chunk.downloaded)
        && let Some(yield_tx) = &chunk.yield_tx
    {
        // A closed receiver can have a terminal message already queued.
        if yield_tx.send(true).is_ok() {
            chunk.split_requested = true;
        }
    }

    for chunk in chunks.iter_mut().filter(|chunk| {
        !chunk.is_done
            && !chunk.split_requested
            && !chunk.retry_requested
            && chunk.downloaded < chunk.range.size()
    }) {
        if chunk.last_progress.elapsed() >= STALL_TIMEOUT {
            if chunk.retries >= MAX_CHUNK_RETRIES {
                return Err(CoordinatorError::Stalled {
                    chunk_id: chunk.range.id,
                });
            }
            if let Some(yield_tx) = &chunk.yield_tx
                && yield_tx.send(true).is_ok()
            {
                chunk.retry_requested = true;
            }
        }
    }
    Ok(())
}

fn record_progress(
    chunks: &mut [ActiveChunk],
    chunk_id: usize,
    bytes_delta: u64,
    range_download: bool,
    total_size: Option<u64>,
) -> bool {
    let Some(chunk) = chunks.iter_mut().find(|chunk| chunk.range.id == chunk_id) else {
        return false;
    };
    let Some(downloaded) = chunk.downloaded.checked_add(bytes_delta) else {
        return false;
    };
    let expected_size = if range_download {
        Some(chunk.range.size())
    } else {
        total_size
    };
    if expected_size.is_some_and(|expected| downloaded > expected) {
        return false;
    }

    chunk.downloaded = downloaded;
    chunk.last_progress = Instant::now();
    true
}

fn record_done(
    chunks: &mut [ActiveChunk],
    chunk_id: usize,
    range_download: bool,
    total_size: Option<u64>,
) -> bool {
    let Some(chunk) = chunks.iter_mut().find(|chunk| chunk.range.id == chunk_id) else {
        return false;
    };
    let expected_size = if range_download {
        Some(chunk.range.size())
    } else {
        total_size
    };
    if expected_size.is_some_and(|expected| chunk.downloaded != expected) {
        return false;
    }

    chunk.is_done = true;
    chunk.yield_tx = None;
    chunk.split_requested = false;
    chunk.retry_requested = false;
    true
}

fn drain_cancelled_progress(
    worker_rx: &mut mpsc::Receiver<WorkerMsg>,
    session_id: u64,
    chunks: &mut [ActiveChunk],
    range_download: bool,
    total_size: Option<u64>,
    resumable: bool,
) -> Result<(), CoordinatorError> {
    let mut error = None;

    while let Ok(msg) = worker_rx.try_recv() {
        match msg {
            WorkerMsg::Progress {
                session_id: sid,
                chunk_id,
                bytes_delta,
            } if sid == session_id => {
                if !record_progress(chunks, chunk_id, bytes_delta, range_download, total_size) {
                    error.get_or_insert(CoordinatorError::InvalidProgress);
                }
            }
            WorkerMsg::Error {
                session_id: sid,
                chunk_id,
                error: source,
                retryable,
            } if sid == session_id => {
                // Pause may win the select race against an otherwise recoverable error.
                let can_resume = resumable
                    && retryable
                    && chunks.get(chunk_id).is_some_and(ActiveChunk::can_retry);
                if source.is_content_changed() {
                    error = Some(CoordinatorError::Worker { chunk_id, source });
                } else if !can_resume {
                    error.get_or_insert(CoordinatorError::Worker { chunk_id, source });
                }
            }
            // Resume revalidates canceled completion boundaries, but must not hide failures.
            _ => {}
        }
    }

    error.map_or(Ok(()), Err)
}

async fn run_coordinator(
    mut action_rx: mpsc::Receiver<DownloadAction>,
    snapshot_tx: watch::Sender<DownloadSnapshot>,
) {
    // defer HTTP/TLS setup until the first download, then keep connection pooling.
    let client = std::cell::LazyCell::new(HttpClient::new);

    let mut session_id: u64 = 0;
    let mut current_url = String::new();
    let mut current_filename = String::new();
    let mut current_path = PathBuf::new();
    let mut current_num_chunks = 1;
    let mut status = DownloadStatus::Idle;
    let mut file_info: Option<RemoteFileInfo> = None;
    let mut active_storage: Option<Storage> = None;
    let mut active_chunks: Vec<ActiveChunk> = Vec::new();
    let mut worker_handles: Vec<JoinHandle<()>> = Vec::new();
    let mut info_handle: Option<JoinHandle<()>> = None;
    let mut restart_required = false;
    let mut content_restarts = 0;

    let (worker_tx, mut worker_rx) = mpsc::channel::<WorkerMsg>(256);
    let (info_tx, mut info_rx) = mpsc::channel::<FetchInfoMsg>(8);
    let (mut cancel_tx, _) = watch::channel(false);

    let mut last_tick = Instant::now();
    let mut bytes_since_last_tick: u64 = 0;
    let mut current_speed: u64 = 0;

    let mut tick_interval = tokio::time::interval(Duration::from_millis(200));

    loop {
        if restart_required {
            restart_required = false;
            let _ = cancel_tx.send(true);
            wait_for_active_tasks(&mut info_handle, &mut worker_handles).await;
            session_id += 1;
            // Only truncate the already-owned file, after every old writer has stopped.
            let reset = if let Some(storage) = &active_storage {
                let storage = storage.clone();
                tokio::task::spawn_blocking(move || storage.set_len(0))
                    .await
                    .map_err(CoordinatorError::StorageTask)
                    .and_then(|result| result.map_err(CoordinatorError::Storage))
            } else {
                Err(CoordinatorError::InvalidProgress)
            };
            active_chunks.clear();
            file_info = None;
            current_speed = 0;
            bytes_since_last_tick = 0;
            last_tick = Instant::now();
            status = match reset {
                Err(error) => {
                    DownloadStatus::Failed(format!("Could not restart changed file: {error}"))
                }
                Ok(()) if content_restarts >= MAX_CONTENT_RESTARTS => {
                    DownloadStatus::Failed(format!(
                        "Remote file keeps changing; stopped after {MAX_CONTENT_RESTARTS} automatic restarts"
                    ))
                }
                Ok(()) => {
                    content_restarts += 1;
                    let (new_cancel_tx, _) = watch::channel(false);
                    cancel_tx = new_cancel_tx;
                    info_handle = Some(spawn_info_fetch(
                        session_id,
                        FetchInfoKind::Restart,
                        current_url.clone(),
                        client.clone(),
                        cancel_tx.subscribe(),
                        info_tx.clone(),
                        None,
                    ));
                    DownloadStatus::Connecting
                }
            };
            publish_snapshot(
                &snapshot_tx,
                &current_url,
                &current_filename,
                &current_path,
                &status,
                None,
                0,
                0,
                None,
                false,
            );
        }
        tokio::select! {
            action = action_rx.recv() => {
                let Some(action) = action else {
                    break;
                };

                match action {
                    DownloadAction::Start { url, save_path, num_chunks } => {
                        let _ = cancel_tx.send(true);
                        wait_for_active_tasks(&mut info_handle, &mut worker_handles).await;
                        session_id += 1;
                        let sid = session_id;

                        let (new_cancel_tx, _) = watch::channel(false);
                        cancel_tx = new_cancel_tx;

                        current_url = url;
                        current_path = save_path;
                        current_num_chunks = num_chunks;
                        content_restarts = 0;
                        current_filename.clear();
                        status = DownloadStatus::Connecting;
                        active_chunks.clear();
                        active_storage = None;
                        file_info = None;
                        current_speed = 0;
                        bytes_since_last_tick = 0;
                        last_tick = Instant::now();

                        publish_snapshot(
                            &snapshot_tx,
                            &current_url,
                            &current_filename,
                            &current_path,
                            &status,
                            None,
                            0,
                            0,
                            None,
                            false,
                        );

                        info_handle = Some(spawn_info_fetch(
                            sid,
                            FetchInfoKind::Start {
                                save_path: current_path.clone(),
                                num_chunks: current_num_chunks,
                            },
                            current_url.clone(),
                            client.clone(),
                            cancel_tx.subscribe(),
                            info_tx.clone(),
                            None,
                        ));
                    }

                    DownloadAction::Pause => {
                        if matches!(status, DownloadStatus::Connecting | DownloadStatus::Downloading) {
                            let _ = cancel_tx.send(true);
                            let range_download = file_info.as_ref().is_some_and(uses_range_workers);
                            let total_size = file_info.as_ref().and_then(|info| info.content_length);
                            wait_for_active_tasks(&mut info_handle, &mut worker_handles).await;
                            match drain_cancelled_progress(
                                &mut worker_rx,
                                session_id,
                                &mut active_chunks,
                                range_download,
                                total_size,
                                file_info.as_ref().is_some_and(uses_range_workers),
                            ) {
                              Ok(()) => {
                                status = DownloadStatus::Paused;
                                current_speed = 0;
                                bytes_since_last_tick = 0;
                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    total_size,
                                    calculate_downloaded(&active_chunks),
                                    0,
                                    None,
                                    file_info.as_ref().is_none_or(uses_range_workers),
                                );
                              }
                              Err(error) => {
                                if matches!(&error, CoordinatorError::Worker { source, .. } if source.is_content_changed()) {
                                    restart_required = true;
                                    continue;
                                }
                                status = DownloadStatus::Failed(error.to_string());
                                current_speed = 0;
                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    total_size,
                                    calculate_downloaded(&active_chunks),
                                    0,
                                    None,
                                    false,
                                );
                              }
                            }
                        }
                    }

                    DownloadAction::Resume => {
                        if status == DownloadStatus::Paused
                            || (matches!(status, DownloadStatus::Failed(_)) && snapshot_tx.borrow().resumable)
                        {
                            wait_for_active_tasks(&mut info_handle, &mut worker_handles).await;
                            session_id += 1;
                            let sid = session_id;
                            let (new_cancel_tx, _) = watch::channel(false);
                            cancel_tx = new_cancel_tx;
                            status = DownloadStatus::Connecting;
                            current_speed = 0;
                            bytes_since_last_tick = 0;
                            last_tick = Instant::now();

                            publish_snapshot(
                                &snapshot_tx,
                                &current_url,
                                &current_filename,
                                &current_path,
                                &status,
                                file_info.as_ref().and_then(|i| i.content_length),
                                calculate_downloaded(&active_chunks),
                                0,
                                None,
                                file_info.as_ref().is_none_or(uses_range_workers),
                            );

                            let kind = if active_storage.is_some() {
                                FetchInfoKind::Resume
                            } else {
                                FetchInfoKind::Start {
                                    save_path: current_path.clone(),
                                    num_chunks: current_num_chunks,
                                }
                            };
                            info_handle = Some(spawn_info_fetch(
                                sid,
                                kind,
                                current_url.clone(),
                                client.clone(),
                                cancel_tx.subscribe(),
                                info_tx.clone(),
                                file_info.as_ref().zip(active_storage.as_ref()).filter(|(info, _)| uses_range_workers(info))
                                    .map(|(info, storage)| SavedDownload::new(info, storage, &active_chunks)),
                            ));
                        }
                    }

                    DownloadAction::Cancel => {
                        let _ = cancel_tx.send(true);
                        wait_for_active_tasks(&mut info_handle, &mut worker_handles).await;
                        session_id += 1;
                        status = DownloadStatus::Idle;
                        current_speed = 0;
                        active_chunks.clear();
                        active_storage = None;
                        file_info = None;
                        publish_snapshot(
                            &snapshot_tx,
                            &current_url,
                            &current_filename,
                            &current_path,
                            &status,
                            None,
                            0,
                            0,
                            None,
                            false,
                        );
                    }
                }
            }

            info_msg = info_rx.recv() => {
                let Some(msg) = info_msg else {
                    continue;
                };

                let expected_status = if matches!(msg.kind, FetchInfoKind::Complete) {
                    DownloadStatus::Downloading
                } else {
                    DownloadStatus::Connecting
                };
                if msg.session_id != session_id || status != expected_status {
                    continue;
                }

                info_handle = None;
                match (msg.kind, msg.result) {
                    (FetchInfoKind::Complete, Ok(_)) => {
                        let flush = if let Some(storage) = &active_storage {
                            let storage = storage.clone();
                            tokio::task::spawn_blocking(move || storage.sync())
                                .await
                                .map_err(CoordinatorError::StorageTask)
                                .and_then(|result| result.map_err(CoordinatorError::Storage))
                        } else {
                            Ok(())
                        };
                        status = match flush {
                            Ok(()) => DownloadStatus::Completed,
                            Err(err) => DownloadStatus::Failed(format!("Failed to flush completed download: {err}")),
                        };
                        current_speed = 0;
                        let downloaded = calculate_downloaded(&active_chunks);
                        let total = file_info.as_ref().and_then(|info| info.content_length).unwrap_or(downloaded);
                        publish_snapshot(
                            &snapshot_tx, &current_url, &current_filename, &current_path,
                            &status, Some(total), downloaded, 0, None,
                            status == DownloadStatus::Completed,
                        );
                    }
                    (FetchInfoKind::Start { save_path, num_chunks }, Ok(info)) => {
                        let is_dir = save_path.is_dir()
                            || save_path.extension().is_none()
                            || save_path.to_string_lossy().ends_with('/')
                            || save_path.to_string_lossy().ends_with('\\');

                        if is_dir {
                            current_filename = info.filename.clone();
                            current_path = save_path.join(&current_filename);
                        } else {
                            current_filename = save_path
                                .file_name()
                                .and_then(|name| name.to_str())
                                .unwrap_or(&info.filename)
                                .to_string();
                            current_path = save_path;
                        }

                        let storage = match tokio::task::spawn_blocking({
                            let path = current_path.clone();
                            let total_size = info.content_length;
                            move || Storage::create_new(&path, total_size)
                        })
                        .await
                        {
                            Ok(Ok(storage)) => Ok(storage),
                            Ok(Err(err)) => Err(match &err {
                                StorageError::Io(error)
                                    if error.kind() == std::io::ErrorKind::AlreadyExists =>
                                {
                                    format!(
                                        "Refusing to overwrite existing file {}. Choose a different path or remove it first: {err}",
                                        current_path.display()
                                    )
                                }
                                _ => err.to_string(),
                            }),
                            Err(err) => Err(format!(
                                "Could not create download file {}: {err}",
                                current_path.display()
                            )),
                        };

                        match storage {
                            Ok(storage) => {
                                let total_size = info.content_length;
                                let resumable = uses_range_workers(&info);
                                spawn_download_workers(
                                    session_id,
                                    &current_url,
                                    &client,
                                    &info,
                                    num_chunks,
                                    &storage,
                                    &cancel_tx,
                                    &worker_tx,
                                    &mut active_chunks,
                                    &mut worker_handles,
                                );
                                active_storage = Some(storage);
                                file_info = Some(info);
                                status = DownloadStatus::Downloading;

                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    total_size,
                                    0,
                                    0,
                                    None,
                                    resumable,
                                );
                            }
                            Err(message) => {
                                status = DownloadStatus::Failed(message);
                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    None,
                                    0,
                                    0,
                                    None,
                                    false,
                                );
                            }
                        }
                    }
                    (kind @ (FetchInfoKind::Resume | FetchInfoKind::Restart), Ok(info)) => {
                        let Some(storage) = active_storage.as_ref() else {
                            status = DownloadStatus::Failed(
                                "Missing storage for paused download".to_string(),
                            );
                            publish_snapshot(
                                &snapshot_tx,
                                &current_url,
                                &current_filename,
                                &current_path,
                                &status,
                                file_info.as_ref().and_then(|current| current.content_length),
                                calculate_downloaded(&active_chunks),
                                0,
                                None,
                                false,
                            );
                            continue;
                        };

                        let was_resumable = !matches!(kind, FetchInfoKind::Restart) && file_info.as_ref().is_some_and(uses_range_workers);
                        let resume = match info.content_length {
                            Some(total_size)
                                if uses_range_workers(&info)
                                    && file_info.as_ref().is_some_and(|previous| previous.resume_metadata_matches(&info))
                                    && resume_chunks_are_valid(&active_chunks, total_size) =>
                            {
                                Some(total_size)
                            }
                            _ => None,
                        };

                        let total_size = info.content_length;
                        if was_resumable {
                            if let Some(total_size) = resume {
                                status = DownloadStatus::Downloading;
                                for chunk in &mut active_chunks {
                                    // Completed chunks acknowledge again so pausing final verification can resume.
                                    chunk.is_done = false;
                                    chunk.retries = 0;
                                    worker_handles.push(chunk.spawn(
                                        session_id,
                                        &current_url,
                                        &client,
                                        total_size,
                                        info.resume_validator(),
                                        storage,
                                        &cancel_tx,
                                        &worker_tx,
                                    ));
                                }
                                file_info = Some(info);

                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    Some(total_size),
                                    calculate_downloaded(&active_chunks),
                                    0,
                                    None,
                                    true,
                                );
                            } else {
                                status = DownloadStatus::Failed(
                                    "Cannot safely resume: the remote file changed or could not be validated"
                                        .to_string(),
                                );
                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    file_info.as_ref().and_then(|current| current.content_length),
                                    calculate_downloaded(&active_chunks),
                                    0,
                                    None,
                                    false,
                                );
                            }
                        } else {
                            let resize = tokio::task::spawn_blocking({
                                let storage = storage.clone();
                                move || storage.set_len(total_size.unwrap_or(0))
                            })
                            .await
                            .map_err(CoordinatorError::StorageTask)
                            .and_then(|result| result.map_err(CoordinatorError::Storage));
                            match resize {
                                Ok(()) => {
                                    active_chunks.clear();
                                    spawn_download_workers(
                                        session_id,
                                        &current_url,
                                        &client,
                                        &info,
                                        current_num_chunks,
                                        storage,
                                        &cancel_tx,
                                        &worker_tx,
                                        &mut active_chunks,
                                        &mut worker_handles,
                                    );
                                    file_info = Some(info);
                                    status = DownloadStatus::Downloading;
                                    publish_snapshot(
                                        &snapshot_tx,
                                        &current_url,
                                        &current_filename,
                                        &current_path,
                                        &status,
                                        total_size,
                                        0,
                                        0,
                                        None,
                                        file_info.as_ref().is_some_and(uses_range_workers),
                                    );
                                }
                                Err(err) => {
                                    status = DownloadStatus::Failed(err.to_string());
                                    publish_snapshot(
                                        &snapshot_tx,
                                        &current_url,
                                        &current_filename,
                                        &current_path,
                                        &status,
                                        file_info.as_ref().and_then(|current| current.content_length),
                                        calculate_downloaded(&active_chunks),
                                        0,
                                        None,
                                        false,
                                    );
                                }
                            }
                        }
                    }
                    (FetchInfoKind::Start { .. }, Err(err)) => {
                        let retryable = err.is_retryable();
                        status = DownloadStatus::Failed(err.to_string());
                        publish_snapshot(
                            &snapshot_tx,
                            &current_url,
                            &current_filename,
                            &current_path,
                            &status,
                            None,
                            0,
                            0,
                            None,
                            retryable,
                        );
                    }
                    (FetchInfoKind::Resume | FetchInfoKind::Restart | FetchInfoKind::Complete, Err(err)) => {
                        if err.is_content_changed() {
                            restart_required = true;
                            continue;
                        }
                        let retryable = err.is_retryable();
                        status = DownloadStatus::Failed(err.to_string());
                        publish_snapshot(
                            &snapshot_tx,
                            &current_url,
                            &current_filename,
                            &current_path,
                            &status,
                            file_info.as_ref().and_then(|current| current.content_length),
                            calculate_downloaded(&active_chunks),
                            0,
                            None,
                            retryable,
                        );
                    }
                }
            }

            msg = worker_rx.recv() => {
                let Some(msg) = msg else {
                    continue;
                };

                match msg {
                    WorkerMsg::Progress { session_id: sid, chunk_id, bytes_delta } => {
                        if sid != session_id || status != DownloadStatus::Downloading {
                            continue;
                        }
                        let range_download = file_info.as_ref().is_some_and(uses_range_workers);
                        let total_size = file_info.as_ref().and_then(|info| info.content_length);
                        if !record_progress(
                            &mut active_chunks,
                            chunk_id,
                            bytes_delta,
                            range_download,
                            total_size,
                        ) {
                            let _ = cancel_tx.send(true);
                            status = DownloadStatus::Failed(
                                "Worker reported invalid download progress".to_string(),
                            );
                            current_speed = 0;
                            publish_snapshot(
                                &snapshot_tx,
                                &current_url,
                                &current_filename,
                                &current_path,
                                &status,
                                total_size,
                                calculate_downloaded(&active_chunks),
                                0,
                                None,
                                false,
                            );
                            continue;
                        }
                        bytes_since_last_tick = bytes_since_last_tick.saturating_add(bytes_delta);
                    }
                    WorkerMsg::Done { session_id: sid, chunk_id } => {
                        if sid != session_id || status != DownloadStatus::Downloading {
                            continue;
                        }
                        let range_download = file_info.as_ref().is_some_and(uses_range_workers);
                        let total_size = file_info.as_ref().and_then(|info| info.content_length);
                        if !record_done(
                            &mut active_chunks,
                            chunk_id,
                            range_download,
                            total_size,
                        ) {
                            let _ = cancel_tx.send(true);
                            status = DownloadStatus::Failed(
                                "Worker completed before receiving its expected bytes".to_string(),
                            );
                            current_speed = 0;
                            publish_snapshot(
                                &snapshot_tx,
                                &current_url,
                                &current_filename,
                                &current_path,
                                &status,
                                file_info.as_ref().and_then(|info| info.content_length),
                                calculate_downloaded(&active_chunks),
                                0,
                                None,
                                false,
                            );
                            continue;
                        }

                        let all_done = !active_chunks.is_empty() && active_chunks.iter().all(|c| c.is_done);
                        if all_done
                            && let (Some(info), Some(storage)) = (&file_info, &active_storage)
                        {
                            info_handle = Some(spawn_info_fetch(
                                session_id,
                                FetchInfoKind::Complete,
                                current_url.clone(),
                                client.clone(),
                                cancel_tx.subscribe(),
                                info_tx.clone(),
                                Some(SavedDownload::new(info, storage, &active_chunks)),
                            ));
                        }
                    }
                    WorkerMsg::Yielded { session_id: sid, chunk_id } => {
                        if sid != session_id || status != DownloadStatus::Downloading {
                            continue;
                        }
                        if let Some(chunk) = active_chunks.get_mut(chunk_id) {
                            // The acknowledgment follows every committed write; only now may we split.
                            chunk.yield_tx = None;
                            if chunk.retry_requested {
                                chunk.retries += 1;
                                chunk.retry_requested = false;
                            }
                        }
                    }
                    WorkerMsg::Error { session_id: sid, chunk_id, error, retryable } => {
                        if sid != session_id || status != DownloadStatus::Downloading {
                            continue;
                        }
                        if error.is_content_changed() {
                            restart_required = true;
                            continue;
                        }
                        if retryable && file_info.as_ref().is_some_and(uses_range_workers)
                            && let Some(chunk) = active_chunks.get_mut(chunk_id)
                            && chunk.can_retry()
                        {
                            chunk.retries += 1;
                            chunk.yield_tx = None;
                            continue;
                        }
                        let _ = cancel_tx.send(true);
                        wait_for_active_tasks(&mut info_handle, &mut worker_handles).await;
                        // Retain other chunks' queued progress when a network failure stops the session.
                        let drained = drain_cancelled_progress(
                            &mut worker_rx, session_id, &mut active_chunks,
                            file_info.as_ref().is_some_and(uses_range_workers),
                            file_info.as_ref().and_then(|info| info.content_length),
                            retryable,
                        );
                        if matches!(&drained, Err(CoordinatorError::Worker { source, .. }) if source.is_content_changed()) {
                            restart_required = true;
                            continue;
                        }
                        status = DownloadStatus::Failed(format!("Stream #{chunk_id} error: {error}"));
                        current_speed = 0;
                        let can_resume = retryable && match &drained {
                            Ok(()) => true,
                            Err(CoordinatorError::Worker { source, .. }) => source.is_retryable(),
                            Err(_) => false,
                        };
                        publish_snapshot(
                            &snapshot_tx,
                            &current_url,
                            &current_filename,
                            &current_path,
                            &status,
                            file_info.as_ref().and_then(|i| i.content_length),
                            calculate_downloaded(&active_chunks),
                            0,
                            None,
                            can_resume && file_info.as_ref().is_some_and(uses_range_workers),
                        );
                    }
                }
            }

            _ = tick_interval.tick() => {
                if status == DownloadStatus::Downloading {
                    if let (Some(info), Some(storage)) = (&file_info, &active_storage)
                        && let Err(error) = rebalance_workers(
                            &mut active_chunks,
                            &mut worker_handles,
                            current_num_chunks,
                            session_id,
                            &current_url,
                            &client,
                            info,
                            storage,
                            &cancel_tx,
                            &worker_tx,
                        )
                    {
                        let _ = cancel_tx.send(true);
                        status = DownloadStatus::Failed(error.to_string());
                        current_speed = 0;
                        publish_snapshot(
                            &snapshot_tx, &current_url, &current_filename, &current_path,
                            &status, info.content_length, calculate_downloaded(&active_chunks),
                            0, None, uses_range_workers(info),
                        );
                        continue;
                    }
                    let now = Instant::now();
                    let elapsed = now.duration_since(last_tick).as_secs_f64();
                    last_tick = now;

                    if elapsed > 0.05 {
                        let instant_speed = (bytes_since_last_tick as f64 / elapsed) as u64;
                        bytes_since_last_tick = 0;

                        // Exponential moving average for smooth speed display
                        current_speed = if current_speed == 0 {
                            instant_speed
                        } else {
                            ((current_speed as f64 * 0.7) + (instant_speed as f64 * 0.3)) as u64
                        };
                    }

                    let downloaded = calculate_downloaded(&active_chunks);
                    let total_bytes = file_info.as_ref().and_then(|i| i.content_length);

                    let eta_seconds = if let Some(total) = total_bytes {
                        if current_speed > 0 && total > downloaded {
                            Some((total - downloaded) / current_speed)
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    let resumable = file_info.as_ref().is_some_and(uses_range_workers);

                    publish_snapshot(
                        &snapshot_tx,
                        &current_url,
                        &current_filename,
                        &current_path,
                        &status,
                        total_bytes,
                        downloaded,
                        current_speed,
                        eta_seconds,
                        resumable,
                    );
                }
            }
        }
    }
}

fn calculate_downloaded(chunks: &[ActiveChunk]) -> u64 {
    chunks.iter().map(|c| c.downloaded).sum()
}

// construct the UI snapshot here without an intermediate state DTO.
#[allow(clippy::too_many_arguments)]
fn publish_snapshot(
    snapshot_tx: &watch::Sender<DownloadSnapshot>,
    url: &str,
    filename: &str,
    save_path: &std::path::Path,
    status: &DownloadStatus,
    total_bytes: Option<u64>,
    downloaded_bytes: u64,
    speed_bytes_per_sec: u64,
    eta_seconds: Option<u64>,
    resumable: bool,
) {
    let snapshot = DownloadSnapshot {
        url: url.to_string(),
        filename: filename.to_string(),
        save_path: save_path.to_path_buf(),
        status: status.clone(),
        total_bytes,
        downloaded_bytes,
        speed_bytes_per_sec,
        eta_seconds,
        resumable,
    };

    let _ = snapshot_tx.send(snapshot);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pause_preserves_queued_failures_and_other_workers_progress() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut chunks: Vec<_> = calculate_chunks(8, 2)
            .into_iter()
            .map(ActiveChunk::new)
            .collect();
        tx.try_send(WorkerMsg::Error {
            session_id: 0,
            chunk_id: 0,
            error: WorkerError::InvalidRange("stale"),
            retryable: false,
        })
        .unwrap();
        tx.try_send(WorkerMsg::Progress {
            session_id: 1,
            chunk_id: 0,
            bytes_delta: 2,
        })
        .unwrap();
        tx.try_send(WorkerMsg::Error {
            session_id: 1,
            chunk_id: 0,
            error: WorkerError::InvalidRange("invalid range"),
            retryable: false,
        })
        .unwrap();
        tx.try_send(WorkerMsg::Progress {
            session_id: 1,
            chunk_id: 1,
            bytes_delta: 1,
        })
        .unwrap();
        assert_eq!(
            drain_cancelled_progress(&mut rx, 1, &mut chunks, true, Some(8), true)
                .unwrap_err()
                .to_string(),
            "Stream #0 error: invalid range",
        );
        assert_eq!(calculate_downloaded(&chunks), 3);
    }

    #[test]
    fn pause_preserves_only_safely_retryable_failures_for_resume() {
        for (resumable, downloaded, retries, can_resume) in [
            (true, 2, 0, true),
            (false, 2, 0, false),
            (true, 2, MAX_CHUNK_RETRIES, false),
            (true, 4, 0, false),
            (true, 5, 0, false),
        ] {
            let (tx, mut rx) = mpsc::channel(3);
            let mut chunks: Vec<_> = calculate_chunks(8, 2)
                .into_iter()
                .map(ActiveChunk::new)
                .collect();
            chunks[0].retries = retries;
            tx.try_send(WorkerMsg::Progress {
                session_id: 1,
                chunk_id: 0,
                bytes_delta: downloaded,
            })
            .unwrap();
            tx.try_send(WorkerMsg::Error {
                session_id: 1,
                chunk_id: 0,
                error: WorkerError::UnexpectedEof {
                    received: downloaded,
                    expected: 4,
                },
                retryable: true,
            })
            .unwrap();
            tx.try_send(WorkerMsg::Progress {
                session_id: 1,
                chunk_id: 1,
                bytes_delta: 1,
            })
            .unwrap();

            let result =
                drain_cancelled_progress(&mut rx, 1, &mut chunks, true, Some(8), resumable);
            assert_eq!(result.is_ok(), can_resume);
            assert_eq!(chunks[1].downloaded, 1, "drain all committed progress");
            if can_resume {
                assert_eq!(chunks[0].downloaded, downloaded);
                assert!(resume_chunks_are_valid(&chunks, 8));
            }
            if downloaded > 4 {
                assert!(matches!(result, Err(CoordinatorError::InvalidProgress)));
            }
        }
    }

    #[test]
    fn dynamic_partitions_preserve_progress_and_resume_coverage() {
        for total in [2 * MIN_SPLIT_BYTES + 17, u64::MAX] {
            let mut chunks: Vec<_> = calculate_chunks(total, 1)
                .into_iter()
                .map(ActiveChunk::new)
                .collect();
            chunks[0].downloaded = 17;
            chunks[0].retries = 2;
            let (yield_tx, _yield_rx) = watch::channel(false);
            chunks[0].yield_tx = Some(yield_tx);
            assert!(
                !split_remaining(&mut chunks, 0),
                "cannot split a live writer"
            );
            chunks[0].yield_tx = None;
            assert!(split_remaining(&mut chunks, 0));
            assert_eq!(calculate_downloaded(&chunks), 17);
            assert_eq!(chunks[1].retries, 2);
            assert_eq!(chunks[0].range.end + 1, chunks[1].range.start);
            assert!(resume_chunks_are_valid(&chunks, total));
            chunks[1].range.start -= 1;
            assert!(!resume_chunks_are_valid(&chunks, total), "overlap");
            chunks[1].range.start += 2;
            assert!(!resume_chunks_are_valid(&chunks, total), "gap");
            chunks[1].range.start -= 1;
            chunks[1].range.id = 0;
            assert!(!resume_chunks_are_valid(&chunks, total), "duplicate ID");
            chunks[1].range.id = 1;
            chunks[0].downloaded = chunks[0].range.size() + 1;
            assert!(!resume_chunks_are_valid(&chunks, total), "progress overrun");
            chunks[0].downloaded = 17;
            chunks[0].is_done = true;
            assert!(
                !resume_chunks_are_valid(&chunks, total),
                "premature completion"
            );
        }
        let mut chunks = vec![ActiveChunk::new(ChunkRange {
            id: 0,
            start: 0,
            end: 2 * MIN_SPLIT_BYTES - 2,
        })];
        assert!(!split_remaining(&mut chunks, 0), "avoid tiny tail requests");

        let total = 8 * MIN_SPLIT_BYTES;
        let mut chunks: Vec<_> = calculate_chunks(total, 2)
            .into_iter()
            .map(ActiveChunk::new)
            .collect();
        chunks[0].downloaded = 17;
        assert!(split_remaining(&mut chunks, 0));
        assert!(chunks[2].range.start < chunks[1].range.start);
        assert!(resume_chunks_are_valid(&chunks, total));
    }
}
