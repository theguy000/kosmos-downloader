use super::chunks::{ChunkRange, calculate_chunks};
use super::model::{DownloadAction, DownloadSnapshot, DownloadStatus};
use super::worker::{WorkerError, WorkerMsg, spawn_chunk_worker, spawn_stream_worker};
use crate::client::{HttpClient, RemoteFileInfo};
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
}

struct FetchInfoMsg {
    session_id: u64,
    kind: FetchInfoKind,
    result: Result<RemoteFileInfo, crate::client::ClientError>,
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
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result = tokio::select! {
            _ = cancel_rx.changed() => return,
            result = client.fetch_info(&url) => result,
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

fn uses_range_workers(info: &RemoteFileInfo) -> bool {
    info.accepts_ranges && matches!(info.content_length, Some(total_size) if total_size > 0)
}

fn is_resumable(info: &RemoteFileInfo) -> bool {
    uses_range_workers(info) && info.resume_validator().is_some()
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
    let (Some(total_size), Some(validator)) = (info.content_length, info.resume_validator()) else {
        return Ok(());
    };
    if !uses_range_workers(info) {
        return Ok(());
    }
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
            session_id,
            url,
            client,
            total_size,
            Some(validator),
            storage,
            cancel_tx,
            worker_tx,
        ));
    }

    // idle connections split the largest remaining range; no throughput model needed.
    if unfinished < max_workers
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
                if !can_resume {
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

    let (worker_tx, mut worker_rx) = mpsc::channel::<WorkerMsg>(256);
    let (info_tx, mut info_rx) = mpsc::channel::<FetchInfoMsg>(8);
    let (mut cancel_tx, _) = watch::channel(false);

    let mut last_tick = Instant::now();
    let mut bytes_since_last_tick: u64 = 0;
    let mut current_speed: u64 = 0;

    let mut tick_interval = tokio::time::interval(Duration::from_millis(200));

    loop {
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
                                file_info.as_ref().is_some_and(is_resumable),
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
                                    file_info.as_ref().is_none_or(is_resumable),
                                );
                              }
                              Err(error) => {
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
                        if status == DownloadStatus::Paused {
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
                                file_info.as_ref().is_none_or(is_resumable),
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

                if msg.session_id != session_id || status != DownloadStatus::Connecting {
                    continue;
                }

                info_handle = None;
                match (msg.kind, msg.result) {
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
                                let resumable = is_resumable(&info);
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
                    (FetchInfoKind::Resume, Ok(info)) => {
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

                        let was_resumable = file_info.as_ref().is_some_and(is_resumable);
                        let resume = match (
                            info.content_length,
                            file_info
                                .as_ref()
                                .and_then(|previous| previous.resume_validator_for(&info)),
                        ) {
                            (Some(total_size), Some(validator))
                                if uses_range_workers(&info)
                                    && resume_chunks_are_valid(&active_chunks, total_size) =>
                            {
                                Some((total_size, validator))
                            }
                            _ => None,
                        };

                        let total_size = info.content_length;
                        if was_resumable {
                            if let Some((total_size, validator)) = resume {
                                file_info = Some(info);
                                status = DownloadStatus::Downloading;
                                for chunk in &mut active_chunks {
                                    if !chunk.is_done {
                                        chunk.retries = 0;
                                        worker_handles.push(chunk.spawn(
                                            session_id,
                                            &current_url,
                                            &client,
                                            total_size,
                                            Some(&validator),
                                            storage,
                                            &cancel_tx,
                                            &worker_tx,
                                        ));
                                    }
                                }

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
                                        file_info.as_ref().is_some_and(is_resumable),
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
                            false,
                        );
                    }
                    (FetchInfoKind::Resume, Err(err)) => {
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
                            file_info.as_ref().is_some_and(is_resumable),
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
                        if all_done {
                            let flush = if let Some(storage) = &active_storage {
                                let storage = storage.clone();
                                tokio::task::spawn_blocking(move || storage.sync())
                                    .await
                                    .map_err(CoordinatorError::StorageTask)
                                    .and_then(|result| result.map_err(CoordinatorError::Storage))
                            } else {
                                Ok(())
                            };
                            if let Err(err) = flush {
                                status = DownloadStatus::Failed(format!(
                                    "Failed to flush completed download: {err}"
                                ));
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
                            status = DownloadStatus::Completed;
                            current_speed = 0;
                            let total = file_info.as_ref().and_then(|i| i.content_length).unwrap_or_else(|| calculate_downloaded(&active_chunks));
                            publish_snapshot(
                                &snapshot_tx,
                                &current_url,
                                &current_filename,
                                &current_path,
                                &status,
                                Some(total),
                                total,
                                0,
                                None,
                                true,
                            );
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
                        if retryable && file_info.as_ref().is_some_and(is_resumable)
                            && let Some(chunk) = active_chunks.get_mut(chunk_id)
                            && chunk.can_retry()
                        {
                            chunk.retries += 1;
                            chunk.yield_tx = None;
                            continue;
                        }
                        let _ = cancel_tx.send(true);
                        status = DownloadStatus::Failed(format!("Stream #{chunk_id} error: {error}"));
                        current_speed = 0;
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
                            file_info.as_ref().is_some_and(is_resumable),
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
                            0, None, is_resumable(info),
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

                    let resumable = file_info.as_ref().is_some_and(is_resumable);

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
