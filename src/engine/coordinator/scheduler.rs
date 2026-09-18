use super::{CoordinatorError, uses_range_workers};
use crate::client::{HttpClient, RemoteFileInfo};
use crate::engine::chunks::{ChunkRange, calculate_chunks};
use crate::engine::worker::{WorkerMsg, spawn_chunk_worker, spawn_stream_worker};
use crate::storage::Storage;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

pub(super) struct ActiveChunk {
    pub(super) range: ChunkRange,
    pub(super) downloaded: u64,
    pub(super) is_done: bool,
    pub(super) yield_tx: Option<watch::Sender<bool>>,
    pub(super) split_requested: bool,
    pub(super) retry_requested: bool,
    pub(super) retries: u8,
    pub(super) last_progress: Instant,
}

pub(super) const MIN_SPLIT_BYTES: u64 = 256 * 1024;
pub(super) const MAX_CHUNK_RETRIES: u8 = 3;
const STALL_TIMEOUT: Duration = Duration::from_secs(5);

impl ActiveChunk {
    pub(super) fn new(range: ChunkRange) -> Self {
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

    pub(super) fn can_retry(&self) -> bool {
        !self.is_done && self.downloaded < self.range.size() && self.retries < MAX_CHUNK_RETRIES
    }

    // pass borrowed session state rather than duplicating it in a worker context.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn(
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

// explicit session inputs keep worker creation independent of coordinator ownership.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_download_workers(
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

pub(super) fn split_remaining(chunks: &mut Vec<ActiveChunk>, index: usize) -> bool {
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
pub(super) fn rebalance_workers(
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
