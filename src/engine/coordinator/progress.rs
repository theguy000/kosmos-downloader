use super::metadata::{FetchInfoKind, spawn_info_fetch};
use super::resume::SavedDownload;
use super::scheduler::ActiveChunk;
use super::{CoordinatorError, Session, calculate_downloaded, uses_range_workers};
use crate::engine::model::DownloadStatus;
use crate::engine::worker::WorkerMsg;
use std::time::Instant;
use tokio::sync::mpsc;

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

pub(super) fn drain_cancelled_progress(
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

impl Session {
    pub(super) async fn handle_worker(&mut self, msg: WorkerMsg) {
        match msg {
            WorkerMsg::Progress {
                session_id: sid,
                chunk_id,
                bytes_delta,
            } => {
                if sid != self.session_id || self.status != DownloadStatus::Downloading {
                    return;
                }
                let range_download = self.file_info.as_ref().is_some_and(uses_range_workers);
                let total_size = self.file_info.as_ref().and_then(|info| info.content_length);
                if !record_progress(
                    &mut self.active_chunks,
                    chunk_id,
                    bytes_delta,
                    range_download,
                    total_size,
                ) {
                    let _ = self.cancel_tx.send(true);
                    self.status = DownloadStatus::Failed(
                        "Worker reported invalid download progress".to_string(),
                    );
                    self.current_speed = 0;
                    self.publish(
                        total_size,
                        calculate_downloaded(&self.active_chunks),
                        0,
                        None,
                        false,
                    );
                    return;
                }
                self.bytes_since_last_tick = self.bytes_since_last_tick.saturating_add(bytes_delta);
            }
            WorkerMsg::Done {
                session_id: sid,
                chunk_id,
            } => {
                if sid != self.session_id || self.status != DownloadStatus::Downloading {
                    return;
                }
                let range_download = self.file_info.as_ref().is_some_and(uses_range_workers);
                let total_size = self.file_info.as_ref().and_then(|info| info.content_length);
                if !record_done(
                    &mut self.active_chunks,
                    chunk_id,
                    range_download,
                    total_size,
                ) {
                    let _ = self.cancel_tx.send(true);
                    self.status = DownloadStatus::Failed(
                        "Worker completed before receiving its expected bytes".to_string(),
                    );
                    self.current_speed = 0;
                    self.publish(
                        self.file_info.as_ref().and_then(|info| info.content_length),
                        calculate_downloaded(&self.active_chunks),
                        0,
                        None,
                        false,
                    );
                    return;
                }

                let all_done =
                    !self.active_chunks.is_empty() && self.active_chunks.iter().all(|c| c.is_done);
                if all_done
                    && let (Some(info), Some(storage)) = (&self.file_info, &self.active_storage)
                {
                    self.info_handle = Some(spawn_info_fetch(
                        self.session_id,
                        FetchInfoKind::Complete,
                        self.current_url.clone(),
                        self.client.clone(),
                        self.cancel_tx.subscribe(),
                        self.info_tx.clone(),
                        Some(SavedDownload::new(info, storage, &self.active_chunks)),
                    ));
                }
            }
            WorkerMsg::Yielded {
                session_id: sid,
                chunk_id,
            } => {
                if sid != self.session_id || self.status != DownloadStatus::Downloading {
                    return;
                }
                if let Some(chunk) = self.active_chunks.get_mut(chunk_id) {
                    // The acknowledgment follows every committed write; only now may we split.
                    chunk.yield_tx = None;
                    if chunk.retry_requested {
                        chunk.retries += 1;
                        chunk.retry_requested = false;
                    }
                }
            }
            WorkerMsg::Error {
                session_id: sid,
                chunk_id,
                error,
                retryable,
            } => {
                if sid != self.session_id || self.status != DownloadStatus::Downloading {
                    return;
                }
                if error.is_content_changed() {
                    self.restart_required = true;
                    return;
                }
                if retryable
                    && self.file_info.as_ref().is_some_and(uses_range_workers)
                    && let Some(chunk) = self.active_chunks.get_mut(chunk_id)
                    && chunk.can_retry()
                {
                    chunk.retries += 1;
                    chunk.yield_tx = None;
                    return;
                }
                let _ = self.cancel_tx.send(true);
                self.wait_for_active_tasks().await;
                // Retain other chunks' queued progress when a network failure stops the session.
                let drained = drain_cancelled_progress(
                    &mut self.worker_rx,
                    self.session_id,
                    &mut self.active_chunks,
                    self.file_info.as_ref().is_some_and(uses_range_workers),
                    self.file_info.as_ref().and_then(|info| info.content_length),
                    retryable,
                );
                if matches!(&drained, Err(CoordinatorError::Worker { source, .. }) if source.is_content_changed())
                {
                    self.restart_required = true;
                    return;
                }
                self.status = DownloadStatus::Failed(format!("Stream #{chunk_id} error: {error}"));
                self.current_speed = 0;
                let can_resume = retryable
                    && match &drained {
                        Ok(()) => true,
                        Err(CoordinatorError::Worker { source, .. }) => source.is_retryable(),
                        Err(_) => false,
                    };
                self.publish(
                    self.file_info.as_ref().and_then(|i| i.content_length),
                    calculate_downloaded(&self.active_chunks),
                    0,
                    None,
                    can_resume && self.file_info.as_ref().is_some_and(uses_range_workers),
                );
            }
        }
    }
}
