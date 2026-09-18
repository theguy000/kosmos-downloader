use super::OVERLAP_BYTES;
use super::body::{BodyCopyMode, copy_response_body};
use super::error::{WorkerError, is_retryable_client_error};
use super::protocol::{WorkerMsg, send_worker_msg, send_yielded};
use crate::client::{HttpClient, is_strong_etag};
use crate::engine::chunks::ChunkRange;
use crate::storage::Storage;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

// direct inputs match one chunk's coordinator state without a one-use context struct.
#[allow(clippy::too_many_arguments)]
pub(in crate::engine) fn spawn_chunk_worker(
    session_id: u64,
    range: ChunkRange,
    initial_downloaded: u64,
    total_size: u64,
    url: String,
    client: HttpClient,
    storage: Storage,
    if_range: Option<String>,
    mut cancel_rx: watch::Receiver<bool>,
    mut yield_rx: watch::Receiver<bool>,
    worker_tx: mpsc::Sender<WorkerMsg>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let chunk_id = range.id;
        if *cancel_rx.borrow() {
            return;
        }
        if *yield_rx.borrow() {
            send_yielded(&worker_tx, &mut cancel_rx, session_id, chunk_id).await;
            return;
        }

        let Some(chunk_size) = range
            .end
            .checked_sub(range.start)
            .and_then(|size| size.checked_add(1))
        else {
            let _ = send_worker_msg(
                &worker_tx,
                &mut cancel_rx,
                WorkerMsg::Error {
                    session_id,
                    chunk_id,
                    error: WorkerError::InvalidRange("Invalid chunk range"),
                    retryable: false,
                },
            )
            .await;
            return;
        };

        if initial_downloaded > chunk_size {
            let _ = send_worker_msg(
                &worker_tx,
                &mut cancel_rx,
                WorkerMsg::Error {
                    session_id,
                    chunk_id,
                    error: WorkerError::InvalidRange("Saved chunk progress exceeds its range"),
                    retryable: false,
                },
            )
            .await;
            return;
        }

        if initial_downloaded == chunk_size {
            let _ = send_worker_msg(
                &worker_tx,
                &mut cancel_rx,
                WorkerMsg::Done {
                    session_id,
                    chunk_id,
                },
            )
            .await;
            return;
        }

        let Some(start) = range.start.checked_add(initial_downloaded) else {
            let _ = send_worker_msg(
                &worker_tx,
                &mut cancel_rx,
                WorkerMsg::Error {
                    session_id,
                    chunk_id,
                    error: WorkerError::InvalidRange("Chunk offset overflowed"),
                    retryable: false,
                },
            )
            .await;
            return;
        };
        // Bounded overlap catches local changes, not changes elsewhere in the file.
        let overlap = if if_range.as_deref().is_some_and(is_strong_etag) {
            0
        } else {
            initial_downloaded.min(OVERLAP_BYTES)
        };
        let request_start = start - overlap;
        let expected_bytes = chunk_size - initial_downloaded + overlap;

        let response = loop {
            if *cancel_rx.borrow() {
                return;
            }
            if *yield_rx.borrow() {
                send_yielded(&worker_tx, &mut cancel_rx, session_id, chunk_id).await;
                return;
            }

            tokio::select! {
                biased;
                _ = cancel_rx.changed() => return,
                changed = yield_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                res = client.download_range_checked(
                    &url,
                    request_start,
                    Some(range.end),
                    Some(total_size),
                    if_range.as_deref(),
                ) => {
                    match res {
                        Ok(response) => break response,
                        Err(e) => {
                            if *cancel_rx.borrow() {
                                return;
                            }
                            let retryable = is_retryable_client_error(&e);
                            let _ = send_worker_msg(
                                &worker_tx,
                                &mut cancel_rx,
                                WorkerMsg::Error {
                                    session_id,
                                    chunk_id,
                                    error: WorkerError::Client(e),
                                    retryable,
                                },
                            )
                            .await;
                            return;
                        }
                    }
                }
            }
        };

        copy_response_body(
            response,
            request_start,
            BodyCopyMode::Range {
                expected_bytes,
                write_start: start,
            },
            session_id,
            chunk_id,
            &storage,
            &mut cancel_rx,
            Some(&mut yield_rx),
            &worker_tx,
        )
        .await;
    })
}
