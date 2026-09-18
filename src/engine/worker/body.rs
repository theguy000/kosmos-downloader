use super::error::{WorkerError, is_retryable_body_error};
use super::protocol::{WorkerMsg, send_worker_msg, send_yielded};
use crate::client::ClientError;
use crate::storage::Storage;
use tokio::sync::{mpsc, watch};

#[derive(Clone, Copy)]
pub(super) enum BodyCopyMode {
    Range {
        expected_bytes: u64,
        write_start: u64,
    },
    Stream {
        expected_bytes: Option<u64>,
    },
}

impl BodyCopyMode {
    fn expected_bytes(self) -> Option<u64> {
        match self {
            Self::Range { expected_bytes, .. } => Some(expected_bytes),
            Self::Stream { expected_bytes } => expected_bytes,
        }
    }

    fn overrun_error(self, expected_bytes: u64) -> WorkerError {
        match self {
            Self::Range { .. } => WorkerError::RangeResponseOverrun {
                expected: expected_bytes,
            },
            Self::Stream { .. } => WorkerError::ResponseOverrun {
                expected: expected_bytes,
            },
        }
    }

    fn offset_error(self) -> WorkerError {
        WorkerError::InvalidRange(match self {
            Self::Range { .. } => "Chunk offset overflowed",
            Self::Stream { .. } => "Stream offset overflowed",
        })
    }
}

// explicit parameters avoid a one-use context struct.
#[allow(clippy::too_many_arguments)]
pub(super) async fn copy_response_body(
    response: reqwest::Response,
    start: u64,
    mode: BodyCopyMode,
    session_id: u64,
    chunk_id: usize,
    storage: &Storage,
    cancel_rx: &mut watch::Receiver<bool>,
    mut yield_rx: Option<&mut watch::Receiver<bool>>,
    worker_tx: &mpsc::Sender<WorkerMsg>,
) {
    let expected_bytes = mode.expected_bytes();
    let mut response = response;
    let mut current_offset = start;
    let mut received = 0u64;

    loop {
        if *cancel_rx.borrow() {
            return;
        }
        let can_yield = expected_bytes.is_none_or(|expected_bytes| received < expected_bytes);
        if can_yield
            && yield_rx
                .as_deref()
                .is_some_and(|yield_rx| *yield_rx.borrow())
        {
            drop(response);
            send_yielded(worker_tx, cancel_rx, session_id, chunk_id).await;
            return;
        }

        let item = if can_yield && let Some(yield_rx) = yield_rx.as_deref_mut() {
            tokio::select! {
                biased;
                _ = cancel_rx.changed() => return,
                changed = yield_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    continue;
                }
                item = response.chunk() => item,
            }
        } else {
            tokio::select! {
                _ = cancel_rx.changed() => return,
                item = response.chunk() => item,
            }
        };

        match item {
            Ok(Some(bytes)) => {
                if *cancel_rx.borrow() {
                    return;
                }
                let len = bytes.len() as u64;
                if let Some(expected_bytes) = expected_bytes {
                    let remaining = expected_bytes.saturating_sub(received);
                    if len > remaining {
                        let _ = send_worker_msg(
                            worker_tx,
                            cancel_rx,
                            WorkerMsg::Error {
                                session_id,
                                chunk_id,
                                error: mode.overrun_error(expected_bytes),
                                retryable: false,
                            },
                        )
                        .await;
                        return;
                    }
                }
                let Some(next_offset) = current_offset.checked_add(len) else {
                    let _ = send_worker_msg(
                        worker_tx,
                        cancel_rx,
                        WorkerMsg::Error {
                            session_id,
                            chunk_id,
                            error: mode.offset_error(),
                            retryable: false,
                        },
                    )
                    .await;
                    return;
                };
                let overlap = match mode {
                    BodyCopyMode::Range { write_start, .. } => {
                        write_start.saturating_sub(current_offset).min(len) as usize
                    }
                    BodyCopyMode::Stream { .. } => 0,
                };
                let bytes_delta = len - overlap as u64;
                // Verify the overlap from this same response before appending any new bytes.
                // Do not select cancellation here: a started write must finish before this worker exits.
                let write_result = tokio::task::spawn_blocking({
                    let storage = storage.clone();
                    move || -> Result<(), WorkerError> {
                        if overlap > 0 {
                            let mut saved = vec![0; overlap];
                            storage.read_at(current_offset, &mut saved)?;
                            if saved != bytes[..overlap] {
                                return Err(ClientError::ContentChanged.into());
                            }
                        }
                        if overlap < bytes.len() {
                            storage.write_at(current_offset + overlap as u64, &bytes[overlap..])?;
                        }
                        Ok(())
                    }
                })
                .await
                .map_err(WorkerError::StorageTask)
                .and_then(|result| result);
                if let Err(error) = write_result {
                    let _ = send_worker_msg(
                        worker_tx,
                        cancel_rx,
                        WorkerMsg::Error {
                            session_id,
                            chunk_id,
                            error,
                            retryable: false,
                        },
                    )
                    .await;
                    return;
                }
                current_offset = next_offset;
                received += len;
                if bytes_delta > 0
                    && !send_worker_msg(
                        worker_tx,
                        cancel_rx,
                        WorkerMsg::Progress {
                            session_id,
                            chunk_id,
                            bytes_delta,
                        },
                    )
                    .await
                {
                    return;
                }
            }
            Err(e) => {
                if *cancel_rx.borrow() {
                    return;
                }
                let retryable = is_retryable_body_error(&e);
                let _ = send_worker_msg(
                    worker_tx,
                    cancel_rx,
                    WorkerMsg::Error {
                        session_id,
                        chunk_id,
                        error: WorkerError::ResponseBody(e),
                        retryable,
                    },
                )
                .await;
                return;
            }
            Ok(None) => {
                if *cancel_rx.borrow() {
                    return;
                }
                if let Some(expected_bytes) = expected_bytes {
                    if received != expected_bytes {
                        let _ = send_worker_msg(
                            worker_tx,
                            cancel_rx,
                            WorkerMsg::Error {
                                session_id,
                                chunk_id,
                                error: WorkerError::UnexpectedEof {
                                    received,
                                    expected: expected_bytes,
                                },
                                retryable: true,
                            },
                        )
                        .await;
                        return;
                    }
                } else {
                    // Do not select cancellation here: a started resize must finish before this worker exits.
                    let set_len_result = tokio::task::spawn_blocking({
                        let storage = storage.clone();
                        move || storage.set_len(current_offset)
                    })
                    .await
                    .map_err(WorkerError::StorageTask)
                    .and_then(|result| result.map_err(WorkerError::Storage));
                    if let Err(error) = set_len_result {
                        let _ = send_worker_msg(
                            worker_tx,
                            cancel_rx,
                            WorkerMsg::Error {
                                session_id,
                                chunk_id,
                                error,
                                retryable: false,
                            },
                        )
                        .await;
                        return;
                    }
                }
                let _ = send_worker_msg(
                    worker_tx,
                    cancel_rx,
                    WorkerMsg::Done {
                        session_id,
                        chunk_id,
                    },
                )
                .await;
                return;
            }
        }
    }
}
