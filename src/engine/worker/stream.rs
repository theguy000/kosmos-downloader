use super::body::{BodyCopyMode, copy_response_body};
use super::error::{WorkerError, is_retryable_client_error};
use super::protocol::{WorkerMsg, send_worker_msg};
use crate::client::HttpClient;
use crate::storage::Storage;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

pub(in crate::engine) fn spawn_stream_worker(
    session_id: u64,
    url: String,
    total_size: Option<u64>,
    client: HttpClient,
    storage: Storage,
    mut cancel_rx: watch::Receiver<bool>,
    worker_tx: mpsc::Sender<WorkerMsg>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let chunk_id = 0;
        let response = tokio::select! {
            _ = cancel_rx.changed() => return,
            res = client.download_range(&url, 0, None) => {
                match res {
                    Ok(r) => r,
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
        };

        copy_response_body(
            response,
            0,
            BodyCopyMode::Stream {
                expected_bytes: total_size,
            },
            session_id,
            chunk_id,
            &storage,
            &mut cancel_rx,
            None,
            &worker_tx,
        )
        .await;
    })
}
