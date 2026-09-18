use super::error::WorkerError;
use tokio::sync::{mpsc, watch};

pub(in crate::engine) enum WorkerMsg {
    Progress {
        session_id: u64,
        chunk_id: usize,
        bytes_delta: u64,
    },
    Done {
        session_id: u64,
        chunk_id: usize,
    },
    Yielded {
        session_id: u64,
        chunk_id: usize,
    },
    Error {
        session_id: u64,
        chunk_id: usize,
        error: WorkerError,
        retryable: bool,
    },
}

pub(super) async fn send_worker_msg(
    worker_tx: &mpsc::Sender<WorkerMsg>,
    cancel_rx: &mut watch::Receiver<bool>,
    msg: WorkerMsg,
) -> bool {
    tokio::select! {
        _ = cancel_rx.changed() => false,
        result = worker_tx.send(msg) => result.is_ok(),
    }
}

pub(super) async fn send_yielded(
    worker_tx: &mpsc::Sender<WorkerMsg>,
    cancel_rx: &mut watch::Receiver<bool>,
    session_id: u64,
    chunk_id: usize,
) {
    let _ = send_worker_msg(
        worker_tx,
        cancel_rx,
        WorkerMsg::Yielded {
            session_id,
            chunk_id,
        },
    )
    .await;
}
