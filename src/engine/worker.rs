use super::chunks::ChunkRange;
use crate::client::{ClientError, HttpClient};
use crate::storage::{Storage, StorageError};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

#[derive(Debug, Error)]
pub(super) enum WorkerError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("{0}")]
    ResponseBody(#[from] reqwest::Error),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("Blocking storage task failed: {0}")]
    StorageTask(#[from] tokio::task::JoinError),
    #[error("{0}")]
    InvalidRange(&'static str),
    #[error("Range response exceeded its expected {expected} bytes")]
    RangeResponseOverrun { expected: u64 },
    #[error("Response exceeded its expected {expected} bytes")]
    ResponseOverrun { expected: u64 },
    #[error("Unexpected EOF: received {received} of {expected} bytes")]
    UnexpectedEof { received: u64, expected: u64 },
}

pub(super) enum WorkerMsg {
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

async fn send_worker_msg(
    worker_tx: &mpsc::Sender<WorkerMsg>,
    cancel_rx: &mut watch::Receiver<bool>,
    msg: WorkerMsg,
) -> bool {
    tokio::select! {
        _ = cancel_rx.changed() => false,
        result = worker_tx.send(msg) => result.is_ok(),
    }
}

fn is_retryable_request_error(error: &reqwest::Error) -> bool {
    !error.is_builder()
        && !error.is_decode()
        && (error.is_connect() || error.is_timeout() || error.is_request() || error.is_body())
}

fn is_retryable_body_error(error: &reqwest::Error) -> bool {
    // `Response::chunk` wraps lower-level frame failures as decode errors.
    is_retryable_request_error(error) || (!error.is_builder() && error.is_decode())
}

fn is_retryable_client_error(error: &ClientError) -> bool {
    match error {
        ClientError::Http(error) => is_retryable_request_error(error),
        ClientError::BadStatus(status, _) => {
            *status == reqwest::StatusCode::REQUEST_TIMEOUT
                || *status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status.is_server_error()
        }
        ClientError::InvalidUrl(_) | ClientError::InvalidRangeResponse(_) => false,
    }
}

async fn send_yielded(
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

#[derive(Clone, Copy)]
enum BodyCopyMode {
    Range { expected_bytes: u64 },
    Stream { expected_bytes: Option<u64> },
}

impl BodyCopyMode {
    fn expected_bytes(self) -> Option<u64> {
        match self {
            Self::Range { expected_bytes } => Some(expected_bytes),
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
async fn copy_response_body(
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
                // Do not select cancellation here: a started write must finish before this worker exits.
                let write_result = tokio::task::spawn_blocking({
                    let storage = storage.clone();
                    move || storage.write_at(current_offset, &bytes)
                })
                .await
                .map_err(WorkerError::StorageTask)
                .and_then(|result| result.map_err(WorkerError::Storage));
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
                if !send_worker_msg(
                    worker_tx,
                    cancel_rx,
                    WorkerMsg::Progress {
                        session_id,
                        chunk_id,
                        bytes_delta: len,
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

// direct inputs match one chunk's coordinator state without a one-use context struct.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_chunk_worker(
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
        let expected_bytes = chunk_size - initial_downloaded;

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
                    start,
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
            start,
            BodyCopyMode::Range { expected_bytes },
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

pub(super) fn spawn_stream_worker(
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc as std_mpsc;
    use std::thread;
    use std::time::Duration;

    static NEXT_TEMP_FILE: AtomicUsize = AtomicUsize::new(0);

    struct TempFile {
        path: PathBuf,
    }

    impl TempFile {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "kosmos-worker-{name}-{}-{}",
                std::process::id(),
                NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_file(&path);
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn chunked_response(status: &str, headers: &str, chunks: &[&[u8]]) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 {status}\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n{headers}\r\n"
        )
        .into_bytes();
        for chunk in chunks {
            response.extend_from_slice(format!("{:X}\r\n", chunk.len()).as_bytes());
            response.extend_from_slice(chunk);
            response.extend_from_slice(b"\r\n");
        }
        response.extend_from_slice(b"0\r\n\r\n");
        response
    }

    fn start_response_server(response: Vec<u8>) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0; 1024];
            if socket.read(&mut request).unwrap() == 0 {
                return;
            }
            socket.write_all(&response).unwrap();
        });
        (format!("http://{address}/download"), server)
    }

    fn start_delayed_response_server() -> (String, std_mpsc::Sender<()>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (release_tx, release_rx) = std_mpsc::channel();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0; 1024];
            if socket.read(&mut request).unwrap() == 0 {
                return;
            }
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n3\r\none\r\n",
                )
                .unwrap();
            socket.flush().unwrap();

            let _ = release_rx.recv_timeout(Duration::from_secs(2));
            let _ = socket.write_all(b"3\r\ntwo\r\n0\r\n\r\n");
        });
        (format!("http://{address}/download"), release_tx, server)
    }

    fn start_yield_ordering_server() -> (
        String,
        std_mpsc::Sender<()>,
        std_mpsc::Sender<()>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (release_second_tx, release_second_rx) = std_mpsc::channel();
        let (release_third_tx, release_third_rx) = std_mpsc::channel();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0; 1024];
            if socket.read(&mut request).unwrap() == 0 {
                return;
            }
            socket
                .write_all(
                    b"HTTP/1.1 206 Partial Content\r\nConnection: close\r\nTransfer-Encoding: chunked\r\nContent-Range: bytes 0-8/9\r\n\r\n3\r\none\r\n",
                )
                .unwrap();
            socket.flush().unwrap();

            let _ = release_second_rx.recv_timeout(Duration::from_secs(2));
            let _ = socket.write_all(b"3\r\ntwo\r\n");
            let _ = socket.flush();

            let _ = release_third_rx.recv_timeout(Duration::from_secs(2));
            let _ = socket.write_all(b"3\r\ntri\r\n0\r\n\r\n");
        });
        (
            format!("http://{address}/download"),
            release_second_tx,
            release_third_tx,
            server,
        )
    }

    fn take_messages(worker_rx: &mut tokio::sync::mpsc::Receiver<WorkerMsg>) -> Vec<WorkerMsg> {
        let mut messages = Vec::new();
        while let Ok(message) = worker_rx.try_recv() {
            messages.push(message);
        }
        messages
    }

    fn only_error(
        mut messages: Vec<WorkerMsg>,
        session_id: u64,
        chunk_id: usize,
    ) -> (WorkerError, bool) {
        assert_eq!(messages.len(), 1);
        let Some(WorkerMsg::Error {
            session_id: actual_session_id,
            chunk_id: actual_chunk_id,
            error,
            retryable,
        }) = messages.pop()
        else {
            panic!("expected worker error");
        };
        assert_eq!(actual_session_id, session_id);
        assert_eq!(actual_chunk_id, chunk_id);
        (error, retryable)
    }

    async fn join_worker(worker: JoinHandle<()>) {
        tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("worker did not finish")
            .expect("worker panicked");
    }

    #[tokio::test]
    async fn unknown_length_stream_truncates_to_received_bytes() {
        let temp_file = TempFile::new("unknown-length");
        let storage = Storage::create_or_open(temp_file.path(), Some(64), true).unwrap();
        let chunks: [&[u8]; 2] = [b"unknown ", b"length"];
        let (url, server) = start_response_server(chunked_response("200 OK", "", &chunks));
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel(8);

        let worker = spawn_stream_worker(
            41,
            url,
            None,
            HttpClient::new(),
            storage,
            cancel_rx,
            worker_tx,
        );
        join_worker(worker).await;
        server.join().unwrap();

        let mut downloaded = 0;
        let mut done = false;
        for message in take_messages(&mut worker_rx) {
            match message {
                WorkerMsg::Progress {
                    session_id,
                    chunk_id,
                    bytes_delta,
                } => {
                    assert_eq!(session_id, 41);
                    assert_eq!(chunk_id, 0);
                    downloaded += bytes_delta;
                }
                WorkerMsg::Done {
                    session_id,
                    chunk_id,
                } => {
                    assert_eq!(session_id, 41);
                    assert_eq!(chunk_id, 0);
                    done = true;
                }
                WorkerMsg::Yielded { .. } | WorkerMsg::Error { .. } => {
                    panic!("unknown-length stream failed")
                }
            }
        }

        assert_eq!(downloaded, 14);
        assert!(done);
        assert_eq!(std::fs::read(temp_file.path()).unwrap(), b"unknown length");
        assert_eq!(std::fs::metadata(temp_file.path()).unwrap().len(), 14);
    }

    #[tokio::test]
    async fn range_body_overrun_is_rejected_before_writing() {
        let temp_file = TempFile::new("range-overrun");
        let storage = Storage::create_or_open(temp_file.path(), Some(3), true).unwrap();
        storage.write_at(0, b"old").unwrap();
        let chunks: [&[u8]; 1] = [b"four"];
        let (url, server) = start_response_server(chunked_response(
            "206 Partial Content",
            "Content-Range: bytes 0-2/3\r\n",
            &chunks,
        ));
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let (_yield_tx, yield_rx) = watch::channel(false);
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel(8);

        let worker = spawn_chunk_worker(
            42,
            ChunkRange {
                id: 7,
                start: 0,
                end: 2,
            },
            0,
            3,
            url,
            HttpClient::new(),
            storage,
            None,
            cancel_rx,
            yield_rx,
            worker_tx,
        );
        join_worker(worker).await;
        server.join().unwrap();

        let (error, retryable) = only_error(take_messages(&mut worker_rx), 42, 7);
        assert_eq!(
            error.to_string(),
            "Range response exceeded its expected 3 bytes"
        );
        assert!(matches!(
            error,
            WorkerError::RangeResponseOverrun { expected: 3 }
        ));
        assert!(!retryable);
        assert_eq!(std::fs::read(temp_file.path()).unwrap(), b"old");
    }

    #[tokio::test]
    async fn invalid_chunk_range_is_typed() {
        let temp_file = TempFile::new("invalid-range");
        let storage = Storage::create_or_open(temp_file.path(), None, true).unwrap();
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let (_yield_tx, yield_rx) = watch::channel(false);
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel(8);

        let worker = spawn_chunk_worker(
            47,
            ChunkRange {
                id: 11,
                start: 1,
                end: 0,
            },
            0,
            1,
            "http://127.0.0.1:1/download".to_string(),
            HttpClient::new(),
            storage,
            None,
            cancel_rx,
            yield_rx,
            worker_tx,
        );
        join_worker(worker).await;

        let (error, retryable) = only_error(take_messages(&mut worker_rx), 47, 11);
        assert!(matches!(
            error,
            WorkerError::InvalidRange("Invalid chunk range")
        ));
        assert!(!retryable);
    }

    #[tokio::test]
    async fn premature_range_eof_is_retryable() {
        let temp_file = TempFile::new("range-eof");
        let storage = Storage::create_or_open(temp_file.path(), Some(3), true).unwrap();
        let chunks: [&[u8]; 1] = [b"xy"];
        let (url, server) = start_response_server(chunked_response(
            "206 Partial Content",
            "Content-Range: bytes 0-2/3\r\n",
            &chunks,
        ));
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let (_yield_tx, yield_rx) = watch::channel(false);
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel(8);

        let worker = spawn_chunk_worker(
            43,
            ChunkRange {
                id: 8,
                start: 0,
                end: 2,
            },
            0,
            3,
            url,
            HttpClient::new(),
            storage,
            None,
            cancel_rx,
            yield_rx,
            worker_tx,
        );
        join_worker(worker).await;
        server.join().unwrap();

        let mut messages = take_messages(&mut worker_rx);
        assert_eq!(messages.len(), 2);
        assert!(matches!(
            messages.remove(0),
            WorkerMsg::Progress {
                session_id: 43,
                chunk_id: 8,
                bytes_delta: 2,
            }
        ));
        let (error, retryable) = only_error(messages, 43, 8);
        assert_eq!(error.to_string(), "Unexpected EOF: received 2 of 3 bytes");
        assert!(matches!(
            error,
            WorkerError::UnexpectedEof {
                received: 2,
                expected: 3,
            }
        ));
        assert!(retryable);
    }

    #[tokio::test]
    async fn truncated_range_body_error_is_retryable() {
        let temp_file = TempFile::new("truncated-range-body");
        let storage = Storage::create_or_open(temp_file.path(), Some(3), true).unwrap();
        let response = b"HTTP/1.1 206 Partial Content\r\nConnection: close\r\nContent-Length: 3\r\nContent-Range: bytes 0-2/3\r\n\r\nxy".to_vec();
        let (url, server) = start_response_server(response);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let (_yield_tx, yield_rx) = watch::channel(false);
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel(8);

        let worker = spawn_chunk_worker(
            46,
            ChunkRange {
                id: 10,
                start: 0,
                end: 2,
            },
            0,
            3,
            url,
            HttpClient::new(),
            storage,
            None,
            cancel_rx,
            yield_rx,
            worker_tx,
        );
        join_worker(worker).await;
        server.join().unwrap();

        let messages = take_messages(&mut worker_rx);
        let Some(WorkerMsg::Error {
            error: WorkerError::ResponseBody(_),
            retryable: true,
            ..
        }) = messages.last()
        else {
            panic!("expected retryable body error");
        };
    }

    #[tokio::test]
    async fn body_copy_reports_storage_write_failure() {
        let temp_file = TempFile::new("write-failure");
        let storage = Storage::create_or_open(temp_file.path(), None, true).unwrap();
        let chunks: [&[u8]; 1] = [b"x"];
        let (url, server) = start_response_server(chunked_response("200 OK", "", &chunks));
        let response = HttpClient::new()
            .download_range(&url, 0, None)
            .await
            .unwrap();
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel(8);

        copy_response_body(
            response,
            i64::MAX as u64,
            BodyCopyMode::Stream {
                expected_bytes: Some(1),
            },
            43,
            0,
            &storage,
            &mut cancel_rx,
            None,
            &worker_tx,
        )
        .await;
        server.join().unwrap();

        let (error, retryable) = only_error(take_messages(&mut worker_rx), 43, 0);
        assert!(
            matches!(&error, WorkerError::Storage(_)),
            "unexpected error: {error}"
        );
        assert!(!retryable);
        assert_eq!(std::fs::metadata(temp_file.path()).unwrap().len(), 0);
    }

    #[tokio::test]
    async fn cancellation_during_body_copy_stops_before_next_write() {
        for drop_sender in [false, true] {
            let temp_file = TempFile::new("cancellation");
            let storage = Storage::create_or_open(temp_file.path(), Some(6), true).unwrap();
            let (url, release_tx, server) = start_delayed_response_server();
            let (cancel_tx, cancel_rx) = watch::channel(false);
            let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel(1);

            let worker = spawn_stream_worker(
                44,
                url,
                Some(6),
                HttpClient::new(),
                storage,
                cancel_rx,
                worker_tx,
            );
            let message = tokio::time::timeout(Duration::from_secs(2), worker_rx.recv())
                .await
                .expect("worker did not report the first body chunk")
                .expect("worker channel closed");
            match message {
                WorkerMsg::Progress {
                    session_id,
                    chunk_id,
                    bytes_delta,
                } => {
                    assert_eq!(session_id, 44);
                    assert_eq!(chunk_id, 0);
                    assert_eq!(bytes_delta, 3);
                }
                WorkerMsg::Done { .. } | WorkerMsg::Yielded { .. } | WorkerMsg::Error { .. } => {
                    panic!("worker did not wait for the delayed body chunk")
                }
            }

            if drop_sender {
                drop(cancel_tx);
            } else {
                cancel_tx.send(true).unwrap();
            }
            join_worker(worker).await;
            release_tx.send(()).unwrap();
            server.join().unwrap();

            assert!(worker_rx.try_recv().is_err());
            let contents = std::fs::read(temp_file.path()).unwrap();
            assert_eq!(&contents[..3], b"one");
            assert_eq!(&contents[3..], &[0; 3]);
        }
    }

    #[tokio::test]
    async fn yielded_range_reports_all_committed_progress_before_acknowledging() {
        let temp_file = TempFile::new("yield-ordering");
        let storage = Storage::create_or_open(temp_file.path(), Some(9), true).unwrap();
        let (url, release_second_tx, release_third_tx, server) = start_yield_ordering_server();
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let (yield_tx, yield_rx) = watch::channel(false);
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel(1);

        let worker = spawn_chunk_worker(
            45,
            ChunkRange {
                id: 9,
                start: 0,
                end: 8,
            },
            0,
            9,
            url,
            HttpClient::new(),
            storage,
            None,
            cancel_rx,
            yield_rx,
            worker_tx,
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            while worker_rx.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker did not queue the first progress update");
        release_second_tx.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if std::fs::read(temp_file.path())
                    .is_ok_and(|contents| contents.starts_with(b"onetwo"))
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker did not write the second chunk");
        yield_tx.send(true).unwrap();

        let first = tokio::time::timeout(Duration::from_secs(2), worker_rx.recv())
            .await
            .expect("worker did not report the first progress update")
            .expect("worker channel closed");
        let second = tokio::time::timeout(Duration::from_secs(2), worker_rx.recv())
            .await
            .expect("worker did not report the second progress update")
            .expect("worker channel closed");
        let yielded = tokio::time::timeout(Duration::from_secs(2), worker_rx.recv())
            .await
            .expect("worker did not acknowledge yielding")
            .expect("worker channel closed");
        assert!(matches!(
            first,
            WorkerMsg::Progress {
                session_id: 45,
                chunk_id: 9,
                bytes_delta: 3,
            }
        ));
        assert!(matches!(
            second,
            WorkerMsg::Progress {
                session_id: 45,
                chunk_id: 9,
                bytes_delta: 3,
            }
        ));
        assert!(matches!(
            yielded,
            WorkerMsg::Yielded {
                session_id: 45,
                chunk_id: 9,
            }
        ));

        release_third_tx.send(()).unwrap();
        join_worker(worker).await;
        server.join().unwrap();
        assert!(worker_rx.try_recv().is_err());
        assert_eq!(std::fs::read(temp_file.path()).unwrap(), b"onetwo\0\0\0");
    }
}
