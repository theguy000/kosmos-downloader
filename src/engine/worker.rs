use super::chunks::ChunkRange;
use crate::client::HttpClient;
use crate::storage::Storage;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

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
    Error {
        session_id: u64,
        chunk_id: usize,
        error: String,
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

    fn overrun_error(self, expected_bytes: u64) -> String {
        match self {
            Self::Range { .. } => {
                format!("Range response exceeded its expected {expected_bytes} bytes")
            }
            Self::Stream { .. } => format!("Response exceeded its expected {expected_bytes} bytes"),
        }
    }

    fn offset_error(self) -> &'static str {
        match self {
            Self::Range { .. } => "Chunk offset overflowed",
            Self::Stream { .. } => "Stream offset overflowed",
        }
    }
}

// ponytail: explicit parameters avoid a one-use context struct.
#[allow(clippy::too_many_arguments)]
async fn copy_response_body(
    response: reqwest::Response,
    start: u64,
    mode: BodyCopyMode,
    session_id: u64,
    chunk_id: usize,
    storage: &Storage,
    cancel_rx: &mut watch::Receiver<bool>,
    worker_tx: &mpsc::Sender<WorkerMsg>,
) {
    let expected_bytes = mode.expected_bytes();
    let mut response = response;
    let mut current_offset = start;
    let mut received = 0u64;

    loop {
        tokio::select! {
            _ = cancel_rx.changed() => return,
            item = response.chunk() => {
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
                                    error: mode.offset_error().to_string(),
                                },
                            )
                            .await;
                            return;
                        };
                        if let Err(e) = storage.write_at(current_offset, &bytes) {
                            let _ = send_worker_msg(
                                worker_tx,
                                cancel_rx,
                                WorkerMsg::Error {
                                    session_id,
                                    chunk_id,
                                    error: e.to_string(),
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
                        let _ = send_worker_msg(
                            worker_tx,
                            cancel_rx,
                            WorkerMsg::Error {
                                session_id,
                                chunk_id,
                                error: e.to_string(),
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
                                        error: format!(
                                            "Unexpected EOF: received {} of {} bytes",
                                            received, expected_bytes
                                        ),
                                    },
                                )
                                .await;
                                return;
                            }
                        } else if let Err(e) = storage.set_len(current_offset) {
                            let _ = send_worker_msg(
                                worker_tx,
                                cancel_rx,
                                WorkerMsg::Error {
                                    session_id,
                                    chunk_id,
                                    error: e.to_string(),
                                },
                            )
                            .await;
                            return;
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
    }
}

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
    worker_tx: mpsc::Sender<WorkerMsg>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let chunk_id = range.id;
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
                    error: "Invalid chunk range".to_string(),
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
                    error: "Saved chunk progress exceeds its range".to_string(),
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
                    error: "Chunk offset overflowed".to_string(),
                },
            )
            .await;
            return;
        };
        let expected_bytes = chunk_size - initial_downloaded;

        let response = tokio::select! {
            _ = cancel_rx.changed() => return,
            res = client.download_range_checked(
                &url,
                start,
                Some(range.end),
                Some(total_size),
                if_range.as_deref(),
            ) => {
                match res {
                    Ok(r) => r,
                    Err(e) => {
                        if *cancel_rx.borrow() {
                            return;
                        }
                        let _ = send_worker_msg(
                            &worker_tx,
                            &mut cancel_rx,
                            WorkerMsg::Error {
                                session_id,
                                chunk_id,
                                error: e.to_string(),
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
            start,
            BodyCopyMode::Range { expected_bytes },
            session_id,
            chunk_id,
            &storage,
            &mut cancel_rx,
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
                        let _ = send_worker_msg(
                            &worker_tx,
                            &mut cancel_rx,
                            WorkerMsg::Error {
                                session_id,
                                chunk_id,
                                error: e.to_string(),
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

    fn take_messages(worker_rx: &mut tokio::sync::mpsc::Receiver<WorkerMsg>) -> Vec<WorkerMsg> {
        let mut messages = Vec::new();
        while let Ok(message) = worker_rx.try_recv() {
            messages.push(message);
        }
        messages
    }

    fn only_error(mut messages: Vec<WorkerMsg>, session_id: u64, chunk_id: usize) -> String {
        assert_eq!(messages.len(), 1);
        let Some(WorkerMsg::Error {
            session_id: actual_session_id,
            chunk_id: actual_chunk_id,
            error,
        }) = messages.pop()
        else {
            panic!("expected worker error");
        };
        assert_eq!(actual_session_id, session_id);
        assert_eq!(actual_chunk_id, chunk_id);
        error
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
                WorkerMsg::Error { .. } => panic!("unknown-length stream failed"),
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
            worker_tx,
        );
        join_worker(worker).await;
        server.join().unwrap();

        assert_eq!(
            only_error(take_messages(&mut worker_rx), 42, 7),
            "Range response exceeded its expected 3 bytes"
        );
        assert_eq!(std::fs::read(temp_file.path()).unwrap(), b"old");
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
            &worker_tx,
        )
        .await;
        server.join().unwrap();

        let error = only_error(take_messages(&mut worker_rx), 43, 0);
        assert!(
            error.starts_with("I/O error:") || error == "Zero bytes written during offset write",
            "unexpected error: {error}"
        );
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
                WorkerMsg::Done { .. } | WorkerMsg::Error { .. } => {
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
}
