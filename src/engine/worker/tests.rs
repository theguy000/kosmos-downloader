use super::body::{BodyCopyMode, copy_response_body};
use super::{WorkerError, WorkerMsg, spawn_chunk_worker, spawn_stream_worker};
use crate::client::HttpClient;
use crate::engine::chunks::ChunkRange;
use crate::storage::Storage;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::thread;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;

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
async fn resumed_range_checks_overlap_before_writing_or_counting_new_bytes() {
    for changed in [false, true] {
        let temp_file = TempFile::new("resume-overlap");
        let storage = Storage::create_or_open(temp_file.path(), Some(11), true).unwrap();
        storage.write_at(0, b"preabcde000").unwrap();
        let middle: &[u8] = if changed { b"cDfX" } else { b"cdeX" };
        let (url, server) = start_response_server(chunked_response(
            "206 Partial Content",
            "Content-Range: bytes 3-10/11\r\n",
            &[b"ab", middle, b"YZ"],
        ));
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let (_yield_tx, yield_rx) = watch::channel(false);
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel(8);
        join_worker(spawn_chunk_worker(
            48,
            ChunkRange {
                id: 0,
                start: 3,
                end: 10,
            },
            5,
            11,
            url,
            HttpClient::new(),
            storage,
            None,
            cancel_rx,
            yield_rx,
            worker_tx,
        ))
        .await;
        server.join().unwrap();
        let messages = take_messages(&mut worker_rx);
        if changed {
            let (error, retryable) = only_error(messages, 48, 0);
            assert!(error.is_content_changed());
            assert!(!retryable);
            assert_eq!(std::fs::read(temp_file.path()).unwrap(), b"preabcde000");
        } else {
            let added: u64 = messages
                .iter()
                .filter_map(|message| match message {
                    WorkerMsg::Progress { bytes_delta, .. } => Some(bytes_delta),
                    _ => None,
                })
                .sum();
            assert_eq!(added, 3, "overlap must not inflate progress");
            assert!(matches!(messages.last(), Some(WorkerMsg::Done { .. })));
            assert_eq!(std::fs::read(temp_file.path()).unwrap(), b"preabcdeXYZ");
        }
    }
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
            if std::fs::read(temp_file.path()).is_ok_and(|contents| contents.starts_with(b"onetwo"))
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
