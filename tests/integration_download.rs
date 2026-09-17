use kosmos_downloader::client::{HttpClient, RemoteFileInfo};
use kosmos_downloader::engine::{DownloadAction, DownloadEngine, DownloadSnapshot, DownloadStatus};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TEST_DATA_SIZE: usize = 64 * 1024; // 64 KB

fn generate_test_payload() -> Vec<u8> {
    (0..TEST_DATA_SIZE).map(|i| (i % 251) as u8).collect()
}

async fn start_local_server<H, F>(handler: H) -> SocketAddr
where
    H: Fn(TcpStream, String) -> F + Send + Sync + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handler = Arc::new(handler);

    tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => break,
            };

            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let mut len = 0;
                while !buf[..len].windows(4).any(|window| window == b"\r\n\r\n") {
                    match socket.read(&mut buf[len..]).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => len += n,
                    }
                }
                handler(socket, String::from_utf8_lossy(&buf[..len]).into_owned()).await;
            });
        }
    });

    addr
}

fn is_head_request(request: &str) -> bool {
    request
        .lines()
        .next()
        .is_some_and(|line| line.starts_with("HEAD"))
}

async fn start_mock_server(payload: Vec<u8>) -> SocketAddr {
    start_mock_server_with_head_delay(payload, Duration::ZERO).await
}

async fn start_mock_server_with_head_delay(payload: Vec<u8>, head_delay: Duration) -> SocketAddr {
    start_local_server(move |mut socket, request| {
        let payload = payload.clone();
        async move {
            let is_head = is_head_request(&request);

            let range_header = request
                .lines()
                .find(|line| line.to_ascii_lowercase().starts_with("range:"));

            if is_head {
                tokio::time::sleep(head_delay).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Length: {}\r\n\
                     Accept-Ranges: bytes\r\n\
                     ETag: \"payload-v1\"\r\n\
                     Content-Disposition: attachment; filename=\"payload.bin\"\r\n\
                     Connection: close\r\n\r\n",
                    payload.len()
                );
                let _ = socket.write_all(resp.as_bytes()).await;
            } else if let Some(range_line) = range_header {
                let range_part = range_line.split('=').nth(1).unwrap_or("").trim();
                let mut parts = range_part.split('-');
                let start: usize = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
                let end: usize = parts
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(payload.len() - 1);
                let end = end.min(payload.len() - 1);

                let slice = &payload[start..=end];
                let content_length = slice.len();

                let resp_header = format!(
                    "HTTP/1.1 206 Partial Content\r\n\
                     Content-Range: bytes {start}-{end}/{}\r\n\
                     Content-Length: {content_length}\r\n\
                     ETag: \"payload-v1\"\r\n\
                     Connection: close\r\n\r\n",
                    payload.len()
                );

                let _ = socket.write_all(resp_header.as_bytes()).await;

                // Write in chunks to allow pause simulation
                for chunk in slice.chunks(2048) {
                    if socket.write_all(chunk).await.is_err() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            } else {
                let resp = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Length: {}\r\n\
                     Accept-Ranges: bytes\r\n\
                     ETag: \"payload-v1\"\r\n\
                     Connection: close\r\n\r\n",
                    payload.len()
                );
                let _ = socket.write_all(resp.as_bytes()).await;
                let _ = socket.write_all(&payload).await;
            }
        }
    })
    .await
}

async fn start_non_range_mock_server(payload: Vec<u8>) -> SocketAddr {
    start_local_server(move |mut socket, request| {
        let payload = payload.clone();
        async move {
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\r\n",
                payload.len()
            );
            if socket.write_all(response.as_bytes()).await.is_err() || is_head_request(&request) {
                return;
            }

            for chunk in payload.chunks(2048) {
                if socket.write_all(chunk).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    })
    .await
}

async fn start_changing_resource_server(initial: Vec<u8>, updated: Vec<u8>) -> SocketAddr {
    let head_count = Arc::new(AtomicUsize::new(0));

    start_local_server(move |mut socket, request| {
        let initial = initial.clone();
        let updated = updated.clone();
        let head_count = Arc::clone(&head_count);
        async move {
            let is_head = is_head_request(&request);
            let is_initial = if is_head {
                head_count.fetch_add(1, Ordering::SeqCst) == 0
            } else {
                head_count.load(Ordering::SeqCst) <= 1
            };
            let (payload, etag) = if is_initial {
                (initial, "\"v1\"")
            } else {
                (updated, "\"v2\"")
            };

            if is_head {
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Length: {}\r\n\
                     Accept-Ranges: bytes\r\n\
                     ETag: {etag}\r\n\
                     Connection: close\r\n\r\n",
                    payload.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                return;
            }

            let range = request
                .lines()
                .find(|line| line.to_ascii_lowercase().starts_with("range:"));
            let Some(range) = range else {
                return;
            };
            let range = range.split('=').nth(1).unwrap_or("").trim();
            let mut parts = range.split('-');
            let start: usize = parts
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            let end = parts
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(payload.len() - 1)
                .min(payload.len() - 1);
            let slice = &payload[start..=end];
            let response = format!(
                "HTTP/1.1 206 Partial Content\r\n\
                 Content-Range: bytes {start}-{end}/{}\r\n\
                 Content-Length: {}\r\n\
                 ETag: {etag}\r\n\
                 Connection: close\r\n\r\n",
                payload.len(),
                slice.len()
            );
            if socket.write_all(response.as_bytes()).await.is_err() {
                return;
            }
            for chunk in slice.chunks(2048) {
                if socket.write_all(chunk).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    })
    .await
}

async fn wait_for_snapshot(
    snapshot_rx: &mut tokio::sync::watch::Receiver<DownloadSnapshot>,
    deadline: Duration,
    timeout_message: &str,
    condition: impl FnMut(&DownloadSnapshot) -> bool,
) -> DownloadSnapshot {
    let result = tokio::time::timeout(deadline, snapshot_rx.wait_for(condition))
        .await
        .map(|result| result.map(|snapshot| snapshot.clone()));

    match result {
        Ok(Ok(snapshot)) => snapshot,
        Ok(Err(_)) => panic!("Snapshot channel closed while waiting for {timeout_message}"),
        Err(_) => {
            let snapshot = snapshot_rx.borrow_and_update().clone();
            panic!("{timeout_message}; last snapshot: {snapshot:?}");
        }
    }
}

async fn assert_existing_target_is_preserved(save_path: PathBuf, target_path: PathBuf) {
    let original = b"existing download data".to_vec();
    std::fs::write(&target_path, &original).unwrap();

    let server_addr = start_mock_server(generate_test_payload()).await;
    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{server_addr}/payload.bin"),
            save_path,
            num_chunks: 2,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Existing target did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;

    match &terminal.status {
        DownloadStatus::Failed(message) => assert!(
            message.contains("Refusing to overwrite existing file"),
            "Expected no-clobber error, got: {message}"
        ),
        DownloadStatus::Completed => {
            panic!("Engine must not report completion for an existing target")
        }
        status => panic!("Expected a terminal download state, got {status:?}"),
    }
    assert_eq!(std::fs::read(&target_path).unwrap(), original);
    assert_eq!(terminal.save_path, target_path);
}

#[tokio::test]
async fn metadata_uses_head_fields_or_range_probe_total() {
    let client = HttpClient::new();
    for (head_status, head_ranges, probe_status, length, accepts_ranges, source) in [
        ("200 OK", true, "206 Partial Content", 64, true, "head"),
        (
            "206 Partial Content",
            true,
            "206 Partial Content",
            64,
            true,
            "head",
        ),
        (
            "405 Method Not Allowed",
            false,
            "206 Partial Content",
            128,
            true,
            "probe",
        ),
        ("405 Method Not Allowed", false, "200 OK", 1, false, "probe"),
        ("200 OK", false, "206 Partial Content", 64, true, "head"),
    ] {
        let addr = start_local_server(move |mut socket, request| async move {
            let is_head = is_head_request(&request);
            let (status, length, source, date) = if is_head {
                (head_status, 64, "head", "Wed, 16 Sep 2026 12:00:00 GMT")
            } else {
                assert!(
                    request
                        .lines()
                        .any(|line| line.eq_ignore_ascii_case("range: bytes=0-0"))
                );
                (probe_status, 1, "probe", "Thu, 17 Sep 2026 12:00:00 GMT")
            };
            let ranges = if !is_head || head_ranges {
                "Accept-Ranges: bytes\r\n"
            } else {
                ""
            };
            let body = if is_head { "" } else { "x" };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {length}\r\n\
                 Content-Range: bytes 0-0/128\r\n{ranges}\
                 Content-Disposition: attachment; filename={source}.bin\r\n\
                 ETag: \"{source}\"\r\nLast-Modified: {date}\r\n\
                 Connection: close\r\n\r\n{body}"
            );
            let _ = socket.write_all(response.as_bytes()).await;
        })
        .await;
        let url = format!("http://{addr}/fallback.bin");
        let info = tokio::time::timeout(Duration::from_secs(3), client.fetch_info(&url))
            .await
            .expect("Metadata lookup timed out")
            .expect("Metadata lookup failed");
        assert_eq!(
            info,
            RemoteFileInfo {
                content_length: Some(length),
                accepts_ranges,
                filename: format!("{source}.bin"),
                etag: Some(format!("\"{source}\"")),
                last_modified: Some(
                    if source == "head" {
                        "Wed, 16 Sep 2026 12:00:00 GMT"
                    } else {
                        "Thu, 17 Sep 2026 12:00:00 GMT"
                    }
                    .into()
                ),
            },
            "HEAD {head_status}, range support {head_ranges}, probe {probe_status}"
        );
    }
}

#[tokio::test]
async fn test_full_multiconnection_download() {
    let payload = generate_test_payload();
    let (request_tx, mut request_rx) = tokio::sync::mpsc::channel(4);
    let server_addr = start_local_server(move |mut socket, request| {
        let request_tx = request_tx.clone();
        async move {
            if is_head_request(&request) {
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {TEST_DATA_SIZE}\r\n\
                     Accept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            } else {
                request_tx.send((socket, request)).await.unwrap();
            }
        }
    })
    .await;

    let temp_dir = std::env::temp_dir();
    let save_path = temp_dir.join(format!("kosmos_full_test_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    let download_url = format!("http://{server_addr}/payload.bin");

    action_tx
        .send(DownloadAction::Start {
            url: download_url,
            save_path: save_path.clone(),
            num_chunks: 4,
        })
        .await
        .unwrap();

    // Hold every response until all four requests arrive: serial downloads must fail.
    let requests = tokio::time::timeout(Duration::from_secs(5), async {
        let mut requests = Vec::new();
        for _ in 0..4 {
            requests.push(request_rx.recv().await.unwrap());
        }
        requests
    })
    .await
    .expect("Chunk requests did not arrive concurrently");

    let mut peers = std::collections::HashSet::new();
    let mut starts = std::collections::HashSet::new();
    for (mut socket, request) in requests {
        assert!(peers.insert(socket.peer_addr().unwrap()));
        assert_eq!(request.lines().next(), Some("GET /payload.bin HTTP/1.1"));
        let range = request
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("range").then_some(value.trim())
            })
            .unwrap();
        let (start, end) = range
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        let start: usize = start.parse().unwrap();
        let end: usize = end.parse().unwrap();
        assert!(starts.insert(start));
        assert!(start < TEST_DATA_SIZE && start.is_multiple_of(TEST_DATA_SIZE / 4));
        assert_eq!(end, start + TEST_DATA_SIZE / 4 - 1);

        let response = format!(
            "HTTP/1.1 206 Partial Content\r\n\
             Content-Range: bytes {start}-{end}/{TEST_DATA_SIZE}\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n",
            end - start + 1
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.write_all(&payload[start..=end]).await.unwrap();
    }

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Download did not complete in time",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "Download failed"
    );

    let downloaded_bytes = std::fs::read(&save_path).expect("Failed to read downloaded file");

    assert_eq!(downloaded_bytes.len(), payload.len());
    assert_eq!(downloaded_bytes, payload);

    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_existing_explicit_target_is_not_overwritten() {
    let save_path = std::env::temp_dir().join(format!(
        "kosmos_no_clobber_explicit_{}.bin",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&save_path);

    assert_existing_target_is_preserved(save_path.clone(), save_path.clone()).await;

    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_existing_directory_target_is_not_overwritten() {
    let save_dir = std::env::temp_dir().join(format!(
        "kosmos_no_clobber_directory_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&save_dir);
    std::fs::create_dir_all(&save_dir).unwrap();
    let target_path = save_dir.join("payload.bin");

    assert_existing_target_is_preserved(save_dir.clone(), target_path).await;

    let _ = std::fs::remove_dir_all(&save_dir);
}

#[tokio::test]
async fn test_pause_and_resume_download() {
    let payload = generate_test_payload();
    let server_addr = start_mock_server(payload.clone()).await;

    let temp_dir = std::env::temp_dir();
    let save_path = temp_dir.join(format!("kosmos_resume_test_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    let download_url = format!("http://{server_addr}/payload.bin");

    // 1. Start download
    action_tx
        .send(DownloadAction::Start {
            url: download_url,
            save_path: save_path.clone(),
            num_chunks: 4,
        })
        .await
        .unwrap();

    // 2. Wait until actively downloading
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_millis(500),
        "Download did not begin before pause",
        |snap| snap.status == DownloadStatus::Downloading && snap.downloaded_bytes > 0,
    )
    .await;

    // 3. Pause
    action_tx.send(DownloadAction::Pause).await.unwrap();

    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Download did not pause",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(
        paused.resumable,
        "Range download with an ETag should be resumable"
    );

    // 4. Resume
    action_tx.send(DownloadAction::Resume).await.unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Resumed download did not complete in time",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "Resumed download failed"
    );

    // 5. Verify data integrity after resume
    let downloaded_bytes = std::fs::read(&save_path).expect("Failed to read downloaded file");

    assert_eq!(downloaded_bytes.len(), payload.len());
    assert_eq!(downloaded_bytes, payload);

    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_pause_during_connecting_restarts_metadata() {
    let payload = generate_test_payload();
    let server_addr =
        start_mock_server_with_head_delay(payload.clone(), Duration::from_millis(150)).await;
    let save_path = std::env::temp_dir().join(format!(
        "kosmos_connecting_resume_test_{}.bin",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{server_addr}/payload.bin"),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let connecting = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Download did not enter Connecting",
        |snap| snap.status == DownloadStatus::Connecting,
    )
    .await;
    assert!(
        connecting.filename.is_empty(),
        "Metadata must not arrive before the delayed HEAD response"
    );

    action_tx.send(DownloadAction::Pause).await.unwrap();
    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Download did not pause during metadata lookup",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(
        paused.resumable,
        "Stopped metadata lookup should be retryable"
    );

    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Resumed metadata lookup did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "Resumed metadata lookup failed"
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);

    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_non_range_pause_restarts_from_zero() {
    let payload = generate_test_payload();
    let server_addr = start_non_range_mock_server(payload.clone()).await;
    let save_path = std::env::temp_dir().join(format!(
        "kosmos_non_range_restart_test_{}.bin",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{server_addr}/payload.bin"),
            save_path: save_path.clone(),
            num_chunks: 4,
        })
        .await
        .unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Non-range download did not begin before pause",
        |snap| snap.status == DownloadStatus::Downloading && snap.downloaded_bytes > 0,
    )
    .await;

    action_tx.send(DownloadAction::Pause).await.unwrap();
    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Non-range download did not pause",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(
        !paused.resumable,
        "A non-range download must advertise Restart"
    );

    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Restarted non-range download did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "Restarted non-range download failed"
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);

    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_changed_resource_refuses_resume_without_truncating_partial_file() {
    let initial = vec![b'A'; TEST_DATA_SIZE];
    let updated = vec![b'B'; TEST_DATA_SIZE];
    let server_addr = start_changing_resource_server(initial, updated).await;
    let save_path = std::env::temp_dir().join(format!(
        "kosmos_changed_resource_test_{}.bin",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{server_addr}/payload.bin"),
            save_path: save_path.clone(),
            num_chunks: 1,
        })
        .await
        .unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Changed-resource download did not make initial progress",
        |snap| snap.status == DownloadStatus::Downloading && snap.downloaded_bytes >= 2048,
    )
    .await;

    action_tx.send(DownloadAction::Pause).await.unwrap();
    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Changed-resource download did not pause",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(paused.resumable);
    let paused_file = std::fs::read(&save_path).unwrap();
    let written_bytes = paused_file.iter().take_while(|&&byte| byte == b'A').count() as u64;
    assert_eq!(paused.downloaded_bytes, written_bytes);

    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Changed-resource resume did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    match terminal.status {
        DownloadStatus::Failed(message) => assert!(message.contains("Cannot safely resume")),
        status => panic!("Expected a safe-resume failure, got {status:?}"),
    }

    let saved = std::fs::read(&save_path).unwrap();
    assert_eq!(saved[0], b'A');
    assert!(!saved.contains(&b'B'));

    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_premature_disconnect_fails_download() {
    // Mock server that advertises 32 KB but drops TCP connection after sending only 256 bytes
    let addr = start_local_server(|mut socket, request| async move {
        if is_head_request(&request) {
            let resp = "HTTP/1.1 200 OK\r\n\
                        Content-Length: 32768\r\n\
                        Accept-Ranges: bytes\r\n\
                        Connection: close\r\n\r\n";
            let _ = socket.write_all(resp.as_bytes()).await;
        } else {
            let resp_header = "HTTP/1.1 206 Partial Content\r\n\
                               Content-Range: bytes 0-32767/32768\r\n\
                               Content-Length: 32768\r\n\
                               Connection: close\r\n\r\n";
            let _ = socket.write_all(resp_header.as_bytes()).await;
            // Only send 256 bytes then close abruptly.
            let truncated = [42u8; 256];
            let _ = socket.write_all(&truncated).await;
            // Drop socket without sending remaining 32512 bytes.
        }
    })
    .await;

    let temp_dir = std::env::temp_dir();
    let save_path = temp_dir.join(format!("kosmos_premature_eof_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{addr}/file.bin"),
            save_path: save_path.clone(),
            num_chunks: 1,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "Engine should transition to Failed status on premature disconnect",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    match terminal.status {
        DownloadStatus::Failed(err) => {
            assert!(
                err.contains("Unexpected EOF") || err.contains("error decoding response body"),
                "Expected error on premature disconnect, got: {err}"
            );
        }
        DownloadStatus::Completed => {
            panic!("Engine must not report Completed on truncated stream!")
        }
        status => panic!("Expected a terminal download state, got {status:?}"),
    }
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_server_ignores_range_returns_200() {
    // Mock server returns 200 OK when a range was requested
    let addr = start_local_server(|mut socket, request| async move {
        if is_head_request(&request) {
            let resp = "HTTP/1.1 200 OK\r\n\
                        Content-Length: 1000\r\n\
                        Accept-Ranges: bytes\r\n\
                        Connection: close\r\n\r\n";
            let _ = socket.write_all(resp.as_bytes()).await;
        } else {
            // Return 200 OK instead of 206 Partial Content.
            let resp = "HTTP/1.1 200 OK\r\n\
                        Content-Length: 1000\r\n\
                        Connection: close\r\n\r\n";
            let _ = socket.write_all(resp.as_bytes()).await;
            let payload = [0u8; 1000];
            let _ = socket.write_all(&payload).await;
        }
    })
    .await;

    let temp_dir = std::env::temp_dir();
    let save_path = temp_dir.join(format!("kosmos_range_200_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{addr}/test.bin"),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "Engine should detect range violation and fail",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    match terminal.status {
        DownloadStatus::Failed(err) => {
            assert!(
                err.contains("206 Partial Content"),
                "Expected 206 requirement error, got: {err}"
            );
        }
        DownloadStatus::Completed => {
            panic!("Engine must not complete when server returns 200 OK for sub-range requests!");
        }
        status => panic!("Expected a terminal download state, got {status:?}"),
    }
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_cancel_during_connecting() {
    let addr =
        start_mock_server_with_head_delay(generate_test_payload(), Duration::from_secs(2)).await;

    let temp_dir = std::env::temp_dir();
    let save_path = temp_dir.join(format!("kosmos_cancel_test_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{addr}/slow.bin"),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    // Verify it transitioned to Connecting
    let connecting = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_millis(200),
        "Should enter Connecting status",
        |snap| snap.status == DownloadStatus::Connecting,
    )
    .await;
    assert!(
        connecting.filename.is_empty(),
        "Cancel must happen before delayed metadata arrives"
    );

    // Cancel immediately while still connecting
    action_tx.send(DownloadAction::Cancel).await.unwrap();

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_millis(200),
        "Should transition to Idle immediately on Cancel",
        |snap| snap.status == DownloadStatus::Idle,
    )
    .await;
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_probe_bad_http_status() {
    // Mock server returning 404
    let addr = start_local_server(|mut socket, _request| async move {
        let resp = "HTTP/1.1 404 Not Found\r\n\
                    Content-Length: 0\r\n\
                    Connection: close\r\n\r\n";
        let _ = socket.write_all(resp.as_bytes()).await;
    })
    .await;

    let temp_dir = std::env::temp_dir();
    let save_path = temp_dir.join(format!("kosmos_404_test_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{addr}/not_found.bin"),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(1),
        "Engine should report Failed for 404 Not Found",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    match terminal.status {
        DownloadStatus::Failed(err) => {
            assert!(
                err.contains("404"),
                "Expected 404 error message, got: {err}"
            );
        }
        DownloadStatus::Completed => panic!("Engine must not complete for 404 Not Found"),
        status => panic!("Expected a terminal download state, got {status:?}"),
    }
    let _ = std::fs::remove_file(&save_path);
}
