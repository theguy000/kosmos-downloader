use crate::support::fixtures::{TEST_DATA_SIZE, generate_test_payload, wait_for_snapshot};
use crate::support::http::{
    is_head_request, requested_byte_range, start_local_server, write_range_response,
};
use crate::support::mock_server::{
    start_mock_server, start_mock_server_with_head_delay, start_non_range_mock_server,
};
use kosmos_downloader::engine::{
    DownloadAction, DownloadEngine, DownloadSnapshot, DownloadStatus, DuplicateChoice,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;

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

    let prompt_snap = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Existing target did not emit a duplicate prompt",
        |snap| snap.duplicate.is_some(),
    )
    .await;

    let prompt = prompt_snap.duplicate.unwrap();
    assert!(!prompt.link_duplicate);
    assert_eq!(prompt.existing_bytes, Some(original.len() as u64));

    // Rejecting the prompt skips the duplicate and returns engine to Idle
    action_tx
        .send(DownloadAction::ResolveDuplicate {
            session_id: prompt.session_id,
            choice: None,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Engine did not return to Idle after rejecting duplicate",
        |snap| snap.status == DownloadStatus::Idle && snap.duplicate.is_none(),
    )
    .await;

    assert_eq!(std::fs::read(&target_path).unwrap(), original);
    assert_eq!(terminal.status, DownloadStatus::Idle);
}

static NEXT_REMOVAL_TEST: AtomicUsize = AtomicUsize::new(0);

fn removal_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "kosmos_remove_{name}_{}_{}.bin",
        std::process::id(),
        NEXT_REMOVAL_TEST.fetch_add(1, Ordering::Relaxed)
    ))
}

fn removal_action(
    snapshot: &DownloadSnapshot,
    delete_file: bool,
    completed_only: bool,
) -> DownloadAction {
    DownloadAction::Remove {
        expected_session_id: snapshot.session_id,
        expected_status: snapshot.status.clone(),
        delete_file,
        completed_only,
    }
}

async fn start_removal_download(
    name: &str,
    payload: Vec<u8>,
) -> (
    tokio::sync::mpsc::Sender<DownloadAction>,
    tokio::sync::watch::Receiver<DownloadSnapshot>,
    PathBuf,
) {
    let server_addr = start_mock_server(payload).await;
    let save_path = removal_path(name);
    let _ = std::fs::remove_file(&save_path);
    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{server_addr}/payload.bin"),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();
    (action_tx, snapshot_rx, save_path)
}

#[tokio::test]
async fn test_full_multiconnection_download() {
    let payload = generate_test_payload();
    let (request_tx, mut request_rx) = tokio::sync::mpsc::channel(4);
    let initial_range_requests = Arc::new(AtomicUsize::new(0));
    let server_payload = payload.clone();
    let server_initial_range_requests = Arc::clone(&initial_range_requests);
    let server_addr = start_local_server(move |mut socket, request| {
        let request_tx = request_tx.clone();
        let payload = server_payload.clone();
        let initial_range_requests = Arc::clone(&server_initial_range_requests);
        async move {
            if is_head_request(&request) {
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {TEST_DATA_SIZE}\r\n\
                     Accept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            } else {
                let request_number = initial_range_requests.fetch_add(1, Ordering::SeqCst);
                if request_number < 4 {
                    request_tx.send((socket, request)).await.unwrap();
                } else if let Some(range) = requested_byte_range(&request)
                    && range.end < payload.len()
                {
                    let _ = write_range_response(&mut socket, &payload, range, None, None).await;
                }
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
    let original = b"existing download data".to_vec();
    std::fs::write(&target_path, &original).unwrap();

    let payload = generate_test_payload();
    let server_addr = start_mock_server(payload.clone()).await;

    // First download: payload.bin exists, should prompt; choosing Numbered saves payload_1.bin
    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{server_addr}/payload.bin"),
            save_path: save_dir.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let prompt_1 = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "First download did not prompt for existing target",
        |snap| snap.duplicate.is_some(),
    )
    .await;

    action_tx
        .send(DownloadAction::ResolveDuplicate {
            session_id: prompt_1.duplicate.unwrap().session_id,
            choice: Some(DuplicateChoice::Numbered),
        })
        .await
        .unwrap();

    let completed_1 = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "First numbered download did not complete",
        |snap| snap.status == DownloadStatus::Completed,
    )
    .await;

    let target_1 = save_dir.join("payload_1.bin");
    assert_eq!(completed_1.filename, "payload_1.bin");
    assert_eq!(completed_1.save_path, target_1);
    assert_eq!(std::fs::read(&target_path).unwrap(), original);
    assert_eq!(std::fs::read(&target_1).unwrap(), payload);

    // Second download: payload.bin and payload_1.bin exist, prompts; choosing Numbered saves payload_2.bin
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{server_addr}/payload.bin"),
            save_path: save_dir.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let prompt_2 = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Second download did not prompt for existing target",
        |snap| snap.duplicate.is_some(),
    )
    .await;

    action_tx
        .send(DownloadAction::ResolveDuplicate {
            session_id: prompt_2.duplicate.unwrap().session_id,
            choice: Some(DuplicateChoice::Numbered),
        })
        .await
        .unwrap();

    let completed_2 = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Second numbered download did not complete",
        |snap| snap.session_id > completed_1.session_id && snap.status == DownloadStatus::Completed,
    )
    .await;

    let target_2 = save_dir.join("payload_2.bin");
    assert_eq!(completed_2.filename, "payload_2.bin");
    assert_eq!(completed_2.save_path, target_2);
    assert_eq!(std::fs::read(&target_path).unwrap(), original);
    assert_eq!(std::fs::read(&target_1).unwrap(), payload);
    assert_eq!(std::fs::read(&target_2).unwrap(), payload);

    // Deleting payload_2 with delete_file: true removes payload_2, preserving payload and payload_1
    action_tx
        .send(removal_action(&completed_2, true, false))
        .await
        .unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "Removal did not clear row",
        |snap| snap.status == DownloadStatus::Idle,
    )
    .await;

    assert!(!target_2.exists());
    assert_eq!(std::fs::read(&target_path).unwrap(), original);
    assert_eq!(std::fs::read(&target_1).unwrap(), payload);

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
async fn test_remove_completed_respects_file_choice() {
    for delete_file in [false, true] {
        let (action_tx, mut snapshot_rx, save_path) = start_removal_download(
            if delete_file {
                "completed_delete"
            } else {
                "completed_keep"
            },
            generate_test_payload(),
        )
        .await;
        let completed = wait_for_snapshot(
            &mut snapshot_rx,
            Duration::from_secs(5),
            "Download did not complete before removal",
            |snapshot| snapshot.status == DownloadStatus::Completed,
        )
        .await;
        assert!(save_path.exists());

        action_tx
            .send(removal_action(&completed, delete_file, true))
            .await
            .unwrap();
        let idle = wait_for_snapshot(
            &mut snapshot_rx,
            Duration::from_secs(2),
            "Completed removal did not clear its row",
            |snapshot| snapshot.status == DownloadStatus::Idle,
        )
        .await;

        assert_ne!(idle.session_id, completed.session_id);
        assert_eq!(save_path.exists(), !delete_file);
        let _ = std::fs::remove_file(save_path);
    }
}

#[tokio::test]
async fn test_remove_active_stops_writers_before_file_choice() {
    for delete_file in [false, true] {
        let (action_tx, mut snapshot_rx, save_path) = start_removal_download(
            if delete_file {
                "active_delete"
            } else {
                "active_keep"
            },
            vec![0x5a; 512 * 1024],
        )
        .await;
        let active = wait_for_snapshot(
            &mut snapshot_rx,
            Duration::from_secs(3),
            "Download did not become active before removal",
            |snapshot| snapshot.status == DownloadStatus::Downloading,
        )
        .await;
        assert!(save_path.exists());

        action_tx
            .send(removal_action(&active, delete_file, false))
            .await
            .unwrap();
        wait_for_snapshot(
            &mut snapshot_rx,
            Duration::from_secs(3),
            "Active removal did not clear its row",
            |snapshot| snapshot.status == DownloadStatus::Idle,
        )
        .await;

        assert_eq!(save_path.exists(), !delete_file);
        let _ = std::fs::remove_file(save_path);
    }
}

#[tokio::test]
async fn test_remove_guards_completed_only_and_stale_status() {
    let (action_tx, mut snapshot_rx, save_path) =
        start_removal_download("guard_status", generate_test_payload()).await;
    let active = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Download did not become active for guard test",
        |snapshot| snapshot.status == DownloadStatus::Downloading,
    )
    .await;

    action_tx
        .send(removal_action(&active, true, true))
        .await
        .unwrap();
    let completed = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Completed-only guard canceled an active download",
        |snapshot| snapshot.status == DownloadStatus::Completed,
    )
    .await;

    action_tx
        .send(removal_action(&active, true, false))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let after_stale = snapshot_rx.borrow_and_update().clone();
    assert_eq!(after_stale.status, DownloadStatus::Completed);
    assert_eq!(after_stale.session_id, completed.session_id);
    assert!(save_path.exists());

    action_tx
        .send(removal_action(&completed, false, true))
        .await
        .unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "Guard test cleanup did not clear row",
        |snapshot| snapshot.status == DownloadStatus::Idle,
    )
    .await;
    let _ = std::fs::remove_file(save_path);
}

#[tokio::test]
async fn test_remove_rejects_stale_session_id_after_new_start() {
    let (action_tx, mut snapshot_rx, first_path) =
        start_removal_download("stale_first", vec![0x11; 512 * 1024]).await;
    let stale = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "First download did not become active",
        |snapshot| snapshot.status == DownloadStatus::Downloading,
    )
    .await;

    let second_server = start_mock_server(generate_test_payload()).await;
    let second_path = removal_path("stale_second");
    let _ = std::fs::remove_file(&second_path);
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{second_server}/payload.bin"),
            save_path: second_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();
    let replacement = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Replacement download did not become active",
        |snapshot| {
            snapshot.save_path == second_path && snapshot.status == DownloadStatus::Downloading
        },
    )
    .await;

    action_tx
        .send(removal_action(&stale, true, false))
        .await
        .unwrap();
    let completed = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Stale removal interrupted the replacement download",
        |snapshot| snapshot.status == DownloadStatus::Completed,
    )
    .await;
    assert_eq!(completed.session_id, replacement.session_id);
    assert!(second_path.exists());

    action_tx
        .send(removal_action(&completed, false, false))
        .await
        .unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "Stale-session cleanup did not clear row",
        |snapshot| snapshot.status == DownloadStatus::Idle,
    )
    .await;
    let _ = std::fs::remove_file(first_path);
    let _ = std::fs::remove_file(second_path);
}

#[tokio::test]
async fn test_remove_failure_retains_row_and_retries_not_found() {
    let (action_tx, mut snapshot_rx, save_path) =
        start_removal_download("retry", generate_test_payload()).await;
    let completed = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Download did not complete before deletion failure test",
        |snapshot| snapshot.status == DownloadStatus::Completed,
    )
    .await;
    let moved_path = save_path.with_extension("moved");
    let _ = std::fs::remove_file(&moved_path);
    std::fs::rename(&save_path, &moved_path).unwrap();
    std::fs::create_dir(&save_path).unwrap();

    action_tx
        .send(removal_action(&completed, true, false))
        .await
        .unwrap();
    let failed = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "File deletion failure did not retain a failed row",
        |snapshot| matches!(snapshot.status, DownloadStatus::Failed(_)),
    )
    .await;
    match &failed.status {
        DownloadStatus::Failed(message) => {
            assert!(message.contains("Failed to delete download file"));
            assert!(message.contains(&save_path.display().to_string()));
        }
        status => panic!("Expected failed removal, got {status:?}"),
    }
    assert!(!failed.resumable);
    assert_ne!(failed.session_id, completed.session_id);
    assert!(save_path.is_dir());

    std::fs::remove_dir(&save_path).unwrap();
    action_tx
        .send(removal_action(&failed, true, false))
        .await
        .unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "NotFound retry did not clear failed removal row",
        |snapshot| snapshot.status == DownloadStatus::Idle,
    )
    .await;
    assert!(!save_path.exists());
    assert!(moved_path.exists());
    let _ = std::fs::remove_file(moved_path);
}

#[tokio::test]
async fn test_remove_deletes_target_created_before_preallocation_failure() {
    let addr = start_local_server(|mut socket, request| async move {
        if is_head_request(&request) {
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\
                 Accept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                u64::MAX
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    })
    .await;
    let save_path = removal_path("preallocation_failure");
    let _ = std::fs::remove_file(&save_path);
    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{addr}/payload.bin"),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let failed = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Preallocation failure was not reported",
        |snapshot| matches!(snapshot.status, DownloadStatus::Failed(_)),
    )
    .await;
    assert!(
        matches!(&failed.status, DownloadStatus::Failed(message) if message.contains("Created file could not be initialized"))
    );
    assert!(save_path.exists());

    action_tx
        .send(removal_action(&failed, true, false))
        .await
        .unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "Owned failed-preallocation target was not removed",
        |snapshot| snapshot.status == DownloadStatus::Idle,
    )
    .await;
    assert!(!save_path.exists());
}
