use crate::support::assertions::observed_requests;
use crate::support::change_server::{
    start_between_range_change_server, start_changing_resource_server,
};
use crate::support::fixtures::{
    TEST_DATA_SIZE, generate_offset_payload, regression_save_path, wait_for_snapshot,
};
use crate::support::http::TestRange;
use crate::support::resume_server::start_no_etag_resume_server;
use kosmos_downloader::engine::{DownloadAction, DownloadEngine, DownloadStatus};
use std::time::Duration;

#[tokio::test]
async fn no_etag_range_download_resumes_when_saved_bytes_match() {
    let payload = generate_offset_payload(TEST_DATA_SIZE);
    let saved_prefix_len = 16 * 1024;
    let full_range = TestRange {
        start: 0,
        end: payload.len() - 1,
    };
    let server =
        start_no_etag_resume_server(payload.clone(), None, full_range, saved_prefix_len).await;
    let save_path = regression_save_path("no_etag_resume");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 1,
        })
        .await
        .unwrap();

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "No-ETag download did not save its initial prefix",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes >= saved_prefix_len as u64
        },
    )
    .await;
    action_tx.send(DownloadAction::Pause).await.unwrap();
    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "No-ETag download did not pause",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(
        paused.resumable,
        "Known-length range downloads must be resumable without an ETag"
    );
    let paused_bytes = usize::try_from(paused.downloaded_bytes).unwrap();
    assert!(paused_bytes >= saved_prefix_len);
    let request_count_before_resume = observed_requests(&server.requests).len();

    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "No-ETag resumed download did not reach a terminal state",
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
        "No-ETag resume failed"
    );

    let requests = observed_requests(&server.requests);
    let resumed_worker = requests[request_count_before_resume..]
        .iter()
        .find(|request| request.range.end == payload.len() - 1 && request.range.len() > 4096)
        .expect("No-ETag resume did not request the remaining range");
    assert_eq!(
        resumed_worker.range.start,
        paused_bytes - 4096,
        "No-ETag resume must verify a bounded overlap in its worker response"
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn no_etag_resume_restarts_after_completed_chunk_changes() {
    let initial = generate_offset_payload(TEST_DATA_SIZE * 3);
    let chunk_size = initial.len() / 3;
    let unfinished_range = TestRange {
        start: 0,
        end: chunk_size - 1,
    };
    let completed_range = TestRange {
        start: chunk_size,
        end: 2 * chunk_size - 1,
    };
    let trailing_completed_range = TestRange {
        start: 2 * chunk_size,
        end: initial.len() - 1,
    };
    let saved_prefix_len = 16 * 1024;
    let mut updated = initial.clone();
    updated[completed_range.start] ^= 0xff;
    updated[completed_range.end] ^= 0xff;
    let server = start_no_etag_resume_server(
        initial.clone(),
        Some("Wed, 16 Sep 2026 12:00:00 GMT"),
        unfinished_range,
        saved_prefix_len,
    )
    .await;
    let save_path = regression_save_path("no_etag_changed_completed_chunk");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 3,
        })
        .await
        .unwrap();

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Completed and unfinished ranges did not save their initial bytes",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes
                    >= (completed_range.len() + trailing_completed_range.len() + saved_prefix_len)
                        as u64
        },
    )
    .await;
    action_tx.send(DownloadAction::Pause).await.unwrap();
    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Changed no-ETag download did not pause",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(paused.resumable);
    let paused_file = std::fs::read(&save_path).unwrap();
    assert_eq!(
        &paused_file[completed_range.start..=completed_range.end],
        &initial[completed_range.start..=completed_range.end],
        "The middle range must be complete before resuming the unfinished range"
    );
    assert_eq!(
        &paused_file[trailing_completed_range.start..=trailing_completed_range.end],
        &initial[trailing_completed_range.start..=trailing_completed_range.end],
    );
    assert_eq!(
        &paused_file[unfinished_range.start..unfinished_range.start + saved_prefix_len],
        &initial[unfinished_range.start..unfinished_range.start + saved_prefix_len],
    );

    server.replace_payload(updated.clone());
    let request_count_before_resume = observed_requests(&server.requests).len();
    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Changed no-ETag resume did not restart and finish",
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
        "Changed no-ETag resume must restart from zero"
    );

    let requests = observed_requests(&server.requests);
    let resume_requests = &requests[request_count_before_resume..];
    assert!(
        resume_requests
            .iter()
            .any(|request| request.range == unfinished_range),
        "Changed saved bytes must restart the first chunk from zero: {resume_requests:?}"
    );
    assert_eq!(
        std::fs::read(&save_path).unwrap(),
        updated,
        "Restarted download must replace every old chunk with the new payload"
    );
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn no_etag_retry_restarts_after_saved_overlap_changes() {
    let initial = generate_offset_payload(TEST_DATA_SIZE);
    let saved_prefix_len = 16 * 1024;
    let full_range = TestRange {
        start: 0,
        end: initial.len() - 1,
    };
    let mut updated = initial.clone();
    updated[saved_prefix_len - 1024] ^= 0xff;
    let server = start_no_etag_resume_server(initial, None, full_range, saved_prefix_len).await;
    let save_path = regression_save_path("no_etag_overlap_change");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 1,
        })
        .await
        .unwrap();

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "No-ETag download did not save its overlap prefix",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes >= saved_prefix_len as u64
        },
    )
    .await;
    server.replace_payload(updated.clone());
    server.release_stalled_response();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Changed no-ETag overlap did not restart and finish",
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
        "Changed overlap must restart instead of mixing source versions"
    );

    let requests = observed_requests(&server.requests);
    assert!(
        requests.iter().any(|request| {
            request.range.start == saved_prefix_len - 4096 && request.range.end == full_range.end
        }),
        "Retry must request a bounded overlap before appending new bytes: {requests:?}"
    );
    assert!(
        requests
            .iter()
            .filter(|request| request.range == full_range)
            .count()
            >= 2,
        "Changed overlap must cause a fresh request from zero: {requests:?}"
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), updated);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn no_validator_parallel_change_restarts_with_consistent_payload() {
    let initial = vec![b'A'; TEST_DATA_SIZE];
    let updated = vec![b'B'; TEST_DATA_SIZE];
    let midpoint = initial.len() / 2;
    let first_range = TestRange {
        start: 0,
        end: midpoint - 1,
    };
    let second_range = TestRange {
        start: midpoint,
        end: initial.len() - 1,
    };
    let server = start_between_range_change_server(initial.clone(), updated.clone()).await;
    let save_path = regression_save_path("no_validator_parallel_change");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    tokio::time::timeout(
        Duration::from_secs(3),
        server.initial_ranges_served.notified(),
    )
    .await
    .expect("Initial parallel ranges were not served from different source versions");
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Changed no-validator parallel download did not restart and finish",
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
        "Changed no-validator source must restart with one consistent payload"
    );

    let served_ranges = server.served_ranges.lock().unwrap().clone();
    assert!(
        served_ranges.contains(&(first_range, false)),
        "The first initial range was not served from the original source"
    );
    assert!(
        served_ranges.contains(&(second_range, true)),
        "The second initial range was not served after the source changed"
    );
    assert_eq!(
        std::fs::read(&save_path).unwrap(),
        updated,
        "Restarted parallel download must not retain the original first range"
    );
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn changed_metadata_restarts_from_zero_with_new_payload() {
    let initial = vec![b'A'; TEST_DATA_SIZE];
    let updated = vec![b'B'; TEST_DATA_SIZE];
    let server = start_changing_resource_server(initial, updated.clone()).await;
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
            url: format!("http://{}/payload.bin", server.addr),
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

    let request_count_before_resume = observed_requests(&server.requests).len();
    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Changed-resource resume did not restart and finish",
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
        "Changed metadata must restart instead of retaining the old partial file"
    );

    let requests = observed_requests(&server.requests);
    assert!(
        requests[request_count_before_resume..]
            .iter()
            .any(|request| {
                request.range
                    == TestRange {
                        start: 0,
                        end: TEST_DATA_SIZE - 1,
                    }
                    && request.if_range.as_deref() == Some("\"v2\"")
            }),
        "Changed metadata must make a fresh strong-validator request from zero: {requests:?}"
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), updated);

    let _ = std::fs::remove_file(&save_path);
}
