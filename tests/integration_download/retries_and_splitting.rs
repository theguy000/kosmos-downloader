use crate::support::assertions::{
    assert_dynamic_split_requests, assert_retry_requests, observed_requests,
};
use crate::support::fixtures::{
    DYNAMIC_DATA_SIZE, DYNAMIC_ETAG, DYNAMIC_INITIAL_CHUNK_SIZE, DYNAMIC_PREFIX_SIZE,
    RETRY_PREFIX_SIZE, generate_offset_payload, regression_save_path, wait_for_snapshot,
};
use crate::support::retry_server::{RecoveryFault, RetryBehavior, start_retry_server};
use crate::support::split_server::start_dynamic_split_server;
use kosmos_downloader::engine::{DownloadAction, DownloadEngine, DownloadStatus};
use std::sync::atomic::Ordering;
use std::time::Duration;

#[tokio::test]
async fn dynamic_split_replaces_stalled_donor_without_corrupting_confirmed_bytes() {
    let payload = generate_offset_payload(DYNAMIC_DATA_SIZE);
    let server = start_dynamic_split_server(payload.clone(), Some(DYNAMIC_ETAG)).await;
    let save_path = regression_save_path("dynamic_split");
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

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "stalled donor did not confirm its prefix",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes >= DYNAMIC_PREFIX_SIZE as u64
        },
    )
    .await;
    server.release_fast.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), server.child_ready.notified())
        .await
        .expect("stalled donor was not split after its sibling completed");
    tokio::time::timeout(Duration::from_secs(2), server.donor_closed.notified())
        .await
        .expect("donor connection remained active after its yield acknowledgement");
    assert_eq!(
        server.max_waiting_children.load(Ordering::SeqCst),
        2,
        "split must not exceed the requested two concurrent workers"
    );
    server.release_children.send(true).unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "dynamically split download did not reach a terminal state",
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
        "split download failed"
    );

    assert_dynamic_split_requests(&observed_requests(&server.requests));
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn dynamic_partitions_survive_pause_and_resume() {
    let payload = generate_offset_payload(DYNAMIC_DATA_SIZE);
    let server = start_dynamic_split_server(payload.clone(), Some(DYNAMIC_ETAG)).await;
    let save_path = regression_save_path("dynamic_split_resume");
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

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "stalled donor did not confirm its prefix",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes >= DYNAMIC_PREFIX_SIZE as u64
        },
    )
    .await;
    server.release_fast.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), server.child_ready.notified())
        .await
        .expect("stalled donor was not split before pause");
    tokio::time::timeout(Duration::from_secs(2), server.donor_closed.notified())
        .await
        .expect("donor connection remained active after its yield acknowledgement");
    let before_pause = observed_requests(&server.requests);
    let expected_children = assert_dynamic_split_requests(&before_pause);

    action_tx.send(DownloadAction::Pause).await.unwrap();
    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "download did not pause after splitting",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(paused.resumable);

    server.release_children.send(true).unwrap();
    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "resumed dynamically split download did not reach a terminal state",
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
        "resumed split download failed"
    );

    let requests = observed_requests(&server.requests);
    let resumed = &requests[before_pause.len()..];
    assert_eq!(
        resumed.len(),
        2,
        "resume should only restart split partitions"
    );
    assert!(
        resumed
            .iter()
            .all(|request| request.if_range.as_deref() == Some(DYNAMIC_ETAG))
    );
    let mut resumed_ranges: Vec<_> = resumed.iter().map(|request| request.range).collect();
    resumed_ranges.sort_unstable();
    assert_eq!(resumed_ranges, expected_children);
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn dynamic_split_without_strong_etag_does_not_redistribute_live_work() {
    let payload = generate_offset_payload(DYNAMIC_DATA_SIZE);
    let server = start_dynamic_split_server(payload, None).await;
    let save_path = regression_save_path("dynamic_split_no_validator");
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

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "stalled donor did not confirm its prefix",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes >= DYNAMIC_PREFIX_SIZE as u64
        },
    )
    .await;
    server.release_fast.send(true).unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "initial ranged workers did not make progress",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes
                    >= (DYNAMIC_INITIAL_CHUNK_SIZE + DYNAMIC_PREFIX_SIZE) as u64
        },
    )
    .await;
    let unexpected_child =
        tokio::time::timeout(Duration::from_millis(750), server.child_seen.notified()).await;

    action_tx.send(DownloadAction::Cancel).await.unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "stalled no-ETag download did not cancel",
        |snap| snap.status == DownloadStatus::Idle,
    )
    .await;

    assert!(
        unexpected_child.is_err(),
        "a download without a strong ETag must not redistribute live work"
    );
    let requests = observed_requests(&server.requests);
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.if_range.is_none()));
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn ranged_drop_retries_from_confirmed_offset_with_original_validator() {
    let payload = generate_offset_payload(512 * 1024);
    let server = start_retry_server(payload.clone(), RetryBehavior::RecoverOnce).await;
    let save_path = regression_save_path("retry_recovery");
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

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "dropped ranged connection did not recover",
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
        "ranged retry failed"
    );
    assert_retry_requests(
        &observed_requests(&server.requests),
        payload.len(),
        &[0, RETRY_PREFIX_SIZE],
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn retry_exhaustion_keeps_partial_bytes_for_manual_resume() {
    let payload = generate_offset_payload(512 * 1024);
    let server = start_retry_server(payload.clone(), RetryBehavior::PersistentDrop).await;
    let save_path = regression_save_path("retry_exhaustion");
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

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "persistent drops did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert!(
        matches!(terminal.status, DownloadStatus::Failed(_)),
        "persistent drops must fail after the retry budget is exhausted"
    );
    assert!(
        terminal.resumable,
        "A retryable network failure must retain a manually resumable partial download"
    );
    assert_eq!(terminal.downloaded_bytes, (RETRY_PREFIX_SIZE * 4) as u64);
    let partial = std::fs::read(&save_path).unwrap();
    assert_eq!(
        &partial[..RETRY_PREFIX_SIZE * 4],
        &payload[..RETRY_PREFIX_SIZE * 4],
        "Retry exhaustion must retain the confirmed bytes"
    );
    assert_retry_requests(
        &observed_requests(&server.requests),
        payload.len(),
        &[
            0,
            RETRY_PREFIX_SIZE,
            RETRY_PREFIX_SIZE * 2,
            RETRY_PREFIX_SIZE * 3,
        ],
    );
    server.allow_recovery();
    action_tx.send(DownloadAction::Resume).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), snapshot_rx.changed())
        .await
        .expect("Manual resume did not leave the failed state")
        .expect("Snapshot channel closed during manual resume");
    let resumed = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Manual resume after retry exhaustion did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        resumed.status,
        DownloadStatus::Completed,
        "Manual resume after a network failure failed; requests: {:?}",
        observed_requests(&server.requests)
    );
    assert_retry_requests(
        &observed_requests(&server.requests),
        payload.len(),
        &[
            0,
            RETRY_PREFIX_SIZE,
            RETRY_PREFIX_SIZE * 2,
            RETRY_PREFIX_SIZE * 3,
            RETRY_PREFIX_SIZE * 4,
        ],
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn retry_recovery_restarts_changed_resources_but_rejects_invalid_responses() {
    for (fault, label, restarts) in [
        (RecoveryFault::ChangedEtag, "retry_changed_etag", true),
        (
            RecoveryFault::StatusOkChangedEtag,
            "retry_status_200_changed_etag",
            true,
        ),
        (
            RecoveryFault::InvalidContentRange,
            "retry_invalid_range",
            false,
        ),
        (RecoveryFault::StatusOk, "retry_status_200", false),
    ] {
        let payload = generate_offset_payload(512 * 1024);
        let server = start_retry_server(payload.clone(), RetryBehavior::RejectSecond(fault)).await;
        let save_path = regression_save_path(label);
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

        let terminal = wait_for_snapshot(
            &mut snapshot_rx,
            Duration::from_secs(5),
            "invalid recovery response did not reach a terminal state",
            |snap| {
                matches!(
                    snap.status,
                    DownloadStatus::Completed | DownloadStatus::Failed(_)
                )
            },
        )
        .await;
        if restarts {
            assert_eq!(
                terminal.status,
                DownloadStatus::Completed,
                "{label} must restart the changed resource"
            );
            assert_retry_requests(
                &observed_requests(&server.requests),
                payload.len(),
                &[0, RETRY_PREFIX_SIZE, 0],
            );
            assert_eq!(std::fs::read(&save_path).unwrap(), payload);
        } else {
            assert!(
                matches!(terminal.status, DownloadStatus::Failed(_)),
                "{label} with the same ETag must remain fatal"
            );
            assert_retry_requests(
                &observed_requests(&server.requests),
                payload.len(),
                &[0, RETRY_PREFIX_SIZE],
            );
            let saved = std::fs::read(&save_path).unwrap();
            assert_eq!(&saved[..RETRY_PREFIX_SIZE], &payload[..RETRY_PREFIX_SIZE]);
        }
        let _ = std::fs::remove_file(&save_path);
    }
}

#[tokio::test]
async fn repeatedly_changed_resource_stops_after_two_restarts_and_clears_partial_file() {
    let payload = generate_offset_payload(512 * 1024);
    let server = start_retry_server(payload.clone(), RetryBehavior::AlwaysChangedEtag).await;
    let save_path = regression_save_path("retry_changed_restart_budget");
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

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Repeated content changes did not exhaust the restart budget",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    match terminal.status {
        DownloadStatus::Failed(message) => assert!(
            message.contains("Remote file keeps changing"),
            "Expected restart-budget failure, got: {message}"
        ),
        status => panic!("Repeatedly changing resource must not complete, got {status:?}"),
    }
    assert!(!terminal.resumable);
    assert_retry_requests(
        &observed_requests(&server.requests),
        payload.len(),
        &[0, 0, 0],
    );
    assert_eq!(
        std::fs::metadata(&save_path).unwrap().len(),
        0,
        "Restart exhaustion must clear invalid partial bytes"
    );
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn stalled_single_worker_restarts_after_idle_timeout() {
    let payload = generate_offset_payload(512 * 1024);
    let server = start_retry_server(payload.clone(), RetryBehavior::StallThenRecover).await;
    let save_path = regression_save_path("idle_restart");
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

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(8),
        "single stalled worker did not restart after the idle timeout",
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
        "idle restart failed"
    );
    assert_retry_requests(
        &observed_requests(&server.requests),
        payload.len(),
        &[0, RETRY_PREFIX_SIZE],
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&save_path);
}
