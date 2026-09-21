use crate::support::fixtures::{generate_test_payload, wait_for_snapshot};
use crate::support::mock_server::start_mock_server;
use kosmos_downloader::engine::{DownloadAction, DownloadEngine, DownloadStatus, DuplicateChoice};
use std::time::Duration;

#[tokio::test]
async fn test_link_duplicate_prompt_and_none_skip() {
    let save_dir =
        std::env::temp_dir().join(format!("kosmos_link_dup_skip_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&save_dir);
    std::fs::create_dir_all(&save_dir).unwrap();

    let payload = generate_test_payload();
    let server_addr = start_mock_server(payload).await;
    let url = format!("http://{server_addr}/payload.bin");

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: url.clone(),
            save_path: save_dir.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let completed = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Initial download did not complete",
        |snap| snap.status == DownloadStatus::Completed,
    )
    .await;

    // Send the identical URL while completed download is in the list
    action_tx
        .send(DownloadAction::Start {
            url: url.clone(),
            save_path: save_dir.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let prompt_snap = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Link duplicate did not emit prompt",
        |snap| snap.duplicate.is_some(),
    )
    .await;

    let prompt = prompt_snap.duplicate.unwrap();
    assert_eq!(prompt.url, url);
    assert!(prompt.link_duplicate);

    // Dismiss with None
    action_tx
        .send(DownloadAction::ResolveDuplicate {
            session_id: prompt.session_id,
            choice: None,
        })
        .await
        .unwrap();

    let cleared = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Prompt was not cleared after None resolution",
        |snap| snap.duplicate.is_none(),
    )
    .await;

    // Original completed status is preserved
    assert_eq!(cleared.session_id, completed.session_id);
    assert_eq!(cleared.status, DownloadStatus::Completed);

    let _ = std::fs::remove_dir_all(&save_dir);
}

#[tokio::test]
async fn test_link_duplicate_numbered_creates_new_copy() {
    let save_dir = std::env::temp_dir().join(format!("kosmos_link_dup_num_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&save_dir);
    std::fs::create_dir_all(&save_dir).unwrap();

    let payload = generate_test_payload();
    let server_addr = start_mock_server(payload.clone()).await;
    let url = format!("http://{server_addr}/payload.bin");

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: url.clone(),
            save_path: save_dir.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let completed_1 = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "First download did not complete",
        |snap| snap.status == DownloadStatus::Completed,
    )
    .await;

    action_tx
        .send(DownloadAction::Start {
            url: url.clone(),
            save_path: save_dir.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let prompt_snap = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Link duplicate prompt did not arrive",
        |snap| snap.duplicate.is_some(),
    )
    .await;

    action_tx
        .send(DownloadAction::ResolveDuplicate {
            session_id: prompt_snap.duplicate.unwrap().session_id,
            choice: Some(DuplicateChoice::Numbered),
        })
        .await
        .unwrap();

    let completed_2 = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Numbered duplicate did not complete",
        |snap| snap.session_id > completed_1.session_id && snap.status == DownloadStatus::Completed,
    )
    .await;

    assert_eq!(completed_2.filename, "payload_1.bin");
    assert_eq!(
        std::fs::read(save_dir.join("payload.bin")).unwrap(),
        payload
    );
    assert_eq!(
        std::fs::read(save_dir.join("payload_1.bin")).unwrap(),
        payload
    );

    let _ = std::fs::remove_dir_all(&save_dir);
}

#[tokio::test]
async fn test_target_exists_use_existing_complete_and_corrupt() {
    let save_dir =
        std::env::temp_dir().join(format!("kosmos_target_use_exist_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&save_dir);
    std::fs::create_dir_all(&save_dir).unwrap();

    let payload = generate_test_payload();
    let server_addr = start_mock_server(payload.clone()).await;
    let url = format!("http://{server_addr}/payload.bin");
    let target_path = save_dir.join("payload.bin");

    // Pre-seed exact matching payload
    std::fs::write(&target_path, &payload).unwrap();

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: url.clone(),
            save_path: save_dir.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let prompt_snap = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Existing target did not emit prompt",
        |snap| snap.duplicate.is_some(),
    )
    .await;

    let prompt = prompt_snap.duplicate.unwrap();
    assert!(!prompt.link_duplicate);
    assert_eq!(prompt.existing_bytes, Some(payload.len() as u64));

    // Resolve with UseExisting -> verifies sample bytes and marks Completed
    action_tx
        .send(DownloadAction::ResolveDuplicate {
            session_id: prompt.session_id,
            choice: Some(DuplicateChoice::UseExisting),
        })
        .await
        .unwrap();

    let completed = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Existing file was not adopted as completed",
        |snap| snap.status == DownloadStatus::Completed,
    )
    .await;

    assert_eq!(completed.downloaded_bytes, payload.len() as u64);

    // Now corrupt the target file bytes and try UseExisting again
    let mut corrupted = payload.clone();
    corrupted[0] ^= 0xff;
    std::fs::write(&target_path, &corrupted).unwrap();

    let corrupted_target = save_dir.join("corrupted.bin");
    std::fs::write(&corrupted_target, &corrupted).unwrap();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{server_addr}/corrupted.bin"),
            save_path: corrupted_target.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let prompt_snap_2 = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Corrupted target did not emit prompt",
        |snap| snap.duplicate.is_some(),
    )
    .await;

    action_tx
        .send(DownloadAction::ResolveDuplicate {
            session_id: prompt_snap_2.duplicate.unwrap().session_id,
            choice: Some(DuplicateChoice::UseExisting),
        })
        .await
        .unwrap();

    let failed = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Corrupted target did not fail verification",
        |snap| matches!(snap.status, DownloadStatus::Failed(_)),
    )
    .await;

    match failed.status {
        DownloadStatus::Failed(msg) => {
            assert!(msg.contains("Cannot use existing file"));
        }
        _ => panic!("Expected failure"),
    }

    let _ = std::fs::remove_dir_all(&save_dir);
}

#[tokio::test]
async fn test_adversarial_extensionless_target_collision_fails_to_prompt() {
    let save_dir = std::env::temp_dir().join(format!("kosmos_adv_extless_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&save_dir);
    std::fs::create_dir_all(&save_dir).unwrap();

    let target_file = save_dir.join("ffmpeg");
    std::fs::write(&target_file, b"existing binary content").unwrap();

    let payload = generate_test_payload();
    let server_addr = start_mock_server(payload).await;
    let url = format!("http://{server_addr}/ffmpeg");

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: url.clone(),
            save_path: target_file.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    // Empirically observe whether it emits DuplicatePrompt or crashes with Storage error
    let snap = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Download did not update snapshot",
        |snap| snap.duplicate.is_some() || matches!(snap.status, DownloadStatus::Failed(_)),
    )
    .await;

    // EMPIRICAL ASSERTION: The bug causes is_directory_target(".../ffmpeg") to return true,
    // which tries to create a directory named "ffmpeg". Because "ffmpeg" is already a file,
    // create_dir_all fails and it transitions to Failed rather than emitting a DuplicatePrompt.
    let bug_manifested =
        snap.duplicate.is_none() && matches!(snap.status, DownloadStatus::Failed(_));
    println!(
        "test_adversarial_extensionless_target_collision: bug_manifested = {bug_manifested}, status = {:?}",
        snap.status
    );
    assert!(
        bug_manifested,
        "Expected bug to manifest: extensionless file collision fails with Storage error instead of DuplicatePrompt"
    );

    let _ = std::fs::remove_dir_all(&save_dir);
}

#[tokio::test]
async fn test_stale_session_id_keeps_pending_prompt() {
    let save_dir =
        std::env::temp_dir().join(format!("kosmos_adv_stale_sid_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&save_dir);
    std::fs::create_dir_all(&save_dir).unwrap();

    let target_file = save_dir.join("payload.bin");
    std::fs::write(&target_file, b"existing content").unwrap();

    let payload = generate_test_payload();
    let server_addr = start_mock_server(payload).await;
    let url = format!("http://{server_addr}/payload.bin");

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: url.clone(),
            save_path: save_dir.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let prompt_snap = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Existing target did not emit prompt",
        |snap| snap.duplicate.is_some(),
    )
    .await;

    let valid_sid = prompt_snap.duplicate.unwrap().session_id;

    // A stale answer must not clear the live prompt.
    action_tx
        .send(DownloadAction::ResolveDuplicate {
            session_id: valid_sid + 100,
            choice: Some(DuplicateChoice::Numbered),
        })
        .await
        .unwrap();

    let still_pending = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Stale answer cleared the pending prompt",
        |snap| {
            snap.duplicate
                .as_ref()
                .is_some_and(|prompt| prompt.session_id == valid_sid)
        },
    )
    .await;
    assert_eq!(
        still_pending.duplicate.as_ref().unwrap().session_id,
        valid_sid
    );

    // The real answer still resolves the prompt afterwards.
    action_tx
        .send(DownloadAction::ResolveDuplicate {
            session_id: valid_sid,
            choice: Some(DuplicateChoice::Numbered),
        })
        .await
        .unwrap();

    let completed = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Valid resolution after a stale one did not complete",
        |snap| snap.status == DownloadStatus::Completed,
    )
    .await;
    assert_eq!(completed.session_id, valid_sid);
    assert!(
        save_dir.join("payload_1.bin").exists(),
        "Numbered copy was not created after the stale answer"
    );

    let _ = std::fs::remove_dir_all(&save_dir);
}

#[tokio::test]
async fn test_remembered_preference_skips_prompt() {
    let save_dir =
        std::env::temp_dir().join(format!("kosmos_remembered_pref_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&save_dir);
    std::fs::create_dir_all(&save_dir).unwrap();

    let payload = generate_test_payload();
    let server_addr = start_mock_server(payload.clone()).await;
    let url = format!("http://{server_addr}/payload.bin");
    let target_path = save_dir.join("payload.bin");
    std::fs::write(&target_path, b"stale bytes that must be replaced").unwrap();

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    // The user already remembered Overwrite, so the engine must not ask again.
    action_tx
        .send(DownloadAction::SetDuplicatePreference {
            choice: Some(DuplicateChoice::Overwrite),
        })
        .await
        .unwrap();

    action_tx
        .send(DownloadAction::Start {
            url: url.clone(),
            save_path: save_dir.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let completed = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Remembered Overwrite did not complete",
        |snap| snap.status == DownloadStatus::Completed,
    )
    .await;

    assert!(
        completed.duplicate.is_none(),
        "Remembered preference must not raise a duplicate prompt"
    );
    assert_eq!(std::fs::read(&target_path).unwrap(), payload);

    let _ = std::fs::remove_dir_all(&save_dir);
}

#[tokio::test]
async fn test_adversarial_server_200_ok_rejects_complete_file_adoption() {
    let save_dir = std::env::temp_dir().join(format!("kosmos_adv_200_ok_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&save_dir);
    std::fs::create_dir_all(&save_dir).unwrap();

    let payload = generate_test_payload();
    // Start a server that returns 200 OK without range support!
    let server_addr =
        crate::support::mock_server::start_non_range_mock_server(payload.clone()).await;
    let url = format!("http://{server_addr}/payload.bin");
    let target_path = save_dir.join("payload.bin");

    // File on disk matches remote size and content exactly!
    std::fs::write(&target_path, &payload).unwrap();

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: url.clone(),
            save_path: save_dir.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let prompt_snap = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Existing target did not emit prompt",
        |snap| snap.duplicate.is_some(),
    )
    .await;

    // Resolve with UseExisting
    action_tx
        .send(DownloadAction::ResolveDuplicate {
            session_id: prompt_snap.duplicate.unwrap().session_id,
            choice: Some(DuplicateChoice::UseExisting),
        })
        .await
        .unwrap();

    let failed_snap = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Non-range server did not fail adoption",
        |snap| matches!(snap.status, DownloadStatus::Failed(_)),
    )
    .await;

    // EMPIRICAL ASSERTION: Even though the file was 100% complete and identical,
    // verify_range requested byte ranges, got 200 OK instead of 206 Partial Content,
    // and failed with BadStatus(200).
    match failed_snap.status {
        DownloadStatus::Failed(err) => {
            println!(
                "test_adversarial_server_200_ok_rejects_complete_file_adoption failed with: {err}"
            );
            assert!(err.contains("200") || err.contains("Cannot use existing file"));
        }
        _ => panic!("Expected failure"),
    }

    let _ = std::fs::remove_dir_all(&save_dir);
}
