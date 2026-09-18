use super::progress::drain_cancelled_progress;
use super::resume::resume_chunks_are_valid;
use super::scheduler::{ActiveChunk, MAX_CHUNK_RETRIES, MIN_SPLIT_BYTES, split_remaining};
use super::{CoordinatorError, calculate_downloaded};
use crate::engine::chunks::{ChunkRange, calculate_chunks};
use crate::engine::worker::{WorkerError, WorkerMsg};
use tokio::sync::{mpsc, watch};

#[test]
fn pause_preserves_queued_failures_and_other_workers_progress() {
    let (tx, mut rx) = mpsc::channel(4);
    let mut chunks: Vec<_> = calculate_chunks(8, 2)
        .into_iter()
        .map(ActiveChunk::new)
        .collect();
    tx.try_send(WorkerMsg::Error {
        session_id: 0,
        chunk_id: 0,
        error: WorkerError::InvalidRange("stale"),
        retryable: false,
    })
    .unwrap();
    tx.try_send(WorkerMsg::Progress {
        session_id: 1,
        chunk_id: 0,
        bytes_delta: 2,
    })
    .unwrap();
    tx.try_send(WorkerMsg::Error {
        session_id: 1,
        chunk_id: 0,
        error: WorkerError::InvalidRange("invalid range"),
        retryable: false,
    })
    .unwrap();
    tx.try_send(WorkerMsg::Progress {
        session_id: 1,
        chunk_id: 1,
        bytes_delta: 1,
    })
    .unwrap();
    assert_eq!(
        drain_cancelled_progress(&mut rx, 1, &mut chunks, true, Some(8), true)
            .unwrap_err()
            .to_string(),
        "Stream #0 error: invalid range",
    );
    assert_eq!(calculate_downloaded(&chunks), 3);
}

#[test]
fn pause_preserves_only_safely_retryable_failures_for_resume() {
    for (resumable, downloaded, retries, can_resume) in [
        (true, 2, 0, true),
        (false, 2, 0, false),
        (true, 2, MAX_CHUNK_RETRIES, false),
        (true, 4, 0, false),
        (true, 5, 0, false),
    ] {
        let (tx, mut rx) = mpsc::channel(3);
        let mut chunks: Vec<_> = calculate_chunks(8, 2)
            .into_iter()
            .map(ActiveChunk::new)
            .collect();
        chunks[0].retries = retries;
        tx.try_send(WorkerMsg::Progress {
            session_id: 1,
            chunk_id: 0,
            bytes_delta: downloaded,
        })
        .unwrap();
        tx.try_send(WorkerMsg::Error {
            session_id: 1,
            chunk_id: 0,
            error: WorkerError::UnexpectedEof {
                received: downloaded,
                expected: 4,
            },
            retryable: true,
        })
        .unwrap();
        tx.try_send(WorkerMsg::Progress {
            session_id: 1,
            chunk_id: 1,
            bytes_delta: 1,
        })
        .unwrap();

        let result = drain_cancelled_progress(&mut rx, 1, &mut chunks, true, Some(8), resumable);
        assert_eq!(result.is_ok(), can_resume);
        assert_eq!(chunks[1].downloaded, 1, "drain all committed progress");
        if can_resume {
            assert_eq!(chunks[0].downloaded, downloaded);
            assert!(resume_chunks_are_valid(&chunks, 8));
        }
        if downloaded > 4 {
            assert!(matches!(result, Err(CoordinatorError::InvalidProgress)));
        }
    }
}

#[test]
fn dynamic_partitions_preserve_progress_and_resume_coverage() {
    for total in [2 * MIN_SPLIT_BYTES + 17, u64::MAX] {
        let mut chunks: Vec<_> = calculate_chunks(total, 1)
            .into_iter()
            .map(ActiveChunk::new)
            .collect();
        chunks[0].downloaded = 17;
        chunks[0].retries = 2;
        let (yield_tx, _yield_rx) = watch::channel(false);
        chunks[0].yield_tx = Some(yield_tx);
        assert!(
            !split_remaining(&mut chunks, 0),
            "cannot split a live writer"
        );
        chunks[0].yield_tx = None;
        assert!(split_remaining(&mut chunks, 0));
        assert_eq!(calculate_downloaded(&chunks), 17);
        assert_eq!(chunks[1].retries, 2);
        assert_eq!(chunks[0].range.end + 1, chunks[1].range.start);
        assert!(resume_chunks_are_valid(&chunks, total));
        chunks[1].range.start -= 1;
        assert!(!resume_chunks_are_valid(&chunks, total), "overlap");
        chunks[1].range.start += 2;
        assert!(!resume_chunks_are_valid(&chunks, total), "gap");
        chunks[1].range.start -= 1;
        chunks[1].range.id = 0;
        assert!(!resume_chunks_are_valid(&chunks, total), "duplicate ID");
        chunks[1].range.id = 1;
        chunks[0].downloaded = chunks[0].range.size() + 1;
        assert!(!resume_chunks_are_valid(&chunks, total), "progress overrun");
        chunks[0].downloaded = 17;
        chunks[0].is_done = true;
        assert!(
            !resume_chunks_are_valid(&chunks, total),
            "premature completion"
        );
    }
    let mut chunks = vec![ActiveChunk::new(ChunkRange {
        id: 0,
        start: 0,
        end: 2 * MIN_SPLIT_BYTES - 2,
    })];
    assert!(!split_remaining(&mut chunks, 0), "avoid tiny tail requests");

    let total = 8 * MIN_SPLIT_BYTES;
    let mut chunks: Vec<_> = calculate_chunks(total, 2)
        .into_iter()
        .map(ActiveChunk::new)
        .collect();
    chunks[0].downloaded = 17;
    assert!(split_remaining(&mut chunks, 0));
    assert!(chunks[2].range.start < chunks[1].range.start);
    assert!(resume_chunks_are_valid(&chunks, total));
}
