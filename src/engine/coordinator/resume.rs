use super::scheduler::ActiveChunk;
use super::uses_range_workers;
use crate::client::{ClientError, HttpClient, RemoteFileInfo, is_strong_etag};
use crate::engine::chunks::{ChunkRange, calculate_chunks};
use crate::engine::worker::{OVERLAP_BYTES, WorkerError};
use crate::storage::{Storage, StorageError};

pub(super) struct SavedDownload {
    pub(super) info: RemoteFileInfo,
    storage: Storage,
    chunks: Vec<(ChunkRange, u64)>,
}

impl SavedDownload {
    pub(super) fn new(info: &RemoteFileInfo, storage: &Storage, chunks: &[ActiveChunk]) -> Self {
        Self {
            info: info.clone(),
            storage: storage.clone(),
            chunks: chunks
                .iter()
                .map(|chunk| (chunk.range, chunk.downloaded))
                .collect(),
        }
    }

    pub(super) async fn verify(
        &self,
        client: &HttpClient,
        url: &str,
        latest: &RemoteFileInfo,
    ) -> Result<(), WorkerError> {
        if !uses_range_workers(latest) || !self.info.resume_metadata_matches(latest) {
            return Err(ClientError::ContentChanged.into());
        }
        if latest.etag.as_deref().is_some_and(is_strong_etag) {
            return Ok(());
        }
        let total_size = latest
            .content_length
            .ok_or(WorkerError::InvalidRange("Missing range size"))?;
        // First/last 4 KiB per saved chunk are a consistency heuristic, not a whole-file proof.
        for &(range, downloaded) in &self.chunks {
            if downloaded == 0 {
                continue;
            }
            let end = range
                .start
                .checked_add(downloaded)
                .filter(|end| *end <= total_size && *end - 1 <= range.end)
                .ok_or(WorkerError::InvalidRange("Invalid saved chunk progress"))?;
            let length = downloaded.min(OVERLAP_BYTES);
            let mut starts = vec![range.start];
            if end - length != range.start {
                starts.push(end - length);
            }
            for start in starts {
                let storage = self.storage.clone();
                let expected = tokio::task::spawn_blocking(move || {
                    let mut bytes = vec![0; length as usize];
                    storage.read_at(start, &mut bytes)?;
                    Ok::<_, StorageError>(bytes)
                })
                .await??;
                client
                    .verify_range(url, start, &expected, total_size, latest.resume_validator())
                    .await?;
            }
        }
        Ok(())
    }
}

/// Seeds chunks for a file whose first `existing` bytes are already on disk.
/// The prefix chunk acknowledges the saved bytes so only the rest is requested.
pub(super) fn seed_existing_prefix(
    existing: u64,
    total: u64,
    num_chunks: usize,
) -> Vec<ActiveChunk> {
    let mut chunks = Vec::new();
    if existing > 0 {
        let mut prefix = ActiveChunk::new(ChunkRange {
            id: 0,
            start: 0,
            end: existing - 1,
        });
        prefix.downloaded = existing;
        prefix.is_done = true;
        chunks.push(prefix);
    }
    for range in calculate_chunks(total - existing, num_chunks) {
        let id = chunks.len();
        chunks.push(ActiveChunk::new(ChunkRange {
            id,
            start: range.start + existing,
            end: range.end + existing,
        }));
    }
    chunks
}

pub(super) fn resume_chunks_are_valid(chunks: &[ActiveChunk], total_size: u64) -> bool {
    if chunks.is_empty()
        || chunks
            .iter()
            .enumerate()
            .any(|(id, chunk)| chunk.range.id != id)
    {
        return false;
    }
    let mut ordered: Vec<_> = chunks.iter().collect();
    ordered.sort_unstable_by_key(|chunk| chunk.range.start);
    let mut next = 0;
    for chunk in ordered {
        let range = chunk.range;
        if range.start != next || range.end < range.start || range.end >= total_size {
            return false;
        }
        let size = range.end - range.start + 1;
        if chunk.downloaded > size || (chunk.is_done && chunk.downloaded != size) {
            return false;
        }
        next = range.end + 1;
    }
    next == total_size
}
