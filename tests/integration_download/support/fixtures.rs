use kosmos_downloader::engine::DownloadSnapshot;
use std::path::PathBuf;
use std::time::Duration;

pub(crate) const TEST_DATA_SIZE: usize = 64 * 1024;
pub(crate) const DYNAMIC_DATA_SIZE: usize = 2 * 1024 * 1024;
pub(crate) const DYNAMIC_INITIAL_CHUNK_SIZE: usize = DYNAMIC_DATA_SIZE / 2;
pub(crate) const DYNAMIC_PREFIX_SIZE: usize = 128 * 1024;
pub(crate) const MIN_DYNAMIC_CHILD_SIZE: usize = 256 * 1024;
pub(crate) const DYNAMIC_ETAG: &str = "\"dynamic-v1\"";
pub(crate) const RETRY_ETAG: &str = "\"retry-v1\"";
pub(crate) const RETRY_PREFIX_SIZE: usize = 64 * 1024;

pub(crate) fn generate_test_payload() -> Vec<u8> {
    (0..TEST_DATA_SIZE).map(|i| (i % 251) as u8).collect()
}

pub(crate) fn generate_offset_payload(size: usize) -> Vec<u8> {
    (0..size)
        .map(|offset| {
            let offset = offset as u64;
            (offset.wrapping_mul(0x9e37_79b9).rotate_left(11) >> 24) as u8
        })
        .collect()
}

pub(crate) fn regression_save_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("kosmos_{name}_{}.bin", std::process::id()))
}

pub(crate) async fn wait_for_snapshot(
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
