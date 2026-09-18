use super::metadata::{FetchInfoKind, spawn_info_fetch};
use super::{CoordinatorError, Session};
use crate::engine::model::DownloadStatus;
use std::time::Instant;
use tokio::sync::watch;

const MAX_CONTENT_RESTARTS: u8 = 2;

impl Session {
    pub(super) async fn wait_for_active_tasks(&mut self) {
        if let Some(handle) = self.info_handle.take() {
            let _ = handle.await;
        }
        for handle in self.worker_handles.drain(..) {
            let _ = handle.await;
        }
    }

    pub(super) async fn restart_changed_content(&mut self) {
        self.restart_required = false;
        let _ = self.cancel_tx.send(true);
        self.wait_for_active_tasks().await;
        self.session_id += 1;
        // Only truncate the already-owned file, after every old writer has stopped.
        let reset = if let Some(storage) = &self.active_storage {
            let storage = storage.clone();
            tokio::task::spawn_blocking(move || storage.set_len(0))
                .await
                .map_err(CoordinatorError::StorageTask)
                .and_then(|result| result.map_err(CoordinatorError::Storage))
        } else {
            Err(CoordinatorError::InvalidProgress)
        };
        self.active_chunks.clear();
        self.file_info = None;
        self.current_speed = 0;
        self.bytes_since_last_tick = 0;
        self.last_tick = Instant::now();
        self.status = match reset {
            Err(error) => {
                DownloadStatus::Failed(format!("Could not restart changed file: {error}"))
            }
            Ok(()) if self.content_restarts >= MAX_CONTENT_RESTARTS => {
                DownloadStatus::Failed(format!(
                    "Remote file keeps changing; stopped after {MAX_CONTENT_RESTARTS} automatic restarts"
                ))
            }
            Ok(()) => {
                self.content_restarts += 1;
                let (new_cancel_tx, _) = watch::channel(false);
                self.cancel_tx = new_cancel_tx;
                self.info_handle = Some(spawn_info_fetch(
                    self.session_id,
                    FetchInfoKind::Restart,
                    self.current_url.clone(),
                    self.client.clone(),
                    self.cancel_tx.subscribe(),
                    self.info_tx.clone(),
                    None,
                ));
                DownloadStatus::Connecting
            }
        };
        self.publish(None, 0, 0, None, false);
    }
}
