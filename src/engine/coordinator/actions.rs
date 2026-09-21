use super::duplicate::TargetMode;
use super::metadata::{FetchInfoKind, spawn_info_fetch};
use super::progress::drain_cancelled_progress;
use super::resume::SavedDownload;
use super::{CoordinatorError, Session, calculate_downloaded, uses_range_workers};
use crate::engine::model::{DownloadAction, DownloadStatus};
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::watch;

impl Session {
    pub(super) async fn handle_action(&mut self, action: DownloadAction) {
        match action {
            DownloadAction::Start {
                url,
                save_path,
                num_chunks,
            } => {
                // The link of a download that is already in the list is answered first.
                if self.duplicate_link(&url) {
                    self.ask_link_duplicate(url, save_path, num_chunks).await;
                    return;
                }
                self.begin_start(url, save_path, num_chunks, TargetMode::Ask)
                    .await;
            }

            DownloadAction::SetDuplicatePreference { choice } => {
                self.duplicate_preference = choice;
            }

            DownloadAction::ResolveDuplicate { session_id, choice } => {
                self.resolve_duplicate(session_id, choice).await;
            }

            DownloadAction::Pause => {
                if matches!(
                    self.status,
                    DownloadStatus::Connecting | DownloadStatus::Downloading
                ) {
                    self.duplicate = None;
                    let _ = self.cancel_tx.send(true);
                    let range_download = self.file_info.as_ref().is_some_and(uses_range_workers);
                    let total_size = self.file_info.as_ref().and_then(|info| info.content_length);
                    self.wait_for_active_tasks().await;
                    match drain_cancelled_progress(
                        &mut self.worker_rx,
                        self.session_id,
                        &mut self.active_chunks,
                        range_download,
                        total_size,
                        self.file_info.as_ref().is_some_and(uses_range_workers),
                    ) {
                        Ok(()) => {
                            self.status = DownloadStatus::Paused;
                            self.current_speed = 0;
                            self.bytes_since_last_tick = 0;
                            self.publish(
                                total_size,
                                calculate_downloaded(&self.active_chunks),
                                0,
                                None,
                                self.file_info.as_ref().is_none_or(uses_range_workers),
                            );
                        }
                        Err(error) => {
                            if matches!(&error, CoordinatorError::Worker { source, .. } if source.is_content_changed())
                            {
                                self.restart_required = true;
                                return;
                            }
                            self.status = DownloadStatus::Failed(error.to_string());
                            self.current_speed = 0;
                            self.publish(
                                total_size,
                                calculate_downloaded(&self.active_chunks),
                                0,
                                None,
                                false,
                            );
                        }
                    }
                }
            }

            DownloadAction::Resume => self.resume_download().await,

            DownloadAction::Cancel => {
                self.duplicate = None;
                let _ = self.cancel_tx.send(true);
                self.wait_for_active_tasks().await;
                self.session_id += 1;
                self.status = DownloadStatus::Idle;
                self.current_speed = 0;
                self.active_chunks.clear();
                self.active_storage = None;
                self.owns_target = false;
                self.file_info = None;
                self.publish(None, 0, 0, None, false);
            }

            DownloadAction::Remove {
                expected_session_id,
                expected_status,
                delete_file,
                completed_only,
            } => {
                if self.status == DownloadStatus::Idle
                    || self.session_id != expected_session_id
                    || self.status != expected_status
                    || (completed_only && self.status != DownloadStatus::Completed)
                {
                    return;
                }

                self.duplicate = None;
                let _ = self.cancel_tx.send(true);
                self.wait_for_active_tasks().await;
                self.session_id += 1;

                let total_bytes = self.file_info.as_ref().and_then(|info| info.content_length);
                let downloaded_bytes = calculate_downloaded(&self.active_chunks);
                let storage = self.active_storage.take();
                self.active_chunks.clear();
                self.file_info = None;
                self.current_speed = 0;
                self.bytes_since_last_tick = 0;
                self.restart_required = false;
                drop(storage);

                let removal = if delete_file && self.owns_target {
                    let path = self.current_path.clone();
                    Some(tokio::task::spawn_blocking(move || std::fs::remove_file(path)).await)
                } else {
                    None
                };
                let error = match removal {
                    Some(Ok(Err(error))) if error.kind() != std::io::ErrorKind::NotFound => {
                        Some(format!(
                            "Failed to delete download file {}: {error}",
                            self.current_path.display()
                        ))
                    }
                    Some(Err(error)) => Some(format!(
                        "Failed to run file deletion for {}: {error}",
                        self.current_path.display()
                    )),
                    _ => None,
                };

                if let Some(error) = error {
                    self.status = DownloadStatus::Failed(error);
                    self.publish(total_bytes, downloaded_bytes, 0, None, false);
                } else {
                    self.owns_target = false;
                    self.status = DownloadStatus::Idle;
                    self.publish(None, 0, 0, None, false);
                }
            }
        }
    }
}

/// Starting and restarting downloads.
impl Session {
    /// Drops the previous session and resolves the target of a new download.
    pub(super) async fn begin_start(
        &mut self,
        url: String,
        save_path: PathBuf,
        num_chunks: usize,
        target: TargetMode,
    ) {
        let _ = self.cancel_tx.send(true);
        self.wait_for_active_tasks().await;
        self.session_id += 1;
        let sid = self.session_id;

        let (new_cancel_tx, _) = watch::channel(false);
        self.cancel_tx = new_cancel_tx;

        self.current_url = url;
        self.current_path = save_path;
        self.current_num_chunks = num_chunks;
        self.content_restarts = 0;
        self.current_filename.clear();
        self.status = DownloadStatus::Connecting;
        self.active_chunks.clear();
        self.active_storage = None;
        self.owns_target = false;
        self.file_info = None;
        self.duplicate = None;
        self.current_speed = 0;
        self.bytes_since_last_tick = 0;
        self.last_tick = Instant::now();

        self.publish(None, 0, 0, None, false);

        self.info_handle = Some(spawn_info_fetch(
            sid,
            FetchInfoKind::Start {
                save_path: self.current_path.clone(),
                num_chunks: self.current_num_chunks,
                target,
            },
            self.current_url.clone(),
            self.client.clone(),
            self.cancel_tx.subscribe(),
            self.info_tx.clone(),
            None,
        ));
    }

    /// Reloads remote metadata and continues a stopped download.
    pub(super) async fn resume_download(&mut self) {
        if self.status != DownloadStatus::Paused
            && !(matches!(self.status, DownloadStatus::Failed(_))
                && self.snapshot_tx.borrow().resumable)
        {
            return;
        }

        self.duplicate = None;
        self.wait_for_active_tasks().await;
        self.session_id += 1;
        let sid = self.session_id;
        let (new_cancel_tx, _) = watch::channel(false);
        self.cancel_tx = new_cancel_tx;
        self.status = DownloadStatus::Connecting;
        self.current_speed = 0;
        self.bytes_since_last_tick = 0;
        self.last_tick = Instant::now();

        self.publish(
            self.file_info.as_ref().and_then(|i| i.content_length),
            calculate_downloaded(&self.active_chunks),
            0,
            None,
            self.file_info.as_ref().is_none_or(uses_range_workers),
        );

        let kind = if self.active_storage.is_some() {
            FetchInfoKind::Resume
        } else {
            FetchInfoKind::Start {
                save_path: self.current_path.clone(),
                num_chunks: self.current_num_chunks,
                // A file that appeared since the failure is confirmed before reuse.
                target: TargetMode::Ask,
            }
        };
        self.info_handle = Some(spawn_info_fetch(
            sid,
            kind,
            self.current_url.clone(),
            self.client.clone(),
            self.cancel_tx.subscribe(),
            self.info_tx.clone(),
            self.file_info
                .as_ref()
                .zip(self.active_storage.as_ref())
                .filter(|(info, _)| uses_range_workers(info))
                .map(|(info, storage)| SavedDownload::new(info, storage, &self.active_chunks)),
        ));
    }
}
