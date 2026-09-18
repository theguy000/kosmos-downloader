use super::metadata::{FetchInfoKind, spawn_info_fetch};
use super::progress::drain_cancelled_progress;
use super::resume::SavedDownload;
use super::{CoordinatorError, Session, calculate_downloaded, uses_range_workers};
use crate::engine::model::{DownloadAction, DownloadStatus};
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
                self.file_info = None;
                self.current_speed = 0;
                self.bytes_since_last_tick = 0;
                self.last_tick = Instant::now();

                self.publish(None, 0, 0, None, false);

                self.info_handle = Some(spawn_info_fetch(
                    sid,
                    FetchInfoKind::Start {
                        save_path: self.current_path.clone(),
                        num_chunks: self.current_num_chunks,
                    },
                    self.current_url.clone(),
                    self.client.clone(),
                    self.cancel_tx.subscribe(),
                    self.info_tx.clone(),
                    None,
                ));
            }

            DownloadAction::Pause => {
                if matches!(
                    self.status,
                    DownloadStatus::Connecting | DownloadStatus::Downloading
                ) {
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

            DownloadAction::Resume => {
                if self.status == DownloadStatus::Paused
                    || (matches!(self.status, DownloadStatus::Failed(_))
                        && self.snapshot_tx.borrow().resumable)
                {
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
                            .map(|(info, storage)| {
                                SavedDownload::new(info, storage, &self.active_chunks)
                            }),
                    ));
                }
            }

            DownloadAction::Cancel => {
                let _ = self.cancel_tx.send(true);
                self.wait_for_active_tasks().await;
                self.session_id += 1;
                self.status = DownloadStatus::Idle;
                self.current_speed = 0;
                self.active_chunks.clear();
                self.active_storage = None;
                self.file_info = None;
                self.publish(None, 0, 0, None, false);
            }
        }
    }
}
