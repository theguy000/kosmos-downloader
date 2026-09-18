use super::resume::{SavedDownload, resume_chunks_are_valid};
use super::scheduler::{MAX_CHUNK_RETRIES, spawn_download_workers};
use super::{CoordinatorError, Session, calculate_downloaded, uses_range_workers};
use crate::client::{HttpClient, RemoteFileInfo, is_strong_etag};
use crate::engine::model::DownloadStatus;
use crate::engine::worker::WorkerError;
use crate::storage::{Storage, StorageError};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

pub(super) enum FetchInfoKind {
    Start {
        save_path: PathBuf,
        num_chunks: usize,
    },
    Resume,
    Restart,
    Complete,
}

pub(super) struct FetchInfoMsg {
    pub(super) session_id: u64,
    pub(super) kind: FetchInfoKind,
    pub(super) result: Result<RemoteFileInfo, WorkerError>,
}

pub(super) fn spawn_info_fetch(
    session_id: u64,
    kind: FetchInfoKind,
    url: String,
    client: HttpClient,
    mut cancel_rx: watch::Receiver<bool>,
    info_tx: mpsc::Sender<FetchInfoMsg>,
    saved: Option<SavedDownload>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result = tokio::select! {
            _ = cancel_rx.changed() => return,
            result = async {
                let mut retries = 0;
                loop {
                    let attempt: Result<_, WorkerError> = async {
                        let info = if matches!(kind, FetchInfoKind::Complete)
                            && let Some(saved) = &saved
                            && (!uses_range_workers(&saved.info)
                                || saved.info.etag.as_deref().is_some_and(is_strong_etag))
                        {
                            saved.info.clone()
                        } else {
                            client.fetch_info(&url).await?
                        };
                        if let Some(saved) = &saved && uses_range_workers(&saved.info) {
                            saved.verify(&client, &url, &info).await?;
                        }
                        Ok(info)
                    }.await;
                    if saved.is_some() && retries < MAX_CHUNK_RETRIES
                        && attempt.as_ref().is_err_and(|error| error.is_retryable())
                    {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                    break attempt;
                }
            } => result,
        };

        if *cancel_rx.borrow() {
            return;
        }

        tokio::select! {
            _ = cancel_rx.changed() => {}
            _ = info_tx.send(FetchInfoMsg { session_id, kind, result }) => {}
        }
    })
}

impl Session {
    pub(super) async fn handle_info(&mut self, msg: FetchInfoMsg) {
        let expected_status = if matches!(msg.kind, FetchInfoKind::Complete) {
            DownloadStatus::Downloading
        } else {
            DownloadStatus::Connecting
        };
        if msg.session_id != self.session_id || self.status != expected_status {
            return;
        }

        self.info_handle = None;
        match (msg.kind, msg.result) {
            (FetchInfoKind::Complete, Ok(_)) => {
                let flush = if let Some(storage) = &self.active_storage {
                    let storage = storage.clone();
                    tokio::task::spawn_blocking(move || storage.sync())
                        .await
                        .map_err(CoordinatorError::StorageTask)
                        .and_then(|result| result.map_err(CoordinatorError::Storage))
                } else {
                    Ok(())
                };
                self.status = match flush {
                    Ok(()) => DownloadStatus::Completed,
                    Err(err) => {
                        DownloadStatus::Failed(format!("Failed to flush completed download: {err}"))
                    }
                };
                self.current_speed = 0;
                let downloaded = calculate_downloaded(&self.active_chunks);
                let total = self
                    .file_info
                    .as_ref()
                    .and_then(|info| info.content_length)
                    .unwrap_or(downloaded);
                self.publish(
                    Some(total),
                    downloaded,
                    0,
                    None,
                    self.status == DownloadStatus::Completed,
                );
            }
            (
                FetchInfoKind::Start {
                    save_path,
                    num_chunks,
                },
                Ok(info),
            ) => {
                let is_dir = save_path.is_dir()
                    || save_path.extension().is_none()
                    || save_path.to_string_lossy().ends_with('/')
                    || save_path.to_string_lossy().ends_with('\\');

                if is_dir {
                    self.current_filename = info.filename.clone();
                    self.current_path = save_path.join(&self.current_filename);
                } else {
                    self.current_filename = save_path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or(&info.filename)
                        .to_string();
                    self.current_path = save_path;
                }

                let storage = match tokio::task::spawn_blocking({
                    let path = self.current_path.clone();
                    let total_size = info.content_length;
                    move || Storage::create_new(&path, total_size)
                })
                .await
                {
                    Ok(Ok(storage)) => Ok(storage),
                    Ok(Err(err)) => Err(match &err {
                        StorageError::Io(error)
                            if error.kind() == std::io::ErrorKind::AlreadyExists =>
                        {
                            format!(
                                "Refusing to overwrite existing file {}. Choose a different path or remove it first: {err}",
                                self.current_path.display()
                            )
                        }
                        _ => err.to_string(),
                    }),
                    Err(err) => Err(format!(
                        "Could not create download file {}: {err}",
                        self.current_path.display()
                    )),
                };

                match storage {
                    Ok(storage) => {
                        let total_size = info.content_length;
                        let resumable = uses_range_workers(&info);
                        spawn_download_workers(
                            self.session_id,
                            &self.current_url,
                            &self.client,
                            &info,
                            num_chunks,
                            &storage,
                            &self.cancel_tx,
                            &self.worker_tx,
                            &mut self.active_chunks,
                            &mut self.worker_handles,
                        );
                        self.active_storage = Some(storage);
                        self.file_info = Some(info);
                        self.status = DownloadStatus::Downloading;

                        self.publish(total_size, 0, 0, None, resumable);
                    }
                    Err(message) => {
                        self.status = DownloadStatus::Failed(message);
                        self.publish(None, 0, 0, None, false);
                    }
                }
            }
            (kind @ (FetchInfoKind::Resume | FetchInfoKind::Restart), Ok(info)) => {
                let Some(storage) = self.active_storage.as_ref() else {
                    self.status =
                        DownloadStatus::Failed("Missing storage for paused download".to_string());
                    self.publish(
                        self.file_info
                            .as_ref()
                            .and_then(|current| current.content_length),
                        calculate_downloaded(&self.active_chunks),
                        0,
                        None,
                        false,
                    );
                    return;
                };

                let was_resumable = !matches!(kind, FetchInfoKind::Restart)
                    && self.file_info.as_ref().is_some_and(uses_range_workers);
                let resume = match info.content_length {
                    Some(total_size)
                        if uses_range_workers(&info)
                            && self.file_info.as_ref().is_some_and(|previous| {
                                previous.resume_metadata_matches(&info)
                            })
                            && resume_chunks_are_valid(&self.active_chunks, total_size) =>
                    {
                        Some(total_size)
                    }
                    _ => None,
                };

                let total_size = info.content_length;
                if was_resumable {
                    if let Some(total_size) = resume {
                        self.status = DownloadStatus::Downloading;
                        for chunk in &mut self.active_chunks {
                            // Completed chunks acknowledge again so pausing final verification can resume.
                            chunk.is_done = false;
                            chunk.retries = 0;
                            self.worker_handles.push(chunk.spawn(
                                self.session_id,
                                &self.current_url,
                                &self.client,
                                total_size,
                                info.resume_validator(),
                                storage,
                                &self.cancel_tx,
                                &self.worker_tx,
                            ));
                        }
                        self.file_info = Some(info);

                        self.publish(
                            Some(total_size),
                            calculate_downloaded(&self.active_chunks),
                            0,
                            None,
                            true,
                        );
                    } else {
                        self.status = DownloadStatus::Failed(
                                    "Cannot safely resume: the remote file changed or could not be validated"
                                        .to_string(),
                                );
                        self.publish(
                            self.file_info
                                .as_ref()
                                .and_then(|current| current.content_length),
                            calculate_downloaded(&self.active_chunks),
                            0,
                            None,
                            false,
                        );
                    }
                } else {
                    let resize = tokio::task::spawn_blocking({
                        let storage = storage.clone();
                        move || storage.set_len(total_size.unwrap_or(0))
                    })
                    .await
                    .map_err(CoordinatorError::StorageTask)
                    .and_then(|result| result.map_err(CoordinatorError::Storage));
                    match resize {
                        Ok(()) => {
                            self.active_chunks.clear();
                            spawn_download_workers(
                                self.session_id,
                                &self.current_url,
                                &self.client,
                                &info,
                                self.current_num_chunks,
                                storage,
                                &self.cancel_tx,
                                &self.worker_tx,
                                &mut self.active_chunks,
                                &mut self.worker_handles,
                            );
                            self.file_info = Some(info);
                            self.status = DownloadStatus::Downloading;
                            self.publish(
                                total_size,
                                0,
                                0,
                                None,
                                self.file_info.as_ref().is_some_and(uses_range_workers),
                            );
                        }
                        Err(err) => {
                            self.status = DownloadStatus::Failed(err.to_string());
                            self.publish(
                                self.file_info
                                    .as_ref()
                                    .and_then(|current| current.content_length),
                                calculate_downloaded(&self.active_chunks),
                                0,
                                None,
                                false,
                            );
                        }
                    }
                }
            }
            (FetchInfoKind::Start { .. }, Err(err)) => {
                let retryable = err.is_retryable();
                self.status = DownloadStatus::Failed(err.to_string());
                self.publish(None, 0, 0, None, retryable);
            }
            (
                FetchInfoKind::Resume | FetchInfoKind::Restart | FetchInfoKind::Complete,
                Err(err),
            ) => {
                if err.is_content_changed() {
                    self.restart_required = true;
                    return;
                }
                let retryable = err.is_retryable();
                self.status = DownloadStatus::Failed(err.to_string());
                self.publish(
                    self.file_info
                        .as_ref()
                        .and_then(|current| current.content_length),
                    calculate_downloaded(&self.active_chunks),
                    0,
                    None,
                    retryable,
                );
            }
        }
    }
}
