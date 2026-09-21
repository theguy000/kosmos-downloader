use super::duplicate::{ExistingFile, TargetError, TargetMode, create_target};
use super::resume::{SavedDownload, resume_chunks_are_valid};
use super::scheduler::{MAX_CHUNK_RETRIES, spawn_download_workers};
use super::{CoordinatorError, Session, calculate_downloaded, uses_range_workers};
use crate::client::{HttpClient, RemoteFileInfo, is_strong_etag};
use crate::engine::model::DownloadStatus;
use crate::engine::worker::WorkerError;
use crate::storage::{Storage, StorageError};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

/// Upper bound on automatic `name_N` collision renames before failing.
/// ponytail: circuit breaker; a normal filesystem finds a free name on the first try.
const MAX_AUTO_RENAME_ATTEMPTS: u32 = 10_000;

pub(super) enum FetchInfoKind {
    Start {
        save_path: PathBuf,
        num_chunks: usize,
        target: TargetMode,
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
                    target,
                },
                Ok(info),
            ) => {
                let created = tokio::task::spawn_blocking({
                    let save_path = save_path.clone();
                    let info_filename = info.filename.clone();
                    let total_size = info.content_length;
                    move || create_target(&save_path, &info_filename, total_size, target)
                })
                .await;

                match created {
                    Ok(Ok((filename, path, storage))) => {
                        self.launch_download(info, filename, path, storage, num_chunks);
                    }
                    Ok(Err(TargetError::Existing {
                        filename,
                        path,
                        bytes,
                    })) => {
                        self.current_filename = filename.clone();
                        self.current_path = path.clone();
                        let existing = ExistingFile {
                            filename,
                            path,
                            bytes,
                        };
                        self.ask_existing_target(save_path, num_chunks, info, existing)
                            .await;
                    }
                    Ok(Err(TargetError::Storage {
                        filename,
                        path,
                        source,
                    })) => {
                        self.fail_target(filename, path, &source);
                    }
                    Err(error) => {
                        let error = CoordinatorError::StorageTask(error);
                        self.status = DownloadStatus::Failed(format!(
                            "Could not create download file {}: {error}",
                            self.current_path.display()
                        ));
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

impl Session {
    /// Writes a prepared target file and starts one worker per chunk.
    pub(super) fn launch_download(
        &mut self,
        info: RemoteFileInfo,
        filename: String,
        path: PathBuf,
        storage: Storage,
        num_chunks: usize,
    ) {
        let total_size = info.content_length;
        let resumable = uses_range_workers(&info);
        self.current_filename = filename;
        self.current_path = path;
        self.owns_target = true;
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

    /// Reports a target file that could not be prepared.
    pub(super) fn fail_target(&mut self, filename: String, path: PathBuf, source: &StorageError) {
        self.current_filename = filename;
        self.current_path = path;
        self.owns_target = matches!(source, StorageError::CreatedFileInitialization(_));
        let message = match source {
            StorageError::Io(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                format!(
                    "Refusing to overwrite existing file {}. Choose a different path or remove it first: {source}",
                    self.current_path.display()
                )
            }
            _ => source.to_string(),
        };
        self.status = DownloadStatus::Failed(message);
        self.publish(None, 0, 0, None, false);
    }
}

pub(super) fn create_collision_free(
    save_path: &Path,
    info_filename: &str,
    total_size: Option<u64>,
) -> Result<(String, PathBuf, Storage), (String, PathBuf, StorageError)> {
    let mut candidate_name = info_filename.to_string();
    let mut candidate_path = save_path.join(&candidate_name);
    let mut index = 0;
    loop {
        if index > 0 {
            candidate_name = next_numbered_filename(info_filename, index);
            candidate_path = save_path.join(&candidate_name);
        }
        match Storage::create_new(&candidate_path, total_size) {
            Ok(storage) => return Ok((candidate_name, candidate_path, storage)),
            Err(StorageError::Io(err)) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                index += 1;
                if index >= MAX_AUTO_RENAME_ATTEMPTS {
                    let err = std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!("no available filename after {MAX_AUTO_RENAME_ATTEMPTS} attempts"),
                    );
                    return Err((candidate_name, candidate_path, StorageError::Io(err)));
                }
            }
            Err(err) => return Err((candidate_name, candidate_path, err)),
        }
    }
}

pub(super) fn next_numbered_filename(original_name: &str, index: u32) -> String {
    let path = Path::new(original_name);
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(original_name);
    let ext = path.extension().and_then(|e| e.to_str());
    match ext {
        Some(ext) => format!("{stem}_{index}.{ext}"),
        None => format!("{stem}_{index}"),
    }
}
