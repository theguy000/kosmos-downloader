use super::metadata::create_collision_free;
use super::resume::seed_existing_prefix;
use super::scheduler::ActiveChunk;
use super::{Session, calculate_downloaded, uses_range_workers};
use crate::client::{HttpClient, RemoteFileInfo};
use crate::engine::chunks::ChunkRange;
use crate::engine::model::{DownloadStatus, DuplicateChoice, DuplicatePrompt};
use crate::engine::worker::OVERLAP_BYTES;
use crate::storage::{Storage, StorageError};
use std::path::{Path, PathBuf};

/// How the engine picks the target file of a download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TargetMode {
    /// Ask the user before touching a file that already exists.
    Ask,
    /// Append a numeric suffix to the server filename.
    Numbered,
    /// Reuse the target path, discarding existing contents.
    Overwrite,
}

/// Why the engine could not prepare a target file.
pub(super) enum TargetError {
    /// A file is already there and the user has to decide what happens to it.
    Existing {
        filename: String,
        path: PathBuf,
        bytes: u64,
    },
    Storage {
        filename: String,
        path: PathBuf,
        source: StorageError,
    },
}

/// The file on disk a duplicate prompt is about.
pub(super) struct ExistingFile {
    pub(super) filename: String,
    pub(super) path: PathBuf,
    pub(super) bytes: u64,
}

impl ExistingFile {
    fn prompt(&self, session_id: u64, url: String) -> DuplicatePrompt {
        DuplicatePrompt {
            session_id,
            url,
            filename: self.filename.clone(),
            existing_bytes: Some(self.bytes),
            link_duplicate: false,
        }
    }
}

pub(super) struct PendingDuplicate {
    pub(super) prompt: DuplicatePrompt,
    request: DuplicateRequest,
}

enum DuplicateRequest {
    /// The added link matches the download that is already in the list.
    Link {
        url: String,
        save_path: PathBuf,
        num_chunks: usize,
    },
    /// The resolved target file already exists on disk.
    Target {
        save_path: PathBuf,
        num_chunks: usize,
        info: RemoteFileInfo,
        existing: ExistingFile,
    },
}

/// Directory targets get the server filename; everything else is an explicit file path.
fn is_directory_target(save_path: &Path) -> bool {
    let display = save_path.to_string_lossy();
    save_path.is_dir()
        || save_path.extension().is_none()
        || display.ends_with('/')
        || display.ends_with('\\')
}

/// Creates the target file for `mode`, never replacing a file the user did not confirm.
pub(super) fn create_target(
    save_path: &Path,
    info_filename: &str,
    total_size: Option<u64>,
    mode: TargetMode,
) -> Result<(String, PathBuf, Storage), TargetError> {
    let (directory, filename) = if is_directory_target(save_path) {
        (save_path.to_path_buf(), info_filename.to_string())
    } else {
        let filename = save_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(info_filename)
            .to_string();
        (
            save_path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_default(),
            filename,
        )
    };

    match mode {
        TargetMode::Ask => {
            let path = directory.join(&filename);
            match Storage::create_new(&path, total_size) {
                Ok(storage) => Ok((filename, path, storage)),
                Err(StorageError::Io(error))
                    if error.kind() == std::io::ErrorKind::AlreadyExists =>
                {
                    match std::fs::metadata(&path) {
                        Ok(metadata) => Err(TargetError::Existing {
                            filename,
                            path,
                            bytes: metadata.len(),
                        }),
                        Err(source) => Err(TargetError::Storage {
                            filename,
                            path,
                            source: StorageError::Io(source),
                        }),
                    }
                }
                Err(source) => Err(TargetError::Storage {
                    filename,
                    path,
                    source,
                }),
            }
        }
        TargetMode::Numbered => match create_collision_free(&directory, &filename, total_size) {
            Ok(created) => Ok(created),
            Err((filename, path, source)) => Err(TargetError::Storage {
                filename,
                path,
                source,
            }),
        },
        TargetMode::Overwrite => {
            let path = directory.join(&filename);
            match Storage::create_or_open(&path, total_size, true) {
                Ok(storage) => Ok((filename, path, storage)),
                Err(source) => Err(TargetError::Storage {
                    filename,
                    path,
                    source,
                }),
            }
        }
    }
}

/// Marks every byte of a file that is already on disk as downloaded.
fn completed_chunks(total: u64) -> Vec<ActiveChunk> {
    if total == 0 {
        return Vec::new();
    }
    let mut chunk = ActiveChunk::new(ChunkRange {
        id: 0,
        start: 0,
        end: total - 1,
    });
    chunk.downloaded = total;
    chunk.is_done = true;
    vec![chunk]
}

/// Sessions answer duplicate prompts through these entry points.
impl Session {
    /// Detects a repeated link while its entry is still in the download list.
    pub(super) fn duplicate_link(&self, url: &str) -> bool {
        self.status != DownloadStatus::Idle
            && !self.current_url.is_empty()
            && self.current_url == url
    }

    /// Publishes the prompt for a repeated link and waits for the user's answer.
    pub(super) async fn ask_link_duplicate(
        &mut self,
        url: String,
        save_path: PathBuf,
        num_chunks: usize,
    ) {
        let prompt = DuplicatePrompt {
            session_id: self.session_id,
            url: url.clone(),
            filename: self.current_filename.clone(),
            existing_bytes: self.file_info.as_ref().and_then(|info| info.content_length),
            link_duplicate: true,
        };
        let pending = PendingDuplicate {
            prompt,
            request: DuplicateRequest::Link {
                url,
                save_path,
                num_chunks,
            },
        };
        self.settle_duplicate(pending).await;
    }

    /// Publishes the prompt for a target file that is already on disk.
    pub(super) async fn ask_existing_target(
        &mut self,
        save_path: PathBuf,
        num_chunks: usize,
        info: RemoteFileInfo,
        existing: ExistingFile,
    ) {
        let pending = PendingDuplicate {
            prompt: existing.prompt(self.session_id, self.current_url.clone()),
            request: DuplicateRequest::Target {
                save_path,
                num_chunks,
                info,
                existing,
            },
        };
        self.settle_duplicate(pending).await;
    }

    /// Applies the remembered decision, or publishes the prompt and waits.
    async fn settle_duplicate(&mut self, pending: PendingDuplicate) {
        match self.duplicate_preference {
            Some(choice) => self.apply_duplicate(pending, Some(choice)).await,
            None => {
                self.duplicate = Some(pending);
                self.publish_prompt();
            }
        }
    }

    /// Reports the current state, so the UI sees a prompt appear or clear.
    pub(super) fn publish_prompt(&self) {
        let info = self.file_info.as_ref();
        self.publish(
            info.and_then(|info| info.content_length),
            calculate_downloaded(&self.active_chunks),
            0,
            None,
            info.is_some_and(uses_range_workers),
        );
    }

    /// Applies the answer to the pending duplicate prompt.
    pub(super) async fn resolve_duplicate(
        &mut self,
        session_id: u64,
        choice: Option<DuplicateChoice>,
    ) {
        // An answer for a replaced download must not clear the current prompt.
        if !self
            .duplicate
            .as_ref()
            .is_some_and(|pending| pending.prompt.session_id == session_id)
        {
            return;
        }
        let Some(pending) = self.duplicate.take() else {
            return;
        };
        self.apply_duplicate(pending, choice).await;
    }

    /// Executes the user's or remembered decision for a pending duplicate.
    async fn apply_duplicate(
        &mut self,
        pending: PendingDuplicate,
        choice: Option<DuplicateChoice>,
    ) {
        match (pending.request, choice) {
            (DuplicateRequest::Link { .. }, None) => self.publish_prompt(),
            (DuplicateRequest::Target { .. }, None) => {
                self.status = DownloadStatus::Idle;
                self.current_speed = 0;
                self.publish(None, 0, 0, None, false);
            }
            (
                DuplicateRequest::Link {
                    url,
                    save_path,
                    num_chunks,
                },
                Some(DuplicateChoice::Numbered),
            ) => {
                self.begin_start(url, save_path, num_chunks, TargetMode::Numbered)
                    .await;
            }
            (
                DuplicateRequest::Link {
                    url,
                    save_path,
                    num_chunks,
                },
                Some(DuplicateChoice::Overwrite),
            ) => {
                self.begin_start(url, save_path, num_chunks, TargetMode::Overwrite)
                    .await;
            }
            (DuplicateRequest::Link { .. }, Some(DuplicateChoice::UseExisting)) => {
                self.keep_or_resume_existing().await;
            }
            (
                DuplicateRequest::Target {
                    save_path,
                    num_chunks,
                    info,
                    ..
                },
                Some(DuplicateChoice::Numbered),
            ) => {
                self.start_decided(info, save_path, num_chunks, TargetMode::Numbered)
                    .await;
            }
            (
                DuplicateRequest::Target {
                    save_path,
                    num_chunks,
                    info,
                    ..
                },
                Some(DuplicateChoice::Overwrite),
            ) => {
                self.start_decided(info, save_path, num_chunks, TargetMode::Overwrite)
                    .await;
            }
            (
                DuplicateRequest::Target { info, existing, .. },
                Some(DuplicateChoice::UseExisting),
            ) => self.adopt_existing(info, existing).await,
        }
    }

    /// Keeps the existing duplicate entry, resuming it only when it stopped.
    async fn keep_or_resume_existing(&mut self) {
        let resumable = self.snapshot_tx.borrow().resumable;
        match self.status.clone() {
            DownloadStatus::Paused => self.resume_download().await,
            DownloadStatus::Failed(_) if resumable => self.resume_download().await,
            // Running and completed downloads stay exactly as they are.
            _ => self.publish_prompt(),
        }
    }
}

/// Creates the target file for a decision the user already made.
impl Session {
    /// Creates a user-chosen target and starts the download that was already resolved.
    async fn start_decided(
        &mut self,
        info: RemoteFileInfo,
        save_path: PathBuf,
        num_chunks: usize,
        mode: TargetMode,
    ) {
        let created = tokio::task::spawn_blocking({
            let save_path = save_path.clone();
            let info_filename = info.filename.clone();
            let total_size = info.content_length;
            move || create_target(&save_path, &info_filename, total_size, mode)
        })
        .await;

        match created {
            Ok(Ok((filename, path, storage))) => {
                self.launch_download(info, filename, path, storage, num_chunks);
            }
            // Numbered and Overwrite always find a usable path, so this only guards a race.
            Ok(Err(TargetError::Existing { filename, path, .. })) => {
                self.fail_target(filename, path, &already_exists());
            }
            Ok(Err(TargetError::Storage {
                filename,
                path,
                source,
            })) => self.fail_target(filename, path, &source),
            Err(error) => {
                self.status =
                    DownloadStatus::Failed(format!("Could not create download file: {error}"));
                self.publish(None, 0, 0, None, false);
            }
        }
    }

    /// Option 3 for an existing file: accept it, resume it, or refuse without touching it.
    async fn adopt_existing(&mut self, info: RemoteFileInfo, existing: ExistingFile) {
        let resumable = uses_range_workers(&info);
        let num_chunks = self.current_num_chunks;
        match info.content_length {
            Some(total) if total == existing.bytes => {
                self.adopt_complete(info, existing, total).await;
            }
            Some(total) if resumable && existing.bytes < total => {
                self.resume_existing(info, existing, total, num_chunks)
                    .await;
            }
            Some(total) if existing.bytes > total => {
                self.fail_existing(&existing, "it is larger than the remote file");
            }
            Some(_) => {
                self.fail_existing(&existing, "the server does not support resuming this file");
            }
            None => self.fail_existing(&existing, "the remote file size is unknown"),
        }
    }

    /// Accepts an on-disk file that already holds the full remote file.
    async fn adopt_complete(&mut self, info: RemoteFileInfo, existing: ExistingFile, total: u64) {
        let opened = tokio::task::spawn_blocking({
            let path = existing.path.clone();
            move || Storage::create_or_open(&path, Some(total), false)
        })
        .await;
        let storage = match opened {
            Ok(Ok(storage)) => storage,
            Ok(Err(error)) => {
                self.fail_existing(&existing, &error.to_string());
                return;
            }
            Err(error) => {
                self.fail_existing(&existing, &format!("it could not be opened: {error}"));
                return;
            }
        };

        // A full-size file is only complete once its bytes match the remote file.
        if total > 0
            && let Err(reason) = self.verify_existing_bytes(&info, &storage, total).await
        {
            self.fail_existing(&existing, &reason);
            return;
        }

        self.current_filename = existing.filename;
        self.current_path = existing.path;
        self.owns_target = true;
        self.active_chunks = completed_chunks(total);
        self.active_storage = Some(storage);
        self.file_info = Some(info);
        self.status = DownloadStatus::Completed;
        self.current_speed = 0;
        self.publish(Some(total), total, 0, None, total > 0);
    }
}

fn already_exists() -> StorageError {
    StorageError::Io(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "target file already exists",
    ))
}

/// Resuming a file the engine did not write itself.
impl Session {
    /// Continues an on-disk file from its saved prefix and downloads the rest.
    async fn resume_existing(
        &mut self,
        info: RemoteFileInfo,
        existing: ExistingFile,
        total: u64,
        num_chunks: usize,
    ) {
        let opened = tokio::task::spawn_blocking({
            let path = existing.path.clone();
            move || Storage::create_or_open(&path, Some(total), false)
        })
        .await;
        let storage = match opened {
            Ok(Ok(storage)) => storage,
            Ok(Err(error)) => {
                self.fail_existing(&existing, &error.to_string());
                return;
            }
            Err(error) => {
                self.fail_existing(&existing, &format!("it could not be opened: {error}"));
                return;
            }
        };

        if let Err(reason) = self
            .verify_existing_bytes(&info, &storage, existing.bytes)
            .await
        {
            self.fail_existing(&existing, &reason);
            return;
        }

        let validator = info.resume_validator().map(str::to_owned);
        self.current_filename = existing.filename;
        self.current_path = existing.path;
        self.owns_target = true;
        self.active_chunks = seed_existing_prefix(existing.bytes, total, num_chunks);
        for chunk in &mut self.active_chunks {
            if chunk.is_done {
                continue;
            }
            self.worker_handles.push(chunk.spawn(
                self.session_id,
                &self.current_url,
                &self.client,
                total,
                validator.as_deref(),
                &storage,
                &self.cancel_tx,
                &self.worker_tx,
            ));
        }
        self.active_storage = Some(storage);
        self.file_info = Some(info);
        self.status = DownloadStatus::Downloading;
        self.publish(
            Some(total),
            calculate_downloaded(&self.active_chunks),
            0,
            None,
            true,
        );
    }

    /// Samples the first and last saved bytes of an existing file against the remote file.
    /// This consistency heuristic covers data the engine did not write itself.
    pub(super) async fn verify_existing_bytes(
        &mut self,
        info: &RemoteFileInfo,
        storage: &Storage,
        bytes: u64,
    ) -> Result<(), String> {
        if bytes == 0 {
            return Ok(());
        }
        let total = info
            .content_length
            .ok_or_else(|| "the remote file size is unknown".to_string())?;
        // The client is cloned so no non-`Sync` session state crosses an await point.
        let client = self.client.clone();
        let url = self.current_url.clone();
        let validator = info.resume_validator().map(str::to_owned);
        verify_saved_bytes(&client, &url, validator.as_deref(), total, storage, bytes).await
    }

    /// Reports an existing file that the engine refuses to reuse or replace.
    fn fail_existing(&mut self, existing: &ExistingFile, reason: &str) {
        self.current_filename = existing.filename.clone();
        self.current_path = existing.path.clone();
        self.owns_target = false;
        self.status = DownloadStatus::Failed(format!(
            "Cannot use existing file {}: {reason}. Choose overwrite or a numbered copy instead",
            existing.path.display()
        ));
        self.publish(None, 0, 0, None, false);
    }
}

/// Samples the first and last saved bytes of a partial file against the remote file.
/// Sampling can miss changes outside the checked regions, so it is a heuristic only.
async fn verify_saved_bytes(
    client: &HttpClient,
    url: &str,
    validator: Option<&str>,
    total: u64,
    storage: &Storage,
    bytes: u64,
) -> Result<(), String> {
    let length = bytes.min(OVERLAP_BYTES);
    let mut starts = vec![0];
    if bytes > length {
        starts.push(bytes - length);
    }

    for start in starts {
        let sample = {
            let storage = storage.clone();
            tokio::task::spawn_blocking(move || {
                let mut buffer = vec![0; length as usize];
                storage.read_at(start, &mut buffer)?;
                Ok::<_, StorageError>(buffer)
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?
        };
        client
            .verify_range(url, start, &sample, total, validator)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}
