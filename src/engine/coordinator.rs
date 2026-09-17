use super::chunks::{ChunkRange, calculate_chunks};
use super::model::{DownloadAction, DownloadSnapshot, DownloadStatus};
use super::worker::{WorkerMsg, spawn_chunk_worker, spawn_stream_worker};
use crate::client::{HttpClient, RemoteFileInfo};
use crate::storage::{Storage, StorageError};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

enum FetchInfoKind {
    Start {
        save_path: PathBuf,
        num_chunks: usize,
    },
    Resume,
}

struct FetchInfoMsg {
    session_id: u64,
    kind: FetchInfoKind,
    result: Result<RemoteFileInfo, crate::client::ClientError>,
}

struct ActiveChunk {
    range: ChunkRange,
    downloaded: u64,
    is_done: bool,
}

pub struct DownloadEngine {
    action_tx: mpsc::Sender<DownloadAction>,
    snapshot_rx: watch::Receiver<DownloadSnapshot>,
}

impl DownloadEngine {
    pub fn new() -> Self {
        let (action_tx, action_rx) = mpsc::channel(32);
        let default_snapshot = DownloadSnapshot::default();
        let (snapshot_tx, snapshot_rx) = watch::channel(default_snapshot);

        tokio::spawn(async move {
            run_coordinator(action_rx, snapshot_tx).await;
        });

        Self {
            action_tx,
            snapshot_rx,
        }
    }

    pub fn action_tx(&self) -> mpsc::Sender<DownloadAction> {
        self.action_tx.clone()
    }

    pub fn snapshot_rx(&self) -> watch::Receiver<DownloadSnapshot> {
        self.snapshot_rx.clone()
    }
}

impl Default for DownloadEngine {
    fn default() -> Self {
        Self::new()
    }
}

fn spawn_info_fetch(
    session_id: u64,
    kind: FetchInfoKind,
    url: String,
    client: HttpClient,
    mut cancel_rx: watch::Receiver<bool>,
    info_tx: mpsc::Sender<FetchInfoMsg>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result = tokio::select! {
            _ = cancel_rx.changed() => return,
            result = client.fetch_info(&url) => result,
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

async fn wait_for_active_tasks(
    info_handle: &mut Option<JoinHandle<()>>,
    worker_handles: &mut Vec<JoinHandle<()>>,
) {
    if let Some(handle) = info_handle.take() {
        let _ = handle.await;
    }

    for handle in worker_handles.drain(..) {
        let _ = handle.await;
    }
}

fn uses_range_workers(info: &RemoteFileInfo) -> bool {
    info.accepts_ranges && matches!(info.content_length, Some(total_size) if total_size > 0)
}

fn is_resumable(info: &RemoteFileInfo) -> bool {
    uses_range_workers(info) && info.resume_validator().is_some()
}

#[allow(clippy::too_many_arguments)]
fn spawn_download_workers(
    session_id: u64,
    url: &str,
    client: &HttpClient,
    info: &RemoteFileInfo,
    num_chunks: usize,
    storage: &Storage,
    cancel_tx: &watch::Sender<bool>,
    worker_tx: &mpsc::Sender<WorkerMsg>,
    active_chunks: &mut Vec<ActiveChunk>,
    worker_handles: &mut Vec<JoinHandle<()>>,
) {
    if uses_range_workers(info)
        && let Some(total_size) = info.content_length
    {
        let validator = info.resume_validator().map(str::to_owned);
        *active_chunks = calculate_chunks(total_size, num_chunks)
            .into_iter()
            .map(|range| ActiveChunk {
                range,
                downloaded: 0,
                is_done: false,
            })
            .collect();

        for chunk in active_chunks.iter() {
            worker_handles.push(spawn_chunk_worker(
                session_id,
                chunk.range,
                0,
                total_size,
                url.to_string(),
                client.clone(),
                storage.clone(),
                validator.clone(),
                cancel_tx.subscribe(),
                worker_tx.clone(),
            ));
        }
    } else {
        *active_chunks = vec![ActiveChunk {
            range: ChunkRange {
                id: 0,
                start: 0,
                end: info
                    .content_length
                    .and_then(|size| size.checked_sub(1))
                    .unwrap_or(0),
            },
            downloaded: 0,
            is_done: false,
        }];
        worker_handles.push(spawn_stream_worker(
            session_id,
            url.to_string(),
            info.content_length,
            client.clone(),
            storage.clone(),
            cancel_tx.subscribe(),
            worker_tx.clone(),
        ));
    }
}

fn resume_chunks_are_valid(chunks: &[ActiveChunk], total_size: u64) -> bool {
    let expected_chunks = calculate_chunks(total_size, chunks.len());
    chunks.len() == expected_chunks.len()
        && chunks.iter().zip(expected_chunks).all(|(chunk, expected)| {
            chunk.range == expected
                && chunk.downloaded <= expected.size()
                && (!chunk.is_done || chunk.downloaded == expected.size())
        })
}

fn record_progress(
    chunks: &mut [ActiveChunk],
    chunk_id: usize,
    bytes_delta: u64,
    range_download: bool,
    total_size: Option<u64>,
) -> bool {
    let Some(chunk) = chunks.iter_mut().find(|chunk| chunk.range.id == chunk_id) else {
        return false;
    };
    let Some(downloaded) = chunk.downloaded.checked_add(bytes_delta) else {
        return false;
    };
    let expected_size = if range_download {
        Some(chunk.range.size())
    } else {
        total_size
    };
    if expected_size.is_some_and(|expected| downloaded > expected) {
        return false;
    }

    chunk.downloaded = downloaded;
    true
}

fn record_done(
    chunks: &mut [ActiveChunk],
    chunk_id: usize,
    range_download: bool,
    total_size: Option<u64>,
) -> bool {
    let Some(chunk) = chunks.iter_mut().find(|chunk| chunk.range.id == chunk_id) else {
        return false;
    };
    let expected_size = if range_download {
        Some(chunk.range.size())
    } else {
        total_size
    };
    if expected_size.is_some_and(|expected| chunk.downloaded != expected) {
        return false;
    }

    chunk.is_done = true;
    true
}

fn drain_cancelled_progress(
    worker_rx: &mut mpsc::Receiver<WorkerMsg>,
    session_id: u64,
    chunks: &mut [ActiveChunk],
    range_download: bool,
    total_size: Option<u64>,
) -> bool {
    let mut valid = true;

    while let Ok(msg) = worker_rx.try_recv() {
        // ponytail: only record writes; Resume revalidates canceled completion boundaries.
        if let WorkerMsg::Progress {
            session_id: sid,
            chunk_id,
            bytes_delta,
        } = msg
            && sid == session_id
        {
            valid &= record_progress(chunks, chunk_id, bytes_delta, range_download, total_size);
        }
    }

    valid
}

async fn run_coordinator(
    mut action_rx: mpsc::Receiver<DownloadAction>,
    snapshot_tx: watch::Sender<DownloadSnapshot>,
) {
    // ponytail: defer HTTP/TLS setup until the first download, then keep connection pooling.
    let client = std::cell::LazyCell::new(HttpClient::new);

    let mut session_id: u64 = 0;
    let mut current_url = String::new();
    let mut current_filename = String::new();
    let mut current_path = PathBuf::new();
    let mut current_num_chunks = 1;
    let mut status = DownloadStatus::Idle;
    let mut file_info: Option<RemoteFileInfo> = None;
    let mut active_storage: Option<Storage> = None;
    let mut active_chunks: Vec<ActiveChunk> = Vec::new();
    let mut worker_handles: Vec<JoinHandle<()>> = Vec::new();
    let mut info_handle: Option<JoinHandle<()>> = None;

    let (worker_tx, mut worker_rx) = mpsc::channel::<WorkerMsg>(256);
    let (info_tx, mut info_rx) = mpsc::channel::<FetchInfoMsg>(8);
    let (mut cancel_tx, _) = watch::channel(false);

    let mut last_tick = Instant::now();
    let mut bytes_since_last_tick: u64 = 0;
    let mut current_speed: u64 = 0;

    let mut tick_interval = tokio::time::interval(Duration::from_millis(200));

    loop {
        tokio::select! {
            action = action_rx.recv() => {
                let Some(action) = action else {
                    break;
                };

                match action {
                    DownloadAction::Start { url, save_path, num_chunks } => {
                        let _ = cancel_tx.send(true);
                        wait_for_active_tasks(&mut info_handle, &mut worker_handles).await;
                        session_id += 1;
                        let sid = session_id;

                        let (new_cancel_tx, _) = watch::channel(false);
                        cancel_tx = new_cancel_tx;

                        current_url = url;
                        current_path = save_path;
                        current_num_chunks = num_chunks;
                        current_filename.clear();
                        status = DownloadStatus::Connecting;
                        active_chunks.clear();
                        active_storage = None;
                        file_info = None;
                        current_speed = 0;
                        bytes_since_last_tick = 0;
                        last_tick = Instant::now();

                        publish_snapshot(
                            &snapshot_tx,
                            &current_url,
                            &current_filename,
                            &current_path,
                            &status,
                            None,
                            0,
                            0,
                            None,
                            false,
                        );

                        info_handle = Some(spawn_info_fetch(
                            sid,
                            FetchInfoKind::Start {
                                save_path: current_path.clone(),
                                num_chunks: current_num_chunks,
                            },
                            current_url.clone(),
                            client.clone(),
                            cancel_tx.subscribe(),
                            info_tx.clone(),
                        ));
                    }

                    DownloadAction::Pause => {
                        if matches!(status, DownloadStatus::Connecting | DownloadStatus::Downloading) {
                            let _ = cancel_tx.send(true);
                            let range_download = file_info.as_ref().is_some_and(uses_range_workers);
                            let total_size = file_info.as_ref().and_then(|info| info.content_length);
                            wait_for_active_tasks(&mut info_handle, &mut worker_handles).await;
                            if drain_cancelled_progress(
                                &mut worker_rx,
                                session_id,
                                &mut active_chunks,
                                range_download,
                                total_size,
                            ) {
                                status = DownloadStatus::Paused;
                                current_speed = 0;
                                bytes_since_last_tick = 0;
                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    total_size,
                                    calculate_downloaded(&active_chunks),
                                    0,
                                    None,
                                    file_info.as_ref().is_none_or(is_resumable),
                                );
                            } else {
                                status = DownloadStatus::Failed(
                                    "Worker reported invalid download progress".to_string(),
                                );
                                current_speed = 0;
                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    total_size,
                                    calculate_downloaded(&active_chunks),
                                    0,
                                    None,
                                    false,
                                );
                            }
                        }
                    }

                    DownloadAction::Resume => {
                        if status == DownloadStatus::Paused {
                            wait_for_active_tasks(&mut info_handle, &mut worker_handles).await;
                            session_id += 1;
                            let sid = session_id;
                            let (new_cancel_tx, _) = watch::channel(false);
                            cancel_tx = new_cancel_tx;
                            status = DownloadStatus::Connecting;
                            current_speed = 0;
                            bytes_since_last_tick = 0;
                            last_tick = Instant::now();

                            publish_snapshot(
                                &snapshot_tx,
                                &current_url,
                                &current_filename,
                                &current_path,
                                &status,
                                file_info.as_ref().and_then(|i| i.content_length),
                                calculate_downloaded(&active_chunks),
                                0,
                                None,
                                file_info.as_ref().is_none_or(is_resumable),
                            );

                            let kind = if active_storage.is_some() {
                                FetchInfoKind::Resume
                            } else {
                                FetchInfoKind::Start {
                                    save_path: current_path.clone(),
                                    num_chunks: current_num_chunks,
                                }
                            };
                            info_handle = Some(spawn_info_fetch(
                                sid,
                                kind,
                                current_url.clone(),
                                client.clone(),
                                cancel_tx.subscribe(),
                                info_tx.clone(),
                            ));
                        }
                    }

                    DownloadAction::Cancel => {
                        let _ = cancel_tx.send(true);
                        wait_for_active_tasks(&mut info_handle, &mut worker_handles).await;
                        session_id += 1;
                        status = DownloadStatus::Idle;
                        current_speed = 0;
                        active_chunks.clear();
                        active_storage = None;
                        file_info = None;
                        publish_snapshot(
                            &snapshot_tx,
                            &current_url,
                            &current_filename,
                            &current_path,
                            &status,
                            None,
                            0,
                            0,
                            None,
                            false,
                        );
                    }
                }
            }

            info_msg = info_rx.recv() => {
                let Some(msg) = info_msg else {
                    continue;
                };

                if msg.session_id != session_id || status != DownloadStatus::Connecting {
                    continue;
                }

                info_handle = None;
                match (msg.kind, msg.result) {
                    (FetchInfoKind::Start { save_path, num_chunks }, Ok(info)) => {
                        let is_dir = save_path.is_dir()
                            || save_path.extension().is_none()
                            || save_path.to_string_lossy().ends_with('/')
                            || save_path.to_string_lossy().ends_with('\\');

                        if is_dir {
                            current_filename = info.filename.clone();
                            current_path = save_path.join(&current_filename);
                        } else {
                            current_filename = save_path
                                .file_name()
                                .and_then(|name| name.to_str())
                                .unwrap_or(&info.filename)
                                .to_string();
                            current_path = save_path;
                        }

                        let storage = match tokio::task::spawn_blocking({
                            let path = current_path.clone();
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
                                        current_path.display()
                                    )
                                }
                                _ => err.to_string(),
                            }),
                            Err(err) => Err(format!(
                                "Could not create download file {}: {err}",
                                current_path.display()
                            )),
                        };

                        match storage {
                            Ok(storage) => {
                                let total_size = info.content_length;
                                let resumable = is_resumable(&info);
                                spawn_download_workers(
                                    session_id,
                                    &current_url,
                                    &client,
                                    &info,
                                    num_chunks,
                                    &storage,
                                    &cancel_tx,
                                    &worker_tx,
                                    &mut active_chunks,
                                    &mut worker_handles,
                                );
                                active_storage = Some(storage);
                                file_info = Some(info);
                                status = DownloadStatus::Downloading;

                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    total_size,
                                    0,
                                    0,
                                    None,
                                    resumable,
                                );
                            }
                            Err(message) => {
                                status = DownloadStatus::Failed(message);
                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    None,
                                    0,
                                    0,
                                    None,
                                    false,
                                );
                            }
                        }
                    }
                    (FetchInfoKind::Resume, Ok(info)) => {
                        let Some(storage) = active_storage.as_ref() else {
                            status = DownloadStatus::Failed(
                                "Missing storage for paused download".to_string(),
                            );
                            publish_snapshot(
                                &snapshot_tx,
                                &current_url,
                                &current_filename,
                                &current_path,
                                &status,
                                file_info.as_ref().and_then(|current| current.content_length),
                                calculate_downloaded(&active_chunks),
                                0,
                                None,
                                false,
                            );
                            continue;
                        };

                        let was_resumable = file_info.as_ref().is_some_and(is_resumable);
                        let resume = match (
                            info.content_length,
                            file_info
                                .as_ref()
                                .and_then(|previous| previous.resume_validator_for(&info)),
                        ) {
                            (Some(total_size), Some(validator))
                                if uses_range_workers(&info)
                                    && resume_chunks_are_valid(&active_chunks, total_size) =>
                            {
                                Some((total_size, validator))
                            }
                            _ => None,
                        };

                        let total_size = info.content_length;
                        if was_resumable {
                            if let Some((total_size, validator)) = resume {
                                file_info = Some(info);
                                status = DownloadStatus::Downloading;
                                for chunk in &active_chunks {
                                    if !chunk.is_done {
                                        worker_handles.push(spawn_chunk_worker(
                                            session_id,
                                            chunk.range,
                                            chunk.downloaded,
                                            total_size,
                                            current_url.clone(),
                                            client.clone(),
                                            storage.clone(),
                                            Some(validator.clone()),
                                            cancel_tx.subscribe(),
                                            worker_tx.clone(),
                                        ));
                                    }
                                }

                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    Some(total_size),
                                    calculate_downloaded(&active_chunks),
                                    0,
                                    None,
                                    true,
                                );
                            } else {
                                status = DownloadStatus::Failed(
                                    "Cannot safely resume: the remote file changed or could not be validated"
                                        .to_string(),
                                );
                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    file_info.as_ref().and_then(|current| current.content_length),
                                    calculate_downloaded(&active_chunks),
                                    0,
                                    None,
                                    false,
                                );
                            }
                        } else {
                            match storage.set_len(total_size.unwrap_or(0)) {
                                Ok(()) => {
                                    spawn_download_workers(
                                        session_id,
                                        &current_url,
                                        &client,
                                        &info,
                                        current_num_chunks,
                                        storage,
                                        &cancel_tx,
                                        &worker_tx,
                                        &mut active_chunks,
                                        &mut worker_handles,
                                    );
                                    file_info = Some(info);
                                    status = DownloadStatus::Downloading;
                                    publish_snapshot(
                                        &snapshot_tx,
                                        &current_url,
                                        &current_filename,
                                        &current_path,
                                        &status,
                                        total_size,
                                        0,
                                        0,
                                        None,
                                        file_info.as_ref().is_some_and(is_resumable),
                                    );
                                }
                                Err(err) => {
                                    status = DownloadStatus::Failed(err.to_string());
                                    publish_snapshot(
                                        &snapshot_tx,
                                        &current_url,
                                        &current_filename,
                                        &current_path,
                                        &status,
                                        file_info.as_ref().and_then(|current| current.content_length),
                                        calculate_downloaded(&active_chunks),
                                        0,
                                        None,
                                        false,
                                    );
                                }
                            }
                        }
                    }
                    (FetchInfoKind::Start { .. }, Err(err)) => {
                        status = DownloadStatus::Failed(err.to_string());
                        publish_snapshot(
                            &snapshot_tx,
                            &current_url,
                            &current_filename,
                            &current_path,
                            &status,
                            None,
                            0,
                            0,
                            None,
                            false,
                        );
                    }
                    (FetchInfoKind::Resume, Err(err)) => {
                        status = DownloadStatus::Failed(err.to_string());
                        publish_snapshot(
                            &snapshot_tx,
                            &current_url,
                            &current_filename,
                            &current_path,
                            &status,
                            file_info.as_ref().and_then(|current| current.content_length),
                            calculate_downloaded(&active_chunks),
                            0,
                            None,
                            file_info.as_ref().is_some_and(is_resumable),
                        );
                    }
                }
            }

            msg = worker_rx.recv() => {
                let Some(msg) = msg else {
                    continue;
                };

                match msg {
                    WorkerMsg::Progress { session_id: sid, chunk_id, bytes_delta } => {
                        if sid != session_id || status != DownloadStatus::Downloading {
                            continue;
                        }
                        let range_download = file_info.as_ref().is_some_and(uses_range_workers);
                        let total_size = file_info.as_ref().and_then(|info| info.content_length);
                        if !record_progress(
                            &mut active_chunks,
                            chunk_id,
                            bytes_delta,
                            range_download,
                            total_size,
                        ) {
                            let _ = cancel_tx.send(true);
                            status = DownloadStatus::Failed(
                                "Worker reported invalid download progress".to_string(),
                            );
                            current_speed = 0;
                            publish_snapshot(
                                &snapshot_tx,
                                &current_url,
                                &current_filename,
                                &current_path,
                                &status,
                                total_size,
                                calculate_downloaded(&active_chunks),
                                0,
                                None,
                                false,
                            );
                            continue;
                        }
                        bytes_since_last_tick = bytes_since_last_tick.saturating_add(bytes_delta);
                    }
                    WorkerMsg::Done { session_id: sid, chunk_id } => {
                        if sid != session_id || status != DownloadStatus::Downloading {
                            continue;
                        }
                        let range_download = file_info.as_ref().is_some_and(uses_range_workers);
                        let total_size = file_info.as_ref().and_then(|info| info.content_length);
                        if !record_done(
                            &mut active_chunks,
                            chunk_id,
                            range_download,
                            total_size,
                        ) {
                            let _ = cancel_tx.send(true);
                            status = DownloadStatus::Failed(
                                "Worker completed before receiving its expected bytes".to_string(),
                            );
                            current_speed = 0;
                            publish_snapshot(
                                &snapshot_tx,
                                &current_url,
                                &current_filename,
                                &current_path,
                                &status,
                                file_info.as_ref().and_then(|info| info.content_length),
                                calculate_downloaded(&active_chunks),
                                0,
                                None,
                                false,
                            );
                            continue;
                        }

                        let all_done = !active_chunks.is_empty() && active_chunks.iter().all(|c| c.is_done);
                        if all_done {
                            if let Some(storage) = &active_storage
                                && let Err(err) = storage.sync()
                            {
                                status = DownloadStatus::Failed(format!(
                                    "Failed to flush completed download: {err}"
                                ));
                                current_speed = 0;
                                publish_snapshot(
                                    &snapshot_tx,
                                    &current_url,
                                    &current_filename,
                                    &current_path,
                                    &status,
                                    file_info.as_ref().and_then(|info| info.content_length),
                                    calculate_downloaded(&active_chunks),
                                    0,
                                    None,
                                    false,
                                );
                                continue;
                            }
                            status = DownloadStatus::Completed;
                            current_speed = 0;
                            let total = file_info.as_ref().and_then(|i| i.content_length).unwrap_or_else(|| calculate_downloaded(&active_chunks));
                            publish_snapshot(
                                &snapshot_tx,
                                &current_url,
                                &current_filename,
                                &current_path,
                                &status,
                                Some(total),
                                total,
                                0,
                                None,
                                true,
                            );
                        }
                    }
                    WorkerMsg::Error { session_id: sid, chunk_id, error } => {
                        if sid != session_id || status != DownloadStatus::Downloading {
                            continue;
                        }
                        let _ = cancel_tx.send(true);
                        status = DownloadStatus::Failed(format!("Stream #{chunk_id} error: {error}"));
                        current_speed = 0;
                        publish_snapshot(
                            &snapshot_tx,
                            &current_url,
                            &current_filename,
                            &current_path,
                            &status,
                            file_info.as_ref().and_then(|i| i.content_length),
                            calculate_downloaded(&active_chunks),
                            0,
                            None,
                            file_info.as_ref().is_some_and(is_resumable),
                        );
                    }
                }
            }

            _ = tick_interval.tick() => {
                if status == DownloadStatus::Downloading {
                    let now = Instant::now();
                    let elapsed = now.duration_since(last_tick).as_secs_f64();
                    last_tick = now;

                    if elapsed > 0.05 {
                        let instant_speed = (bytes_since_last_tick as f64 / elapsed) as u64;
                        bytes_since_last_tick = 0;

                        // Exponential moving average for smooth speed display
                        current_speed = if current_speed == 0 {
                            instant_speed
                        } else {
                            ((current_speed as f64 * 0.7) + (instant_speed as f64 * 0.3)) as u64
                        };
                    }

                    let downloaded = calculate_downloaded(&active_chunks);
                    let total_bytes = file_info.as_ref().and_then(|i| i.content_length);

                    let eta_seconds = if let Some(total) = total_bytes {
                        if current_speed > 0 && total > downloaded {
                            Some((total - downloaded) / current_speed)
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    let resumable = file_info.as_ref().is_some_and(is_resumable);

                    publish_snapshot(
                        &snapshot_tx,
                        &current_url,
                        &current_filename,
                        &current_path,
                        &status,
                        total_bytes,
                        downloaded,
                        current_speed,
                        eta_seconds,
                        resumable,
                    );
                }
            }
        }
    }
}

fn calculate_downloaded(chunks: &[ActiveChunk]) -> u64 {
    chunks.iter().map(|c| c.downloaded).sum()
}

#[allow(clippy::too_many_arguments)]
fn publish_snapshot(
    snapshot_tx: &watch::Sender<DownloadSnapshot>,
    url: &str,
    filename: &str,
    save_path: &std::path::Path,
    status: &DownloadStatus,
    total_bytes: Option<u64>,
    downloaded_bytes: u64,
    speed_bytes_per_sec: u64,
    eta_seconds: Option<u64>,
    resumable: bool,
) {
    let snapshot = DownloadSnapshot {
        url: url.to_string(),
        filename: filename.to_string(),
        save_path: save_path.to_path_buf(),
        status: status.clone(),
        total_bytes,
        downloaded_bytes,
        speed_bytes_per_sec,
        eta_seconds,
        resumable,
    };

    let _ = snapshot_tx.send(snapshot);
}
