use std::path::PathBuf;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum DownloadStatus {
    #[default]
    Idle,
    Connecting,
    Downloading,
    Paused,
    Completed,
    Failed(String),
}

#[derive(Debug, Clone, Default)]
pub struct DownloadSnapshot {
    pub url: String,
    pub filename: String,
    pub save_path: PathBuf,
    pub status: DownloadStatus,
    pub total_bytes: Option<u64>,
    pub downloaded_bytes: u64,
    pub speed_bytes_per_sec: u64,
    pub eta_seconds: Option<u64>,
    pub resumable: bool,
}

#[derive(Debug)]
pub enum DownloadAction {
    Start {
        url: String,
        save_path: PathBuf,
        num_chunks: usize,
    },
    Pause,
    Resume,
    Cancel,
}
