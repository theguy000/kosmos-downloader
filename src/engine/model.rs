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

/// Resolution for a download that collides with an existing file or link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplicateChoice {
    /// Keep the existing file and save the new copy as `name_N`.
    Numbered,
    /// Save the new copy over the existing file, discarding its contents.
    Overwrite,
    /// Keep or resume the existing file instead of adding a copy.
    UseExisting,
}

/// Duplicate decision the user still has to answer before a copy is created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicatePrompt {
    pub session_id: u64,
    pub url: String,
    /// Name of the file that already exists, when the engine knows it yet.
    pub filename: String,
    /// Size of the existing file, when it is already on disk.
    pub existing_bytes: Option<u64>,
    /// True when this link is already an entry in the download list.
    pub link_duplicate: bool,
}

#[derive(Debug, Clone, Default)]
pub struct DownloadSnapshot {
    pub session_id: u64,
    pub url: String,
    pub filename: String,
    pub save_path: PathBuf,
    pub status: DownloadStatus,
    pub total_bytes: Option<u64>,
    pub downloaded_bytes: u64,
    pub speed_bytes_per_sec: u64,
    pub eta_seconds: Option<u64>,
    pub resumable: bool,
    /// Set while the engine waits for a duplicate decision from the user.
    pub duplicate: Option<DuplicatePrompt>,
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
    /// Remembers the duplicate decision the engine applies without asking again.
    ///
    /// TODO: add this setting to the Options screen; it is currently only set from
    /// the duplicate dialog's "Remember my selection" checkbox.
    SetDuplicatePreference {
        choice: Option<DuplicateChoice>,
    },
    /// Answers a pending duplicate prompt; `None` skips the new duplicate.
    ResolveDuplicate {
        session_id: u64,
        choice: Option<DuplicateChoice>,
    },
    Remove {
        expected_session_id: u64,
        expected_status: DownloadStatus,
        delete_file: bool,
        completed_only: bool,
    },
}
