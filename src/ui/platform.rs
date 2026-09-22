use std::path::{Path, PathBuf};

/// Opens `path` with the system handler, ignoring files that were moved or removed.
pub(super) fn open_file(path: &Path) {
    if !path.exists() {
        return;
    }

    #[cfg(windows)]
    {
        if let Some(path_str) = path.to_str() {
            let _ = std::process::Command::new("cmd")
                .args(["/c", "start", "", path_str])
                .spawn();
        }
    }
    #[cfg(unix)]
    {
        let _ = std::process::Command::new("xdg-open").arg(path).spawn();
    }
}

pub(super) fn default_download_directory() -> PathBuf {
    #[cfg(windows)]
    {
        if let Ok(userprofile) = std::env::var("USERPROFILE") {
            let p = PathBuf::from(userprofile).join("Downloads");
            if p.exists() {
                return p;
            }
        }
    }
    #[cfg(unix)]
    {
        if let Ok(home) = std::env::var("HOME") {
            let p = PathBuf::from(home).join("Downloads");
            if p.exists() {
                return p;
            }
        }
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}
