//! Persisted layout settings for the download table.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const SETTINGS_FILE_NAME: &str = "table_settings.json";

// ponytail: mirrors the drag minimums in components/download_table.slint; keep in sync.
const FILENAME_MIN_WIDTH: f32 = 80.0;
const COLUMN_MIN_WIDTH: f32 = 50.0;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub(crate) struct TableColumnWidths {
    pub(crate) filename: f32,
    pub(crate) size: f32,
    pub(crate) status: f32,
    pub(crate) time_left: f32,
    pub(crate) transfer_rate: f32,
    pub(crate) date_added: f32,
}

impl TableColumnWidths {
    pub(crate) fn load() -> Option<Self> {
        Self::load_from(&settings_path())
    }

    pub(crate) fn load_from(path: &Path) -> Option<Self> {
        let text = std::fs::read_to_string(path).ok()?;
        let mut widths: Self = serde_json::from_str(&text).ok()?;
        widths.clamp_to_minimums();
        Some(widths)
    }

    fn clamp_to_minimums(&mut self) {
        self.filename = self.filename.max(FILENAME_MIN_WIDTH);
        self.size = self.size.max(COLUMN_MIN_WIDTH);
        self.status = self.status.max(COLUMN_MIN_WIDTH);
        self.time_left = self.time_left.max(COLUMN_MIN_WIDTH);
        self.transfer_rate = self.transfer_rate.max(COLUMN_MIN_WIDTH);
        self.date_added = self.date_added.max(COLUMN_MIN_WIDTH);
    }

    pub(crate) fn save(&self) -> std::io::Result<()> {
        self.save_to(&settings_path())
    }

    pub(crate) fn save_to(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self).map_err(std::io::Error::other)?;
        let mut temp = path.as_os_str().to_os_string();
        temp.push(".tmp");
        let temp_path = PathBuf::from(temp);
        std::fs::write(&temp_path, json.as_bytes())?;
        if let Err(error) = std::fs::rename(&temp_path, path) {
            // Best-effort cleanup; the rename error is the one worth reporting.
            let _ = std::fs::remove_file(&temp_path);
            return Err(error);
        }
        Ok(())
    }
}

fn settings_path() -> PathBuf {
    crate::history::data_directory().join(SETTINGS_FILE_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_save_and_load_round_trip() {
        let dir = std::env::temp_dir().join(format!("kosmos_test_{}", std::process::id()));
        let path = dir.join("table_settings.json");

        let original = TableColumnWidths {
            filename: 350.0,
            size: 140.0,
            status: 110.0,
            time_left: 95.0,
            transfer_rate: 105.0,
            date_added: 130.0,
        };

        assert!(original.save_to(&path).is_ok());
        let loaded = TableColumnWidths::load_from(&path);
        assert_eq!(loaded, Some(original));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn test_load_clamps_degenerate_widths() {
        let dir = std::env::temp_dir().join(format!("kosmos_clamp_test_{}", std::process::id()));
        let path = dir.join("table_settings.json");
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(
            &path,
            br#"{"filename":0.0,"size":-40.0,"status":0.0,"time_left":-1.0,"transfer_rate":0.0,"date_added":-999.0}"#,
        );

        let loaded = TableColumnWidths::load_from(&path).expect("valid json loads");
        let widths = [
            loaded.filename,
            loaded.size,
            loaded.status,
            loaded.time_left,
            loaded.transfer_rate,
            loaded.date_added,
        ];
        assert_eq!(widths[0], FILENAME_MIN_WIDTH);
        assert!(widths[1..].iter().all(|width| *width == COLUMN_MIN_WIDTH));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn test_load_non_existent_returns_none() {
        let path = PathBuf::from("non_existent_settings_file_path_12345.json");
        assert_eq!(TableColumnWidths::load_from(&path), None);
    }

    #[test]
    fn test_load_corrupted_json_returns_none() {
        let dir = std::env::temp_dir().join(format!("kosmos_corrupt_test_{}", std::process::id()));
        let path = dir.join("table_settings.json");
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(&path, b"not valid json {{{");

        assert_eq!(TableColumnWidths::load_from(&path), None);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }
}
