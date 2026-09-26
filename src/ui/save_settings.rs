//! Persisted settings for download destination directories and categories.

use super::platform::default_download_directory;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const SETTINGS_FILE_NAME: &str = "save_settings.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(i32)]
pub enum Category {
    General = 0,
    Compressed = 1,
    Documents = 2,
    Music = 3,
    Programs = 4,
    Video = 5,
    Images = 6,
    Ebooks = 7,
    SourceCode = 8,
    DiskImages = 9,
    Torrents = 10,
    Databases = 11,
}

impl Category {
    pub const ALL: [Self; 12] = [
        Self::General,
        Self::Compressed,
        Self::Documents,
        Self::Music,
        Self::Programs,
        Self::Video,
        Self::Images,
        Self::Ebooks,
        Self::SourceCode,
        Self::DiskImages,
        Self::Torrents,
        Self::Databases,
    ];

    #[must_use]
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::General => "General",
            other => SaveSettings::category_subfolder_name(other),
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::General | Self::Documents => "doc",
            Self::Compressed => "zip",
            Self::Music => "audio",
            Self::Programs => "exe",
            Self::Video => "video",
            Self::Images => "image",
            Self::Ebooks => "ebook",
            Self::SourceCode => "code",
            Self::DiskImages => "iso",
            Self::Torrents => "torrent",
            Self::Databases => "db",
        }
    }

    #[must_use]
    pub const fn category_id(self) -> i32 {
        self as i32
    }
}

impl From<i32> for Category {
    fn from(index: i32) -> Self {
        usize::try_from(index)
            .ok()
            .and_then(|index| Self::ALL.get(index))
            .copied()
            .unwrap_or(Self::General)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SaveSettings {
    pub default_dir: PathBuf,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub category_dirs: BTreeMap<Category, PathBuf>,
    /// Extension lists that override the built-in defaults, keyed by category.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub file_types: BTreeMap<Category, Vec<String>>,
}

impl Default for SaveSettings {
    fn default() -> Self {
        Self {
            default_dir: default_download_directory(),
            category_dirs: BTreeMap::new(),
            file_types: BTreeMap::new(),
        }
    }
}

impl SaveSettings {
    #[must_use]
    pub const fn category_subfolder_name(category: Category) -> &'static str {
        match category {
            Category::General => "",
            Category::Compressed => "Compressed",
            Category::Documents => "Documents",
            Category::Music => "Music",
            Category::Programs => "Programs",
            Category::Video => "Video",
            Category::Images => "Images",
            Category::Ebooks => "Ebooks",
            Category::SourceCode => "Source Code",
            Category::DiskImages => "Disk Images",
            Category::Torrents => "Torrents",
            Category::Databases => "Databases",
        }
    }

    #[must_use]
    pub fn default_subfolder(default_dir: &Path, category: Category) -> PathBuf {
        let sub = Self::category_subfolder_name(category);
        if sub.is_empty() {
            default_dir.to_path_buf()
        } else {
            default_dir.join(sub)
        }
    }

    /// The directory a category overrides its default subfolder with, if any.
    fn custom_dir(&self, category: Category) -> Option<&Path> {
        self.category_dirs.get(&category).map(PathBuf::as_path)
    }

    /// Points a category at a custom directory, or back at its default subfolder with `None`.
    pub fn set_category_dir(&mut self, category: Category, dir: Option<PathBuf>) {
        if category == Category::General {
            return;
        }
        if let Some(dir) = dir {
            self.category_dirs.insert(category, dir);
        } else {
            self.category_dirs.remove(&category);
        }
    }

    /// Returns the effective directory for the given category.
    #[must_use]
    pub fn category_path(&self, category: Category) -> PathBuf {
        self.custom_dir(category).map_or_else(
            || Self::default_subfolder(&self.default_dir, category),
            Path::to_path_buf,
        )
    }

    /// The effective extensions for a category: the saved override, or the built-in defaults.
    #[must_use]
    pub fn extensions_for(&self, category: Category) -> Vec<String> {
        self.file_types.get(&category).map_or_else(
            || {
                Category::extensions(category)
                    .iter()
                    .map(|extension| (*extension).to_string())
                    .collect()
            },
            Clone::clone,
        )
    }

    /// Parses a comma, semicolon, or whitespace separated extension list into unique lowercase
    /// tokens without surrounding dots.
    #[must_use]
    pub fn normalize_extensions(text: &str) -> Vec<String> {
        let mut extensions: Vec<String> = Vec::new();
        for token in text.split(|character: char| {
            character == ',' || character == ';' || character.is_whitespace()
        }) {
            let extension = token.trim_matches('.').to_ascii_lowercase();
            if !extension.is_empty() && !extensions.contains(&extension) {
                extensions.push(extension);
            }
        }
        extensions
    }

    /// Returns the override to persist for a category, or `None` when the parsed list matches
    /// the built-in defaults.
    #[must_use]
    pub fn file_type_override(category: Category, text: &str) -> Option<Vec<String>> {
        let extensions = Self::normalize_extensions(text);
        let defaults = Category::extensions(category);
        let differs = extensions.len() != defaults.len()
            || extensions
                .iter()
                .zip(defaults)
                .any(|(ext, &default_ext)| ext.as_str() != default_ext);
        differs.then_some(extensions)
    }

    /// The category a filename routes to, honoring the saved file type overrides.
    #[must_use]
    pub fn category_for_filename(&self, filename: &str) -> Category {
        let extension = extension_of(filename);
        if extension.is_empty() {
            return Category::General;
        }
        Category::ALL
            .into_iter()
            .find(|category| self.has_extension(*category, extension))
            .unwrap_or(Category::General)
    }

    fn has_extension(&self, category: Category, extension: &str) -> bool {
        self.file_types.get(&category).map_or_else(
            || {
                Category::extensions(category)
                    .iter()
                    .any(|saved| saved.eq_ignore_ascii_case(extension))
            },
            |extensions| {
                extensions
                    .iter()
                    .any(|saved| saved.eq_ignore_ascii_case(extension))
            },
        )
    }

    /// Determines the save folder for a download URL.
    #[must_use]
    pub fn path_for_url(&self, url: &str) -> PathBuf {
        let filename = crate::client::extract_filename(url, None);
        let category = self.category_for_filename(&filename);
        self.category_path(category)
    }

    /// Checks if a path matches the default directory or any configured/default category path.
    #[must_use]
    pub fn is_managed_path(&self, path: &Path) -> bool {
        path == self.default_dir
            || Category::ALL.iter().any(|&category| {
                category != Category::General && path == self.category_path(category)
            })
    }

    /// Returns the directory a category overrides the default with, or `None` when it stays
    /// on its default subfolder.
    #[must_use]
    pub fn category_override(
        value: &str,
        default_dir: &Path,
        category: Category,
    ) -> Option<PathBuf> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }
        let path = PathBuf::from(trimmed);
        (path != Self::default_subfolder(default_dir, category)).then_some(path)
    }

    pub fn load() -> Self {
        Self::load_from(&settings_path()).unwrap_or_default()
    }

    pub fn load_from(path: &Path) -> Option<Self> {
        let text = std::fs::read_to_string(path).ok()?;
        let mut settings: Self = serde_json::from_str(&text).ok()?;
        if settings.default_dir.as_os_str().is_empty() {
            settings.default_dir = default_download_directory();
        }
        settings
            .category_dirs
            .retain(|_, path| !path.as_os_str().is_empty());
        Some(settings)
    }

    pub fn save(&self) -> std::io::Result<()> {
        self.save_to(&settings_path())
    }

    pub fn save_to(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self).map_err(std::io::Error::other)?;
        let temp_path = path.with_extension(format!("tmp.{}", std::process::id()));
        std::fs::write(&temp_path, json.as_bytes())?;
        if let Err(error) = std::fs::rename(&temp_path, path) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(error);
        }
        Ok(())
    }
}

fn extension_of(filename: &str) -> &str {
    Path::new(filename)
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("")
}

fn settings_path() -> PathBuf {
    crate::history::data_directory().join(SETTINGS_FILE_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_save_settings_round_trip() {
        let dir = std::env::temp_dir().join(format!("kosmos_save_test_{}", std::process::id()));
        let path = dir.join("save_settings.json");

        let original = SaveSettings {
            default_dir: PathBuf::from("D:\\Downloads"),
            category_dirs: BTreeMap::from([
                (
                    Category::Compressed,
                    PathBuf::from("D:\\Downloads\\CustomArchives"),
                ),
                (Category::Music, PathBuf::from("D:\\Music")),
            ]),
            file_types: BTreeMap::from([(Category::Video, vec!["webm".to_string()])]),
        };

        assert!(original.save_to(&path).is_ok());
        let loaded = SaveSettings::load_from(&path);
        assert_eq!(loaded, Some(original));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn test_default_subfolders() {
        let settings = SaveSettings {
            default_dir: PathBuf::from("C:\\Users\\test\\Downloads"),
            ..Default::default()
        };

        assert_eq!(
            settings.category_path(Category::General),
            PathBuf::from("C:\\Users\\test\\Downloads")
        );
        assert_eq!(
            settings.category_path(Category::Compressed),
            PathBuf::from("C:\\Users\\test\\Downloads").join("Compressed")
        );
        assert_eq!(
            settings.category_path(Category::Video),
            PathBuf::from("C:\\Users\\test\\Downloads").join("Video")
        );
    }

    #[test]
    fn test_path_for_url_routing() {
        let settings = SaveSettings {
            default_dir: PathBuf::from("/home/user/Downloads"),
            ..Default::default()
        };

        assert_eq!(
            settings.path_for_url("https://example.com/test.zip"),
            PathBuf::from("/home/user/Downloads/Compressed")
        );
        assert_eq!(
            settings.path_for_url("https://example.com/movie.mp4"),
            PathBuf::from("/home/user/Downloads/Video")
        );
        assert_eq!(
            settings.path_for_url("https://example.com/song.mp3"),
            PathBuf::from("/home/user/Downloads/Music")
        );
        assert_eq!(
            settings.path_for_url("https://example.com/doc.pdf"),
            PathBuf::from("/home/user/Downloads/Documents")
        );
        assert_eq!(
            settings.path_for_url("https://example.com/app.exe"),
            PathBuf::from("/home/user/Downloads/Programs")
        );
        assert_eq!(
            settings.path_for_url("https://example.com/unknown_file"),
            PathBuf::from("/home/user/Downloads")
        );
    }

    #[test]
    fn test_category_override_keeps_explicit_old_default_subfolder() {
        let default_dir = PathBuf::from("D:\\Downloads");

        assert_eq!(
            SaveSettings::category_override(
                "D:\\Downloads\\Compressed",
                &default_dir,
                Category::Compressed
            ),
            None,
            "The current default subfolder is not an override"
        );
        assert_eq!(
            SaveSettings::category_override(
                "C:\\Old\\Downloads\\Compressed",
                &default_dir,
                Category::Compressed
            ),
            Some(PathBuf::from("C:\\Old\\Downloads\\Compressed")),
            "A folder from an earlier default directory stays an explicit override"
        );
        assert_eq!(
            SaveSettings::category_override("   ", &default_dir, Category::Video),
            None,
            "An empty field falls back to the default subfolder"
        );
    }

    #[test]
    fn test_category_from_trait() {
        for (index, category) in Category::ALL.iter().enumerate() {
            assert_eq!(Category::from(index as i32), *category);
            assert_eq!(category.category_id(), index as i32);
            assert!(!category.as_str().is_empty());
            assert!(!category.display_name().is_empty());
        }
        assert_eq!(Category::from(99), Category::General);
        assert_eq!(Category::from(-1), Category::General);
    }

    #[test]
    fn test_is_managed_path() {
        let mut settings = SaveSettings {
            default_dir: PathBuf::from("D:\\Downloads"),
            ..Default::default()
        };
        settings.set_category_dir(
            Category::Compressed,
            Some(PathBuf::from("D:\\CustomArchives")),
        );

        // Default dir itself
        assert!(settings.is_managed_path(Path::new("D:\\Downloads")));
        // Custom directory
        assert!(settings.is_managed_path(Path::new("D:\\CustomArchives")));
        // Default subfolder for Documents
        assert!(settings.is_managed_path(Path::new("D:\\Downloads\\Documents")));
        // Default subfolder for Video
        assert!(settings.is_managed_path(Path::new("D:\\Downloads\\Video")));
        // Overridden default subfolder is not matched as custom is active
        assert!(!settings.is_managed_path(Path::new("D:\\Downloads\\Compressed")));
        // Completely unrelated path
        assert!(!settings.is_managed_path(Path::new("D:\\Other\\Folder")));
    }

    #[test]
    fn test_file_type_overrides_route_and_classify() {
        let mut settings = SaveSettings {
            default_dir: PathBuf::from("D:\\Downloads"),
            ..Default::default()
        };
        settings
            .file_types
            .insert(Category::Video, vec!["webm".into()]);

        assert_eq!(settings.category_for_filename("clip.webm"), Category::Video);
        assert_eq!(
            settings.path_for_url("https://example.com/clip.webm"),
            PathBuf::from("D:\\Downloads\\Video")
        );
        assert_eq!(
            settings.category_for_filename("movie.mp4"),
            Category::General,
            "A removed extension falls back to the default category"
        );
    }

    #[test]
    fn test_normalize_extensions() {
        assert_eq!(
            SaveSettings::normalize_extensions(" .ZIP, rar;7z  7z "),
            vec!["zip", "rar", "7z"]
        );
        assert!(SaveSettings::normalize_extensions(" , ; ").is_empty());
    }

    #[test]
    fn test_file_type_override_only_persists_changes() {
        let defaults = Category::extensions(Category::Video).join(", ");
        assert_eq!(
            SaveSettings::file_type_override(Category::Video, &defaults),
            None
        );
        assert_eq!(
            SaveSettings::file_type_override(Category::Video, "WebM .mkv"),
            Some(vec!["webm".into(), "mkv".into()])
        );
        assert_eq!(
            SaveSettings::file_type_override(Category::Music, ""),
            Some(Vec::new()),
            "Clearing the field disables the category instead of restoring defaults"
        );
    }
}
