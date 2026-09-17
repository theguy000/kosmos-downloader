use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Zero bytes written during offset write")]
    ZeroWrite,
}

#[derive(Clone)]
pub struct Storage {
    file: Arc<std::fs::File>,
}

impl Storage {
    /// Creates a new target file without replacing an existing file.
    pub(crate) fn create_new(path: &Path, total_size: Option<u64>) -> Result<Self, StorageError> {
        Self::open(path, total_size, true, true)
    }

    /// Creates or opens a target file for direct concurrent writes.
    /// Pre-allocates the file size if `total_size` is known.
    /// When `truncate` is `true`, existing contents are discarded.
    pub fn create_or_open(
        path: &Path,
        total_size: Option<u64>,
        truncate: bool,
    ) -> Result<Self, StorageError> {
        Self::open(path, total_size, truncate, false)
    }

    fn open(
        path: &Path,
        total_size: Option<u64>,
        truncate: bool,
        exclusive: bool,
    ) -> Result<Self, StorageError> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        if exclusive {
            options.create_new(true);
        } else {
            options.create(true).truncate(truncate);
        }
        let file = options.open(path)?;

        if truncate {
            if let Some(size) = total_size
                && size > 0
            {
                file.set_len(size)?;
            }
        } else if let Some(size) = total_size {
            let meta = file.metadata()?;
            if meta.len() < size {
                file.set_len(size)?;
            }
        }

        Ok(Self {
            file: Arc::new(file),
        })
    }

    /// Writes data directly at the specified byte offset.
    /// Thread-safe and lock-free across concurrent worker threads.
    pub fn write_at(&self, mut offset: u64, mut data: &[u8]) -> Result<(), StorageError> {
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            while !data.is_empty() {
                let written = self.file.seek_write(data, offset)?;
                if written == 0 {
                    return Err(StorageError::ZeroWrite);
                }
                offset += written as u64;
                data = &data[written..];
            }
            Ok(())
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file
                .write_all_at(data, offset)
                .map_err(StorageError::Io)
        }

        #[cfg(not(any(windows, unix)))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = (&*self.file).try_clone()?;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(data)?;
            Ok(())
        }
    }

    /// Flushes all pending writes to physical disk storage.
    pub fn sync(&self) -> Result<(), StorageError> {
        self.file.sync_all().map_err(StorageError::Io)
    }

    /// Sets or truncates the file length.
    pub fn set_len(&self, len: u64) -> Result<(), StorageError> {
        self.file.set_len(len).map_err(StorageError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_preallocation() {
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("kosmos_test_prealloc.bin");
        let _ = std::fs::remove_file(&file_path);

        let size = 1024 * 1024; // 1 MB
        let storage = Storage::create_or_open(&file_path, Some(size), true);
        assert!(storage.is_ok());

        let meta = std::fs::metadata(&file_path).unwrap();
        assert_eq!(meta.len(), size);

        let _ = std::fs::remove_file(&file_path);
    }

    #[test]
    fn test_storage_concurrent_offset_writes() {
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("kosmos_test_concurrent_writes.bin");
        let _ = std::fs::remove_file(&file_path);

        let total_size = 400;
        let storage = Storage::create_or_open(&file_path, Some(total_size), true).unwrap();

        let mut handles = Vec::new();
        for i in 0..4 {
            let s = storage.clone();
            let handle = std::thread::spawn(move || {
                let offset = i * 100;
                let data = vec![b'0' + i as u8; 100];
                s.write_at(offset, &data).unwrap();
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        storage.sync().unwrap();

        let buf = std::fs::read(&file_path).unwrap();

        assert_eq!(buf.len(), 400);
        assert_eq!(&buf[0..100], &[b'0'; 100]);
        assert_eq!(&buf[100..200], &[b'1'; 100]);
        assert_eq!(&buf[200..300], &[b'2'; 100]);
        assert_eq!(&buf[300..400], &[b'3'; 100]);

        let _ = std::fs::remove_file(&file_path);
    }

    #[test]
    fn test_storage_open_without_truncation_preserves_and_extends() {
        let file_path = std::env::temp_dir().join(format!(
            "kosmos_test_open_existing_{}.bin",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&file_path);
        let original = b"existing contents";
        std::fs::write(&file_path, original).unwrap();

        let storage = Storage::create_or_open(&file_path, Some(4), false).unwrap();
        drop(storage);
        assert_eq!(std::fs::read(&file_path).unwrap(), original);

        let extended_size = original.len() as u64 + 8;
        let storage = Storage::create_or_open(&file_path, Some(extended_size), false).unwrap();
        drop(storage);
        let bytes = std::fs::read(&file_path).unwrap();
        assert_eq!(&bytes[..original.len()], original);
        assert_eq!(bytes.len() as u64, extended_size);

        let _ = std::fs::remove_file(&file_path);
    }
}
