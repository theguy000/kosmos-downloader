use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Created file could not be initialized: {0}")]
    CreatedFileInitialization(#[source] std::io::Error),
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
                if exclusive {
                    file.set_len(size)
                        .map_err(StorageError::CreatedFileInitialization)?;
                } else {
                    file.set_len(size)?;
                }
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

    /// Reads exactly `data.len()` bytes at the specified byte offset.
    pub(crate) fn read_at(&self, offset: u64, data: &mut [u8]) -> Result<(), StorageError> {
        let data_len = u64::try_from(data.len()).map_err(|_| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "read length exceeds u64",
            ))
        })?;
        offset.checked_add(data_len).ok_or_else(|| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "read range overflows u64",
            ))
        })?;

        use std::os::windows::fs::FileExt;

        let mut current_offset = offset;
        let mut remaining = data;
        while !remaining.is_empty() {
            match self.file.seek_read(remaining, current_offset) {
                Ok(0) => {
                    return Err(StorageError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "unexpected EOF during offset read",
                    )));
                }
                Ok(bytes_read) => {
                    // The entire read range was checked above; reads cannot exceed it.
                    current_offset += bytes_read as u64;
                    remaining = &mut remaining[bytes_read..];
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(StorageError::Io(error)),
            }
        }
        Ok(())
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
mod tests;
