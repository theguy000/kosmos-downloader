use super::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_TEMP_FILE: AtomicUsize = AtomicUsize::new(0);

struct TempFile {
    path: PathBuf,
}

impl TempFile {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "kosmos-storage-{name}-{}-{}",
            std::process::id(),
            NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

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

#[test]
fn test_storage_read_at_reads_requested_offset() {
    let temp_file = TempFile::new("read-at");
    std::fs::write(temp_file.path(), b"0123456789").unwrap();
    let storage = Storage::create_or_open(temp_file.path(), None, false).unwrap();

    let mut data = [0; 4];
    storage.read_at(3, &mut data).unwrap();

    assert_eq!(&data, b"3456");
}

#[test]
fn test_storage_read_at_reports_unexpected_eof() {
    let temp_file = TempFile::new("read-at-eof");
    std::fs::write(temp_file.path(), b"abc").unwrap();
    let storage = Storage::create_or_open(temp_file.path(), None, false).unwrap();

    let mut data = [0; 2];
    let error = storage.read_at(2, &mut data).unwrap_err();

    assert!(matches!(
        error,
        StorageError::Io(error) if error.kind() == std::io::ErrorKind::UnexpectedEof
    ));
}

#[test]
fn test_storage_read_at_rejects_offset_overflow() {
    let temp_file = TempFile::new("read-at-overflow");
    let storage = Storage::create_or_open(temp_file.path(), None, true).unwrap();

    let mut data = [0];
    let error = storage.read_at(u64::MAX, &mut data).unwrap_err();

    assert!(matches!(
        error,
        StorageError::Io(error) if error.kind() == std::io::ErrorKind::InvalidInput
    ));
}
