mod chunks;
mod coordinator;
mod model;
mod worker;

pub use chunks::{ChunkRange, calculate_chunks};
pub use coordinator::DownloadEngine;
pub use model::{DownloadAction, DownloadSnapshot, DownloadStatus};
