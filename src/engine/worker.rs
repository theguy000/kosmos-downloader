mod body;
mod error;
mod protocol;
mod range;
mod stream;

pub(super) const OVERLAP_BYTES: u64 = 4096;

pub(super) use error::WorkerError;
pub(super) use protocol::WorkerMsg;
pub(super) use range::spawn_chunk_worker;
pub(super) use stream::spawn_stream_worker;

#[cfg(test)]
mod tests;
