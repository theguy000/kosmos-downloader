use crate::client::ClientError;
use crate::storage::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(in crate::engine) enum WorkerError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("{0}")]
    ResponseBody(#[from] reqwest::Error),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("Blocking storage task failed: {0}")]
    StorageTask(#[from] tokio::task::JoinError),
    #[error("{0}")]
    InvalidRange(&'static str),
    #[error("Range response exceeded its expected {expected} bytes")]
    RangeResponseOverrun { expected: u64 },
    #[error("Response exceeded its expected {expected} bytes")]
    ResponseOverrun { expected: u64 },
    #[error("Unexpected EOF: received {received} of {expected} bytes")]
    UnexpectedEof { received: u64, expected: u64 },
}

impl WorkerError {
    pub(in crate::engine) fn is_content_changed(&self) -> bool {
        matches!(self, Self::Client(ClientError::ContentChanged))
    }

    pub(in crate::engine) fn is_retryable(&self) -> bool {
        match self {
            Self::Client(ClientError::Http(error)) | Self::ResponseBody(error) => {
                is_retryable_body_error(error)
            }
            Self::Client(error) => is_retryable_client_error(error),
            Self::UnexpectedEof { .. } => true,
            _ => false,
        }
    }
}

fn is_retryable_request_error(error: &reqwest::Error) -> bool {
    !error.is_builder()
        && !error.is_decode()
        && (error.is_connect() || error.is_timeout() || error.is_request() || error.is_body())
}

pub(super) fn is_retryable_body_error(error: &reqwest::Error) -> bool {
    // `Response::chunk` wraps lower-level frame failures as decode errors.
    is_retryable_request_error(error) || (!error.is_builder() && error.is_decode())
}

pub(super) fn is_retryable_client_error(error: &ClientError) -> bool {
    match error {
        ClientError::Http(error) => is_retryable_request_error(error),
        ClientError::BadStatus(status, _) => {
            *status == reqwest::StatusCode::REQUEST_TIMEOUT
                || *status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status.is_server_error()
        }
        ClientError::InvalidUrl(_)
        | ClientError::InvalidRangeResponse(_)
        | ClientError::ContentChanged => false,
    }
}
