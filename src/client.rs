mod filename;
mod metadata;
mod range;

use std::time::Duration;
use thiserror::Error;

pub use filename::extract_filename;
pub use metadata::RemoteFileInfo;
pub(crate) use range::is_strong_etag;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("HTTP request error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),
    #[error("Server returned status {0}: {1}")]
    BadStatus(reqwest::StatusCode, String),
    #[error("Invalid range response: {0}")]
    InvalidRangeResponse(String),
    #[error("Remote file content changed")]
    ContentChanged,
}

#[derive(Clone)]
pub struct HttpClient {
    client: reqwest::Client,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .http1_only()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::limited(10))
            .user_agent("KosmosDownloader/1.0")
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self { client }
    }
}
