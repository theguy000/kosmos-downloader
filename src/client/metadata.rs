use super::filename::extract_filename;
use super::{ClientError, HttpClient, is_strong_etag};
use reqwest::header::{
    ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, ETAG, LAST_MODIFIED, RANGE,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFileInfo {
    pub content_length: Option<u64>,
    pub accepts_ranges: bool,
    pub filename: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

impl RemoteFileInfo {
    fn from_headers(url: &str, headers: &reqwest::header::HeaderMap) -> Self {
        Self {
            content_length: headers
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok()),
            accepts_ranges: headers
                .get(ACCEPT_RANGES)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.to_ascii_lowercase().contains("bytes")),
            filename: extract_filename(
                url,
                headers
                    .get(CONTENT_DISPOSITION)
                    .and_then(|value| value.to_str().ok()),
            ),
            etag: headers
                .get(ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            last_modified: headers
                .get(LAST_MODIFIED)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        }
    }

    pub(crate) fn resume_validator(&self) -> Option<&str> {
        self.etag
            .as_deref()
            .filter(|etag| is_strong_etag(etag))
            .or_else(|| {
                self.last_modified
                    .as_deref()
                    .filter(|value| !value.is_empty())
            })
    }

    /// Metadata agreement permits byte verification; dates alone do not prove identity.
    pub(crate) fn resume_metadata_matches(&self, latest: &Self) -> bool {
        self.content_length == latest.content_length
            && self.etag == latest.etag
            && (latest.etag.as_deref().is_some_and(is_strong_etag)
                || self.last_modified == latest.last_modified)
    }
}

impl HttpClient {
    /// Queries server metadata (Content-Length, Range support, suggested filename).
    /// Uses HEAD with fallback to a single-byte GET probe if HEAD is unsupported.
    pub async fn fetch_info(&self, url: &str) -> Result<RemoteFileInfo, ClientError> {
        let parsed_url =
            reqwest::Url::parse(url).map_err(|e| ClientError::InvalidUrl(e.to_string()))?;

        // 1. Try HEAD request first
        let head_result = self.client.head(parsed_url.clone()).send().await;

        if let Ok(response) = head_result
            && response.status().is_success()
        {
            let mut info = RemoteFileInfo::from_headers(url, response.headers());

            // If Accept-Ranges is not explicitly stated in HEAD, probe with Range: bytes=0-0
            if !info.accepts_ranges
                && info.content_length.is_some()
                && let Ok(probe_resp) = self
                    .client
                    .get(parsed_url.clone())
                    .header(RANGE, "bytes=0-0")
                    .send()
                    .await
                && probe_resp.status() == reqwest::StatusCode::PARTIAL_CONTENT
            {
                info.accepts_ranges = true;
            }

            return Ok(info);
        }

        // 2. Fallback to GET with Range probe (bytes=0-0)
        let get_probe = self
            .client
            .get(parsed_url)
            .header(RANGE, "bytes=0-0")
            .send()
            .await?;

        let status = get_probe.status();
        if !status.is_success() {
            return Err(ClientError::BadStatus(
                status,
                format!("HTTP probe failed with status: {status}"),
            ));
        }

        let headers = get_probe.headers();
        let mut info = RemoteFileInfo::from_headers(url, headers);
        info.accepts_ranges = status == reqwest::StatusCode::PARTIAL_CONTENT;
        if info.accepts_ranges {
            info.content_length = super::range::parse_content_range_total(
                headers.get(CONTENT_RANGE).and_then(|v| v.to_str().ok()),
            )
            .or(info.content_length);
        }

        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_headers_preserve_fields_and_handle_missing_values() {
        use reqwest::header::{HeaderMap, HeaderValue};

        let url = "https://example.com/fallback.bin";
        let mut headers = HeaderMap::new();
        assert_eq!(
            RemoteFileInfo::from_headers(url, &headers),
            RemoteFileInfo {
                content_length: None,
                accepts_ranges: false,
                filename: "fallback.bin".into(),
                etag: None,
                last_modified: None,
            }
        );

        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("64"));
        headers.insert(ACCEPT_RANGES, HeaderValue::from_static("Bytes"));
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment; filename=report.zip"),
        );
        headers.insert(ETAG, HeaderValue::from_static("\"v1\""));
        headers.insert(
            LAST_MODIFIED,
            HeaderValue::from_static("Wed, 16 Sep 2026 12:00:00 GMT"),
        );
        assert_eq!(
            RemoteFileInfo::from_headers(url, &headers),
            RemoteFileInfo {
                content_length: Some(64),
                accepts_ranges: true,
                filename: "report.zip".into(),
                etag: Some("\"v1\"".into()),
                last_modified: Some("Wed, 16 Sep 2026 12:00:00 GMT".into()),
            }
        );

        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("invalid"));
        headers.insert(ACCEPT_RANGES, HeaderValue::from_static("none"));
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_bytes(b"\xff").unwrap(),
        );
        headers.insert(ETAG, HeaderValue::from_bytes(b"\xff").unwrap());
        headers.insert(LAST_MODIFIED, HeaderValue::from_bytes(b"\xff").unwrap());
        let info = RemoteFileInfo::from_headers(url, &headers);
        assert_eq!(info.content_length, None);
        assert!(!info.accepts_ranges);
        assert_eq!(info.filename, "fallback.bin");
        assert_eq!(info.etag, None);
        assert_eq!(info.last_modified, None);

        for (value, expected) in [
            ("0", Some(0)),
            ("18446744073709551615", Some(u64::MAX)),
            ("18446744073709551616", None),
        ] {
            headers.insert(CONTENT_LENGTH, HeaderValue::from_static(value));
            assert_eq!(
                RemoteFileInfo::from_headers(url, &headers).content_length,
                expected
            );
        }
    }

    #[test]
    fn resume_requires_matching_metadata_before_byte_verification() {
        let mut previous = RemoteFileInfo {
            content_length: Some(8),
            accepts_ranges: true,
            filename: "file.bin".into(),
            etag: Some("\"v1\"".into()),
            last_modified: None,
        };
        assert!(previous.resume_metadata_matches(&previous));
        let mut latest = previous.clone();
        latest.content_length = Some(9);
        assert!(!previous.resume_metadata_matches(&latest));
        latest.content_length = Some(8);
        latest.etag = Some("\"v2\"".into());
        assert!(!previous.resume_metadata_matches(&latest));
        latest.etag = None;
        assert!(!previous.resume_metadata_matches(&latest));
        for invalid in ["", "unquoted", "W/\"v1\"", "\"unclosed"] {
            previous.etag = Some(invalid.into());
            assert!(previous.resume_metadata_matches(&previous), "{invalid}");
        }
        previous.etag = None;
        assert!(previous.resume_metadata_matches(&previous));
        previous.last_modified = Some("Wed, 16 Sep 2026 12:00:00 GMT".into());
        assert!(previous.resume_metadata_matches(&previous));
        previous.etag = Some("W/\"v1\"".into());
        latest = previous.clone();
        latest.etag = Some("W/\"v2\"".into());
        assert!(!previous.resume_metadata_matches(&latest));
        previous.etag = None;
        latest = previous.clone();
        latest.last_modified = Some("Thu, 17 Sep 2026 12:00:00 GMT".into());
        assert!(!previous.resume_metadata_matches(&latest));
    }
}
