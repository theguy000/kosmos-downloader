use reqwest::header::{
    ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, ETAG, IF_RANGE,
    LAST_MODIFIED, RANGE,
};
use std::time::Duration;
use thiserror::Error;

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
            info.content_length =
                parse_content_range_total(headers.get(CONTENT_RANGE).and_then(|v| v.to_str().ok()))
                    .or(info.content_length);
        }

        Ok(info)
    }

    /// Sends a GET request for a specific byte range.
    pub async fn download_range(
        &self,
        url: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<reqwest::Response, ClientError> {
        self.download_range_checked(url, start, end, None, None)
            .await
    }

    /// Sends a GET request and validates the returned range before any bytes are written.
    pub(crate) async fn download_range_checked(
        &self,
        url: &str,
        start: u64,
        end: Option<u64>,
        expected_total: Option<u64>,
        if_range: Option<&str>,
    ) -> Result<reqwest::Response, ClientError> {
        let mut req = self.client.get(url);
        let requested_range = start > 0 || end.is_some();

        if let Some(end) = end {
            req = req.header(RANGE, format!("bytes={start}-{end}"));
        } else if start > 0 {
            req = req.header(RANGE, format!("bytes={start}-"));
        }

        // Dates are advisory: without proving their strength, do not send them as If-Range.
        if requested_range && let Some(validator) = if_range.filter(|value| is_strong_etag(value)) {
            req = req.header(IF_RANGE, validator);
        }

        let response = req.send().await?;
        let status = response.status();

        if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE
            && let Some(expected_total) = expected_total
        {
            let total = response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| {
                    let mut parts = value.split_whitespace();
                    if !parts.next()?.eq_ignore_ascii_case("bytes") {
                        return None;
                    }
                    let total = parts.next()?.strip_prefix("*/")?.parse::<u64>().ok()?;
                    parts.next().is_none().then_some(total)
                });
            if total.is_some_and(|total| total != expected_total) {
                return Err(ClientError::ContentChanged);
            }
        }
        if !status.is_success() {
            return Err(ClientError::BadStatus(
                status,
                format!("HTTP error downloading range: {status}"),
            ));
        }

        // If a sub-range was requested, server must return 206 Partial Content.
        // Returning 200 OK means the server ignored the Range header and sent from byte 0.
        if requested_range && status != reqwest::StatusCode::PARTIAL_CONTENT {
            if status == reqwest::StatusCode::OK
                && let Some(validator) = if_range.filter(|value| is_strong_etag(value))
                && response
                    .headers()
                    .get(ETAG)
                    .and_then(|value| value.to_str().ok())
                    != Some(validator)
            {
                return Err(ClientError::ContentChanged);
            }
            return Err(ClientError::BadStatus(
                status,
                format!(
                    "Server returned {status} instead of 206 Partial Content for range request"
                ),
            ));
        }

        if requested_range {
            validate_range_response(response.headers(), start, end, expected_total, if_range)?;
        }

        Ok(response)
    }

    /// Compares a bounded saved sample, validating both headers and the entire response body.
    pub(crate) async fn verify_range(
        &self,
        url: &str,
        start: u64,
        expected: &[u8],
        total_size: u64,
        validator: Option<&str>,
    ) -> Result<(), ClientError> {
        let end = u64::try_from(expected.len())
            .ok()
            .and_then(|length| length.checked_sub(1))
            .and_then(|length| start.checked_add(length))
            .ok_or_else(|| {
                ClientError::InvalidRangeResponse("invalid verification range".into())
            })?;
        let mut response = self
            .download_range_checked(url, start, Some(end), Some(total_size), validator)
            .await?;
        let mut remaining = expected;
        while let Some(bytes) = response.chunk().await? {
            if bytes.len() > remaining.len() {
                return Err(ClientError::InvalidRangeResponse(
                    "verification response exceeded its expected length".into(),
                ));
            }
            if bytes.as_ref() != &remaining[..bytes.len()] {
                return Err(ClientError::ContentChanged);
            }
            remaining = &remaining[bytes.len()..];
        }
        if !remaining.is_empty() {
            return Err(ClientError::InvalidRangeResponse(
                "verification response ended before its expected length".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct ContentRange {
    start: u64,
    end: u64,
    total: Option<u64>,
}

pub(crate) fn is_strong_etag(etag: &str) -> bool {
    let Some(opaque_tag) = etag
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    else {
        return false;
    };

    opaque_tag
        .bytes()
        .all(|byte| byte == b'!' || (b'#'..=b'~').contains(&byte) || byte >= 0x80)
}

fn validate_range_response(
    headers: &reqwest::header::HeaderMap,
    expected_start: u64,
    expected_end: Option<u64>,
    expected_total: Option<u64>,
    if_range: Option<&str>,
) -> Result<(), ClientError> {
    let content_range = headers
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range)
        .ok_or_else(|| {
            ClientError::InvalidRangeResponse(
                "missing or malformed Content-Range header".to_string(),
            )
        })?;

    if content_range.start != expected_start {
        return Err(ClientError::InvalidRangeResponse(format!(
            "range starts at {}, expected {expected_start}",
            content_range.start
        )));
    }

    if let Some(expected_end) = expected_end
        && content_range.end != expected_end
    {
        return Err(ClientError::InvalidRangeResponse(format!(
            "range ends at {}, expected {expected_end}",
            content_range.end
        )));
    }

    if let Some(expected_total) = expected_total
        && content_range.total != Some(expected_total)
    {
        if content_range.total.is_some() {
            return Err(ClientError::ContentChanged);
        }
        return Err(ClientError::InvalidRangeResponse(format!(
            "range total is {:?}, expected {expected_total}",
            content_range.total
        )));
    }

    if let Some(content_length) = headers.get(CONTENT_LENGTH) {
        let actual_length = content_length
            .to_str()
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| {
                ClientError::InvalidRangeResponse("invalid Content-Length header".to_string())
            })?;
        let expected_length = content_range
            .end
            .checked_sub(content_range.start)
            .and_then(|length| length.checked_add(1))
            .ok_or_else(|| {
                ClientError::InvalidRangeResponse("range length overflowed".to_string())
            })?;

        if actual_length != expected_length {
            return Err(ClientError::InvalidRangeResponse(format!(
                "Content-Length is {actual_length}, expected {expected_length}"
            )));
        }
    }

    if let Some(validator) = if_range.filter(|validator| is_strong_etag(validator))
        && let Some(etag) = headers.get(ETAG).and_then(|value| value.to_str().ok())
        && etag != validator
    {
        return Err(ClientError::ContentChanged);
    }

    if let Some(validator) = if_range.filter(|validator| !is_strong_etag(validator))
        && let Some(modified) = headers
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
        && modified != validator
    {
        return Err(ClientError::ContentChanged);
    }

    Ok(())
}

fn parse_content_range(content_range: &str) -> Option<ContentRange> {
    let mut parts = content_range.split_whitespace();
    if !parts.next()?.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let range_and_total = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let (range, total) = range_and_total.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let start = start.parse().ok()?;
    let end = end.parse().ok()?;
    if start > end {
        return None;
    }

    let total = if total == "*" {
        None
    } else {
        let total = total.parse().ok()?;
        if total <= end {
            return None;
        }
        Some(total)
    };

    Some(ContentRange { start, end, total })
}

/// Parses the total length from a Content-Range header: `bytes 0-0/12345` -> `Some(12345)`
fn parse_content_range_total(content_range: Option<&str>) -> Option<u64> {
    parse_content_range(content_range?)?.total
}

/// Extracts and sanitizes the suggested filename from Content-Disposition or URL path.
pub fn extract_filename(url_str: &str, content_disposition: Option<&str>) -> String {
    // 1. Try Content-Disposition header
    if let Some(disposition) = content_disposition {
        // e.g. filename*=utf-8''encoded_name.ext
        if let Some(idx) = disposition.to_ascii_lowercase().find("filename*=") {
            let value = &disposition[idx + 10..].trim();
            let raw_name = value.split(';').next().unwrap_or(value).trim();
            let name_part = if let Some(last_quote) = raw_name.rfind("''") {
                &raw_name[last_quote + 2..]
            } else {
                raw_name
            };
            let cleaned = name_part.trim_matches('"').trim();
            if !cleaned.is_empty() {
                return sanitize_filename(cleaned);
            }
        }

        // e.g. filename="name.ext" or filename=name.ext
        if let Some(idx) = disposition.to_ascii_lowercase().find("filename=") {
            let value = &disposition[idx + 9..].trim();
            let raw_name = value.split(';').next().unwrap_or(value).trim();
            let cleaned = raw_name.trim_matches('"').trim();
            if !cleaned.is_empty() {
                return sanitize_filename(cleaned);
            }
        }
    }

    // 2. Extract from URL path
    if let Ok(parsed) = reqwest::Url::parse(url_str) {
        let path = parsed.path();
        if let Some(segment) = path.split('/').rfind(|s| !s.is_empty()) {
            let cleaned = segment.trim();
            if !cleaned.is_empty() {
                return sanitize_filename(cleaned);
            }
        }
    }

    "download.bin".to_string()
}

/// Replaces characters that are illegal in file systems with underscores.
fn sanitize_filename(name: &str) -> String {
    let decoded = urlencoding_decode(name);
    let mut sanitized = String::with_capacity(decoded.len());

    for ch in decoded.chars() {
        match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => sanitized.push('_'),
            c if c.is_control() => sanitized.push('_'),
            c => sanitized.push(c),
        }
    }

    let trimmed = sanitized.trim().trim_matches('.').trim();
    if trimmed.is_empty() {
        return "download.bin".to_string();
    }

    // Guard against Windows reserved device names (CON, PRN, AUX, NUL, COM1-9, LPT1-9)
    let upper = trimmed.to_ascii_uppercase();
    let stem = upper.split('.').next().unwrap_or(&upper);
    const RESERVED: &[&str] = &[
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    if RESERVED.contains(&stem) {
        format!("_{trimmed}")
    } else {
        trimmed.to_string()
    }
}

/// Simple percent-decode helper without pulling extra dependencies.
fn urlencoding_decode(s: &str) -> String {
    let mut bytes = Vec::with_capacity(s.len());
    let mut chars = s.bytes();

    while let Some(b) = chars.next() {
        if b == b'%' {
            let h1 = chars.next();
            let h2 = chars.next();
            if let (Some(c1), Some(c2)) = (h1, h2) {
                let hex_str = [c1, c2];
                if let Ok(hex_val) = std::str::from_utf8(&hex_str)
                    && let Ok(byte) = u8::from_str_radix(hex_val, 16)
                {
                    bytes.push(byte);
                    continue;
                }
                bytes.push(b'%');
                bytes.push(c1);
                bytes.push(c2);
            } else {
                bytes.push(b'%');
                if let Some(c1) = h1 {
                    bytes.push(c1);
                }
            }
        } else {
            bytes.push(b);
        }
    }

    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn verification_validates_sample_content_and_exact_body_length() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        for (body, outcome, unsatisfied_range) in [
            ("4\r\nabcd\r\n0\r\n\r\n", "match", None),
            ("4\r\nabXd\r\n0\r\n\r\n", "changed", None),
            ("2\r\nab\r\n0\r\n\r\n", "invalid", None),
            ("5\r\nabcde\r\n0\r\n\r\n", "invalid", None),
            ("0\r\n\r\n", "changed", Some("bytes */7")),
            ("0\r\n\r\n", "status", Some("bytes */8")),
            ("0\r\n\r\n", "status", Some("bytes */invalid")),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/file", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 2048];
                let mut length = 0;
                while !request[..length].windows(4).any(|part| part == b"\r\n\r\n") {
                    let read = socket.read(&mut request[length..]).await.unwrap();
                    assert!(read > 0);
                    length += read;
                }
                assert!(
                    !String::from_utf8_lossy(&request[..length])
                        .to_ascii_lowercase()
                        .contains("\r\nif-range:"),
                    "an unproven date must not be sent as If-Range"
                );
                let status = if unsatisfied_range.is_some() {
                    "416 Range Not Satisfiable"
                } else {
                    "206 Partial Content"
                };
                let range = unsatisfied_range.unwrap_or("bytes 2-5/8");
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Range: {range}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{body}"
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            });
            let result = HttpClient::new()
                .verify_range(&url, 2, b"abcd", 8, Some("Wed, 16 Sep 2026 12:00:00 GMT"))
                .await;
            match outcome {
                "match" => assert!(result.is_ok()),
                "changed" => assert!(matches!(result, Err(ClientError::ContentChanged))),
                "status" => assert!(matches!(
                    result,
                    Err(ClientError::BadStatus(
                        reqwest::StatusCode::RANGE_NOT_SATISFIABLE,
                        _
                    ))
                )),
                _ => assert!(matches!(result, Err(ClientError::InvalidRangeResponse(_)))),
            }
            server.await.unwrap();
        }
    }

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
    fn test_extract_filename_from_url() {
        assert_eq!(
            extract_filename("https://example.com/files/archive.zip", None),
            "archive.zip"
        );
        assert_eq!(
            extract_filename("https://example.com/downloads/setup.exe?key=123", None),
            "setup.exe"
        );
        assert_eq!(
            extract_filename("https://example.com/spaced%20file%20name.pdf", None),
            "spaced file name.pdf"
        );
    }

    #[test]
    fn test_extract_filename_content_disposition() {
        let disp = "attachment; filename=\"report_2026.docx\"";
        assert_eq!(
            extract_filename("https://example.com/get", Some(disp)),
            "report_2026.docx"
        );

        let disp_unquoted = "attachment; filename=image.png; size=1234";
        assert_eq!(
            extract_filename("https://example.com/get", Some(disp_unquoted)),
            "image.png"
        );

        let disp_utf8 = "attachment; filename*=UTF-8''my%20data%20sheet.csv";
        assert_eq!(
            extract_filename("https://example.com/get", Some(disp_utf8)),
            "my data sheet.csv"
        );
    }

    #[test]
    fn test_extract_filename_sanitization() {
        let disp = "attachment; filename=\"bad:file*name?.iso\"";
        assert_eq!(
            extract_filename("https://example.com/test", Some(disp)),
            "bad_file_name_.iso"
        );
    }

    #[test]
    fn test_parse_content_range_total() {
        assert_eq!(
            parse_content_range_total(Some("bytes 0-0/10485760")),
            Some(10485760)
        );
        assert_eq!(
            parse_content_range_total(Some("bytes 100-200/5000")),
            Some(5000)
        );
        assert_eq!(parse_content_range_total(Some("bytes 0-0/*")), None);
        assert_eq!(parse_content_range_total(None), None);
    }

    #[test]
    fn test_extract_filename_trailing_slash_and_reserved_names() {
        assert_eq!(
            extract_filename("https://example.com/files/archive.tar.gz/", None),
            "archive.tar.gz"
        );
        assert_eq!(extract_filename("https://example.com/nul", None), "_nul");
        assert_eq!(
            extract_filename("https://example.com/con.txt", None),
            "_con.txt"
        );
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

    #[test]
    fn range_headers_must_match_requested_bytes() {
        use reqwest::header::HeaderMap;

        let mut headers = HeaderMap::new();
        assert!(validate_range_response(&headers, 2, Some(3), Some(8), None).is_err());
        headers.insert(CONTENT_RANGE, "bytes 2-3/8".parse().unwrap());
        headers.insert(CONTENT_LENGTH, "2".parse().unwrap());
        assert!(validate_range_response(&headers, 2, Some(3), Some(8), None).is_ok());
        headers.insert(ETAG, "\"v1\"".parse().unwrap());
        assert!(validate_range_response(&headers, 2, Some(3), Some(8), Some("\"v1\"")).is_ok());
        assert!(validate_range_response(&headers, 2, Some(3), Some(8), Some("\"v2\"")).is_err());
        let modified = "Wed, 16 Sep 2026 12:00:00 GMT";
        headers.insert(LAST_MODIFIED, modified.parse().unwrap());
        assert!(validate_range_response(&headers, 2, Some(3), Some(8), Some(modified)).is_ok());
        assert!(
            validate_range_response(
                &headers,
                2,
                Some(3),
                Some(8),
                Some("Thu, 17 Sep 2026 12:00:00 GMT")
            )
            .is_err()
        );
        for invalid in [
            "items 2-3/8",
            "bytes 1-3/8",
            "bytes 2-4/8",
            "bytes 2-3/9",
            "bytes 2-3/*",
            "bytes 3-2/8",
            "bytes 2-3/3",
            "bytes 2-3/8 extra",
        ] {
            headers.insert(CONTENT_RANGE, invalid.parse().unwrap());
            assert!(
                validate_range_response(&headers, 2, Some(3), Some(8), None).is_err(),
                "{invalid}"
            );
        }
        headers.insert(CONTENT_RANGE, "bytes 2-3/8".parse().unwrap());
        for invalid in ["1", "3", "invalid"] {
            headers.insert(CONTENT_LENGTH, invalid.parse().unwrap());
            assert!(validate_range_response(&headers, 2, Some(3), Some(8), None).is_err());
        }
        headers.insert(
            CONTENT_RANGE,
            "bytes 0-18446744073709551615/*".parse().unwrap(),
        );
        headers.insert(CONTENT_LENGTH, "0".parse().unwrap());
        assert!(validate_range_response(&headers, 0, Some(u64::MAX), None, None).is_err());
    }
}
