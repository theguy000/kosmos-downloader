use super::{ClientError, HttpClient};
use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, ETAG, IF_RANGE, LAST_MODIFIED, RANGE};

impl HttpClient {
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
pub(super) fn parse_content_range_total(content_range: Option<&str>) -> Option<u64> {
    parse_content_range(content_range?)?.total
}

#[cfg(test)]
mod tests {
    use super::{parse_content_range_total, validate_range_response};
    use crate::client::{ClientError, HttpClient};
    use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, ETAG, LAST_MODIFIED};

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
    fn changed_range_total_takes_priority_over_shortened_end() {
        let mut headers = reqwest::header::HeaderMap::new();
        for content_range in ["bytes 0-79/80", "bytes 0-99/120"] {
            headers.insert(CONTENT_RANGE, content_range.parse().unwrap());
            assert!(matches!(
                validate_range_response(&headers, 0, Some(99), Some(100), None),
                Err(ClientError::ContentChanged)
            ));
        }
        for content_range in ["bytes 0-79/100", "bytes 0-79/*"] {
            headers.insert(CONTENT_RANGE, content_range.parse().unwrap());
            assert!(matches!(
                validate_range_response(&headers, 0, Some(99), Some(100), None),
                Err(ClientError::InvalidRangeResponse(_))
            ));
        }
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
