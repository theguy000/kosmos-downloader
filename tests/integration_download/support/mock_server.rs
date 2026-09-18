use super::http::{is_head_request, start_local_server};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

pub(crate) async fn start_mock_server(payload: Vec<u8>) -> SocketAddr {
    start_mock_server_with_head_delay(payload, Duration::ZERO).await
}

pub(crate) async fn start_mock_server_with_head_delay(
    payload: Vec<u8>,
    head_delay: Duration,
) -> SocketAddr {
    start_local_server(move |mut socket, request| {
        let payload = payload.clone();
        async move {
            let is_head = is_head_request(&request);

            let range_header = request
                .lines()
                .find(|line| line.to_ascii_lowercase().starts_with("range:"));

            if is_head {
                tokio::time::sleep(head_delay).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Length: {}\r\n\
                     Accept-Ranges: bytes\r\n\
                     ETag: \"payload-v1\"\r\n\
                     Content-Disposition: attachment; filename=\"payload.bin\"\r\n\
                     Connection: close\r\n\r\n",
                    payload.len()
                );
                let _ = socket.write_all(resp.as_bytes()).await;
            } else if let Some(range_line) = range_header {
                let range_part = range_line.split('=').nth(1).unwrap_or("").trim();
                let mut parts = range_part.split('-');
                let start: usize = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
                let end: usize = parts
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(payload.len() - 1);
                let end = end.min(payload.len() - 1);

                let slice = &payload[start..=end];
                let content_length = slice.len();

                let resp_header = format!(
                    "HTTP/1.1 206 Partial Content\r\n\
                     Content-Range: bytes {start}-{end}/{}\r\n\
                     Content-Length: {content_length}\r\n\
                     ETag: \"payload-v1\"\r\n\
                     Connection: close\r\n\r\n",
                    payload.len()
                );

                let _ = socket.write_all(resp_header.as_bytes()).await;

                // Write in chunks to allow pause simulation
                for chunk in slice.chunks(2048) {
                    if socket.write_all(chunk).await.is_err() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            } else {
                let resp = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Length: {}\r\n\
                     Accept-Ranges: bytes\r\n\
                     ETag: \"payload-v1\"\r\n\
                     Connection: close\r\n\r\n",
                    payload.len()
                );
                let _ = socket.write_all(resp.as_bytes()).await;
                let _ = socket.write_all(&payload).await;
            }
        }
    })
    .await
}

pub(crate) async fn start_non_range_mock_server(payload: Vec<u8>) -> SocketAddr {
    start_local_server(move |mut socket, request| {
        let payload = payload.clone();
        async move {
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\r\n",
                payload.len()
            );
            if socket.write_all(response.as_bytes()).await.is_err() || is_head_request(&request) {
                return;
            }

            for chunk in payload.chunks(2048) {
                if socket.write_all(chunk).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    })
    .await
}
