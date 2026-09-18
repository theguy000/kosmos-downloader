use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct TestRange {
    pub(crate) start: usize,
    pub(crate) end: usize,
}

impl TestRange {
    pub(crate) fn len(self) -> usize {
        self.end - self.start + 1
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ObservedRangeRequest {
    pub(crate) range: TestRange,
    pub(crate) if_range: Option<String>,
}

pub(crate) fn request_header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.lines().find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header.eq_ignore_ascii_case(name).then_some(value.trim())
    })
}

pub(crate) fn requested_byte_range(request: &str) -> Option<TestRange> {
    let value = request_header(request, "range")?.strip_prefix("bytes=")?;
    let (start, end) = value.split_once('-')?;
    let start = start.trim().parse::<usize>().ok()?;
    let end = end.trim().parse::<usize>().ok()?;
    (start <= end).then_some(TestRange { start, end })
}

pub(crate) async fn write_metadata_response(
    socket: &mut TcpStream,
    total_size: usize,
    etag: Option<&str>,
    last_modified: Option<&str>,
) -> bool {
    let etag = etag
        .map(|value| format!("ETag: {value}\r\n"))
        .unwrap_or_default();
    let last_modified = last_modified
        .map(|value| format!("Last-Modified: {value}\r\n"))
        .unwrap_or_default();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {total_size}\r\nAccept-Ranges: bytes\r\n{etag}{last_modified}Connection: close\r\n\r\n"
    );
    socket.write_all(response.as_bytes()).await.is_ok()
}

pub(crate) async fn write_range_headers(
    socket: &mut TcpStream,
    range: TestRange,
    total_size: usize,
    etag: Option<&str>,
    last_modified: Option<&str>,
) -> bool {
    let etag = etag
        .map(|value| format!("ETag: {value}\r\n"))
        .unwrap_or_default();
    let last_modified = last_modified
        .map(|value| format!("Last-Modified: {value}\r\n"))
        .unwrap_or_default();
    let response = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{total_size}\r\nContent-Length: {}\r\n{etag}{last_modified}Connection: close\r\n\r\n",
        range.start,
        range.end,
        range.len()
    );
    socket.write_all(response.as_bytes()).await.is_ok()
}

pub(crate) async fn write_range_response(
    socket: &mut TcpStream,
    payload: &[u8],
    range: TestRange,
    etag: Option<&str>,
    last_modified: Option<&str>,
) -> bool {
    write_range_headers(socket, range, payload.len(), etag, last_modified).await
        && socket
            .write_all(&payload[range.start..=range.end])
            .await
            .is_ok()
}

pub(crate) async fn wait_until_released(release_rx: &mut watch::Receiver<bool>) {
    let _ = release_rx.wait_for(|released| *released).await;
}

pub(crate) async fn start_local_server<H, F>(handler: H) -> SocketAddr
where
    H: Fn(TcpStream, String) -> F + Send + Sync + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handler = Arc::new(handler);

    tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => break,
            };

            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let mut len = 0;
                while !buf[..len].windows(4).any(|window| window == b"\r\n\r\n") {
                    match socket.read(&mut buf[len..]).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => len += n,
                    }
                }
                handler(socket, String::from_utf8_lossy(&buf[..len]).into_owned()).await;
            });
        }
    });

    addr
}

pub(crate) fn is_head_request(request: &str) -> bool {
    request
        .lines()
        .next()
        .is_some_and(|line| line.starts_with("HEAD"))
}
