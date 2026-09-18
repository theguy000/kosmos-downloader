use kosmos_downloader::client::{HttpClient, RemoteFileInfo};
use kosmos_downloader::engine::{DownloadAction, DownloadEngine, DownloadSnapshot, DownloadStatus};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, watch};

const TEST_DATA_SIZE: usize = 64 * 1024; // 64 KB
const DYNAMIC_DATA_SIZE: usize = 2 * 1024 * 1024;
const DYNAMIC_INITIAL_CHUNK_SIZE: usize = DYNAMIC_DATA_SIZE / 2;
const DYNAMIC_PREFIX_SIZE: usize = 128 * 1024;
const MIN_DYNAMIC_CHILD_SIZE: usize = 256 * 1024;
const DYNAMIC_ETAG: &str = "\"dynamic-v1\"";
const RETRY_ETAG: &str = "\"retry-v1\"";
const RETRY_PREFIX_SIZE: usize = 64 * 1024;

fn generate_test_payload() -> Vec<u8> {
    (0..TEST_DATA_SIZE).map(|i| (i % 251) as u8).collect()
}

fn generate_offset_payload(size: usize) -> Vec<u8> {
    (0..size)
        .map(|offset| {
            let offset = offset as u64;
            (offset.wrapping_mul(0x9e37_79b9).rotate_left(11) >> 24) as u8
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TestRange {
    start: usize,
    end: usize,
}

impl TestRange {
    fn len(self) -> usize {
        self.end - self.start + 1
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedRangeRequest {
    range: TestRange,
    if_range: Option<String>,
}

fn request_header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.lines().find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header.eq_ignore_ascii_case(name).then_some(value.trim())
    })
}

fn requested_byte_range(request: &str) -> Option<TestRange> {
    let value = request_header(request, "range")?.strip_prefix("bytes=")?;
    let (start, end) = value.split_once('-')?;
    let start = start.trim().parse::<usize>().ok()?;
    let end = end.trim().parse::<usize>().ok()?;
    (start <= end).then_some(TestRange { start, end })
}

async fn write_metadata_response(
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

async fn write_range_headers(
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

async fn write_range_response(
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

async fn write_prefix_then_drop(
    socket: &mut TcpStream,
    payload: &[u8],
    range: TestRange,
    etag: &str,
) {
    if write_range_headers(socket, range, payload.len(), Some(etag), None).await {
        let prefix_end = range.start + RETRY_PREFIX_SIZE.min(range.len());
        let _ = socket.write_all(&payload[range.start..prefix_end]).await;
        let _ = socket.flush().await;
    }
}

async fn wait_until_released(release_rx: &mut watch::Receiver<bool>) {
    let _ = release_rx.wait_for(|released| *released).await;
}

async fn start_local_server<H, F>(handler: H) -> SocketAddr
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

fn is_head_request(request: &str) -> bool {
    request
        .lines()
        .next()
        .is_some_and(|line| line.starts_with("HEAD"))
}

async fn start_mock_server(payload: Vec<u8>) -> SocketAddr {
    start_mock_server_with_head_delay(payload, Duration::ZERO).await
}

async fn start_mock_server_with_head_delay(payload: Vec<u8>, head_delay: Duration) -> SocketAddr {
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

async fn start_non_range_mock_server(payload: Vec<u8>) -> SocketAddr {
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

struct ChangingResourceServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<ObservedRangeRequest>>>,
}

async fn start_changing_resource_server(
    initial: Vec<u8>,
    updated: Vec<u8>,
) -> ChangingResourceServer {
    let head_count = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let server_requests = Arc::clone(&requests);

    let addr = start_local_server(move |mut socket, request| {
        let initial = initial.clone();
        let updated = updated.clone();
        let head_count = Arc::clone(&head_count);
        let requests = Arc::clone(&server_requests);
        async move {
            let is_head = is_head_request(&request);
            let is_initial = if is_head {
                head_count.fetch_add(1, Ordering::SeqCst) == 0
            } else {
                head_count.load(Ordering::SeqCst) <= 1
            };
            let (payload, etag) = if is_initial {
                (initial, "\"v1\"")
            } else {
                (updated, "\"v2\"")
            };

            if is_head {
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Length: {}\r\n\
                     Accept-Ranges: bytes\r\n\
                     ETag: {etag}\r\n\
                     Connection: close\r\n\r\n",
                    payload.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                return;
            }

            let range = request
                .lines()
                .find(|line| line.to_ascii_lowercase().starts_with("range:"));
            let Some(range) = range else {
                return;
            };
            let range = range.split('=').nth(1).unwrap_or("").trim();
            let mut parts = range.split('-');
            let start: usize = parts
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            let end = parts
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(payload.len() - 1)
                .min(payload.len() - 1);
            requests.lock().unwrap().push(ObservedRangeRequest {
                range: TestRange { start, end },
                if_range: request_header(&request, "if-range").map(str::to_owned),
            });
            let slice = &payload[start..=end];
            let response = format!(
                "HTTP/1.1 206 Partial Content\r\n\
                 Content-Range: bytes {start}-{end}/{}\r\n\
                 Content-Length: {}\r\n\
                 ETag: {etag}\r\n\
                 Connection: close\r\n\r\n",
                payload.len(),
                slice.len()
            );
            if socket.write_all(response.as_bytes()).await.is_err() {
                return;
            }
            for chunk in slice.chunks(2048) {
                if socket.write_all(chunk).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    })
    .await;

    ChangingResourceServer { addr, requests }
}

struct NoEtagResumeServer {
    addr: SocketAddr,
    payload: Arc<Mutex<Vec<u8>>>,
    requests: Arc<Mutex<Vec<ObservedRangeRequest>>>,
    release_stall: watch::Sender<bool>,
}

impl NoEtagResumeServer {
    fn replace_payload(&self, updated: Vec<u8>) {
        let mut payload = self.payload.lock().unwrap();
        assert_eq!(payload.len(), updated.len());
        *payload = updated;
    }

    fn release_stalled_response(&self) {
        let _ = self.release_stall.send(true);
    }
}

async fn start_no_etag_resume_server(
    payload: Vec<u8>,
    last_modified: Option<&str>,
    stalled_range: TestRange,
    stalled_prefix_len: usize,
) -> NoEtagResumeServer {
    assert!(stalled_prefix_len > 0 && stalled_prefix_len < stalled_range.len());
    assert!(stalled_range.end < payload.len());

    let payload = Arc::new(Mutex::new(payload));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let stalled_once = Arc::new(AtomicBool::new(false));
    let (release_stall, release_stall_rx) = watch::channel(false);
    let last_modified = last_modified.map(str::to_owned);
    let server_payload = Arc::clone(&payload);
    let server_requests = Arc::clone(&requests);
    let server_stalled_once = Arc::clone(&stalled_once);

    let addr = start_local_server(move |mut socket, request| {
        let payload = Arc::clone(&server_payload);
        let requests = Arc::clone(&server_requests);
        let stalled_once = Arc::clone(&server_stalled_once);
        let release_stall_rx = release_stall_rx.clone();
        let last_modified = last_modified.clone();
        async move {
            if is_head_request(&request) {
                let total_size = payload.lock().unwrap().len();
                let _ = write_metadata_response(
                    &mut socket,
                    total_size,
                    None,
                    last_modified.as_deref(),
                )
                .await;
                return;
            }

            let Some(range) = requested_byte_range(&request) else {
                return;
            };
            let response_payload = payload.lock().unwrap().clone();
            if range.end >= response_payload.len() {
                return;
            }
            requests.lock().unwrap().push(ObservedRangeRequest {
                range,
                if_range: request_header(&request, "if-range").map(str::to_owned),
            });

            if range == stalled_range && !stalled_once.swap(true, Ordering::SeqCst) {
                if write_range_headers(
                    &mut socket,
                    range,
                    response_payload.len(),
                    None,
                    last_modified.as_deref(),
                )
                .await
                {
                    let prefix_end = range.start + stalled_prefix_len;
                    if socket
                        .write_all(&response_payload[range.start..prefix_end])
                        .await
                        .is_ok()
                    {
                        let _ = socket.flush().await;
                        let mut discard = [0u8; 1];
                        let mut release_stall_rx = release_stall_rx;
                        tokio::select! {
                            _ = wait_until_released(&mut release_stall_rx) => {}
                            _ = socket.read(&mut discard) => {}
                        }
                    }
                }
            } else {
                let _ = write_range_response(
                    &mut socket,
                    &response_payload,
                    range,
                    None,
                    last_modified.as_deref(),
                )
                .await;
            }
        }
    })
    .await;

    NoEtagResumeServer {
        addr,
        payload,
        requests,
        release_stall,
    }
}

struct BetweenRangeChangeServer {
    addr: SocketAddr,
    initial_ranges_served: Arc<Notify>,
    served_ranges: Arc<Mutex<Vec<(TestRange, bool)>>>,
}

async fn start_between_range_change_server(
    initial: Vec<u8>,
    updated: Vec<u8>,
) -> BetweenRangeChangeServer {
    assert_eq!(initial.len(), updated.len());
    assert!(initial.len().is_multiple_of(2));

    let midpoint = initial.len() / 2;
    let first_range = TestRange {
        start: 0,
        end: midpoint - 1,
    };
    let second_range = TestRange {
        start: midpoint,
        end: initial.len() - 1,
    };
    let initial = Arc::new(initial);
    let updated = Arc::new(updated);
    let served_ranges = Arc::new(Mutex::new(Vec::new()));
    let initial_ranges_served = Arc::new(Notify::new());
    let served_initial_ranges = Arc::new(AtomicUsize::new(0));
    let source_changed = Arc::new(AtomicBool::new(false));
    let (switch_tx, switch_rx) = watch::channel(false);
    let server_initial = Arc::clone(&initial);
    let server_updated = Arc::clone(&updated);
    let server_served_ranges = Arc::clone(&served_ranges);
    let server_initial_ranges_served = Arc::clone(&initial_ranges_served);
    let server_served_initial_ranges = Arc::clone(&served_initial_ranges);
    let server_source_changed = Arc::clone(&source_changed);

    let addr = start_local_server(move |mut socket, request| {
        let initial = Arc::clone(&server_initial);
        let updated = Arc::clone(&server_updated);
        let served_ranges = Arc::clone(&server_served_ranges);
        let initial_ranges_served = Arc::clone(&server_initial_ranges_served);
        let served_initial_ranges = Arc::clone(&server_served_initial_ranges);
        let source_changed = Arc::clone(&server_source_changed);
        let switch_tx = switch_tx.clone();
        let switch_rx = switch_rx.clone();
        async move {
            if is_head_request(&request) {
                let _ = write_metadata_response(&mut socket, initial.len(), None, None).await;
                return;
            }

            let Some(range) = requested_byte_range(&request) else {
                return;
            };
            if range.end >= initial.len() {
                return;
            }

            let (response_written, served_updated) = if range == first_range
                && !source_changed.swap(true, Ordering::SeqCst)
            {
                let result = write_range_response(&mut socket, &initial, range, None, None).await;
                let _ = switch_tx.send(true);
                (result, false)
            } else {
                if !source_changed.load(Ordering::SeqCst) {
                    let mut switch_rx = switch_rx;
                    wait_until_released(&mut switch_rx).await;
                }
                (
                    write_range_response(&mut socket, &updated, range, None, None).await,
                    true,
                )
            };
            served_ranges.lock().unwrap().push((range, served_updated));

            if (range == first_range || range == second_range)
                && response_written
                && served_initial_ranges.fetch_add(1, Ordering::SeqCst) == 1
            {
                initial_ranges_served.notify_one();
            }
        }
    })
    .await;

    BetweenRangeChangeServer {
        addr,
        initial_ranges_served,
        served_ranges,
    }
}

struct DynamicSplitServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<ObservedRangeRequest>>>,
    child_ready: Arc<Notify>,
    child_seen: Arc<Notify>,
    donor_closed: Arc<Notify>,
    release_fast: watch::Sender<bool>,
    release_children: watch::Sender<bool>,
    max_waiting_children: Arc<AtomicUsize>,
}

async fn start_dynamic_split_server(
    payload: Vec<u8>,
    validator: Option<&str>,
) -> DynamicSplitServer {
    assert_eq!(payload.len(), DYNAMIC_DATA_SIZE);

    let payload = Arc::new(payload);
    let validator = validator.map(str::to_owned);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let child_ready = Arc::new(Notify::new());
    let child_seen = Arc::new(Notify::new());
    let donor_closed = Arc::new(Notify::new());
    let yield_late_body = Arc::new(Notify::new());
    let max_waiting_children = Arc::new(AtomicUsize::new(0));
    let waiting_children = Arc::new(AtomicUsize::new(0));
    let (release_fast, fast_release_rx) = watch::channel(false);
    let (release_children, child_release_rx) = watch::channel(false);
    let first_chunk = TestRange {
        start: 0,
        end: DYNAMIC_INITIAL_CHUNK_SIZE - 1,
    };
    let second_chunk = TestRange {
        start: DYNAMIC_INITIAL_CHUNK_SIZE,
        end: DYNAMIC_DATA_SIZE - 1,
    };
    let server_requests = Arc::clone(&requests);
    let server_child_ready = Arc::clone(&child_ready);
    let server_child_seen = Arc::clone(&child_seen);
    let server_donor_closed = Arc::clone(&donor_closed);
    let server_max_waiting_children = Arc::clone(&max_waiting_children);

    let addr = start_local_server(move |mut socket, request| {
        let payload = Arc::clone(&payload);
        let validator = validator.clone();
        let requests = Arc::clone(&server_requests);
        let child_ready = Arc::clone(&server_child_ready);
        let child_seen = Arc::clone(&server_child_seen);
        let donor_closed = Arc::clone(&server_donor_closed);
        let yield_late_body = Arc::clone(&yield_late_body);
        let max_waiting_children = Arc::clone(&server_max_waiting_children);
        let waiting_children = Arc::clone(&waiting_children);
        let fast_release_rx = fast_release_rx.clone();
        let child_release_rx = child_release_rx.clone();
        async move {
            if is_head_request(&request) {
                let _ = write_metadata_response(&mut socket, payload.len(), validator.as_deref(), None).await;
                return;
            }

            let Some(range) = requested_byte_range(&request) else {
                return;
            };
            if range.end >= payload.len() {
                return;
            }
            requests.lock().unwrap().push(ObservedRangeRequest {
                range,
                if_range: request_header(&request, "if-range").map(str::to_owned),
            });

            if range == first_chunk {
                if !write_range_headers(&mut socket, range, payload.len(), validator.as_deref(), None).await {
                    return;
                }
                if socket
                    .write_all(&payload.as_slice()[..DYNAMIC_PREFIX_SIZE])
                    .await
                    .is_err()
                {
                    return;
                }
                let _ = socket.flush().await;

                let mut discard = [0u8; 1];
                tokio::select! {
                    _ = yield_late_body.notified() => {
                        let mut corrupt = payload.as_slice()[DYNAMIC_PREFIX_SIZE..DYNAMIC_PREFIX_SIZE + 4096].to_vec();
                        for byte in &mut corrupt {
                            *byte ^= 0xff;
                        }
                        let _ = socket.write_all(&corrupt).await;
                        let _ = socket.flush().await;
                        let _ = socket.read(&mut discard).await;
                    }
                    _ = socket.read(&mut discard) => {}
                }
                donor_closed.notify_one();
            } else if range == second_chunk {
                let mut fast_release_rx = fast_release_rx;
                wait_until_released(&mut fast_release_rx).await;
                let _ = write_range_response(&mut socket, payload.as_slice(), range, validator.as_deref(), None).await;
            } else {
                child_seen.notify_one();
                yield_late_body.notify_one();
                let waiting = waiting_children.fetch_add(1, Ordering::SeqCst) + 1;
                max_waiting_children.fetch_max(waiting, Ordering::SeqCst);
                if waiting == 2 {
                    child_ready.notify_one();
                }

                let mut child_release_rx = child_release_rx;
                if write_range_headers(&mut socket, range, payload.len(), validator.as_deref(), None).await {
                    wait_until_released(&mut child_release_rx).await;
                    let _ = socket
                        .write_all(&payload.as_slice()[range.start..=range.end])
                        .await;
                }
                waiting_children.fetch_sub(1, Ordering::SeqCst);
            }
        }
    })
    .await;

    DynamicSplitServer {
        addr,
        requests,
        child_ready,
        child_seen,
        donor_closed,
        release_fast,
        release_children,
        max_waiting_children,
    }
}

#[derive(Clone, Copy)]
enum RetryBehavior {
    RecoverOnce,
    PersistentDrop,
    RejectSecond(RecoveryFault),
    AlwaysChangedEtag,
    StallThenRecover,
}

#[derive(Clone, Copy)]
enum RecoveryFault {
    ChangedEtag,
    InvalidContentRange,
    StatusOk,
    StatusOkChangedEtag,
}

struct RetryServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<ObservedRangeRequest>>>,
    allow_recovery: Arc<AtomicBool>,
}

impl RetryServer {
    fn allow_recovery(&self) {
        self.allow_recovery.store(true, Ordering::SeqCst);
    }
}

async fn start_retry_server(payload: Vec<u8>, behavior: RetryBehavior) -> RetryServer {
    let payload = Arc::new(payload);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let allow_recovery = Arc::new(AtomicBool::new(false));
    let server_requests = Arc::clone(&requests);
    let server_allow_recovery = Arc::clone(&allow_recovery);

    let addr = start_local_server(move |mut socket, request| {
        let payload = Arc::clone(&payload);
        let requests = Arc::clone(&server_requests);
        let attempts = Arc::clone(&attempts);
        let allow_recovery = Arc::clone(&server_allow_recovery);
        async move {
            if is_head_request(&request) {
                let _ = write_metadata_response(&mut socket, payload.len(), Some(RETRY_ETAG), None).await;
                return;
            }

            let Some(range) = requested_byte_range(&request) else {
                return;
            };
            if range.end >= payload.len() {
                return;
            }
            requests.lock().unwrap().push(ObservedRangeRequest {
                range,
                if_range: request_header(&request, "if-range").map(str::to_owned),
            });
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);

            match behavior {
                RetryBehavior::RecoverOnce => {
                    if attempt == 0 {
                        write_prefix_then_drop(&mut socket, payload.as_slice(), range, RETRY_ETAG).await;
                    } else {
                        let _ = write_range_response(&mut socket, payload.as_slice(), range, Some(RETRY_ETAG), None).await;
                    }
                }
                RetryBehavior::PersistentDrop => {
                    if allow_recovery.load(Ordering::SeqCst) {
                        let _ = write_range_response(
                            &mut socket,
                            payload.as_slice(),
                            range,
                            Some(RETRY_ETAG),
                            None,
                        )
                        .await;
                    } else {
                        write_prefix_then_drop(&mut socket, payload.as_slice(), range, RETRY_ETAG)
                            .await;
                    }
                }
                RetryBehavior::RejectSecond(_) if attempt == 0 => {
                    write_prefix_then_drop(&mut socket, payload.as_slice(), range, RETRY_ETAG).await;
                }
                RetryBehavior::RejectSecond(RecoveryFault::ChangedEtag) if attempt == 1 => {
                    let _ = write_range_response(&mut socket, payload.as_slice(), range, Some("\"retry-v2\""), None).await;
                }
                RetryBehavior::RejectSecond(RecoveryFault::InvalidContentRange) if attempt == 1 => {
                    let invalid_range = TestRange {
                        start: range.start + 1,
                        end: range.end,
                    };
                    let _ = write_range_response(
                        &mut socket,
                        payload.as_slice(),
                        invalid_range,
                        Some(RETRY_ETAG),
                        None,
                    )
                    .await;
                }
                RetryBehavior::RejectSecond(
                    fault @ (RecoveryFault::StatusOk | RecoveryFault::StatusOkChangedEtag),
                ) if attempt == 1 => {
                    let etag = if matches!(fault, RecoveryFault::StatusOkChangedEtag) {
                        "\"retry-v2\""
                    } else {
                        RETRY_ETAG
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: {etag}\r\nConnection: close\r\n\r\n",
                        range.len()
                    );
                    if socket.write_all(response.as_bytes()).await.is_ok() {
                        let _ = socket
                            .write_all(&payload.as_slice()[range.start..=range.end])
                            .await;
                    }
                }
                RetryBehavior::StallThenRecover if attempt == 0 => {
                    if write_range_headers(&mut socket, range, payload.len(), Some(RETRY_ETAG), None).await {
                        let prefix_end = range.start + RETRY_PREFIX_SIZE.min(range.len());
                        if socket
                            .write_all(&payload.as_slice()[range.start..prefix_end])
                            .await
                            .is_ok()
                        {
                            let _ = socket.flush().await;
                            let mut discard = [0u8; 1];
                            let _ = socket.read(&mut discard).await;
                        }
                    }
                }
                RetryBehavior::AlwaysChangedEtag => {
                    let _ = write_range_response(
                        &mut socket,
                        payload.as_slice(),
                        range,
                        Some("\"retry-v2\""),
                        None,
                    )
                    .await;
                }
                RetryBehavior::RejectSecond(_) | RetryBehavior::StallThenRecover => {
                    let _ = write_range_response(&mut socket, payload.as_slice(), range, Some(RETRY_ETAG), None).await;
                }
            }
        }
    })
    .await;

    RetryServer {
        addr,
        requests,
        allow_recovery,
    }
}

fn observed_requests(
    requests: &Arc<Mutex<Vec<ObservedRangeRequest>>>,
) -> Vec<ObservedRangeRequest> {
    requests.lock().unwrap().clone()
}

fn assert_dynamic_split_requests(requests: &[ObservedRangeRequest]) -> Vec<TestRange> {
    let first_chunk = TestRange {
        start: 0,
        end: DYNAMIC_INITIAL_CHUNK_SIZE - 1,
    };
    let second_chunk = TestRange {
        start: DYNAMIC_INITIAL_CHUNK_SIZE,
        end: DYNAMIC_DATA_SIZE - 1,
    };
    assert_eq!(requests.len(), 4, "unexpected range requests: {requests:?}");
    assert!(
        requests
            .iter()
            .all(|request| { request.if_range.as_deref() == Some(DYNAMIC_ETAG) })
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.range == first_chunk)
            .count(),
        1,
        "confirmed prefix must not be fetched again"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.range == second_chunk)
            .count(),
        1,
        "completed sibling must not be fetched again"
    );

    let mut children: Vec<_> = requests
        .iter()
        .filter(|request| {
            request.range.start >= DYNAMIC_PREFIX_SIZE
                && request.range.end < DYNAMIC_INITIAL_CHUNK_SIZE
        })
        .map(|request| request.range)
        .collect();
    assert_eq!(children.len(), 2, "expected two replacement ranges");
    children.sort_unstable();
    assert!(
        children
            .iter()
            .all(|range| range.len() >= MIN_DYNAMIC_CHILD_SIZE)
    );

    let mut next = DYNAMIC_PREFIX_SIZE;
    for range in &children {
        assert_eq!(
            range.start, next,
            "replacement ranges have a gap or overlap"
        );
        next = range.end + 1;
    }
    assert_eq!(next, DYNAMIC_INITIAL_CHUNK_SIZE);
    children
}

fn assert_retry_requests(
    requests: &[ObservedRangeRequest],
    total_size: usize,
    expected_starts: &[usize],
) {
    assert_eq!(
        requests.len(),
        expected_starts.len(),
        "unexpected retry count"
    );
    for (request, &start) in requests.iter().zip(expected_starts) {
        assert_eq!(
            request.range,
            TestRange {
                start,
                end: total_size - 1,
            }
        );
        assert_eq!(request.if_range.as_deref(), Some(RETRY_ETAG));
    }
}

fn regression_save_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("kosmos_{name}_{}.bin", std::process::id()))
}

async fn wait_for_snapshot(
    snapshot_rx: &mut tokio::sync::watch::Receiver<DownloadSnapshot>,
    deadline: Duration,
    timeout_message: &str,
    condition: impl FnMut(&DownloadSnapshot) -> bool,
) -> DownloadSnapshot {
    let result = tokio::time::timeout(deadline, snapshot_rx.wait_for(condition))
        .await
        .map(|result| result.map(|snapshot| snapshot.clone()));

    match result {
        Ok(Ok(snapshot)) => snapshot,
        Ok(Err(_)) => panic!("Snapshot channel closed while waiting for {timeout_message}"),
        Err(_) => {
            let snapshot = snapshot_rx.borrow_and_update().clone();
            panic!("{timeout_message}; last snapshot: {snapshot:?}");
        }
    }
}

async fn assert_existing_target_is_preserved(save_path: PathBuf, target_path: PathBuf) {
    let original = b"existing download data".to_vec();
    std::fs::write(&target_path, &original).unwrap();

    let server_addr = start_mock_server(generate_test_payload()).await;
    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{server_addr}/payload.bin"),
            save_path,
            num_chunks: 2,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Existing target did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;

    match &terminal.status {
        DownloadStatus::Failed(message) => assert!(
            message.contains("Refusing to overwrite existing file"),
            "Expected no-clobber error, got: {message}"
        ),
        DownloadStatus::Completed => {
            panic!("Engine must not report completion for an existing target")
        }
        status => panic!("Expected a terminal download state, got {status:?}"),
    }
    assert_eq!(std::fs::read(&target_path).unwrap(), original);
    assert_eq!(terminal.save_path, target_path);
}

#[tokio::test]
async fn metadata_uses_head_fields_or_range_probe_total() {
    let client = HttpClient::new();
    for (head_status, head_ranges, probe_status, length, accepts_ranges, source) in [
        ("200 OK", true, "206 Partial Content", 64, true, "head"),
        (
            "206 Partial Content",
            true,
            "206 Partial Content",
            64,
            true,
            "head",
        ),
        (
            "405 Method Not Allowed",
            false,
            "206 Partial Content",
            128,
            true,
            "probe",
        ),
        ("405 Method Not Allowed", false, "200 OK", 1, false, "probe"),
        ("200 OK", false, "206 Partial Content", 64, true, "head"),
    ] {
        let addr = start_local_server(move |mut socket, request| async move {
            let is_head = is_head_request(&request);
            let (status, length, source, date) = if is_head {
                (head_status, 64, "head", "Wed, 16 Sep 2026 12:00:00 GMT")
            } else {
                assert!(
                    request
                        .lines()
                        .any(|line| line.eq_ignore_ascii_case("range: bytes=0-0"))
                );
                (probe_status, 1, "probe", "Thu, 17 Sep 2026 12:00:00 GMT")
            };
            let ranges = if !is_head || head_ranges {
                "Accept-Ranges: bytes\r\n"
            } else {
                ""
            };
            let body = if is_head { "" } else { "x" };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {length}\r\n\
                 Content-Range: bytes 0-0/128\r\n{ranges}\
                 Content-Disposition: attachment; filename={source}.bin\r\n\
                 ETag: \"{source}\"\r\nLast-Modified: {date}\r\n\
                 Connection: close\r\n\r\n{body}"
            );
            let _ = socket.write_all(response.as_bytes()).await;
        })
        .await;
        let url = format!("http://{addr}/fallback.bin");
        let info = tokio::time::timeout(Duration::from_secs(3), client.fetch_info(&url))
            .await
            .expect("Metadata lookup timed out")
            .expect("Metadata lookup failed");
        assert_eq!(
            info,
            RemoteFileInfo {
                content_length: Some(length),
                accepts_ranges,
                filename: format!("{source}.bin"),
                etag: Some(format!("\"{source}\"")),
                last_modified: Some(
                    if source == "head" {
                        "Wed, 16 Sep 2026 12:00:00 GMT"
                    } else {
                        "Thu, 17 Sep 2026 12:00:00 GMT"
                    }
                    .into()
                ),
            },
            "HEAD {head_status}, range support {head_ranges}, probe {probe_status}"
        );
    }
}

#[tokio::test]
async fn test_full_multiconnection_download() {
    let payload = generate_test_payload();
    let (request_tx, mut request_rx) = tokio::sync::mpsc::channel(4);
    let initial_range_requests = Arc::new(AtomicUsize::new(0));
    let server_payload = payload.clone();
    let server_initial_range_requests = Arc::clone(&initial_range_requests);
    let server_addr = start_local_server(move |mut socket, request| {
        let request_tx = request_tx.clone();
        let payload = server_payload.clone();
        let initial_range_requests = Arc::clone(&server_initial_range_requests);
        async move {
            if is_head_request(&request) {
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {TEST_DATA_SIZE}\r\n\
                     Accept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            } else {
                let request_number = initial_range_requests.fetch_add(1, Ordering::SeqCst);
                if request_number < 4 {
                    request_tx.send((socket, request)).await.unwrap();
                } else if let Some(range) = requested_byte_range(&request)
                    && range.end < payload.len()
                {
                    let _ = write_range_response(&mut socket, &payload, range, None, None).await;
                }
            }
        }
    })
    .await;

    let temp_dir = std::env::temp_dir();
    let save_path = temp_dir.join(format!("kosmos_full_test_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    let download_url = format!("http://{server_addr}/payload.bin");

    action_tx
        .send(DownloadAction::Start {
            url: download_url,
            save_path: save_path.clone(),
            num_chunks: 4,
        })
        .await
        .unwrap();

    // Hold every response until all four requests arrive: serial downloads must fail.
    let requests = tokio::time::timeout(Duration::from_secs(5), async {
        let mut requests = Vec::new();
        for _ in 0..4 {
            requests.push(request_rx.recv().await.unwrap());
        }
        requests
    })
    .await
    .expect("Chunk requests did not arrive concurrently");

    let mut peers = std::collections::HashSet::new();
    let mut starts = std::collections::HashSet::new();
    for (mut socket, request) in requests {
        assert!(peers.insert(socket.peer_addr().unwrap()));
        assert_eq!(request.lines().next(), Some("GET /payload.bin HTTP/1.1"));
        let range = request
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("range").then_some(value.trim())
            })
            .unwrap();
        let (start, end) = range
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        let start: usize = start.parse().unwrap();
        let end: usize = end.parse().unwrap();
        assert!(starts.insert(start));
        assert!(start < TEST_DATA_SIZE && start.is_multiple_of(TEST_DATA_SIZE / 4));
        assert_eq!(end, start + TEST_DATA_SIZE / 4 - 1);

        let response = format!(
            "HTTP/1.1 206 Partial Content\r\n\
             Content-Range: bytes {start}-{end}/{TEST_DATA_SIZE}\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n",
            end - start + 1
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.write_all(&payload[start..=end]).await.unwrap();
    }

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Download did not complete in time",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "Download failed"
    );

    let downloaded_bytes = std::fs::read(&save_path).expect("Failed to read downloaded file");

    assert_eq!(downloaded_bytes.len(), payload.len());
    assert_eq!(downloaded_bytes, payload);

    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_existing_explicit_target_is_not_overwritten() {
    let save_path = std::env::temp_dir().join(format!(
        "kosmos_no_clobber_explicit_{}.bin",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&save_path);

    assert_existing_target_is_preserved(save_path.clone(), save_path.clone()).await;

    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_existing_directory_target_is_not_overwritten() {
    let save_dir = std::env::temp_dir().join(format!(
        "kosmos_no_clobber_directory_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&save_dir);
    std::fs::create_dir_all(&save_dir).unwrap();
    let target_path = save_dir.join("payload.bin");

    assert_existing_target_is_preserved(save_dir.clone(), target_path).await;

    let _ = std::fs::remove_dir_all(&save_dir);
}

#[tokio::test]
async fn test_pause_and_resume_download() {
    let payload = generate_test_payload();
    let server_addr = start_mock_server(payload.clone()).await;

    let temp_dir = std::env::temp_dir();
    let save_path = temp_dir.join(format!("kosmos_resume_test_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    let download_url = format!("http://{server_addr}/payload.bin");

    // 1. Start download
    action_tx
        .send(DownloadAction::Start {
            url: download_url,
            save_path: save_path.clone(),
            num_chunks: 4,
        })
        .await
        .unwrap();

    // 2. Wait until actively downloading
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_millis(500),
        "Download did not begin before pause",
        |snap| snap.status == DownloadStatus::Downloading && snap.downloaded_bytes > 0,
    )
    .await;

    // 3. Pause
    action_tx.send(DownloadAction::Pause).await.unwrap();

    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Download did not pause",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(
        paused.resumable,
        "Range download with an ETag should be resumable"
    );

    // 4. Resume
    action_tx.send(DownloadAction::Resume).await.unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Resumed download did not complete in time",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "Resumed download failed"
    );

    // 5. Verify data integrity after resume
    let downloaded_bytes = std::fs::read(&save_path).expect("Failed to read downloaded file");

    assert_eq!(downloaded_bytes.len(), payload.len());
    assert_eq!(downloaded_bytes, payload);

    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn no_etag_range_download_resumes_when_saved_bytes_match() {
    let payload = generate_offset_payload(TEST_DATA_SIZE);
    let saved_prefix_len = 16 * 1024;
    let full_range = TestRange {
        start: 0,
        end: payload.len() - 1,
    };
    let server =
        start_no_etag_resume_server(payload.clone(), None, full_range, saved_prefix_len).await;
    let save_path = regression_save_path("no_etag_resume");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 1,
        })
        .await
        .unwrap();

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "No-ETag download did not save its initial prefix",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes >= saved_prefix_len as u64
        },
    )
    .await;
    action_tx.send(DownloadAction::Pause).await.unwrap();
    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "No-ETag download did not pause",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(
        paused.resumable,
        "Known-length range downloads must be resumable without an ETag"
    );
    let paused_bytes = usize::try_from(paused.downloaded_bytes).unwrap();
    assert!(paused_bytes >= saved_prefix_len);
    let request_count_before_resume = observed_requests(&server.requests).len();

    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "No-ETag resumed download did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "No-ETag resume failed"
    );

    let requests = observed_requests(&server.requests);
    let resumed_worker = requests[request_count_before_resume..]
        .iter()
        .find(|request| request.range.end == payload.len() - 1 && request.range.len() > 4096)
        .expect("No-ETag resume did not request the remaining range");
    assert_eq!(
        resumed_worker.range.start,
        paused_bytes - 4096,
        "No-ETag resume must verify a bounded overlap in its worker response"
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn no_etag_resume_restarts_after_completed_chunk_changes() {
    let initial = generate_offset_payload(TEST_DATA_SIZE * 3);
    let chunk_size = initial.len() / 3;
    let unfinished_range = TestRange {
        start: 0,
        end: chunk_size - 1,
    };
    let completed_range = TestRange {
        start: chunk_size,
        end: 2 * chunk_size - 1,
    };
    let trailing_completed_range = TestRange {
        start: 2 * chunk_size,
        end: initial.len() - 1,
    };
    let saved_prefix_len = 16 * 1024;
    let mut updated = initial.clone();
    updated[completed_range.start] ^= 0xff;
    updated[completed_range.end] ^= 0xff;
    let server = start_no_etag_resume_server(
        initial.clone(),
        Some("Wed, 16 Sep 2026 12:00:00 GMT"),
        unfinished_range,
        saved_prefix_len,
    )
    .await;
    let save_path = regression_save_path("no_etag_changed_completed_chunk");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 3,
        })
        .await
        .unwrap();

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Completed and unfinished ranges did not save their initial bytes",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes
                    >= (completed_range.len() + trailing_completed_range.len() + saved_prefix_len)
                        as u64
        },
    )
    .await;
    action_tx.send(DownloadAction::Pause).await.unwrap();
    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Changed no-ETag download did not pause",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(paused.resumable);
    let paused_file = std::fs::read(&save_path).unwrap();
    assert_eq!(
        &paused_file[completed_range.start..=completed_range.end],
        &initial[completed_range.start..=completed_range.end],
        "The middle range must be complete before resuming the unfinished range"
    );
    assert_eq!(
        &paused_file[trailing_completed_range.start..=trailing_completed_range.end],
        &initial[trailing_completed_range.start..=trailing_completed_range.end],
    );
    assert_eq!(
        &paused_file[unfinished_range.start..unfinished_range.start + saved_prefix_len],
        &initial[unfinished_range.start..unfinished_range.start + saved_prefix_len],
    );

    server.replace_payload(updated.clone());
    let request_count_before_resume = observed_requests(&server.requests).len();
    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Changed no-ETag resume did not restart and finish",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "Changed no-ETag resume must restart from zero"
    );

    let requests = observed_requests(&server.requests);
    let resume_requests = &requests[request_count_before_resume..];
    assert!(
        resume_requests
            .iter()
            .any(|request| request.range == unfinished_range),
        "Changed saved bytes must restart the first chunk from zero: {resume_requests:?}"
    );
    assert_eq!(
        std::fs::read(&save_path).unwrap(),
        updated,
        "Restarted download must replace every old chunk with the new payload"
    );
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn no_etag_retry_restarts_after_saved_overlap_changes() {
    let initial = generate_offset_payload(TEST_DATA_SIZE);
    let saved_prefix_len = 16 * 1024;
    let full_range = TestRange {
        start: 0,
        end: initial.len() - 1,
    };
    let mut updated = initial.clone();
    updated[saved_prefix_len - 1024] ^= 0xff;
    let server = start_no_etag_resume_server(initial, None, full_range, saved_prefix_len).await;
    let save_path = regression_save_path("no_etag_overlap_change");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 1,
        })
        .await
        .unwrap();

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "No-ETag download did not save its overlap prefix",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes >= saved_prefix_len as u64
        },
    )
    .await;
    server.replace_payload(updated.clone());
    server.release_stalled_response();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Changed no-ETag overlap did not restart and finish",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "Changed overlap must restart instead of mixing source versions"
    );

    let requests = observed_requests(&server.requests);
    assert!(
        requests.iter().any(|request| {
            request.range.start == saved_prefix_len - 4096 && request.range.end == full_range.end
        }),
        "Retry must request a bounded overlap before appending new bytes: {requests:?}"
    );
    assert!(
        requests
            .iter()
            .filter(|request| request.range == full_range)
            .count()
            >= 2,
        "Changed overlap must cause a fresh request from zero: {requests:?}"
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), updated);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn no_validator_parallel_change_restarts_with_consistent_payload() {
    let initial = vec![b'A'; TEST_DATA_SIZE];
    let updated = vec![b'B'; TEST_DATA_SIZE];
    let midpoint = initial.len() / 2;
    let first_range = TestRange {
        start: 0,
        end: midpoint - 1,
    };
    let second_range = TestRange {
        start: midpoint,
        end: initial.len() - 1,
    };
    let server = start_between_range_change_server(initial.clone(), updated.clone()).await;
    let save_path = regression_save_path("no_validator_parallel_change");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    tokio::time::timeout(
        Duration::from_secs(3),
        server.initial_ranges_served.notified(),
    )
    .await
    .expect("Initial parallel ranges were not served from different source versions");
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Changed no-validator parallel download did not restart and finish",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "Changed no-validator source must restart with one consistent payload"
    );

    let served_ranges = server.served_ranges.lock().unwrap().clone();
    assert!(
        served_ranges.contains(&(first_range, false)),
        "The first initial range was not served from the original source"
    );
    assert!(
        served_ranges.contains(&(second_range, true)),
        "The second initial range was not served after the source changed"
    );
    assert_eq!(
        std::fs::read(&save_path).unwrap(),
        updated,
        "Restarted parallel download must not retain the original first range"
    );
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_pause_during_connecting_restarts_metadata() {
    let payload = generate_test_payload();
    let server_addr =
        start_mock_server_with_head_delay(payload.clone(), Duration::from_millis(150)).await;
    let save_path = std::env::temp_dir().join(format!(
        "kosmos_connecting_resume_test_{}.bin",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{server_addr}/payload.bin"),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let connecting = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Download did not enter Connecting",
        |snap| snap.status == DownloadStatus::Connecting,
    )
    .await;
    assert!(
        connecting.filename.is_empty(),
        "Metadata must not arrive before the delayed HEAD response"
    );

    action_tx.send(DownloadAction::Pause).await.unwrap();
    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Download did not pause during metadata lookup",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(
        paused.resumable,
        "Stopped metadata lookup should be retryable"
    );

    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Resumed metadata lookup did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "Resumed metadata lookup failed"
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);

    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_non_range_pause_restarts_from_zero() {
    let payload = generate_test_payload();
    let server_addr = start_non_range_mock_server(payload.clone()).await;
    let save_path = std::env::temp_dir().join(format!(
        "kosmos_non_range_restart_test_{}.bin",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{server_addr}/payload.bin"),
            save_path: save_path.clone(),
            num_chunks: 4,
        })
        .await
        .unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Non-range download did not begin before pause",
        |snap| snap.status == DownloadStatus::Downloading && snap.downloaded_bytes > 0,
    )
    .await;

    action_tx.send(DownloadAction::Pause).await.unwrap();
    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Non-range download did not pause",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(
        !paused.resumable,
        "A non-range download must advertise Restart"
    );

    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Restarted non-range download did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "Restarted non-range download failed"
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);

    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn changed_metadata_restarts_from_zero_with_new_payload() {
    let initial = vec![b'A'; TEST_DATA_SIZE];
    let updated = vec![b'B'; TEST_DATA_SIZE];
    let server = start_changing_resource_server(initial, updated.clone()).await;
    let save_path = std::env::temp_dir().join(format!(
        "kosmos_changed_resource_test_{}.bin",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 1,
        })
        .await
        .unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Changed-resource download did not make initial progress",
        |snap| snap.status == DownloadStatus::Downloading && snap.downloaded_bytes >= 2048,
    )
    .await;

    action_tx.send(DownloadAction::Pause).await.unwrap();
    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Changed-resource download did not pause",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(paused.resumable);
    let paused_file = std::fs::read(&save_path).unwrap();
    let written_bytes = paused_file.iter().take_while(|&&byte| byte == b'A').count() as u64;
    assert_eq!(paused.downloaded_bytes, written_bytes);

    let request_count_before_resume = observed_requests(&server.requests).len();
    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "Changed-resource resume did not restart and finish",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "Changed metadata must restart instead of retaining the old partial file"
    );

    let requests = observed_requests(&server.requests);
    assert!(
        requests[request_count_before_resume..]
            .iter()
            .any(|request| {
                request.range
                    == TestRange {
                        start: 0,
                        end: TEST_DATA_SIZE - 1,
                    }
                    && request.if_range.as_deref() == Some("\"v2\"")
            }),
        "Changed metadata must make a fresh strong-validator request from zero: {requests:?}"
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), updated);

    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_premature_disconnect_fails_download() {
    // Mock server that advertises 32 KB but drops TCP connection after sending only 256 bytes
    let addr = start_local_server(|mut socket, request| async move {
        if is_head_request(&request) {
            let resp = "HTTP/1.1 200 OK\r\n\
                        Content-Length: 32768\r\n\
                        Accept-Ranges: bytes\r\n\
                        Connection: close\r\n\r\n";
            let _ = socket.write_all(resp.as_bytes()).await;
        } else {
            let resp_header = "HTTP/1.1 206 Partial Content\r\n\
                               Content-Range: bytes 0-32767/32768\r\n\
                               Content-Length: 32768\r\n\
                               Connection: close\r\n\r\n";
            let _ = socket.write_all(resp_header.as_bytes()).await;
            // Only send 256 bytes then close abruptly.
            let truncated = [42u8; 256];
            let _ = socket.write_all(&truncated).await;
            // Drop socket without sending remaining 32512 bytes.
        }
    })
    .await;

    let temp_dir = std::env::temp_dir();
    let save_path = temp_dir.join(format!("kosmos_premature_eof_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{addr}/file.bin"),
            save_path: save_path.clone(),
            num_chunks: 1,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "Engine should transition to Failed status on premature disconnect",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    match terminal.status {
        DownloadStatus::Failed(err) => {
            assert!(
                err.contains("Unexpected EOF") || err.contains("error decoding response body"),
                "Expected error on premature disconnect, got: {err}"
            );
        }
        DownloadStatus::Completed => {
            panic!("Engine must not report Completed on truncated stream!")
        }
        status => panic!("Expected a terminal download state, got {status:?}"),
    }
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn dynamic_split_replaces_stalled_donor_without_corrupting_confirmed_bytes() {
    let payload = generate_offset_payload(DYNAMIC_DATA_SIZE);
    let server = start_dynamic_split_server(payload.clone(), Some(DYNAMIC_ETAG)).await;
    let save_path = regression_save_path("dynamic_split");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "stalled donor did not confirm its prefix",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes >= DYNAMIC_PREFIX_SIZE as u64
        },
    )
    .await;
    server.release_fast.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), server.child_ready.notified())
        .await
        .expect("stalled donor was not split after its sibling completed");
    tokio::time::timeout(Duration::from_secs(2), server.donor_closed.notified())
        .await
        .expect("donor connection remained active after its yield acknowledgement");
    assert_eq!(
        server.max_waiting_children.load(Ordering::SeqCst),
        2,
        "split must not exceed the requested two concurrent workers"
    );
    server.release_children.send(true).unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "dynamically split download did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "split download failed"
    );

    assert_dynamic_split_requests(&observed_requests(&server.requests));
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn dynamic_partitions_survive_pause_and_resume() {
    let payload = generate_offset_payload(DYNAMIC_DATA_SIZE);
    let server = start_dynamic_split_server(payload.clone(), Some(DYNAMIC_ETAG)).await;
    let save_path = regression_save_path("dynamic_split_resume");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "stalled donor did not confirm its prefix",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes >= DYNAMIC_PREFIX_SIZE as u64
        },
    )
    .await;
    server.release_fast.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), server.child_ready.notified())
        .await
        .expect("stalled donor was not split before pause");
    tokio::time::timeout(Duration::from_secs(2), server.donor_closed.notified())
        .await
        .expect("donor connection remained active after its yield acknowledgement");
    let before_pause = observed_requests(&server.requests);
    let expected_children = assert_dynamic_split_requests(&before_pause);

    action_tx.send(DownloadAction::Pause).await.unwrap();
    let paused = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(3),
        "download did not pause after splitting",
        |snap| snap.status == DownloadStatus::Paused,
    )
    .await;
    assert!(paused.resumable);

    server.release_children.send(true).unwrap();
    action_tx.send(DownloadAction::Resume).await.unwrap();
    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "resumed dynamically split download did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "resumed split download failed"
    );

    let requests = observed_requests(&server.requests);
    let resumed = &requests[before_pause.len()..];
    assert_eq!(
        resumed.len(),
        2,
        "resume should only restart split partitions"
    );
    assert!(
        resumed
            .iter()
            .all(|request| request.if_range.as_deref() == Some(DYNAMIC_ETAG))
    );
    let mut resumed_ranges: Vec<_> = resumed.iter().map(|request| request.range).collect();
    resumed_ranges.sort_unstable();
    assert_eq!(resumed_ranges, expected_children);
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn dynamic_split_without_strong_etag_does_not_redistribute_live_work() {
    let payload = generate_offset_payload(DYNAMIC_DATA_SIZE);
    let server = start_dynamic_split_server(payload, None).await;
    let save_path = regression_save_path("dynamic_split_no_validator");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "stalled donor did not confirm its prefix",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes >= DYNAMIC_PREFIX_SIZE as u64
        },
    )
    .await;
    server.release_fast.send(true).unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "initial ranged workers did not make progress",
        |snap| {
            snap.status == DownloadStatus::Downloading
                && snap.downloaded_bytes
                    >= (DYNAMIC_INITIAL_CHUNK_SIZE + DYNAMIC_PREFIX_SIZE) as u64
        },
    )
    .await;
    let unexpected_child =
        tokio::time::timeout(Duration::from_millis(750), server.child_seen.notified()).await;

    action_tx.send(DownloadAction::Cancel).await.unwrap();
    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "stalled no-ETag download did not cancel",
        |snap| snap.status == DownloadStatus::Idle,
    )
    .await;

    assert!(
        unexpected_child.is_err(),
        "a download without a strong ETag must not redistribute live work"
    );
    let requests = observed_requests(&server.requests);
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.if_range.is_none()));
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn ranged_drop_retries_from_confirmed_offset_with_original_validator() {
    let payload = generate_offset_payload(512 * 1024);
    let server = start_retry_server(payload.clone(), RetryBehavior::RecoverOnce).await;
    let save_path = regression_save_path("retry_recovery");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 1,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "dropped ranged connection did not recover",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "ranged retry failed"
    );
    assert_retry_requests(
        &observed_requests(&server.requests),
        payload.len(),
        &[0, RETRY_PREFIX_SIZE],
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn retry_exhaustion_keeps_partial_bytes_for_manual_resume() {
    let payload = generate_offset_payload(512 * 1024);
    let server = start_retry_server(payload.clone(), RetryBehavior::PersistentDrop).await;
    let save_path = regression_save_path("retry_exhaustion");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 1,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "persistent drops did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert!(
        matches!(terminal.status, DownloadStatus::Failed(_)),
        "persistent drops must fail after the retry budget is exhausted"
    );
    assert!(
        terminal.resumable,
        "A retryable network failure must retain a manually resumable partial download"
    );
    assert_eq!(terminal.downloaded_bytes, (RETRY_PREFIX_SIZE * 4) as u64);
    let partial = std::fs::read(&save_path).unwrap();
    assert_eq!(
        &partial[..RETRY_PREFIX_SIZE * 4],
        &payload[..RETRY_PREFIX_SIZE * 4],
        "Retry exhaustion must retain the confirmed bytes"
    );
    assert_retry_requests(
        &observed_requests(&server.requests),
        payload.len(),
        &[
            0,
            RETRY_PREFIX_SIZE,
            RETRY_PREFIX_SIZE * 2,
            RETRY_PREFIX_SIZE * 3,
        ],
    );
    server.allow_recovery();
    action_tx.send(DownloadAction::Resume).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), snapshot_rx.changed())
        .await
        .expect("Manual resume did not leave the failed state")
        .expect("Snapshot channel closed during manual resume");
    let resumed = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Manual resume after retry exhaustion did not reach a terminal state",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        resumed.status,
        DownloadStatus::Completed,
        "Manual resume after a network failure failed; requests: {:?}",
        observed_requests(&server.requests)
    );
    assert_retry_requests(
        &observed_requests(&server.requests),
        payload.len(),
        &[
            0,
            RETRY_PREFIX_SIZE,
            RETRY_PREFIX_SIZE * 2,
            RETRY_PREFIX_SIZE * 3,
            RETRY_PREFIX_SIZE * 4,
        ],
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn retry_recovery_restarts_changed_resources_but_rejects_invalid_responses() {
    for (fault, label, restarts) in [
        (RecoveryFault::ChangedEtag, "retry_changed_etag", true),
        (
            RecoveryFault::StatusOkChangedEtag,
            "retry_status_200_changed_etag",
            true,
        ),
        (
            RecoveryFault::InvalidContentRange,
            "retry_invalid_range",
            false,
        ),
        (RecoveryFault::StatusOk, "retry_status_200", false),
    ] {
        let payload = generate_offset_payload(512 * 1024);
        let server = start_retry_server(payload.clone(), RetryBehavior::RejectSecond(fault)).await;
        let save_path = regression_save_path(label);
        let _ = std::fs::remove_file(&save_path);

        let engine = DownloadEngine::new();
        let action_tx = engine.action_tx();
        let mut snapshot_rx = engine.snapshot_rx();
        action_tx
            .send(DownloadAction::Start {
                url: format!("http://{}/payload.bin", server.addr),
                save_path: save_path.clone(),
                num_chunks: 1,
            })
            .await
            .unwrap();

        let terminal = wait_for_snapshot(
            &mut snapshot_rx,
            Duration::from_secs(5),
            "invalid recovery response did not reach a terminal state",
            |snap| {
                matches!(
                    snap.status,
                    DownloadStatus::Completed | DownloadStatus::Failed(_)
                )
            },
        )
        .await;
        if restarts {
            assert_eq!(
                terminal.status,
                DownloadStatus::Completed,
                "{label} must restart the changed resource"
            );
            assert_retry_requests(
                &observed_requests(&server.requests),
                payload.len(),
                &[0, RETRY_PREFIX_SIZE, 0],
            );
            assert_eq!(std::fs::read(&save_path).unwrap(), payload);
        } else {
            assert!(
                matches!(terminal.status, DownloadStatus::Failed(_)),
                "{label} with the same ETag must remain fatal"
            );
            assert_retry_requests(
                &observed_requests(&server.requests),
                payload.len(),
                &[0, RETRY_PREFIX_SIZE],
            );
            let saved = std::fs::read(&save_path).unwrap();
            assert_eq!(&saved[..RETRY_PREFIX_SIZE], &payload[..RETRY_PREFIX_SIZE]);
        }
        let _ = std::fs::remove_file(&save_path);
    }
}

#[tokio::test]
async fn repeatedly_changed_resource_stops_after_two_restarts_and_clears_partial_file() {
    let payload = generate_offset_payload(512 * 1024);
    let server = start_retry_server(payload.clone(), RetryBehavior::AlwaysChangedEtag).await;
    let save_path = regression_save_path("retry_changed_restart_budget");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 1,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(5),
        "Repeated content changes did not exhaust the restart budget",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    match terminal.status {
        DownloadStatus::Failed(message) => assert!(
            message.contains("Remote file keeps changing"),
            "Expected restart-budget failure, got: {message}"
        ),
        status => panic!("Repeatedly changing resource must not complete, got {status:?}"),
    }
    assert!(!terminal.resumable);
    assert_retry_requests(
        &observed_requests(&server.requests),
        payload.len(),
        &[0, 0, 0],
    );
    assert_eq!(
        std::fs::metadata(&save_path).unwrap().len(),
        0,
        "Restart exhaustion must clear invalid partial bytes"
    );
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn stalled_single_worker_restarts_after_idle_timeout() {
    let payload = generate_offset_payload(512 * 1024);
    let server = start_retry_server(payload.clone(), RetryBehavior::StallThenRecover).await;
    let save_path = regression_save_path("idle_restart");
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();
    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{}/payload.bin", server.addr),
            save_path: save_path.clone(),
            num_chunks: 1,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(8),
        "single stalled worker did not restart after the idle timeout",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    assert_eq!(
        terminal.status,
        DownloadStatus::Completed,
        "idle restart failed"
    );
    assert_retry_requests(
        &observed_requests(&server.requests),
        payload.len(),
        &[0, RETRY_PREFIX_SIZE],
    );
    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_server_ignores_range_returns_200() {
    // Mock server returns 200 OK when a range was requested
    let addr = start_local_server(|mut socket, request| async move {
        if is_head_request(&request) {
            let resp = "HTTP/1.1 200 OK\r\n\
                        Content-Length: 1000\r\n\
                        Accept-Ranges: bytes\r\n\
                        Connection: close\r\n\r\n";
            let _ = socket.write_all(resp.as_bytes()).await;
        } else {
            // Return 200 OK instead of 206 Partial Content.
            let resp = "HTTP/1.1 200 OK\r\n\
                        Content-Length: 1000\r\n\
                        Connection: close\r\n\r\n";
            let _ = socket.write_all(resp.as_bytes()).await;
            let payload = [0u8; 1000];
            let _ = socket.write_all(&payload).await;
        }
    })
    .await;

    let temp_dir = std::env::temp_dir();
    let save_path = temp_dir.join(format!("kosmos_range_200_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{addr}/test.bin"),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(2),
        "Engine should detect range violation and fail",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    match terminal.status {
        DownloadStatus::Failed(err) => {
            assert!(
                err.contains("206 Partial Content"),
                "Expected 206 requirement error, got: {err}"
            );
        }
        DownloadStatus::Completed => {
            panic!("Engine must not complete when server returns 200 OK for sub-range requests!");
        }
        status => panic!("Expected a terminal download state, got {status:?}"),
    }
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_cancel_during_connecting() {
    let addr =
        start_mock_server_with_head_delay(generate_test_payload(), Duration::from_secs(2)).await;

    let temp_dir = std::env::temp_dir();
    let save_path = temp_dir.join(format!("kosmos_cancel_test_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{addr}/slow.bin"),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    // Verify it transitioned to Connecting
    let connecting = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_millis(200),
        "Should enter Connecting status",
        |snap| snap.status == DownloadStatus::Connecting,
    )
    .await;
    assert!(
        connecting.filename.is_empty(),
        "Cancel must happen before delayed metadata arrives"
    );

    // Cancel immediately while still connecting
    action_tx.send(DownloadAction::Cancel).await.unwrap();

    wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_millis(200),
        "Should transition to Idle immediately on Cancel",
        |snap| snap.status == DownloadStatus::Idle,
    )
    .await;
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn test_probe_bad_http_status() {
    // Mock server returning 404
    let addr = start_local_server(|mut socket, _request| async move {
        let resp = "HTTP/1.1 404 Not Found\r\n\
                    Content-Length: 0\r\n\
                    Connection: close\r\n\r\n";
        let _ = socket.write_all(resp.as_bytes()).await;
    })
    .await;

    let temp_dir = std::env::temp_dir();
    let save_path = temp_dir.join(format!("kosmos_404_test_{}.bin", std::process::id()));
    let _ = std::fs::remove_file(&save_path);

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let mut snapshot_rx = engine.snapshot_rx();

    action_tx
        .send(DownloadAction::Start {
            url: format!("http://{addr}/not_found.bin"),
            save_path: save_path.clone(),
            num_chunks: 2,
        })
        .await
        .unwrap();

    let terminal = wait_for_snapshot(
        &mut snapshot_rx,
        Duration::from_secs(1),
        "Engine should report Failed for 404 Not Found",
        |snap| {
            matches!(
                snap.status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            )
        },
    )
    .await;
    match terminal.status {
        DownloadStatus::Failed(err) => {
            assert!(
                err.contains("404"),
                "Expected 404 error message, got: {err}"
            );
        }
        DownloadStatus::Completed => panic!("Engine must not complete for 404 Not Found"),
        status => panic!("Expected a terminal download state, got {status:?}"),
    }
    let _ = std::fs::remove_file(&save_path);
}
