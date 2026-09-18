use super::http::{
    ObservedRangeRequest, TestRange, is_head_request, request_header, requested_byte_range,
    start_local_server, wait_until_released, write_metadata_response, write_range_response,
};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Notify, watch};

pub(crate) struct ChangingResourceServer {
    pub(crate) addr: SocketAddr,
    pub(crate) requests: Arc<Mutex<Vec<ObservedRangeRequest>>>,
}

pub(crate) async fn start_changing_resource_server(
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

pub(crate) struct BetweenRangeChangeServer {
    pub(crate) addr: SocketAddr,
    pub(crate) initial_ranges_served: Arc<Notify>,
    pub(crate) served_ranges: Arc<Mutex<Vec<(TestRange, bool)>>>,
}

pub(crate) async fn start_between_range_change_server(
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
