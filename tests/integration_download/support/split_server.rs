use super::fixtures::{DYNAMIC_DATA_SIZE, DYNAMIC_INITIAL_CHUNK_SIZE, DYNAMIC_PREFIX_SIZE};
use super::http::{
    ObservedRangeRequest, TestRange, is_head_request, request_header, requested_byte_range,
    start_local_server, wait_until_released, write_metadata_response, write_range_headers,
    write_range_response,
};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, watch};

pub(crate) struct DynamicSplitServer {
    pub(crate) addr: SocketAddr,
    pub(crate) requests: Arc<Mutex<Vec<ObservedRangeRequest>>>,
    pub(crate) child_ready: Arc<Notify>,
    pub(crate) child_seen: Arc<Notify>,
    pub(crate) donor_closed: Arc<Notify>,
    pub(crate) release_fast: watch::Sender<bool>,
    pub(crate) release_children: watch::Sender<bool>,
    pub(crate) max_waiting_children: Arc<AtomicUsize>,
}

pub(crate) async fn start_dynamic_split_server(
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
                let _ = write_metadata_response(
                    &mut socket,
                    payload.len(),
                    validator.as_deref(),
                    None,
                )
                .await;
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
                if !write_range_headers(
                    &mut socket,
                    range,
                    payload.len(),
                    validator.as_deref(),
                    None,
                )
                .await
                {
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
                let _ = write_range_response(
                    &mut socket,
                    payload.as_slice(),
                    range,
                    validator.as_deref(),
                    None,
                )
                .await;
            } else {
                child_seen.notify_one();
                yield_late_body.notify_one();
                let waiting = waiting_children.fetch_add(1, Ordering::SeqCst) + 1;
                max_waiting_children.fetch_max(waiting, Ordering::SeqCst);
                if waiting == 2 {
                    child_ready.notify_one();
                }

                let mut child_release_rx = child_release_rx;
                if write_range_headers(
                    &mut socket,
                    range,
                    payload.len(),
                    validator.as_deref(),
                    None,
                )
                .await
                {
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
