use super::http::{
    ObservedRangeRequest, TestRange, is_head_request, request_header, requested_byte_range,
    start_local_server, wait_until_released, write_metadata_response, write_range_headers,
    write_range_response,
};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

pub(crate) struct NoEtagResumeServer {
    pub(crate) addr: SocketAddr,
    payload: Arc<Mutex<Vec<u8>>>,
    pub(crate) requests: Arc<Mutex<Vec<ObservedRangeRequest>>>,
    release_stall: watch::Sender<bool>,
}

impl NoEtagResumeServer {
    pub(crate) fn replace_payload(&self, updated: Vec<u8>) {
        let mut payload = self.payload.lock().unwrap();
        assert_eq!(payload.len(), updated.len());
        *payload = updated;
    }

    pub(crate) fn release_stalled_response(&self) {
        let _ = self.release_stall.send(true);
    }
}

pub(crate) async fn start_no_etag_resume_server(
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
