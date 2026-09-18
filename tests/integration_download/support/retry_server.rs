use super::fixtures::{RETRY_ETAG, RETRY_PREFIX_SIZE};
use super::http::{
    ObservedRangeRequest, TestRange, is_head_request, request_header, requested_byte_range,
    start_local_server, write_metadata_response, write_range_headers, write_range_response,
};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn write_prefix_then_drop(
    socket: &mut tokio::net::TcpStream,
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

#[derive(Clone, Copy)]
pub(crate) enum RetryBehavior {
    RecoverOnce,
    PersistentDrop,
    RejectSecond(RecoveryFault),
    AlwaysChangedEtag,
    StallThenRecover,
}

#[derive(Clone, Copy)]
pub(crate) enum RecoveryFault {
    ChangedEtag,
    InvalidContentRange,
    StatusOk,
    StatusOkChangedEtag,
}

pub(crate) struct RetryServer {
    pub(crate) addr: SocketAddr,
    pub(crate) requests: Arc<Mutex<Vec<ObservedRangeRequest>>>,
    allow_recovery: Arc<AtomicBool>,
}

impl RetryServer {
    pub(crate) fn allow_recovery(&self) {
        self.allow_recovery.store(true, Ordering::SeqCst);
    }
}

pub(crate) async fn start_retry_server(payload: Vec<u8>, behavior: RetryBehavior) -> RetryServer {
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
                let _ =
                    write_metadata_response(&mut socket, payload.len(), Some(RETRY_ETAG), None).await;
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
                        write_prefix_then_drop(&mut socket, payload.as_slice(), range, RETRY_ETAG)
                            .await;
                    } else {
                        let _ = write_range_response(
                            &mut socket,
                            payload.as_slice(),
                            range,
                            Some(RETRY_ETAG),
                            None,
                        )
                        .await;
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
                    write_prefix_then_drop(&mut socket, payload.as_slice(), range, RETRY_ETAG)
                        .await;
                }
                RetryBehavior::RejectSecond(RecoveryFault::ChangedEtag) if attempt == 1 => {
                    let _ = write_range_response(
                        &mut socket,
                        payload.as_slice(),
                        range,
                        Some("\"retry-v2\""),
                        None,
                    )
                    .await;
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
                    if write_range_headers(
                        &mut socket,
                        range,
                        payload.len(),
                        Some(RETRY_ETAG),
                        None,
                    )
                    .await
                    {
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
                    let _ = write_range_response(
                        &mut socket,
                        payload.as_slice(),
                        range,
                        Some(RETRY_ETAG),
                        None,
                    )
                    .await;
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
