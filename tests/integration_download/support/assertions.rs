use super::fixtures::{
    DYNAMIC_DATA_SIZE, DYNAMIC_ETAG, DYNAMIC_INITIAL_CHUNK_SIZE, DYNAMIC_PREFIX_SIZE,
    MIN_DYNAMIC_CHILD_SIZE, RETRY_ETAG,
};
use super::http::{ObservedRangeRequest, TestRange};
use std::sync::{Arc, Mutex};

pub(crate) fn observed_requests(
    requests: &Arc<Mutex<Vec<ObservedRangeRequest>>>,
) -> Vec<ObservedRangeRequest> {
    requests.lock().unwrap().clone()
}

pub(crate) fn assert_dynamic_split_requests(requests: &[ObservedRangeRequest]) -> Vec<TestRange> {
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

pub(crate) fn assert_retry_requests(
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
