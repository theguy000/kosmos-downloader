#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkRange {
    pub id: usize,
    pub start: u64,
    pub end: u64,
}

impl ChunkRange {
    pub fn size(&self) -> u64 {
        if self.end >= self.start {
            self.end - self.start + 1
        } else {
            0
        }
    }
}

/// Splits a file of `total_size` into `num_chunks` balanced byte ranges.
/// Guarantees zero gaps, zero overlaps, and balanced distribution of remainders.
pub fn calculate_chunks(total_size: u64, num_chunks: usize) -> Vec<ChunkRange> {
    if total_size == 0 {
        return Vec::new();
    }

    let k = num_chunks.clamp(1, total_size.min(usize::MAX as u64) as usize);
    let base_size = total_size / k as u64;
    let remainder = total_size % k as u64;

    let mut chunks = Vec::with_capacity(k);
    let mut current_offset = 0;

    for i in 0..k {
        let extra = if (i as u64) < remainder { 1 } else { 0 };
        let size = base_size + extra;
        let start = current_offset;
        let end = start + size - 1;
        chunks.push(ChunkRange { id: i, start, end });
        current_offset += size;
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_splitting_empty() {
        let chunks = calculate_chunks(0, 4);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_chunk_splitting_single_byte() {
        let chunks = calculate_chunks(1, 4);
        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks[0],
            ChunkRange {
                id: 0,
                start: 0,
                end: 0
            }
        );
        assert_eq!(chunks[0].size(), 1);
    }

    #[test]
    fn test_chunk_splitting_fewer_bytes_than_chunks() {
        let chunks = calculate_chunks(3, 8);
        assert_eq!(chunks.len(), 3);
        assert_eq!(
            chunks[0],
            ChunkRange {
                id: 0,
                start: 0,
                end: 0
            }
        );
        assert_eq!(
            chunks[1],
            ChunkRange {
                id: 1,
                start: 1,
                end: 1
            }
        );
        assert_eq!(
            chunks[2],
            ChunkRange {
                id: 2,
                start: 2,
                end: 2
            }
        );
    }

    #[test]
    fn test_chunk_splitting_exact_multiple() {
        let chunks = calculate_chunks(1000, 4);
        assert_eq!(chunks.len(), 4);
        assert_eq!(
            chunks[0],
            ChunkRange {
                id: 0,
                start: 0,
                end: 249
            }
        );
        assert_eq!(
            chunks[1],
            ChunkRange {
                id: 1,
                start: 250,
                end: 499
            }
        );
        assert_eq!(
            chunks[2],
            ChunkRange {
                id: 2,
                start: 500,
                end: 749
            }
        );
        assert_eq!(
            chunks[3],
            ChunkRange {
                id: 3,
                start: 750,
                end: 999
            }
        );

        for c in &chunks {
            assert_eq!(c.size(), 250);
        }
    }

    #[test]
    fn test_chunk_splitting_with_remainder() {
        let chunks = calculate_chunks(10, 3);
        assert_eq!(chunks.len(), 3);
        // 10 / 3 = 3 remainder 1 => sizes 4, 3, 3
        assert_eq!(
            chunks[0],
            ChunkRange {
                id: 0,
                start: 0,
                end: 3
            }
        ); // 4 bytes
        assert_eq!(
            chunks[1],
            ChunkRange {
                id: 1,
                start: 4,
                end: 6
            }
        ); // 3 bytes
        assert_eq!(
            chunks[2],
            ChunkRange {
                id: 2,
                start: 7,
                end: 9
            }
        ); // 3 bytes

        assert_eq!(chunks[0].size(), 4);
        assert_eq!(chunks[1].size(), 3);
        assert_eq!(chunks[2].size(), 3);
        assert_eq!(chunks.iter().map(|c| c.size()).sum::<u64>(), 10);
    }

    #[test]
    fn test_chunk_splitting_invariants() {
        for total_size in [1, 2, 7, 15, 64, 100, 1024, 1_000_007] {
            for num_chunks in [1, 2, 3, 4, 5, 8, 16, 32] {
                let chunks = calculate_chunks(total_size, num_chunks);
                assert!(!chunks.is_empty());
                assert_eq!(chunks[0].start, 0);
                assert_eq!(chunks.last().unwrap().end, total_size - 1);

                // Check continuity and no gaps/overlaps
                for i in 0..chunks.len() - 1 {
                    assert_eq!(chunks[i].end + 1, chunks[i + 1].start);
                }

                // Check total sum
                let sum: u64 = chunks.iter().map(|c| c.size()).sum();
                assert_eq!(sum, total_size);

                // Check balanced distribution (sizes differ by at most 1)
                let min_size = chunks.iter().map(|c| c.size()).min().unwrap();
                let max_size = chunks.iter().map(|c| c.size()).max().unwrap();
                assert!(max_size - min_size <= 1);
            }
        }
    }
}
