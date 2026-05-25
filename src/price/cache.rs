// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

use alloy_primitives::{Address, BlockNumber};

use crate::cache::block_range::{BlockRangeCache, Mergeable};
use crate::price::calculator::TokenPriceResult;

/// A range of blocks with start and end inclusive
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRange {
    pub start: BlockNumber,
    pub end: BlockNumber,
}

impl BlockRange {
    /// Create a new block range
    pub const fn new(start: BlockNumber, end: BlockNumber) -> Self {
        Self { start, end }
    }

    /// Get the length of this block range (inclusive)
    pub fn len(&self) -> u64 {
        if self.end >= self.start {
            self.end.saturating_sub(self.start) + 1
        } else {
            0
        }
    }

    /// Check if this range is empty
    pub fn is_empty(&self) -> bool {
        self.end < self.start
    }

    /// Check if this range contains a specific block
    pub fn contains(&self, block: BlockNumber) -> bool {
        block >= self.start && block <= self.end
    }
}

impl From<(BlockNumber, BlockNumber)> for BlockRange {
    fn from((start, end): (BlockNumber, BlockNumber)) -> Self {
        Self { start, end }
    }
}

impl From<BlockRange> for (BlockNumber, BlockNumber) {
    fn from(range: BlockRange) -> Self {
        (range.start, range.end)
    }
}

// Implement Mergeable for TokenPriceResult
impl Mergeable for TokenPriceResult {
    fn merge(&mut self, other: &Self) {
        TokenPriceResult::merge(self, other);
    }
}

/// Cache for token price calculation results
///
/// Stores price data keyed by `(token_address, start_block, end_block)`. Cached
/// ranges for the same token are kept disjoint so aggregate token and USDC
/// amounts are never double-counted when ranges overlap. A cached aggregate is
/// only used to answer a query whose window is exactly the entry's range or is
/// exactly tiled by entries fully inside the window — a wider cached entry is
/// never returned for a narrower query, because its totals cover blocks the
/// caller did not ask about.
#[derive(Debug, Clone, Default)]
pub struct PriceCache {
    inner: BlockRangeCache<Address, TokenPriceResult>,
}

impl PriceCache {
    /// Retrieve the cached result whose range exactly matches the query
    ///
    /// Cached aggregates summarise the blocks they were computed over and
    /// cannot be scoped down to a narrower window, so `get` only returns a
    /// value when an entry's range is exactly `(start_block, end_block)`.
    /// Use [`Self::calculate_gaps`] for gap-aware lookup that combines
    /// disjoint entries lying inside the query window.
    pub fn get(
        &self,
        token_address: Address,
        start_block: BlockNumber,
        end_block: BlockNumber,
    ) -> Option<TokenPriceResult> {
        self.inner.get(&token_address, start_block, end_block)
    }

    /// Insert a result while keeping cached ranges disjoint
    ///
    /// Overlap is resolved by choosing whose range is authoritative rather
    /// than combining aggregates:
    ///
    /// - **No overlap with existing entries**: stored as a new disjoint segment.
    /// - **`[start_block, end_block]` covers every overlapping entry**: those
    ///   entries are replaced with the new value (intended for the calculator
    ///   pattern of writing back an aggregate for the full query range after
    ///   filling gaps).
    /// - **An existing entry already covers `[start_block, end_block]`** or the
    ///   ranges only partially overlap: the new insert is dropped to preserve
    ///   the disjoint invariant. A wider existing entry is not used to serve
    ///   the narrower range; a follow-up query at the narrower range will
    ///   rescan rather than read an over-counted aggregate.
    pub fn insert(
        &mut self,
        token_address: Address,
        start_block: BlockNumber,
        end_block: BlockNumber,
        result: TokenPriceResult,
    ) {
        self.inner
            .insert(token_address, start_block, end_block, result);
    }

    /// Calculate which block ranges need to be processed by finding gaps in the cached data
    ///
    /// Only cached entries whose range lies fully inside `[start_block,
    /// end_block]` contribute to the merged result — a wider entry that
    /// extends outside the query window is ignored, and the corresponding
    /// blocks are reported as a gap so the caller can rescan and produce a
    /// window-scoped aggregate.
    ///
    /// Returns a tuple of:
    /// - `Option<TokenPriceResult>`: Merged data from all cached entries inside the query window
    /// - `Vec<BlockRange>`: Sorted list of uncached ranges (gaps) to scan
    pub fn calculate_gaps(
        &self,
        token_address: Address,
        start_block: BlockNumber,
        end_block: BlockNumber,
    ) -> (Option<TokenPriceResult>, Vec<BlockRange>) {
        let (result, gaps) =
            self.inner
                .calculate_gaps(&token_address, start_block, end_block, || {
                    TokenPriceResult::new(token_address)
                });

        // Convert Vec<(u64, u64)> to Vec<BlockRange>
        let typed_gaps = gaps.into_iter().map(BlockRange::from).collect();
        (result, typed_gaps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    /// Helper to create a test price result with specified amounts
    fn create_price_result(
        token: Address,
        token_amount: f64,
        usdc_amount: f64,
    ) -> TokenPriceResult {
        use crate::{NormalizedAmount, TransactionCount, UsdValue};

        TokenPriceResult {
            token_address: token,
            total_token_amount: NormalizedAmount::new(token_amount),
            total_usdc_amount: UsdValue::new(usdc_amount),
            transaction_count: TransactionCount::new(1),
        }
    }

    #[test]
    fn test_cache_empty_get_returns_none() {
        let cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        let result = cache.get(token, 100, 200);
        assert!(result.is_none(), "Empty cache should return None");
    }

    #[test]
    fn test_cache_exact_match() {
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");
        let expected = create_price_result(token, 1000.0, 500.0);

        cache.insert(token, 100, 200, expected.clone());

        let result = cache.get(token, 100, 200);
        assert!(result.is_some(), "Should find exact match");
        let retrieved = result.unwrap();
        assert_eq!(
            retrieved.total_token_amount.as_f64(),
            expected.total_token_amount.as_f64()
        );
        assert_eq!(
            retrieved.total_usdc_amount.as_f64(),
            expected.total_usdc_amount.as_f64()
        );
    }

    #[test]
    fn test_get_does_not_return_wider_entry_for_narrower_query() {
        // A wider cached entry's aggregate sums blocks outside the query
        // window. `get` is exact-match-only so callers cannot accidentally
        // consume an over-counted aggregate.
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");
        cache.insert(token, 50, 250, create_price_result(token, 1000.0, 500.0));

        let result = cache.get(token, 100, 200);
        assert!(
            result.is_none(),
            "wider cached entry must not serve narrower query"
        );
    }

    #[test]
    fn test_cache_partial_overlap_returns_none() {
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");
        let cached = create_price_result(token, 1000.0, 500.0);

        // Cache blocks 100-200
        cache.insert(token, 100, 200, cached);

        // Request blocks 150-250 (partial overlap)
        let result = cache.get(token, 150, 250);
        assert!(
            result.is_none(),
            "Partial overlap should return None from get()"
        );
    }

    #[test]
    fn test_cache_different_token_returns_none() {
        let mut cache = PriceCache::default();
        let token1 = address!("0000000000000000000000000000000000000001");
        let token2 = address!("0000000000000000000000000000000000000002");
        let cached = create_price_result(token1, 1000.0, 500.0);

        cache.insert(token1, 100, 200, cached);

        let result = cache.get(token2, 100, 200);
        assert!(result.is_none(), "Different token should return None");
    }

    #[test]
    fn test_calculate_gaps_empty_cache() {
        let cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        let (result, gaps) = cache.calculate_gaps(token, 100, 200);

        assert!(result.is_none(), "Empty cache should return None result");
        assert_eq!(gaps.len(), 1, "Should have one gap covering entire range");
        assert_eq!(gaps[0], BlockRange::new(100, 200));
    }

    #[test]
    fn test_calculate_gaps_exact_match_fully_cached() {
        // An exact-match cached entry serves the query directly with no gaps.
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");
        let expected = create_price_result(token, 1000.0, 500.0);

        cache.insert(token, 100, 200, expected.clone());

        let (result, gaps) = cache.calculate_gaps(token, 100, 200);

        let retrieved = result.expect("exact-match query returns cached result");
        assert_eq!(
            retrieved.total_token_amount.as_f64(),
            expected.total_token_amount.as_f64()
        );
        assert_eq!(
            retrieved.total_usdc_amount.as_f64(),
            expected.total_usdc_amount.as_f64()
        );
        assert!(gaps.is_empty(), "No gaps when query matches a cached entry");
    }

    #[test]
    fn test_calculate_gaps_wider_entry_reports_whole_query_as_gap() {
        // A wider cached entry's aggregate covers blocks outside the query
        // window, so it cannot be used to answer the narrower query. The
        // whole window is reported as a gap so the caller rescans and
        // produces a window-scoped aggregate.
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        cache.insert(token, 50, 350, create_price_result(token, 1000.0, 500.0));

        let (result, gaps) = cache.calculate_gaps(token, 100, 300);
        assert!(
            result.is_none(),
            "wider cached entry must not contribute to narrower query"
        );
        assert_eq!(gaps, vec![BlockRange::new(100, 300)]);
    }

    #[test]
    fn test_calculate_gaps_single_gap_at_start() {
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");
        let cached = create_price_result(token, 1000.0, 500.0);

        // Cache blocks 150-250
        cache.insert(token, 150, 250, cached);

        // Request blocks 100-250
        let (result, gaps) = cache.calculate_gaps(token, 100, 250);

        assert!(result.is_some(), "Should merge cached data");
        assert_eq!(gaps.len(), 1, "Should have gap at start");
        assert_eq!(
            gaps[0],
            BlockRange::new(100, 149),
            "Gap should be from 100 to 149"
        );
    }

    #[test]
    fn test_calculate_gaps_single_gap_at_end() {
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");
        let cached = create_price_result(token, 1000.0, 500.0);

        // Cache blocks 100-150
        cache.insert(token, 100, 150, cached);

        // Request blocks 100-200
        let (result, gaps) = cache.calculate_gaps(token, 100, 200);

        assert!(result.is_some(), "Should merge cached data");
        assert_eq!(gaps.len(), 1, "Should have gap at end");
        assert_eq!(
            gaps[0],
            BlockRange::new(151, 200),
            "Gap should be from 151 to 200"
        );
    }

    #[test]
    fn test_calculate_gaps_middle_gap() {
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        // Cache blocks 100-150 and 200-250
        cache.insert(token, 100, 150, create_price_result(token, 500.0, 250.0));
        cache.insert(token, 200, 250, create_price_result(token, 800.0, 400.0));

        // Request blocks 100-250
        let (result, gaps) = cache.calculate_gaps(token, 100, 250);

        assert!(result.is_some(), "Should merge cached data");

        // Should have a gap in the middle
        assert_eq!(gaps.len(), 1, "Should have one gap in middle");
        assert_eq!(
            gaps[0],
            BlockRange::new(151, 199),
            "Gap should be from 151 to 199"
        );

        // Verify merged result has combined amounts
        let merged = result.unwrap();
        assert_eq!(merged.total_token_amount.as_f64(), 1300.0); // 500 + 800
        assert_eq!(merged.total_usdc_amount.as_f64(), 650.0); // 250 + 400
    }

    #[test]
    fn test_calculate_gaps_multiple_gaps() {
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        // Cache blocks: [100-150], [200-250], [300-350]
        cache.insert(token, 100, 150, create_price_result(token, 100.0, 50.0));
        cache.insert(token, 200, 250, create_price_result(token, 200.0, 100.0));
        cache.insert(token, 300, 350, create_price_result(token, 300.0, 150.0));

        // Request blocks 100-350
        let (result, gaps) = cache.calculate_gaps(token, 100, 350);

        assert!(result.is_some(), "Should merge all cached data");

        // Should have two gaps: 151-199 and 251-299
        assert_eq!(gaps.len(), 2, "Should have two gaps");
        assert_eq!(gaps[0], BlockRange::new(151, 199));
        assert_eq!(gaps[1], BlockRange::new(251, 299));

        // Verify merged result
        let merged = result.unwrap();
        assert_eq!(merged.total_token_amount.as_f64(), 600.0); // 100+200+300
        assert_eq!(merged.total_usdc_amount.as_f64(), 300.0); // 50+100+150
    }

    #[test]
    fn test_calculate_gaps_with_surrounding_gaps() {
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        // Cache blocks 200-300
        cache.insert(token, 200, 300, create_price_result(token, 1000.0, 500.0));

        // Request blocks 100-400 (gaps before and after cached range)
        let (result, gaps) = cache.calculate_gaps(token, 100, 400);

        assert!(result.is_some(), "Should include cached data");
        assert_eq!(gaps.len(), 2, "Should have gaps at start and end");
        assert_eq!(
            gaps[0],
            BlockRange::new(100, 199),
            "Gap before cached range"
        );
        assert_eq!(gaps[1], BlockRange::new(301, 400), "Gap after cached range");
    }

    #[test]
    fn test_insert_with_no_overlap() {
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        cache.insert(token, 100, 200, create_price_result(token, 100.0, 50.0));
        cache.insert(token, 300, 400, create_price_result(token, 200.0, 100.0));

        // Both ranges should be cached separately
        let result1 = cache.get(token, 100, 200);
        let result2 = cache.get(token, 300, 400);

        assert!(result1.is_some(), "First range should be cached");
        assert!(result2.is_some(), "Second range should be cached");
        assert_eq!(result1.unwrap().total_token_amount.as_f64(), 100.0);
        assert_eq!(result2.unwrap().total_token_amount.as_f64(), 200.0);
    }

    #[test]
    fn test_insert_partial_overlap_does_not_double_count() {
        // Two partially overlapping inserts must never produce a single cached
        // entry whose aggregate counts the overlapping blocks twice. The first
        // entry is kept; the second is dropped to preserve disjoint segments.
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        cache.insert(token, 100, 200, create_price_result(token, 500.0, 250.0));
        cache.insert(token, 150, 250, create_price_result(token, 800.0, 400.0));

        assert!(
            cache.get(token, 100, 250).is_none(),
            "no cached entry should claim coverage of [100, 250]"
        );
        let kept = cache
            .get(token, 100, 200)
            .expect("original range preserved");
        assert_eq!(kept.total_token_amount.as_f64(), 500.0);
        assert_eq!(kept.total_usdc_amount.as_f64(), 250.0);
    }

    #[test]
    fn test_covering_insert_replaces_prior_segments() {
        // Calculator pattern: a final aggregate that covers prior gap-fill
        // inserts must collapse those entries to a single authoritative entry
        // instead of being re-merged into them.
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        cache.insert(token, 100, 150, create_price_result(token, 100.0, 50.0));
        cache.insert(token, 200, 250, create_price_result(token, 200.0, 100.0));

        // Caller-aggregated total for [100, 250] including both prior segments
        // and the rescanned middle gap.
        cache.insert(token, 100, 250, create_price_result(token, 450.0, 225.0));

        let stored = cache.get(token, 100, 250).expect("covering range cached");
        assert_eq!(stored.total_token_amount.as_f64(), 450.0);
        assert_eq!(stored.total_usdc_amount.as_f64(), 225.0);
        let (_, gaps) = cache.calculate_gaps(token, 100, 250);
        assert!(gaps.is_empty());
    }

    #[test]
    fn test_insert_adjacent_ranges_no_merge() {
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        // Insert adjacent ranges: 100-200 and 201-300 (no overlap, but contiguous)
        cache.insert(token, 100, 200, create_price_result(token, 100.0, 50.0));
        cache.insert(token, 201, 300, create_price_result(token, 200.0, 100.0));

        // Adjacent ranges don't overlap, so they won't be merged by get()
        let result = cache.get(token, 100, 300);
        assert!(
            result.is_none(),
            "Adjacent ranges (no overlap) are not merged by get()"
        );

        // calculate_gaps should merge the results and find no gaps (ranges are contiguous)
        let (merged_result, gaps) = cache.calculate_gaps(token, 100, 300);
        assert!(
            merged_result.is_some(),
            "calculate_gaps should merge adjacent ranges"
        );
        assert_eq!(gaps.len(), 0, "No gaps - ranges are contiguous");

        // Verify the merged result has combined amounts
        let merged = merged_result.unwrap();
        assert_eq!(merged.total_token_amount.as_f64(), 300.0); // 100 + 200
        assert_eq!(merged.total_usdc_amount.as_f64(), 150.0); // 50 + 100
    }

    #[test]
    fn test_partial_overlap_insert_leaves_prior_disjoint_segments() {
        // A new range that only partially overlaps existing segments (here the
        // outer two) cannot be safely combined with their aggregates, so it is
        // dropped. The disjoint segments are preserved and `calculate_gaps`
        // still reports the real uncached blocks.
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        cache.insert(token, 100, 150, create_price_result(token, 100.0, 50.0));
        cache.insert(token, 200, 250, create_price_result(token, 200.0, 100.0));
        cache.insert(token, 300, 350, create_price_result(token, 300.0, 150.0));

        // Partially overlaps [100, 150] and [300, 350]; fully contains [200, 250].
        cache.insert(token, 140, 340, create_price_result(token, 999.0, 999.0));

        let (merged, gaps) = cache.calculate_gaps(token, 100, 350);
        let merged = merged.expect("cached segments are merged for query");
        // Only the three original segments contribute, no double-count of the
        // dropped partial-overlap insert.
        assert_eq!(merged.total_token_amount.as_f64(), 600.0);
        assert_eq!(merged.total_usdc_amount.as_f64(), 300.0);
        assert_eq!(
            gaps,
            vec![BlockRange::new(151, 199), BlockRange::new(251, 299)]
        );
    }

    #[test]
    fn test_edge_case_zero_length_range() {
        let cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        // Request zero-length range (same start and end)
        let (result, gaps) = cache.calculate_gaps(token, 100, 100);

        assert!(result.is_none(), "Empty cache returns None");
        assert_eq!(gaps.len(), 1, "Should have one gap");
        assert_eq!(
            gaps[0],
            BlockRange::new(100, 100),
            "Gap covers the single block"
        );
    }

    #[test]
    fn test_edge_case_large_block_numbers() {
        let mut cache = PriceCache::default();
        let token = address!("0000000000000000000000000000000000000001");

        // Use large block numbers (realistic for Arbitrum, etc.)
        let large_block = 250_000_000_u64;
        cache.insert(
            token,
            large_block,
            large_block + 1000,
            create_price_result(token, 1000.0, 500.0),
        );

        let result = cache.get(token, large_block, large_block + 1000);
        assert!(result.is_some(), "Should handle large block numbers");
    }

    #[test]
    fn test_multiple_tokens_isolated() {
        let mut cache = PriceCache::default();
        let token1 = address!("0000000000000000000000000000000000000001");
        let token2 = address!("0000000000000000000000000000000000000002");

        cache.insert(token1, 100, 200, create_price_result(token1, 100.0, 50.0));
        cache.insert(token2, 100, 200, create_price_result(token2, 200.0, 100.0));

        let result1 = cache.get(token1, 100, 200);
        let result2 = cache.get(token2, 100, 200);

        assert!(result1.is_some(), "Token 1 should be cached");
        assert!(result2.is_some(), "Token 2 should be cached");

        // Verify they have different values
        assert_eq!(result1.unwrap().total_token_amount.as_f64(), 100.0);
        assert_eq!(result2.unwrap().total_token_amount.as_f64(), 200.0);
    }

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        /// Strategy for generating valid block ranges
        fn block_range_strategy() -> impl Strategy<Value = (BlockNumber, BlockNumber)> {
            (0u64..100_000u64)
                .prop_flat_map(|start| (Just(start), start..start.saturating_add(10_000)))
        }

        /// Strategy for generating multiple non-overlapping cached ranges
        fn cached_ranges_strategy() -> impl Strategy<Value = Vec<(BlockNumber, BlockNumber)>> {
            prop::collection::vec(block_range_strategy(), 0..10).prop_map(|mut ranges| {
                // Sort and make them non-overlapping
                ranges.sort_by_key(|(start, _)| *start);
                let mut non_overlapping = Vec::new();
                let mut last_end = 0u64;

                for (start, end) in ranges {
                    let adjusted_start = start.max(last_end + 2);
                    if adjusted_start < end {
                        non_overlapping.push((adjusted_start, end));
                        last_end = end;
                    }
                }

                non_overlapping
            })
        }

        proptest! {
            /// Property: Gaps should never overlap with cached ranges fully inside the query window
            ///
            /// Only fully-within cached ranges contribute to the merged result;
            /// partial-overlap ranges are ignored and their overlap region is
            /// reported as a gap so the caller can produce a window-scoped
            /// aggregate by rescanning.
            #[test]
            fn test_gaps_never_overlap_with_within_query_cached(
                cached_ranges in cached_ranges_strategy(),
                (query_start, query_end) in block_range_strategy()
            ) {
                let mut cache = PriceCache::default();
                let token = address!("0000000000000000000000000000000000000001");

                for (start, end) in &cached_ranges {
                    cache.insert(token, *start, *end, create_price_result(token, 1000.0, 500.0));
                }

                let (_, gaps) = cache.calculate_gaps(token, query_start, query_end);

                for gap in &gaps {
                    for (cached_start, cached_end) in &cached_ranges {
                        // Only fully-within cached ranges contribute; skip the rest.
                        if *cached_start < query_start || *cached_end > query_end {
                            continue;
                        }

                        let no_overlap = gap.end < *cached_start || gap.start > *cached_end;
                        prop_assert!(
                            no_overlap,
                            "Gap [{}, {}] overlaps with within-query cached range [{cached_start}, {cached_end}]",
                            gap.start, gap.end
                        );
                    }
                }
            }

            /// Property: All gaps should be sorted by start block
            #[test]
            fn test_gaps_are_sorted(
                cached_ranges in cached_ranges_strategy(),
                (query_start, query_end) in block_range_strategy()
            ) {
                let mut cache = PriceCache::default();
                let token = address!("0000000000000000000000000000000000000001");

                // Insert cached ranges
                for (start, end) in &cached_ranges {
                    cache.insert(token, *start, *end, create_price_result(token, 1000.0, 500.0));
                }

                // Calculate gaps
                let (_, gaps) = cache.calculate_gaps(token, query_start, query_end);

                // Verify gaps are sorted
                for i in 1..gaps.len() {
                    prop_assert!(
                        gaps[i - 1].start < gaps[i].start,
                        "Gaps not sorted: gap[{i_prev}] = {prev:?}, gap[{i}] = {curr:?}",
                        i_prev = i - 1,
                        prev = gaps[i - 1],
                        curr = gaps[i]
                    );
                }
            }

            /// Property: Gaps should cover entire uncached space within the query range
            #[test]
            fn test_gaps_cover_uncached_space(
                cached_ranges in cached_ranges_strategy(),
                (query_start, query_end) in block_range_strategy()
            ) {
                let mut cache = PriceCache::default();
                let token = address!("0000000000000000000000000000000000000001");

                // Insert cached ranges
                for (start, end) in &cached_ranges {
                    cache.insert(token, *start, *end, create_price_result(token, 1000.0, 500.0));
                }

                // Calculate gaps
                let (_, gaps) = cache.calculate_gaps(token, query_start, query_end);

                // Build a set of all blocks that are either cached or in gaps
                let mut covered_blocks = std::collections::HashSet::new();

                // Add cached blocks (within query range)
                for (cached_start, cached_end) in &cached_ranges {
                    let start = (*cached_start).max(query_start);
                    let end = (*cached_end).min(query_end);
                    if start <= end {
                        for block in start..=end {
                            covered_blocks.insert(block);
                        }
                    }
                }

                // Add gap blocks
                for gap in &gaps {
                    for block in gap.start..=gap.end {
                        covered_blocks.insert(block);
                    }
                }

                // Verify all blocks in query range are covered
                for block in query_start..=query_end {
                    prop_assert!(
                        covered_blocks.contains(&block),
                        "Block {block} in range [{query_start}, {query_end}] is not covered by cache or gaps"
                    );
                }
            }

            /// Property: Gaps should not overlap with each other
            #[test]
            fn test_gaps_dont_overlap_each_other(
                cached_ranges in cached_ranges_strategy(),
                (query_start, query_end) in block_range_strategy()
            ) {
                let mut cache = PriceCache::default();
                let token = address!("0000000000000000000000000000000000000001");

                // Insert cached ranges
                for (start, end) in &cached_ranges {
                    cache.insert(token, *start, *end, create_price_result(token, 1000.0, 500.0));
                }

                // Calculate gaps
                let (_, gaps) = cache.calculate_gaps(token, query_start, query_end);

                // Verify no gap overlaps with another gap
                for i in 0..gaps.len() {
                    for j in (i + 1)..gaps.len() {
                        let gap_i = gaps[i];
                        let gap_j = gaps[j];

                        let no_overlap = gap_i.end < gap_j.start || gap_j.end < gap_i.start;
                        prop_assert!(
                            no_overlap,
                            "Gap {i} [{}, {}] overlaps with gap {j} [{}, {}]",
                            gap_i.start, gap_i.end, gap_j.start, gap_j.end
                        );
                    }
                }
            }

            /// Property: When cache is empty, should return entire query range as gap
            #[test]
            fn test_empty_cache_returns_full_range(
                (query_start, query_end) in block_range_strategy()
            ) {
                let cache = PriceCache::default();
                let token = address!("0000000000000000000000000000000000000001");

                let (result, gaps) = cache.calculate_gaps(token, query_start, query_end);

                prop_assert!(result.is_none(), "Empty cache should return None result");
                prop_assert_eq!(gaps.len(), 1, "Empty cache should return exactly one gap");
                prop_assert_eq!(gaps[0], BlockRange::new(query_start, query_end), "Gap should cover entire query range");
            }

            /// Property: When a cached entry exactly matches the query, return no gaps
            #[test]
            fn test_exact_match_returns_no_gaps(
                (start, end) in block_range_strategy()
            ) {
                let mut cache = PriceCache::default();
                let token = address!("0000000000000000000000000000000000000001");

                cache.insert(token, start, end, create_price_result(token, 1000.0, 500.0));

                let (result, gaps) = cache.calculate_gaps(token, start, end);

                prop_assert!(result.is_some(), "Exact-match query should return a cached result");
                prop_assert_eq!(gaps.len(), 0, "Exact-match query should return no gaps");
            }

            /// Property: A wider cached entry never satisfies a strictly narrower query;
            /// the whole inner window is reported as a single gap.
            #[test]
            fn test_wider_entry_does_not_satisfy_narrower_query(
                (inner_start, inner_end) in block_range_strategy()
            ) {
                prop_assume!(inner_start >= 10);

                let mut cache = PriceCache::default();
                let token = address!("0000000000000000000000000000000000000001");

                let cache_start = inner_start - 10;
                let cache_end = inner_end.saturating_add(10);

                cache.insert(token, cache_start, cache_end, create_price_result(token, 1000.0, 500.0));

                let (result, gaps) = cache.calculate_gaps(token, inner_start, inner_end);

                prop_assert!(
                    result.is_none(),
                    "wider cached entry must not contribute to a narrower query"
                );
                prop_assert_eq!(gaps, vec![BlockRange::new(inner_start, inner_end)]);
            }
        }
    }
}
