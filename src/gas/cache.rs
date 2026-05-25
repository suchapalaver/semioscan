// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! In-memory cache for gas cost calculations with gap detection
//!
//! This module provides caching for gas cost calculations that supports:
//! - Disjoint storage of cached block ranges (so aggregate costs are never
//!   silently double-counted when ranges overlap)
//! - Gap detection to identify uncached regions
//! - Cache invalidation by address or block height
//!
//! # Use Cases
//!
//! - **Avoid redundant RPC calls**: Cache gas calculations to prevent re-scanning the same blocks
//! - **Incremental updates**: Add new block ranges and automatically merge with existing data
//! - **Gap filling**: Identify precisely which block ranges still need to be scanned
//!
//! # Example: Basic caching
//!
//! ```rust
//! use semioscan::{GasCache, GasCostResult, WeiAmount};
//! use alloy_chains::NamedChain;
//! use alloy_primitives::Address;
//!
//! let mut cache = GasCache::default();
//! let from = Address::ZERO;
//! let to = Address::ZERO;
//!
//! // Insert a result for blocks 100-200
//! let mut result = GasCostResult::new(NamedChain::Mainnet, from, to);
//! result.total_gas_cost = WeiAmount::from(1_000_000u64);
//! cache.insert(from, to, 100, 200, result);
//!
//! // Retrieve it
//! let cached = cache.get(from, to, 100, 200);
//! assert!(cached.is_some());
//! ```
//!
//! # Example: Gap detection
//!
//! ```rust
//! use semioscan::{GasCache, GasCostResult};
//! use alloy_chains::NamedChain;
//! use alloy_primitives::Address;
//!
//! let mut cache = GasCache::default();
//! let from = Address::ZERO;
//! let to = Address::ZERO;
//!
//! // Cache blocks 100-200 and 300-400
//! cache.insert(from, to, 100, 200, GasCostResult::new(NamedChain::Mainnet, from, to));
//! cache.insert(from, to, 300, 400, GasCostResult::new(NamedChain::Mainnet, from, to));
//!
//! // Find gaps in range 50-500
//! let (cached, gaps) = cache.calculate_gaps(NamedChain::Mainnet, from, to, 50, 500);
//!
//! // Gaps: [50, 99], [201, 299], [401, 500]
//! assert_eq!(gaps.len(), 3);
//! assert_eq!(gaps[0], (50, 99));
//! assert_eq!(gaps[1], (201, 299));
//! assert_eq!(gaps[2], (401, 500));
//! ```

use alloy_chains::NamedChain;
use alloy_primitives::{Address, BlockNumber};

use crate::cache::block_range::{BlockRangeCache, Mergeable};
use crate::gas::calculator::GasCostResult;

// Implement Mergeable for GasCostResult
impl Mergeable for GasCostResult {
    fn merge(&mut self, other: &Self) {
        GasCostResult::merge(self, other);
    }
}

/// In-memory cache for gas cost calculation results
///
/// Stores gas cost data keyed by `(from, to, start_block, end_block)`. Cached
/// ranges for the same address pair are kept disjoint so aggregates are never
/// double-counted when a query overlaps prior inserts. A cached aggregate is
/// only used to answer a query whose window is exactly the entry's range or
/// is exactly tiled by entries fully inside the window — a wider cached
/// entry is never returned for a narrower query, because its total covers
/// blocks the caller did not ask about.
///
/// # Features
///
/// - **Exact-match lookup**: [`Self::get`] returns a cached value only when an
///   entry's range exactly matches the query
/// - **Disjoint storage**: Inserts that partially overlap existing entries are
///   resolved without combining their aggregates (see [`Self::insert`])
/// - **Gap detection**: Calculate precisely which blocks are not yet cached,
///   using only cached entries inside the query window
/// - **Cache management**: Clear by address or block height
#[derive(Debug, Clone, Default)]
pub struct GasCache {
    inner: BlockRangeCache<(Address, Address), GasCostResult>,
}

impl GasCache {
    /// Retrieve the cached result whose range exactly matches the query
    ///
    /// Cached aggregates summarise the blocks they were computed over and
    /// cannot be scoped down to a narrower window, so `get` only returns a
    /// value when an entry's range is exactly `(start_block, end_block)`.
    /// Use [`Self::calculate_gaps`] for gap-aware lookup that combines
    /// disjoint entries lying inside the query window.
    ///
    /// # Arguments
    ///
    /// * `from` - Source address
    /// * `to` - Destination address
    /// * `start_block` - Start of requested range (inclusive)
    /// * `end_block` - End of requested range (inclusive)
    ///
    /// # Returns
    ///
    /// - `Some(result)`: An entry exists with this exact range
    /// - `None`: No exact-match entry; wider or narrower entries are not returned
    ///
    /// # Example
    ///
    /// ```rust
    /// use semioscan::{GasCache, GasCostResult};
    /// use alloy_chains::NamedChain;
    /// use alloy_primitives::Address;
    ///
    /// let mut cache = GasCache::default();
    /// let from = Address::ZERO;
    /// let to = Address::ZERO;
    ///
    /// cache.insert(from, to, 100, 300, GasCostResult::new(NamedChain::Mainnet, from, to));
    ///
    /// // Exact match - returns cached data
    /// assert!(cache.get(from, to, 100, 300).is_some());
    ///
    /// // Subset query - returns None; use calculate_gaps for gap-aware lookup
    /// assert!(cache.get(from, to, 150, 250).is_none());
    ///
    /// // Superset query - returns None
    /// assert!(cache.get(from, to, 50, 350).is_none());
    /// ```
    pub fn get(
        &self,
        from: Address,
        to: Address,
        start_block: BlockNumber,
        end_block: BlockNumber,
    ) -> Option<GasCostResult> {
        self.inner.get(&(from, to), start_block, end_block)
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
    ///
    /// # Arguments
    ///
    /// * `from` - Source address
    /// * `to` - Destination address
    /// * `start_block` - Start of block range (inclusive)
    /// * `end_block` - End of block range (inclusive)
    /// * `result` - Gas cost data for this range
    pub fn insert(
        &mut self,
        from: Address,
        to: Address,
        start_block: BlockNumber,
        end_block: BlockNumber,
        result: GasCostResult,
    ) {
        self.inner
            .insert((from, to), start_block, end_block, result);
    }

    /// Calculate uncached block ranges (gaps) and return merged cached data
    ///
    /// This is the key method for incremental scanning. Only cached entries
    /// whose range lies fully inside `[start_block, end_block]` contribute to
    /// the merged result — a wider entry that extends outside the query
    /// window is ignored, and the corresponding blocks are reported as a
    /// gap so the caller can rescan and produce a window-scoped aggregate.
    ///
    /// # Behavior
    ///
    /// 1. If no inside-window entries exist, returns `(None, vec![(start, end)])`
    /// 2. If inside-window entries exactly tile `[start, end]`, returns `(Some(merged), vec![])`
    /// 3. Otherwise returns the merged value of all inside-window entries plus
    ///    the gaps that remain inside `[start, end]`
    ///
    /// # Arguments
    ///
    /// * `chain` - Chain (used when creating merged result)
    /// * `from` - Source address
    /// * `to` - Destination address
    /// * `start_block` - Start of requested range (inclusive)
    /// * `end_block` - End of requested range (inclusive)
    ///
    /// # Returns
    ///
    /// A tuple of:
    /// - `Option<GasCostResult>`: Merged data from all cached entries inside the query window
    /// - `Vec<(BlockNumber, BlockNumber)>`: Sorted list of uncached ranges (gaps) to scan
    ///
    /// # Example
    ///
    /// ```rust
    /// use semioscan::{GasCache, GasCostResult};
    /// use alloy_chains::NamedChain;
    /// use alloy_primitives::Address;
    ///
    /// let mut cache = GasCache::default();
    /// let from = Address::ZERO;
    /// let to = Address::ZERO;
    ///
    /// // Cache two ranges with a gap
    /// cache.insert(from, to, 100, 200, GasCostResult::new(NamedChain::Mainnet, from, to));
    /// cache.insert(from, to, 300, 400, GasCostResult::new(NamedChain::Mainnet, from, to));
    ///
    /// // Request range [50, 500]
    /// let (cached, gaps) = cache.calculate_gaps(NamedChain::Mainnet, from, to, 50, 500);
    ///
    /// // We get cached data and three gaps to fill
    /// assert!(cached.is_some());
    /// assert_eq!(gaps, vec![
    ///     (50, 99),    // Before first cached range
    ///     (201, 299),  // Between cached ranges
    ///     (401, 500),  // After last cached range
    /// ]);
    /// ```
    pub fn calculate_gaps(
        &self,
        chain: NamedChain,
        from: Address,
        to: Address,
        start_block: BlockNumber,
        end_block: BlockNumber,
    ) -> (Option<GasCostResult>, Vec<(BlockNumber, BlockNumber)>) {
        self.inner
            .calculate_gaps(&(from, to), start_block, end_block, || {
                GasCostResult::new(chain, from, to)
            })
    }

    /// Clear all cached data for a specific address pair
    ///
    /// Removes all entries where transactions were sent from `from` to `to`.
    /// Useful when you want to invalidate cached data for a specific route.
    ///
    /// # Example
    ///
    /// ```rust
    /// use semioscan::{GasCache, GasCostResult};
    /// use alloy_chains::NamedChain;
    /// use alloy_primitives::{Address, address};
    ///
    /// let mut cache = GasCache::default();
    /// let addr1 = address!("0x1111111111111111111111111111111111111111");
    /// let addr2 = address!("0x2222222222222222222222222222222222222222");
    ///
    /// cache.insert(addr1, addr2, 100, 200, GasCostResult::new(NamedChain::Mainnet, addr1, addr2));
    /// assert_eq!(cache.len(), 1);
    ///
    /// cache.clear_signer_data(addr1, addr2);
    /// assert_eq!(cache.len(), 0);
    /// ```
    pub fn clear_signer_data(&mut self, from: Address, to: Address) {
        self.inner
            .retain(|(cached_from, cached_to), _, _| *cached_from != from || *cached_to != to);
    }

    /// Clear all cached entries that end before a minimum block height
    ///
    /// Useful for invalidating old data when you know earlier blocks
    /// are no longer relevant (e.g., after a blockchain reorganization).
    ///
    /// # Arguments
    ///
    /// * `min_block` - Minimum block height to keep (entries ending before this are removed)
    ///
    /// # Example
    ///
    /// ```rust
    /// use semioscan::{GasCache, GasCostResult};
    /// use alloy_chains::NamedChain;
    /// use alloy_primitives::Address;
    ///
    /// let mut cache = GasCache::default();
    /// let from = Address::ZERO;
    /// let to = Address::ZERO;
    ///
    /// cache.insert(from, to, 100, 200, GasCostResult::new(NamedChain::Mainnet, from, to));
    /// cache.insert(from, to, 500, 600, GasCostResult::new(NamedChain::Mainnet, from, to));
    /// assert_eq!(cache.len(), 2);
    ///
    /// // Clear entries ending before block 300
    /// cache.clear_old_blocks(300);
    /// assert_eq!(cache.len(), 1); // Only [500, 600] remains
    /// ```
    pub fn clear_old_blocks(&mut self, min_block: BlockNumber) {
        self.inner.retain(|_, _, end_block| end_block >= min_block);
    }

    /// Get the total number of cached entries
    ///
    /// # Example
    ///
    /// ```rust
    /// use semioscan::{GasCache, GasCostResult};
    /// use alloy_chains::NamedChain;
    /// use alloy_primitives::Address;
    ///
    /// let mut cache = GasCache::default();
    /// assert_eq!(cache.len(), 0);
    ///
    /// cache.insert(Address::ZERO, Address::ZERO, 100, 200, GasCostResult::new(NamedChain::Mainnet, Address::ZERO, Address::ZERO));
    /// assert_eq!(cache.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if the cache contains no entries
    ///
    /// # Example
    ///
    /// ```rust
    /// use semioscan::{GasCache, GasCostResult};
    /// use alloy_primitives::Address;
    ///
    /// let cache = GasCache::default();
    /// assert!(cache.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TransactionCount, WeiAmount};
    use alloy_chains::NamedChain;
    use alloy_primitives::Address;

    fn create_test_result(
        chain: NamedChain,
        from: Address,
        to: Address,
        tx_count: usize,
        gas_cost: u64,
    ) -> GasCostResult {
        let mut result = GasCostResult::new(chain, from, to);
        result.transaction_count = TransactionCount::new(tx_count);
        result.total_gas_cost = WeiAmount::from(gas_cost);
        result
    }

    #[test]
    fn test_cache_insert_and_get() {
        let mut cache = GasCache::default();
        let from = Address::ZERO;
        let to = Address::ZERO;

        let result = create_test_result(NamedChain::Mainnet, from, to, 5, 100_000);
        cache.insert(from, to, 100, 200, result.clone());

        // Exact match returns the cached value.
        let cached = cache.get(from, to, 100, 200);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().transaction_count, TransactionCount::new(5));

        // Subset query does not return the wider entry: its aggregate
        // sums blocks outside [120, 180] and cannot be scoped down.
        assert!(cache.get(from, to, 120, 180).is_none());

        // Superset query is not satisfied by the narrower entry either.
        assert!(cache.get(from, to, 50, 300).is_none());
    }

    #[test]
    fn test_calculate_gaps() {
        let mut cache = GasCache::default();
        let from = Address::ZERO;
        let to = Address::ZERO;

        // Insert a few ranges with gaps
        cache.insert(
            from,
            to,
            100,
            200,
            create_test_result(NamedChain::Mainnet, from, to, 5, 100_000),
        );
        cache.insert(
            from,
            to,
            300,
            400,
            create_test_result(NamedChain::Mainnet, from, to, 3, 60_000),
        );
        cache.insert(
            from,
            to,
            600,
            700,
            create_test_result(NamedChain::Mainnet, from, to, 2, 40_000),
        );

        // Calculate gaps for a range that covers all cached ranges
        let (result, gaps) = cache.calculate_gaps(NamedChain::Mainnet, from, to, 50, 800);
        assert!(result.is_some());

        // Expected gaps: 50-99, 201-299, 401-599, 701-800
        assert_eq!(gaps.len(), 4);
        assert_eq!(gaps[0], (50, 99));
        assert_eq!(gaps[1], (201, 299));
        assert_eq!(gaps[2], (401, 599));
        assert_eq!(gaps[3], (701, 800));

        // Merged result should have 10 transactions
        assert_eq!(result.unwrap().transaction_count, TransactionCount::new(10));
    }

    #[test]
    fn test_partial_overlap_does_not_double_count() {
        // Two partially overlapping inserts must never collapse into a single
        // aggregate that counts the overlapping blocks twice. The earlier entry
        // is kept untouched.
        let mut cache = GasCache::default();
        let from = Address::ZERO;
        let to = Address::ZERO;

        cache.insert(
            from,
            to,
            100,
            300,
            create_test_result(NamedChain::Mainnet, from, to, 5, 100_000),
        );
        cache.insert(
            from,
            to,
            250,
            400,
            create_test_result(NamedChain::Mainnet, from, to, 3, 60_000),
        );

        assert!(
            cache.get(from, to, 100, 400).is_none(),
            "no cached entry should claim coverage of [100, 400]"
        );
        let kept = cache
            .get(from, to, 100, 300)
            .expect("original range preserved");
        assert_eq!(kept.transaction_count, TransactionCount::new(5));
        assert_eq!(kept.total_gas_cost, WeiAmount::from(100_000u64));
    }

    #[test]
    fn test_covering_insert_replaces_prior_segments() {
        // Mirrors the calculator pattern: scan two disjoint gaps, then write
        // back a single aggregate for the full query range. The cache must
        // collapse the prior segments into the new authoritative entry rather
        // than re-merge them.
        let mut cache = GasCache::default();
        let from = Address::ZERO;
        let to = Address::ZERO;

        cache.insert(
            from,
            to,
            100,
            150,
            create_test_result(NamedChain::Mainnet, from, to, 2, 20_000),
        );
        cache.insert(
            from,
            to,
            200,
            250,
            create_test_result(NamedChain::Mainnet, from, to, 3, 30_000),
        );

        // Caller already merged the prior cache and the rescanned gap into this
        // result; passing it straight back must not be added to the prior
        // entries.
        let aggregated = create_test_result(NamedChain::Mainnet, from, to, 7, 90_000);
        cache.insert(from, to, 100, 250, aggregated);

        assert_eq!(cache.len(), 1);
        let stored = cache.get(from, to, 100, 250).unwrap();
        assert_eq!(stored.transaction_count, TransactionCount::new(7));
        assert_eq!(stored.total_gas_cost, WeiAmount::from(90_000u64));
    }

    #[test]
    fn test_cache_merge_preserves_gas_breakdown() {
        // Caching gas data must not silently drop the breakdown - regression
        // for Mergeable<GasCostResult> only updating total/count.
        use crate::types::gas::{BlobCount, GasBreakdown};
        use alloy_primitives::U256;

        let mut cache = GasCache::default();
        let from = Address::ZERO;
        let to = Address::ZERO;

        let mut first = create_test_result(NamedChain::Mainnet, from, to, 1, 1_000);
        first.breakdown = GasBreakdown::builder()
            .execution_gas_cost(U256::from(700u64))
            .blob_gas_cost(U256::from(200u64))
            .l1_data_fee(U256::from(100u64))
            .blob_count(BlobCount::new(2))
            .build();
        cache.insert(from, to, 100, 200, first);

        let mut second = create_test_result(NamedChain::Mainnet, from, to, 1, 500);
        second.breakdown = GasBreakdown::builder()
            .execution_gas_cost(U256::from(400u64))
            .blob_gas_cost(U256::from(50u64))
            .l1_data_fee(U256::from(50u64))
            .blob_count(BlobCount::new(1))
            .build();
        cache.insert(from, to, 300, 400, second);

        let (merged, gaps) = cache.calculate_gaps(NamedChain::Mainnet, from, to, 100, 400);
        let merged = merged.expect("cached segments are merged for the query");
        assert_eq!(gaps, vec![(201, 299)]);
        assert_eq!(merged.breakdown.execution_gas_cost, U256::from(1_100u64));
        assert_eq!(merged.breakdown.blob_gas_cost, U256::from(250u64));
        assert_eq!(merged.breakdown.l1_data_fee, U256::from(150u64));
        assert_eq!(merged.breakdown.blob_count, BlobCount::new(3));
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
            /// aggregate by rescanning. So we only assert non-overlap against
            /// the entries that actually contribute.
            #[test]
            fn test_gaps_never_overlap_with_within_query_cached(
                cached_ranges in cached_ranges_strategy(),
                (query_start, query_end) in block_range_strategy()
            ) {
                let mut cache = GasCache::default();
                let from = Address::ZERO;
                let to = Address::ZERO;
                let chain = NamedChain::Mainnet;

                for (start, end) in &cached_ranges {
                    cache.insert(from, to, *start, *end, create_test_result(chain, from, to, 1, 1000));
                }

                let (_, gaps) = cache.calculate_gaps(chain, from, to, query_start, query_end);

                for (gap_start, gap_end) in &gaps {
                    for (cached_start, cached_end) in &cached_ranges {
                        // Only fully-within cached ranges contribute; skip the rest.
                        if *cached_start < query_start || *cached_end > query_end {
                            continue;
                        }

                        let no_overlap = *gap_end < *cached_start || *gap_start > *cached_end;
                        prop_assert!(
                            no_overlap,
                            "Gap [{gap_start}, {gap_end}] overlaps with within-query cached range [{cached_start}, {cached_end}]"
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
                let mut cache = GasCache::default();
                let from = Address::ZERO;
                let to = Address::ZERO;
                let chain = NamedChain::Mainnet;

                // Insert cached ranges
                for (start, end) in &cached_ranges {
                    cache.insert(from, to, *start, *end, create_test_result(chain, from, to, 1, 1000));
                }

                // Calculate gaps
                let (_, gaps) = cache.calculate_gaps(chain, from, to, query_start, query_end);

                // Verify gaps are sorted
                for i in 1..gaps.len() {
                    prop_assert!(
                        gaps[i - 1].0 < gaps[i].0,
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
                let mut cache = GasCache::default();
                let from = Address::ZERO;
                let to = Address::ZERO;
                let chain = NamedChain::Mainnet;

                // Insert cached ranges
                for (start, end) in &cached_ranges {
                    cache.insert(from, to, *start, *end, create_test_result(chain, from, to, 1, 1000));
                }

                // Calculate gaps
                let (_, gaps) = cache.calculate_gaps(chain, from, to, query_start, query_end);

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
                for (gap_start, gap_end) in &gaps {
                    for block in *gap_start..=*gap_end {
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
                let mut cache = GasCache::default();
                let from = Address::ZERO;
                let to = Address::ZERO;
                let chain = NamedChain::Mainnet;

                // Insert cached ranges
                for (start, end) in &cached_ranges {
                    cache.insert(from, to, *start, *end, create_test_result(chain, from, to, 1, 1000));
                }

                // Calculate gaps
                let (_, gaps) = cache.calculate_gaps(chain, from, to, query_start, query_end);

                // Verify no gap overlaps with another gap
                for i in 0..gaps.len() {
                    for j in (i + 1)..gaps.len() {
                        let (gap_i_start, gap_i_end) = gaps[i];
                        let (gap_j_start, gap_j_end) = gaps[j];

                        let no_overlap = gap_i_end < gap_j_start || gap_j_end < gap_i_start;
                        prop_assert!(
                            no_overlap,
                            "Gap {i} [{gap_i_start}, {gap_i_end}] overlaps with gap {j} [{gap_j_start}, {gap_j_end}]"
                        );
                    }
                }
            }

            /// Property: When cache is empty, should return entire query range as gap
            #[test]
            fn test_empty_cache_returns_full_range(
                (query_start, query_end) in block_range_strategy()
            ) {
                let cache = GasCache::default();
                let from = Address::ZERO;
                let to = Address::ZERO;
                let chain = NamedChain::Mainnet;

                let (result, gaps) = cache.calculate_gaps(chain, from, to, query_start, query_end);

                prop_assert!(result.is_none(), "Empty cache should return None result");
                prop_assert_eq!(gaps.len(), 1, "Empty cache should return exactly one gap");
                prop_assert_eq!(gaps[0], (query_start, query_end), "Gap should cover entire query range");
            }

            /// Property: When a cached entry exactly matches the query, return no gaps
            #[test]
            fn test_exact_match_returns_no_gaps(
                (start, end) in block_range_strategy()
            ) {
                let mut cache = GasCache::default();
                let from = Address::ZERO;
                let to = Address::ZERO;
                let chain = NamedChain::Mainnet;

                cache.insert(from, to, start, end, create_test_result(chain, from, to, 1, 1000));

                let (result, gaps) = cache.calculate_gaps(chain, from, to, start, end);

                prop_assert!(result.is_some(), "Exact-match query should return a cached result");
                prop_assert_eq!(gaps.len(), 0, "Exact-match query should return no gaps");
            }

            /// Property: A wider cached entry never satisfies a strictly narrower query;
            /// the whole inner window is reported as a single gap.
            #[test]
            fn test_wider_entry_does_not_satisfy_narrower_query(
                (inner_start, inner_end) in block_range_strategy()
            ) {
                // Skip the edge case where padding would underflow or where
                // the padded outer range equals the inner range.
                prop_assume!(inner_start >= 10);

                let mut cache = GasCache::default();
                let from = Address::ZERO;
                let to = Address::ZERO;
                let chain = NamedChain::Mainnet;

                let cache_start = inner_start - 10;
                let cache_end = inner_end.saturating_add(10);

                cache.insert(from, to, cache_start, cache_end, create_test_result(chain, from, to, 1, 1000));

                let (result, gaps) = cache.calculate_gaps(chain, from, to, inner_start, inner_end);

                prop_assert!(
                    result.is_none(),
                    "wider cached entry must not contribute to a narrower query"
                );
                prop_assert_eq!(gaps, vec![(inner_start, inner_end)], "whole window is reported as gap");
            }
        }
    }
}
