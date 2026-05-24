// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Block window calculation for mapping UTC dates to blockchain block ranges
//!
//! This module provides tools for calculating which blockchain blocks correspond to
//! a specific UTC date. This is useful for analyzing blockchain data by date rather
//! than by block number.
//!
//! # Caching
//!
//! Block windows are automatically cached to disk to avoid repeated RPC calls for
//! the same date. The cache is stored as JSON and persists across program runs.
//!
//! # Examples
//!
//! ```rust,ignore
//! use semioscan::BlockWindowCalculator;
//! use alloy_provider::ProviderBuilder;
//! use alloy_chains::NamedChain;
//! use chrono::NaiveDate;
//!
//! let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
//!
//! // With disk cache (recommended for production)
//! let calculator = BlockWindowCalculator::with_disk_cache(provider, "cache.json")?;
//!
//! // Or with memory cache (data lost on exit)
//! let calculator = BlockWindowCalculator::with_memory_cache(provider);
//!
//! let date = NaiveDate::from_ymd_opt(2025, 10, 15).unwrap();
//! let window = calculator.get_daily_window(NamedChain::Arbitrum, date).await?;
//!
//! println!("Blocks for {}: [{}, {}]", date, window.start_block, window.end_block);
//! ```

use alloy_chains::NamedChain;
use alloy_primitives::BlockNumber;
use alloy_provider::Provider;
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::{debug, info};

use crate::blocks::cache::{BlockWindowCache, CacheKey, DiskCache};
use crate::errors::{BlockWindowError, RpcError};
use crate::tracing::spans;
use crate::types::config::BlockCount;

/// Unix timestamp in seconds (always UTC)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UnixTimestamp(pub i64);

impl UnixTimestamp {
    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        Self(dt.timestamp())
    }

    /// Creates a UnixTimestamp from a u64 value
    pub fn from_u64(ts: u64) -> Self {
        Self(ts as i64)
    }

    /// Converts to u64 for use with blockchain timestamps
    pub fn as_u64(&self) -> u64 {
        self.0 as u64
    }

    /// Subtracts one second from the timestamp
    pub fn pred(&self) -> Self {
        Self(self.0 - 1)
    }
}

impl std::fmt::Display for UnixTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Represents an inclusive block range for a specific UTC day on a blockchain
///
/// A daily window captures:
/// - The first block produced on or after 00:00:00 UTC on the given date
/// - The last block produced at or before 23:59:59 UTC on the given date
/// - The exact UTC timestamps that define the day boundaries
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DailyBlockWindow {
    /// First block number in the window (inclusive)
    pub start_block: BlockNumber,

    /// Last block number in the window (inclusive)
    pub end_block: BlockNumber,

    /// UTC timestamp at start of day (00:00:00 UTC)
    pub start_ts: UnixTimestamp,

    /// UTC timestamp at start of next day (00:00:00 UTC next day) - exclusive boundary
    pub end_ts_exclusive: UnixTimestamp,
}

impl DailyBlockWindow {
    /// Creates a new daily block window
    pub fn new(
        start_block: BlockNumber,
        end_block: BlockNumber,
        start_ts: UnixTimestamp,
        end_ts_exclusive: UnixTimestamp,
    ) -> Result<Self, BlockWindowError> {
        if end_block < start_block {
            return Err(BlockWindowError::invalid_range(start_block, end_block));
        }
        if end_ts_exclusive.0 <= start_ts.0 {
            return Err(BlockWindowError::invalid_timestamp_range(
                start_ts,
                end_ts_exclusive,
            ));
        }
        Ok(Self {
            start_block,
            end_block,
            start_ts,
            end_ts_exclusive,
        })
    }

    /// Returns the number of blocks in this window (inclusive)
    pub fn block_count(&self) -> BlockCount {
        let count = self
            .end_block
            .saturating_sub(self.start_block)
            .saturating_add(1);
        BlockCount::new(count)
    }
}

/// Calculates and caches daily block windows for blockchain queries
///
/// This calculator uses binary search to find block ranges for specific UTC dates.
/// Results are cached using a configurable cache backend to avoid repeated RPC calls.
///
/// # Examples
///
/// ```rust,ignore
/// use semioscan::{BlockWindowCalculator, DiskCache, MemoryCache};
///
/// // With disk cache (default, backward compatible)
/// let calculator = BlockWindowCalculator::with_disk_cache(provider, "cache.json")?;
///
/// // With memory cache
/// let calculator = BlockWindowCalculator::with_memory_cache(provider);
///
/// // With custom cache backend
/// let cache = DiskCache::new("cache.json")
///     .with_ttl(Duration::from_secs(86400 * 7))
///     .validate()?;
/// let calculator = BlockWindowCalculator::new(provider, Box::new(cache));
/// ```
pub struct BlockWindowCalculator<P> {
    provider: P,
    cache: Box<dyn BlockWindowCache>,
}

impl<P: Provider> BlockWindowCalculator<P> {
    /// Creates a new calculator with the given provider and cache backend
    ///
    /// This is the most flexible constructor, allowing you to provide any cache implementation.
    ///
    /// # Arguments
    ///
    /// * `provider` - The blockchain provider for RPC calls
    /// * `cache` - The cache backend (DiskCache, MemoryCache, NoOpCache, or custom)
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use semioscan::{BlockWindowCalculator, DiskCache, MemoryCache, NoOpCache};
    /// use std::time::Duration;
    ///
    /// // Disk cache with TTL
    /// let cache = DiskCache::new("cache.json")
    ///     .with_ttl(Duration::from_secs(86400 * 7))
    ///     .validate()?;
    /// let calculator = BlockWindowCalculator::new(provider, Box::new(cache));
    ///
    /// // Memory cache with size limit
    /// let cache = MemoryCache::new().with_max_entries(500);
    /// let calculator = BlockWindowCalculator::new(provider, Box::new(cache));
    ///
    /// // No cache
    /// let calculator = BlockWindowCalculator::new(provider, Box::new(NoOpCache));
    /// ```
    pub fn new(provider: P, cache: Box<dyn BlockWindowCache>) -> Self {
        Self { provider, cache }
    }

    /// Creates a calculator with a disk cache at the specified path
    ///
    /// This is the recommended constructor for most use cases. It provides persistent
    /// caching with automatic validation and helpful error messages.
    ///
    /// # Arguments
    ///
    /// * `provider` - The blockchain provider for RPC calls
    /// * `cache_path` - Path to the cache file (will be created if it doesn't exist)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The parent directory doesn't exist and cannot be created
    /// - The parent directory is not writable
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use semioscan::BlockWindowCalculator;
    ///
    /// // Relative path
    /// let calculator = BlockWindowCalculator::with_disk_cache(provider, "cache.json")?;
    ///
    /// // Absolute path
    /// let calculator = BlockWindowCalculator::with_disk_cache(
    ///     provider,
    ///     "/var/cache/block_windows.json"
    /// )?;
    /// ```
    pub fn with_disk_cache(
        provider: P,
        cache_path: impl AsRef<Path>,
    ) -> Result<Self, BlockWindowError> {
        let cache = DiskCache::new(cache_path.as_ref()).validate()?;
        Ok(Self::new(provider, Box::new(cache)))
    }

    /// Creates a calculator with an in-memory cache
    ///
    /// The in-memory cache is faster than disk cache but data is lost when the program exits.
    /// Use this for:
    /// - Short-lived processes
    /// - Testing
    /// - Scenarios where disk I/O is undesirable
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use semioscan::BlockWindowCalculator;
    ///
    /// // Unbounded memory cache
    /// let calculator = BlockWindowCalculator::with_memory_cache(provider);
    /// ```
    pub fn with_memory_cache(provider: P) -> Self {
        use crate::blocks::cache::MemoryCache;
        Self::new(provider, Box::new(MemoryCache::new()))
    }

    /// Creates a calculator without caching
    ///
    /// Every call to `get_daily_window()` will perform RPC queries. Use this for:
    /// - Testing
    /// - Scenarios where caching is not desired
    /// - One-time queries
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use semioscan::BlockWindowCalculator;
    ///
    /// let calculator = BlockWindowCalculator::without_cache(provider);
    /// ```
    pub fn without_cache(provider: P) -> Self {
        use crate::blocks::cache::NoOpCache;
        Self::new(provider, Box::new(NoOpCache))
    }

    /// Returns current cache statistics
    ///
    /// Provides insights into cache performance including hits, misses, evictions,
    /// and current size. Useful for monitoring and optimization.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let stats = calculator.cache_stats().await;
    /// println!("Cache hit rate: {:.1}%", stats.hit_rate());
    /// println!("Entries: {}, Evictions: {}", stats.entries, stats.evictions);
    /// ```
    pub async fn cache_stats(&self) -> crate::blocks::cache::CacheStats {
        self.cache.stats().await
    }

    /// Fetches the timestamp of a specific block
    async fn get_block_timestamp(
        &self,
        block_number: BlockNumber,
    ) -> Result<UnixTimestamp, BlockWindowError> {
        let span = spans::get_block_timestamp(block_number);
        let _guard = span.enter();

        let block = self
            .provider
            .get_block_by_number(block_number.into())
            .await
            .map_err(|e| RpcError::get_block_failed(block_number, e))?
            .ok_or_else(|| RpcError::BlockNotFound { block_number })?;

        Ok(UnixTimestamp::from_u64(block.header.timestamp))
    }

    /// Binary search to find the first block at or after the target timestamp.
    ///
    /// Thin instrumentation wrapper over [`find_first_at_or_after_with`].
    async fn find_first_block_at_or_after(
        &self,
        target_ts: UnixTimestamp,
        latest_block: BlockNumber,
    ) -> Result<BlockNumber, BlockWindowError> {
        let span = spans::find_first_block_at_or_after(target_ts.as_u64(), latest_block);
        let _guard = span.enter();

        find_first_at_or_after_with(target_ts, latest_block, |n| self.get_block_timestamp(n)).await
    }

    /// Binary search to find the last block at or before the target timestamp.
    ///
    /// Thin instrumentation wrapper over [`find_last_at_or_before_with`].
    async fn find_last_block_at_or_before(
        &self,
        target_ts: UnixTimestamp,
        latest_block: BlockNumber,
    ) -> Result<BlockNumber, BlockWindowError> {
        let span = spans::find_last_block_at_or_before(target_ts.as_u64(), latest_block);
        let _guard = span.enter();

        find_last_at_or_before_with(target_ts, latest_block, |n| self.get_block_timestamp(n)).await
    }

    /// Resolves an inclusive timestamp range to the inclusive block range that
    /// covers it.
    ///
    /// Returns `(start_block, end_block)` where:
    /// - `start_block` is the first block with `timestamp >= start_ts`
    /// - `end_block` is the last block with `timestamp <= end_ts`
    ///
    /// This is the same binary search used by [`Self::get_daily_window`], but
    /// at arbitrary timestamp granularity rather than full UTC days. It is
    /// intended for callers that need to resolve event-driven or
    /// configurable time windows (for example, bridge reconciliation where
    /// the search window is `[event_ts - padding, event_ts + lookahead]`).
    ///
    /// # Edge cases
    ///
    /// - `start_ts` at or before the genesis block's timestamp → `start_block = 0`.
    /// - `end_ts` at or after the chain head's timestamp → `end_block = latest`.
    /// - `start_ts` strictly greater than the chain head's timestamp → both
    ///   blocks equal `latest`, signalling an empty window past chain tip.
    /// - `end_ts` strictly less than the genesis block's timestamp → both
    ///   blocks equal `0`, signalling an empty window before chain history.
    /// - The window falls strictly between two consecutive blocks (no block
    ///   has a timestamp in `[start_ts, end_ts]`) → `start_block > end_block`,
    ///   i.e. the returned tuple is inverted. Callers iterating over the
    ///   block range should check for this to detect an empty window.
    ///
    /// # Errors
    ///
    /// - [`BlockWindowError::InvalidTimestampRange`] when `start_ts > end_ts`.
    /// - [`BlockWindowError::NonMonotonicTimestamps`] when the chain's genesis
    ///   and head timestamps are not in increasing order. The binary search
    ///   assumes monotonic timestamps; surfacing this as a typed error avoids
    ///   silently returning wrong block boundaries.
    /// - [`BlockWindowError::Rpc`] for provider failures.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use semioscan::{BlockWindowCalculator, UnixTimestamp};
    ///
    /// let calculator = BlockWindowCalculator::without_cache(provider);
    /// let start = UnixTimestamp::from_u64(1_700_000_000);
    /// let end = UnixTimestamp::from_u64(1_700_086_400);
    /// let (start_block, end_block) =
    ///     calculator.block_range_for_timestamps(start, end).await?;
    /// ```
    pub async fn block_range_for_timestamps(
        &self,
        start_ts: UnixTimestamp,
        end_ts: UnixTimestamp,
    ) -> Result<(BlockNumber, BlockNumber), BlockWindowError> {
        let span = spans::block_range_for_timestamps(start_ts.as_u64(), end_ts.as_u64());
        let _guard = span.enter();

        if start_ts > end_ts {
            return Err(BlockWindowError::invalid_timestamp_range(start_ts, end_ts));
        }

        let latest_block = self
            .provider
            .get_block_number()
            .await
            .map_err(RpcError::get_block_number_failed)?;

        info!(
            start_ts = %start_ts,
            end_ts = %end_ts,
            latest_block,
            "Resolving timestamp range to block range"
        );

        let (start_block, end_block) =
            compute_block_range_with(start_ts, end_ts, latest_block, |n| {
                self.get_block_timestamp(n)
            })
            .await?;

        info!(
            start_ts = %start_ts,
            end_ts = %end_ts,
            start_block,
            end_block,
            "Resolved timestamp range to block range"
        );

        Ok((start_block, end_block))
    }

    /// Gets (or computes and caches) the daily block window for a specific chain and date
    ///
    /// This method:
    /// 1. Checks the cache for an existing window
    /// 2. If not found, performs binary searches to find the block range
    /// 3. Saves the result to the cache for future use
    ///
    /// # Arguments
    /// * `chain` - The named chain for which to calculate the block window
    /// * `date` - The UTC date for which to calculate the block window
    ///
    /// # Returns
    /// A `DailyBlockWindow` containing the start/end blocks and timestamps
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use semioscan::BlockWindowCalculator;
    /// use alloy_chains::NamedChain;
    /// use chrono::NaiveDate;
    ///
    /// let calculator = BlockWindowCalculator::with_disk_cache(provider, "cache.json")?;
    /// let date = NaiveDate::from_ymd_opt(2025, 10, 15).unwrap();
    /// let window = calculator.get_daily_window(NamedChain::Arbitrum, date).await?;
    ///
    /// println!("Blocks: {} to {}", window.start_block, window.end_block);
    /// println!("Count: {}", window.block_count().as_u64());
    /// ```
    pub async fn get_daily_window(
        &self,
        chain: NamedChain,
        date: NaiveDate,
    ) -> Result<DailyBlockWindow, BlockWindowError> {
        let span = spans::get_daily_window(chain, date);
        let _guard = span.enter();

        let key = CacheKey::new(chain, date);

        // Check cache first
        if let Some(window) = self.cache.get(&key).await {
            info!(
                chain = %chain,
                date = %date,
                cache = %self.cache.name(),
                cached = true,
                "Retrieved daily block window from cache"
            );
            return Ok(window);
        }

        // Calculate UTC day boundaries
        let start_dt = Utc
            .with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
            .single()
            .ok_or_else(|| BlockWindowError::invalid_date_conversion(date))?;

        let end_dt = start_dt
            .checked_add_signed(chrono::TimeDelta::days(1))
            .ok_or_else(|| BlockWindowError::date_arithmetic_overflow(date))?;

        let start_ts = UnixTimestamp::from_datetime(start_dt);
        let end_ts_exclusive = UnixTimestamp::from_datetime(end_dt);

        // Get latest block number
        let latest_block = self
            .provider
            .get_block_number()
            .await
            .map_err(RpcError::get_block_number_failed)?;

        info!(
            chain = %chain,
            date = %date,
            start_ts = %start_ts,
            end_ts_exclusive = %end_ts_exclusive,
            latest_block,
            "Computing daily block window"
        );

        // Binary search for block boundaries
        let start_block = self
            .find_first_block_at_or_after(start_ts, latest_block)
            .await?;

        let end_block = self
            .find_last_block_at_or_before(end_ts_exclusive.pred(), latest_block)
            .await?;

        let window = DailyBlockWindow::new(start_block, end_block, start_ts, end_ts_exclusive)?;

        info!(
            chain = %chain,
            date = %date,
            start_block = window.start_block,
            end_block = window.end_block,
            block_count = window.block_count().as_u64(),
            cache = %self.cache.name(),
            "Computed daily block window"
        );

        // Save to cache (ignore errors - caching is best-effort)
        if let Err(e) = self.cache.insert(key, window.clone()).await {
            debug!(error = %e, "Failed to cache block window (continuing anyway)");
        }

        Ok(window)
    }
}

/// Binary search for the first block with `timestamp >= target_ts`, using a
/// caller-supplied async timestamp fetcher.
///
/// Decoupling the algorithm from the [`Provider`] makes the search testable
/// against in-memory chain fixtures without standing up a real RPC endpoint.
///
/// # Algorithm
///
/// - **Search space**: `[0, latest_block]`
/// - **Invariant**: blocks `< lo` have `timestamp < target_ts`
/// - **Invariant**: `result` (when assigned) has `timestamp >= target_ts`
/// - **Result**: the smallest block number with `timestamp >= target_ts`, or
///   `latest_block` if no block satisfies the predicate
///
/// # Complexity
///
/// - Time: O(log n) where n is the number of blocks
/// - Calls: O(log n) invocations of `fetch_ts`
async fn find_first_at_or_after_with<F, Fut>(
    target_ts: UnixTimestamp,
    latest_block: BlockNumber,
    mut fetch_ts: F,
) -> Result<BlockNumber, BlockWindowError>
where
    F: FnMut(BlockNumber) -> Fut,
    Fut: std::future::Future<Output = Result<UnixTimestamp, BlockWindowError>>,
{
    let mut lo = 0u64;
    let mut hi = latest_block;
    let mut result = latest_block;

    while lo <= hi {
        let mid = (lo + hi) / 2;
        let ts = fetch_ts(mid).await?;

        if ts >= target_ts {
            result = mid;
            if mid == 0 {
                break;
            }
            hi = mid - 1;
        } else {
            lo = mid + 1;
        }
    }

    debug!(target_ts = %target_ts, result, "Found first block at or after timestamp");
    Ok(result)
}

/// Binary search for the last block with `timestamp <= target_ts`, using a
/// caller-supplied async timestamp fetcher.
///
/// Counterpart to [`find_first_at_or_after_with`].
///
/// # Algorithm
///
/// - **Search space**: `[0, latest_block]`
/// - **Invariant**: blocks `> hi` have `timestamp > target_ts`
/// - **Invariant**: `result` (when assigned) has `timestamp <= target_ts`
/// - **Result**: the largest block number with `timestamp <= target_ts`, or
///   `0` if no block satisfies the predicate
async fn find_last_at_or_before_with<F, Fut>(
    target_ts: UnixTimestamp,
    latest_block: BlockNumber,
    mut fetch_ts: F,
) -> Result<BlockNumber, BlockWindowError>
where
    F: FnMut(BlockNumber) -> Fut,
    Fut: std::future::Future<Output = Result<UnixTimestamp, BlockWindowError>>,
{
    let mut lo = 0u64;
    let mut hi = latest_block;
    let mut result = 0u64;

    while lo <= hi {
        let mid = (lo + hi) / 2;
        let ts = fetch_ts(mid).await?;

        if ts <= target_ts {
            result = mid;
            lo = mid + 1;
        } else {
            if mid == 0 {
                break;
            }
            hi = mid - 1;
        }
    }

    debug!(target_ts = %target_ts, result, "Found last block at or before timestamp");
    Ok(result)
}

/// Resolves a timestamp range to a block range using a caller-supplied
/// timestamp fetcher. Pure algorithmic core of
/// [`BlockWindowCalculator::block_range_for_timestamps`].
///
/// The function probes the chain head and the genesis block to:
/// - short-circuit ranges that fall entirely outside chain history without
///   running the full binary search
/// - flag obviously non-monotonic chains (genesis timestamp newer than head)
///   as [`BlockWindowError::NonMonotonicTimestamps`] before the binary search
///   can return a silently-wrong boundary
///
/// The full binary search only runs for ranges that overlap chain history.
async fn compute_block_range_with<F, Fut>(
    start_ts: UnixTimestamp,
    end_ts: UnixTimestamp,
    latest_block: BlockNumber,
    mut fetch_ts: F,
) -> Result<(BlockNumber, BlockNumber), BlockWindowError>
where
    F: FnMut(BlockNumber) -> Fut,
    Fut: std::future::Future<Output = Result<UnixTimestamp, BlockWindowError>>,
{
    debug_assert!(
        start_ts <= end_ts,
        "caller must validate start_ts <= end_ts"
    );

    let genesis_ts = fetch_ts(0).await?;
    let latest_ts = if latest_block == 0 {
        genesis_ts
    } else {
        fetch_ts(latest_block).await?
    };

    if latest_block > 0 && genesis_ts > latest_ts {
        return Err(BlockWindowError::non_monotonic_timestamps(
            0,
            latest_block,
            genesis_ts,
            latest_ts,
        ));
    }

    // Range entirely past chain head: empty window at chain tip.
    if start_ts > latest_ts {
        return Ok((latest_block, latest_block));
    }
    // Range entirely before genesis: empty window at chain start.
    if end_ts < genesis_ts {
        return Ok((0, 0));
    }

    let start_block = if start_ts <= genesis_ts {
        0
    } else {
        find_first_at_or_after_with(start_ts, latest_block, &mut fetch_ts).await?
    };
    let end_block = if end_ts >= latest_ts {
        latest_block
    } else {
        find_last_at_or_before_with(end_ts, latest_block, &mut fetch_ts).await?
    };

    Ok((start_block, end_block))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_provider::ProviderBuilder;

    /// Provider for validation tests that fail before any RPC call.
    fn dummy_provider() -> impl Provider {
        ProviderBuilder::new().connect_http("http://localhost:1".parse().unwrap())
    }

    #[tokio::test]
    async fn block_range_for_timestamps_rejects_inverted_range() {
        let calculator = BlockWindowCalculator::without_cache(dummy_provider());
        let err = calculator
            .block_range_for_timestamps(UnixTimestamp(2000), UnixTimestamp(1000))
            .await
            .unwrap_err();
        assert!(
            matches!(err, BlockWindowError::InvalidTimestampRange { .. }),
            "expected InvalidTimestampRange, got: {err:?}"
        );
    }

    #[test]
    fn test_cache_key_display() {
        let key = CacheKey::new(
            NamedChain::Arbitrum,
            NaiveDate::from_ymd_opt(2025, 10, 10).unwrap(),
        );
        let serialized = key.to_string();
        assert_eq!(serialized, "42161:2025-10-10");
    }

    #[test]
    fn test_daily_block_window_validation() {
        let start_ts = UnixTimestamp(1728518400);
        let end_ts = UnixTimestamp(1728604800);

        // Valid window
        let window = DailyBlockWindow::new(1000, 2000, start_ts, end_ts);
        assert!(window.is_ok());
        assert_eq!(window.unwrap().block_count().as_u64(), 1001);

        // Invalid: end_block < start_block
        let invalid = DailyBlockWindow::new(2000, 1000, start_ts, end_ts);
        assert!(invalid.is_err());

        // Invalid: end_ts <= start_ts
        let invalid = DailyBlockWindow::new(1000, 2000, end_ts, start_ts);
        assert!(invalid.is_err());
    }

    #[test]
    fn test_block_window_edge_cases() {
        // Test edge cases for block window calculations

        // Single block window
        let single = DailyBlockWindow {
            start_block: 1000,
            end_block: 1000,
            start_ts: UnixTimestamp(1697328000),
            end_ts_exclusive: UnixTimestamp(1697414400),
        };
        // Single block: [1000, 1000] contains 1 block
        assert_eq!(single.block_count().as_u64(), 1);

        // Large block range (e.g., Arbitrum produces ~40k blocks per day)
        let large = DailyBlockWindow {
            start_block: 100_000_000,
            end_block: 100_040_000,
            start_ts: UnixTimestamp(1697328000),
            end_ts_exclusive: UnixTimestamp(1697414400),
        };
        // Inclusive: [100M, 100M+40k] contains 40,001 blocks
        assert_eq!(large.block_count().as_u64(), 40_001);

        // Standard range
        let window = DailyBlockWindow {
            start_block: 1000,
            end_block: 2000,
            start_ts: UnixTimestamp(1697328000),
            end_ts_exclusive: UnixTimestamp(1697414400),
        };
        // Inclusive count: [1000, 2000] contains 1001 blocks
        assert_eq!(window.block_count().as_u64(), 1001);
    }

    #[test]
    fn test_block_window_validation_errors() {
        // Test all validation error cases
        let start_ts = UnixTimestamp(1728518400);
        let end_ts = UnixTimestamp(1728604800);

        // Error: end_block < start_block
        let result = DailyBlockWindow::new(2000, 1000, start_ts, end_ts);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid block range"));

        // Error: end_ts <= start_ts (equal)
        let result = DailyBlockWindow::new(1000, 2000, start_ts, start_ts);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid timestamp range"));

        // Error: end_ts < start_ts (reversed)
        let result = DailyBlockWindow::new(1000, 2000, end_ts, start_ts);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid timestamp range"));
    }

    #[test]
    fn test_block_window_zero_values() {
        // Test edge case: block numbers starting at 0
        let start_ts = UnixTimestamp(1728518400);
        let end_ts = UnixTimestamp(1728604800);

        // Valid: blocks 0 to 100
        let window = DailyBlockWindow::new(0, 100, start_ts, end_ts);
        assert!(window.is_ok());
        assert_eq!(window.unwrap().block_count().as_u64(), 101);

        // Valid: single block at 0
        let window = DailyBlockWindow::new(0, 0, start_ts, end_ts);
        assert!(window.is_ok());
        assert_eq!(window.unwrap().block_count().as_u64(), 1);
    }

    #[test]
    fn test_block_window_large_values() {
        // Test with very large block numbers (real-world Arbitrum has blocks > 100M)
        let start_ts = UnixTimestamp(1728518400);
        let end_ts = UnixTimestamp(1728604800);

        // Arbitrum-scale block numbers
        let window = DailyBlockWindow::new(100_000_000, 100_040_000, start_ts, end_ts);
        assert!(window.is_ok());
        assert_eq!(window.unwrap().block_count().as_u64(), 40_001);

        // Very large range
        let window = DailyBlockWindow::new(1_000_000_000, 1_001_000_000, start_ts, end_ts);
        assert!(window.is_ok());
        assert_eq!(window.unwrap().block_count().as_u64(), 1_000_001);
    }

    #[test]
    fn test_block_window_count_overflow_protection() {
        // Test that block_count() handles near-overflow cases safely
        let start_ts = UnixTimestamp(1728518400);
        let end_ts = UnixTimestamp(1728604800);

        // Near u64::MAX (should use saturating arithmetic)
        let window = DailyBlockWindow::new(u64::MAX - 100, u64::MAX, start_ts, end_ts);
        assert!(window.is_ok());
        // Should saturate rather than wrap
        let count = window.unwrap().block_count();
        assert_eq!(count.as_u64(), 101);
    }

    /// In-memory chain fixture used by `compute_block_range_with` tests.
    ///
    /// `timestamps[i]` is the timestamp of block `i`. Tests pick whether the
    /// sequence is monotonic; deliberately non-monotonic fixtures exercise
    /// the typed error path.
    fn fetcher_from(
        timestamps: Vec<i64>,
    ) -> impl FnMut(
        BlockNumber,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<UnixTimestamp, BlockWindowError>>>,
    > {
        move |n: BlockNumber| {
            let ts = timestamps[n as usize];
            Box::pin(async move { Ok(UnixTimestamp(ts)) })
        }
    }

    #[tokio::test]
    async fn block_range_target_inside_chain_history() {
        // Five-block monotonic chain spanning ts=1000..=1400.
        let timestamps = vec![1000, 1100, 1200, 1300, 1400];
        let latest_block: BlockNumber = (timestamps.len() - 1) as BlockNumber;

        let (start, end) = compute_block_range_with(
            UnixTimestamp(1150),
            UnixTimestamp(1350),
            latest_block,
            fetcher_from(timestamps),
        )
        .await
        .unwrap();

        // First block with ts >= 1150 is block 2 (ts=1200).
        // Last block with ts <= 1350 is block 3 (ts=1300).
        assert_eq!(start, 2);
        assert_eq!(end, 3);
    }

    #[tokio::test]
    async fn block_range_exact_boundary_match() {
        // Edge case: the timestamp range hits block boundaries exactly.
        let timestamps = vec![1000, 1100, 1200, 1300, 1400];
        let latest_block: BlockNumber = 4;

        let (start, end) = compute_block_range_with(
            UnixTimestamp(1100),
            UnixTimestamp(1300),
            latest_block,
            fetcher_from(timestamps),
        )
        .await
        .unwrap();

        assert_eq!(start, 1);
        assert_eq!(end, 3);
    }

    #[tokio::test]
    async fn block_range_target_before_genesis_returns_zero() {
        // Chain starts at ts=1000; query a window that ends before genesis.
        let timestamps = vec![1000, 1100, 1200, 1300, 1400];
        let latest_block: BlockNumber = 4;

        let (start, end) = compute_block_range_with(
            UnixTimestamp(500),
            UnixTimestamp(900),
            latest_block,
            fetcher_from(timestamps),
        )
        .await
        .unwrap();

        // Range entirely before chain history collapses to (0, 0).
        assert_eq!(start, 0);
        assert_eq!(end, 0);
    }

    #[tokio::test]
    async fn block_range_start_before_genesis_clamps_to_zero() {
        // Range starts before genesis but ends inside chain history:
        // start_block should clamp to 0.
        let timestamps = vec![1000, 1100, 1200, 1300, 1400];
        let latest_block: BlockNumber = 4;

        let (start, end) = compute_block_range_with(
            UnixTimestamp(500),
            UnixTimestamp(1250),
            latest_block,
            fetcher_from(timestamps),
        )
        .await
        .unwrap();

        assert_eq!(start, 0);
        // Last block with ts <= 1250 is block 2 (ts=1200).
        assert_eq!(end, 2);
    }

    #[tokio::test]
    async fn block_range_target_after_latest_returns_latest() {
        // Query a window that lies entirely after the chain head.
        let timestamps = vec![1000, 1100, 1200, 1300, 1400];
        let latest_block: BlockNumber = 4;

        let (start, end) = compute_block_range_with(
            UnixTimestamp(2000),
            UnixTimestamp(3000),
            latest_block,
            fetcher_from(timestamps),
        )
        .await
        .unwrap();

        // Range entirely past chain head collapses to (latest, latest).
        assert_eq!(start, latest_block);
        assert_eq!(end, latest_block);
    }

    #[tokio::test]
    async fn block_range_end_after_latest_clamps_to_latest() {
        // Range starts inside chain history but ends past chain head:
        // end_block should clamp to latest.
        let timestamps = vec![1000, 1100, 1200, 1300, 1400];
        let latest_block: BlockNumber = 4;

        let (start, end) = compute_block_range_with(
            UnixTimestamp(1250),
            UnixTimestamp(9999),
            latest_block,
            fetcher_from(timestamps),
        )
        .await
        .unwrap();

        // First block with ts >= 1250 is block 3 (ts=1300).
        assert_eq!(start, 3);
        assert_eq!(end, latest_block);
    }

    #[tokio::test]
    async fn block_range_between_consecutive_blocks_returns_inverted() {
        // The timestamp range [1150, 1180] falls strictly between block 1
        // (ts=1100) and block 2 (ts=1200) — no block has a timestamp in the
        // window, and the documented behaviour is that the returned tuple is
        // inverted so callers can detect emptiness.
        let timestamps = vec![1000, 1100, 1200, 1300, 1400];
        let latest_block: BlockNumber = 4;

        let (start, end) = compute_block_range_with(
            UnixTimestamp(1150),
            UnixTimestamp(1180),
            latest_block,
            fetcher_from(timestamps),
        )
        .await
        .unwrap();

        // First block with ts >= 1150 is block 2 (ts=1200).
        // Last  block with ts <= 1180 is block 1 (ts=1100).
        assert_eq!(start, 2);
        assert_eq!(end, 1);
        assert!(start > end, "empty window should yield inverted range");
    }

    #[tokio::test]
    async fn block_range_non_monotonic_chain_errors() {
        // Genesis ts > head ts — flagged as non-monotonic before the binary
        // search can return wrong boundaries.
        let timestamps = vec![5000, 4000, 3000, 2000, 1000];
        let latest_block: BlockNumber = 4;

        let err = compute_block_range_with(
            UnixTimestamp(2500),
            UnixTimestamp(4500),
            latest_block,
            fetcher_from(timestamps),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            BlockWindowError::NonMonotonicTimestamps { .. }
        ));

        let msg = err.to_string();
        assert!(
            msg.contains("Non-monotonic"),
            "expected non-monotonic message, got: {msg}"
        );
    }

    #[tokio::test]
    async fn block_range_single_block_chain() {
        // Edge case: chain with only the genesis block.
        let timestamps = vec![1500];
        let latest_block: BlockNumber = 0;

        // Range that contains the single block.
        let (start, end) = compute_block_range_with(
            UnixTimestamp(1000),
            UnixTimestamp(2000),
            latest_block,
            fetcher_from(timestamps.clone()),
        )
        .await
        .unwrap();
        assert_eq!((start, end), (0, 0));

        // Range entirely after the single block.
        let (start, end) = compute_block_range_with(
            UnixTimestamp(3000),
            UnixTimestamp(4000),
            latest_block,
            fetcher_from(timestamps.clone()),
        )
        .await
        .unwrap();
        assert_eq!((start, end), (0, 0));

        // Range entirely before the single block.
        let (start, end) = compute_block_range_with(
            UnixTimestamp(500),
            UnixTimestamp(800),
            latest_block,
            fetcher_from(timestamps),
        )
        .await
        .unwrap();
        assert_eq!((start, end), (0, 0));
    }

    #[tokio::test]
    async fn find_first_at_or_after_target_inside_history() {
        let timestamps = vec![1000, 1100, 1200, 1300, 1400];
        let result = find_first_at_or_after_with(UnixTimestamp(1150), 4, fetcher_from(timestamps))
            .await
            .unwrap();
        // First block with ts >= 1150 is block 2 (ts=1200).
        assert_eq!(result, 2);
    }

    #[tokio::test]
    async fn find_first_at_or_after_returns_latest_when_target_past_head() {
        let timestamps = vec![1000, 1100, 1200];
        let result = find_first_at_or_after_with(UnixTimestamp(5000), 2, fetcher_from(timestamps))
            .await
            .unwrap();
        // No block satisfies, default to latest.
        assert_eq!(result, 2);
    }

    #[tokio::test]
    async fn find_first_at_or_after_returns_zero_when_target_before_genesis() {
        let timestamps = vec![1000, 1100, 1200];
        let result = find_first_at_or_after_with(UnixTimestamp(500), 2, fetcher_from(timestamps))
            .await
            .unwrap();
        // Block 0 satisfies (>= 500), so first qualifying block is 0.
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn find_last_at_or_before_target_inside_history() {
        let timestamps = vec![1000, 1100, 1200, 1300, 1400];
        let result = find_last_at_or_before_with(UnixTimestamp(1250), 4, fetcher_from(timestamps))
            .await
            .unwrap();
        // Last block with ts <= 1250 is block 2 (ts=1200).
        assert_eq!(result, 2);
    }

    #[tokio::test]
    async fn find_last_at_or_before_returns_latest_when_target_past_head() {
        let timestamps = vec![1000, 1100, 1200];
        let result = find_last_at_or_before_with(UnixTimestamp(5000), 2, fetcher_from(timestamps))
            .await
            .unwrap();
        // All blocks satisfy <= 5000, so the latest one wins.
        assert_eq!(result, 2);
    }

    #[tokio::test]
    async fn find_last_at_or_before_returns_zero_when_target_before_genesis() {
        let timestamps = vec![1000, 1100, 1200];
        let result = find_last_at_or_before_with(UnixTimestamp(500), 2, fetcher_from(timestamps))
            .await
            .unwrap();
        // No block satisfies, default to 0.
        assert_eq!(result, 0);
    }
}
