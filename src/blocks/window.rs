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
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OnceCell};
use tracing::{debug, info};

use crate::blocks::cache::{BlockWindowCache, CacheKey, DiskCache};
use crate::errors::{BlockWindowError, RpcError};
use crate::tracing::spans;
use crate::types::config::BlockCount;

/// Default TTL for the memoized chain head shared by
/// [`BlockWindowCalculator::block_range_for_timestamps`] and
/// [`BlockWindowCalculator::get_daily_window`].
///
/// Chosen to amortize a single reconciliation sweep (typically seconds to a
/// few tens of seconds) without letting the cached head drift far enough to
/// shadow a recent reorg. Exposed publicly so consumers can derive values
/// from it (e.g. `with_head_ttl(DEFAULT_HEAD_TTL * 4)`) rather than using
/// magic literals.
pub const DEFAULT_HEAD_TTL: Duration = Duration::from_secs(30);

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
    bounds_memo: ChainBoundsMemo,
}

/// Memoized chain bounds shared across calls to
/// [`BlockWindowCalculator::block_range_for_timestamps`] and
/// [`BlockWindowCalculator::get_daily_window`].
///
/// `block_range_for_timestamps` probes the genesis block and the chain head
/// on every invocation to short-circuit out-of-range windows and to detect
/// non-monotonic chains; `get_daily_window` needs the same chain head to
/// bound its binary search. Those values are effectively constant within a
/// single reconciliation sweep (genesis is immutable; head moves much slower
/// than the binary-search resolution requires). Caching them on the
/// calculator instance eliminates redundant header fetches both within each
/// method and across mixed workloads that interleave the two.
///
/// Independent of the [`BlockWindowCache`] choice — that cache is keyed by
/// `(NamedChain, NaiveDate)` and only services `get_daily_window`.
struct ChainBoundsMemo {
    /// Genesis timestamp. Immutable per chain — fetched once and reused for
    /// the lifetime of the calculator.
    genesis: OnceCell<UnixTimestamp>,
    /// Most recently observed chain head. `latest_ts` is `None` when the
    /// entry was populated by [`Self::get_or_fetch_latest_block`], which
    /// only fetches the block number; [`Self::get_or_fetch_head`] promotes
    /// such a partial entry by refetching the full `(block, ts)` pair when
    /// it needs the timestamp. Refetched in full when [`Self::head_ttl`]
    /// has elapsed.
    head: Mutex<Option<HeadEntry>>,
    head_ttl: Duration,
}

#[derive(Clone, Copy)]
struct HeadEntry {
    fetched_at: Instant,
    latest_block: BlockNumber,
    latest_ts: Option<UnixTimestamp>,
}

impl ChainBoundsMemo {
    fn new(head_ttl: Duration) -> Self {
        Self {
            genesis: OnceCell::new(),
            head: Mutex::new(None),
            head_ttl,
        }
    }

    /// Returns the genesis timestamp, fetching it on first call only.
    async fn get_or_fetch_genesis<F, Fut>(
        &self,
        fetch: F,
    ) -> Result<UnixTimestamp, BlockWindowError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<UnixTimestamp, BlockWindowError>>,
    {
        self.genesis.get_or_try_init(fetch).await.copied()
    }

    /// Returns the chain head's `(block, timestamp)` pair, refetching only
    /// when the TTL has elapsed or when the cached entry is partial (a
    /// block number without a timestamp, populated by a prior
    /// [`Self::get_or_fetch_latest_block`] call).
    ///
    /// The lock is held across the fetch so concurrent callers funnel a
    /// single in-flight RPC into one shared result, avoiding thundering
    /// herds at TTL expiry.
    async fn get_or_fetch_head<F, Fut>(
        &self,
        fetch: F,
    ) -> Result<(BlockNumber, UnixTimestamp), BlockWindowError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(BlockNumber, UnixTimestamp), BlockWindowError>>,
    {
        let mut guard = self.head.lock().await;
        if let Some(entry) = guard.as_ref() {
            if entry.fetched_at.elapsed() < self.head_ttl {
                if let Some(latest_ts) = entry.latest_ts {
                    return Ok((entry.latest_block, latest_ts));
                }
            }
        }
        let (latest_block, latest_ts) = fetch().await?;
        *guard = Some(HeadEntry {
            fetched_at: Instant::now(),
            latest_block,
            latest_ts: Some(latest_ts),
        });
        Ok((latest_block, latest_ts))
    }

    /// Returns the chain head's block number along with the head's
    /// timestamp *if a prior call has already memoized it*, refetching
    /// only when the TTL has elapsed. Cheaper sibling of
    /// [`Self::get_or_fetch_head`] for callers that don't need the
    /// head's timestamp but can opportunistically use it.
    ///
    /// A cold or stale memo is populated with only the block number — the
    /// returned `Option<UnixTimestamp>` is `None`. This avoids the
    /// `eth_getBlockByNumber(head)` round-trip that
    /// [`Self::get_or_fetch_head`] performs, and therefore avoids the
    /// transient failure modes that round-trip exposes (one-block reorgs
    /// orphaning the just-reported head, free-tier provider cache lag).
    /// A subsequent [`Self::get_or_fetch_head`] call promotes the partial
    /// entry by refetching `(block, ts)` together.
    ///
    /// When the memo holds a full `(block, ts)` entry left by a prior
    /// [`Self::get_or_fetch_head`] call, the timestamp is surfaced here
    /// at zero RPC cost. Callers (notably [`get_daily_window_with`]) use
    /// it to short-circuit tip-adjacent and past-tip windows without
    /// forcing a head-timestamp fetch on the cold-memo path.
    ///
    /// The lock is held across the fetch for the same thundering-herd
    /// reason as [`Self::get_or_fetch_head`].
    async fn get_or_fetch_latest_block<F, Fut>(
        &self,
        fetch: F,
    ) -> Result<(BlockNumber, Option<UnixTimestamp>), BlockWindowError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<BlockNumber, BlockWindowError>>,
    {
        let mut guard = self.head.lock().await;
        if let Some(entry) = guard.as_ref() {
            if entry.fetched_at.elapsed() < self.head_ttl {
                return Ok((entry.latest_block, entry.latest_ts));
            }
        }
        let latest_block = fetch().await?;
        *guard = Some(HeadEntry {
            fetched_at: Instant::now(),
            latest_block,
            latest_ts: None,
        });
        Ok((latest_block, None))
    }
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
        Self {
            provider,
            cache,
            bounds_memo: ChainBoundsMemo::new(DEFAULT_HEAD_TTL),
        }
    }

    /// Overrides the TTL used to memoize the chain head for both
    /// [`Self::block_range_for_timestamps`] and [`Self::get_daily_window`].
    ///
    /// The genesis timestamp is immutable per chain and is always memoized
    /// for the lifetime of the calculator regardless of this setting. Only
    /// the chain head — the `(latest_block, latest_ts)` pair — respects this
    /// TTL. Updating the TTL preserves any already-memoized genesis and any
    /// still-valid cached head; the change takes effect on the next call.
    ///
    /// Both methods share the same memo, so a head fetched by either
    /// amortizes across subsequent calls to the other within the TTL.
    ///
    /// # Trade-offs
    ///
    /// - Shorter TTLs trade RPC traffic for fresher results. A stale head
    ///   affects every call whose window touches the chain tip — not just
    ///   reorg recovery: tip-adjacent ranges short-circuit through
    ///   `start_ts > latest_ts` / `end_ts >= latest_ts` using the cached
    ///   head, so blocks produced during the TTL window can be silently
    ///   excluded.
    /// - At TTL expiry, concurrent callers funnel through a single
    ///   in-flight head fetch. This avoids a thundering-herd against the
    ///   RPC provider, but it also means a slow provider stalls every
    ///   concurrent caller for the duration of that fetch.
    /// - [`Duration::ZERO`] disables head memoization entirely. Use this in
    ///   tests against a freshly-mined chain (e.g. a brand-new Anvil) where
    ///   the head is expected to move faster than the TTL.
    /// - The default ([`Self::with_head_ttl`] not called) is
    ///   [`DEFAULT_HEAD_TTL`].
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use semioscan::BlockWindowCalculator;
    /// use std::time::Duration;
    ///
    /// // Aggressive amortization for a long-running batch sweep.
    /// let calculator = BlockWindowCalculator::with_memory_cache(provider)
    ///     .with_head_ttl(Duration::from_secs(120));
    ///
    /// // Disable head memoization (each call refetches the chain tip).
    /// let calculator = BlockWindowCalculator::without_cache(provider)
    ///     .with_head_ttl(Duration::ZERO);
    /// ```
    pub fn with_head_ttl(mut self, ttl: Duration) -> Self {
        self.bounds_memo.head_ttl = ttl;
        self
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
    /// # Caching
    ///
    /// This method does not consult the [`BlockWindowCache`] supplied to the
    /// constructor — that cache is keyed by `(NamedChain, NaiveDate)` and
    /// only services [`Self::get_daily_window`].
    ///
    /// Instead, the genesis timestamp and the chain head
    /// (`(latest_block, latest_ts)`) are memoized per calculator instance.
    /// Genesis is fetched once and reused forever (it is immutable per
    /// chain); the head is refetched after a TTL elapses (default 30
    /// seconds, configurable via [`Self::with_head_ttl`]). For
    /// long-running consumers that resolve many timestamp ranges per
    /// sweep, this eliminates the `2·N` redundant header fetches the
    /// naive implementation would issue.
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

        block_range_for_timestamps_with(
            &self.bounds_memo,
            start_ts,
            end_ts,
            |n| self.get_block_timestamp(n),
            || async {
                self.provider
                    .get_block_number()
                    .await
                    .map_err(RpcError::get_block_number_failed)
                    .map_err(BlockWindowError::from)
            },
        )
        .await
    }

    /// Gets (or computes and caches) the daily block window for a specific chain and date
    ///
    /// This method:
    /// 1. Checks the cache for an existing window
    /// 2. If not found, performs binary searches to find the block range
    /// 3. Saves the result to the cache for future use
    ///
    /// # Caching
    ///
    /// The `(chain, date)` result is stored in the [`BlockWindowCache`] supplied
    /// to the constructor (disk / memory / no-op). Independently, the chain
    /// head's block number used to bound the binary search is memoized per
    /// calculator instance via [`ChainBoundsMemo`], with the TTL configurable
    /// through [`Self::with_head_ttl`] (default [`DEFAULT_HEAD_TTL`]). That
    /// memo is shared with [`Self::block_range_for_timestamps`], so the head
    /// fetched by either method amortizes subsequent
    /// [`Self::get_daily_window`] calls within the TTL — long-cold-cache
    /// backfills issue a single `eth_blockNumber` instead of one per uncached
    /// day. The reverse direction is partial: a head populated by
    /// [`Self::get_daily_window`] lacks the timestamp that
    /// [`Self::block_range_for_timestamps`] needs, so the next BR4T call
    /// refetches the full `(block, ts)` pair rather than reusing the cached
    /// block number.
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

        get_daily_window_with(
            &self.bounds_memo,
            self.cache.as_ref(),
            chain,
            date,
            |n| self.get_block_timestamp(n),
            || async {
                self.provider
                    .get_block_number()
                    .await
                    .map_err(RpcError::get_block_number_failed)
                    .map_err(BlockWindowError::from)
            },
        )
        .await
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

/// Resolves a timestamp range to a block range using caller-supplied chain
/// bounds and a caller-supplied per-block timestamp fetcher. Pure algorithmic
/// core of [`BlockWindowCalculator::block_range_for_timestamps`].
///
/// `genesis_ts` and `latest_ts` are supplied by the caller (memoized by
/// [`ChainBoundsMemo`] in the production path) so that the function:
/// - short-circuits ranges that fall entirely outside chain history without
///   running the full binary search
/// - flags obviously non-monotonic chains (genesis timestamp newer than head)
///   as [`BlockWindowError::NonMonotonicTimestamps`] before the binary search
///   can return a silently-wrong boundary
///
/// The full binary search only runs for ranges that overlap chain history.
async fn compute_block_range_given_bounds<F, Fut>(
    start_ts: UnixTimestamp,
    end_ts: UnixTimestamp,
    latest_block: BlockNumber,
    genesis_ts: UnixTimestamp,
    latest_ts: UnixTimestamp,
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

/// Memo-aware resolution of an inclusive timestamp range to its inclusive
/// block range. Pulled out of [`BlockWindowCalculator::block_range_for_timestamps`]
/// so the full path — input validation, genesis/head memoization via
/// [`ChainBoundsMemo`], non-monotonic detection, and binary search — can be
/// exercised in tests without a live RPC.
///
/// `fetch_ts(0)` supplies the genesis timestamp, `fetch_latest_block_number()`
/// supplies the chain head's block number, and `fetch_ts(latest_block)`
/// supplies the head's timestamp. The production path wires
/// `self.get_block_timestamp` and `self.provider.get_block_number` through.
async fn block_range_for_timestamps_with<F, FtFut, G, GnFut>(
    bounds_memo: &ChainBoundsMemo,
    start_ts: UnixTimestamp,
    end_ts: UnixTimestamp,
    mut fetch_ts: F,
    fetch_latest_block_number: G,
) -> Result<(BlockNumber, BlockNumber), BlockWindowError>
where
    F: FnMut(BlockNumber) -> FtFut,
    FtFut: std::future::Future<Output = Result<UnixTimestamp, BlockWindowError>>,
    G: FnOnce() -> GnFut,
    GnFut: std::future::Future<Output = Result<BlockNumber, BlockWindowError>>,
{
    if start_ts > end_ts {
        return Err(BlockWindowError::invalid_timestamp_range(start_ts, end_ts));
    }

    let genesis_ts = bounds_memo.get_or_fetch_genesis(|| fetch_ts(0)).await?;

    let (latest_block, latest_ts) = bounds_memo
        .get_or_fetch_head(|| async {
            let latest_block = fetch_latest_block_number().await?;
            let latest_ts = if latest_block == 0 {
                genesis_ts
            } else {
                fetch_ts(latest_block).await?
            };
            Ok((latest_block, latest_ts))
        })
        .await?;

    info!(
        start_ts = %start_ts,
        end_ts = %end_ts,
        latest_block,
        "Resolving timestamp range to block range"
    );

    let (start_block, end_block) = compute_block_range_given_bounds(
        start_ts,
        end_ts,
        latest_block,
        genesis_ts,
        latest_ts,
        fetch_ts,
    )
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

/// Memo-aware resolution of a UTC date to its inclusive daily block window.
/// Pulled out of [`BlockWindowCalculator::get_daily_window`] so the full path —
/// cache lookup, date-to-timestamp conversion, head memoization via
/// [`ChainBoundsMemo`], binary search, and cache insertion — can be exercised
/// in tests without a live RPC.
///
/// The chain head's block number is routed through
/// [`ChainBoundsMemo::get_or_fetch_latest_block`] — the thinner sibling of
/// [`ChainBoundsMemo::get_or_fetch_head`] that fetches only the block
/// number. The binary search needs the block number to bound its probes but
/// never the head's timestamp, so the daily-window path doesn't pay the
/// `eth_getBlockByNumber(head)` round-trip nor inherit its transient
/// failure modes (one-block tip reorgs, free-tier provider cache lag).
///
/// Storage is shared with [`ChainBoundsMemo::get_or_fetch_head`]: a head
/// populated here is reused by subsequent
/// [`BlockWindowCalculator::get_daily_window`] calls within the TTL.
/// [`BlockWindowCalculator::block_range_for_timestamps`] treats a partial
/// entry left here as a miss and refetches the full `(block, ts)` pair —
/// the closure it supplies to [`ChainBoundsMemo::get_or_fetch_head`] fetches
/// both, so the partial block number is discarded rather than upgraded in
/// place.
///
/// # Tip-adjacent and past-tip dates
///
/// When the memo already holds a full `(block, ts)` entry — populated by a
/// prior [`BlockWindowCalculator::block_range_for_timestamps`] call within
/// the TTL — the head's timestamp is surfaced here at zero RPC cost and
/// drives the same short-circuits that
/// [`compute_block_range_given_bounds`] applies:
///
/// - `start_ts > latest_ts` collapses the window to `(latest, latest)`
///   without running the binary search — the day is entirely past the
///   chain tip.
/// - `end_ts_exclusive.pred() >= latest_ts` indicates the day touches the
///   chain tip. The binary search still runs, but the result is not
///   persisted to the [`BlockWindowCache`]: more blocks may yet land in
///   the day's range, and the cache key `(chain, date)` has no notion of
///   which head was current when the window was computed.
///
/// Cold-memo daily-window calls (no prior
/// [`BlockWindowCalculator::block_range_for_timestamps`]) retain the
/// pre-existing behaviour: the timestamp is unknown, the short-circuits
/// cannot fire, and the result is cached as before. The widened failure
/// surface introduced by the shared memo — a stale `latest_ts` left by an
/// earlier `block_range_for_timestamps` call shadowing a `get_daily_window`
/// query for today's date — is closed.
async fn get_daily_window_with<F, FtFut, G, GnFut>(
    bounds_memo: &ChainBoundsMemo,
    cache: &dyn BlockWindowCache,
    chain: NamedChain,
    date: NaiveDate,
    mut fetch_ts: F,
    fetch_latest_block_number: G,
) -> Result<DailyBlockWindow, BlockWindowError>
where
    F: FnMut(BlockNumber) -> FtFut,
    FtFut: std::future::Future<Output = Result<UnixTimestamp, BlockWindowError>>,
    G: FnOnce() -> GnFut,
    GnFut: std::future::Future<Output = Result<BlockNumber, BlockWindowError>>,
{
    let key = CacheKey::new(chain, date);

    if let Some(window) = cache.get(&key).await {
        info!(
            chain = %chain,
            date = %date,
            cache = %cache.name(),
            cached = true,
            "Retrieved daily block window from cache"
        );
        return Ok(window);
    }

    let start_dt = Utc
        .with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
        .single()
        .ok_or_else(|| BlockWindowError::invalid_date_conversion(date))?;

    let end_dt = start_dt
        .checked_add_signed(chrono::TimeDelta::days(1))
        .ok_or_else(|| BlockWindowError::date_arithmetic_overflow(date))?;

    let start_ts = UnixTimestamp::from_datetime(start_dt);
    let end_ts_exclusive = UnixTimestamp::from_datetime(end_dt);

    let (latest_block, memoized_latest_ts) = bounds_memo
        .get_or_fetch_latest_block(fetch_latest_block_number)
        .await?;

    info!(
        chain = %chain,
        date = %date,
        start_ts = %start_ts,
        end_ts_exclusive = %end_ts_exclusive,
        latest_block,
        "Computing daily block window"
    );

    // The day touches or sits past the chain tip when the last second of
    // the day is at or after the memoized head's timestamp. Whichever
    // branch we take below, we must not persist the result: a future
    // chain advance will produce a different (correct) window for the
    // same `(chain, date)` cache key.
    let day_touches_tip =
        memoized_latest_ts.is_some_and(|latest_ts| end_ts_exclusive.pred() >= latest_ts);

    let window = if memoized_latest_ts.is_some_and(|latest_ts| start_ts > latest_ts) {
        // Date is entirely past the chain tip. Mirror the empty-window
        // sentinel that `compute_block_range_given_bounds` returns for
        // the same case rather than running a binary search whose only
        // possible outcome is `(latest, latest)` from the find-first /
        // find-last sentinels.
        debug!(
            chain = %chain,
            date = %date,
            start_ts = %start_ts,
            latest_ts = ?memoized_latest_ts,
            "Date past chain tip — returning empty window at tip without caching"
        );
        DailyBlockWindow::new(latest_block, latest_block, start_ts, end_ts_exclusive)?
    } else {
        let start_block =
            find_first_at_or_after_with(start_ts, latest_block, &mut fetch_ts).await?;
        let end_block =
            find_last_at_or_before_with(end_ts_exclusive.pred(), latest_block, &mut fetch_ts)
                .await?;
        DailyBlockWindow::new(start_block, end_block, start_ts, end_ts_exclusive)?
    };

    info!(
        chain = %chain,
        date = %date,
        start_block = window.start_block,
        end_block = window.end_block,
        block_count = window.block_count().as_u64(),
        cache = %cache.name(),
        cached = !day_touches_tip,
        "Computed daily block window"
    );

    if day_touches_tip {
        debug!(
            chain = %chain,
            date = %date,
            "Skipping cache insert: day touches chain tip and may yet gain or lose blocks"
        );
    } else if let Err(e) = cache.insert(key, window.clone()).await {
        debug!(error = %e, "Failed to cache block window (continuing anyway)");
    }

    Ok(window)
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

    /// Test wrapper that probes block 0 and `latest_block` of an in-memory
    /// fixture and forwards to [`compute_block_range_given_bounds`].
    ///
    /// Mirrors what [`BlockWindowCalculator::block_range_for_timestamps`]
    /// does at runtime via [`ChainBoundsMemo`], without requiring tests to
    /// hand-thread genesis/head timestamps into every call site.
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
        let genesis_ts = fetch_ts(0).await?;
        let latest_ts = if latest_block == 0 {
            genesis_ts
        } else {
            fetch_ts(latest_block).await?
        };
        compute_block_range_given_bounds(
            start_ts,
            end_ts,
            latest_block,
            genesis_ts,
            latest_ts,
            fetch_ts,
        )
        .await
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

    mod bounds_memo {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        #[tokio::test]
        async fn genesis_fetched_only_once() {
            let memo = ChainBoundsMemo::new(Duration::from_secs(60));
            let counter = Arc::new(AtomicUsize::new(0));

            let c1 = counter.clone();
            let v1 = memo
                .get_or_fetch_genesis(|| async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    Ok(UnixTimestamp(1000))
                })
                .await
                .unwrap();

            let c2 = counter.clone();
            let v2 = memo
                .get_or_fetch_genesis(|| async move {
                    c2.fetch_add(1, Ordering::SeqCst);
                    Ok(UnixTimestamp(9999))
                })
                .await
                .unwrap();

            assert_eq!(v1, UnixTimestamp(1000));
            assert_eq!(v2, UnixTimestamp(1000));
            assert_eq!(
                counter.load(Ordering::SeqCst),
                1,
                "second call must reuse the memoized genesis"
            );
        }

        #[tokio::test]
        async fn genesis_fetch_error_is_not_memoized() {
            let memo = ChainBoundsMemo::new(Duration::from_secs(60));
            let counter = Arc::new(AtomicUsize::new(0));

            let c1 = counter.clone();
            let err = memo
                .get_or_fetch_genesis(|| async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    Err(BlockWindowError::invalid_timestamp_range(
                        UnixTimestamp(2),
                        UnixTimestamp(1),
                    ))
                })
                .await;
            assert!(err.is_err());

            // The second attempt should retry, not return the cached error.
            let c2 = counter.clone();
            let v = memo
                .get_or_fetch_genesis(|| async move {
                    c2.fetch_add(1, Ordering::SeqCst);
                    Ok(UnixTimestamp(1000))
                })
                .await
                .unwrap();
            assert_eq!(v, UnixTimestamp(1000));
            assert_eq!(counter.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn head_reused_within_ttl() {
            let memo = ChainBoundsMemo::new(Duration::from_secs(60));
            let counter = Arc::new(AtomicUsize::new(0));

            let c1 = counter.clone();
            let (b1, t1) = memo
                .get_or_fetch_head(|| async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    Ok((100, UnixTimestamp(5000)))
                })
                .await
                .unwrap();

            let c2 = counter.clone();
            let (b2, t2) = memo
                .get_or_fetch_head(|| async move {
                    c2.fetch_add(1, Ordering::SeqCst);
                    Ok((200, UnixTimestamp(9999)))
                })
                .await
                .unwrap();

            assert_eq!((b1, t1), (100, UnixTimestamp(5000)));
            assert_eq!((b2, t2), (100, UnixTimestamp(5000)));
            assert_eq!(
                counter.load(Ordering::SeqCst),
                1,
                "second call within TTL must reuse the memoized head"
            );
        }

        #[tokio::test]
        async fn with_head_ttl_preserves_memo() {
            let mut calc = BlockWindowCalculator::without_cache(dummy_provider());

            // Seed the genesis OnceCell by hand (calling into the calculator
            // would require a live provider).
            calc.bounds_memo
                .genesis
                .set(UnixTimestamp(1234))
                .expect("genesis OnceCell starts empty");

            calc = calc.with_head_ttl(Duration::from_secs(120));

            assert_eq!(
                calc.bounds_memo.genesis.get().copied(),
                Some(UnixTimestamp(1234)),
                "with_head_ttl must preserve the memoized genesis"
            );
            assert_eq!(calc.bounds_memo.head_ttl, Duration::from_secs(120));
        }

        #[tokio::test]
        async fn latest_block_reused_within_ttl() {
            // Sibling of `head_reused_within_ttl` for the thinner
            // block-number-only path. Two calls within the TTL must
            // collapse onto a single `eth_blockNumber` fetch.
            let memo = ChainBoundsMemo::new(Duration::from_secs(60));
            let counter = Arc::new(AtomicUsize::new(0));

            let c1 = counter.clone();
            let (b1, t1) = memo
                .get_or_fetch_latest_block(|| async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    Ok(100)
                })
                .await
                .unwrap();

            let c2 = counter.clone();
            let (b2, t2) = memo
                .get_or_fetch_latest_block(|| async move {
                    c2.fetch_add(1, Ordering::SeqCst);
                    Ok(200)
                })
                .await
                .unwrap();

            assert_eq!(b1, 100);
            assert_eq!(b2, 100);
            assert_eq!(
                t1, None,
                "partial entry from get_or_fetch_latest_block leaves latest_ts unset"
            );
            assert_eq!(
                t2, None,
                "second call must reuse the partial entry, still without a timestamp"
            );
            assert_eq!(
                counter.load(Ordering::SeqCst),
                1,
                "second call within TTL must reuse the memoized block number"
            );
        }

        #[tokio::test]
        async fn get_or_fetch_latest_block_reuses_full_entry() {
            // Cross-method reuse in the direction `get_or_fetch_head →
            // get_or_fetch_latest_block`: a full entry already has the
            // block number we need, so the second call must not fetch.
            let memo = ChainBoundsMemo::new(Duration::from_secs(60));
            let head_counter = Arc::new(AtomicUsize::new(0));
            let block_counter = Arc::new(AtomicUsize::new(0));

            let hc = head_counter.clone();
            memo.get_or_fetch_head(|| async move {
                hc.fetch_add(1, Ordering::SeqCst);
                Ok((100, UnixTimestamp(5000)))
            })
            .await
            .unwrap();

            let bc = block_counter.clone();
            let (b, t) = memo
                .get_or_fetch_latest_block(|| async move {
                    bc.fetch_add(1, Ordering::SeqCst);
                    Ok(999)
                })
                .await
                .unwrap();

            assert_eq!(b, 100);
            assert_eq!(
                t,
                Some(UnixTimestamp(5000)),
                "full entry must surface its memoized timestamp at zero RPC cost"
            );
            assert_eq!(head_counter.load(Ordering::SeqCst), 1);
            assert_eq!(
                block_counter.load(Ordering::SeqCst),
                0,
                "block-only fetcher must not fire when a full entry is fresh"
            );
        }

        #[tokio::test]
        async fn get_or_fetch_head_upgrades_partial_entry() {
            // Cross-method reuse in the direction `get_or_fetch_latest_block
            // → get_or_fetch_head`: a partial entry lacks the timestamp,
            // so `get_or_fetch_head` must fall through and refetch the
            // `(block, ts)` pair. The block from the partial entry is
            // discarded — head may have moved between the two calls, and
            // the simplest correct behaviour is to refetch both rather
            // than reuse a stale block number.
            let memo = ChainBoundsMemo::new(Duration::from_secs(60));
            let block_counter = Arc::new(AtomicUsize::new(0));
            let head_counter = Arc::new(AtomicUsize::new(0));

            let bc = block_counter.clone();
            memo.get_or_fetch_latest_block(|| async move {
                bc.fetch_add(1, Ordering::SeqCst);
                Ok(100)
            })
            .await
            .unwrap();

            let hc = head_counter.clone();
            let (b, t) = memo
                .get_or_fetch_head(|| async move {
                    hc.fetch_add(1, Ordering::SeqCst);
                    Ok((101, UnixTimestamp(6000)))
                })
                .await
                .unwrap();

            assert_eq!((b, t), (101, UnixTimestamp(6000)));
            assert_eq!(block_counter.load(Ordering::SeqCst), 1);
            assert_eq!(
                head_counter.load(Ordering::SeqCst),
                1,
                "full-head fetcher must fire to upgrade a partial entry"
            );
        }

        #[tokio::test]
        async fn head_refetched_when_ttl_zero() {
            let memo = ChainBoundsMemo::new(Duration::ZERO);
            let counter = Arc::new(AtomicUsize::new(0));

            let c1 = counter.clone();
            let _ = memo
                .get_or_fetch_head(|| async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    Ok((100, UnixTimestamp(5000)))
                })
                .await
                .unwrap();

            let c2 = counter.clone();
            let (b, t) = memo
                .get_or_fetch_head(|| async move {
                    c2.fetch_add(1, Ordering::SeqCst);
                    Ok((200, UnixTimestamp(6000)))
                })
                .await
                .unwrap();

            assert_eq!((b, t), (200, UnixTimestamp(6000)));
            assert_eq!(
                counter.load(Ordering::SeqCst),
                2,
                "TTL=ZERO must skip memoization"
            );
        }
    }

    /// End-to-end tests for the memo-aware wiring in
    /// [`block_range_for_timestamps_with`] — the closures inside
    /// [`BlockWindowCalculator::block_range_for_timestamps`] are reachable
    /// today only by running against a live RPC. These tests drive that
    /// path via the closure-injected helper using an in-memory fixture so
    /// the wiring (genesis fetch → head fetch → bounds-aware binary search)
    /// is exercised under `cargo test`.
    mod wiring {
        use super::*;
        use crate::blocks::cache::NoOpCache;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex as StdMutex};

        type BoxedTsFut = std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<UnixTimestamp, BlockWindowError>>>,
        >;
        type BoxedHeadFut = std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<BlockNumber, BlockWindowError>>>,
        >;

        /// Records each block requested in `log`, then returns
        /// `timestamps[block]`. Lets tests count how many times the
        /// underlying timestamp fetcher was invoked for any given block.
        fn counting_fetch_ts(
            timestamps: Vec<i64>,
            log: Arc<StdMutex<Vec<BlockNumber>>>,
        ) -> impl FnMut(BlockNumber) -> BoxedTsFut {
            move |n: BlockNumber| {
                let ts = timestamps[n as usize];
                let log = log.clone();
                Box::pin(async move {
                    log.lock().expect("test log mutex poisoned").push(n);
                    Ok(UnixTimestamp(ts))
                })
            }
        }

        /// Mirrors `self.provider.get_block_number()` and increments
        /// `counter` on each invocation.
        fn counting_fetch_head(
            latest: BlockNumber,
            counter: Arc<AtomicUsize>,
        ) -> impl FnOnce() -> BoxedHeadFut {
            move || {
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(latest)
                })
            }
        }

        #[tokio::test]
        async fn bounds_memoized_across_two_calls_within_ttl() {
            // Five-block monotonic chain. Two back-to-back resolutions
            // within the default TTL should collapse the genesis+head
            // probes from 2·2 into a single pair.
            //
            // Both windows are chosen so that the bound-overlap short-circuits
            // in `compute_block_range_given_bounds` fire instead of the
            // binary search — that isolates the memo-bootstrap fetches
            // (block 0 + block `latest`) from the binary-search probes,
            // which are not memoized and would otherwise re-probe both
            // bookends during their bisection.
            let timestamps = vec![1000, 1100, 1200, 1300, 1400];
            let latest: BlockNumber = 4;
            let bounds_memo = ChainBoundsMemo::new(DEFAULT_HEAD_TTL);

            let log = Arc::new(StdMutex::new(Vec::<BlockNumber>::new()));
            let head_counter = Arc::new(AtomicUsize::new(0));

            // First call covers the full chain — both bounds clamp to the
            // chain extremes without running the binary search. Only the
            // memo bootstrap (genesis + head) hits `fetch_ts`.
            let (s1, e1) = block_range_for_timestamps_with(
                &bounds_memo,
                UnixTimestamp(500),
                UnixTimestamp(9999),
                counting_fetch_ts(timestamps.clone(), log.clone()),
                counting_fetch_head(latest, head_counter.clone()),
            )
            .await
            .unwrap();
            assert_eq!((s1, e1), (0, latest));

            // Second call lies strictly past chain head: the `start_ts >
            // latest_ts` short-circuit fires using the memoized `latest_ts`
            // (1400). The returned (latest, latest) proves the cached head
            // was read back, not just written.
            let (s2, e2) = block_range_for_timestamps_with(
                &bounds_memo,
                UnixTimestamp(2000),
                UnixTimestamp(3000),
                counting_fetch_ts(timestamps, log.clone()),
                counting_fetch_head(latest, head_counter.clone()),
            )
            .await
            .unwrap();
            assert_eq!((s2, e2), (latest, latest));

            // The memo collapses the four genesis+head probes that the
            // naive implementation would issue (2 per call) into a single
            // pair, in the order genesis-then-head.
            let calls = log.lock().unwrap();
            assert_eq!(
                calls.as_slice(),
                &[0u64, latest],
                "memo must collapse genesis+head fetches across calls within TTL (got: {calls:?})"
            );
            assert_eq!(
                head_counter.load(Ordering::SeqCst),
                1,
                "head block number must be fetched once within TTL"
            );
        }

        #[tokio::test]
        async fn single_block_chain_reuses_genesis_for_head() {
            // Single-block chain (latest_block == 0). The head closure
            // must reuse the memoized genesis timestamp instead of
            // refetching block 0; a regression that drops the
            // `latest_block == 0` short-circuit shows up here as a second
            // fetch of block 0.
            let timestamps = vec![1500];
            let latest: BlockNumber = 0;
            let bounds_memo = ChainBoundsMemo::new(DEFAULT_HEAD_TTL);

            let log = Arc::new(StdMutex::new(Vec::<BlockNumber>::new()));
            let head_counter = Arc::new(AtomicUsize::new(0));

            let (s, e) = block_range_for_timestamps_with(
                &bounds_memo,
                UnixTimestamp(1000),
                UnixTimestamp(2000),
                counting_fetch_ts(timestamps, log.clone()),
                counting_fetch_head(latest, head_counter.clone()),
            )
            .await
            .unwrap();
            assert_eq!((s, e), (0, 0));

            let calls = log.lock().unwrap();
            assert_eq!(
                calls.as_slice(),
                &[0u64],
                "block 0 must be fetched exactly once for a single-block chain (calls: {calls:?})"
            );
            assert_eq!(head_counter.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn daily_window_head_memoized_across_two_calls_within_ttl() {
            // Two consecutive cache misses on the same calculator should
            // issue exactly one `eth_blockNumber`. The chain head fetched
            // for the first call must be reused by the second within the
            // TTL — this is the per-instance amortization that the issue
            // (closes #6) calls out as missing on the daily-window path.
            let timestamps = vec![
                1_735_689_500, // block 0 — before 2025-01-01 UTC
                1_735_689_700, // block 1 — first block on 2025-01-01 UTC
                1_735_745_000, // block 2 — mid-day
                1_735_775_000, // block 3 — last block on 2025-01-01 UTC
                1_735_776_100, // block 4 — after the day (head)
            ];
            let latest: BlockNumber = 4;
            let bounds_memo = ChainBoundsMemo::new(DEFAULT_HEAD_TTL);
            let cache = NoOpCache;
            let date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();

            let log = Arc::new(StdMutex::new(Vec::<BlockNumber>::new()));
            let head_counter = Arc::new(AtomicUsize::new(0));

            let w1 = get_daily_window_with(
                &bounds_memo,
                &cache,
                NamedChain::Arbitrum,
                date,
                counting_fetch_ts(timestamps.clone(), log.clone()),
                counting_fetch_head(latest, head_counter.clone()),
            )
            .await
            .unwrap();
            assert_eq!((w1.start_block, w1.end_block), (1, 3));

            let w2 = get_daily_window_with(
                &bounds_memo,
                &cache,
                NamedChain::Arbitrum,
                date,
                counting_fetch_ts(timestamps, log.clone()),
                counting_fetch_head(latest, head_counter.clone()),
            )
            .await
            .unwrap();
            assert_eq!((w2.start_block, w2.end_block), (1, 3));

            assert_eq!(
                head_counter.load(Ordering::SeqCst),
                1,
                "eth_blockNumber must fire exactly once across two cache misses within TTL"
            );
        }

        #[tokio::test]
        async fn daily_window_reuses_head_populated_by_block_range_for_timestamps() {
            // Mixed workload: a `block_range_for_timestamps` call leaves the
            // memo populated (genesis + head). A subsequent
            // `get_daily_window` call within the same TTL must reuse that
            // head instead of issuing a fresh `eth_blockNumber`. The 2·N
            // reduction won in #4 only generalises to mixed workloads if
            // both methods consult the same memo.
            let timestamps = vec![
                1_735_689_500,
                1_735_689_700,
                1_735_745_000,
                1_735_775_000,
                1_735_776_100,
            ];
            let latest: BlockNumber = 4;
            let bounds_memo = ChainBoundsMemo::new(DEFAULT_HEAD_TTL);
            let cache = NoOpCache;

            let log = Arc::new(StdMutex::new(Vec::<BlockNumber>::new()));
            let head_counter = Arc::new(AtomicUsize::new(0));

            block_range_for_timestamps_with(
                &bounds_memo,
                UnixTimestamp(1_735_689_600),
                UnixTimestamp(1_735_775_999),
                counting_fetch_ts(timestamps.clone(), log.clone()),
                counting_fetch_head(latest, head_counter.clone()),
            )
            .await
            .unwrap();

            let date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
            let w = get_daily_window_with(
                &bounds_memo,
                &cache,
                NamedChain::Arbitrum,
                date,
                counting_fetch_ts(timestamps, log.clone()),
                counting_fetch_head(latest, head_counter.clone()),
            )
            .await
            .unwrap();
            assert_eq!((w.start_block, w.end_block), (1, 3));

            assert_eq!(
                head_counter.load(Ordering::SeqCst),
                1,
                "get_daily_window must reuse the head memoized by block_range_for_timestamps"
            );
        }

        #[tokio::test]
        async fn daily_window_uses_memoized_latest_block_in_binary_search() {
            // Pre-seed the memo and stub the head fetcher with a fixture
            // index that would panic if invoked (`latest=99` against a
            // 5-element timestamp table). A regression that bypasses the
            // memo — e.g. by reverting to `provider.get_block_number()`
            // inside `get_daily_window` — would invoke the stub and
            // either index-panic or return the wrong bound. Either
            // failure mode surfaces here.
            let timestamps = vec![
                1_735_689_500,
                1_735_689_700,
                1_735_745_000,
                1_735_775_000,
                1_735_776_100,
            ];
            let bounds_memo = ChainBoundsMemo::new(DEFAULT_HEAD_TTL);
            *bounds_memo.head.lock().await = Some(HeadEntry {
                fetched_at: Instant::now(),
                latest_block: 4,
                latest_ts: Some(UnixTimestamp(1_735_776_100)),
            });

            let log = Arc::new(StdMutex::new(Vec::<BlockNumber>::new()));
            let head_counter = Arc::new(AtomicUsize::new(0));
            let cache = NoOpCache;
            let date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();

            let w = get_daily_window_with(
                &bounds_memo,
                &cache,
                NamedChain::Arbitrum,
                date,
                counting_fetch_ts(timestamps, log.clone()),
                counting_fetch_head(99, head_counter.clone()),
            )
            .await
            .unwrap();

            assert_eq!((w.start_block, w.end_block), (1, 3));
            assert_eq!(
                head_counter.load(Ordering::SeqCst),
                0,
                "pre-populated head must short-circuit the fetcher"
            );
        }

        #[tokio::test]
        async fn interior_window_threads_memoized_latest_ts_through_binary_search() {
            // Window strictly inside chain history: both bounds drive the
            // binary search, so the returned `(start, end)` depends on
            // `latest_ts` being honestly threaded from the memo into
            // `compute_block_range_given_bounds`. A regression that swaps
            // `genesis_ts` and `latest_ts` at the call site, or zeroes
            // out `latest_block` after the head closure runs, changes
            // the answer (the `end_ts >= latest_ts` short-circuit fires
            // under the wrong `latest_ts`, clamping `end_block` to
            // `latest` instead of the correct interior block).
            //
            // The earlier tests prove the memo is consulted; this one
            // proves the memoized values are actually fed to the binary
            // search.
            let timestamps = vec![1000, 1100, 1200, 1300, 1400];
            let latest: BlockNumber = 4;
            let bounds_memo = ChainBoundsMemo::new(DEFAULT_HEAD_TTL);

            let log = Arc::new(StdMutex::new(Vec::<BlockNumber>::new()));
            let head_counter = Arc::new(AtomicUsize::new(0));

            let (s, e) = block_range_for_timestamps_with(
                &bounds_memo,
                UnixTimestamp(1150),
                UnixTimestamp(1250),
                counting_fetch_ts(timestamps, log.clone()),
                counting_fetch_head(latest, head_counter.clone()),
            )
            .await
            .unwrap();

            // First block with ts >= 1150 is block 2 (ts=1200).
            // Last block with ts <= 1250 is also block 2 (ts=1200).
            assert_eq!((s, e), (2, 2));
            assert_eq!(head_counter.load(Ordering::SeqCst), 1);
        }

        /// Returns `timestamps[block]` for every block except `failing_block`,
        /// for which it returns [`RpcError::BlockNotFound`] — simulating a
        /// transient tip error (one-block reorg, free-tier provider cache
        /// lag) that orphans the just-reported head between
        /// `eth_blockNumber` and `eth_getBlockByNumber`.
        fn fetch_ts_failing_at(
            timestamps: Vec<i64>,
            failing_block: BlockNumber,
        ) -> impl FnMut(BlockNumber) -> BoxedTsFut {
            move |n: BlockNumber| {
                if n == failing_block {
                    return Box::pin(async move {
                        Err(BlockWindowError::from(RpcError::BlockNotFound {
                            block_number: n,
                        }))
                    });
                }
                let ts = timestamps[n as usize];
                Box::pin(async move { Ok(UnixTimestamp(ts)) })
            }
        }

        #[tokio::test]
        async fn daily_window_survives_transient_head_ts_failure() {
            // Cold memo, historical date well inside chain history. The
            // chain has just experienced a one-block reorg, so
            // `eth_getBlockByNumber(head)` returns BlockNotFound. The
            // binary search only needs to probe blocks 0–3 for this
            // interior date, so the head's timestamp is operationally
            // irrelevant — the call should succeed.
            //
            // Pre-fix, `get_daily_window_with` routes through
            // `get_or_fetch_head`, whose closure fetches the head's
            // timestamp eagerly; this test exposes the regression by
            // failing on `fetch_ts(4)`. Post-fix, daily-window routes
            // through `get_or_fetch_latest_block` and never fetches the
            // head's timestamp on the cold-memo path.
            //
            // 2025-01-15 UTC: start_ts=1736899200, end_ts_exclusive=1736985600.
            let timestamps = vec![
                1_736_899_100, // block 0 — just before the day (genesis)
                1_736_899_300, // block 1 — first block on the day (start_block)
                1_736_950_000, // block 2 — mid-day (end_block)
                1_736_990_000, // block 3 — after the day
                1_736_995_000, // block 4 — head, after the day
            ];
            let latest: BlockNumber = 4;
            let bounds_memo = ChainBoundsMemo::new(DEFAULT_HEAD_TTL);
            let cache = NoOpCache;
            let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();

            let head_counter = Arc::new(AtomicUsize::new(0));

            let window = get_daily_window_with(
                &bounds_memo,
                &cache,
                NamedChain::Arbitrum,
                date,
                fetch_ts_failing_at(timestamps, latest),
                counting_fetch_head(latest, head_counter.clone()),
            )
            .await
            .expect(
                "historical-date daily-window must succeed even when \
                 fetch_ts(head) errors — the binary search never needs \
                 the head's timestamp for an interior date",
            );

            assert_eq!((window.start_block, window.end_block), (1, 2));
            assert_eq!(
                head_counter.load(Ordering::SeqCst),
                1,
                "eth_blockNumber must fire exactly once on cold memo"
            );
        }

        /// Fetcher that panics if invoked — used to prove the past-tip
        /// short-circuit fires without touching the binary search. Mirrors
        /// the pattern in
        /// `daily_window_uses_memoized_latest_block_in_binary_search`,
        /// which stubs the head fetcher the same way.
        fn fetch_ts_unreachable() -> impl FnMut(BlockNumber) -> BoxedTsFut {
            |n: BlockNumber| {
                Box::pin(async move {
                    panic!(
                        "binary search must not run when the date is past the memoized \
                         chain tip — fetch_ts({n}) should never be called"
                    )
                })
            }
        }

        #[tokio::test]
        async fn daily_window_past_tip_skips_binary_search_and_does_not_cache() {
            // Failure scenario from the issue: a prior `block_range_for_timestamps`
            // call has populated the memo with a full `(latest_block, latest_ts)`
            // entry. A subsequent `get_daily_window` call asks for a date whose
            // `start_ts` is strictly past the memoized `latest_ts`. The pre-fix
            // behaviour ran the binary search anyway, got `(latest, latest)` from
            // both find-first / find-last sentinels, accepted it as a valid
            // window, and persisted it to the cache — poisoning future
            // `(chain, date)` lookups with a window that was never correct.
            //
            // The fix consults the memoized `latest_ts` and:
            //   1. Returns the `(latest, latest)` empty-window sentinel without
            //      probing any block (the binary search would be wasted work —
            //      every possible probe answers "before target_ts").
            //   2. Skips the cache insert entirely so the bad window cannot
            //      shadow a correct one once the head advances and `latest_ts`
            //      catches up.
            //
            // 2026-01-01 UTC: start_ts=1767225600. Memoized latest_ts=1_000_000_000
            // (well before the date), latest_block=100.
            let bounds_memo = ChainBoundsMemo::new(DEFAULT_HEAD_TTL);
            *bounds_memo.head.lock().await = Some(HeadEntry {
                fetched_at: Instant::now(),
                latest_block: 100,
                latest_ts: Some(UnixTimestamp(1_000_000_000)),
            });

            let cache = crate::blocks::cache::MemoryCache::new();
            let head_counter = Arc::new(AtomicUsize::new(0));
            let date = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();

            let window = get_daily_window_with(
                &bounds_memo,
                &cache,
                NamedChain::Arbitrum,
                date,
                fetch_ts_unreachable(),
                counting_fetch_head(100, head_counter.clone()),
            )
            .await
            .expect("past-tip window must return the empty sentinel, not error");

            assert_eq!(
                (window.start_block, window.end_block),
                (100, 100),
                "past-tip window collapses to (latest, latest) — same sentinel \
                 compute_block_range_given_bounds returns for the matching case"
            );
            assert_eq!(
                head_counter.load(Ordering::SeqCst),
                0,
                "memoized head must short-circuit the eth_blockNumber fetch too"
            );
            assert_eq!(
                cache.stats().await.entries,
                0,
                "past-tip window must not be cached — the (chain, date) key has \
                 no notion of which head was current and would shadow the correct \
                 window once the chain catches up"
            );
        }

        #[tokio::test]
        async fn daily_window_touching_tip_runs_search_but_does_not_cache() {
            // Tip-adjacent case: the day contains the chain head but extends
            // past it. The binary search still runs (the day's start_block is
            // an honest interior probe), but the result is not persisted —
            // more blocks may land in the day's `end_ts_exclusive` range, and
            // the cache key cannot disambiguate "computed at head=4" from
            // "computed at head=N>4".
            //
            // Five-block monotonic chain. The day 2025-01-01 UTC spans
            // 1_735_689_600..1_735_776_000. Block 1 starts the day at
            // ts=1_735_689_700; block 4 (memoized head) has ts=1_735_750_000,
            // which is inside the day. So `start_ts <= latest_ts <
            // end_ts_exclusive.pred()` — day touches tip.
            let timestamps = vec![
                1_735_689_500, // block 0 — before the day
                1_735_689_700, // block 1 — first block on the day
                1_735_720_000, // block 2 — mid-day
                1_735_745_000, // block 3 — mid-day
                1_735_750_000, // block 4 — head, still inside the day
            ];
            let bounds_memo = ChainBoundsMemo::new(DEFAULT_HEAD_TTL);
            *bounds_memo.head.lock().await = Some(HeadEntry {
                fetched_at: Instant::now(),
                latest_block: 4,
                latest_ts: Some(UnixTimestamp(1_735_750_000)),
            });

            let cache = crate::blocks::cache::MemoryCache::new();
            let log = Arc::new(StdMutex::new(Vec::<BlockNumber>::new()));
            let head_counter = Arc::new(AtomicUsize::new(0));
            let date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();

            let window = get_daily_window_with(
                &bounds_memo,
                &cache,
                NamedChain::Arbitrum,
                date,
                counting_fetch_ts(timestamps, log.clone()),
                counting_fetch_head(4, head_counter.clone()),
            )
            .await
            .unwrap();

            // The day's end is past the head, so `end_block` clamps to `latest`.
            assert_eq!((window.start_block, window.end_block), (1, 4));
            assert!(
                !log.lock().unwrap().is_empty(),
                "binary search must still probe blocks when the day starts \
                 inside chain history"
            );
            assert_eq!(
                cache.stats().await.entries,
                0,
                "tip-adjacent window must not be cached — more blocks may land \
                 in the day's range and the cached entry would go stale silently"
            );
        }

        #[tokio::test]
        async fn daily_window_cold_memo_still_caches_historical_date() {
            // Cold memo, fully historical date. The fix must not regress this
            // path — without a memoized `latest_ts`, the short-circuits cannot
            // fire and the cache insert must proceed as before. (If a future
            // fix wants to gate caching on a sentinel check too, it should
            // come from a separate decision; this test pins down today's
            // behaviour.)
            let timestamps = vec![
                1_735_689_500,
                1_735_689_700,
                1_735_745_000,
                1_735_775_000,
                1_735_776_100,
            ];
            let bounds_memo = ChainBoundsMemo::new(DEFAULT_HEAD_TTL);
            let cache = crate::blocks::cache::MemoryCache::new();
            let date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();

            let log = Arc::new(StdMutex::new(Vec::<BlockNumber>::new()));
            let head_counter = Arc::new(AtomicUsize::new(0));

            let window = get_daily_window_with(
                &bounds_memo,
                &cache,
                NamedChain::Arbitrum,
                date,
                counting_fetch_ts(timestamps, log.clone()),
                counting_fetch_head(4, head_counter.clone()),
            )
            .await
            .unwrap();

            assert_eq!((window.start_block, window.end_block), (1, 3));
            assert_eq!(
                cache.stats().await.entries,
                1,
                "cold-memo historical date must still populate the cache"
            );
        }
    }
}
