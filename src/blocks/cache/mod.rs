// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Cache backends for block window calculations
//!
//! This module provides different caching strategies for storing block window data:
//!
//! - [`DiskCache`]: Persistent JSON-based cache with file locking (default)
//! - [`MemoryCache`]: In-memory cache with optional size limits
//! - [`NoOpCache`]: Disables caching entirely (for testing or specific use cases)
//!
//! # Examples
//!
//! ```rust,ignore
//! use semioscan::cache::{DiskCache, MemoryCache, NoOpCache};
//! use semioscan::BlockWindowCalculator;
//! use std::time::Duration;
//!
//! // Disk cache with TTL and size limits
//! let cache = DiskCache::new("cache.json")
//!     .with_ttl(Duration::from_secs(86400 * 7)) // 7 days
//!     .with_max_entries(1000)
//!     .validate()?;
//! let calculator = BlockWindowCalculator::new(provider, Box::new(cache));
//!
//! // Memory cache (no persistence)
//! let cache = MemoryCache::new()
//!     .with_max_entries(500);
//! let calculator = BlockWindowCalculator::new(provider, Box::new(cache));
//!
//! // No cache (always compute)
//! let cache = NoOpCache;
//! let calculator = BlockWindowCalculator::new(provider, Box::new(cache));
//! ```

use alloy_chains::NamedChain;
use async_trait::async_trait;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::blocks::window::DailyBlockWindow;
use crate::errors::BlockWindowError;

mod disk;
mod memory;
mod noop;
pub mod types;

pub use disk::DiskCache;
pub use memory::MemoryCache;
pub use noop::NoOpCache;

/// Key for caching daily block windows
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheKey {
    pub(crate) chain: NamedChain,
    pub(crate) date: NaiveDate,
}

impl CacheKey {
    /// Creates a new cache key for a specific chain and date
    pub fn new(chain: NamedChain, date: NaiveDate) -> Self {
        Self { chain, date }
    }
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.chain as u64, self.date)
    }
}

/// Statistics about cache performance
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheStats {
    /// Number of cache hits (successful retrievals)
    pub hits: u64,
    /// Number of cache misses (key not found)
    pub misses: u64,
    /// Number of entries evicted due to size limits
    pub evictions: u64,
    /// Number of entries expired due to TTL
    pub expirations: u64,
    /// Number of times an insert was deliberately skipped after a miss.
    ///
    /// Currently produced by [`BlockWindowCalculator::get_daily_window`]
    /// when the requested window touches or extends past the memoized
    /// chain tip: the lookup still misses (incrementing `misses`), but the
    /// computed window is intentionally not persisted because future
    /// blocks may shift the day's range and the `(chain, date)` cache key
    /// cannot disambiguate which head was current. Surfacing the count
    /// lets operators distinguish "expected tip skip" from "broken insert"
    /// (cache disk full, JSON write error) without staring at debug logs,
    /// and excludes the deliberate skips from [`Self::hit_rate`].
    ///
    /// [`BlockWindowCalculator::get_daily_window`]:
    ///     crate::blocks::window::BlockWindowCalculator::get_daily_window
    #[serde(default)]
    pub skip_inserts: u64,
    /// Current number of entries in the cache
    pub entries: usize,
}

impl CacheStats {
    /// Calculates the cache hit rate as a percentage (0.0 to 100.0).
    ///
    /// Deliberate skip-insert misses (see [`Self::skip_inserts`]) are
    /// excluded from the denominator so that tip-touching workloads do not
    /// degrade the reported rate. The formula is
    /// `hits / (hits + misses - skip_inserts)`; saturating subtraction
    /// keeps the denominator non-negative if a future cache backend
    /// records skips without a matching miss.
    pub fn hit_rate(&self) -> f64 {
        let denominator = self
            .hits
            .saturating_add(self.misses)
            .saturating_sub(self.skip_inserts);
        if denominator == 0 {
            0.0
        } else {
            (self.hits as f64 / denominator as f64) * 100.0
        }
    }
}

impl fmt::Display for CacheStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "hits={}, misses={}, skip_inserts={}, evictions={}, expirations={}, entries={}, hit_rate={:.1}%",
            self.hits,
            self.misses,
            self.skip_inserts,
            self.evictions,
            self.expirations,
            self.entries,
            self.hit_rate()
        )
    }
}

/// Trait for block window cache backends
///
/// Implementations provide different storage strategies for caching block windows.
/// All cache operations are async to support both in-memory and disk-based backends.
///
/// # Thread Safety
///
/// Implementations must be thread-safe and support concurrent access. Use interior
/// mutability (e.g., `Mutex`, `RwLock`) as needed.
///
/// # Error Handling
///
/// Cache operations should not fail the entire operation. If a cache read/write fails,
/// implementations should log the error and continue (treating failures as cache misses).
#[async_trait]
pub trait BlockWindowCache: Send + Sync {
    /// Retrieves a cached block window for the given key
    ///
    /// Returns `None` if:
    /// - The key is not in the cache
    /// - The cached entry has expired (if TTL is enabled)
    /// - A cache read error occurred (logged internally)
    async fn get(&self, key: &CacheKey) -> Option<DailyBlockWindow>;

    /// Inserts a block window into the cache
    ///
    /// If the cache has size limits and is full, this may evict older entries.
    /// Cache write errors are logged but do not cause failures.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the entry was cached successfully, or `Err` if caching failed.
    /// Callers should typically ignore errors (caching is best-effort).
    async fn insert(&self, key: CacheKey, window: DailyBlockWindow)
        -> Result<(), BlockWindowError>;

    /// Clears all entries from the cache
    ///
    /// Used for testing and cache management. Not all backends may support this.
    async fn clear(&self) -> Result<(), BlockWindowError>;

    /// Returns current cache statistics
    ///
    /// Statistics include hits, misses, evictions, and current size.
    async fn stats(&self) -> CacheStats;

    /// Records a deliberate decision not to insert a window after a miss.
    ///
    /// Currently invoked by
    /// [`BlockWindowCalculator::get_daily_window`] when the requested
    /// window touches or extends past the memoized chain tip — the
    /// computed window is partial and the `(chain, date)` key cannot
    /// disambiguate which head was current, so the insert is suppressed
    /// even though the preceding `get` produced a miss. Implementations
    /// increment [`CacheStats::skip_inserts`] so the count surfaces in
    /// operator-facing metrics; backends that don't track stats (e.g.
    /// [`NoOpCache`]) may leave this as a no-op.
    ///
    /// [`BlockWindowCalculator::get_daily_window`]:
    ///     crate::blocks::window::BlockWindowCalculator::get_daily_window
    async fn record_skip_insert(&self);

    /// Returns a human-readable name for this cache backend
    ///
    /// Used for logging and debugging.
    fn name(&self) -> &'static str;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hit_rate_excludes_skip_inserts_from_denominator() {
        // Workload: 99 historical hits + 30 tip-touching misses, each of
        // which also incremented skip_inserts. Without the fix, the
        // reported rate is 99/129 ≈ 76.7%, suggesting cache trouble.
        // With the skip_inserts adjustment, the deliberate skips are
        // excluded and the rate reflects only the lookup intent the
        // operator can act on.
        let stats = CacheStats {
            hits: 99,
            misses: 30,
            skip_inserts: 30,
            ..Default::default()
        };
        assert_eq!(stats.hit_rate(), 100.0);
    }

    #[test]
    fn hit_rate_falls_back_to_zero_when_all_misses_are_skips() {
        // Cold workload that only ever queries tip-touching dates:
        // every miss is a deliberate skip, no real lookups land. Reporting
        // the rate as 0% (rather than NaN or panicking on divide-by-zero)
        // keeps the metric usable.
        let stats = CacheStats {
            hits: 0,
            misses: 5,
            skip_inserts: 5,
            ..Default::default()
        };
        assert_eq!(stats.hit_rate(), 0.0);
    }

    #[test]
    fn hit_rate_unchanged_when_skip_inserts_zero() {
        // Backwards-compat: a workload that never hits the tip-skip path
        // produces the same rate the pre-counter formula would have.
        let stats = CacheStats {
            hits: 3,
            misses: 1,
            skip_inserts: 0,
            ..Default::default()
        };
        assert_eq!(stats.hit_rate(), 75.0);
    }

    #[test]
    fn hit_rate_saturates_when_skip_inserts_exceeds_misses() {
        // Defensive: a future backend could conceivably record a skip
        // without a matching miss (or surface a serialized snapshot whose
        // counters drifted). Saturating subtraction prevents the
        // denominator from underflowing to a giant u64 and producing a
        // nonsense near-zero rate.
        let stats = CacheStats {
            hits: 0,
            misses: 1,
            skip_inserts: 5,
            ..Default::default()
        };
        assert_eq!(stats.hit_rate(), 0.0);
    }

    #[test]
    fn display_includes_skip_inserts() {
        let stats = CacheStats {
            hits: 10,
            misses: 4,
            skip_inserts: 2,
            evictions: 1,
            expirations: 0,
            entries: 7,
        };
        let rendered = stats.to_string();
        assert!(
            rendered.contains("skip_inserts=2"),
            "Display must surface skip_inserts so operators see it in logs: {rendered}"
        );
    }
}
