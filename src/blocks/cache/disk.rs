// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
// SPDX-FileCopyrightText: 2026 Joseph Livesey <jlivesey@gmail.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Disk-based cache implementation with file locking and versioning

use alloy_chains::NamedChain;
use async_trait::async_trait;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    ffi::OsString,
    fmt,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::Mutex;
use tokio::task::JoinError;
use tracing::{debug, info, warn};

use super::{BlockWindowCache, CacheKey, CacheStats};
use crate::blocks::window::DailyBlockWindow;
use crate::errors::BlockWindowError;
use crate::types::cache::TimestampMillis;

/// Current cache format version
const CACHE_VERSION: u32 = 1;

/// Entry in the disk cache with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    /// The cached block window
    window: DailyBlockWindow,
    /// When this entry was created (for TTL and eviction ordering)
    #[serde(default)]
    created_at: TimestampMillis,
}

impl CacheEntry {
    fn new(window: DailyBlockWindow) -> Self {
        Self {
            window,
            created_at: TimestampMillis::now(),
        }
    }

    fn is_expired(&self, ttl: Option<Duration>) -> bool {
        if let Some(ttl) = ttl {
            return self.created_at.is_older_than(ttl);
        }
        false
    }
}

/// Serialized cache format (versioned)
#[derive(Debug, Serialize, Deserialize)]
struct CacheData {
    /// Cache format version
    version: u32,
    /// Cached entries (serialized with String keys for JSON compatibility)
    #[serde(
        serialize_with = "serialize_cache_entries",
        deserialize_with = "deserialize_cache_entries"
    )]
    entries: HashMap<CacheKey, CacheEntry>,
}

// Helper functions for serializing HashMap<CacheKey, CacheEntry> as HashMap<String, CacheEntry>
fn serialize_cache_entries<S>(
    entries: &HashMap<CacheKey, CacheEntry>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::Serialize;
    let string_map: HashMap<String, &CacheEntry> =
        entries.iter().map(|(k, v)| (k.to_string(), v)).collect();
    string_map.serialize(serializer)
}

fn deserialize_cache_entries<'de, D>(
    deserializer: D,
) -> Result<HashMap<CacheKey, CacheEntry>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let string_map: HashMap<String, CacheEntry> = HashMap::deserialize(deserializer)?;

    string_map
        .into_iter()
        .map(|(k, v)| {
            // Parse key string back to CacheKey (format: "chain_id:YYYY-MM-DD")
            let parts: Vec<&str> = k.split(':').collect();
            if parts.len() != 2 {
                return Err(serde::de::Error::custom(format!(
                    "Invalid cache key format: {}",
                    k
                )));
            }

            let chain_id: u64 = parts[0].parse().map_err(|e| {
                serde::de::Error::custom(format!("Invalid chain ID in key '{}': {}", k, e))
            })?;

            let chain = NamedChain::try_from(chain_id)
                .map_err(|_| serde::de::Error::custom(format!("Unknown chain ID: {}", chain_id)))?;

            let date = NaiveDate::parse_from_str(parts[1], "%Y-%m-%d").map_err(|e| {
                serde::de::Error::custom(format!("Invalid date in key '{}': {}", k, e))
            })?;

            Ok((CacheKey::new(chain, date), v))
        })
        .collect()
}

impl Default for CacheData {
    fn default() -> Self {
        Self {
            version: CACHE_VERSION,
            entries: HashMap::new(),
        }
    }
}

/// Configuration for disk cache
#[derive(Debug, Clone, Default)]
struct DiskCacheConfig {
    /// Maximum number of entries before eviction starts
    max_entries: Option<usize>,
    /// Time-to-live for cache entries
    ttl: Option<Duration>,
}

/// Internal state for disk cache
#[derive(Debug, Default)]
struct DiskCacheState {
    /// Cache statistics (in-memory only, not persisted)
    stats: CacheStats,
}

/// Disk-based cache with file locking, versioning, and TTL support
///
/// This cache persists block windows to disk as JSON with:
/// - File locking for multi-process safety (using advisory locks)
/// - Cache format versioning for future migrations
/// - Optional TTL (time-to-live) for automatic expiration
/// - Optional size limits with oldest-first eviction
/// - Path validation and helpful error messages
///
/// # Examples
///
/// ```rust,ignore
/// use semioscan::cache::DiskCache;
/// use std::time::Duration;
///
/// // Basic disk cache
/// let cache = DiskCache::new("cache.json")?;
///
/// // With TTL
/// let cache = DiskCache::new("cache.json")?
///     .with_ttl(Duration::from_secs(86400 * 7)); // 7 days
///
/// // With size limit
/// let cache = DiskCache::new("/var/cache/blocks.json")?
///     .with_max_entries(1000);
///
/// // With validation
/// let cache = DiskCache::new("cache.json")?
///     .validate()?;
/// ```
///
/// # File Locking
///
/// Uses advisory file locking on a stable companion lock file
/// (`<cache-file>.lock`) to protect read-modify-write updates. Multiple
/// processes can safely share the same cache file without losing inserts.
///
/// # Performance
///
/// - Get: O(1) HashMap lookup + file I/O (~1-2ms)
/// - Insert: O(1) + file write (~2-5ms)
/// - File size: Approximately 200 bytes per cached entry
#[derive(Debug)]
pub struct DiskCache {
    path: PathBuf,
    config: DiskCacheConfig,
    state: Mutex<DiskCacheState>,
}

impl DiskCache {
    /// Creates a new disk cache at the specified path
    ///
    /// The path can be absolute or relative. The parent directory must exist
    /// and be writable. If the cache file doesn't exist, it will be created
    /// on the first insert.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the cache file (e.g., "cache.json" or "/var/cache/blocks.json")
    ///
    /// # Returns
    ///
    /// Returns a `DiskCache` instance. Note that path validation is NOT performed
    /// until the first I/O operation. Use [`validate()`](Self::validate) to check
    /// the path immediately.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            config: DiskCacheConfig::default(),
            state: Mutex::new(DiskCacheState::default()),
        }
    }

    /// Sets the maximum number of entries in the cache
    ///
    /// When the limit is reached, the oldest entries (by creation time) will be
    /// evicted to make room for new entries.
    pub fn with_max_entries(mut self, max_entries: usize) -> Self {
        self.config.max_entries = Some(max_entries);
        self
    }

    /// Sets the time-to-live for cache entries
    ///
    /// Entries older than the TTL will be automatically expired when accessed.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.config.ttl = Some(ttl);
        self
    }

    /// Validates the cache path and creates parent directory if needed
    ///
    /// This method checks that:
    /// - The parent directory exists (or creates it)
    /// - The parent directory is writable
    /// - The path is valid
    ///
    /// # Errors
    ///
    /// Returns an error if the parent directory cannot be created or is not writable.
    pub fn validate(self) -> Result<Self, BlockWindowError> {
        // Get parent directory
        let parent = self.path.parent().ok_or_else(|| {
            BlockWindowError::cache_io_error(
                self.path.display().to_string(),
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Cache path has no parent directory",
                ),
            )
        })?;

        // Create parent directory if it doesn't exist
        if !parent.exists() {
            std::fs::create_dir_all(parent).map_err(|e| {
                BlockWindowError::cache_io_error(
                    format!(
                        "Failed to create cache directory '{}': {}",
                        parent.display(),
                        e
                    ),
                    e,
                )
            })?;
            debug!(path = %parent.display(), "Created cache directory");
        }

        // Validate parent is writable by attempting to create a temp file
        let test_file = parent.join(".cache_write_test");
        std::fs::write(&test_file, b"test").map_err(|e| {
            BlockWindowError::cache_io_error(
                format!(
                    "Cache directory '{}' is not writable: {}",
                    parent.display(),
                    e
                ),
                e,
            )
        })?;
        let _ = std::fs::remove_file(&test_file);

        debug!(path = %self.path.display(), "Cache path validated successfully");
        Ok(self)
    }

    /// Loads cache data from disk with file locking
    async fn load(&self) -> Result<CacheData, BlockWindowError> {
        let path = self.path.clone();
        let lock_path = Self::lock_path_for(&path);
        let error_path = path.clone();

        let data = tokio::task::spawn_blocking(move || -> Result<CacheData, BlockWindowError> {
            let _lock = Self::acquire_lock(&lock_path, LockMode::Shared)?;
            Self::load_unlocked(&path)
        })
        .await
        .map_err(|e| Self::blocking_task_error(BlockingCacheOperation::Load, error_path, e))??;

        info!(
            path = %self.path.display(),
            entries = data.entries.len(),
            version = data.version,
            "Loaded block window cache"
        );

        Ok(data)
    }

    fn load_unlocked(path: &Path) -> Result<CacheData, BlockWindowError> {
        if !path.exists() {
            debug!(path = %path.display(), "Cache file does not exist, using empty cache");
            return Ok(CacheData::default());
        }

        let file = match File::open(path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!(path = %path.display(), "Cache file does not exist, using empty cache");
                return Ok(CacheData::default());
            }
            Err(e) => {
                return Err(BlockWindowError::cache_io_error(
                    format!(
                        "Failed to open cache file '{}': {}. Ensure the file is readable.",
                        path.display(),
                        e
                    ),
                    e,
                ));
            }
        };

        let data: CacheData = serde_json::from_reader(&file).map_err(|e| {
            warn!(
                path = %path.display(),
                error = %e,
                "Failed to parse cache file"
            );
            BlockWindowError::serialization_error(e)
        })?;

        if data.version != CACHE_VERSION {
            warn!(
                path = %path.display(),
                cached_version = data.version,
                current_version = CACHE_VERSION,
                "Cache version mismatch, ignoring cached data"
            );
            return Ok(CacheData::default());
        }

        Ok(data)
    }

    /// Saves cache data to disk with atomic write. The caller must hold the
    /// stable cache lock for writes that participate in cross-instance safety.
    fn save_unlocked(path: &Path, data: CacheData) -> Result<(), BlockWindowError> {
        let json =
            serde_json::to_vec_pretty(&data).map_err(BlockWindowError::serialization_error)?;

        Self::ensure_parent_dir(path)?;

        // Write-then-rename for atomicity.
        let temp_path = path.with_extension("tmp");

        std::fs::write(&temp_path, &json).map_err(|e| {
            BlockWindowError::cache_io_error(
                format!(
                    "Failed to write cache to '{}': {}. Ensure the parent directory is writable.",
                    temp_path.display(),
                    e
                ),
                e,
            )
        })?;

        std::fs::rename(&temp_path, path).map_err(|e| {
            BlockWindowError::cache_io_error(
                format!(
                    "Failed to rename cache file from '{}' to '{}': {}",
                    temp_path.display(),
                    path.display(),
                    e
                ),
                e,
            )
        })?;

        Ok(())
    }

    fn insert_locked(
        path: &Path,
        config: &DiskCacheConfig,
        key: CacheKey,
        window: DailyBlockWindow,
    ) -> Result<(usize, usize), BlockWindowError> {
        let lock_path = Self::lock_path_for(path);
        let _lock = Self::acquire_lock(&lock_path, LockMode::Exclusive)?;

        let mut data = Self::load_unlocked(path).unwrap_or_default();
        debug!(key = %key, "Inserting entry into disk cache");
        data.entries.insert(key, CacheEntry::new(window));

        let evicted = match config.max_entries {
            Some(max_entries) => Self::evict_oldest(&mut data, max_entries),
            None => 0,
        };
        let entry_count = data.entries.len();

        Self::save_unlocked(path, data)?;

        Ok((entry_count, evicted))
    }

    fn clear_locked(path: &Path) -> Result<(), BlockWindowError> {
        let lock_path = Self::lock_path_for(path);
        let _lock = Self::acquire_lock(&lock_path, LockMode::Exclusive)?;

        if path.exists() {
            std::fs::remove_file(path).map_err(|e| {
                BlockWindowError::cache_io_error(
                    format!("Failed to delete cache file '{}': {}", path.display(), e),
                    e,
                )
            })?;
        }

        Ok(())
    }

    fn acquire_lock(lock_path: &Path, mode: LockMode) -> Result<File, BlockWindowError> {
        Self::ensure_parent_dir(lock_path)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .map_err(|e| {
                BlockWindowError::cache_io_error(
                    format!(
                        "Failed to open cache lock file '{}': {}",
                        lock_path.display(),
                        e
                    ),
                    e,
                )
            })?;

        match mode {
            LockMode::Shared => file.lock_shared(),
            LockMode::Exclusive => file.lock(),
        }
        .map_err(|e| {
            BlockWindowError::cache_io_error(
                format!(
                    "Failed to acquire {} lock on cache lock file '{}': {}",
                    mode,
                    lock_path.display(),
                    e
                ),
                e,
            )
        })?;

        Ok(file)
    }

    fn ensure_parent_dir(path: &Path) -> Result<(), BlockWindowError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                BlockWindowError::cache_io_error(
                    format!(
                        "Failed to create cache directory '{}': {}. Ensure you have write permissions.",
                        parent.display(),
                        e
                    ),
                    e,
                )
            })?;
        }

        Ok(())
    }

    fn lock_path_for(path: &Path) -> PathBuf {
        let lock_file_name = path
            .file_name()
            .map(|file_name| {
                let mut name = OsString::from(file_name);
                name.push(".lock");
                name
            })
            .unwrap_or_else(|| OsString::from(".semioscan-cache.lock"));

        path.with_file_name(lock_file_name)
    }

    fn blocking_task_error(
        operation: BlockingCacheOperation,
        path: PathBuf,
        error: JoinError,
    ) -> BlockWindowError {
        BlockWindowError::cache_io_error(
            format!("Failed to {operation} '{}'", path.display()),
            std::io::Error::other(format!("blocking cache task failed: {error}")),
        )
    }

    /// Evicts the oldest entries to maintain size limit
    fn evict_oldest(data: &mut CacheData, max_entries: usize) -> usize {
        let mut evicted = 0;

        while data.entries.len() > max_entries {
            // Find oldest entry by created_at timestamp, using cache key as stable tiebreaker
            let oldest_key = data
                .entries
                .iter()
                .min_by(|(key_a, entry_a), (key_b, entry_b)| {
                    // Primary sort: by timestamp (oldest first)
                    entry_a
                        .created_at
                        .cmp(&entry_b.created_at)
                        // Secondary sort: by cache key (for deterministic ordering when timestamps equal)
                        .then_with(|| key_a.to_string().cmp(&key_b.to_string()))
                })
                .map(|(key, _)| key.clone());

            if let Some(key) = oldest_key {
                debug!(key = %key, "Evicting oldest cache entry");
                data.entries.remove(&key);
                evicted += 1;
            } else {
                break;
            }
        }

        evicted
    }
}

#[derive(Debug, Clone, Copy)]
enum LockMode {
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, Copy)]
enum BlockingCacheOperation {
    Load,
    Insert,
    Clear,
}

impl fmt::Display for LockMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockMode::Shared => formatter.write_str("shared"),
            LockMode::Exclusive => formatter.write_str("exclusive"),
        }
    }
}

impl fmt::Display for BlockingCacheOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockingCacheOperation::Load => formatter.write_str("load cache file"),
            BlockingCacheOperation::Insert => formatter.write_str("insert cache entry"),
            BlockingCacheOperation::Clear => formatter.write_str("clear cache file"),
        }
    }
}

#[async_trait]
impl BlockWindowCache for DiskCache {
    async fn get(&self, key: &CacheKey) -> Option<DailyBlockWindow> {
        let mut state = self.state.lock().await;

        // Load cache data
        let data = match self.load().await {
            Ok(data) => data,
            Err(e) => {
                warn!(error = %e, "Failed to load cache, treating as miss");
                state.stats.misses += 1;
                return None;
            }
        };

        if let Some(entry) = data.entries.get(key) {
            // Check if expired
            if entry.is_expired(self.config.ttl) {
                debug!(key = %key, "Cache entry expired");
                state.stats.expirations += 1;
                state.stats.misses += 1;
                return None;
            }

            state.stats.hits += 1;
            debug!(key = %key, "Cache hit (disk)");
            Some(entry.window.clone())
        } else {
            state.stats.misses += 1;
            debug!(key = %key, "Cache miss (disk)");
            None
        }
    }

    async fn insert(
        &self,
        key: CacheKey,
        window: DailyBlockWindow,
    ) -> Result<(), BlockWindowError> {
        let path = self.path.clone();
        let config = self.config.clone();
        let error_path = path.clone();

        let (entry_count, evicted) =
            tokio::task::spawn_blocking(move || Self::insert_locked(&path, &config, key, window))
                .await
                .map_err(|e| {
                    Self::blocking_task_error(BlockingCacheOperation::Insert, error_path, e)
                })??;

        let mut state = self.state.lock().await;
        state.stats.entries = entry_count;
        state.stats.evictions += evicted as u64;

        debug!(
            path = %self.path.display(),
            entries = entry_count,
            "Saved block window cache"
        );

        Ok(())
    }

    async fn clear(&self) -> Result<(), BlockWindowError> {
        let path = self.path.clone();
        let error_path = path.clone();

        debug!(path = %self.path.display(), "Clearing disk cache");

        tokio::task::spawn_blocking(move || Self::clear_locked(&path))
            .await
            .map_err(|e| {
                Self::blocking_task_error(BlockingCacheOperation::Clear, error_path, e)
            })??;

        let mut state = self.state.lock().await;
        state.stats.entries = 0;
        Ok(())
    }

    async fn stats(&self) -> CacheStats {
        let mut state = self.state.lock().await;

        // Update entry count from disk
        if let Ok(data) = self.load().await {
            state.stats.entries = data.entries.len();
        }

        state.stats.clone()
    }

    async fn record_skip_insert(&self) {
        let mut state = self.state.lock().await;
        state.stats.skip_inserts += 1;
    }

    fn name(&self) -> &'static str {
        "DiskCache"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_chains::NamedChain;
    use chrono::NaiveDate;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::Barrier;

    fn create_test_window(start_block: u64, end_block: u64) -> DailyBlockWindow {
        DailyBlockWindow {
            start_block,
            end_block,
            start_ts: crate::blocks::window::UnixTimestamp(1728518400),
            end_ts_exclusive: crate::blocks::window::UnixTimestamp(1728604800),
        }
    }

    fn create_test_key(day: u32) -> CacheKey {
        CacheKey::new(
            NamedChain::Arbitrum,
            NaiveDate::from_ymd_opt(2025, 10, day).unwrap(),
        )
    }

    #[tokio::test]
    async fn test_disk_cache_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("cache.json");
        let cache = DiskCache::new(&cache_path).validate().unwrap();

        let key = create_test_key(15);
        let window = create_test_window(1000, 2000);

        // Cache miss initially
        assert!(cache.get(&key).await.is_none());

        // Insert and verify
        assert!(cache.insert(key.clone(), window.clone()).await.is_ok());
        let retrieved = cache.get(&key).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().start_block, 1000);

        // Stats should show 1 hit, 1 miss
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[tokio::test]
    async fn test_disk_cache_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("cache.json");

        let key = create_test_key(15);
        let window = create_test_window(1000, 2000);

        // Create cache and insert
        {
            let cache = DiskCache::new(&cache_path).validate().unwrap();
            cache.insert(key.clone(), window).await.unwrap();
        }

        // Create new cache instance and verify data persisted
        {
            let cache = DiskCache::new(&cache_path).validate().unwrap();
            let retrieved = cache.get(&key).await;
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().start_block, 1000);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_inserts_from_two_instances_preserve_both_entries() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("cache.json");

        let cache_a = Arc::new(DiskCache::new(&cache_path).validate().unwrap());
        let cache_b = Arc::new(DiskCache::new(&cache_path).validate().unwrap());
        let barrier = Arc::new(Barrier::new(3));

        let key_a = create_test_key(15);
        let key_b = create_test_key(16);
        let window_a = create_test_window(1000, 2000);
        let window_b = create_test_window(3000, 4000);

        let insert_a = {
            let cache = Arc::clone(&cache_a);
            let barrier = Arc::clone(&barrier);
            let key = key_a.clone();
            let window = window_a.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                cache.insert(key, window).await
            })
        };

        let insert_b = {
            let cache = Arc::clone(&cache_b);
            let barrier = Arc::clone(&barrier);
            let key = key_b.clone();
            let window = window_b.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                cache.insert(key, window).await
            })
        };

        barrier.wait().await;
        insert_a.await.unwrap().unwrap();
        insert_b.await.unwrap().unwrap();

        let verifier = DiskCache::new(&cache_path).validate().unwrap();
        assert_eq!(verifier.get(&key_a).await.unwrap().start_block, 1000);
        assert_eq!(verifier.get(&key_b).await.unwrap().start_block, 3000);
        assert!(
            DiskCache::lock_path_for(&cache_path).exists(),
            "disk cache updates should coordinate through a stable companion lock file"
        );
    }

    #[tokio::test]
    async fn test_disk_cache_size_limit() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("cache.json");
        let cache = DiskCache::new(&cache_path)
            .with_max_entries(3)
            .validate()
            .unwrap();

        // Insert 4 entries (eviction uses stable ordering even if timestamps are equal)
        for day in 1..=4 {
            let key = create_test_key(day);
            let window = create_test_window(day as u64 * 1000, day as u64 * 2000);
            cache.insert(key, window).await.unwrap();
        }

        // Only 3 should remain (oldest evicted)
        let stats = cache.stats().await;
        assert_eq!(stats.entries, 3);
        assert_eq!(stats.evictions, 1);

        // First entry should be gone (evicted based on timestamp + key ordering)
        assert!(cache.get(&create_test_key(1)).await.is_none());

        // Last 3 should still be present
        assert!(cache.get(&create_test_key(2)).await.is_some());
        assert!(cache.get(&create_test_key(3)).await.is_some());
        assert!(cache.get(&create_test_key(4)).await.is_some());
    }

    #[tokio::test]
    async fn test_disk_cache_deterministic_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("cache.json");
        let cache = DiskCache::new(&cache_path)
            .with_max_entries(2)
            .validate()
            .unwrap();

        // Insert 3 entries rapidly (all will have same timestamp)
        // Keys are: Arbitrum:2025-10-01, Arbitrum:2025-10-02, Arbitrum:2025-10-03
        // When sorted lexicographically: 2025-10-01 < 2025-10-02 < 2025-10-03
        for day in 1..=3 {
            let key = create_test_key(day);
            let window = create_test_window(day as u64 * 1000, day as u64 * 2000);
            cache.insert(key, window).await.unwrap();
        }

        // Should evict deterministically based on cache key ordering when timestamps equal
        // Expected: 2025-10-01 is evicted (smallest key lexicographically)
        assert!(cache.get(&create_test_key(1)).await.is_none());
        assert!(cache.get(&create_test_key(2)).await.is_some());
        assert!(cache.get(&create_test_key(3)).await.is_some());
    }

    #[tokio::test]
    async fn test_disk_cache_ttl() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("cache.json");
        let cache = DiskCache::new(&cache_path)
            .with_ttl(Duration::from_millis(50))
            .validate()
            .unwrap();

        let key = create_test_key(15);
        let window = create_test_window(1000, 2000);

        // Insert and verify immediately
        cache.insert(key.clone(), window).await.unwrap();
        assert!(cache.get(&key).await.is_some());

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should be expired now
        assert!(cache.get(&key).await.is_none());

        let stats = cache.stats().await;
        assert_eq!(stats.expirations, 1);
    }

    #[tokio::test]
    async fn test_disk_cache_clear() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("cache.json");
        let cache = DiskCache::new(&cache_path).validate().unwrap();

        // Insert entries
        for day in 1..=5 {
            let key = create_test_key(day);
            let window = create_test_window(day as u64 * 1000, day as u64 * 2000);
            cache.insert(key, window).await.unwrap();
        }

        // Clear cache
        cache.clear().await.unwrap();

        // File should be deleted
        assert!(!cache_path.exists());

        let stats = cache.stats().await;
        assert_eq!(stats.entries, 0);
    }

    #[tokio::test]
    async fn test_disk_cache_validation() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("subdir").join("cache.json");

        // Validation should create parent directory
        let cache = DiskCache::new(&cache_path).validate();
        assert!(cache.is_ok());

        // Parent directory should exist now
        assert!(cache_path.parent().unwrap().exists());
    }
}
