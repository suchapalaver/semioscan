# Changelog

All notable changes to semioscan will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking Changes

- `MaxBlockRange::chunk_range`, `MaxBlockRange::chunks_needed`, and the
  `ChunkIterator` type are no longer part of the public API. They had
  no in-crate caller after the chunked log path was consolidated onto
  `LogScanner`, so they exposed dead surface area that diverged from
  the scanner's actual iteration (the scanner's loop terminates
  correctly when `end_block == u64::MAX`; the iterator did not).
  Consumers that were slicing block ranges directly should fold the
  arithmetic into their own loop (`current.saturating_add(chunk_size
  - 1).min(end)` per chunk, advancing via `to_block.checked_add(1)`
  so the loop breaks when the chunk ends at `u64::MAX`) or reach for
  `LogScanner` / `fetch_logs_chunked` / `EventScanner` to fetch logs
  in chunks. Closes #36.

### Fixed

- `GasCache` and `PriceCache` no longer return an over-counted aggregate
  when a query window is narrower than a cached entry. Before, a cache
  holding `[50, 350]` would answer a `[100, 300]` query with the wider
  range's totals, inflating gas, transaction-count, and USDC figures by
  the contributions of blocks `50..=99` and `301..=350`. Daily/per-window
  reports run against the same cache could no longer be reconciled
  against an independent ledger. The cache now answers the narrower
  query by rescanning the requested window and returning totals scoped
  to it. Closes #29.

### Changed

- `GasCache::get`, `PriceCache::get`, and the underlying
  `BlockRangeCache::get` now return a cached value only on an exact
  range match. Calls that previously succeeded because a wider cached
  entry contained the query window now return `None`; use
  `calculate_gaps` for gap-aware lookup.
- `calculate_gaps` returns merged data only from cached entries that lie
  fully inside the query window. A wider entry that extends outside the
  window is ignored and the whole window is reported as a gap so the
  caller can produce a window-scoped aggregate. As a consequence, a
  narrower query against a cache holding a wider partially-overlapping
  entry will rescan its window on every call until the wider entry is
  invalidated; consumers that need both granularities should query at
  the narrower window first or call `clear_signer_data` /
  `clear_old_blocks` between phases.

## [0.14.1] - 2026-05-25

### Changed

- `BlockWindowCalculator::with_disk_cache` now makes one additional RPC
  call per chain (per 30 seconds) when `get_daily_window` first runs in
  a process. This restores disk persistence for daily-window-only
  callers (see "Fixed"), but means `get_daily_window` against a
  `with_disk_cache` calculator can now return
  `BlockWindowError::Rpc(BlockNotFound)` if the chain reorgs out the
  just-reported head between the two RPC calls — a failure mode 0.14.0
  removed for daily-window callers. Callers that need persistence
  across restarts but cannot tolerate this failure should build a
  `DiskCache` and pass it through `BlockWindowCalculator::new`
  instead; that path keeps 0.14.0's behavior.

### Fixed

- `BlockWindowCalculator::with_disk_cache` now writes block windows to
  the cache file for callers that only use `get_daily_window`.
  Previously, a daily reconciliation backfill or any one-method-per-process
  consumer paired with this constructor found an empty cache file after
  every process restart and re-ran the binary search for every date —
  the constructor's "persistence across restarts" promise silently held
  only for callers that also used `block_range_for_timestamps`. Other
  constructors (`new`, `with_memory_cache`, `without_cache`) are
  unchanged. Closes #18.

## [0.14.0] - 2026-05-24

### Breaking Changes

- `BlockWindowCache` gained a `record_skip_insert` trait method without a
  default implementation. Downstream implementations of the trait must add
  the method; in-tree backends (`MemoryCache`, `DiskCache`, `NoOpCache`)
  already implement it. The method records deliberate cache-insert skips
  (see the daily-window changes below) into `CacheStats::skip_inserts` so
  operators can distinguish them from broken inserts; backends that don't
  track stats may leave the body empty.
- `CacheStats` gained a `pub skip_inserts: u64` field. Code constructing
  `CacheStats` with a struct literal must include the field; consumers
  that read it through accessor methods (`hit_rate`, `Display`) are
  unaffected.

### Added

- `BlockWindowCalculator::with_head_ttl(Duration)` — builder method that
  overrides the TTL used to memoize the chain head shared by
  `block_range_for_timestamps` and `get_daily_window`. The default
  (`DEFAULT_HEAD_TTL`, also newly public) is 30 seconds; pass
  `Duration::ZERO` to disable head memoization entirely.
- `DEFAULT_HEAD_TTL` — public `const Duration` exposing the default head
  TTL so consumers can derive values from it (e.g.
  `with_head_ttl(DEFAULT_HEAD_TTL * 4)`) instead of using magic literals.
- `CacheStats::skip_inserts` — counter that surfaces deliberate
  cache-insert skips (tip-touching daily windows, cold-memo daily
  windows) in operator-facing metrics. `CacheStats::hit_rate` now
  excludes deliberate skips from the denominator so the rate reflects
  the cacheable-population only.

### Changed

- `BlockWindowCalculator::block_range_for_timestamps` no longer issues a
  redundant genesis-block header fetch, `eth_blockNumber`, and head-block
  header fetch on every call. Genesis is now memoized for the lifetime
  of the calculator (immutable per chain); the chain head is memoized
  per-instance with the configurable TTL above. A long-running sweep
  that resolves N timestamp ranges on the same chain now performs 1
  genesis fetch and 1 head fetch per TTL window, eliminating the prior
  2·N redundant header fetches against a rate-limited RPC.
- `BlockWindowCalculator::get_daily_window` now shares the same head
  memo as `block_range_for_timestamps`, so the head TTL configured via
  `with_head_ttl` amortizes across both methods. A long-cold-cache
  backfill issues a single `eth_blockNumber` per TTL window instead of
  one per uncached day, and mixed workloads that interleave the two
  methods share a single head fetch. The daily-window path only fetches
  the head's block number (no extra `eth_getBlockByNumber`); a
  subsequent `block_range_for_timestamps` call promotes the partial
  entry to the full `(block, ts)` shape when it needs the timestamp.

### Fixed

- `get_daily_window` no longer fails on a transient `eth_getBlockByNumber`
  error against the just-reported head (one-block reorg, free-tier
  provider cache lag) when the requested date is fully inside chain
  history. The daily-window path only needs the head's block number, so
  the head's timestamp is no longer fetched eagerly on the cold-memo
  path.
- `get_daily_window` no longer persists windows that touch or extend
  past the memoized chain tip. Such windows depend on future chain
  state, and the `(chain, date)` cache key cannot disambiguate a
  window computed against one head from one computed against a later
  head — caching it would shadow the correct window once the chain
  advanced into the day's range. Deliberate skips increment
  `CacheStats::skip_inserts`. Dates strictly past the tip short-circuit
  to the empty-window sentinel `(latest, latest)` without running the
  binary search.
- `get_daily_window` no longer persists windows when the head's
  timestamp is unknown (cold memo from a daily-window-only caller).
  Without `latest_ts` the caller cannot distinguish a fully-historical
  day from one whose head sits inside the requested day, so the
  conservative shape is to skip the cache insert. The best-effort
  binary-search window is still returned; only the persistence step is
  skipped, at the cost of one extra binary search on a subsequent cold
  restart for the same date. Closes #14.

## [0.13.0] - 2026-05-23

### Added

- `BlockWindowCalculator::block_range_for_timestamps` — public API for
  resolving an arbitrary inclusive timestamp range to the inclusive block
  range that covers it. Same binary search as `get_daily_window`, but at
  arbitrary granularity (useful for event-driven or bridge-reconciliation
  windows).
- `BlockWindowError::NonMonotonicTimestamps` — typed error returned when
  the chain's genesis and head timestamps are out of order, surfacing
  invalid binary-search preconditions rather than returning silently-wrong
  block boundaries.

### Changed

- `DiskCache` file I/O (open, lock, read, write, rename) now runs on the
  blocking thread pool via `tokio::task::spawn_blocking`, so synchronous
  std file locks no longer stall the async runtime worker holding them.
  `load` short-circuits before the dispatch when the cache file does not
  yet exist.

## [0.12.1] - 2026-05-23

### Changed

- Upgraded `alloy-primitives`, `alloy-dyn-abi`, `alloy-sol-types`, and `alloy-sol-type-parser` to 1.6.

## [0.12.0] - 2026-05-04

### Breaking Changes

- Removed the `odos-example` feature and the `odos-sdk` dependency. `OdosPriceSource`, the `RouterType` re-export, and the `router_token_discovery` example are deleted. Consumers that need Odos integration should vendor `OdosPriceSource` from a prior tag (last available in `v0.11.3` at `src/price/odos.rs`) into their own crate.
- `PriceCalculator`, `RawSwapResult`, `TokenPriceResult`, and `price::cache` are no longer feature-gated and are now part of the always-on public API.
- `RouterType` is no longer re-exported from `semioscan`. Import it from `odos-sdk` directly if needed.

## [0.11.3] - 2026-05-04

### Changed

- Upgraded `odos-sdk` to 10.0 (only affects consumers using the `odos-example` feature).
- Upgraded `proptest` dev dependency to 1.11.

## [0.11.2] - 2026-04-29

### Changed

- Upgraded `odos-sdk` to 9.0 (only affects consumers using the `odos-example` feature).

## [0.11.1] - 2026-04-29

### Changed

- Upgraded `odos-sdk` to 8.0 (only affects consumers using the `odos-example` feature).

## [0.11.0] - 2026-04-29

### Changed

- Upgraded `alloy` crates to 2.0 (and `op-alloy-network` to 2.0). Consumers must update their own `alloy` dependency to 2.0 to use this version.
- Upgraded `odos-sdk` to 6.0 (only affects consumers using the `odos-example` feature).
- `create_typed_http_provider<N>` no longer requires `N: RecommendedFillers`; the helper now produces a bare `RootProvider<N>` for any `N: Network`.

## [0.10.1] - 2026-04-10

### Changed

- Upgraded `odos-sdk` to 4.0

## [0.10.0] - 2026-03-13

### Breaking Changes

- `CombinedDataResult` now includes `retrieval_metadata`, exposing structured partial-failure details so callers can reject incomplete combined retrieval results explicitly.

### Added

- Configurable serial combined-lookup fallback attempts in `SemioscanConfig`, including per-chain overrides.
- `zksync_combined_probe` diagnostic example for comparing scanned transfer totals, typed transaction lookup behavior, permissive raw decoding, and the combined retrieval path against a real zkSync provider.

### Fixed

- Combined transfer-and-gas retrieval no longer silently hides decoded transfers that fail transaction or receipt enrichment; the result now records structured partial-failure metadata and does one bounded serial fallback pass.
- zkSync combined retrieval now retries transaction lookups with permissive raw decoding when Ethereum-typed transaction deserialization fails on zkSync-specific response shapes.
- EIP-2930 (access-list) transactions now correctly use the transaction's own gas price instead of the receipt effective gas price.
- Async tracing spans now correctly instrument the full async function body instead of only the synchronous prefix before the first `.await`.

### Deprecated

- `RpcError::ChainConnectionFailed` and its `chain_connection_failed()` helper in favor of `RpcError::RequestFailed` / `request_failed()` for tx/receipt/log method failures.

## [0.9.2] - 2026-02-26

### Changed

- Upgraded `odos-sdk` to 3.0

## [0.9.1] - 2026-02-25

### Changed

- Upgraded alloy crates to 1.7
- Switched from `alloy-erc20-full` fork to upstream `alloy-erc20`
- Upgraded `op-alloy-network` to 0.24

### Fixed

- Removed broken intra-doc link to non-existent `ChainProvider` type

## [0.9.0] - 2026-01-16

### Breaking Changes

**Renamed liquidator filter to sender filter in OdosPriceSource**

- `with_liquidator_filter()` renamed to `with_sender_filter()`
- Internal `liquidator_address` field renamed to `sender_address`
- The filter applies to any sender address, not just liquidation bots

#### Migration Guide

```rust
// Before (v0.8.x)
let price_source = OdosPriceSource::for_chain(NamedChain::Arbitrum, RouterType::V2)?
    .with_liquidator_filter("0x123...".parse().unwrap());

// After (v0.9.0)
let price_source = OdosPriceSource::for_chain(NamedChain::Arbitrum, RouterType::V2)?
    .with_sender_filter("0x123...".parse().unwrap());
```

### Added

- **`sender_address()` method on `PriceSource` trait**: Returns the configured sender filter address (if any). Default implementation returns `None`.
- **V3 router tests**: Added signature verification test for SwapMultiV3 event and Base chain V3 support test
- **SwapMulti V3 extraction test**: Added integration test for V3 router SwapMulti event decoding

## [0.8.1] - 2026-01-16

### Changed

- Updated odos-sdk to 2.0
- Updated and upgraded cargo dependencies

## [0.8.0] - 2026-01-07

### Added

- **Standalone chunked log fetching utility**
  - `fetch_logs_chunked()` function for fetching logs over large block ranges
  - Simpler API than `EventScanner` for use cases that just need chunked log fetching with a custom filter
  - No `SemioscanConfig` or chain-specific configuration required
  - Exported from crate root as `semioscan::fetch_logs_chunked`

- **Input validation for chunked log fetching**
  - Returns `InvalidInput` error for zero chunk size
  - Returns `InvalidInput` error for missing `from_block` or `to_block` in filter

## [0.7.0] - 2026-01-06

### Added

- **Per-transaction swap data extraction**
  - `RawSwapResult` struct for individual swap details with normalized amounts
  - `PriceCalculator::extract_raw_swaps()` method for per-transaction granularity
  - Useful for fee calculations and detailed swap analysis

- **Transaction metadata in SwapData**
  - `tx_hash: Option<B256>` field for transaction hash
  - `block_number: Option<BlockNumber>` field for block number
  - Automatically populated when extracting swaps from logs

- **Custom decimal precision support**
  - `DecimalPrecision::Custom(u8)` variant for arbitrary token decimals
  - Enables `u256_to_bigdecimal()` to work with any token, not just USDC/native

- **EventScanner public export**
  - `EventScanner` now exported from crate root as `semioscan::EventScanner`
  - Enables external consumers to use the scanner with built-in chunking and rate limiting

## [0.6.0] - 2026-01-04

### Added

- **Chain-aware OdosPriceSource with V3 router support**
  - `for_chain(chain, router_type)` constructor resolves router addresses via odos-sdk's chain registry
  - V3 router support with `SwapV3`/`SwapMultiV3` event decoding
  - `all_routers_for_chain(chain)` discovers all available routers for a chain
  - `OdosError` type for unsupported chain/router handling
  - Re-export `RouterType` from odos-sdk for ergonomic imports

### Changed

- **Upgraded to odos-sdk 1.2 APIs**
  - Use `RouterType::swap_routers()` instead of hardcoded router list
  - Use `router_type.emits_swap_events()` for type-safe router validation

### Fixed

- **Reject LimitOrder router type in OdosPriceSource**: LimitOrder routers emit `LimitOrderFilled` events instead of `Swap`/`SwapMulti` events, which was incorrectly handled by falling back to V2 event parsing

## [0.5.2] - 2026-01-04

### Changed

- **Cleaner INFO-level logs**: Demoted `EventScanner` logs from INFO to DEBUG level
  - "Starting event scan", "Finished event scan", and per-chunk logs are now DEBUG
  - Domain-level APIs (token discovery, transfer amounts) provide better context at INFO level
  - Reduces log noise for production deployments

## [0.5.1] - 2026-01-03

### Changed

- Updated and upgraded cargo dependencies

## [0.5.0] - 2026-01-03

### Breaking Changes

**Removed `LoggingLayer` in favor of alloy's native tracing**

- Removed `LoggingLayer` and `LoggingService` from `semioscan::transport`
- Removed `logging_enabled` field from `ProviderConfig`
- Removed `with_logging()` method from `ProviderConfig` and `DynProviderBuilder`
- Removed `enable_logging` field from `ProviderPool` and `ProviderPoolBuilder`
- Removed `with_logging()` method from `ProviderPoolBuilder`

Alloy's HTTP transport now provides native tracing at DEBUG/TRACE level, making our custom logging layer redundant.

#### Migration Guide

**Before (v0.4.x)**:

```rust
use semioscan::{LoggingLayer, ProviderConfig, create_http_provider};

// Using LoggingLayer directly
let client = ClientBuilder::default()
    .layer(LoggingLayer::new())
    .http(url);

// Using ProviderConfig
let provider = create_http_provider(
    ProviderConfig::new("https://eth.llamarpc.com")
        .with_logging(true)
)?;
```

**After (v0.5.0)**:

```rust
use semioscan::{ProviderConfig, create_http_provider};

// Enable logging via tracing subscriber
tracing_subscriber::fmt()
    .with_env_filter("alloy_transport=debug")  // or "trace" for full bodies
    .init();

// Create provider normally (logging happens automatically)
let provider = create_http_provider(
    ProviderConfig::new("https://eth.llamarpc.com")
)?;
```

### Changed

- `ProviderPool::with_defaults()` now takes only `rate_limit: Option<u32>` (removed `enable_logging` parameter)
- Provider factory functions simplified by removing logging-related match branches

## [0.4.1] - 2026-01-03

### Changed

- **WebSocket dependencies now optional**: Moved `alloy-provider`'s `pubsub` and `ws` features behind a new `ws` feature flag
  - Users who don't need WebSocket support no longer pull in `tokio-tungstenite`, `tungstenite`, `alloy-transport-ws`, and `alloy-pubsub`
  - Reduces binary size and compile times for HTTP-only use cases
  - To enable WebSocket support: `semioscan = { version = "0.4", features = ["ws"] }`

### Feature-Gated

- `create_ws_provider()` function now requires `ws` feature
- `RealtimeEventScanner` and `events::realtime` module now require `ws` feature

## [0.4.0] - 2026-01-03

### Added

#### Provider Utilities

- **Connection Pooling**: Thread-safe provider pooling for efficient connection reuse across concurrent operations
  - `ProviderPool`: Thread-safe pool using RwLock for concurrent access
  - `ProviderPoolBuilder`: Builder pattern for easy configuration
  - `ChainEndpoint`: Configuration struct with chain-specific helpers
  - `PooledProvider`: Type alias for Arc<RootProvider<AnyNetwork>>
  - Lazy provider initialization via `get_or_add()`
  - Per-chain rate limiting support
  - Compatible with `std::sync::LazyLock` for static pools

- **Dynamic Provider Utilities**: Runtime chain selection without compile-time network constraints
  - Type aliases: `AnyHttpProvider`, `EthereumHttpProvider`, `OptimismHttpProvider`
  - `NetworkType` enum with `network_type_for_chain()` for chain categorization
  - `ChainAwareProvider` wrapper for tracking chain metadata
  - Factory functions: `create_http_provider`, `create_ws_provider`, `create_typed_http_provider`
  - `ProviderConfig` with presets for public/private endpoints, Infura, Alchemy

#### Transport Layers

- **Rate Limiting Layer**: Token bucket rate limiter with configurable limits
  - `RateLimitLayer::per_second(n)` for requests-per-second limiting
  - `RateLimitLayer::with_min_delay(duration)` for fixed delays between requests
  - Async-safe with Arc<Mutex<RateLimitState>>

- **Logging Layer**: RPC call tracing with configurable verbosity
  - Automatic method extraction from RequestPacket
  - Duration tracking in tracing spans
  - Optional request/response payload logging via `with_request_logging()` and `verbose()`

- **Retry Layer**: Automatic retry of transient RPC failures with exponential backoff
  - `RetryLayer::new()` with configurable max retries, base delay, and max delay
  - Builder pattern for flexible configuration
  - Preset configurations: `aggressive()` and `conservative()`
  - Uses Alloy's `is_retry_err()` for smart error classification

#### Gas Calculation

- **EIP-4844 Blob Gas Support**: Comprehensive blob gas tracking and utilities
  - `BlobGasPrice` type with `from_gwei()` and `cost_for_blobs()` methods
  - `GasBreakdown` struct separating execution/blob/L1 costs
  - `GasBreakdownBuilder` for flexible breakdown construction
  - Enhanced `L1Gas`/`L2Gas` with `blob_count` and `blob_gas_price` fields
  - `GasCostResult` now includes `breakdown` field for analytics
  - New `blob` module with utilities:
    - `get_blob_base_fee()` - fetch from latest block
    - `get_blob_base_fee_at_block()` - fetch from specific block
    - `estimate_blob_cost()` - estimate cost for N blobs
    - `calculate_blob_gas()` - pure blob gas calculation
    - `max_blob_gas_per_block()` - returns 786,432 max
    - `estimate_total_tx_cost()` - combines execution + blob costs

#### Batch Operations

- **Batch Fetching for Transactions and Receipts**: Two-pass batch approach for fetching transactions and receipts in parallel via `futures::join_all`
- **Batch Balance Utilities**: Fetch multiple token/ETH balances efficiently
  - `batch_fetch_balances()` for ERC-20 token balances
  - `batch_fetch_eth_balances()` for native ETH balances
  - New types: `BalanceQuery`, `BalanceResult`, `BalanceError`
  - Compatible with Alloy's `CallBatchLayer` for automatic Multicall3 batching

- **Batch Token Decimals**: `batch_fetch_decimals()` for fetching multiple token decimals in parallel

#### Real-Time Events

- **WebSocket Support**: Real-time event streaming via WebSocket subscriptions
  - `RealtimeEventScanner` for WebSocket-based event subscriptions
  - `subscribe_blocks()` for real-time block headers
  - `subscribe_logs()` for real-time log events
  - `subscribe_logs_with_catchup()` for subscriptions with historical catchup
  - New `SubscriptionFailed` error variant for WebSocket errors

#### Documentation

- **Network Selection Guide** (`docs/NETWORK_SELECTION.md`): Comprehensive guide for choosing between Ethereum, Optimism, and AnyNetwork types
- **Provider Setup Examples** (`docs/PROVIDER_SETUP.md`): Practical examples covering rate limiting, retry, logging, pooling, and WebSocket providers

### Changed

- **Minimum Rust version**: Updated to 1.92 (from 1.89)
- **Dependencies**: Updated and upgraded all cargo dependencies

### Fixed

- Fixed doctest imports in blob module to use correct public paths

## [0.3.0] - 2025-11-16

### Breaking Changes

**Removed default feature coupling**

- `odos-example` is no longer included in default features
- Users must now explicitly enable `features = ["odos-example"]` if they want the Odos DEX reference implementation
- This reduces dependencies for users who only need core functionality (gas calculation, block windows, event scanning)

### Added

- **RPC Timeout Support**: Configurable timeouts for RPC requests to prevent hanging on unresponsive providers
  - Added `rpc_timeout: Duration` field to `SemioscanConfig` (default: 30 seconds)
  - Added `rpc_timeout: Option<Duration>` to `ChainConfig` for per-chain overrides
  - Added `RpcError::Timeout` variant for timeout errors
  - Added `SemioscanConfigBuilder::rpc_timeout()` method
  - Added `SemioscanConfigBuilder::chain_timeout()` method for per-chain configuration
  - Added `SemioscanConfig::get_rpc_timeout()` method

- **Documentation**: Added comprehensive open-source preparation documentation
  - `SECURITY.md`: Security policy, vulnerability reporting, and security considerations
  - `CODE_OF_CONDUCT.md`: Contributor Covenant v2.1 code of conduct
  - `ROADMAP.md`: Version milestones and development roadmap
  - `docs/STAFF_REVIEW.md`: Comprehensive staff engineer review for open-sourcing

### Changed

- **README**: Updated feature flag documentation to reflect that `odos-example` is optional, not default
- **Configuration**: All builder methods now properly preserve the new `rpc_timeout` field when updating chain overrides

### Migration Guide

**For Users Relying on Default Features**:

If you were implicitly using the Odos price source via default features, you now need to explicitly enable it:

```toml
# Before (v0.2.x) - odos-example included by default
[dependencies]
semioscan = "0.2"

# After (v0.3.0) - explicitly enable if needed
[dependencies]
semioscan = { version = "0.3", features = ["odos-example"] }

# Or if you only need core functionality
[dependencies]
semioscan = "0.3"  # No Odos dependency
```

**For Users Implementing Custom Configurations**:

Chain configuration structs now include an `rpc_timeout` field:

```rust
// Before (v0.2.x)
let chain_config = ChainConfig {
    max_block_range: Some(MaxBlockRange::new(1000)),
    rate_limit_delay: Some(Duration::from_millis(250)),
};

// After (v0.3.0)
let chain_config = ChainConfig {
    max_block_range: Some(MaxBlockRange::new(1000)),
    rate_limit_delay: Some(Duration::from_millis(250)),
    rpc_timeout: None,  // Use default or specify custom timeout
};
```

## [0.2.0] - 2025-11-15

### Breaking Changes

**Semioscan is now a library-only crate**. All application-layer functionality (binaries, CLI, API server) has been removed to make the crate more focused and reusable.

#### Removed

- **All binaries and application code** (~1,150 LOC removed):
  - CLI entry point (`src/main.rs`)
  - CLI bootstrapping (`src/bootstrap.rs`)
  - CLI commands (`src/command.rs`)
  - HTTP API server (`src/api.rs`)
- **Provider creation module** (`src/provider.rs`, 265 LOC):
  - Removed `create_ethereum_provider()` and `create_optimism_provider()` functions
  - Removed `ChainFeatures` trait
  - Provider creation is now the responsibility of application code (see Migration Guide below)
- **Feature flags**:
  - Removed `cli` feature (CLI code removed)
  - Removed `api-server` feature (API server code removed)
  - Removed `core` feature (all features are now part of core library)
- **Cloud infrastructure**:
  - Removed `infra/semioscan/` directory
  - Removed semioscan Cloud Run service from GCP deployment
- **Dependencies**:
  - Removed `clap` (CLI parsing)
  - Removed `axum` (HTTP server)
  - Removed `tower` and `tower-http` (API middleware)

#### Migration Guide

**For Applications Using Semioscan**:

If your application was using semioscan's provider creation functions, you now need to create providers yourself using [Alloy](https://github.com/alloy-rs/alloy):

```rust
// Before (v0.1.x) - provider creation in semioscan
use semioscan::{create_ethereum_provider, create_optimism_provider};
let provider = create_ethereum_provider(NamedChain::Mainnet)?;

// After (v0.2.0) - use Alloy directly
use alloy_provider::ProviderBuilder;
let rpc_url = "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY";
let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
```

If you were using semioscan as a CLI tool or API server, those features have been removed. The library now focuses exclusively on providing reusable analytics primitives. You can build your own CLI/API using the library components.

### Added

#### New Architecture

- **Trait-based price extraction system**:
  - `PriceSource` trait for implementing custom DEX price extractors
  - Object-safe design allows runtime pluggability via `Box<dyn PriceSource>`
  - `SwapData` struct as common format for swap events
  - `OdosPriceSource` as reference implementation (behind `odos-example` feature)

- **Configuration system**:
  - `SemioscanConfig` for customizing RPC behavior per chain
  - `SemioscanConfigBuilder` for fluent API configuration
  - Chain-specific overrides for block ranges and rate limiting
  - Sane defaults for common chains (Base, Sonic, Arbitrum)

- **Enhanced documentation**:
  - Comprehensive README with quick start guides
  - Detailed rustdoc API documentation for all public types
  - Uniswap V3 implementation example in trait docs
  - Module-level documentation with examples

#### New Features

- **Flexible provider injection**: All calculators now accept providers via constructor rather than creating them internally
- **Configuration support**: All calculators support optional `SemioscanConfig` for customizing RPC behavior
- **Better error types**: `PriceSourceError` with clear `DecodeError` and `InvalidSwapData` variants

### Changed

#### API Changes

- **`PriceCalculator` is now generic over `PriceSource`**:

  ```rust
  // Before (v0.1.x) - hardcoded to Odos
  let calculator = PriceCalculator::new(provider);

  // After (v0.2.0) - generic over any PriceSource implementation
  let price_source = OdosPriceSource::new(router_address);
  let calculator = PriceCalculator::with_price_source(
      provider,
      Box::new(price_source)
  );
  ```

- **Feature flags simplified**:
  - `default = ["odos-example"]` - includes Odos reference implementation
  - `odos-example` - optional Odos DEX support (requires `odos-sdk`)
  - All other functionality is always included (no feature gates for core library)

- **Gas calculation constants deprecated**:
  - `MAX_BLOCK_RANGE` constant deprecated in favor of `SemioscanConfig.max_block_range`
  - Use `config.get_max_block_range(chain)` for chain-specific limits

#### Module Organization

- **`price` module made public**:
  - `PriceSource` trait exported at `semioscan::price::PriceSource`
  - `SwapData` struct exported at `semioscan::price::SwapData`
  - `odos` submodule available with `odos-example` feature

- **Removed CLI-specific code**:
  - Removed `SupportedEvent` enum (CLI-specific)
  - Removed API handler methods from `gas.rs` and `price_calculator.rs`

### Fixed

- **Improved type safety**: Provider functions now use `NamedChain` consistently
- **Better documentation coverage**: All public types now have comprehensive rustdoc comments
- **Cleaner dependency tree**: Removed unused CLI and HTTP server dependencies

### Internal

- **Code size reduction**: ~1,415 lines of application code removed
- **Dependency cleanup**: Removed 5 dependencies (`clap`, `axum`, `tower`, `tower-http`, `http`)
- **Testing improvements**: All 16 unit tests passing, zero clippy warnings

## [0.1.0] - 2025-11-10

Initial internal release as part of Likwid workspace.

### Features

- Gas cost calculation for L1 and L2 chains
- Block window calculation for UTC dates
- Price extraction from Odos DEX events
- Transfer amount tracking for ERC-20 tokens
- Multi-chain support (12+ EVM chains)
- HTTP API server for price queries
- CLI tool for blockchain analytics

---

**Notes**:

- Version 0.1.x was used internally within the Likwid workspace
- Version 0.2.0 is the first version prepared for public open-source release
- This changelog will be maintained going forward for all public releases
