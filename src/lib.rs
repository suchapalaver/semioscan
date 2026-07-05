// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
// SPDX-FileCopyrightText: 2026 Joseph Livesey <jlivesey@gmail.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Semioscan: Blockchain analytics library for EVM chains
//!
//! Semioscan provides production-grade tools for:
//! - Gas cost calculation (L1 and L2 chains)
//! - Price extraction from DEX events
//! - Block window calculations
//! - Token event processing
//!
//! # Domain Organization
//!
//! - `types` - Strong types for type safety
//! - `config` - Configuration system
//! - `gas` - Gas calculation domain
//! - `price` - Price extraction domain
//! - `blocks` - Block window calculations
//! - `events` - Event processing
//! - `provider` - Dynamic provider utilities for runtime chain selection
//! - `transport` - Transport layer utilities (rate limiting, etc.)
//! - `cache` - Caching infrastructure (internal)
//! - `retrieval` - Data orchestration (internal)
//! - `tracing` - Observability (internal)
//!
//! # Preferred imports
//!
//! The most commonly used types are re-exported at the crate root, so
//! `use semioscan::{GasCostCalculator, PriceCalculator, BlockWindowCalculator};`
//! covers typical usage. Domain-specific items that aren't part of that facade
//! live under their public modules — for example a custom price source pulls
//! `PriceSource` and `PriceSourceError` from `semioscan::price`, and transport
//! layers come from `semioscan::transport`.
//!
//! # Feature flags
//!
//! Every domain is a Cargo feature, and the `default` feature enables all of
//! them — so a default `semioscan` dependency behaves exactly as before.
//! Downstream crates that only need part of the library can opt out and pull
//! in just the domains they use:
//!
//! ```toml
//! # Just block-window calculations, nothing else:
//! semioscan = { version = "0.14", default-features = false, features = ["blocks"] }
//! ```
//!
//! Feature dependencies follow the real module dependencies:
//!
//! - `blocks`, `events`, `transport` — standalone domains
//! - `gas` enables `events` (it decodes `Transfer`/`Approval` logs)
//! - `price` — DEX price extraction (reads on-chain token decimals)
//! - `provider` enables `transport`
//! - `retrieval` enables `events` and `gas` (combined gas/price/balance lookups)
//! - `ws` enables `provider` and adds WebSocket transport (`create_ws_provider`)
//!
//! A `--no-default-features` build with no domains selected compiles the core
//! configuration, error, and type machinery only.

// === Module Declarations ===
#[cfg(feature = "blocks")]
mod blocks;
#[cfg(any(feature = "gas", feature = "price"))]
mod cache;
pub mod config;
pub mod errors;
#[cfg(feature = "events")]
mod events;
#[cfg(feature = "gas")]
mod gas;
#[cfg(feature = "price")]
pub mod price;
#[cfg(feature = "provider")]
pub mod provider;
#[cfg(feature = "retrieval")]
mod retrieval;
#[cfg(any(
    feature = "events",
    feature = "gas",
    feature = "price",
    feature = "retrieval"
))]
mod scan;
mod tracing;
#[cfg(feature = "transport")]
pub mod transport;
mod types;

// === Core Types (from types/) ===
pub use types::config::{BlockCount, MaxBlockRange, TransactionCount};
pub use types::decimal_precision::DecimalPrecision;
pub use types::fees::{L1DataFee, Percentage};
pub use types::gas::{
    BlobCount, BlobGasAmount, BlobGasPrice, GasAmount, GasBreakdown, GasBreakdownBuilder, GasPrice,
};
pub use types::tokens::{
    NormalizedAmount, TokenAmount, TokenDecimals, TokenPrice, TokenSet, UsdValue, UsdValueError,
};
pub use types::wei::WeiAmount;

// === Configuration (from config/) ===
pub use config::constants;
pub use config::policy::{
    LookupConfig, LookupPolicy, RpcConfig, RpcPolicy, ScanConfig, ScanPolicy,
};
pub use config::{ChainConfig, SemioscanConfig, SemioscanConfigBuilder};

// === Error Types (from errors/) ===
#[cfg(feature = "blocks")]
pub use errors::BlockWindowError;
#[cfg(feature = "events")]
pub use errors::EventProcessingError;
#[cfg(feature = "gas")]
pub use errors::GasCalculationError;
#[cfg(feature = "price")]
pub use errors::PriceCalculationError;
#[cfg(feature = "retrieval")]
pub use errors::RetrievalError;
pub use errors::{RpcError, SemioscanError};

// === Gas Calculation (from gas/) ===
#[cfg(feature = "gas")]
pub use gas::adapter::{EthereumReceiptAdapter, OptimismReceiptAdapter, ReceiptAdapter};
#[cfg(feature = "gas")]
pub use gas::blob;
#[cfg(feature = "gas")]
pub use gas::cache::GasCache;
#[cfg(feature = "gas")]
pub use gas::{EventType, GasCostCalculator, GasCostResult, GasForTx};

// === Price Extraction (from price/) ===
#[cfg(feature = "price")]
pub use price::{
    PriceCalculator, PriceSource, PriceSourceError, RawSwapResult, SwapData, TokenPriceResult,
};

// === Block Windows (from blocks/) ===
#[cfg(feature = "blocks")]
pub use blocks::{
    BlockWindowCache, BlockWindowCalculator, CacheKey, CacheStats, DailyBlockWindow, DiskCache,
    MemoryCache, NoOpCache, UnixTimestamp, DEFAULT_HEAD_TTL,
};

// === Cache Types (from types/cache) ===
pub use types::cache::{AccessSequence, TimestampMillis};

// === Events (from events/) ===
#[cfg(feature = "events")]
pub use events::fetch_logs_chunked;
#[cfg(feature = "events")]
pub use events::fetch_logs_chunked_range;
#[cfg(feature = "events")]
pub use events::EventScanner;
#[cfg(feature = "events")]
pub use events::{extract_transferred_to_tokens, extract_transferred_to_tokens_with_config};
#[cfg(feature = "events")]
pub use events::{AmountCalculator, AmountResult};
#[cfg(feature = "events")]
pub use events::{Approval, Transfer};

// === Retrieval (Data Orchestration) ===
#[cfg(feature = "retrieval")]
pub use retrieval::{
    batch_fetch_balances, batch_fetch_eth_balances, get_token_decimal_precision,
    u256_to_bigdecimal, BalanceError, BalanceQuery, BalanceResult, CombinedCalculator,
    CombinedDataLookupAttempt, CombinedDataLookupFailure, CombinedDataLookupPass,
    CombinedDataLookupStage, CombinedDataResult, CombinedDataRetrievalMetadata, GasAndAmountForTx,
};

// === Transport Layers ===
#[cfg(feature = "transport")]
pub use transport::{
    RateLimitLayer, RateLimitService, RetryConfig, RetryLayer, RetryLayerBuilder, RetryService,
};

// === Provider Utilities ===
#[cfg(feature = "ws")]
pub use provider::create_ws_provider;
#[cfg(feature = "provider")]
pub use provider::{
    create_http_provider, create_typed_http_provider, network_type_for_chain,
    rate_limited_http_provider, simple_http_provider, AnyHttpProvider, ChainAwareProvider,
    ChainEndpoint, DynProviderBuilder, EthereumHttpProvider, NetworkType, OptimismHttpProvider,
    PooledProvider, ProviderConfig, ProviderPool, ProviderPoolBuilder, SharedProvider,
};

// Note: Cache internals (cache::BlockRangeCache) and tracing spans are NOT re-exported
// as they are implementation details. Users can access them via fully-qualified paths if needed.
