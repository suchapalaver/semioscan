// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Provider connection pooling for high-throughput scenarios
//!
//! This module provides thread-safe provider pooling that allows reusing
//! provider connections across multiple concurrent operations.
//!
//! # Overview
//!
//! The [`ProviderPool`] maintains a collection of providers indexed by chain,
//! enabling efficient connection reuse without creating new providers for each
//! operation. This is particularly useful for:
//!
//! - Multi-chain applications that query many chains concurrently
//! - High-throughput indexing or analytics workloads
//! - Long-running services that process blocks continuously
//!
//! # Examples
//!
//! ## Static Pool Initialization
//!
//! For applications that know their chains at startup, use a static pool:
//!
//! ```rust,ignore
//! use semioscan::provider::{ProviderPool, ProviderPoolBuilder};
//! use alloy_chains::NamedChain;
//! use std::sync::LazyLock;
//!
//! // Static pool initialized once on first access
//! static PROVIDERS: LazyLock<ProviderPool> = LazyLock::new(|| {
//!     ProviderPoolBuilder::new()
//!         .add_chain(NamedChain::Mainnet, "https://eth.llamarpc.com")
//!         .add_chain(NamedChain::Base, "https://mainnet.base.org")
//!         .with_rate_limit(10)
//!         .build()
//!         .expect("Failed to create provider pool")
//! });
//!
//! async fn get_block_number(chain: NamedChain) -> u64 {
//!     let provider = PROVIDERS.get(chain).expect("Chain not configured");
//!     provider.get_block_number().await.unwrap()
//! }
//! ```
//!
//! ## Dynamic Pool with Lazy Loading
//!
//! For applications that discover chains at runtime:
//!
//! ```rust,ignore
//! use semioscan::provider::ProviderPool;
//! use alloy_chains::NamedChain;
//!
//! let mut pool = ProviderPool::new();
//!
//! // Add providers as needed
//! pool.add(NamedChain::Mainnet, "https://eth.llamarpc.com", None)?;
//! pool.add(NamedChain::Base, "https://mainnet.base.org", Some(10))?;
//!
//! // Access providers
//! if let Some(provider) = pool.get(NamedChain::Mainnet) {
//!     let block = provider.get_block_number().await?;
//! }
//! ```
//!
//! ## With Preset Configurations
//!
//! ```rust,ignore
//! use semioscan::provider::{ProviderPool, ChainEndpoint};
//!
//! let endpoints = vec![
//!     ChainEndpoint::mainnet("https://eth.llamarpc.com"),
//!     ChainEndpoint::base("https://mainnet.base.org"),
//!     ChainEndpoint::optimism("https://mainnet.optimism.io"),
//! ];
//!
//! let pool = ProviderPool::from_endpoints(endpoints, Some(10))?;
//! ```

use alloy_chains::NamedChain;
use alloy_network::AnyNetwork;
use alloy_provider::RootProvider;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::config::policy::RpcPolicy;
use crate::errors::RpcError;

use super::config::ProviderConfig;
use super::factory::build_http_client;

/// Type alias for a pooled provider using `AnyNetwork`
pub type PooledProvider = Arc<RootProvider<AnyNetwork>>;

/// A thread-safe pool of providers indexed by chain
///
/// The pool uses read-write locks for efficient concurrent access:
/// - Multiple readers can access providers simultaneously
/// - Writers acquire exclusive access when adding new providers
///
/// # Thread Safety
///
/// The pool is safe to share across threads via `Arc<ProviderPool>` or
/// by storing it in a static variable with `LazyLock`.
#[derive(Debug, Default)]
pub struct ProviderPool {
    /// Map of chain to provider
    providers: RwLock<HashMap<NamedChain, PooledProvider>>,
    /// Default rate limit for new providers (requests per second)
    default_rate_limit: Option<u32>,
    /// Default per-request timeout for new providers.
    /// When neither this nor the per-endpoint `timeout` is set, the
    /// underlying transport's default timeout applies.
    default_timeout: Option<Duration>,
}

impl ProviderPool {
    /// Create a new empty provider pool
    #[must_use]
    pub fn new() -> Self {
        Self {
            providers: RwLock::new(HashMap::new()),
            default_rate_limit: None,
            default_timeout: None,
        }
    }

    /// Create a pool with default rate limit for new providers
    #[must_use]
    pub fn with_defaults(rate_limit: Option<u32>) -> Self {
        Self {
            providers: RwLock::new(HashMap::new()),
            default_rate_limit: rate_limit,
            default_timeout: None,
        }
    }

    /// Set the default per-request timeout used when an endpoint does not
    /// override its own.
    #[must_use]
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = Some(timeout);
        self
    }

    /// Create a pool from a list of chain endpoints
    ///
    /// # Errors
    ///
    /// Returns an error if any endpoint URL is invalid
    pub fn from_endpoints(
        endpoints: Vec<ChainEndpoint>,
        rate_limit: Option<u32>,
    ) -> Result<Self, RpcError> {
        let pool = Self::with_defaults(rate_limit);
        for endpoint in endpoints {
            pool.add_endpoint(&endpoint)?;
        }
        Ok(pool)
    }

    /// Add a provider for a specific chain
    ///
    /// If a provider already exists for this chain, it will be replaced.
    ///
    /// # Arguments
    ///
    /// * `chain` - The chain to add the provider for
    /// * `url` - The RPC endpoint URL
    /// * `rate_limit` - Optional rate limit in requests per second
    ///
    /// The per-request timeout is taken from the pool's default
    /// (see [`with_default_timeout`](Self::with_default_timeout)).
    /// Use [`add_endpoint`](Self::add_endpoint) to set a per-chain timeout
    /// or minimum-delay pacing.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid
    pub fn add(
        &self,
        chain: NamedChain,
        url: &str,
        rate_limit: Option<u32>,
    ) -> Result<(), RpcError> {
        self.add_inner(chain, url, rate_limit, None, None)
    }

    /// Add a chain endpoint to the pool.
    ///
    /// Uses the endpoint's `rate_limit`, `timeout`, and `min_delay` when
    /// set, otherwise falls back to the pool defaults.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL is invalid.
    pub fn add_endpoint(&self, endpoint: &ChainEndpoint) -> Result<(), RpcError> {
        self.add_inner(
            endpoint.chain,
            &endpoint.url,
            endpoint.rate_limit,
            endpoint.timeout,
            endpoint.min_delay,
        )
    }

    fn add_inner(
        &self,
        chain: NamedChain,
        url: &str,
        rate_limit: Option<u32>,
        timeout: Option<Duration>,
        min_delay: Option<Duration>,
    ) -> Result<(), RpcError> {
        let provider = create_pooled_provider(
            url,
            rate_limit.or(self.default_rate_limit),
            timeout.or(self.default_timeout),
            min_delay,
        )?;

        let mut providers = self.providers.write().map_err(|_| {
            RpcError::ProviderConnectionFailed("Provider pool lock poisoned".to_string())
        })?;

        if providers.contains_key(&chain) {
            debug!(chain = ?chain, "Replacing existing provider");
        } else {
            info!(chain = ?chain, url = url, "Added provider to pool");
        }

        providers.insert(chain, Arc::new(provider));
        Ok(())
    }

    /// Get a provider for a specific chain
    ///
    /// Returns `None` if no provider is configured for the chain.
    #[must_use]
    pub fn get(&self, chain: NamedChain) -> Option<PooledProvider> {
        self.providers
            .read()
            .ok()
            .and_then(|providers| providers.get(&chain).cloned())
    }

    /// Get a provider for a chain, or add it if not present
    ///
    /// This is useful for lazy initialization of providers.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid and the provider needs to be created
    pub fn get_or_add(
        &self,
        chain: NamedChain,
        url: &str,
        rate_limit: Option<u32>,
    ) -> Result<PooledProvider, RpcError> {
        // Try read lock first for better concurrency
        if let Some(provider) = self.get(chain) {
            return Ok(provider);
        }

        // Provider not found, need to add it
        self.add(chain, url, rate_limit)?;
        self.get(chain).ok_or_else(|| {
            RpcError::ProviderConnectionFailed("Failed to retrieve newly added provider".into())
        })
    }

    /// Remove a provider from the pool
    ///
    /// Returns the removed provider if it existed.
    pub fn remove(&self, chain: NamedChain) -> Option<PooledProvider> {
        self.providers
            .write()
            .ok()
            .and_then(|mut providers| providers.remove(&chain))
    }

    /// Check if a provider exists for a chain
    #[must_use]
    pub fn contains(&self, chain: NamedChain) -> bool {
        self.providers
            .read()
            .ok()
            .is_some_and(|providers| providers.contains_key(&chain))
    }

    /// Get the number of providers in the pool
    #[must_use]
    pub fn len(&self) -> usize {
        self.providers
            .read()
            .map(|providers| providers.len())
            .unwrap_or(0)
    }

    /// Check if the pool is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get all configured chains
    #[must_use]
    pub fn chains(&self) -> Vec<NamedChain> {
        self.providers
            .read()
            .map(|providers| providers.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Clear all providers from the pool
    pub fn clear(&self) {
        if let Ok(mut providers) = self.providers.write() {
            providers.clear();
            info!("Cleared all providers from pool");
        }
    }
}

/// Builder for creating provider pools with common configurations
#[derive(Default)]
pub struct ProviderPoolBuilder {
    endpoints: Vec<ChainEndpoint>,
    default_rate_limit: Option<u32>,
    default_timeout: Option<Duration>,
    rpc_policy_timeouts: HashMap<NamedChain, Duration>,
    rpc_policy_min_delays: HashMap<NamedChain, Duration>,
}

impl ProviderPoolBuilder {
    /// Create a new builder
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a chain endpoint to the pool
    #[must_use]
    pub fn add_chain(mut self, chain: NamedChain, url: &str) -> Self {
        self.endpoints.push(ChainEndpoint::new(chain, url));
        self
    }

    /// Add a chain endpoint with a specific rate limit
    #[must_use]
    pub fn add_chain_with_rate_limit(
        mut self,
        chain: NamedChain,
        url: &str,
        rate_limit: u32,
    ) -> Self {
        self.endpoints
            .push(ChainEndpoint::new(chain, url).with_rate_limit(rate_limit));
        self
    }

    /// Add a chain endpoint with a specific per-request timeout
    #[must_use]
    pub fn add_chain_with_timeout(
        mut self,
        chain: NamedChain,
        url: &str,
        timeout: Duration,
    ) -> Self {
        self.endpoints
            .push(ChainEndpoint::new(chain, url).with_timeout(timeout));
        self
    }

    /// Add a fully-specified [`ChainEndpoint`] to the pool
    #[must_use]
    pub fn add_endpoint(mut self, endpoint: ChainEndpoint) -> Self {
        self.endpoints.push(endpoint);
        self
    }

    /// Set the default requests-per-second budget applied to every endpoint
    /// in the pool that does not carry its own [`ChainEndpoint::rate_limit`].
    ///
    /// **Interaction with [`with_rpc_policy`](Self::with_rpc_policy).** The
    /// per-second budget and a policy-supplied `rate_limit_delay` are not
    /// stacked on the wire — each endpoint ends up with at most one
    /// rate-limit layer, and the per-second axis wins. A chain that has
    /// both a builder-level (or endpoint-level) requests-per-second and a
    /// policy `rate_limit_delay` is paced by requests-per-second only; the
    /// policy's per-chain minimum gap is dropped at pool construction and
    /// a `tracing::warn!` fires for the affected endpoint. If the per-chain
    /// minimum gap is the one that matters, set the per-second budget only
    /// on the endpoints that need it via
    /// [`ChainEndpoint::with_rate_limit`] instead of this pool-wide default,
    /// and the policy delay will apply to the remaining chains.
    #[must_use]
    pub fn with_rate_limit(mut self, requests_per_second: u32) -> Self {
        self.default_rate_limit = Some(requests_per_second);
        self
    }

    /// Set the default per-request timeout for all providers
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = Some(timeout);
        self
    }

    /// Apply per-chain settings from an [`RpcPolicy`].
    ///
    /// The policy is evaluated for every endpoint that has already been
    /// added to the builder; call this **after** all `add_chain*` /
    /// `add_endpoint` calls for the policy to take effect. Endpoints added
    /// after this call do **not** pick up the policy values — call
    /// `with_rpc_policy` again to refresh.
    ///
    /// For each axis the policy supplies (`rpc_timeout`, `rate_limit_delay`),
    /// a per-endpoint value always wins; the builder's `default_timeout`
    /// is the fallback when neither the endpoint nor the policy supplies
    /// a timeout for a chain. There is no pool-level default for the
    /// minimum-delay pacing axis, so a chain with no endpoint `min_delay`
    /// and no policy `rate_limit_delay` simply runs unpaced.
    ///
    /// **Interaction with [`with_rate_limit`](Self::with_rate_limit) and
    /// [`ChainEndpoint::with_rate_limit`].** A policy's `rate_limit_delay`
    /// and a requests-per-second budget on the same chain are not stacked
    /// on the wire: each endpoint installs at most one rate-limit layer
    /// and the per-second axis wins. If `with_rate_limit(rps)` or an
    /// endpoint-level `rate_limit` is set on a chain that the policy also
    /// supplies a `rate_limit_delay` for, the policy's per-chain minimum
    /// gap is dropped at construction and a `tracing::warn!` fires for the
    /// affected endpoint. To keep both knobs effective across the pool,
    /// move the per-second budget off this pool-wide default and onto the
    /// chains that need it via [`ChainEndpoint::with_rate_limit`], leaving
    /// the policy delay in place for the rest.
    #[must_use]
    pub fn with_rpc_policy<P: RpcPolicy>(mut self, policy: &P) -> Self {
        for endpoint in &self.endpoints {
            let cfg = policy.rpc_config(endpoint.chain);
            self.rpc_policy_timeouts
                .insert(endpoint.chain, cfg.rpc_timeout);
            if let Some(delay) = cfg.rate_limit_delay {
                self.rpc_policy_min_delays.insert(endpoint.chain, delay);
            }
        }
        self
    }

    /// Build the provider pool
    ///
    /// # Errors
    ///
    /// Returns an error if any endpoint URL is invalid
    pub fn build(self) -> Result<ProviderPool, RpcError> {
        let pool = ProviderPool::with_defaults(self.default_rate_limit);
        let pool = match self.default_timeout {
            Some(t) => pool.with_default_timeout(t),
            None => pool,
        };

        for endpoint in &self.endpoints {
            let policy_timeout = self.rpc_policy_timeouts.get(&endpoint.chain).copied();
            let policy_min_delay = self.rpc_policy_min_delays.get(&endpoint.chain).copied();
            let effective = ChainEndpoint {
                chain: endpoint.chain,
                url: endpoint.url.clone(),
                rate_limit: endpoint.rate_limit.or(self.default_rate_limit),
                timeout: endpoint.timeout.or(policy_timeout),
                min_delay: endpoint.min_delay.or(policy_min_delay),
            };
            pool.add_endpoint(&effective)?;
        }

        Ok(pool)
    }
}

/// Configuration for a chain endpoint.
///
/// Construct via [`ChainEndpoint::new`] (or one of the preset constructors
/// such as [`ChainEndpoint::mainnet`]) and the `with_*` setters. The struct
/// is `#[non_exhaustive]` so additional optional fields can be added in
/// future releases without forcing struct-literal callers to update.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ChainEndpoint {
    /// The chain this endpoint serves
    pub chain: NamedChain,
    /// The RPC endpoint URL
    pub url: String,
    /// Optional rate limit override for this specific chain
    pub rate_limit: Option<u32>,
    /// Optional per-request timeout override for this specific chain.
    /// When `None`, the pool's default timeout (if any) is used.
    pub timeout: Option<Duration>,
    /// Optional minimum spacing between requests for this specific chain.
    /// When set, the underlying transport installs a minimum-delay layer
    /// that guarantees at least this gap between consecutive RPC calls.
    /// When `None`, the policy's per-chain `rate_limit_delay` (if any)
    /// supplies the value at build time.
    pub min_delay: Option<Duration>,
}

impl ChainEndpoint {
    /// Create a new chain endpoint
    #[must_use]
    pub fn new(chain: NamedChain, url: impl Into<String>) -> Self {
        Self {
            chain,
            url: url.into(),
            rate_limit: None,
            timeout: None,
            min_delay: None,
        }
    }

    /// Create a new chain endpoint with rate limiting
    #[must_use]
    pub fn with_rate_limit(mut self, rate_limit: u32) -> Self {
        self.rate_limit = Some(rate_limit);
        self
    }

    /// Override the per-request timeout for this chain
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Override the minimum spacing between requests for this chain.
    ///
    /// Mirrors [`ProviderConfig::with_min_delay`](crate::provider::ProviderConfig::with_min_delay):
    /// the underlying transport installs a layer that guarantees at least
    /// `delay` between consecutive RPC calls.
    ///
    /// # Panics
    ///
    /// Panics if `delay` is [`Duration::ZERO`]. See
    /// [`ProviderConfig::with_min_delay`](crate::provider::ProviderConfig::with_min_delay)
    /// for the reasoning and how to express "no pacing" for an endpoint
    /// instead.
    #[must_use]
    #[track_caller]
    pub fn with_min_delay(mut self, delay: Duration) -> Self {
        super::assert_nonzero_min_delay(delay);
        self.min_delay = Some(delay);
        self
    }

    /// Create an Ethereum mainnet endpoint
    #[must_use]
    pub fn mainnet(url: impl Into<String>) -> Self {
        Self::new(NamedChain::Mainnet, url)
    }

    /// Create a Base mainnet endpoint
    #[must_use]
    pub fn base(url: impl Into<String>) -> Self {
        Self::new(NamedChain::Base, url)
    }

    /// Create an Optimism mainnet endpoint
    #[must_use]
    pub fn optimism(url: impl Into<String>) -> Self {
        Self::new(NamedChain::Optimism, url)
    }

    /// Create an Arbitrum One endpoint
    #[must_use]
    pub fn arbitrum(url: impl Into<String>) -> Self {
        Self::new(NamedChain::Arbitrum, url)
    }

    /// Create a Polygon mainnet endpoint
    #[must_use]
    pub fn polygon(url: impl Into<String>) -> Self {
        Self::new(NamedChain::Polygon, url)
    }

    /// Create a Sepolia testnet endpoint
    #[must_use]
    pub fn sepolia(url: impl Into<String>) -> Self {
        Self::new(NamedChain::Sepolia, url)
    }
}

/// Create a pooled provider with optional rate limiting and pacing.
///
/// Routes through the shared HTTP client builder so the
/// `(rate_limit_per_second, min_delay, timeout)` dispatch matrix matches
/// every other HTTP provider this crate hands out. Returns a bare
/// `RootProvider` without fillers, as fillers are application-specific
/// and should be added by the consumer if needed.
///
/// Note: RPC request/response logging is handled natively by alloy's transport
/// layer at DEBUG/TRACE level.
fn create_pooled_provider(
    url: &str,
    rate_limit: Option<u32>,
    timeout: Option<Duration>,
    min_delay: Option<Duration>,
) -> Result<RootProvider<AnyNetwork>, RpcError> {
    let mut config = ProviderConfig::new(url).with_rate_limit_opt(rate_limit);
    if let Some(t) = timeout {
        config = config.with_timeout(t);
    }
    if let Some(d) = min_delay {
        config = config.with_min_delay(d);
    }

    let client = build_http_client(config).inspect_err(|e| {
        warn!(url = url, error = ?e, "Invalid provider URL");
    })?;

    Ok(RootProvider::<AnyNetwork>::new(client))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_new() {
        let pool = ProviderPool::new();
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn test_pool_with_defaults() {
        let pool = ProviderPool::with_defaults(Some(10));
        assert_eq!(pool.default_rate_limit, Some(10));
    }

    #[test]
    fn test_chain_endpoint_constructors() {
        let endpoint = ChainEndpoint::mainnet("https://eth.llamarpc.com");
        assert_eq!(endpoint.chain, NamedChain::Mainnet);
        assert_eq!(endpoint.url, "https://eth.llamarpc.com");
        assert!(endpoint.rate_limit.is_none());

        let endpoint = ChainEndpoint::base("https://mainnet.base.org").with_rate_limit(5);
        assert_eq!(endpoint.chain, NamedChain::Base);
        assert_eq!(endpoint.rate_limit, Some(5));
    }

    #[test]
    fn test_pool_builder() {
        let builder = ProviderPoolBuilder::new()
            .add_chain(NamedChain::Mainnet, "https://eth.llamarpc.com")
            .add_chain_with_rate_limit(NamedChain::Base, "https://mainnet.base.org", 5)
            .with_rate_limit(10);

        assert_eq!(builder.endpoints.len(), 2);
        assert_eq!(builder.default_rate_limit, Some(10));
    }

    #[test]
    fn test_pool_contains_and_chains() {
        let pool = ProviderPool::new();

        // Add a provider (using a valid URL format)
        let result = pool.add(NamedChain::Mainnet, "https://eth.llamarpc.com", None);
        assert!(result.is_ok());

        assert!(pool.contains(NamedChain::Mainnet));
        assert!(!pool.contains(NamedChain::Base));

        let chains = pool.chains();
        assert_eq!(chains.len(), 1);
        assert!(chains.contains(&NamedChain::Mainnet));
    }

    #[test]
    fn test_pool_remove() {
        let pool = ProviderPool::new();
        pool.add(NamedChain::Mainnet, "https://eth.llamarpc.com", None)
            .unwrap();

        assert!(pool.contains(NamedChain::Mainnet));

        let removed = pool.remove(NamedChain::Mainnet);
        assert!(removed.is_some());
        assert!(!pool.contains(NamedChain::Mainnet));

        // Removing again should return None
        let removed_again = pool.remove(NamedChain::Mainnet);
        assert!(removed_again.is_none());
    }

    #[test]
    fn test_pool_clear() {
        let pool = ProviderPool::new();
        pool.add(NamedChain::Mainnet, "https://eth.llamarpc.com", None)
            .unwrap();
        pool.add(NamedChain::Base, "https://mainnet.base.org", None)
            .unwrap();

        assert_eq!(pool.len(), 2);

        pool.clear();
        assert!(pool.is_empty());
    }

    #[test]
    fn test_invalid_url() {
        let pool = ProviderPool::new();
        let result = pool.add(NamedChain::Mainnet, "not a valid url", None);
        assert!(result.is_err());
    }

    /// Zero-duration `min_delay` is rejected at the endpoint builder call
    /// site. Pins the contract documented on
    /// [`ChainEndpoint::with_min_delay`]'s `# Panics` section: the
    /// token-bucket layer it ultimately feeds cannot represent a zero
    /// period, so the setter fails fast at the operator's call line
    /// rather than letting `Some(Duration::ZERO)` propagate to a later
    /// pool-build panic with a useless backtrace.
    #[test]
    #[should_panic(expected = "min_delay must be > 0")]
    fn test_chain_endpoint_with_min_delay_rejects_zero() {
        let _ = ChainEndpoint::new(NamedChain::Mainnet, "https://eth.llamarpc.com")
            .with_min_delay(Duration::ZERO);
    }

    #[test]
    fn with_rpc_policy_only_covers_chains_added_before_it() {
        use crate::config::policy::RpcConfig;

        struct FixedPolicy {
            timeout: Duration,
            rate_limit_delay: Option<Duration>,
        }
        impl RpcPolicy for FixedPolicy {
            fn rpc_config(&self, _: NamedChain) -> RpcConfig {
                RpcConfig {
                    rpc_timeout: self.timeout,
                    rate_limit_delay: self.rate_limit_delay,
                }
            }
        }

        let policy = FixedPolicy {
            timeout: Duration::from_secs(5),
            rate_limit_delay: Some(Duration::from_millis(250)),
        };

        let before = ProviderPoolBuilder::new()
            .add_chain(NamedChain::Mainnet, "http://localhost:8545")
            .with_rpc_policy(&policy);
        assert_eq!(
            before
                .rpc_policy_timeouts
                .get(&NamedChain::Mainnet)
                .copied(),
            Some(Duration::from_secs(5)),
            "policy timeout must apply when chain is added before with_rpc_policy",
        );
        assert_eq!(
            before
                .rpc_policy_min_delays
                .get(&NamedChain::Mainnet)
                .copied(),
            Some(Duration::from_millis(250)),
            "policy rate_limit_delay must apply when chain is added before with_rpc_policy",
        );

        let after = ProviderPoolBuilder::new()
            .with_rpc_policy(&policy)
            .add_chain(NamedChain::Mainnet, "http://localhost:8545");
        assert!(
            !after.rpc_policy_timeouts.contains_key(&NamedChain::Mainnet),
            "policy timeout must not apply when chain is added after with_rpc_policy",
        );
        assert!(
            !after
                .rpc_policy_min_delays
                .contains_key(&NamedChain::Mainnet),
            "policy rate_limit_delay must not apply when chain is added after with_rpc_policy",
        );
    }
}
