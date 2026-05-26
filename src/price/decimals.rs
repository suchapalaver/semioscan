// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Token-decimals state and the I/O helper that populates it.
//!
//! The two pieces live in one module but stay orthogonal:
//!
//! - [`TokenDecimalsCache`] is a provider-free key/value store that can be
//!   exercised in unit tests without any RPC plumbing.
//! - [`TokenMetadataProvider`] borrows an Alloy [`Provider`] and reads ERC-20
//!   `decimals()` calls into the cache, single-shot or in parallel batches.
//!
//! Splitting the storage from the fetcher means callers can pre-seed the
//! cache (e.g. with a known USDC value) and replace the fetcher in tests
//! that don't want to talk to an RPC.

use alloy_erc20::LazyToken;
use alloy_primitives::Address;
use alloy_provider::Provider;
use futures::future::join_all;
use std::collections::HashMap;
use tracing::{info, warn};

use crate::errors::PriceCalculationError;
use crate::TokenDecimals;

/// In-memory cache of ERC-20 token decimals.
///
/// The cache is intentionally a thin map: it knows nothing about fetching
/// or eviction. Combine it with [`TokenMetadataProvider`] to populate it,
/// or seed it directly in tests.
#[derive(Debug, Default, Clone)]
pub(crate) struct TokenDecimalsCache {
    inner: HashMap<Address, TokenDecimals>,
}

impl TokenDecimalsCache {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the cached decimals for `addr`, if any.
    pub fn get(&self, addr: &Address) -> Option<TokenDecimals> {
        self.inner.get(addr).copied()
    }

    /// Insert or overwrite the cached decimals for `addr`.
    pub fn insert(&mut self, addr: Address, decimals: TokenDecimals) {
        self.inner.insert(addr, decimals);
    }

    /// `true` if `addr` is already cached.
    pub fn contains(&self, addr: &Address) -> bool {
        self.inner.contains_key(addr)
    }
}

/// Fetches ERC-20 token decimals via an Alloy provider into a
/// [`TokenDecimalsCache`].
///
/// When the underlying provider is built with `CallBatchLayer`, concurrent
/// calls issued by [`ensure_decimals`](Self::ensure_decimals) collapse into a
/// single Multicall3 RPC, so batching many tokens is cheap.
pub(crate) struct TokenMetadataProvider<'a, P> {
    provider: &'a P,
}

impl<'a, P: Provider + Clone> TokenMetadataProvider<'a, P> {
    /// Wrap `provider` as a fetcher; no RPC calls are issued at construction.
    pub fn new(provider: &'a P) -> Self {
        Self { provider }
    }

    /// Return the cached decimals for `addr`, fetching them if absent.
    ///
    /// On success the cache is updated. A fetch failure is surfaced as
    /// [`PriceCalculationError`] and the cache is left untouched, so a
    /// later call can retry.
    pub async fn get_or_fetch(
        &self,
        cache: &mut TokenDecimalsCache,
        addr: Address,
    ) -> Result<TokenDecimals, PriceCalculationError> {
        if let Some(d) = cache.get(&addr) {
            return Ok(d);
        }

        let token = LazyToken::new(addr, self.provider.clone());
        let raw = token
            .decimals()
            .await
            .map_err(|e| PriceCalculationError::metadata_fetch_failed(addr, e))?;
        let decimals = TokenDecimals::new(*raw);
        cache.insert(addr, decimals);
        Ok(decimals)
    }

    /// Fetch decimals for every uncached address in parallel.
    ///
    /// Failures are traced at `warn` level and the address is left out of
    /// the cache so a follow-up [`get_or_fetch`](Self::get_or_fetch) can
    /// retry on demand. The method always returns; it never propagates a
    /// per-token failure.
    pub async fn ensure_decimals(&self, cache: &mut TokenDecimalsCache, addresses: &[Address]) {
        let uncached: Vec<Address> = addresses
            .iter()
            .copied()
            .filter(|addr| !cache.contains(addr))
            .collect();

        if uncached.is_empty() {
            return;
        }

        info!(
            count = uncached.len(),
            "Batch fetching token decimals for uncached tokens"
        );

        let futures = uncached.iter().map(|&addr| {
            let provider = self.provider.clone();
            async move {
                let token = LazyToken::new(addr, provider);
                let result = token.decimals().await.copied();
                (addr, result)
            }
        });

        let results = join_all(futures).await;

        for (addr, result) in results {
            match result {
                Ok(raw) => {
                    cache.insert(addr, TokenDecimals::new(raw));
                }
                Err(e) => {
                    warn!(
                        token = ?addr,
                        error = ?e,
                        "Failed to fetch decimals for token, will retry on demand"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    #[test]
    fn cache_starts_empty() {
        let cache = TokenDecimalsCache::new();
        let addr = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert!(!cache.contains(&addr));
        assert_eq!(cache.get(&addr), None);
    }

    #[test]
    fn insert_then_get_returns_value() {
        let mut cache = TokenDecimalsCache::new();
        let addr = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        cache.insert(addr, TokenDecimals::new(18));

        assert!(cache.contains(&addr));
        assert_eq!(cache.get(&addr), Some(TokenDecimals::new(18)));
    }

    #[test]
    fn insert_overwrites_previous_value() {
        let mut cache = TokenDecimalsCache::new();
        let addr = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        cache.insert(addr, TokenDecimals::new(6));
        cache.insert(addr, TokenDecimals::new(18));

        assert_eq!(cache.get(&addr), Some(TokenDecimals::new(18)));
    }

    #[test]
    fn entries_are_token_scoped() {
        let mut cache = TokenDecimalsCache::new();
        let a = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let b = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        cache.insert(a, TokenDecimals::new(18));
        cache.insert(b, TokenDecimals::new(6));

        assert_eq!(cache.get(&a), Some(TokenDecimals::new(18)));
        assert_eq!(cache.get(&b), Some(TokenDecimals::new(6)));
    }
}
