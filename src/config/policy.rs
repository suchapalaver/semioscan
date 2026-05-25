// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Concern-specific configuration views.
//!
//! [`SemioscanConfig`](crate::SemioscanConfig) is the user-facing aggregate
//! that controls every configurable RPC behavior in this crate. Internally,
//! however, no single module needs every field. This module declares the
//! narrower views each domain actually depends on, plus the traits that
//! resolve them per chain:
//!
//! - [`ScanConfig`] / [`ScanPolicy`] — what the chunked log scanner reads:
//!   chunk size and inter-chunk rate-limit delay.
//! - [`LookupConfig`] / [`LookupPolicy`] — what serial transaction/receipt
//!   retries read: the maximum number of fallback attempts per failed
//!   lookup.
//! - [`RpcConfig`] / [`RpcPolicy`] — what provider construction reads:
//!   the per-request timeout to apply to the underlying HTTP transport.
//!
//! [`SemioscanConfig`](crate::SemioscanConfig) implements all three traits, so
//! existing call sites that pass it through unchanged keep working. Internal
//! consumers (`LogScanner`, `CombinedCalculator`, `GasCostCalculator`,
//! `ProviderPoolBuilder`) reach into the narrower view rather than the full
//! aggregate, making each module's actual configuration dependency visible at
//! the call site.

use std::time::Duration;

use alloy_chains::NamedChain;

use crate::types::config::MaxBlockRange;

/// Per-chain settings consumed by the chunked log scanner.
///
/// The scanner needs only the chunk size and the optional inter-chunk
/// rate-limit delay. Other policy axes (RPC timeout, lookup retries,
/// caching) are deliberately absent from this view so the scanner cannot
/// accidentally couple itself to them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanConfig {
    /// Maximum number of blocks per `eth_getLogs` call.
    pub max_block_range: MaxBlockRange,
    /// Delay applied between chunks, when set.
    pub rate_limit_delay: Option<Duration>,
}

/// Per-chain settings consumed by serial transaction/receipt fallback
/// lookups in `CombinedCalculator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LookupConfig {
    /// Maximum number of serial retry attempts per failed batch lookup.
    /// `0` disables the serial fallback pass entirely.
    pub serial_lookup_fallback_attempts: usize,
}

/// Per-chain settings consumed by provider construction.
///
/// Provider factories (`create_http_provider`, `ProviderPoolBuilder`) apply
/// `rpc_timeout` to the underlying HTTP transport so a single hung request
/// cannot block a calling task indefinitely. When `rate_limit_delay` is set,
/// the same factories install a minimum-delay layer so requests for that
/// chain are paced at least that far apart.
///
/// `#[non_exhaustive]` so future per-chain RPC settings can be added without
/// breaking struct-literal construction in downstream policy implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RpcConfig {
    /// Per-request timeout for RPC calls.
    pub rpc_timeout: Duration,
    /// Minimum spacing between RPC calls, when the policy wants the provider
    /// to throttle requests for this chain. `None` leaves the provider
    /// unthrottled (a per-endpoint `min_delay` on the pool can still install
    /// a layer).
    pub rate_limit_delay: Option<Duration>,
}

impl RpcConfig {
    /// Construct an `RpcConfig` with the given timeout and no rate-limit
    /// delay. Use the `with_*` setters to populate additional fields.
    #[must_use]
    pub fn new(rpc_timeout: Duration) -> Self {
        Self {
            rpc_timeout,
            rate_limit_delay: None,
        }
    }

    /// Set the minimum spacing between RPC calls for this chain.
    #[must_use]
    pub fn with_rate_limit_delay(mut self, delay: Duration) -> Self {
        self.rate_limit_delay = Some(delay);
        self
    }
}

/// Resolves a [`ScanConfig`] for a given chain.
///
/// Implemented by [`SemioscanConfig`](crate::SemioscanConfig); custom
/// implementations let callers inject narrower policy objects without
/// depending on the full config surface.
pub trait ScanPolicy {
    /// Effective scan settings for `chain`.
    fn scan_config(&self, chain: NamedChain) -> ScanConfig;
}

/// Resolves a [`LookupConfig`] for a given chain.
///
/// Implemented by [`SemioscanConfig`](crate::SemioscanConfig).
pub trait LookupPolicy {
    /// Effective lookup settings for `chain`.
    fn lookup_config(&self, chain: NamedChain) -> LookupConfig;
}

/// Resolves an [`RpcConfig`] for a given chain.
///
/// Implemented by [`SemioscanConfig`](crate::SemioscanConfig); custom
/// implementations let callers feed the provider construction path their own
/// timeout policy without depending on the full config surface.
pub trait RpcPolicy {
    /// Effective RPC settings for `chain`.
    fn rpc_config(&self, chain: NamedChain) -> RpcConfig;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SemioscanConfig, SemioscanConfigBuilder};

    #[test]
    fn semioscan_config_scan_view_matches_chain_lookups() {
        let config = SemioscanConfigBuilder::with_defaults()
            .chain_max_blocks(NamedChain::Arbitrum, 1234)
            .chain_rate_limit(NamedChain::Arbitrum, Duration::from_millis(77))
            .build();

        let scan = config.scan_config(NamedChain::Arbitrum);
        assert_eq!(scan.max_block_range, MaxBlockRange::new(1234));
        assert_eq!(scan.rate_limit_delay, Some(Duration::from_millis(77)));
    }

    #[test]
    fn semioscan_config_scan_view_falls_back_to_global_defaults() {
        let config = SemioscanConfig::default();

        let scan = config.scan_config(NamedChain::Optimism);
        assert_eq!(scan.max_block_range, MaxBlockRange::new(500));
        assert_eq!(scan.rate_limit_delay, None);

        let base = config.scan_config(NamedChain::Base);
        assert_eq!(base.rate_limit_delay, Some(Duration::from_millis(250)));
    }

    #[test]
    fn semioscan_config_lookup_view_matches_chain_lookups() {
        let config = SemioscanConfigBuilder::with_defaults()
            .serial_lookup_fallback_attempts(3)
            .chain_serial_lookup_fallback_attempts(NamedChain::ZkSync, 0)
            .build();

        assert_eq!(
            config
                .lookup_config(NamedChain::Mainnet)
                .serial_lookup_fallback_attempts,
            3
        );
        assert_eq!(
            config
                .lookup_config(NamedChain::ZkSync)
                .serial_lookup_fallback_attempts,
            0
        );
    }

    #[test]
    fn minimal_config_lookup_view_uses_default_attempts() {
        let config = SemioscanConfig::minimal();
        assert_eq!(
            config
                .lookup_config(NamedChain::Mainnet)
                .serial_lookup_fallback_attempts,
            1
        );
    }

    #[test]
    fn semioscan_config_rpc_view_returns_global_timeout_by_default() {
        let config = SemioscanConfig::default();
        assert_eq!(
            config.rpc_config(NamedChain::Mainnet).rpc_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(
            config.rpc_config(NamedChain::Arbitrum).rpc_timeout,
            Duration::from_secs(30)
        );
    }

    #[test]
    fn semioscan_config_rpc_view_honors_chain_override() {
        let config = SemioscanConfigBuilder::with_defaults()
            .rpc_timeout(Duration::from_secs(45))
            .chain_timeout(NamedChain::Polygon, Duration::from_secs(90))
            .build();

        assert_eq!(
            config.rpc_config(NamedChain::Mainnet).rpc_timeout,
            Duration::from_secs(45)
        );
        assert_eq!(
            config.rpc_config(NamedChain::Polygon).rpc_timeout,
            Duration::from_secs(90)
        );
    }
}
