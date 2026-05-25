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
//!
//! [`SemioscanConfig`](crate::SemioscanConfig) implements both traits, so
//! existing call sites that pass it through unchanged keep working. Internal
//! consumers (`LogScanner`, `CombinedCalculator`, `GasCostCalculator`) reach
//! into the narrower view rather than the full aggregate, making each
//! module's actual configuration dependency visible at the call site.

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
}
