// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Swap-log scanner: a thin wrapper that builds the right [`Filter`] for a
//! [`PriceSource`] and delegates to the shared [`LogScanner`].

use alloy_chains::NamedChain;
use alloy_primitives::BlockNumber;
use alloy_provider::Provider;
use alloy_rpc_types::{Filter, Log};

use crate::config::SemioscanConfig;
use crate::errors::PriceCalculationError;
use crate::price::PriceSource;
use crate::scan::LogScanner;

/// Scans a block range for swap logs emitted by a [`PriceSource`]'s router.
///
/// Encapsulates the filter construction (router address + event topics) so
/// price code never has to repeat it. Errors inside individual chunks are
/// reported as "continue on coverage loss" — a single failing chunk reduces
/// the swap set but never aborts the scan.
pub(crate) struct SwapLogScanner<'a, P> {
    provider: &'a P,
    chain: NamedChain,
    config: SemioscanConfig,
    filter: Filter,
}

impl<'a, P: Provider + Clone> SwapLogScanner<'a, P> {
    /// Wrap `provider` and pre-compute the swap-log filter from `price_source`.
    pub fn new(
        provider: &'a P,
        chain: NamedChain,
        price_source: &dyn PriceSource,
        config: SemioscanConfig,
    ) -> Self {
        let filter = Filter::new()
            .address(price_source.router_address())
            .event_signature(price_source.event_topics());

        Self {
            provider,
            chain,
            config,
            filter,
        }
    }

    /// Scan `[start, end]` for swap logs.
    ///
    /// Returns the accumulated logs across every chunk that succeeded.
    /// Per-chunk failures are continue-on-error: they reduce coverage but
    /// never fail the call.
    pub async fn scan(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> Result<Vec<Log>, PriceCalculationError> {
        let scanner = LogScanner::new(self.provider, self.config.clone());
        scanner
            .scan::<PriceCalculationError, _>(
                self.chain,
                self.filter.clone(),
                start,
                end,
                |_, _, _| None,
            )
            .await
    }
}
