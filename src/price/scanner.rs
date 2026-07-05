// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
// SPDX-FileCopyrightText: 2026 Joseph Livesey <jlivesey@gmail.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Swap-log scanner: a thin wrapper that builds the right [`Filter`] for a
//! [`PriceSource`] and delegates to the shared [`LogScanner`].

use alloy_chains::NamedChain;
use alloy_primitives::BlockNumber;
use alloy_provider::Provider;
use alloy_rpc_types::{Filter, Log};

use crate::config::SemioscanConfig;
use crate::errors::{PriceCalculationError, RpcError};
use crate::price::PriceSource;
use crate::scan::LogScanner;

/// Scans a block range for swap logs emitted by a [`PriceSource`]'s router.
///
/// Encapsulates the filter construction (router address + event topics) so
/// price code never has to repeat it. Chunk failures abort the scan so partial
/// log coverage cannot be cached as an authoritative price aggregate.
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
    /// Returns the accumulated logs when every chunk succeeds.
    /// Per-chunk failures abort the call and identify the failing block window.
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
                |chunk_from, chunk_to, e| {
                    Some(PriceCalculationError::from(RpcError::get_logs_failed(
                        format!("swap logs from block {chunk_from} to {chunk_to}"),
                        e,
                    )))
                },
            )
            .await
    }
}
