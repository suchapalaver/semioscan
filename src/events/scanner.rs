// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Event-domain wrapper around the neutral `LogScanner` primitive.
//!
//! [`EventScanner`] preserves the historical continue-on-error scanning
//! semantics used by the event-extraction call sites: failures on individual
//! chunks are logged and skipped rather than aborting the whole scan. The
//! underlying chunking, rate-limit, and tracing logic lives in the internal
//! `scan` module.
//!
//! # Examples
//!
//! ```rust,ignore
//! use semioscan::EventScanner;
//! use semioscan::events::filter::TransferFilterBuilder;
//! use alloy_chains::NamedChain;
//!
//! let scanner = EventScanner::new(provider, config);
//!
//! let filter = TransferFilterBuilder::new()
//!     .to_recipient(router)
//!     .build();
//!
//! let logs = scanner.scan(
//!     NamedChain::Arbitrum,
//!     filter,
//!     start_block,
//!     end_block,
//! ).await?;
//! ```

use alloy_chains::NamedChain;
use alloy_primitives::BlockNumber;
use alloy_provider::Provider;
use alloy_rpc_types::{Filter, Log};
use tracing::{debug, error};

use crate::config::SemioscanConfig;
use crate::errors::EventProcessingError;
use crate::scan::LogScanner;

/// Event scanner that fetches logs over a block range with chunking and
/// rate limiting, skipping any chunk whose `eth_getLogs` call fails.
///
/// This is a thin wrapper around the internal `LogScanner` that fixes the
/// per-chunk error policy to "continue on error", preserving the original
/// `EventScanner` behavior for downstream consumers.
///
/// # Examples
///
/// ```rust,ignore
/// use semioscan::{EventScanner, SemioscanConfig};
///
/// let scanner = EventScanner::new(provider, SemioscanConfig::default());
/// let logs = scanner.scan(chain, filter, start_block, end_block).await?;
/// ```
pub struct EventScanner<P> {
    inner: LogScanner<P>,
}

impl<P: Provider> EventScanner<P> {
    /// Create a new event scanner.
    pub fn new(provider: P, config: SemioscanConfig) -> Self {
        Self {
            inner: LogScanner::new(provider, config),
        }
    }

    /// Scan for events over a block range with automatic chunking and rate
    /// limiting. Per-chunk RPC failures are logged and the scan continues.
    pub async fn scan(
        &self,
        chain: NamedChain,
        filter_template: Filter,
        start_block: BlockNumber,
        end_block: BlockNumber,
    ) -> Result<Vec<Log>, EventProcessingError> {
        // The closure always returns None, so the Err arm of LogScanner::scan
        // is never produced. EventProcessingError is supplied only to satisfy
        // the type parameter.
        self.inner
            .scan::<EventProcessingError, _>(
                chain,
                filter_template,
                start_block,
                end_block,
                |_, _, _| None,
            )
            .await
    }

    /// Scan for events and pass the resulting logs to a handler.
    ///
    /// Per-chunk RPC failures are logged and skipped. After all chunks have
    /// been fetched, the handler is invoked **once** with the concatenated
    /// `Vec<Log>` from every successful chunk. This wrapper exists for API
    /// continuity; it does not stream per-chunk batches.
    #[allow(dead_code)]
    pub async fn scan_with_handler<F, Fut>(
        &self,
        chain: NamedChain,
        filter_template: Filter,
        start_block: BlockNumber,
        end_block: BlockNumber,
        mut handler: F,
    ) -> Result<(), EventProcessingError>
    where
        F: FnMut(Vec<Log>) -> Fut,
        Fut: std::future::Future<Output = Result<(), EventProcessingError>>,
    {
        debug!(
            chain = %chain,
            start_block,
            end_block,
            "Starting event scan with handler"
        );

        let logs = self
            .inner
            .scan::<EventProcessingError, _>(
                chain,
                filter_template,
                start_block,
                end_block,
                |_, _, _| None,
            )
            .await?;

        match handler(logs).await {
            Ok(()) => {}
            Err(e) => {
                error!(?e, %chain, "Handler returned error during event scan");
                return Err(e);
            }
        }

        debug!(chain = %chain, "Finished event scan with handler");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SemioscanConfigBuilder;
    use alloy_chains::NamedChain;
    use std::time::Duration;

    #[test]
    fn test_config_provides_correct_defaults() {
        let config = SemioscanConfig::default();

        assert_eq!(
            config.get_rate_limit_delay(NamedChain::Base),
            Some(Duration::from_millis(250))
        );
        assert_eq!(
            config.get_rate_limit_delay(NamedChain::Sonic),
            Some(Duration::from_millis(250))
        );
        assert_eq!(config.get_rate_limit_delay(NamedChain::Arbitrum), None);
    }

    #[test]
    fn test_custom_config_overrides() {
        let config = SemioscanConfigBuilder::with_defaults()
            .chain_rate_limit(NamedChain::Arbitrum, Duration::from_millis(100))
            .build();

        assert_eq!(
            config.get_rate_limit_delay(NamedChain::Arbitrum),
            Some(Duration::from_millis(100))
        );
    }

    #[test]
    fn test_minimal_config_has_no_delays() {
        let config = SemioscanConfig::minimal();

        assert_eq!(config.get_rate_limit_delay(NamedChain::Base), None);
        assert_eq!(config.get_rate_limit_delay(NamedChain::Sonic), None);
        assert_eq!(config.get_rate_limit_delay(NamedChain::Arbitrum), None);
    }
}
