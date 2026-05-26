// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Pure swap extraction from log entries.
//!
//! [`extract_swaps`] walks a slice of logs through a [`PriceSource`]'s
//! [`extract_swap_from_log`](PriceSource::extract_swap_from_log) and
//! [`should_include_swap`](PriceSource::should_include_swap) hooks and
//! collects the accepted swaps. Decode failures and malformed events are
//! traced at `error` level but never abort the walk.

use alloy_primitives::Address;
use alloy_rpc_types::Log;
use std::collections::HashSet;
use tracing::error;

use crate::price::{PriceSource, PriceSourceError, SwapData};

/// Result of walking a batch of logs through a [`PriceSource`].
pub(crate) struct ExtractedSwaps {
    /// Swaps accepted by the price source and its filter.
    pub swaps: Vec<SwapData>,
}

impl ExtractedSwaps {
    /// Unique token addresses referenced by the extracted swaps.
    ///
    /// Useful for batching token-metadata fetches before normalisation.
    pub fn unique_token_addresses(&self) -> Vec<Address> {
        let mut seen = HashSet::with_capacity(self.swaps.len() * 2);
        for swap in &self.swaps {
            seen.insert(swap.token_in);
            seen.insert(swap.token_out);
        }
        seen.into_iter().collect()
    }
}

/// Walk `logs` through `price_source` and collect every accepted swap.
///
/// Logs that the price source rejects (returns `Ok(None)`) or that fail the
/// optional `should_include_swap` filter are silently dropped. Decode errors
/// and structurally invalid events are traced at `error` level so they
/// surface in operator dashboards without stopping the rest of the batch.
pub(crate) fn extract_swaps(price_source: &dyn PriceSource, logs: &[Log]) -> ExtractedSwaps {
    let mut swaps = Vec::new();

    for log in logs {
        match price_source.extract_swap_from_log(log) {
            Ok(Some(swap)) => {
                if price_source.should_include_swap(&swap) {
                    swaps.push(swap);
                }
            }
            Ok(None) => {}
            Err(e @ PriceSourceError::DecodeError(_)) => {
                error!(error = ?e, "Failed to decode log");
            }
            Err(
                e @ (PriceSourceError::EmptyTokenArrays
                | PriceSourceError::ArrayLengthMismatch { .. }
                | PriceSourceError::InvalidSwapData { .. }),
            ) => {
                error!(error = ?e, "Invalid swap data in log");
            }
        }
    }

    ExtractedSwaps { swaps }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, B256, U256};
    use alloy_rpc_types::Log as RpcLog;

    use std::sync::Mutex;

    /// Test double for [`PriceSource`] that returns a queued response per log.
    struct StubPriceSource {
        responses: Mutex<Vec<Result<Option<SwapData>, PriceSourceError>>>,
        accept: Box<dyn Fn(&SwapData) -> bool + Send + Sync>,
        cursor: Mutex<usize>,
    }

    impl StubPriceSource {
        fn new(responses: Vec<Result<Option<SwapData>, PriceSourceError>>) -> Self {
            Self {
                responses: Mutex::new(responses),
                accept: Box::new(|_| true),
                cursor: Mutex::new(0),
            }
        }

        fn with_filter<F: Fn(&SwapData) -> bool + Send + Sync + 'static>(mut self, f: F) -> Self {
            self.accept = Box::new(f);
            self
        }
    }

    impl PriceSource for StubPriceSource {
        fn router_address(&self) -> Address {
            Address::ZERO
        }

        fn event_topics(&self) -> Vec<B256> {
            Vec::new()
        }

        fn extract_swap_from_log(
            &self,
            _log: &RpcLog,
        ) -> Result<Option<SwapData>, PriceSourceError> {
            let mut cursor = self.cursor.lock().unwrap();
            let idx = *cursor;
            *cursor += 1;
            let mut responses = self.responses.lock().unwrap();
            std::mem::replace(&mut responses[idx], Ok(None))
        }

        fn should_include_swap(&self, swap: &SwapData) -> bool {
            (self.accept)(swap)
        }
    }

    fn empty_log() -> RpcLog {
        RpcLog::default()
    }

    fn fake_swap(token_in: Address, token_out: Address) -> SwapData {
        SwapData {
            token_in,
            token_in_amount: U256::from(1u64),
            token_out,
            token_out_amount: U256::from(2u64),
            sender: None,
            tx_hash: None,
            block_number: None,
        }
    }

    #[test]
    fn collects_only_accepted_swaps() {
        let a = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let b = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let c = address!("cccccccccccccccccccccccccccccccccccccccc");

        let stub = StubPriceSource::new(vec![
            Ok(Some(fake_swap(a, b))),
            Ok(None),
            Ok(Some(fake_swap(b, c))),
        ]);

        let logs = vec![empty_log(), empty_log(), empty_log()];
        let extracted = extract_swaps(&stub, &logs);

        assert_eq!(extracted.swaps.len(), 2);
        assert_eq!(extracted.swaps[0].token_in, a);
        assert_eq!(extracted.swaps[1].token_in, b);
    }

    #[test]
    fn applies_should_include_filter() {
        let a = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let b = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let allowed = a;

        let stub = StubPriceSource::new(vec![Ok(Some(fake_swap(a, b))), Ok(Some(fake_swap(b, a)))])
            .with_filter(move |swap| swap.token_in == allowed);

        let logs = vec![empty_log(), empty_log()];
        let extracted = extract_swaps(&stub, &logs);

        assert_eq!(extracted.swaps.len(), 1);
        assert_eq!(extracted.swaps[0].token_in, a);
    }

    #[test]
    fn errors_do_not_abort_walk() {
        let a = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let b = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        let stub = StubPriceSource::new(vec![
            Err(PriceSourceError::EmptyTokenArrays),
            Ok(Some(fake_swap(a, b))),
            Err(PriceSourceError::InvalidSwapData {
                details: "boom".into(),
            }),
            Ok(Some(fake_swap(b, a))),
        ]);

        let logs = vec![empty_log(), empty_log(), empty_log(), empty_log()];
        let extracted = extract_swaps(&stub, &logs);

        assert_eq!(extracted.swaps.len(), 2);
    }

    #[test]
    fn unique_token_addresses_deduplicates_across_swaps() {
        let a = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let b = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let c = address!("cccccccccccccccccccccccccccccccccccccccc");

        let extracted = ExtractedSwaps {
            swaps: vec![fake_swap(a, b), fake_swap(b, c), fake_swap(a, c)],
        };

        let mut addrs = extracted.unique_token_addresses();
        addrs.sort();
        assert_eq!(addrs, vec![a, b, c]);
    }
}
