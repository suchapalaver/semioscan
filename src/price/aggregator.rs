// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Pure aggregator that folds normalised swap amounts into a
//! [`TokenPriceResult`].

use alloy_primitives::Address;

use crate::price::calculator::TokenPriceResult;
use crate::price::normalize::SwapAmounts;

/// Accumulates normalised swap amounts for a single token into a
/// [`TokenPriceResult`].
///
/// The aggregator is provider-free so price-folding logic can be exercised
/// with in-memory amounts. Consumers feed it [`SwapAmounts`] values and
/// finalise into a [`TokenPriceResult`] when the batch is done.
pub(crate) struct PriceAggregator {
    result: TokenPriceResult,
}

impl PriceAggregator {
    /// Create an aggregator that will collect amounts under `token_address`.
    pub fn new(token_address: Address) -> Self {
        Self {
            result: TokenPriceResult::new(token_address),
        }
    }

    /// Add a single normalised swap to the aggregate.
    pub fn add(&mut self, amounts: &SwapAmounts) {
        self.result
            .add_swap(amounts.token_amount.as_f64(), amounts.usdc_amount.as_f64());
    }

    /// Finalise the aggregator and return the accumulated result.
    pub fn finish(self) -> TokenPriceResult {
        self.result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NormalizedAmount, UsdValue};
    use alloy_primitives::address;

    #[test]
    fn empty_aggregator_finishes_to_zero_result() {
        let token = address!("1111111111111111111111111111111111111111");
        let result = PriceAggregator::new(token).finish();

        assert_eq!(result.token_address, token);
        assert_eq!(result.total_token_amount.as_f64(), 0.0);
        assert_eq!(result.total_usdc_amount.as_f64(), 0.0);
        assert_eq!(result.transaction_count.as_usize(), 0);
    }

    #[test]
    fn add_accumulates_amounts_and_counts() {
        let token = address!("1111111111111111111111111111111111111111");
        let mut agg = PriceAggregator::new(token);

        agg.add(&SwapAmounts {
            token_amount: NormalizedAmount::new(2.0),
            usdc_amount: UsdValue::new(4.0),
        });
        agg.add(&SwapAmounts {
            token_amount: NormalizedAmount::new(1.5),
            usdc_amount: UsdValue::new(3.0),
        });

        let result = agg.finish();
        assert!((result.total_token_amount.as_f64() - 3.5).abs() < 1e-9);
        assert!((result.total_usdc_amount.as_f64() - 7.0).abs() < 1e-9);
        assert_eq!(result.transaction_count.as_usize(), 2);
        assert!((result.get_average_price().as_f64() - 2.0).abs() < 1e-9);
    }
}
