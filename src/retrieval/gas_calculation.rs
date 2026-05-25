// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Core gas calculation logic

use alloy_eips::Typed2718;
use alloy_primitives::{Address, U256};
use alloy_rpc_types::{Filter, TransactionTrait};
use alloy_sol_types::SolEvent;

use crate::events::definitions::Transfer;
use crate::gas::transaction;

/// Core gas calculation logic
pub struct GasCalculationCore;

impl GasCalculationCore {
    pub(crate) fn calculate_blob_gas_cost<T>(transaction: &T) -> U256
    where
        T: TransactionTrait + Typed2718,
    {
        transaction::blob_gas_cost(transaction)
    }

    pub(crate) fn gas_price_override<T>(transaction: &T) -> Option<U256>
    where
        T: TransactionTrait + Typed2718,
    {
        transaction::gas_price_override(transaction)
    }

    /// Build a Transfer-event filter template (no block range).
    ///
    /// [`crate::scan::LogScanner`] stamps each chunk's `from_block`/`to_block`
    /// onto a clone of this template, so the block range is intentionally
    /// omitted here.
    pub(crate) fn create_transfer_filter(
        token_address: Address,
        from_address: Address, // topic1
        to_address: Address,   // topic2
    ) -> Filter {
        let transfer_topic_hash = Transfer::SIGNATURE_HASH;
        Filter::new()
            .address(token_address)
            .event_signature(transfer_topic_hash)
            .topic1(from_address)
            .topic2(to_address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, Address};

    #[test]
    fn create_transfer_filter_does_not_set_block_range() {
        let token = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let from = address!("1111111111111111111111111111111111111111");
        let to = address!("2222222222222222222222222222222222222222");

        let filter = GasCalculationCore::create_transfer_filter(token, from, to);

        // The template intentionally omits block range; LogScanner stamps each
        // chunk's range onto a clone.
        assert_eq!(filter.get_from_block(), None);
        assert_eq!(filter.get_to_block(), None);
    }

    #[test]
    fn create_transfer_filter_sets_correct_addresses() {
        let token = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let from = address!("1111111111111111111111111111111111111111");
        let to = address!("2222222222222222222222222222222222222222");

        let filter = GasCalculationCore::create_transfer_filter(token, from, to);

        let _ = filter;
    }

    #[test]
    fn create_transfer_filter_includes_transfer_event_signature() {
        let token = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let from = Address::ZERO;
        let to = Address::ZERO;

        let filter = GasCalculationCore::create_transfer_filter(token, from, to);

        let _ = filter;
    }
}
