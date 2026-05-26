// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Pure conversion of (transaction, receipt, adapter) into the
//! [`GasAndAmountForTx`] fields combined retrieval reports.
//!
//! The extractor is a free function rather than a struct so it stays
//! straightforward to unit-test: pass in synthetic [`LogBatchEntry`],
//! [`TransactionGasData`], and receipt values, and assert the assembled
//! result. It does no I/O — the I/O happens in the enricher upstream.

use alloy_eips::Typed2718;
use alloy_network::Network;
use alloy_primitives::U256;
use alloy_rpc_types::TransactionTrait;
use alloy_transport::TransportError;

use crate::errors::RetrievalError;
use crate::gas::adapter::ReceiptAdapter;
use crate::types::gas::{GasAmount, GasPrice};

use super::failure::{build_lookup_failure, lookup_request_failed};
use super::gas_calculation::GasCalculationCore;
use super::transfer_log_scanner::LogBatchEntry;
use super::types::{
    CombinedDataLookupFailure, CombinedDataLookupPass, CombinedDataLookupStage, GasAndAmountForTx,
};

/// Pure summary of the gas-side data the extractor needs from the
/// transaction itself.
///
/// `gas_price_override` carries the EIP-2930 / legacy / zkSync `gasPrice`
/// value when the transaction type forces it to win over the receipt's
/// `effective_gas_price`; otherwise it is `None` and the receipt's
/// effective price applies. `blob_gas_cost` is the EIP-4844 blob cost
/// computed from the transaction type, in wei.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TransactionGasData {
    pub(crate) gas_price_override: Option<U256>,
    pub(crate) blob_gas_cost: U256,
}

impl TransactionGasData {
    pub(crate) fn from_transaction<T>(transaction: &T) -> Self
    where
        T: TransactionTrait + Typed2718,
    {
        Self {
            gas_price_override: GasCalculationCore::gas_price_override(transaction),
            blob_gas_cost: GasCalculationCore::calculate_blob_gas_cost(transaction),
        }
    }

    pub(crate) fn effective_gas_price(self, receipt_effective_gas_price: U256) -> U256 {
        self.gas_price_override
            .unwrap_or(receipt_effective_gas_price)
    }
}

/// Assemble a [`GasAndAmountForTx`] from a fetched (transaction, receipt)
/// pair, or return the failure that caused either side to be unavailable.
///
/// `tx_result` carries the optional [`TransactionGasData`] when the tx
/// lookup succeeded (and `None` when the upstream returned no transaction
/// at all). `receipt_result` is the raw transport-level receipt response.
/// `pass` identifies which pass of the pipeline produced these inputs;
/// it flows into the failure record so partial-failure metadata stays
/// accurate.
pub(crate) fn extract_gas_and_amount<N, A>(
    entry: LogBatchEntry,
    tx_result: Result<Option<TransactionGasData>, CombinedDataLookupFailure>,
    receipt_result: Result<Option<N::ReceiptResponse>, TransportError>,
    pass: CombinedDataLookupPass,
    adapter: &A,
) -> Result<GasAndAmountForTx, CombinedDataLookupFailure>
where
    N: Network,
    A: ReceiptAdapter<N> + Send + Sync,
{
    let tx_hash = entry.tx_hash;

    let transaction = tx_result?.ok_or_else(|| {
        build_lookup_failure(
            entry,
            pass,
            CombinedDataLookupStage::Transaction,
            RetrievalError::missing_transaction(&tx_hash.to_string()),
        )
    })?;

    let receipt = receipt_result
        .map_err(|error| {
            build_lookup_failure(
                entry,
                pass,
                CombinedDataLookupStage::Receipt,
                lookup_request_failed(tx_hash, CombinedDataLookupStage::Receipt, error),
            )
        })?
        .ok_or_else(|| {
            build_lookup_failure(
                entry,
                pass,
                CombinedDataLookupStage::Receipt,
                RetrievalError::missing_receipt(&tx_hash.to_string()),
            )
        })?;

    let gas_used = adapter.gas_used(&receipt);
    let receipt_effective_gas_price = adapter.effective_gas_price(&receipt);
    let l1_fee = adapter.l1_data_fee(&receipt);

    let effective_gas_price = transaction.effective_gas_price(receipt_effective_gas_price);
    let blob_gas_cost = transaction.blob_gas_cost;

    Ok(GasAndAmountForTx {
        tx_hash,
        block_number: entry.block_number,
        gas_used: GasAmount::from(gas_used),
        effective_gas_price: GasPrice::from(effective_gas_price),
        l1_fee,
        transferred_amount: entry.transfer_value,
        blob_gas_cost,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_network::Ethereum;
    use alloy_primitives::{address, TxHash, B256};
    use alloy_transport::TransportErrorKind;
    use serde_json::json;

    use crate::gas::adapter::EthereumReceiptAdapter;

    fn entry(value: u64) -> LogBatchEntry {
        LogBatchEntry {
            tx_hash: TxHash::from(B256::repeat_byte(0xAA)),
            block_number: 100,
            transfer_value: U256::from(value),
        }
    }

    fn receipt(effective_gas_price: u128) -> <Ethereum as Network>::ReceiptResponse {
        let from = address!("0x1111111111111111111111111111111111111111");
        let to = address!("0x2222222222222222222222222222222222222222");
        let tx_hash = TxHash::from(B256::repeat_byte(0xAA));
        serde_json::from_value(json!({
            "transactionHash": tx_hash,
            "blockHash": B256::repeat_byte(0x22),
            "blockNumber": "0x64",
            "transactionIndex": "0x0",
            "from": from,
            "to": to,
            "cumulativeGasUsed": "0x5208",
            "gasUsed": "0x5208",
            "effectiveGasPrice": format!("0x{effective_gas_price:x}"),
            "logs": [],
            "logsBloom": format!("0x{}", "0".repeat(512)),
            "status": "0x1",
            "type": "0x2"
        }))
        .expect("valid receipt response")
    }

    #[test]
    fn missing_transaction_produces_transaction_stage_failure() {
        let adapter = EthereumReceiptAdapter;
        let err = extract_gas_and_amount::<Ethereum, _>(
            entry(1),
            Ok(None),
            Ok(Some(receipt(100))),
            CombinedDataLookupPass::Batch,
            &adapter,
        )
        .expect_err("missing transaction must surface as a failure");

        assert_eq!(err.attempts.len(), 1);
        assert_eq!(err.attempts[0].stage, CombinedDataLookupStage::Transaction);
    }

    #[test]
    fn missing_receipt_produces_receipt_stage_failure() {
        let adapter = EthereumReceiptAdapter;
        let err = extract_gas_and_amount::<Ethereum, _>(
            entry(1),
            Ok(Some(TransactionGasData {
                gas_price_override: None,
                blob_gas_cost: U256::ZERO,
            })),
            Ok(None),
            CombinedDataLookupPass::Batch,
            &adapter,
        )
        .expect_err("missing receipt must surface as a failure");

        assert_eq!(err.attempts.len(), 1);
        assert_eq!(err.attempts[0].stage, CombinedDataLookupStage::Receipt);
    }

    #[test]
    fn receipt_transport_error_produces_receipt_stage_failure_carrying_transport_string() {
        let adapter = EthereumReceiptAdapter;
        let err = extract_gas_and_amount::<Ethereum, _>(
            entry(1),
            Ok(Some(TransactionGasData {
                gas_price_override: None,
                blob_gas_cost: U256::ZERO,
            })),
            Err(TransportError::from(TransportErrorKind::custom_str(
                "receipt boom",
            ))),
            CombinedDataLookupPass::Batch,
            &adapter,
        )
        .expect_err("transport error must surface as a receipt-stage failure");

        assert_eq!(err.attempts.len(), 1);
        assert_eq!(err.attempts[0].stage, CombinedDataLookupStage::Receipt);
        assert!(err.attempts[0]
            .transport_error
            .as_deref()
            .is_some_and(|s| s.contains("receipt boom")));
    }

    #[test]
    fn gas_price_override_wins_over_receipt_effective_price() {
        let adapter = EthereumReceiptAdapter;
        let result = extract_gas_and_amount::<Ethereum, _>(
            entry(7),
            Ok(Some(TransactionGasData {
                gas_price_override: Some(U256::from(999_u64)),
                blob_gas_cost: U256::ZERO,
            })),
            Ok(Some(receipt(100))),
            CombinedDataLookupPass::Batch,
            &adapter,
        )
        .expect("complete inputs must extract");

        assert_eq!(
            result.effective_gas_price,
            GasPrice::from(U256::from(999_u64)),
            "the legacy/zkSync override must shadow the receipt's effective_gas_price"
        );
        assert_eq!(result.transferred_amount, U256::from(7_u64));
    }

    #[test]
    fn no_gas_price_override_falls_back_to_receipt_effective_price() {
        let adapter = EthereumReceiptAdapter;
        let result = extract_gas_and_amount::<Ethereum, _>(
            entry(7),
            Ok(Some(TransactionGasData {
                gas_price_override: None,
                blob_gas_cost: U256::ZERO,
            })),
            Ok(Some(receipt(100))),
            CombinedDataLookupPass::Batch,
            &adapter,
        )
        .expect("complete inputs must extract");

        assert_eq!(
            result.effective_gas_price,
            GasPrice::from(U256::from(100_u64))
        );
    }
}
