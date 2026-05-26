// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Transfer-event log scanning for combined retrieval.
//!
//! [`TransferLogScanner`] builds a `Transfer` filter and chunks the scan via
//! [`LogScanner`], then decodes raw `RpcLog` entries into [`LogBatchEntry`]
//! records ready for tx/receipt enrichment. Decoding lives here so the
//! enrichment pipeline downstream can be tested with synthetic
//! `LogBatchEntry` values, without needing a provider.

use std::sync::Arc;

use alloy_chains::NamedChain;
use alloy_network::Network;
use alloy_primitives::{Address, BlockNumber, TxHash};
use alloy_provider::Provider;
use alloy_rpc_types::Log as RpcLog;
use alloy_sol_types::SolEvent;
use tracing::{error, info};

use crate::config::SemioscanConfig;
use crate::errors::RetrievalError;
use crate::events::definitions::Transfer;
use crate::scan::LogScanner;

use super::gas_calculation::GasCalculationCore;

/// Decoded `Transfer` log entry with the validated fields the enrichment
/// pipeline needs: `tx_hash`, `block_number`, and the raw transfer value.
///
/// `RpcLog` carries `transaction_hash` and `block_number` as `Option`; this
/// struct holds them unwrapped so downstream components can treat them as
/// required without re-validating.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LogBatchEntry {
    pub(crate) tx_hash: TxHash,
    pub(crate) block_number: BlockNumber,
    pub(crate) transfer_value: alloy_primitives::U256,
}

/// Scans a block range for ERC-20 `Transfer` events matching a (token, from,
/// to) triple and returns the decoded entries.
pub(crate) struct TransferLogScanner<N: Network, P: Provider<N> + Send + Sync + Clone + 'static> {
    provider: Arc<P>,
    config: SemioscanConfig,
    network_marker: std::marker::PhantomData<N>,
}

impl<N: Network, P: Provider<N> + Send + Sync + Clone + 'static> TransferLogScanner<N, P> {
    pub(crate) fn new(provider: Arc<P>, config: SemioscanConfig) -> Self {
        Self {
            provider,
            config,
            network_marker: std::marker::PhantomData,
        }
    }

    /// Scan the requested block range and return decoded transfer entries.
    ///
    /// Logs missing `transaction_hash` or `block_number`, and logs that fail
    /// `Transfer::decode_log`, are dropped with an `error!` event so they
    /// remain visible in operator traces. Skipping individual malformed
    /// logs is preferable to failing the whole scan: the enrichment pass
    /// downstream can still report partial-failure metadata for whatever
    /// did decode.
    pub(crate) async fn scan(
        &self,
        chain: NamedChain,
        from_address: Address,
        to_address: Address,
        token_address: Address,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> Result<Vec<LogBatchEntry>, RetrievalError> {
        let filter_template =
            GasCalculationCore::create_transfer_filter(token_address, from_address, to_address);

        let scanner = LogScanner::<_, N>::new(Arc::clone(&self.provider), self.config.clone());
        let logs: Vec<RpcLog> = scanner
            .scan::<RetrievalError, _>(
                chain,
                filter_template,
                from_block,
                to_block,
                |chunk_from, chunk_to, e| {
                    Some(RetrievalError::Rpc(
                        crate::errors::RpcError::get_logs_failed(
                            format!("get_logs for blocks {chunk_from}-{chunk_to} on {chain:?}"),
                            e,
                        ),
                    ))
                },
            )
            .await?;

        Ok(decode_transfer_logs(
            &logs,
            chain,
            from_address,
            to_address,
            token_address,
        ))
    }
}

/// Decode raw `RpcLog` entries into [`LogBatchEntry`] records, skipping
/// entries with missing required fields or unparseable transfer data.
///
/// Split out from [`TransferLogScanner::scan`] so the decode loop is
/// unit-testable against synthetic logs without standing up a provider.
pub(crate) fn decode_transfer_logs(
    logs: &[RpcLog],
    chain: NamedChain,
    from_address: Address,
    to_address: Address,
    token_address: Address,
) -> Vec<LogBatchEntry> {
    let mut entries = Vec::with_capacity(logs.len());
    for rpc_log_entry in logs {
        match Transfer::decode_log(&rpc_log_entry.inner) {
            Ok(transfer_event_data) => {
                let tx_hash = match rpc_log_entry.transaction_hash {
                    Some(hash) => hash,
                    None => {
                        error!("Missing transaction hash in log entry");
                        continue;
                    }
                };
                let block_number = match rpc_log_entry.block_number {
                    Some(num) => num,
                    None => {
                        error!("Missing block number in log entry");
                        continue;
                    }
                };

                info!(
                    ?chain, ?from_address, ?to_address, ?token_address,
                    amount = ?transfer_event_data.value,
                    block = block_number,
                    ?tx_hash,
                    "Decoded Transfer event for batch processing"
                );

                entries.push(LogBatchEntry {
                    tx_hash,
                    block_number,
                    transfer_value: transfer_event_data.value,
                });
            }
            Err(e) => {
                error!(
                    error = %e,
                    log_data = ?rpc_log_entry.data(),
                    log_topics = ?rpc_log_entry.topics(),
                    "Failed to decode Transfer log. Skipping log."
                );
            }
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, LogData, B256, U256};
    use alloy_sol_types::SolValue;

    fn make_transfer_log(
        tx_hash: Option<TxHash>,
        block_number: Option<BlockNumber>,
        token_address: Address,
        from_address: Address,
        to_address: Address,
        transfer_value: U256,
    ) -> RpcLog {
        RpcLog {
            inner: alloy_primitives::Log {
                address: token_address,
                data: LogData::new(
                    vec![
                        Transfer::SIGNATURE_HASH,
                        from_address.into_word(),
                        to_address.into_word(),
                    ],
                    transfer_value.abi_encode().into(),
                )
                .expect("valid log data"),
            },
            block_hash: Some(B256::repeat_byte(0x11)),
            block_number,
            block_timestamp: Some(1_700_000_000),
            transaction_hash: tx_hash,
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        }
    }

    fn make_malformed_log() -> RpcLog {
        RpcLog {
            inner: alloy_primitives::Log {
                address: Address::ZERO,
                data: LogData::new(vec![B256::repeat_byte(0xAA)], vec![].into())
                    .expect("valid log data"),
            },
            block_hash: Some(B256::repeat_byte(0x11)),
            block_number: Some(1),
            block_timestamp: Some(1_700_000_000),
            transaction_hash: Some(TxHash::from(B256::repeat_byte(0x22))),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        }
    }

    #[test]
    fn decode_valid_transfer_log_yields_entry_with_required_fields() {
        let token = address!("0xc333333333333333333333333333333333333333");
        let from = address!("0xa111111111111111111111111111111111111111");
        let to = address!("0xb222222222222222222222222222222222222222");
        let tx_hash = TxHash::from(B256::repeat_byte(0x10));
        let value = U256::from(1_234_u64);
        let log = make_transfer_log(Some(tx_hash), Some(42), token, from, to, value);

        let entries = decode_transfer_logs(&[log], NamedChain::Mainnet, from, to, token);

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tx_hash, tx_hash);
        assert_eq!(entries[0].block_number, 42);
        assert_eq!(entries[0].transfer_value, value);
    }

    #[test]
    fn log_missing_tx_hash_is_skipped_not_panicking() {
        let token = address!("0xc333333333333333333333333333333333333333");
        let from = address!("0xa111111111111111111111111111111111111111");
        let to = address!("0xb222222222222222222222222222222222222222");
        let log = make_transfer_log(None, Some(42), token, from, to, U256::from(1_u64));

        let entries = decode_transfer_logs(&[log], NamedChain::Mainnet, from, to, token);

        assert!(entries.is_empty());
    }

    #[test]
    fn log_missing_block_number_is_skipped_not_panicking() {
        let token = address!("0xc333333333333333333333333333333333333333");
        let from = address!("0xa111111111111111111111111111111111111111");
        let to = address!("0xb222222222222222222222222222222222222222");
        let tx_hash = TxHash::from(B256::repeat_byte(0x10));
        let log = make_transfer_log(Some(tx_hash), None, token, from, to, U256::from(1_u64));

        let entries = decode_transfer_logs(&[log], NamedChain::Mainnet, from, to, token);

        assert!(entries.is_empty());
    }

    #[test]
    fn malformed_log_topics_are_skipped_without_aborting_the_batch() {
        let token = address!("0xc333333333333333333333333333333333333333");
        let from = address!("0xa111111111111111111111111111111111111111");
        let to = address!("0xb222222222222222222222222222222222222222");
        let tx_hash = TxHash::from(B256::repeat_byte(0x10));
        let valid = make_transfer_log(Some(tx_hash), Some(42), token, from, to, U256::from(7_u64));

        let entries = decode_transfer_logs(
            &[make_malformed_log(), valid],
            NamedChain::Mainnet,
            from,
            to,
            token,
        );

        assert_eq!(
            entries.len(),
            1,
            "the malformed entry must not abort the batch"
        );
        assert_eq!(entries[0].transfer_value, U256::from(7_u64));
    }
}
