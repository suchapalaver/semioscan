// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Transaction + receipt enrichment for combined retrieval.
//!
//! [`TxReceiptEnricher`] takes the [`LogBatchEntry`] values the scanner
//! produced and fetches the corresponding transaction and receipt for
//! each, in parallel via `tokio::join!`. The batch pass runs all entries
//! concurrently; a bounded serial-fallback pass retries individual
//! failures one at a time so partial outages don't reproduce the original
//! burst against the upstream.
//!
//! The zkSync permissive raw-decode fallback lives here: when
//! `eth_getTransactionByHash` deserialization fails on zkSync (typically
//! because the tx is missing `accessList`), the enricher retries the
//! same hash via a raw RPC call that accepts any-shaped tx.
//! [`should_attempt_permissive_tx_decode`] gates this fallback so it does
//! not fire on other chains or other error kinds.

use std::{borrow::Cow, sync::Arc};

use alloy_chains::NamedChain;
use alloy_eips::Typed2718;
use alloy_network::{AnyRpcTransaction, Network};
use alloy_provider::Provider;
use alloy_rpc_types::TransactionTrait;
use alloy_transport::TransportError;
use futures::future::join_all;
use tracing::{info, warn, Instrument};

use crate::errors::RetrievalError;
use crate::gas::adapter::ReceiptAdapter;
use crate::tracing::spans;

use super::failure::{build_lookup_attempt, build_lookup_failure, lookup_request_failed};
use super::gas_extractor::{extract_gas_and_amount, TransactionGasData};
use super::transfer_log_scanner::LogBatchEntry;
use super::types::{
    CombinedDataLookupFailure, CombinedDataLookupPass, CombinedDataLookupStage, GasAndAmountForTx,
};

/// Guard that decides whether to retry a failed `eth_getTransactionByHash`
/// with the permissive raw-decode path.
///
/// The observed zkSync incident shape is an Alloy deserialization error
/// (`missing field accessList`), so this matches the structured error
/// variant instead of a brittle rendered-string substring.
pub(crate) fn should_attempt_permissive_tx_decode(
    chain: NamedChain,
    error: &TransportError,
) -> bool {
    matches!(chain, NamedChain::ZkSync | NamedChain::ZkSyncTestnet) && error.is_deser_error()
}

/// Fetches the transaction and receipt for each [`LogBatchEntry`], with a
/// bounded serial-retry pass for batch failures.
pub(crate) struct TxReceiptEnricher<N, P>
where
    N: Network,
    N::TransactionResponse:
        TransactionTrait + alloy_provider::network::eip2718::Typed2718 + Send + Sync + Clone,
    N::ReceiptResponse: Send + Sync + std::fmt::Debug + Clone,
    P: Provider<N> + Send + Sync + Clone + 'static,
{
    provider: Arc<P>,
    network_marker: std::marker::PhantomData<N>,
}

impl<N, P> TxReceiptEnricher<N, P>
where
    N: Network,
    N::TransactionResponse:
        TransactionTrait + alloy_provider::network::eip2718::Typed2718 + Send + Sync + Clone,
    N::ReceiptResponse: Send + Sync + std::fmt::Debug + Clone,
    P: Provider<N> + Send + Sync + Clone + 'static,
{
    pub(crate) fn new(provider: Arc<P>) -> Self {
        Self {
            provider,
            network_marker: std::marker::PhantomData,
        }
    }

    /// Run the batch pass: enrich every entry concurrently.
    ///
    /// Empty input short-circuits to an empty vector without spawning any
    /// futures or hitting the provider.
    pub(crate) async fn enrich_batch<A>(
        &self,
        chain: NamedChain,
        entries: &[LogBatchEntry],
        adapter: &A,
    ) -> Vec<Result<GasAndAmountForTx, CombinedDataLookupFailure>>
    where
        A: ReceiptAdapter<N> + Send + Sync,
    {
        if entries.is_empty() {
            return vec![];
        }

        info!(
            count = entries.len(),
            "Batch fetching transaction data for logs"
        );

        let fetch_futures: Vec<_> = entries
            .iter()
            .copied()
            .map(|entry| async move {
                self.enrich_one(chain, entry, CombinedDataLookupPass::Batch, adapter)
                    .await
            })
            .collect();

        join_all(fetch_futures).await
    }

    /// Retry a single batch failure serially, up to `max_attempts` times.
    ///
    /// Each failed attempt's record is appended to the original failure's
    /// `attempts` vector so partial-failure metadata captures the full
    /// pass + retry history in order. Returns the consumed attempt count
    /// alongside the result so the caller can record fallback-attempt
    /// metadata.
    ///
    /// `max_attempts == 0` is a documented configuration: the loop body
    /// is skipped entirely and the original batch failure is returned
    /// with zero serial-fallback attempts recorded.
    pub(crate) async fn retry_failed<A>(
        &self,
        chain: NamedChain,
        mut failure: CombinedDataLookupFailure,
        max_attempts: usize,
        adapter: &A,
    ) -> (Result<GasAndAmountForTx, CombinedDataLookupFailure>, usize)
    where
        A: ReceiptAdapter<N> + Send + Sync,
    {
        let entry = LogBatchEntry {
            tx_hash: failure.tx_hash,
            block_number: failure.block_number,
            transfer_value: failure.transfer_value,
        };

        let mut attempts = 0;
        while attempts < max_attempts {
            attempts += 1;
            warn!(
                ?failure.tx_hash,
                block_number = failure.block_number,
                transfer_value = ?failure.transfer_value,
                attempt = attempts,
                max_attempts,
                "Retrying combined data lookup serially after batch failure"
            );

            match self
                .enrich_one(
                    chain,
                    entry,
                    CombinedDataLookupPass::SerialFallback,
                    adapter,
                )
                .await
            {
                Ok(data) => return (Ok(data), attempts),
                Err(retry_failure) => failure.attempts.extend(retry_failure.attempts),
            }
        }

        (Err(failure), attempts)
    }

    async fn enrich_one<A>(
        &self,
        chain: NamedChain,
        entry: LogBatchEntry,
        pass: CombinedDataLookupPass,
        adapter: &A,
    ) -> Result<GasAndAmountForTx, CombinedDataLookupFailure>
    where
        A: ReceiptAdapter<N> + Send + Sync,
    {
        let provider = self.provider.clone();
        let tx_hash = entry.tx_hash;
        let span = spans::process_log_for_combined_data(tx_hash);

        // The serial fallback intentionally re-fetches both tx and receipt even if
        // only one side failed in the batch pass. That keeps the retry path simple
        // and symmetric at the cost of at most one redundant RPC with current bounds.
        let (tx_result, receipt_result) = async move {
            tokio::join!(
                self.fetch_transaction_gas_data(chain, entry, pass),
                provider.get_transaction_receipt(tx_hash)
            )
        }
        .instrument(span)
        .await;

        extract_gas_and_amount::<N, A>(entry, tx_result, receipt_result, pass, adapter)
    }

    async fn fetch_transaction_gas_data(
        &self,
        chain: NamedChain,
        entry: LogBatchEntry,
        pass: CombinedDataLookupPass,
    ) -> Result<Option<TransactionGasData>, CombinedDataLookupFailure> {
        let tx_hash = entry.tx_hash;

        match self.provider.get_transaction_by_hash(tx_hash).await {
            Ok(transaction) => Ok(transaction
                .as_ref()
                .map(TransactionGasData::from_transaction)),
            Err(error) if should_attempt_permissive_tx_decode(chain, &error) => {
                warn!(
                    ?chain,
                    ?tx_hash,
                    original_error = %error,
                    "Typed transaction lookup failed; retrying with permissive raw transaction decoding"
                );

                match self
                    .provider
                    .raw_request::<_, Option<AnyRpcTransaction>>(
                        Cow::Borrowed("eth_getTransactionByHash"),
                        (tx_hash,),
                    )
                    .await
                {
                    Ok(transaction) => {
                        if let Some(transaction) = transaction.as_ref() {
                            info!(
                                ?chain,
                                ?tx_hash,
                                tx_type = transaction.ty(),
                                "Recovered transaction lookup with permissive raw transaction decoding"
                            );
                        }

                        Ok(transaction
                            .as_ref()
                            .map(TransactionGasData::from_transaction))
                    }
                    Err(raw_error) => {
                        warn!(
                            ?chain,
                            ?tx_hash,
                            original_error = %error,
                            raw_fallback_error = %raw_error,
                            "Permissive raw transaction decoding failed after typed lookup error"
                        );

                        let typed_failure = lookup_request_failed(
                            tx_hash,
                            CombinedDataLookupStage::Transaction,
                            error,
                        );
                        let raw_fallback_failure =
                            RetrievalError::Rpc(crate::errors::RpcError::request_failed(
                                format!("permissive_raw_get_transaction_by_hash({tx_hash})"),
                                raw_error,
                            ));
                        let mut failure = build_lookup_failure(
                            entry,
                            pass,
                            CombinedDataLookupStage::Transaction,
                            typed_failure,
                        );
                        failure.attempts.push(build_lookup_attempt(
                            pass,
                            CombinedDataLookupStage::Transaction,
                            &raw_fallback_failure,
                        ));

                        Err(failure)
                    }
                }
            }
            Err(error) => Err(build_lookup_failure(
                entry,
                pass,
                CombinedDataLookupStage::Transaction,
                lookup_request_failed(tx_hash, CombinedDataLookupStage::Transaction, error),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_transport::TransportErrorKind;

    #[test]
    fn permissive_tx_decode_guard_only_accepts_zksync_deser_errors() {
        let deser_error =
            serde_json::from_str::<u64>("\"not-a-number\"").expect_err("response should fail");
        let zksync_error = TransportError::deser_err(deser_error, "\"not-a-number\"");
        let transport_error = TransportError::from(TransportErrorKind::custom_str("boom"));

        assert!(should_attempt_permissive_tx_decode(
            NamedChain::ZkSync,
            &zksync_error
        ));
        assert!(should_attempt_permissive_tx_decode(
            NamedChain::ZkSyncTestnet,
            &zksync_error
        ));
        assert!(!should_attempt_permissive_tx_decode(
            NamedChain::Mainnet,
            &zksync_error
        ));
        assert!(!should_attempt_permissive_tx_decode(
            NamedChain::ZkSync,
            &transport_error
        ));
    }
}
