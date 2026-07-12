// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
// SPDX-FileCopyrightText: 2026 Joseph Livesey <jlivesey@gmail.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Shared failure-building helpers for the combined retrieval pipeline.
//!
//! Every component in the pipeline produces the same shape of error record
//! (a [`CombinedDataLookupAttempt`] inside a [`CombinedDataLookupFailure`]),
//! and a handful of pure helpers — error-chain collection, transport-error
//! extraction, attempt/failure construction — get reused by the scanner,
//! the enricher, and the orchestration loop. They live here so each
//! component imports a single neutral module instead of poking at
//! sibling internals.

use std::error::Error as StdError;

use alloy_chains::NamedChain;
use alloy_primitives::{Address, BlockNumber, TxHash};
use alloy_transport::TransportError;
use tracing::error;

use crate::errors::RetrievalError;

use super::transfer_log_scanner::LogBatchEntry;
use super::types::{
    CombinedDataLookupAttempt, CombinedDataLookupFailure, CombinedDataLookupPass,
    CombinedDataLookupStage,
};

pub(crate) fn collect_error_chain(error: &(dyn StdError + 'static)) -> Vec<String> {
    let mut chain = vec![error.to_string()];
    let mut source = error.source();

    while let Some(err) = source {
        chain.push(err.to_string());
        source = err.source();
    }

    chain
}

#[allow(deprecated)]
pub(crate) fn transport_error_string(error: &RetrievalError) -> Option<String> {
    match error {
        RetrievalError::Rpc(crate::errors::RpcError::GetLogsFailed { source, .. })
        | RetrievalError::Rpc(crate::errors::RpcError::ChainConnectionFailed { source, .. })
        | RetrievalError::Rpc(crate::errors::RpcError::RequestFailed { source, .. })
        | RetrievalError::Rpc(crate::errors::RpcError::GetBlockNumberFailed { source })
        | RetrievalError::Rpc(crate::errors::RpcError::GetBlockFailed { source, .. }) => {
            Some(source.to_string())
        }
        _ => None,
    }
}

pub(crate) fn build_lookup_attempt(
    pass: CombinedDataLookupPass,
    stage: CombinedDataLookupStage,
    error: &RetrievalError,
) -> CombinedDataLookupAttempt {
    CombinedDataLookupAttempt {
        pass,
        stage,
        error: error.to_string(),
        error_chain: collect_error_chain(error),
        transport_error: transport_error_string(error),
    }
}

pub(crate) fn build_lookup_failure(
    entry: LogBatchEntry,
    pass: CombinedDataLookupPass,
    stage: CombinedDataLookupStage,
    error: RetrievalError,
) -> CombinedDataLookupFailure {
    CombinedDataLookupFailure {
        tx_hash: entry.tx_hash,
        block_number: entry.block_number,
        transfer_value: entry.transfer_value,
        attempts: vec![build_lookup_attempt(pass, stage, &error)],
    }
}

/// Build the [`RetrievalError`] returned when a transport-level lookup
/// (e.g. `get_transaction_by_hash`, `get_transaction_receipt`) fails for a
/// known tx hash. The stage name flows into the operation label so the
/// rendered error tells the operator *which* RPC dropped.
pub(crate) fn lookup_request_failed(
    tx_hash: TxHash,
    stage: CombinedDataLookupStage,
    error: TransportError,
) -> RetrievalError {
    RetrievalError::Rpc(crate::errors::RpcError::request_failed(
        format!("{stage}({tx_hash})"),
        error,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn log_combined_data_skip(
    failure: &CombinedDataLookupFailure,
    chain: NamedChain,
    from_address: Address,
    to_address: Address,
    token_address: Address,
    from_block: BlockNumber,
    to_block: BlockNumber,
) {
    let fallback_attempts = failure
        .attempts
        .iter()
        .filter(|attempt| attempt.pass == CombinedDataLookupPass::SerialFallback)
        .count();
    if let Some(final_attempt) = failure.final_attempt() {
        error!(
            ?chain,
            %from_address,
            %to_address,
            %token_address,
            from_block,
            to_block,
            ?failure.tx_hash,
            block_number = failure.block_number,
            transfer_value = ?failure.transfer_value,
            lookup_stage = ?final_attempt.stage,
            attempt_count = failure.attempts.len(),
            fallback_attempts,
            error = %final_attempt.error,
            error_chain = ?final_attempt.error_chain,
            transport_error = ?final_attempt.transport_error,
            attempt_history = ?failure.attempts,
            "Error processing decoded transfer for combined data. Skipping transfer and marking result partial."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::RpcError;
    use alloy_transport::TransportErrorKind;
    use std::fmt;

    #[derive(Debug)]
    struct OuterError {
        inner: InnerError,
    }

    #[derive(Debug)]
    struct InnerError;

    impl fmt::Display for OuterError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "outer error")
        }
    }

    impl fmt::Display for InnerError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "inner error")
        }
    }

    impl StdError for OuterError {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            Some(&self.inner)
        }
    }

    impl StdError for InnerError {}

    #[test]
    fn collect_error_chain_walks_nested_sources_in_order() {
        let err = OuterError { inner: InnerError };
        let chain = collect_error_chain(&err);
        assert_eq!(
            chain,
            vec!["outer error".to_string(), "inner error".to_string()]
        );
    }

    #[test]
    fn transport_error_string_extracts_source_for_request_failed_rpc_variant() {
        let transport = TransportError::from(TransportErrorKind::custom_str("boom"));
        let err = RetrievalError::Rpc(RpcError::request_failed("op", transport));

        let surfaced = transport_error_string(&err)
            .expect("RequestFailed must surface a transport error string");
        assert!(
            surfaced.contains("boom"),
            "transport source must be reachable from the rendered string; got {surfaced}"
        );
    }

    #[test]
    fn transport_error_string_returns_none_for_non_transport_rpc_variant() {
        let err = RetrievalError::missing_transaction("0xabc");
        assert!(transport_error_string(&err).is_none());
    }
}
