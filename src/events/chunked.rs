// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Chunked log fetching utility
//!
//! Provides a standalone function for fetching logs in chunks without
//! requiring a [`SemioscanConfig`](crate::SemioscanConfig) or chain-specific
//! configuration. Useful when a caller has only a chunk size in hand and
//! wants fail-fast semantics.
//!
//! Internally this delegates to the canonical
//! [`LogScanner`](crate::scan::LogScanner) so that the chunking loop has a
//! single implementation shared with `EventScanner`, `GasCostCalculator`,
//! `CombinedCalculator`, and `PriceCalculator`.
//!
//! # Example
//!
//! ```rust,no_run
//! use semioscan::fetch_logs_chunked;
//! use alloy_primitives::Address;
//! use alloy_provider::ProviderBuilder;
//! use alloy_rpc_types::Filter;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = ProviderBuilder::new()
//!     .connect_http("https://eth.llamarpc.com".parse()?);
//!
//! let contract_address: Address = "0xdAC17F958D2ee523a2206206994597C13D831ec7".parse()?;
//!
//! let filter = Filter::new()
//!     .address(contract_address)
//!     .from_block(20_000_000)
//!     .to_block(20_000_100);
//!
//! // Fetch in 50-block chunks
//! let logs = fetch_logs_chunked(&provider, filter, 50).await?;
//! # Ok(())
//! # }
//! ```

use alloy_chains::NamedChain;
use alloy_provider::Provider;
use alloy_rpc_types::{Filter, Log};

use crate::errors::EventProcessingError;
use crate::scan::LogScanner;
use crate::SemioscanConfigBuilder;

/// Fetch logs in chunks to handle large block ranges.
///
/// Splits the filter's block range into chunks of `chunk_size` blocks and
/// fetches sequentially, concatenating results. This is useful when RPC
/// providers reject queries spanning too many blocks.
///
/// The fetch loop is the same one used by `EventScanner`, `GasCostCalculator`,
/// `CombinedCalculator`, and `PriceCalculator`; this function configures it
/// with no rate-limit delay and a fail-fast per-chunk error policy.
///
/// # Arguments
///
/// * `provider` - Any Alloy provider
/// * `filter` - Filter with `from_block` and `to_block` set
/// * `chunk_size` - Maximum blocks per RPC call (e.g., 500)
///
/// # Returns
///
/// All logs matching the filter, concatenated across all chunks.
///
/// # Errors
///
/// Returns an error if:
/// - `chunk_size` is 0
/// - The filter doesn't have both `from_block` and `to_block` set
/// - Any chunk fetch fails (fails fast, no partial results)
///
/// # Example
///
/// ```rust,no_run
/// use semioscan::fetch_logs_chunked;
/// use alloy_primitives::Address;
/// use alloy_provider::ProviderBuilder;
/// use alloy_rpc_types::Filter;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let provider = ProviderBuilder::new()
///     .connect_http("https://eth.llamarpc.com".parse()?);
///
/// let swap_router: Address = "0x1111111254EEB25477B68fb85Ed929f73A960582".parse()?;
///
/// // Build filter with block range
/// let filter = Filter::new()
///     .address(swap_router)
///     .from_block(20_000_000)
///     .to_block(20_000_500);
///
/// // Fetch in 100-block chunks
/// let logs = fetch_logs_chunked(&provider, filter, 100).await?;
///
/// for log in logs {
///     println!("Log from block {:?}", log.block_number);
/// }
/// # Ok(())
/// # }
/// ```
pub async fn fetch_logs_chunked<P: Provider>(
    provider: &P,
    filter: Filter,
    chunk_size: u64,
) -> Result<Vec<Log>, EventProcessingError> {
    if chunk_size == 0 {
        return Err(EventProcessingError::invalid_input(
            "chunk_size must be greater than 0",
        ));
    }

    let start_block = filter
        .get_from_block()
        .ok_or_else(|| EventProcessingError::invalid_input("Filter must have from_block set"))?;

    let end_block = filter
        .get_to_block()
        .ok_or_else(|| EventProcessingError::invalid_input("Filter must have to_block set"))?;

    // `LogScanner` reads its chunk size and (optional) rate-limit delay from
    // `SemioscanConfig` keyed by chain. `fetch_logs_chunked` is chain-agnostic,
    // so build a one-off config that pins the caller-supplied chunk size to a
    // sentinel chain and leaves rate limiting disabled. `SemioscanConfigBuilder::new`
    // starts from `SemioscanConfig::minimal` — no rate-limit delay and no chain
    // overrides — so the sentinel inherits "no delay" without explicit clearing.
    const SENTINEL_CHAIN: NamedChain = NamedChain::Mainnet;
    let config = SemioscanConfigBuilder::new()
        .chain_max_blocks(SENTINEL_CHAIN, chunk_size)
        .build();

    let scanner = LogScanner::new(provider, config);

    scanner
        .scan(SENTINEL_CHAIN, filter, start_block, end_block, |e| {
            Some(EventProcessingError::rpc_failed(format!(
                "Failed to fetch logs: {e}"
            )))
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_json_rpc as j;
    use alloy_primitives::{Address, B256};
    use alloy_provider::{ProviderBuilder, RootProvider};
    use alloy_rpc_client::RpcClient;
    use alloy_rpc_types::Log as RpcLog;
    use alloy_transport::{TransportError, TransportErrorKind, TransportFut, TransportResult};
    use std::{
        borrow::Cow,
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };

    /// Test transport that returns queued JSON-RPC responses per method.
    /// Mirrors the helper in `src/scan/logs.rs::tests`; lifted here so
    /// `fetch_logs_chunked` can be exercised without a live RPC.
    #[derive(Clone, Default)]
    struct ScriptedTransport {
        responses: Arc<Mutex<HashMap<String, VecDeque<j::ResponsePayload>>>>,
        call_count: Arc<Mutex<usize>>,
    }

    impl ScriptedTransport {
        fn push_success<R: serde::Serialize>(&self, method: &str, response: &R) {
            let serialized = serde_json::to_string(response).expect("response should serialize");
            let payload = j::ResponsePayload::Success(
                serde_json::value::RawValue::from_string(serialized)
                    .expect("response should convert to raw JSON"),
            );
            self.responses
                .lock()
                .expect("responses lock")
                .entry(method.to_string())
                .or_default()
                .push_back(payload);
        }

        fn push_failure(&self, method: &str, message: impl Into<Cow<'static, str>>) {
            self.responses
                .lock()
                .expect("responses lock")
                .entry(method.to_string())
                .or_default()
                .push_back(j::ResponsePayload::internal_error_message(message.into()));
        }

        fn calls(&self) -> usize {
            *self.call_count.lock().expect("call_count lock")
        }

        fn map_request(&self, request: j::SerializedRequest) -> TransportResult<j::Response> {
            *self.call_count.lock().expect("call_count lock") += 1;

            let method = request.method().to_string();
            let payload = self
                .responses
                .lock()
                .expect("responses lock")
                .entry(method.clone())
                .or_default()
                .pop_front()
                .ok_or_else(|| {
                    TransportErrorKind::custom_str(&format!(
                        "no mocked response queued for method {method}"
                    ))
                })?;

            Ok(j::Response {
                id: request.id().clone(),
                payload,
            })
        }

        async fn handle(self, request: j::RequestPacket) -> TransportResult<j::ResponsePacket> {
            Ok(match request {
                j::RequestPacket::Single(request) => {
                    j::ResponsePacket::Single(self.map_request(request)?)
                }
                j::RequestPacket::Batch(requests) => j::ResponsePacket::Batch(
                    requests
                        .into_iter()
                        .map(|request| self.map_request(request))
                        .collect::<TransportResult<_>>()?,
                ),
            })
        }
    }

    impl tower::Service<j::RequestPacket> for ScriptedTransport {
        type Response = j::ResponsePacket;
        type Error = TransportError;
        type Future = TransportFut<'static>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: j::RequestPacket) -> Self::Future {
            Box::pin(self.clone().handle(request))
        }
    }

    fn build_provider(transport: ScriptedTransport) -> RootProvider {
        ProviderBuilder::default().connect_client(RpcClient::new(transport, false))
    }

    fn dummy_log() -> RpcLog {
        RpcLog {
            inner: alloy_primitives::Log {
                address: Address::repeat_byte(0xaa),
                data: alloy_primitives::LogData::new_unchecked(
                    vec![B256::repeat_byte(0x11)],
                    Default::default(),
                ),
            },
            block_hash: Some(B256::repeat_byte(0x22)),
            block_number: Some(0),
            block_timestamp: Some(0),
            transaction_hash: Some(B256::repeat_byte(0x33)),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        }
    }

    /// Validation-only provider; never reached when validation rejects first.
    fn dummy_provider() -> impl Provider {
        ProviderBuilder::new().connect_http("http://localhost:1".parse().unwrap())
    }

    #[tokio::test]
    async fn chunk_size_zero_returns_error() {
        let provider = dummy_provider();
        let filter = Filter::new().from_block(0).to_block(100);

        let result = fetch_logs_chunked(&provider, filter, 0).await;

        let err = result.expect_err("chunk_size 0 must be rejected");
        assert!(
            matches!(err, EventProcessingError::InvalidInput { .. }),
            "expected InvalidInput, got: {err:?}"
        );
        assert!(err.to_string().contains("chunk_size"));
    }

    #[tokio::test]
    async fn missing_from_block_returns_error() {
        let provider = dummy_provider();
        let filter = Filter::new().to_block(100); // no from_block

        let result = fetch_logs_chunked(&provider, filter, 500).await;

        let err = result.expect_err("missing from_block must be rejected");
        assert!(
            matches!(err, EventProcessingError::InvalidInput { .. }),
            "expected InvalidInput, got: {err:?}"
        );
        assert!(err.to_string().contains("from_block"));
    }

    #[tokio::test]
    async fn missing_to_block_returns_error() {
        let provider = dummy_provider();
        let filter = Filter::new().from_block(0); // no to_block

        let result = fetch_logs_chunked(&provider, filter, 500).await;

        let err = result.expect_err("missing to_block must be rejected");
        assert!(
            matches!(err, EventProcessingError::InvalidInput { .. }),
            "expected InvalidInput, got: {err:?}"
        );
        assert!(err.to_string().contains("to_block"));
    }

    #[tokio::test]
    async fn multi_chunk_range_issues_one_call_per_chunk() {
        let transport = ScriptedTransport::default();
        // 0..=299 in 100-block chunks = 3 chunks.
        for _ in 0..3 {
            transport.push_success("eth_getLogs", &Vec::<RpcLog>::new());
        }

        let provider = build_provider(transport.clone());
        let filter = Filter::new().from_block(0).to_block(299);

        fetch_logs_chunked(&provider, filter, 100)
            .await
            .expect("happy-path chunked fetch must succeed");

        assert_eq!(transport.calls(), 3);
    }

    #[tokio::test]
    async fn logs_from_every_chunk_are_concatenated() {
        let transport = ScriptedTransport::default();
        // Chunk 1 returns one log, chunk 2 returns two logs.
        transport.push_success("eth_getLogs", &vec![dummy_log()]);
        transport.push_success("eth_getLogs", &vec![dummy_log(), dummy_log()]);

        let provider = build_provider(transport);
        let filter = Filter::new().from_block(0).to_block(199);

        let logs = fetch_logs_chunked(&provider, filter, 100)
            .await
            .expect("happy-path chunked fetch must succeed");

        assert_eq!(
            logs.len(),
            3,
            "concatenation must preserve every chunk's logs"
        );
    }

    #[tokio::test]
    async fn fails_fast_on_first_chunk_error() {
        let transport = ScriptedTransport::default();
        // First chunk fails; a second response is queued and must NOT be consumed.
        transport.push_failure("eth_getLogs", "rpc unavailable");
        transport.push_success("eth_getLogs", &Vec::<RpcLog>::new());

        let provider = build_provider(transport.clone());
        let filter = Filter::new().from_block(0).to_block(199);

        let err = fetch_logs_chunked(&provider, filter, 100)
            .await
            .expect_err("fail-fast policy must surface the first chunk's error");
        assert!(
            matches!(err, EventProcessingError::RpcFailed { .. }),
            "transport error must map to EventProcessingError::RpcFailed, got: {err:?}"
        );
        assert_eq!(
            transport.calls(),
            1,
            "fail-fast must not attempt subsequent chunks after the first failure"
        );
    }

    #[tokio::test]
    async fn fails_fast_on_mid_stream_chunk_error() {
        let transport = ScriptedTransport::default();
        // First chunk OK, second fails, third would succeed — must never be reached.
        transport.push_success("eth_getLogs", &vec![dummy_log()]);
        transport.push_failure("eth_getLogs", "boom on chunk two");
        transport.push_success("eth_getLogs", &Vec::<RpcLog>::new());

        let provider = build_provider(transport.clone());
        let filter = Filter::new().from_block(0).to_block(299);

        let err = fetch_logs_chunked(&provider, filter, 100)
            .await
            .expect_err("fail-fast must surface a mid-stream chunk error");
        assert!(matches!(err, EventProcessingError::RpcFailed { .. }));
        assert_eq!(
            transport.calls(),
            2,
            "third chunk must not be attempted after the second fails"
        );
    }
}
