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

use alloy_provider::Provider;
use alloy_rpc_types::{Filter, Log};

use crate::errors::EventProcessingError;
use crate::scan::LogScanner;
use crate::SemioscanConfig;

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

    // `fetch_logs_chunked` is chain-agnostic: it carries no chain context, so
    // it uses `LogScanner::scan_raw` to pass `chunk_size` directly and skip
    // both the chain-keyed config lookup and the `chain` tracing field. The
    // config is only here to satisfy `LogScanner::new`; `scan_raw` doesn't
    // read chunk size or rate limit from it.
    let scanner = LogScanner::new(provider, SemioscanConfig::minimal());

    scanner
        .scan_raw(
            chunk_size,
            None,
            filter,
            start_block,
            end_block,
            |chunk_from, chunk_to, e| {
                Some(EventProcessingError::rpc_failed(format!(
                    "Failed to fetch logs for blocks {chunk_from}-{chunk_to}: {e}"
                )))
            },
        )
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

    #[tokio::test]
    async fn chunk_error_message_identifies_the_failing_block_window() {
        // Operators rely on the chunked-fetch error string to pinpoint which
        // chunk of a multi-chunk scan failed. With three 100-block chunks
        // (0-99, 100-199, 200-299) and only the middle chunk failing, the
        // error must name 100-199 rather than the outer 0-299 range or any
        // other ambiguous shape.
        let transport = ScriptedTransport::default();
        transport.push_success("eth_getLogs", &vec![dummy_log()]);
        transport.push_failure("eth_getLogs", "boom on chunk two");
        transport.push_success("eth_getLogs", &Vec::<RpcLog>::new());

        let provider = build_provider(transport);
        let filter = Filter::new().from_block(0).to_block(299);

        let err = fetch_logs_chunked(&provider, filter, 100)
            .await
            .expect_err("fail-fast must surface a mid-stream chunk error");
        let msg = err.to_string();
        assert!(
            msg.contains("100-199"),
            "error message must identify the failing chunk window 100-199; got {msg:?}"
        );
    }

    /// JSON-line tracing capture for asserting on event fields. Mirrors the
    /// helper in `src/scan/logs.rs::tests`; lifted here so `fetch_logs_chunked`
    /// can be exercised without a live RPC.
    #[derive(Clone, Default)]
    struct CapturingWriter {
        buf: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturingWriter {
        fn captured(&self) -> String {
            String::from_utf8(self.buf.lock().expect("capture buffer lock").clone())
                .expect("captured tracing output should be valid UTF-8")
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturingWriter {
        type Writer = CapturingWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl std::io::Write for CapturingWriter {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.buf
                .lock()
                .expect("capture buffer lock")
                .extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn install_subscriber(writer: CapturingWriter) -> tracing::subscriber::DefaultGuard {
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(writer)
            .with_current_span(true)
            .with_span_list(true)
            .with_max_level(tracing::Level::DEBUG)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    /// Mirrors `src/scan/logs.rs::tests::parse_events`.
    fn parse_events(captured: &str) -> Vec<serde_json::Value> {
        captured
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str::<serde_json::Value>(l).expect("tracing line is JSON"))
            .collect()
    }

    /// Mirrors `src/scan/logs.rs::tests::find_event`.
    fn find_event<'a>(events: &'a [serde_json::Value], message: &str) -> &'a serde_json::Value {
        events
            .iter()
            .find(|e| e.pointer("/fields/message").and_then(|m| m.as_str()) == Some(message))
            .unwrap_or_else(|| panic!("expected event with message {message:?}; got {events:#?}"))
    }

    /// Mirrors `src/scan/logs.rs::tests::event_runs_in_log_scan`.
    fn event_runs_in_log_scan(event: &serde_json::Value) -> bool {
        event
            .get("spans")
            .and_then(|s| s.as_array())
            .map(|spans| {
                spans
                    .iter()
                    .any(|s| s.get("name").and_then(|n| n.as_str()) == Some("log_scan"))
            })
            .unwrap_or(false)
    }

    #[tokio::test]
    async fn tracing_is_chain_neutral_and_records_chunk_dimensions() {
        let writer = CapturingWriter::default();
        {
            let _g = install_subscriber(writer.clone());
            let transport = ScriptedTransport::default();
            // 0..=299 in 100-block chunks = 3 chunks.
            for _ in 0..3 {
                transport.push_success("eth_getLogs", &Vec::<RpcLog>::new());
            }
            let provider = build_provider(transport);
            let filter = Filter::new().from_block(0).to_block(299);

            fetch_logs_chunked(&provider, filter, 100)
                .await
                .expect("happy-path chunked fetch must succeed");
        }

        let events = parse_events(&writer.captured());
        for event in &events {
            assert!(
                event.pointer("/fields/chain").is_none(),
                "fetch_logs_chunked event must not carry chain as a direct field: {event}"
            );
            assert!(
                !event_runs_in_log_scan(event),
                "fetch_logs_chunked must not route through the chain-bearing log_scan span: {event}"
            );
        }

        let start = find_event(&events, "Starting log scan");
        assert_eq!(
            start.pointer("/fields/chunk_size").and_then(|v| v.as_u64()),
            Some(100),
            "start event must record chunk_size for capacity-planning dashboards: {start}"
        );
        assert_eq!(
            start.pointer("/fields/num_chunks").and_then(|v| v.as_u64()),
            Some(3),
            "start event must record num_chunks for capacity-planning dashboards: {start}"
        );
    }
}
