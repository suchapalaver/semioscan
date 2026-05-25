// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Block-range log scanner.
//!
//! [`LogScanner`] is the single implementation of the chunked log-scanning
//! loop used internally by `EventScanner`, `GasCostCalculator`, and
//! `CombinedCalculator`. It iterates a block range in chain-sized chunks,
//! issues `eth_getLogs` for each chunk, applies a configured rate-limit
//! delay between calls, and lets the caller decide per-chunk error policy
//! via a closure.

use alloy_chains::NamedChain;
use alloy_network::{Ethereum, Network};
use alloy_primitives::BlockNumber;
use alloy_provider::Provider;
use alloy_rpc_types::{Filter, Log};
use alloy_transport::TransportError;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, Instrument};

use crate::config::SemioscanConfig;

/// Block-range log scanner that chunks `eth_getLogs` calls and applies
/// chain-specific rate limiting.
///
/// The chunk size and inter-chunk delay are read from [`SemioscanConfig`] per
/// chain. Per-chunk error policy is supplied as a closure to
/// [`LogScanner::scan`]: returning `None` continues with the next chunk,
/// returning `Some(err)` aborts the scan with that error.
///
/// This type is crate-internal. External consumers reach the same
/// functionality through [`crate::events::EventScanner`] (which fixes the
/// policy to continue-on-error) or through `GasCostCalculator` /
/// `CombinedCalculator` / `PriceCalculator`, each of which picks the
/// per-chunk error policy that matches its domain.
pub struct LogScanner<P, N: Network = Ethereum> {
    provider: P,
    config: SemioscanConfig,
    _network: PhantomData<fn() -> N>,
}

impl<P, N> LogScanner<P, N>
where
    N: Network,
    P: Provider<N>,
{
    /// Create a new scanner over the given provider and config.
    pub fn new(provider: P, config: SemioscanConfig) -> Self {
        Self {
            provider,
            config,
            _network: PhantomData,
        }
    }

    /// Scan `start_block..=end_block` for logs matching `filter_template`,
    /// using chunk size and rate-limit delay resolved from [`SemioscanConfig`]
    /// for `chain`. Tracing events emitted by the scan are tagged with
    /// `chain = <chain>` via a parent span so per-chain dashboards can filter
    /// them.
    ///
    /// The block range is split into chunks of `config.get_max_block_range(chain)`
    /// blocks. For each chunk, `filter_template` is cloned and stamped with the
    /// chunk's `from_block`/`to_block`, then `eth_getLogs` is issued. Between
    /// chunks, `config.get_rate_limit_delay(chain)` is applied (when set).
    ///
    /// On a per-chunk transport error, `on_chunk_error` is called:
    /// - `None`  — log the error and continue with the next chunk.
    /// - `Some(err)` — abort the scan and return `Err(err)`.
    ///
    /// Callers that have no chain to attribute the scan to (e.g.
    /// [`crate::fetch_logs_chunked`]) should use [`LogScanner::scan_raw`]
    /// instead, which omits the chain dimension entirely rather than picking
    /// a sentinel.
    ///
    /// # Panics
    ///
    /// Panics if `config.get_max_block_range(chain)` returns a zero
    /// `MaxBlockRange` — i.e. the caller built the config with
    /// `SemioscanConfigBuilder::chain_max_blocks(chain, 0)`. A zero chunk size
    /// would otherwise cause the loop to spin without making progress.
    pub async fn scan<E, F>(
        &self,
        chain: NamedChain,
        filter_template: Filter,
        start_block: BlockNumber,
        end_block: BlockNumber,
        on_chunk_error: F,
    ) -> Result<Vec<Log>, E>
    where
        F: FnMut(TransportError) -> Option<E>,
    {
        let chunk_size = self.config.get_max_block_range(chain).as_u64();
        assert!(
            chunk_size > 0,
            "chunk size for {chain:?} must be > 0; got 0 from SemioscanConfig"
        );
        let rate_limit = self.config.get_rate_limit_delay(chain);

        self.scan_raw(
            chunk_size,
            rate_limit,
            filter_template,
            start_block,
            end_block,
            on_chunk_error,
        )
        .instrument(tracing::debug_span!("log_scan", chain = %chain))
        .await
    }

    /// Chain-neutral entrypoint that drives the same chunked scan as
    /// [`LogScanner::scan`] but takes `chunk_size` and `rate_limit` directly
    /// rather than resolving them from a chain key.
    ///
    /// Tracing events emitted here carry the structural shape of the scan
    /// (`start_block`, `end_block`, `chunk_size`, `num_chunks`,
    /// `current_block`, `to_block`, `logs_count`, `total_logs`) but no
    /// `chain` field. Callers that do have a chain in hand should reach
    /// [`LogScanner::scan`] so the chain is attached as a parent-span field.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size` is 0.
    pub async fn scan_raw<E, F>(
        &self,
        chunk_size: u64,
        rate_limit: Option<Duration>,
        filter_template: Filter,
        start_block: BlockNumber,
        end_block: BlockNumber,
        mut on_chunk_error: F,
    ) -> Result<Vec<Log>, E>
    where
        F: FnMut(TransportError) -> Option<E>,
    {
        assert!(chunk_size > 0, "chunk_size must be > 0");

        let num_chunks = if start_block > end_block {
            0
        } else {
            (end_block - start_block)
                .saturating_add(1)
                .div_ceil(chunk_size)
        };

        debug!(
            start_block,
            end_block, chunk_size, num_chunks, "Starting log scan"
        );

        let mut all_logs = Vec::new();
        let mut current_block = start_block;

        while current_block <= end_block {
            // Subtract 1 from `chunk_size` BEFORE the saturating add. Doing
            // `saturating_add(chunk_size).saturating_sub(1)` instead would let
            // the add saturate at `u64::MAX`, after which `-1` produces
            // `MAX - 1 < current_block` when `current_block == u64::MAX`,
            // making the loop fail to advance. (`chunk_size > 0` is asserted
            // above, so the subtraction never underflows the chunk semantics.)
            let to_block = current_block.saturating_add(chunk_size - 1).min(end_block);

            let filter = filter_template
                .clone()
                .from_block(current_block)
                .to_block(to_block);

            debug!(current_block, to_block, "Fetching logs for chunk");

            match self.provider.get_logs(&filter).await {
                Ok(logs) => {
                    debug!(
                        logs_count = logs.len(),
                        current_block, to_block, "Fetched logs for block range"
                    );
                    all_logs.extend(logs);
                }
                Err(e) => {
                    error!(
                        ?e,
                        %current_block,
                        %to_block,
                        "Error fetching logs in range"
                    );
                    if let Some(mapped) = on_chunk_error(e) {
                        return Err(mapped);
                    }
                }
            }

            // `checked_add` rather than `saturating_add` here: when `to_block`
            // is already `u64::MAX` the scan has consumed the entire address
            // space and must terminate, otherwise the loop guard
            // `current_block <= end_block` (with `end_block == u64::MAX`)
            // would hold forever.
            current_block = match to_block.checked_add(1) {
                Some(next) => next,
                None => break,
            };

            if let Some(delay) = rate_limit {
                if current_block <= end_block {
                    debug!(delay_ms = delay.as_millis(), "Applying rate limit delay");
                    sleep(delay).await;
                }
            }
        }

        debug!(total_logs = all_logs.len(), "Finished log scan");

        Ok(all_logs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SemioscanConfigBuilder;
    use alloy_json_rpc as j;
    use alloy_primitives::{Address, B256};
    use alloy_provider::{ProviderBuilder, RootProvider};
    use alloy_rpc_client::RpcClient;
    use alloy_rpc_types::Log as RpcLog;
    use alloy_transport::{TransportErrorKind, TransportFut, TransportResult};
    use std::{
        borrow::Cow,
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex},
        task::{Context, Poll},
        time::{Duration, Instant},
    };

    /// Test transport that returns queued JSON-RPC responses per method and
    /// records the wall-clock instant of each request. Modeled on the
    /// `MethodResponseTransport` in `retrieval::calculator::tests`.
    #[derive(Clone, Default)]
    struct ScriptedTransport {
        responses: Arc<Mutex<HashMap<String, VecDeque<j::ResponsePayload>>>>,
        call_log: Arc<Mutex<Vec<Instant>>>,
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

        fn call_count(&self) -> usize {
            self.call_log.lock().expect("call_log lock").len()
        }

        fn call_instants(&self) -> Vec<Instant> {
            self.call_log.lock().expect("call_log lock").clone()
        }

        fn map_request(&self, request: j::SerializedRequest) -> TransportResult<j::Response> {
            self.call_log
                .lock()
                .expect("call_log lock")
                .push(Instant::now());

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

    fn empty_log_response() -> Vec<RpcLog> {
        Vec::new()
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

    /// Config that yields a 100-block chunk size and no rate-limit delay for
    /// Arbitrum, isolating chunk-boundary tests from timing concerns.
    fn config_with_chunk_size(chunk: u64) -> SemioscanConfig {
        SemioscanConfigBuilder::with_defaults()
            .chain_max_blocks(NamedChain::Arbitrum, chunk)
            .build()
    }

    #[tokio::test]
    async fn fail_fast_aborts_on_first_chunk_error() {
        let transport = ScriptedTransport::default();
        // First chunk errors; second chunk would succeed but must never be issued.
        transport.push_failure("eth_getLogs", "rpc unavailable");
        transport.push_success("eth_getLogs", &empty_log_response());

        let provider = build_provider(transport.clone());
        let scanner = LogScanner::new(provider, config_with_chunk_size(100));

        let result = scanner
            .scan::<&'static str, _>(NamedChain::Arbitrum, Filter::new(), 0, 199, |_| {
                Some("failed")
            })
            .await;

        assert_eq!(result, Err("failed"));
        assert_eq!(
            transport.call_count(),
            1,
            "second chunk must not be attempted on fail-fast"
        );
    }

    #[tokio::test]
    async fn continue_on_error_returns_logs_from_successful_chunks() {
        let transport = ScriptedTransport::default();
        // chunk 0..=99: ok with one log
        transport.push_success("eth_getLogs", &vec![dummy_log()]);
        // chunk 100..=199: fails — should be skipped, not propagated
        transport.push_failure("eth_getLogs", "transient");
        // chunk 200..=299: ok with one log
        transport.push_success("eth_getLogs", &vec![dummy_log()]);

        let provider = build_provider(transport.clone());
        let scanner = LogScanner::new(provider, config_with_chunk_size(100));

        let logs = scanner
            .scan::<std::convert::Infallible, _>(
                NamedChain::Arbitrum,
                Filter::new(),
                0,
                299,
                |_| None,
            )
            .await
            .expect("continue policy never returns error");

        assert_eq!(logs.len(), 2, "logs from failing chunk must be skipped");
        assert_eq!(transport.call_count(), 3);
    }

    #[tokio::test]
    async fn range_smaller_than_chunk_issues_single_call() {
        let transport = ScriptedTransport::default();
        transport.push_success("eth_getLogs", &empty_log_response());

        let provider = build_provider(transport.clone());
        let scanner = LogScanner::new(provider, config_with_chunk_size(100));

        scanner
            .scan::<std::convert::Infallible, _>(
                NamedChain::Arbitrum,
                Filter::new(),
                10,
                42,
                |_| None,
            )
            .await
            .unwrap();

        assert_eq!(transport.call_count(), 1);
    }

    #[tokio::test]
    async fn range_equal_to_chunk_issues_single_call() {
        let transport = ScriptedTransport::default();
        transport.push_success("eth_getLogs", &empty_log_response());

        let provider = build_provider(transport.clone());
        let scanner = LogScanner::new(provider, config_with_chunk_size(100));

        scanner
            .scan::<std::convert::Infallible, _>(NamedChain::Arbitrum, Filter::new(), 0, 99, |_| {
                None
            })
            .await
            .unwrap();

        assert_eq!(transport.call_count(), 1);
    }

    #[tokio::test]
    async fn multi_chunk_range_issues_one_call_per_chunk() {
        let transport = ScriptedTransport::default();
        for _ in 0..3 {
            transport.push_success("eth_getLogs", &empty_log_response());
        }

        let provider = build_provider(transport.clone());
        let scanner = LogScanner::new(provider, config_with_chunk_size(100));

        scanner
            .scan::<std::convert::Infallible, _>(
                NamedChain::Arbitrum,
                Filter::new(),
                0,
                299,
                |_| None,
            )
            .await
            .unwrap();

        assert_eq!(transport.call_count(), 3);
    }

    #[tokio::test]
    async fn rate_limit_delay_applied_between_chunks() {
        let transport = ScriptedTransport::default();
        transport.push_success("eth_getLogs", &empty_log_response());
        transport.push_success("eth_getLogs", &empty_log_response());

        let provider = build_provider(transport.clone());
        let delay = Duration::from_millis(120);
        let config = SemioscanConfigBuilder::with_defaults()
            .chain_max_blocks(NamedChain::Arbitrum, 100)
            .chain_rate_limit(NamedChain::Arbitrum, delay)
            .build();
        let scanner = LogScanner::new(provider, config);

        scanner
            .scan::<std::convert::Infallible, _>(
                NamedChain::Arbitrum,
                Filter::new(),
                0,
                199,
                |_| None,
            )
            .await
            .unwrap();

        let instants = transport.call_instants();
        assert_eq!(instants.len(), 2);
        let gap = instants[1].duration_since(instants[0]);
        assert!(
            gap >= delay,
            "expected at least {delay:?} between chunks, observed {gap:?}"
        );
    }

    #[tokio::test]
    async fn no_rate_limit_delay_after_final_chunk() {
        let transport = ScriptedTransport::default();
        transport.push_success("eth_getLogs", &empty_log_response());

        let provider = build_provider(transport.clone());
        let config = SemioscanConfigBuilder::with_defaults()
            .chain_max_blocks(NamedChain::Arbitrum, 100)
            .chain_rate_limit(NamedChain::Arbitrum, Duration::from_secs(60))
            .build();
        let scanner = LogScanner::new(provider, config);

        let started = Instant::now();
        scanner
            .scan::<std::convert::Infallible, _>(NamedChain::Arbitrum, Filter::new(), 0, 50, |_| {
                None
            })
            .await
            .unwrap();
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "single-chunk scan should not sleep on the trailing delay; took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn scan_terminates_when_end_block_is_u64_max() {
        // Cover the entire u64 range in two oversized chunks. The bug being
        // guarded against was that `current_block = to_block.saturating_add(1)`
        // would saturate at `u64::MAX`, leaving the loop guard
        // `current_block <= end_block` permanently true.
        let transport = ScriptedTransport::default();
        transport.push_success("eth_getLogs", &empty_log_response());
        transport.push_success("eth_getLogs", &empty_log_response());

        let provider = build_provider(transport.clone());
        let scanner = LogScanner::new(
            provider,
            SemioscanConfigBuilder::with_defaults()
                .chain_max_blocks(NamedChain::Arbitrum, u64::MAX / 2 + 1)
                .build(),
        );

        scanner
            .scan::<std::convert::Infallible, _>(
                NamedChain::Arbitrum,
                Filter::new(),
                0,
                u64::MAX,
                |_| None,
            )
            .await
            .expect("scan must terminate even when end_block == u64::MAX");

        assert_eq!(transport.call_count(), 2);
    }

    #[tokio::test]
    async fn continue_on_error_returns_empty_when_every_chunk_fails() {
        let transport = ScriptedTransport::default();
        for _ in 0..3 {
            transport.push_failure("eth_getLogs", "down");
        }

        let provider = build_provider(transport.clone());
        let scanner = LogScanner::new(provider, config_with_chunk_size(100));

        let logs = scanner
            .scan::<std::convert::Infallible, _>(
                NamedChain::Arbitrum,
                Filter::new(),
                0,
                299,
                |_| None,
            )
            .await
            .expect("continue policy must not surface chunk errors");

        assert!(logs.is_empty(), "all chunks failed; logs vec must be empty");
        assert_eq!(
            transport.call_count(),
            3,
            "every chunk must still be attempted"
        );
    }

    #[tokio::test]
    async fn rate_limit_delay_applied_between_failed_chunks_under_continue() {
        // A continue-on-error scan must still pace itself between chunks
        // even when chunks fail — otherwise an outage at the provider would
        // turn into an instant retry storm.
        let transport = ScriptedTransport::default();
        transport.push_failure("eth_getLogs", "down");
        transport.push_failure("eth_getLogs", "down");

        let provider = build_provider(transport.clone());
        let delay = Duration::from_millis(120);
        let config = SemioscanConfigBuilder::with_defaults()
            .chain_max_blocks(NamedChain::Arbitrum, 100)
            .chain_rate_limit(NamedChain::Arbitrum, delay)
            .build();
        let scanner = LogScanner::new(provider, config);

        scanner
            .scan::<std::convert::Infallible, _>(
                NamedChain::Arbitrum,
                Filter::new(),
                0,
                199,
                |_| None,
            )
            .await
            .unwrap();

        let instants = transport.call_instants();
        assert_eq!(instants.len(), 2);
        let gap = instants[1].duration_since(instants[0]);
        assert!(
            gap >= delay,
            "rate-limit delay must apply between failed chunks; observed {gap:?}"
        );
    }

    #[tokio::test]
    async fn fail_fast_aborts_on_second_chunk_when_first_succeeds() {
        // The fail_fast_aborts_on_first_chunk_error test covered the first
        // chunk; this confirms the same policy works mid-stream, and that
        // the closure is given the actual transport error from the failing
        // chunk (not from a previous successful one).
        let transport = ScriptedTransport::default();
        transport.push_success("eth_getLogs", &vec![dummy_log()]);
        transport.push_failure("eth_getLogs", "boom on chunk two");
        transport.push_success("eth_getLogs", &empty_log_response());

        let provider = build_provider(transport.clone());
        let scanner = LogScanner::new(provider, config_with_chunk_size(100));

        let result = scanner
            .scan::<&'static str, _>(NamedChain::Arbitrum, Filter::new(), 0, 299, |_| {
                Some("aborted on second chunk")
            })
            .await;

        assert_eq!(result, Err("aborted on second chunk"));
        assert_eq!(
            transport.call_count(),
            2,
            "fail-fast must not attempt chunks after the failure"
        );
    }

    #[tokio::test]
    async fn event_scanner_wrapper_skips_failed_chunks() {
        // Integration test: confirm the public EventScanner wrapper actually
        // routes through LogScanner with continue-on-error policy. Without
        // this, a future refactor could silently flip the policy.
        use crate::events::EventScanner;

        let transport = ScriptedTransport::default();
        transport.push_success("eth_getLogs", &vec![dummy_log()]);
        transport.push_failure("eth_getLogs", "transient");
        transport.push_success("eth_getLogs", &vec![dummy_log(), dummy_log()]);

        let provider = build_provider(transport.clone());
        let scanner = EventScanner::new(provider, config_with_chunk_size(100));

        let logs = scanner
            .scan(NamedChain::Arbitrum, Filter::new(), 0, 299)
            .await
            .expect("EventScanner must surface continue-on-error semantics");

        assert_eq!(logs.len(), 3, "logs from surviving chunks must be returned");
        assert_eq!(transport.call_count(), 3);
    }

    #[tokio::test]
    #[should_panic(expected = "chunk size for")]
    async fn scan_panics_on_zero_chunk_size() {
        let transport = ScriptedTransport::default();
        let provider = build_provider(transport);
        let scanner = LogScanner::new(
            provider,
            SemioscanConfigBuilder::with_defaults()
                .chain_max_blocks(NamedChain::Arbitrum, 0)
                .build(),
        );

        let _ = scanner
            .scan::<std::convert::Infallible, _>(NamedChain::Arbitrum, Filter::new(), 0, 10, |_| {
                None
            })
            .await;
    }

    /// JSON-line tracing capture for asserting on event fields. Each
    /// formatted event is appended as a UTF-8 line; tests inspect the
    /// flattened transcript with substring checks.
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

    /// Parses captured JSON-line tracing output into one `serde_json::Value`
    /// per event. Tests use structured field/span inspection instead of
    /// substring searches so the assertions stay tight under future
    /// formatter changes or ambient parent spans.
    fn parse_events(captured: &str) -> Vec<serde_json::Value> {
        captured
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str::<serde_json::Value>(l).expect("tracing line is JSON"))
            .collect()
    }

    /// Returns the start-of-scan event, identified by its message field.
    fn find_event<'a>(events: &'a [serde_json::Value], message: &str) -> &'a serde_json::Value {
        events
            .iter()
            .find(|e| e.pointer("/fields/message").and_then(|m| m.as_str()) == Some(message))
            .unwrap_or_else(|| panic!("expected event with message {message:?}; got {events:#?}"))
    }

    /// Returns true iff `event` is run inside a `log_scan` span — the
    /// chain-bearing span attached by `LogScanner::scan`. Iterates the
    /// `spans` list directly rather than scanning the raw JSON for the
    /// substring `"chain"`, which would also match unrelated
    /// `chain_id`-style fields in future trace shapes.
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
    async fn scan_raw_tracing_carries_chunk_size_and_num_chunks_without_chain() {
        let writer = CapturingWriter::default();
        {
            let _g = install_subscriber(writer.clone());
            let transport = ScriptedTransport::default();
            for _ in 0..3 {
                transport.push_success("eth_getLogs", &empty_log_response());
            }
            let provider = build_provider(transport);
            let scanner = LogScanner::new(provider, config_with_chunk_size(100));

            scanner
                .scan_raw::<std::convert::Infallible, _>(100, None, Filter::new(), 0, 299, |_| None)
                .await
                .expect("scan_raw happy path");
        }

        let events = parse_events(&writer.captured());
        for event in &events {
            assert!(
                event.pointer("/fields/chain").is_none(),
                "scan_raw event must not carry chain as a direct field: {event}"
            );
            assert!(
                !event_runs_in_log_scan(event),
                "scan_raw event must not run inside a log_scan span: {event}"
            );
        }

        let start = find_event(&events, "Starting log scan");
        assert_eq!(
            start.pointer("/fields/chunk_size").and_then(|v| v.as_u64()),
            Some(100),
            "start event must record chunk_size for capacity planning: {start}"
        );
        assert_eq!(
            start.pointer("/fields/num_chunks").and_then(|v| v.as_u64()),
            Some(3),
            "start event must record num_chunks for capacity planning: {start}"
        );
    }

    #[tokio::test]
    async fn scan_tracing_tags_events_with_chain_via_span() {
        let writer = CapturingWriter::default();
        {
            let _g = install_subscriber(writer.clone());
            let transport = ScriptedTransport::default();
            transport.push_success("eth_getLogs", &empty_log_response());
            let provider = build_provider(transport);
            let scanner = LogScanner::new(provider, config_with_chunk_size(100));

            scanner
                .scan::<std::convert::Infallible, _>(
                    NamedChain::Arbitrum,
                    Filter::new(),
                    0,
                    50,
                    |_| None,
                )
                .await
                .expect("scan happy path");
        }

        let events = parse_events(&writer.captured());
        let start = find_event(&events, "Starting log scan");
        assert!(
            event_runs_in_log_scan(start),
            "scan must enter a log_scan span: {start}"
        );

        let chain_field = start
            .get("spans")
            .and_then(|s| s.as_array())
            .and_then(|spans| spans.iter().find(|s| s.get("name").is_some()))
            .and_then(|span| span.get("chain"))
            .and_then(|c| c.as_str())
            .expect("log_scan span must expose a chain field");
        assert!(
            chain_field.to_lowercase().contains("arbitrum"),
            "log_scan span must carry the supplied NamedChain; got {chain_field:?}"
        );
    }
}
