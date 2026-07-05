// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
// SPDX-FileCopyrightText: 2026 Joseph Livesey <jlivesey@gmail.com>
//
// SPDX-License-Identifier: Apache-2.0

use alloy_chains::NamedChain;
use alloy_primitives::{Address, BlockNumber, B256};
use alloy_provider::Provider;
use serde::Serialize;
use std::sync::Mutex;
use tracing::{error, info, warn};

use crate::config::SemioscanConfig;
use crate::errors::PriceCalculationError;
use crate::price::aggregator::PriceAggregator;
use crate::price::cache::PriceCache;
use crate::price::decimals::{TokenDecimalsCache, TokenMetadataProvider};
use crate::price::extractor::extract_swaps;
use crate::price::normalize::{involves_pair, normalize_against_pair, normalize_swap};
use crate::price::scanner::SwapLogScanner;
use crate::price::{PriceSource, SwapData};
use crate::{NormalizedAmount, TokenDecimals, TokenPrice, TransactionCount, UsdValue};

/// Aggregate price totals for a single token over a block range.
#[derive(Debug, Clone, Serialize)]
pub struct TokenPriceResult {
    pub token_address: Address,
    pub total_token_amount: NormalizedAmount,
    pub total_usdc_amount: UsdValue,
    pub transaction_count: TransactionCount,
}

impl Default for TokenPriceResult {
    fn default() -> Self {
        Self {
            token_address: Address::ZERO,
            total_token_amount: NormalizedAmount::ZERO,
            total_usdc_amount: UsdValue::ZERO,
            transaction_count: TransactionCount::ZERO,
        }
    }
}

impl TokenPriceResult {
    pub fn new(token_address: Address) -> Self {
        Self {
            token_address,
            total_token_amount: NormalizedAmount::ZERO,
            total_usdc_amount: UsdValue::ZERO,
            transaction_count: TransactionCount::ZERO,
        }
    }

    pub(crate) fn add_swap(&mut self, token_amount: f64, usdc_amount: f64) {
        self.total_token_amount += NormalizedAmount::new(token_amount);
        self.total_usdc_amount += UsdValue::new(usdc_amount);
        self.transaction_count += TransactionCount::new(1);
    }

    /// Get the average price of the token
    pub fn get_average_price(&self) -> TokenPrice {
        if self.total_token_amount.is_zero() {
            return TokenPrice::ZERO;
        }
        TokenPrice::new(self.total_usdc_amount.as_f64() / self.total_token_amount.as_f64())
    }

    /// Merge two price results together
    pub fn merge(&mut self, other: &Self) {
        self.total_token_amount += other.total_token_amount;
        self.total_usdc_amount += other.total_usdc_amount;
        self.transaction_count += other.transaction_count;
    }

    /// Get the total token amount
    pub fn total_token_amount(&self) -> NormalizedAmount {
        self.total_token_amount
    }

    /// Get the total USDC amount
    pub fn total_usdc_amount(&self) -> UsdValue {
        self.total_usdc_amount
    }

    /// Get the transaction count
    pub fn transaction_count(&self) -> TransactionCount {
        self.transaction_count
    }
}

/// A single raw swap with normalized amounts and transaction metadata.
///
/// This struct provides per-transaction granularity for swap data,
/// useful when you need individual swap details rather than aggregated totals.
#[derive(Debug, Clone, Serialize)]
pub struct RawSwapResult {
    /// The raw swap data from the DEX event
    pub swap: SwapData,
    /// Normalized amount of input token (accounting for decimals)
    pub normalized_token_in_amount: NormalizedAmount,
    /// Normalized amount of output token (accounting for decimals)
    pub normalized_token_out_amount: NormalizedAmount,
    /// Decimals for the input token
    pub token_in_decimals: TokenDecimals,
    /// Decimals for the output token
    pub token_out_decimals: TokenDecimals,
}

impl RawSwapResult {
    /// Get the transaction hash if available
    pub fn tx_hash(&self) -> Option<B256> {
        self.swap.tx_hash
    }

    /// Get the block number if available
    pub fn block_number(&self) -> Option<BlockNumber> {
        self.swap.block_number
    }

    /// Get the sender address if available
    pub fn sender(&self) -> Option<Address> {
        self.swap.sender
    }
}

/// Outcome of scanning a single gap.
///
/// `degraded` is `true` when at least one relevant swap was dropped because
/// its token decimals could not be fetched. A degraded scan yields a zeroed
/// or partial aggregate that reflects a transient RPC failure rather than the
/// real on-chain activity, so the orchestration must not cache it as
/// authoritative.
struct GapScan {
    result: TokenPriceResult,
    degraded: bool,
}

/// Calculates token prices from blockchain swap events using a configurable price source.
///
/// This calculator fetches swap events from the blockchain, extracts price information,
/// and provides caching to minimize RPC calls. It's designed to work with any DEX protocol
/// through the [`PriceSource`] trait.
///
/// # Performance with CallBatchLayer
///
/// For optimal performance, construct your provider with Alloy's `CallBatchLayer`.
/// This automatically batches concurrent `eth_call` requests (like token decimals
/// fetches) into a single Multicall3 RPC request:
///
/// ```rust,ignore
/// use alloy_provider::{layers::CallBatchLayer, ProviderBuilder};
/// use std::time::Duration;
///
/// let provider = ProviderBuilder::new()
///     .layer(CallBatchLayer::new().wait(Duration::from_millis(10)))
///     .connect_http(rpc_url);
///
/// let calculator = PriceCalculator::new(provider, chain, usdc_address, price_source);
/// ```
///
/// Without `CallBatchLayer`, the calculator still benefits from parallel fetching
/// but each call is sent as a separate RPC request.
///
/// # Caching and Thread Safety
///
/// The calculator uses an internal cache (protected by a mutex) to store price results
/// for block ranges. This cache is thread-safe and can be shared across async tasks.
///
/// **Panic Behavior**: If a panic occurs while holding the cache mutex lock, the mutex
/// will become "poisoned" and subsequent operations will panic with a descriptive error
/// message. This is an intentional fail-fast behavior, as mutex poisoning indicates a
/// serious bug in the price calculation logic that should be investigated.
///
/// # Examples
///
/// See [`PriceCalculator::new`] for usage examples.
pub struct PriceCalculator<P> {
    provider: P,
    price_source: Box<dyn PriceSource>,
    usdc_address: Address,
    chain: NamedChain,
    decimals_cache: TokenDecimalsCache,
    price_cache: Mutex<PriceCache>,
    config: SemioscanConfig,
}

impl<P: Provider + Clone> PriceCalculator<P> {
    /// Create a new PriceCalculator with a custom price source
    ///
    /// # Arguments
    ///
    /// * `provider` - Blockchain provider for querying logs and token data
    /// * `usdc_address` - Address of the stablecoin to calculate prices against
    /// * `price_source` - Implementation of PriceSource trait for extracting swap data
    ///
    /// # Example
    ///
    /// See [`examples/custom_dex_integration.rs`](https://github.com/suchapalaver/semioscan/blob/main/examples/custom_dex_integration.rs)
    /// for a complete `PriceSource` implementation that can be passed in here.
    pub fn new(
        provider: P,
        chain: NamedChain,
        usdc_address: Address,
        price_source: Box<dyn PriceSource>,
    ) -> Self {
        Self::with_config(
            provider,
            chain,
            usdc_address,
            price_source,
            crate::SemioscanConfig::default(),
        )
    }

    /// Create a new PriceCalculator with custom configuration
    ///
    /// # Arguments
    ///
    /// * `provider` - Blockchain provider for querying logs and token data
    /// * `chain` - The blockchain network (used for config lookups)
    /// * `usdc_address` - Address of the stablecoin to calculate prices against
    /// * `price_source` - Implementation of PriceSource trait for extracting swap data
    /// * `config` - Configuration for RPC behavior (block ranges, rate limiting)
    pub fn with_config(
        provider: P,
        chain: NamedChain,
        usdc_address: Address,
        price_source: Box<dyn PriceSource>,
        config: crate::SemioscanConfig,
    ) -> Self {
        Self {
            provider,
            price_source,
            usdc_address,
            chain,
            decimals_cache: TokenDecimalsCache::new(),
            price_cache: Mutex::new(PriceCache::default()),
            config,
        }
    }

    /// Scan, extract, and aggregate swaps for `[gap_start, gap_end]`.
    ///
    /// Uses [`SwapLogScanner`] to fetch logs, [`extract_swaps`] to turn them
    /// into [`SwapData`], [`TokenMetadataProvider::ensure_decimals`] to bulk
    /// prime the decimals cache (one Multicall3 round-trip under
    /// `CallBatchLayer`), then folds normalised target/USDC pairs through a
    /// [`PriceAggregator`].
    async fn process_gap_for_price(
        &mut self,
        token_address: Address,
        gap_start: BlockNumber,
        gap_end: BlockNumber,
    ) -> Result<GapScan, PriceCalculationError> {
        let scanner = SwapLogScanner::new(
            &self.provider,
            self.chain,
            self.price_source.as_ref(),
            self.config.clone(),
        );
        let logs = scanner.scan(gap_start, gap_end).await?;

        info!(
            logs_count = logs.len(),
            gap_start = gap_start,
            gap_end = gap_end,
            "Fetched logs for gap"
        );

        let extracted = extract_swaps(self.price_source.as_ref(), &logs);

        // Only swaps pairing the target token against USDC contribute to the
        // aggregate. Decimals for any other token are irrelevant, so the batch
        // fetch is scoped to exactly those two addresses (a single Multicall3
        // round-trip under `CallBatchLayer`).
        let relevant: Vec<&SwapData> = extracted
            .swaps
            .iter()
            .filter(|swap| involves_pair(swap, token_address, self.usdc_address))
            .collect();

        let mut aggregator = PriceAggregator::new(token_address);
        if relevant.is_empty() {
            return Ok(GapScan {
                result: aggregator.finish(),
                degraded: false,
            });
        }

        let metadata = TokenMetadataProvider::new(&self.provider);
        metadata
            .ensure_decimals(
                &mut self.decimals_cache,
                &[token_address, self.usdc_address],
            )
            .await;

        let mut degraded = false;
        for swap in relevant {
            // A decimals fetch failure skips the swap rather than aborting the
            // whole range, so one unreadable token can't void an entire scan.
            // The drop is recorded so the orchestration declines to cache this
            // gap as authoritative — otherwise a transient failure would freeze
            // a zeroed aggregate in and defeat retries.
            let target_decimals = match metadata
                .get_or_fetch(&mut self.decimals_cache, token_address)
                .await
            {
                Ok(d) => d,
                Err(e) => {
                    error!(error = ?e, "Error processing swap data");
                    degraded = true;
                    continue;
                }
            };
            let usdc_decimals = match metadata
                .get_or_fetch(&mut self.decimals_cache, self.usdc_address)
                .await
            {
                Ok(d) => d,
                Err(e) => {
                    error!(error = ?e, "Error processing swap data");
                    degraded = true;
                    continue;
                }
            };

            if let Some(amounts) = normalize_against_pair(
                swap,
                token_address,
                self.usdc_address,
                target_decimals,
                usdc_decimals,
            ) {
                aggregator.add(&amounts);
            }
        }

        Ok(GapScan {
            result: aggregator.finish(),
            degraded,
        })
    }

    /// Calculate aggregated price totals for `token_address` between
    /// `start_block` and `end_block`.
    ///
    /// Uses the internal range cache to skip already-scanned sub-ranges and
    /// only fetches uncached gaps.
    pub async fn calculate_price_between_blocks(
        &mut self,
        token_address: Address,
        start_block: BlockNumber,
        end_block: BlockNumber,
    ) -> Result<TokenPriceResult, PriceCalculationError> {
        info!(
            token_address = ?token_address,
            start_block = start_block,
            end_block = end_block,
            "Starting price calculation"
        );

        // Check cache and calculate gaps that need to be filled
        let (cached_result, gaps) = {
            let cache = self.price_cache.lock().expect(
                "Price cache mutex poisoned - indicates a panic occurred while holding the lock",
            );
            cache.calculate_gaps(token_address, start_block, end_block)
        };

        // If there are no gaps, we can return the cached result
        if let Some(result) = cached_result.clone() {
            if gaps.is_empty() {
                info!(
                    token_address = ?token_address,
                    "Using complete cached result for block range"
                );
                return Ok(result);
            }
        }

        // Initialize with any cached data or create new result
        let mut price_data = cached_result.unwrap_or_else(|| TokenPriceResult::new(token_address));

        // A gap degraded by a decimals fetch failure must not be cached: doing
        // so would freeze a transient failure in and return the zeroed
        // aggregate on every later call. Track whether any gap degraded so the
        // full-range write-back below is skipped too — it would otherwise
        // re-cache the same degraded data under the whole query window.
        let mut any_degraded = false;

        // Process each gap
        for gap in gaps {
            info!(
                token_address = ?token_address,
                gap_start = gap.start,
                gap_end = gap.end,
                "Processing uncached block range"
            );

            // Process the gap by fetching logs in chunks with rate limiting
            let GapScan {
                result: gap_result,
                degraded,
            } = self
                .process_gap_for_price(token_address, gap.start, gap.end)
                .await?;

            if degraded {
                any_degraded = true;
                warn!(
                    token_address = ?token_address,
                    gap_start = gap.start,
                    gap_end = gap.end,
                    "Gap scan degraded by a token-decimals fetch failure; not caching so the range is rescanned on a later call"
                );
            } else {
                // Cache the gap result
                let mut cache = self.price_cache.lock()
                    .expect("Price cache mutex poisoned - indicates a panic occurred while holding the lock");
                cache.insert(token_address, gap.start, gap.end, gap_result.clone());
            }

            // Merge the gap result with our main result
            price_data.merge(&gap_result);
        }

        // Cache the complete result, unless a gap degraded — a covering
        // write-back would collapse the disjoint segments into one entry that
        // includes the degraded gap and serve it for the whole window.
        if any_degraded {
            warn!(
                token_address = ?token_address,
                start_block = start_block,
                end_block = end_block,
                "Range scan degraded by a token-decimals fetch failure; skipping full-range cache write-back so a later call rescans the affected gap"
            );
        } else {
            let mut cache = self.price_cache.lock().expect(
                "Price cache mutex poisoned - indicates a panic occurred while holding the lock",
            );
            cache.insert(token_address, start_block, end_block, price_data.clone());
        }

        info!(
            token_address = ?token_address,
            total_token_amount = price_data.total_token_amount.as_f64(),
            total_usdc_amount = price_data.total_usdc_amount.as_f64(),
            transaction_count = price_data.transaction_count.as_usize(),
            "Finished price calculation"
        );

        Ok(price_data)
    }

    /// Extract raw swap data per transaction from a block range.
    ///
    /// Unlike [`calculate_price_between_blocks`](Self::calculate_price_between_blocks) which
    /// returns aggregated totals, this method returns individual swap data for each transaction.
    /// This is useful when you need per-transaction granularity, such as for fee calculations
    /// or detailed swap analysis.
    ///
    /// # Arguments
    ///
    /// * `start_block` - The starting block number (inclusive)
    /// * `end_block` - The ending block number (inclusive)
    ///
    /// # Returns
    ///
    /// A vector of `RawSwapResult` containing each swap with normalized amounts and metadata.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let swaps = calculator.extract_raw_swaps(start_block, end_block).await?;
    ///
    /// for swap in swaps {
    ///     println!(
    ///         "Tx: {:?}, Token In: {:?}, Amount: {}",
    ///         swap.tx_hash(),
    ///         swap.swap.token_in,
    ///         swap.normalized_token_in_amount.as_f64()
    ///     );
    /// }
    /// ```
    pub async fn extract_raw_swaps(
        &mut self,
        start_block: BlockNumber,
        end_block: BlockNumber,
    ) -> Result<Vec<RawSwapResult>, PriceCalculationError> {
        info!(
            start_block = start_block,
            end_block = end_block,
            "Extracting raw swaps"
        );

        let scanner = SwapLogScanner::new(
            &self.provider,
            self.chain,
            self.price_source.as_ref(),
            self.config.clone(),
        );
        let logs = scanner.scan(start_block, end_block).await?;

        info!(
            logs_count = logs.len(),
            start_block = start_block,
            end_block = end_block,
            "Fetched logs for raw swap extraction"
        );

        let extracted = extract_swaps(self.price_source.as_ref(), &logs);

        let metadata = TokenMetadataProvider::new(&self.provider);
        metadata
            .ensure_decimals(
                &mut self.decimals_cache,
                &extracted.unique_token_addresses(),
            )
            .await;

        let mut results = Vec::with_capacity(extracted.swaps.len());
        for swap in extracted.swaps {
            let token_in_decimals = match metadata
                .get_or_fetch(&mut self.decimals_cache, swap.token_in)
                .await
            {
                Ok(d) => d,
                Err(e) => {
                    warn!(token = ?swap.token_in, error = ?e, "Failed to get decimals for token_in, skipping swap");
                    continue;
                }
            };

            let token_out_decimals = match metadata
                .get_or_fetch(&mut self.decimals_cache, swap.token_out)
                .await
            {
                Ok(d) => d,
                Err(e) => {
                    warn!(token = ?swap.token_out, error = ?e, "Failed to get decimals for token_out, skipping swap");
                    continue;
                }
            };

            let normalized = normalize_swap(&swap, token_in_decimals, token_out_decimals);

            results.push(RawSwapResult {
                swap,
                normalized_token_in_amount: normalized.token_in_amount,
                normalized_token_out_amount: normalized.token_out_amount,
                token_in_decimals: normalized.token_in_decimals,
                token_out_decimals: normalized.token_out_decimals,
            });
        }

        info!(swap_count = results.len(), "Finished raw swap extraction");

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_json_rpc as j;
    use alloy_primitives::{address, Address, B256};
    use alloy_provider::{ProviderBuilder, RootProvider};
    use alloy_rpc_client::RpcClient;
    use alloy_rpc_types::Log as RpcLog;
    use alloy_transport::{TransportErrorKind, TransportFut, TransportResult};
    use std::{
        borrow::Cow,
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex as StdMutex},
        task::{Context, Poll},
    };

    use crate::{errors::RpcError, price::PriceSourceError, SemioscanConfigBuilder};

    #[derive(Clone, Debug, Default)]
    struct MethodResponseTransport {
        responses: Arc<StdMutex<HashMap<String, VecDeque<j::ResponsePayload>>>>,
        request_counts: Arc<StdMutex<HashMap<String, usize>>>,
    }

    impl MethodResponseTransport {
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

        fn push_failure_msg(&self, method: &str, message: impl Into<Cow<'static, str>>) {
            self.responses
                .lock()
                .expect("responses lock")
                .entry(method.to_string())
                .or_default()
                .push_back(j::ResponsePayload::internal_error_message(message.into()));
        }

        fn request_count(&self, method: &str) -> usize {
            self.request_counts
                .lock()
                .expect("request_counts lock")
                .get(method)
                .copied()
                .unwrap_or_default()
        }

        fn map_request(&self, request: j::SerializedRequest) -> TransportResult<j::Response> {
            let method = request.method().to_string();

            {
                let mut request_counts = self.request_counts.lock().expect("request_counts lock");
                *request_counts.entry(method.clone()).or_default() += 1;
            }

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

    impl tower::Service<j::RequestPacket> for MethodResponseTransport {
        type Response = j::ResponsePacket;
        type Error = alloy_transport::TransportError;
        type Future = TransportFut<'static>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: j::RequestPacket) -> Self::Future {
            Box::pin(self.clone().handle(request))
        }
    }

    struct NoSwapPriceSource {
        router: Address,
    }

    impl PriceSource for NoSwapPriceSource {
        fn router_address(&self) -> Address {
            self.router
        }

        fn event_topics(&self) -> Vec<B256> {
            vec![B256::ZERO]
        }

        fn extract_swap_from_log(
            &self,
            _log: &RpcLog,
        ) -> Result<Option<SwapData>, PriceSourceError> {
            Ok(None)
        }
    }

    fn build_provider(transport: MethodResponseTransport) -> RootProvider {
        ProviderBuilder::default().connect_client(RpcClient::new(transport, false))
    }

    fn dummy_log(address: Address) -> RpcLog {
        RpcLog {
            inner: alloy_primitives::Log {
                address,
                data: alloy_primitives::LogData::new_unchecked(
                    vec![B256::ZERO],
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

    #[tokio::test]
    async fn calculate_price_fails_fast_on_swap_log_chunk_error_and_does_not_cache() {
        let chain = NamedChain::Arbitrum;
        let router = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let token = address!("1111111111111111111111111111111111111111");
        let usdc = address!("2222222222222222222222222222222222222222");
        let transport = MethodResponseTransport::default();

        transport.push_success("eth_getLogs", &vec![dummy_log(router)]);
        transport.push_failure_msg("eth_getLogs", "boom on chunk two");
        transport.push_success("eth_getLogs", &Vec::<RpcLog>::new());
        transport.push_success("eth_getLogs", &Vec::<RpcLog>::new());

        let provider = build_provider(transport.clone());
        let config = SemioscanConfigBuilder::new()
            .chain_max_blocks(chain, 100)
            .build();
        let mut calculator = PriceCalculator::with_config(
            provider,
            chain,
            usdc,
            Box::new(NoSwapPriceSource { router }),
            config,
        );

        let err = calculator
            .calculate_price_between_blocks(token, 0, 199)
            .await
            .expect_err("mid-stream swap-log failure must abort the price calculation");

        assert!(
            matches!(
                err,
                PriceCalculationError::Rpc(RpcError::GetLogsFailed { .. })
            ),
            "chunk transport failure must be surfaced as a getLogs RPC error, got: {err:?}"
        );
        assert_eq!(
            transport.request_count("eth_getLogs"),
            2,
            "the failing calculation should stop after the failing second chunk"
        );

        let result = calculator
            .calculate_price_between_blocks(token, 0, 199)
            .await
            .expect("same range should be rescanned after the transient chunk failure clears");

        assert_eq!(result.transaction_count().as_usize(), 0);
        assert_eq!(
            transport.request_count("eth_getLogs"),
            4,
            "a failed scan must not leave an authoritative cache entry for the range"
        );
    }

    #[test]
    fn test_add_swap_accumulates_amounts() {
        let token = address!("1111111111111111111111111111111111111111");
        let mut result = TokenPriceResult::new(token);

        // Add first swap
        result.add_swap(100.0, 200.0);
        assert_eq!(result.total_token_amount().as_f64(), 100.0);
        assert_eq!(result.total_usdc_amount().as_f64(), 200.0);
        assert_eq!(result.transaction_count().as_usize(), 1);

        // Add second swap
        result.add_swap(50.0, 75.0);
        assert_eq!(result.total_token_amount().as_f64(), 150.0);
        assert_eq!(result.total_usdc_amount().as_f64(), 275.0);
        assert_eq!(result.transaction_count().as_usize(), 2);
    }

    #[test]
    fn test_get_average_price_normal_case() {
        let token = address!("1111111111111111111111111111111111111111");
        let mut result = TokenPriceResult::new(token);

        // Swap 1: 100 tokens for 200 USDC = $2.00 per token
        result.add_swap(100.0, 200.0);
        // Swap 2: 50 tokens for 150 USDC = $3.00 per token
        result.add_swap(50.0, 150.0);

        // Average: 350 USDC / 150 tokens = $2.333... per token
        let avg_price = result.get_average_price();
        assert!((avg_price.as_f64() - 2.333333).abs() < 0.0001);
    }

    #[test]
    fn test_get_average_price_zero_volume() {
        let token = address!("1111111111111111111111111111111111111111");
        let result = TokenPriceResult::new(token);

        // Edge case: no volume should return 0.0, not panic
        assert_eq!(result.get_average_price(), TokenPrice::ZERO);
    }

    #[test]
    fn test_get_average_price_zero_token_amount_after_swaps() {
        let token = address!("1111111111111111111111111111111111111111");
        let mut result = TokenPriceResult::new(token);

        // Edge case: USDC amount but zero token amount
        result.add_swap(0.0, 100.0);
        assert_eq!(result.get_average_price(), TokenPrice::ZERO);
    }

    #[test]
    fn test_merge_combines_results() {
        let token = address!("1111111111111111111111111111111111111111");

        let mut result1 = TokenPriceResult::new(token);
        result1.add_swap(100.0, 200.0);
        result1.add_swap(50.0, 100.0);

        let mut result2 = TokenPriceResult::new(token);
        result2.add_swap(25.0, 50.0);

        result1.merge(&result2);

        assert_eq!(result1.total_token_amount().as_f64(), 175.0);
        assert_eq!(result1.total_usdc_amount().as_f64(), 350.0);
        assert_eq!(result1.transaction_count().as_usize(), 3);
    }

    #[test]
    fn test_merge_with_empty_result() {
        let token = address!("1111111111111111111111111111111111111111");

        let mut result = TokenPriceResult::new(token);
        result.add_swap(100.0, 200.0);

        let empty = TokenPriceResult::new(token);

        result.merge(&empty);

        assert_eq!(result.total_token_amount().as_f64(), 100.0);
        assert_eq!(result.total_usdc_amount().as_f64(), 200.0);
        assert_eq!(result.transaction_count().as_usize(), 1);
    }

    #[test]
    fn test_merge_two_empty_results() {
        let token = address!("1111111111111111111111111111111111111111");

        let mut result1 = TokenPriceResult::new(token);
        let result2 = TokenPriceResult::new(token);

        result1.merge(&result2);

        assert_eq!(result1.total_token_amount().as_f64(), 0.0);
        assert_eq!(result1.total_usdc_amount().as_f64(), 0.0);
        assert_eq!(result1.transaction_count().as_usize(), 0);
        assert_eq!(result1.get_average_price(), TokenPrice::ZERO);
    }

    #[test]
    fn test_large_amounts() {
        let token = address!("1111111111111111111111111111111111111111");
        let mut result = TokenPriceResult::new(token);

        result.add_swap(1_000_000_000.0, 2_000_000_000.0);
        result.add_swap(500_000_000.0, 1_000_000_000.0);

        assert_eq!(result.total_token_amount().as_f64(), 1_500_000_000.0);
        assert_eq!(result.total_usdc_amount().as_f64(), 3_000_000_000.0);
        assert_eq!(result.get_average_price().as_f64(), 2.0);
    }

    #[test]
    fn test_fractional_amounts() {
        let token = address!("1111111111111111111111111111111111111111");
        let mut result = TokenPriceResult::new(token);

        result.add_swap(0.001, 0.002);
        result.add_swap(0.0005, 0.001);

        assert!((result.total_token_amount().as_f64() - 0.0015).abs() < 1e-10);
        assert!((result.total_usdc_amount().as_f64() - 0.003).abs() < 1e-10);
        assert!((result.get_average_price().as_f64() - 2.0).abs() < 1e-6);
    }

    #[test]
    fn test_average_price_calculation() {
        let token = address!("1111111111111111111111111111111111111111");

        let result = TokenPriceResult {
            token_address: token,
            total_token_amount: NormalizedAmount::new(100.0),
            total_usdc_amount: UsdValue::new(200.0),
            transaction_count: TransactionCount::new(5),
        };

        assert_eq!(result.get_average_price().as_f64(), 2.0);
    }

    #[test]
    fn test_average_price_fractional() {
        let token = address!("1111111111111111111111111111111111111111");
        let result = TokenPriceResult {
            token_address: token,
            total_token_amount: NormalizedAmount::new(333.33),
            total_usdc_amount: UsdValue::new(999.99),
            transaction_count: TransactionCount::new(10),
        };

        let price = result.get_average_price();
        assert!(
            (price.as_f64() - 3.0).abs() < 0.01,
            "Expected ~3.0, got {price}",
            price = price.as_f64()
        );
    }

    #[test]
    fn test_price_result_multiple_merges() {
        let token = address!("1111111111111111111111111111111111111111");

        let mut total = TokenPriceResult::new(token);

        let r1 = TokenPriceResult {
            token_address: token,
            total_token_amount: NormalizedAmount::new(10.0),
            total_usdc_amount: UsdValue::new(20.0),
            transaction_count: TransactionCount::new(1),
        };

        let r2 = TokenPriceResult {
            token_address: token,
            total_token_amount: NormalizedAmount::new(20.0),
            total_usdc_amount: UsdValue::new(40.0),
            transaction_count: TransactionCount::new(2),
        };

        let r3 = TokenPriceResult {
            token_address: token,
            total_token_amount: NormalizedAmount::new(30.0),
            total_usdc_amount: UsdValue::new(60.0),
            transaction_count: TransactionCount::new(3),
        };

        total.merge(&r1);
        total.merge(&r2);
        total.merge(&r3);

        assert_eq!(total.total_token_amount().as_f64(), 60.0);
        assert_eq!(total.total_usdc_amount().as_f64(), 120.0);
        assert_eq!(total.transaction_count().as_usize(), 6);
        assert_eq!(total.get_average_price().as_f64(), 2.0);
    }

    #[test]
    fn test_price_calculation_high_precision() {
        let token = address!("1111111111111111111111111111111111111111");

        let result = TokenPriceResult {
            token_address: token,
            total_token_amount: NormalizedAmount::new(0.000001),
            total_usdc_amount: UsdValue::new(0.00000123),
            transaction_count: TransactionCount::new(1),
        };

        let price = result.get_average_price();
        assert!(
            (price.as_f64() - 1.23).abs() < 0.001,
            "Expected ~1.23, got {price}",
            price = price.as_f64()
        );
    }
}
