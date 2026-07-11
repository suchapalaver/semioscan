// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
// SPDX-FileCopyrightText: 2026 Joseph Livesey <jlivesey@gmail.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Tower-based retry layer with exponential backoff for Alloy RPC providers.
//!
//! This module implements a retry layer that automatically retries failed RPC
//! requests with configurable exponential backoff. It integrates with Alloy's
//! transport system via Tower's `Layer` trait.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use alloy_json_rpc::{RequestPacket, ResponsePacket, RpcError};
use alloy_transport::{TransportError, TransportErrorKind};
use tower::Layer;
use tracing::{debug, warn};

/// Default maximum number of retry attempts.
const DEFAULT_MAX_RETRIES: u32 = 3;
/// Default base delay for exponential backoff (100ms).
const DEFAULT_BASE_DELAY_MS: u64 = 100;
/// Default maximum delay between retries (30 seconds).
const DEFAULT_MAX_DELAY_MS: u64 = 30_000;

/// A Tower layer that adds retry logic with exponential backoff to RPC requests.
///
/// This layer wraps each RPC request and automatically retries on transient failures
/// using exponential backoff. The backoff formula is:
///
/// ```text
/// delay = min(base_delay * 2^attempt, max_delay)
/// ```
///
/// # Example
///
/// ```rust,ignore
/// use semioscan::transport::RetryLayer;
/// use alloy_rpc_client::ClientBuilder;
/// use std::time::Duration;
///
/// // Retry up to 3 times with exponential backoff
/// let layer = RetryLayer::new();
///
/// // Or with custom configuration
/// let layer = RetryLayer::builder()
///     .max_retries(5)
///     .base_delay(Duration::from_millis(200))
///     .max_delay(Duration::from_secs(60))
///     .build();
///
/// let client = ClientBuilder::default()
///     .layer(layer)
///     .http(rpc_url);
/// ```
#[derive(Clone, Debug)]
pub struct RetryLayer {
    config: Arc<RetryConfig>,
}

/// Configuration for retry behavior.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (not including the initial request).
    pub max_retries: u32,
    /// Base delay for exponential backoff.
    pub base_delay: Duration,
    /// Maximum delay between retries.
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            base_delay: Duration::from_millis(DEFAULT_BASE_DELAY_MS),
            max_delay: Duration::from_millis(DEFAULT_MAX_DELAY_MS),
        }
    }
}

impl RetryLayer {
    /// Creates a new retry layer with default settings.
    ///
    /// Default settings:
    /// - 3 retry attempts
    /// - 100ms base delay
    /// - 30s maximum delay
    ///
    /// # Example
    ///
    /// ```rust
    /// use semioscan::transport::RetryLayer;
    ///
    /// let layer = RetryLayer::new();
    /// ```
    pub fn new() -> Self {
        Self {
            config: Arc::new(RetryConfig::default()),
        }
    }

    /// Creates a builder for customizing retry configuration.
    ///
    /// # Example
    ///
    /// ```rust
    /// use semioscan::transport::RetryLayer;
    /// use std::time::Duration;
    ///
    /// let layer = RetryLayer::builder()
    ///     .max_retries(5)
    ///     .base_delay(Duration::from_millis(200))
    ///     .build();
    /// ```
    pub fn builder() -> RetryLayerBuilder {
        RetryLayerBuilder::new()
    }

    /// Creates a retry layer with a specific number of retries.
    ///
    /// Uses default base and max delays.
    ///
    /// # Example
    ///
    /// ```rust
    /// use semioscan::transport::RetryLayer;
    ///
    /// // Retry up to 5 times
    /// let layer = RetryLayer::with_max_retries(5);
    /// ```
    pub fn with_max_retries(max_retries: u32) -> Self {
        Self {
            config: Arc::new(RetryConfig {
                max_retries,
                ..Default::default()
            }),
        }
    }

    /// Creates a retry layer with aggressive retry settings.
    ///
    /// This preset uses:
    /// - 5 retry attempts
    /// - 50ms base delay
    /// - 10s maximum delay
    ///
    /// Suitable for high-availability scenarios where quick retries are needed.
    ///
    /// # Example
    ///
    /// ```rust
    /// use semioscan::transport::RetryLayer;
    ///
    /// let layer = RetryLayer::aggressive();
    /// ```
    pub fn aggressive() -> Self {
        Self {
            config: Arc::new(RetryConfig {
                max_retries: 5,
                base_delay: Duration::from_millis(50),
                max_delay: Duration::from_secs(10),
            }),
        }
    }

    /// Creates a retry layer with conservative retry settings.
    ///
    /// This preset uses:
    /// - 3 retry attempts
    /// - 500ms base delay
    /// - 60s maximum delay
    ///
    /// Suitable for scenarios where RPC endpoints may need time to recover.
    ///
    /// # Example
    ///
    /// ```rust
    /// use semioscan::transport::RetryLayer;
    ///
    /// let layer = RetryLayer::conservative();
    /// ```
    pub fn conservative() -> Self {
        Self {
            config: Arc::new(RetryConfig {
                max_retries: 3,
                base_delay: Duration::from_millis(500),
                max_delay: Duration::from_secs(60),
            }),
        }
    }
}

impl Default for RetryLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for RetryLayer {
    type Service = RetryService<S>;

    fn layer(&self, service: S) -> Self::Service {
        RetryService {
            service,
            config: self.config.clone(),
        }
    }
}

/// Builder for configuring a [`RetryLayer`].
#[derive(Clone, Debug, Default)]
pub struct RetryLayerBuilder {
    config: RetryConfig,
}

impl RetryLayerBuilder {
    /// Creates a new builder with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of retry attempts.
    ///
    /// # Arguments
    ///
    /// * `max_retries` - Maximum retries (not including the initial request)
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.config.max_retries = max_retries;
        self
    }

    /// Sets the base delay for exponential backoff.
    ///
    /// The actual delay for attempt `n` will be `min(base_delay * 2^n, max_delay)`.
    pub fn base_delay(mut self, delay: Duration) -> Self {
        self.config.base_delay = delay;
        self
    }

    /// Sets the maximum delay between retries.
    ///
    /// Delays will be capped at this value regardless of the exponential calculation.
    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.config.max_delay = delay;
        self
    }

    /// Builds the configured [`RetryLayer`].
    pub fn build(self) -> RetryLayer {
        RetryLayer {
            config: Arc::new(self.config),
        }
    }
}

/// A Tower service that adds retry logic with exponential backoff.
///
/// This service wraps an inner service and automatically retries failed
/// requests based on the configured retry policy.
#[derive(Clone, Debug)]
pub struct RetryService<S> {
    service: S,
    config: Arc<RetryConfig>,
}

impl<S> tower::Service<RequestPacket> for RetryService<S>
where
    S: tower::Service<RequestPacket, Response = ResponsePacket, Error = TransportError>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: RequestPacket) -> Self::Future {
        let service = self.service.clone();
        let config = self.config.clone();

        Box::pin(async move {
            let mut attempt = 0u32;
            loop {
                let mut service_clone = service.clone();

                match service_clone.call(request.clone()).await {
                    Ok(response) => {
                        if attempt > 0 {
                            debug!(attempt = attempt, "Request succeeded after retry");
                        }
                        return Ok(response);
                    }
                    Err(error) => {
                        if !is_retryable_error(&error) {
                            debug!(
                                error = %error,
                                "Non-retryable error, not retrying"
                            );
                            return Err(error);
                        }

                        if attempt >= config.max_retries {
                            warn!(
                                error = %error,
                                attempts = attempt + 1,
                                "Max retries exceeded"
                            );
                            return Err(error);
                        }

                        let delay = calculate_backoff(attempt, &config);
                        warn!(
                            error = %error,
                            attempt = attempt + 1,
                            max_retries = config.max_retries,
                            delay_ms = delay.as_millis(),
                            "Retryable error, backing off"
                        );

                        tokio::time::sleep(delay).await;
                        attempt += 1;
                    }
                }
            }
        })
    }
}

/// Calculates the backoff duration for a given attempt.
///
/// Uses exponential backoff: `min(base_delay * 2^attempt, max_delay)`
fn calculate_backoff(attempt: u32, config: &RetryConfig) -> Duration {
    let multiplier = 2u64.saturating_pow(attempt);
    let delay_ms = config
        .base_delay
        .as_millis()
        .saturating_mul(multiplier as u128);
    let capped_delay_ms = delay_ms.min(config.max_delay.as_millis()) as u64;
    Duration::from_millis(capped_delay_ms)
}

/// Determines if an error is retryable.
///
/// Returns `true` for transient errors that may succeed on retry:
/// - Transport/connection errors (connection issues, HTTP 5xx, rate limits)
/// - Missing batch responses
/// - Null responses (may be transient)
///
/// Returns `false` for errors that will not benefit from retry:
/// - Serialization errors (request is malformed)
/// - Error responses with non-retryable error codes
fn is_retryable_error(error: &TransportError) -> bool {
    match error {
        // Transport errors - check the inner TransportErrorKind for retry eligibility
        RpcError::Transport(kind) => is_transport_kind_retryable(kind),

        // Serialization errors indicate malformed request - not retryable
        RpcError::SerError(_) => false,

        // Deserialization errors may be transient (malformed response from server)
        RpcError::DeserError { .. } => true,

        // Error responses - check if it's a retryable error code
        RpcError::ErrorResp(err) => err.is_retry_err(),

        // Null response may be a transient issue
        RpcError::NullResp => true,

        // Catch-all for other/future variants
        _ => false,
    }
}

/// Determines if a transport error kind is retryable.
fn is_transport_kind_retryable(kind: &TransportErrorKind) -> bool {
    // TransportErrorKind has its own is_retry_err() method
    kind.is_retry_err()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_json_rpc::{Id, Request, Response, ResponsePayload};
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };
    use tower::Service as _;

    #[derive(Clone, Debug)]
    struct ScriptedRetryService {
        state: Arc<ScriptedRetryState>,
    }

    #[derive(Debug, Default)]
    struct ScriptedRetryState {
        outcomes: Mutex<VecDeque<ScriptedOutcome>>,
        call_count: Mutex<usize>,
    }

    #[derive(Debug)]
    enum ScriptedOutcome {
        Success,
        RetryableError,
        NonRetryableError,
    }

    impl ScriptedRetryService {
        fn new(outcomes: impl IntoIterator<Item = ScriptedOutcome>) -> Self {
            Self {
                state: Arc::new(ScriptedRetryState {
                    outcomes: Mutex::new(outcomes.into_iter().collect()),
                    call_count: Mutex::new(0),
                }),
            }
        }

        fn calls(&self) -> usize {
            *self.state.call_count.lock().expect("call count lock")
        }

        fn next_result(&self) -> Result<ResponsePacket, TransportError> {
            *self.state.call_count.lock().expect("call count lock") += 1;

            match self
                .state
                .outcomes
                .lock()
                .expect("outcomes lock")
                .pop_front()
                .expect("scripted outcome")
            {
                ScriptedOutcome::Success => Ok(success_response()),
                ScriptedOutcome::RetryableError => Err(retryable_error()),
                ScriptedOutcome::NonRetryableError => Err(non_retryable_error()),
            }
        }
    }

    impl tower::Service<RequestPacket> for ScriptedRetryService {
        type Response = ResponsePacket;
        type Error = TransportError;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: RequestPacket) -> Self::Future {
            let result = self.next_result();
            Box::pin(async move { result })
        }
    }

    fn zero_delay_retry_layer(max_retries: u32) -> RetryLayer {
        RetryLayer::builder()
            .max_retries(max_retries)
            .base_delay(Duration::ZERO)
            .max_delay(Duration::ZERO)
            .build()
    }

    fn request_packet() -> RequestPacket {
        Request::new("test_method", Id::Number(1), ())
            .serialize()
            .expect("request should serialize")
            .into()
    }

    fn success_response() -> ResponsePacket {
        ResponsePacket::Single(Response {
            id: Id::Number(1),
            payload: ResponsePayload::Success(
                serde_json::value::RawValue::from_string("true".to_string())
                    .expect("valid raw JSON"),
            ),
        })
    }

    fn retryable_error() -> TransportError {
        TransportErrorKind::http_error(503, "temporarily unavailable".to_string())
    }

    fn non_retryable_error() -> TransportError {
        TransportErrorKind::non_retryable_str("malformed request")
    }

    fn assert_retryable_error(error: TransportError) {
        assert!(
            matches!(
                error,
                RpcError::Transport(TransportErrorKind::HttpError(http_error))
                    if http_error.status == 503
            ),
            "expected retryable HTTP 503 error"
        );
    }

    fn assert_non_retryable_error(error: TransportError) {
        assert!(
            matches!(
                error,
                RpcError::Transport(TransportErrorKind::NonRetryable(_))
            ),
            "expected non-retryable transport error"
        );
    }

    #[test]
    fn test_retry_layer_default() {
        let layer = RetryLayer::new();
        assert_eq!(layer.config.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(
            layer.config.base_delay,
            Duration::from_millis(DEFAULT_BASE_DELAY_MS)
        );
        assert_eq!(
            layer.config.max_delay,
            Duration::from_millis(DEFAULT_MAX_DELAY_MS)
        );
    }

    #[test]
    fn test_retry_layer_builder() {
        let layer = RetryLayer::builder()
            .max_retries(5)
            .base_delay(Duration::from_millis(200))
            .max_delay(Duration::from_secs(60))
            .build();

        assert_eq!(layer.config.max_retries, 5);
        assert_eq!(layer.config.base_delay, Duration::from_millis(200));
        assert_eq!(layer.config.max_delay, Duration::from_secs(60));
    }

    #[test]
    fn test_retry_layer_with_max_retries() {
        let layer = RetryLayer::with_max_retries(10);
        assert_eq!(layer.config.max_retries, 10);
    }

    #[test]
    fn test_retry_layer_aggressive() {
        let layer = RetryLayer::aggressive();
        assert_eq!(layer.config.max_retries, 5);
        assert_eq!(layer.config.base_delay, Duration::from_millis(50));
        assert_eq!(layer.config.max_delay, Duration::from_secs(10));
    }

    #[test]
    fn test_retry_layer_conservative() {
        let layer = RetryLayer::conservative();
        assert_eq!(layer.config.max_retries, 3);
        assert_eq!(layer.config.base_delay, Duration::from_millis(500));
        assert_eq!(layer.config.max_delay, Duration::from_secs(60));
    }

    #[test]
    fn test_calculate_backoff() {
        let config = RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        };

        // Attempt 0: 100ms * 2^0 = 100ms
        assert_eq!(calculate_backoff(0, &config), Duration::from_millis(100));

        // Attempt 1: 100ms * 2^1 = 200ms
        assert_eq!(calculate_backoff(1, &config), Duration::from_millis(200));

        // Attempt 2: 100ms * 2^2 = 400ms
        assert_eq!(calculate_backoff(2, &config), Duration::from_millis(400));

        // Attempt 3: 100ms * 2^3 = 800ms
        assert_eq!(calculate_backoff(3, &config), Duration::from_millis(800));
    }

    #[test]
    fn test_calculate_backoff_capped() {
        let config = RetryConfig {
            max_retries: 10,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
        };

        // Attempt 3: 100ms * 2^3 = 800ms, but capped at 500ms
        assert_eq!(calculate_backoff(3, &config), Duration::from_millis(500));

        // Attempt 10: would be huge, but capped at 500ms
        assert_eq!(calculate_backoff(10, &config), Duration::from_millis(500));
    }

    #[test]
    fn test_calculate_backoff_overflow_protection() {
        let config = RetryConfig {
            max_retries: 100,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
        };

        // Very high attempt number should not overflow, just cap at max_delay
        assert_eq!(calculate_backoff(50, &config), Duration::from_secs(60));
    }

    #[tokio::test]
    async fn retry_service_succeeds_after_retryable_failures() {
        let inner = ScriptedRetryService::new([
            ScriptedOutcome::RetryableError,
            ScriptedOutcome::RetryableError,
            ScriptedOutcome::Success,
        ]);
        let call_counter = inner.clone();
        let mut service = zero_delay_retry_layer(3).layer(inner);

        let response = service
            .call(request_packet())
            .await
            .expect("retryable failures should eventually succeed");

        assert!(matches!(response, ResponsePacket::Single(response) if response.is_success()));
        assert_eq!(call_counter.calls(), 3);
    }

    #[tokio::test]
    async fn retry_service_stops_after_max_retries_are_exhausted() {
        let inner = ScriptedRetryService::new([
            ScriptedOutcome::RetryableError,
            ScriptedOutcome::RetryableError,
            ScriptedOutcome::RetryableError,
        ]);
        let call_counter = inner.clone();
        let mut service = zero_delay_retry_layer(2).layer(inner);

        let error = service
            .call(request_packet())
            .await
            .expect_err("third retryable failure should exhaust max_retries=2");

        assert_retryable_error(error);
        assert_eq!(call_counter.calls(), 3);
    }

    #[tokio::test]
    async fn retry_service_does_not_retry_non_retryable_errors() {
        let inner = ScriptedRetryService::new([
            ScriptedOutcome::NonRetryableError,
            ScriptedOutcome::Success,
        ]);
        let call_counter = inner.clone();
        let mut service = zero_delay_retry_layer(3).layer(inner);

        let error = service
            .call(request_packet())
            .await
            .expect_err("non-retryable failure should stop immediately");

        assert_non_retryable_error(error);
        assert_eq!(call_counter.calls(), 1);
    }
}
