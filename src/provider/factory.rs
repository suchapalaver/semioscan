// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Provider factory functions for creating type-erased providers

use std::time::Duration;

use alloy_network::AnyNetwork;
use alloy_provider::RootProvider;
use alloy_rpc_client::{ClientBuilder, RpcClient};

use crate::errors::RpcError;
use crate::transport::RateLimitLayer;

use super::config::ProviderConfig;
use super::http_client::reqwest_client_with_timeout;
use super::AnyHttpProvider;

/// Resolve the `(rate_limit_per_second, min_delay)` pair into the single
/// rate-limit layer the transport should install, or `None` for unpaced.
///
/// Centralising the dispatch here keeps the HTTP and WS factories from
/// drifting on which axis wins when both are set, and makes the
/// construction-time `tracing::warn!` for the over-specified case fire
/// from one place regardless of transport. Adding a new rate-limit axis
/// is a single edit to this helper rather than a parallel change in
/// every factory.
///
/// Precedence: when both axes are set, `rate_limit_per_second` wins and
/// `min_delay` is dropped with a warn. This matches the documented
/// `ProviderPoolBuilder` precedence and the historical HTTP behaviour.
#[track_caller]
pub(super) fn rate_limit_layer_for(
    rate_limit_per_second: Option<u32>,
    min_delay: Option<Duration>,
) -> Option<RateLimitLayer> {
    match (rate_limit_per_second, min_delay) {
        (Some(rps), Some(_)) => {
            tracing::warn!(
                "Both rate_limit_per_second and min_delay specified, using rate_limit_per_second"
            );
            Some(RateLimitLayer::per_second(rps))
        }
        (Some(rps), None) => Some(RateLimitLayer::per_second(rps)),
        (None, Some(delay)) => Some(RateLimitLayer::with_min_delay(delay)),
        (None, None) => None,
    }
}

/// Build the configured `RpcClient` shared by every HTTP factory.
///
/// Centralizing the `(rate_limit_per_second, min_delay, timeout)` dispatch keeps
/// the type-erased and typed factories from drifting out of sync — every HTTP
/// provider this crate hands out flows through the same matrix. Exposed to
/// the rest of the `provider` module so sibling builders (e.g. the pool
/// factory in #47) can route through the same dispatch instead of growing a
/// parallel matrix of their own.
pub(super) fn build_http_client(config: ProviderConfig) -> Result<RpcClient, RpcError> {
    let url: url::Url = config
        .url
        .parse()
        .map_err(|e| RpcError::ProviderUrlInvalid(format!("{e}")))?;

    let builder = ClientBuilder::default();
    let layer = rate_limit_layer_for(config.rate_limit_per_second, config.min_delay);

    let client = match (layer, config.timeout) {
        (Some(layer), Some(timeout)) => builder
            .layer(layer)
            .http_with_client(reqwest_client_with_timeout(timeout)?, url),
        (Some(layer), None) => builder.layer(layer).http(url),
        (None, Some(timeout)) => {
            builder.http_with_client(reqwest_client_with_timeout(timeout)?, url)
        }
        (None, None) => builder.http(url),
    };

    Ok(client)
}

/// Create an HTTP provider with the given configuration
///
/// This creates a provider using `AnyNetwork` for type erasure, enabling
/// runtime chain selection at the cost of some type safety.
///
/// # Configuration Options
///
/// - Rate limiting: Automatically throttles requests
/// - Timeout: Sets request timeout
///
/// Note: RPC request/response logging is handled natively by alloy's transport
/// layer at DEBUG/TRACE level.
///
/// # Examples
///
/// Basic usage:
/// ```rust,ignore
/// use semioscan::provider::{create_http_provider, ProviderConfig};
///
/// let provider = create_http_provider(
///     ProviderConfig::new("https://eth.llamarpc.com")
/// )?;
/// ```
///
/// With rate limiting:
/// ```rust,ignore
/// use semioscan::provider::{create_http_provider, ProviderConfig};
///
/// let provider = create_http_provider(
///     ProviderConfig::new("https://eth.llamarpc.com")
///         .with_rate_limit(10)
/// )?;
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The URL is malformed
/// - The URL cannot be parsed
pub fn create_http_provider(config: ProviderConfig) -> Result<AnyHttpProvider, RpcError> {
    Ok(RootProvider::<AnyNetwork>::new(build_http_client(config)?))
}

/// Create a WebSocket provider with the given configuration
///
/// WebSocket providers enable real-time subscriptions to blocks, logs, and
/// pending transactions. They're ideal for applications that need low-latency
/// event monitoring.
///
/// # Configuration Options
///
/// Honors the rate-limit axes on [`ProviderConfig`] using the same matrix
/// as the HTTP factories:
///
/// - `rate_limit_per_second` — installs a token-bucket layer that throttles
///   requests to the configured rate.
/// - `min_delay` — installs a minimum-delay layer that guarantees at least
///   the configured gap between consecutive requests; useful for strict
///   upstreams that prefer pacing over bursts.
///
/// If both `rate_limit_per_second` and `min_delay` are set, the rate-limit
/// axis wins and a `tracing::warn!` is emitted so the operator can spot the
/// conflicting configuration. This precedence matches every other transport
/// in the crate.
///
/// `config.timeout` is **not honored** for WebSocket providers: the
/// underlying `alloy_provider::WsConnect` does not expose a per-request
/// timeout knob, so a `timeout` set on the config is dropped at construction
/// with a `tracing::warn!`. If you need a request-level timeout on a WS
/// connection, wrap the calls at the application layer.
///
/// # Note
///
/// This function is async because WebSocket connections require a handshake.
///
/// # Examples
///
/// ```rust,ignore
/// use semioscan::provider::{create_ws_provider, ProviderConfig};
/// use std::time::Duration;
///
/// let provider = create_ws_provider(
///     ProviderConfig::new("wss://eth.llamarpc.com/ws")
///         .with_min_delay(Duration::from_millis(250))
/// ).await?;
///
/// // Subscribe to new blocks
/// let mut stream = provider.subscribe_blocks().await?;
/// while let Some(block) = stream.next().await {
///     println!("New block: {}", block.number);
/// }
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The URL is malformed
/// - The WebSocket connection fails
#[cfg(feature = "ws")]
pub async fn create_ws_provider(
    config: ProviderConfig,
) -> Result<alloy_provider::RootProvider<AnyNetwork>, RpcError> {
    use alloy_provider::WsConnect;

    if config.timeout.is_some() {
        tracing::warn!(
            "ProviderConfig::timeout is ignored for WebSocket providers; \
             alloy_provider::WsConnect does not expose a per-request timeout knob"
        );
    }

    let ws = WsConnect::new(&config.url);

    let builder = ClientBuilder::default();
    let layer = rate_limit_layer_for(config.rate_limit_per_second, config.min_delay);

    let client = match layer {
        Some(layer) => builder
            .layer(layer)
            .ws(ws)
            .await
            .map_err(|e| RpcError::ProviderConnectionFailed(e.to_string()))?,
        None => builder
            .ws(ws)
            .await
            .map_err(|e| RpcError::ProviderConnectionFailed(e.to_string()))?,
    };

    Ok(RootProvider::<AnyNetwork>::new(client))
}

/// Create an HTTP provider with specific network type
///
/// For applications that know the network type at compile time, this function
/// provides better type safety by returning a provider with the specific network.
///
/// # Type Parameters
///
/// - `N`: The network type (e.g., `Ethereum`, `Optimism`, `AnyNetwork`)
///
/// # Configuration Options
///
/// Honors every field on [`ProviderConfig`] the type-erased
/// [`create_http_provider`] does:
///
/// - `rate_limit_per_second` — installs a token-bucket layer that throttles
///   requests to the configured rate.
/// - `min_delay` — installs a minimum-delay layer that guarantees at least
///   the configured gap between consecutive requests; useful for strict
///   upstreams that prefer pacing over bursts.
/// - `timeout` — applied at the HTTP transport (reqwest) layer.
///
/// If both `rate_limit_per_second` and `min_delay` are set, the rate-limit
/// axis wins and a `tracing::warn!` is emitted so the operator can spot the
/// conflicting configuration.
///
/// # Examples
///
/// ```rust,ignore
/// use alloy_network::Ethereum;
/// use semioscan::provider::{create_typed_http_provider, ProviderConfig};
/// use std::time::Duration;
///
/// let provider = create_typed_http_provider::<Ethereum>(
///     ProviderConfig::new("https://eth.llamarpc.com")
///         .with_min_delay(Duration::from_millis(250))
/// )?;
/// ```
pub fn create_typed_http_provider<N>(
    config: ProviderConfig,
) -> Result<alloy_provider::RootProvider<N>, RpcError>
where
    N: alloy_network::Network,
{
    Ok(RootProvider::<N>::new(build_http_client(config)?))
}

/// Quick helper to create a simple HTTP provider without configuration
///
/// This is a convenience function for simple use cases where no rate limiting
/// or logging is needed.
///
/// # Errors
///
/// Returns an error if the URL is invalid
pub fn simple_http_provider(url: &str) -> Result<AnyHttpProvider, RpcError> {
    create_http_provider(ProviderConfig::new(url))
}

/// Quick helper to create a rate-limited HTTP provider
///
/// This is a convenience function that combines URL and rate limiting.
///
/// # Errors
///
/// Returns an error if the URL is invalid
pub fn rate_limited_http_provider(
    url: &str,
    requests_per_second: u32,
) -> Result<AnyHttpProvider, RpcError> {
    create_http_provider(ProviderConfig::new(url).with_rate_limit(requests_per_second))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_http_provider_invalid_url() {
        let result = create_http_provider(ProviderConfig::new("not-a-valid-url"));
        assert!(result.is_err());
    }

    #[test]
    fn test_create_http_provider_valid_url() {
        let result = create_http_provider(ProviderConfig::new("http://localhost:8545"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_http_provider_with_rate_limit() {
        let result =
            create_http_provider(ProviderConfig::new("http://localhost:8545").with_rate_limit(10));
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_http_provider() {
        let result = simple_http_provider("http://localhost:8545");
        assert!(result.is_ok());
    }

    #[test]
    fn test_rate_limited_http_provider() {
        let result = rate_limited_http_provider("http://localhost:8545", 10);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_typed_http_provider() {
        use alloy_network::Ethereum;

        let result =
            create_typed_http_provider::<Ethereum>(ProviderConfig::new("http://localhost:8545"));
        assert!(result.is_ok());
    }

    /// Build-time acceptance check for every `(rate_limit_per_second, min_delay)`
    /// combination on the typed factory. This does NOT by itself prove the
    /// matching rate-limit layer is installed — the pre-#45 buggy code also
    /// returned `Ok` for `(None, Some(delay))` while silently dropping the
    /// `min_delay`. The behavioural contract is covered by the
    /// `typed_provider_min_delay_test` integration test; this test's job is
    /// to keep the dispatch surface from shrinking back to the buggy shape.
    #[test]
    fn typed_http_provider_accepts_full_dispatch_matrix() {
        use alloy_network::Ethereum;
        use std::time::Duration;

        let url = "http://localhost:8545";

        create_typed_http_provider::<Ethereum>(ProviderConfig::new(url)).expect("no rate limiting");

        create_typed_http_provider::<Ethereum>(ProviderConfig::new(url).with_rate_limit(10))
            .expect("rate_limit_per_second");

        create_typed_http_provider::<Ethereum>(
            ProviderConfig::new(url).with_min_delay(Duration::from_millis(250)),
        )
        .expect("min_delay only");

        create_typed_http_provider::<Ethereum>(
            ProviderConfig::new(url)
                .with_rate_limit(5)
                .with_min_delay(Duration::from_millis(250)),
        )
        .expect("both axes");
    }

    /// Build-time acceptance check against the shared builder directly, so
    /// the dispatch matrix stays exercised even if the public wrappers change
    /// shape. See `typed_http_provider_accepts_full_dispatch_matrix` for the
    /// limits of this kind of check and pointers to the behavioural test.
    #[test]
    fn shared_builder_accepts_full_dispatch_matrix() {
        use std::time::Duration;

        let url = "http://localhost:8545";

        build_http_client(ProviderConfig::new(url)).expect("no rate limiting");
        build_http_client(ProviderConfig::new(url).with_rate_limit(10))
            .expect("rate_limit_per_second");
        build_http_client(ProviderConfig::new(url).with_min_delay(Duration::from_millis(250)))
            .expect("min_delay only");
        build_http_client(
            ProviderConfig::new(url)
                .with_rate_limit(5)
                .with_min_delay(Duration::from_millis(250)),
        )
        .expect("both axes");
    }

    /// `rate_limit_layer_for` is the single point of `(rate_limit_per_second,
    /// min_delay)` dispatch shared by the HTTP and WS factories. Drift on
    /// which axis wins, on whether a layer is installed at all, or on
    /// which value reaches the layer would silently change the wire
    /// behaviour of every provider this crate builds — this test pins the
    /// matrix shape so a regression has to touch one assertion per arm.
    ///
    /// Each Some-arm pins the layer's `capacity` field via the derived
    /// `Debug` output: `RateLimitLayer::per_second(rps)` constructs the
    /// underlying state with `capacity = rps`, while
    /// `RateLimitLayer::with_min_delay(_)` always has `capacity = 1`.
    /// Asserting on capacity therefore (a) discriminates the two layer
    /// kinds (a future axis-swap regression that returned
    /// `per_second(delay_ms)` for the `min_delay` arm would land
    /// `capacity > 1` and fail), and (b) catches per-second value
    /// clobbering for the rate-limit arms. The `min_delay` value itself
    /// reaches the layer via `refill_rate`, which is a non-trivial
    /// floating-point quotient and not stably snapshot-friendly; the
    /// end-to-end value pass-through for that axis is covered by the
    /// `typed_provider_min_delay_test` integration test, which observes
    /// real pacing on the wire.
    #[test]
    fn rate_limit_layer_for_covers_full_matrix() {
        use std::time::Duration;

        assert!(
            rate_limit_layer_for(None, None).is_none(),
            "both axes unset must produce no layer"
        );

        let rps_only = rate_limit_layer_for(Some(10), None).expect("rate_limit_per_second alone");
        assert!(
            format!("{rps_only:?}").contains("capacity: 10"),
            "rate_limit_per_second arm must produce a per_second layer with the given budget; got {rps_only:?}"
        );

        let delay_only =
            rate_limit_layer_for(None, Some(Duration::from_millis(250))).expect("min_delay alone");
        assert!(
            format!("{delay_only:?}").contains("capacity: 1"),
            "min_delay arm must produce a single-token (capacity = 1) layer; got {delay_only:?}"
        );

        let both =
            rate_limit_layer_for(Some(5), Some(Duration::from_millis(250))).expect("both axes set");
        assert!(
            format!("{both:?}").contains("capacity: 5"),
            "both-axes arm must keep the per-second budget (rate-limit wins; min_delay dropped); got {both:?}"
        );
    }

    /// Surface check that `create_ws_provider` compiles and runs through
    /// every `(rate_limit_per_second, min_delay)` combination without
    /// shape-level regressions (e.g. an arm reintroducing a one-axis
    /// match and dropping a layer at the type level).
    ///
    /// This does **not** prove the matching rate-limit layer is installed
    /// — the WS factory's `None` arm in the pre-fix shape also returns an
    /// error against this URL while silently dropping `min_delay`. The
    /// behavioural contract for the matrix lives in
    /// `rate_limit_layer_for_covers_full_matrix` above (which the WS
    /// factory routes through) and in the HTTP `typed_provider_min_delay`
    /// integration test (which exercises the same helper end-to-end on a
    /// real transport).
    #[cfg(feature = "ws")]
    #[tokio::test]
    async fn create_ws_provider_accepts_full_dispatch_matrix() {
        use std::time::Duration;

        let url = "not-a-valid-ws-url";

        assert!(
            create_ws_provider(ProviderConfig::new(url)).await.is_err(),
            "no rate limiting"
        );
        assert!(
            create_ws_provider(ProviderConfig::new(url).with_rate_limit(10))
                .await
                .is_err(),
            "rate_limit_per_second"
        );
        assert!(
            create_ws_provider(ProviderConfig::new(url).with_min_delay(Duration::from_millis(250)))
                .await
                .is_err(),
            "min_delay only"
        );
        assert!(
            create_ws_provider(
                ProviderConfig::new(url)
                    .with_rate_limit(5)
                    .with_min_delay(Duration::from_millis(250)),
            )
            .await
            .is_err(),
            "both axes"
        );
    }
}
