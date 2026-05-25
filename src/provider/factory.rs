// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Provider factory functions for creating type-erased providers

use alloy_network::AnyNetwork;
use alloy_provider::RootProvider;
use alloy_rpc_client::{ClientBuilder, RpcClient};

use crate::errors::RpcError;
use crate::transport::RateLimitLayer;

use super::config::ProviderConfig;
use super::http_client::reqwest_client_with_timeout;
use super::AnyHttpProvider;

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

    let client = match (config.rate_limit_per_second, config.min_delay) {
        // Both rate limit and min delay (prefer rate limit)
        (Some(rps), Some(_)) => {
            tracing::warn!(
                "Both rate_limit_per_second and min_delay specified, using rate_limit_per_second"
            );
            let builder = builder.layer(RateLimitLayer::per_second(rps));
            match config.timeout {
                Some(timeout) => {
                    builder.http_with_client(reqwest_client_with_timeout(timeout)?, url)
                }
                None => builder.http(url),
            }
        }

        // Rate limit only
        (Some(rps), None) => {
            let builder = builder.layer(RateLimitLayer::per_second(rps));
            match config.timeout {
                Some(timeout) => {
                    builder.http_with_client(reqwest_client_with_timeout(timeout)?, url)
                }
                None => builder.http(url),
            }
        }

        // Min delay only
        (None, Some(delay)) => {
            let builder = builder.layer(RateLimitLayer::with_min_delay(delay));
            match config.timeout {
                Some(timeout) => {
                    builder.http_with_client(reqwest_client_with_timeout(timeout)?, url)
                }
                None => builder.http(url),
            }
        }

        // No rate-limiting layer
        (None, None) => match config.timeout {
            Some(timeout) => builder.http_with_client(reqwest_client_with_timeout(timeout)?, url),
            None => builder.http(url),
        },
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
/// # Note
///
/// This function is async because WebSocket connections require a handshake.
///
/// # Examples
///
/// ```rust,ignore
/// use semioscan::provider::{create_ws_provider, ProviderConfig};
///
/// let provider = create_ws_provider(
///     ProviderConfig::new("wss://eth.llamarpc.com/ws")
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

    let ws = WsConnect::new(&config.url);

    let client = match config.rate_limit_per_second {
        Some(rps) => ClientBuilder::default()
            .layer(RateLimitLayer::per_second(rps))
            .ws(ws)
            .await
            .map_err(|e| RpcError::ProviderConnectionFailed(e.to_string()))?,

        None => ClientBuilder::default()
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
}
