// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests verifying that `SemioscanConfig::rpc_timeout` reaches the
//! HTTP transport.
//!
//! Each test stands up a TCP listener on `127.0.0.1` that accepts *every*
//! connection and deliberately holds it open without writing anything back.
//! With a short configured timeout, the alloy provider built through our
//! factories must surface a reqwest-side timeout error rather than hanging
//! on the TCP layer.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_chains::NamedChain;
use alloy_provider::Provider;
use semioscan::provider::{
    create_http_provider, ChainEndpoint, ProviderConfig, ProviderPoolBuilder,
};
use semioscan::{RpcPolicy, SemioscanConfigBuilder};
use tokio::net::{TcpListener, TcpStream};

/// Bind a listener on `127.0.0.1:0` and spawn an accept-loop that keeps every
/// inbound connection alive (without writing any bytes) until the test ends.
/// Returns the bound URL.
///
/// Looping on `accept` is important: reqwest's connection pool may open more
/// than one socket per logical request, and a listener that only accepts once
/// would cause subsequent connections to stall at the TCP layer instead of
/// stalling inside an established HTTP request — that would let the test pass
/// for the wrong reason.
async fn spawn_stalled_listener() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let url = format!("http://{addr}");
    // Held-open streams parked here so they stay alive without responding.
    let parked: Arc<tokio::sync::Mutex<Vec<TcpStream>>> = Arc::new(tokio::sync::Mutex::new(vec![]));
    tokio::spawn({
        let parked = Arc::clone(&parked);
        async move {
            loop {
                let Ok((stream, _peer)) = listener.accept().await else {
                    break;
                };
                parked.lock().await.push(stream);
            }
        }
    });
    url
}

/// Convert any error into a string and look for transport-timeout markers.
/// Reqwest's deadline error reaches us as `reqwest::Error { ..., source:
/// TimedOut }`; alloy's `Display` says "operation timed out" while `Debug`
/// shows the bare `TimedOut` variant. We accept both spellings so the test
/// is independent of which formatting path alloy/reqwest happens to use.
fn is_timeout_error<E: std::fmt::Display + std::fmt::Debug>(err: &E) -> bool {
    let display = format!("{err}").to_lowercase();
    let debug = format!("{err:?}").to_lowercase();
    let needles = ["timed out", "timedout", "deadline"];
    needles
        .iter()
        .any(|n| display.contains(n) || debug.contains(n))
}

#[tokio::test(flavor = "current_thread")]
async fn provider_config_timeout_is_applied_to_http_transport() {
    let url = spawn_stalled_listener().await;

    let provider =
        create_http_provider(ProviderConfig::new(&url).with_timeout(Duration::from_millis(150)))
            .expect("provider built");

    let start = Instant::now();
    let result = provider.get_block_number().await;
    let elapsed = start.elapsed();

    let err = result.expect_err("expected a transport error");
    assert!(
        is_timeout_error(&err),
        "expected reqwest timeout error, got {err:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "expected error within ~150ms, elapsed {elapsed:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn provider_pool_applies_rpc_policy_timeout_per_chain() {
    let url = spawn_stalled_listener().await;

    let config = SemioscanConfigBuilder::with_defaults()
        .chain_timeout(NamedChain::Mainnet, Duration::from_millis(150))
        .build();

    let pool = ProviderPoolBuilder::new()
        .add_chain(NamedChain::Mainnet, &url)
        .with_rpc_policy(&config)
        .build()
        .expect("pool built");

    let provider = pool.get(NamedChain::Mainnet).expect("provider present");

    let start = Instant::now();
    let result = provider.get_block_number().await;
    let elapsed = start.elapsed();

    let err = result.expect_err("expected a transport error");
    assert!(
        is_timeout_error(&err),
        "expected reqwest timeout error, got {err:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "expected error within ~150ms, elapsed {elapsed:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn provider_pool_endpoint_timeout_overrides_default() {
    let url = spawn_stalled_listener().await;

    let endpoint =
        ChainEndpoint::new(NamedChain::Mainnet, &url).with_timeout(Duration::from_millis(150));

    let pool = ProviderPoolBuilder::new()
        .with_timeout(Duration::from_secs(30))
        .add_endpoint(endpoint)
        .build()
        .expect("pool built");

    let provider = pool.get(NamedChain::Mainnet).expect("provider present");

    let start = Instant::now();
    let result = provider.get_block_number().await;
    let elapsed = start.elapsed();

    let err = result.expect_err("expected a transport error");
    assert!(
        is_timeout_error(&err),
        "expected reqwest timeout error, got {err:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "endpoint timeout must override default; elapsed {elapsed:?}"
    );
}

/// Explicit per-endpoint timeout wins over a timeout supplied by an
/// [`RpcPolicy`] for the same chain.
#[tokio::test(flavor = "current_thread")]
async fn endpoint_timeout_overrides_rpc_policy() {
    let url = spawn_stalled_listener().await;

    // Policy says 30s (would hang the test); endpoint says 150ms.
    let config = SemioscanConfigBuilder::with_defaults()
        .chain_timeout(NamedChain::Mainnet, Duration::from_secs(30))
        .build();

    let endpoint =
        ChainEndpoint::new(NamedChain::Mainnet, &url).with_timeout(Duration::from_millis(150));

    let pool = ProviderPoolBuilder::new()
        .add_endpoint(endpoint)
        .with_rpc_policy(&config)
        .build()
        .expect("pool built");

    let provider = pool.get(NamedChain::Mainnet).expect("provider present");

    let start = Instant::now();
    let result = provider.get_block_number().await;
    let elapsed = start.elapsed();

    let err = result.expect_err("expected a transport error");
    assert!(
        is_timeout_error(&err),
        "expected reqwest timeout error, got {err:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "endpoint timeout must override policy timeout; elapsed {elapsed:?}"
    );
}

#[test]
fn rpc_policy_resolution_is_exposed_via_public_api() {
    let config = SemioscanConfigBuilder::with_defaults()
        .rpc_timeout(Duration::from_secs(45))
        .chain_timeout(NamedChain::Polygon, Duration::from_secs(90))
        .build();

    assert_eq!(
        config.rpc_config(NamedChain::Mainnet).rpc_timeout,
        Duration::from_secs(45)
    );
    assert_eq!(
        config.rpc_config(NamedChain::Polygon).rpc_timeout,
        Duration::from_secs(90)
    );
}
