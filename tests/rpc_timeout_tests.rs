// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests verifying that `SemioscanConfig::rpc_timeout` reaches the
//! HTTP transport.
//!
//! Each test stands up a TCP listener on `127.0.0.1` that accepts connections
//! and then deliberately stalls without ever responding. With a short
//! configured timeout, the alloy provider built through our factories must
//! surface a transport error rather than hanging.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use alloy_chains::NamedChain;
use alloy_provider::Provider;
use semioscan::provider::{
    create_http_provider, ChainEndpoint, ProviderConfig, ProviderPoolBuilder,
};
use semioscan::{RpcPolicy, SemioscanConfigBuilder};
use tokio::net::TcpListener;

/// Spawn a listener that accepts a single connection and then holds it open
/// without writing anything back. Returns the bound URL.
async fn spawn_stalled_listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let url = format!("http://{addr}");
    (listener, url)
}

#[tokio::test(flavor = "current_thread")]
async fn provider_config_timeout_is_applied_to_http_transport() {
    let (listener, url) = spawn_stalled_listener().await;
    let _accept = tokio::spawn(async move {
        // Hold the connection forever so the client must time out.
        let _ = listener.accept().await;
        std::future::pending::<()>().await;
    });

    let provider =
        create_http_provider(ProviderConfig::new(&url).with_timeout(Duration::from_millis(150)))
            .expect("provider built");

    let start = Instant::now();
    let result = provider.get_block_number().await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "expected timeout error, got {result:?}");
    assert!(
        elapsed < Duration::from_secs(2),
        "expected error within ~150ms, elapsed {elapsed:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn provider_pool_applies_rpc_policy_timeout_per_chain() {
    let (listener, url) = spawn_stalled_listener().await;
    let _accept = tokio::spawn(async move {
        let _ = listener.accept().await;
        std::future::pending::<()>().await;
    });

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

    assert!(result.is_err(), "expected timeout error, got {result:?}");
    assert!(
        elapsed < Duration::from_secs(2),
        "expected error within ~150ms, elapsed {elapsed:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn provider_pool_endpoint_timeout_overrides_default() {
    let (listener, url) = spawn_stalled_listener().await;
    let _accept = tokio::spawn(async move {
        let _ = listener.accept().await;
        std::future::pending::<()>().await;
    });

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

    assert!(result.is_err(), "expected timeout error, got {result:?}");
    assert!(
        elapsed < Duration::from_secs(2),
        "endpoint timeout must override default; elapsed {elapsed:?}"
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
