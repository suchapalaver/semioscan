// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Behaviour guard for issue #47: a provider pool built through
//! `ProviderPoolBuilder::with_rpc_policy(&config)` must honour the policy's
//! per-chain `rate_limit_delay`. Before the fix, the pool path read only
//! `cfg.rpc_timeout` and silently dropped `rate_limit_delay`, so chains the
//! operator had explicitly paced (`chain_rate_limit(...)`) fired requests
//! back-to-back at strict upstreams.
//!
//! The check is observable, not structural. A stalled TCP listener accepts
//! every connection without ever responding, so every request will eventually
//! time out on the transport. The test captures two regimes back-to-back in
//! the same process — once with a policy that sets `rate_limit_delay`, once
//! without — and compares how long two concurrent requests take to drain in
//! each. With the layer installed, the second request is held for at least
//! `rate_limit_delay` before it reaches the transport, so the policy regime
//! is meaningfully slower than the baseline. With the policy ignored (the
//! pre-fix bug), the two regimes collapse together.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_chains::NamedChain;
use alloy_provider::Provider;
use semioscan::provider::{ChainEndpoint, ProviderPoolBuilder};
use semioscan::SemioscanConfigBuilder;
use tokio::net::{TcpListener, TcpStream};

/// Bind a listener on `127.0.0.1:0`, accept every inbound connection, and
/// keep each one parked open without writing any bytes back. Returns the
/// bound URL the pool should target.
async fn spawn_stalled_listener() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let url = format!("http://{addr}");
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

/// Build a pool through `with_rpc_policy(&config)` and fire two concurrent
/// `get_block_number` calls against a freshly bound stalled listener; return
/// how long both futures took to drain. Used by both the layered and the
/// no-layer cases so the two regimes can be compared against each other
/// rather than against absolute thresholds.
async fn drain_two_concurrent_pool_requests(
    chain_rate_limit_delay: Option<Duration>,
    transport_timeout: Duration,
) -> Duration {
    let url = spawn_stalled_listener().await;

    let mut builder = SemioscanConfigBuilder::with_defaults()
        .chain_timeout(NamedChain::Mainnet, transport_timeout);
    if let Some(d) = chain_rate_limit_delay {
        builder = builder.chain_rate_limit(NamedChain::Mainnet, d);
    }
    let config = builder.build();

    let pool = ProviderPoolBuilder::new()
        .add_chain(NamedChain::Mainnet, &url)
        .with_rpc_policy(&config)
        .build()
        .expect("pool built");

    let provider = pool.get(NamedChain::Mainnet).expect("provider present");

    let start = Instant::now();
    let (a, b) = tokio::join!(provider.get_block_number(), provider.get_block_number());
    let elapsed = start.elapsed();

    assert!(a.is_err(), "first request should time out: {a:?}");
    assert!(b.is_err(), "second request should time out: {b:?}");
    elapsed
}

/// With `chain_rate_limit` set on the policy, two concurrent calls against a
/// stalled transport must serialise through the pool's rate-limit layer: the
/// policy regime has to drain meaningfully slower than an otherwise-identical
/// no-policy-delay baseline measured on the same runtime.
///
/// A relative gap of at least `rate_limit_delay / 2` between the two timings
/// is the contract — if the pool ignored the policy delay (the pre-fix bug),
/// the two regimes would collapse to roughly the same elapsed window and
/// this assertion would fire. Multi-threaded flavour avoids the
/// cooperative-scheduler trap where two futures awaiting the same single
/// runtime thread would serialise even without a rate-limit layer present.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_with_rpc_policy_honors_chain_rate_limit_delay() {
    let delay = Duration::from_millis(400);
    let transport_timeout = Duration::from_millis(150);

    let baseline = drain_two_concurrent_pool_requests(None, transport_timeout).await;
    let throttled = drain_two_concurrent_pool_requests(Some(delay), transport_timeout).await;

    let minimum_gap = delay / 2;
    let actual_gap = throttled.saturating_sub(baseline);
    assert!(
        actual_gap >= minimum_gap,
        "pool ignored policy rate_limit_delay: baseline={baseline:?}, \
         throttled={throttled:?}, gap={actual_gap:?} < required {minimum_gap:?} \
         (rate_limit_delay={delay:?})"
    );
}

/// An explicit per-endpoint `min_delay` must override the policy's
/// `rate_limit_delay` for the same chain. The endpoint asks for a much
/// larger gap than the policy; we measure two concurrent calls and require
/// the elapsed window to be at least `endpoint_delay / 2` larger than the
/// transport-timeout floor — which it can only be if the longer endpoint
/// delay is what reached the transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn endpoint_min_delay_overrides_policy_rate_limit_delay() {
    let url = spawn_stalled_listener().await;

    let endpoint_delay = Duration::from_millis(400);
    let policy_delay = Duration::from_millis(50);
    let transport_timeout = Duration::from_millis(150);

    let config = SemioscanConfigBuilder::with_defaults()
        .chain_timeout(NamedChain::Mainnet, transport_timeout)
        .chain_rate_limit(NamedChain::Mainnet, policy_delay)
        .build();

    let endpoint = ChainEndpoint::new(NamedChain::Mainnet, &url).with_min_delay(endpoint_delay);

    let pool = ProviderPoolBuilder::new()
        .add_endpoint(endpoint)
        .with_rpc_policy(&config)
        .build()
        .expect("pool built");

    let provider = pool.get(NamedChain::Mainnet).expect("provider present");

    let start = Instant::now();
    let (a, b) = tokio::join!(provider.get_block_number(), provider.get_block_number());
    let elapsed = start.elapsed();

    assert!(a.is_err(), "first request should time out: {a:?}");
    assert!(b.is_err(), "second request should time out: {b:?}");

    // The endpoint delay (400ms) is much longer than the transport timeout
    // floor (150ms × 2 ≈ 300ms in the worst serialised case). If the policy
    // delay won instead, the two requests would race past in ≈150ms each.
    let minimum_total = transport_timeout + endpoint_delay / 2;
    assert!(
        elapsed >= minimum_total,
        "endpoint min_delay did not override policy delay: elapsed={elapsed:?} \
         < required {minimum_total:?} (endpoint_delay={endpoint_delay:?}, \
         policy_delay={policy_delay:?})"
    );
}
