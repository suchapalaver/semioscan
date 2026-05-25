// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Behaviour guard for the issue #45 regression: a `ProviderConfig` whose only
//! rate-limiting axis is `min_delay` must produce a typed HTTP provider that
//! actually throttles requests.
//!
//! The check is observable, not structural. A stalled TCP listener accepts
//! every connection without ever responding, so every request will eventually
//! time out on the transport. Two requests are issued concurrently:
//!
//! * If the `min_delay` layer is installed (correct behaviour), the second
//!   request is held by the layer for at least `min_delay` before it ever
//!   reaches the transport. The total wall-clock time to drain both futures
//!   is roughly `min_delay + transport_timeout`.
//! * If `min_delay` was silently dropped (the pre-fix bug for the typed
//!   factory), both requests fire immediately and drain in roughly
//!   `transport_timeout` — no per-request gap.
//!
//! A threshold between the two regimes turns this into a one-way regression
//! guard: any future change that drops the `(None, Some(delay))` arm from the
//! shared client builder will collapse the elapsed window and trip the
//! assertion.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_network::Ethereum;
use alloy_provider::Provider;
use semioscan::provider::{create_typed_http_provider, ProviderConfig};
use tokio::net::{TcpListener, TcpStream};

/// Bind a listener on `127.0.0.1:0`, accept every inbound connection, and
/// keep each one parked open without writing any bytes back. Returns the
/// bound URL the provider should target.
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

/// With `min_delay` set as the only rate-limiting axis, two concurrent calls
/// against a stalled transport must serialise through the layer: the elapsed
/// window covers the first call's transport timeout *and* the layer-held
/// wait before the second call reaches the transport at all.
#[tokio::test(flavor = "current_thread")]
async fn typed_provider_with_min_delay_throttles_concurrent_requests() {
    let url = spawn_stalled_listener().await;

    let min_delay = Duration::from_millis(400);
    let transport_timeout = Duration::from_millis(150);

    let provider = create_typed_http_provider::<Ethereum>(
        ProviderConfig::new(&url)
            .with_min_delay(min_delay)
            .with_timeout(transport_timeout),
    )
    .expect("provider built");

    let start = Instant::now();
    let (a, b) = tokio::join!(provider.get_block_number(), provider.get_block_number());
    let elapsed = start.elapsed();

    assert!(a.is_err(), "first request should time out: {a:?}");
    assert!(b.is_err(), "second request should time out: {b:?}");

    // Without the layer, both futures drain in roughly `transport_timeout` (~150ms).
    // With the layer, the second is held for `min_delay` (~400ms) before it
    // even reaches the transport. Threshold sits above the no-layer regime
    // and below the expected layered regime, with margin for runtime jitter.
    let lower_bound = Duration::from_millis(300);
    assert!(
        elapsed >= lower_bound,
        "min_delay layer not throttling: elapsed {elapsed:?} < {lower_bound:?} \
         (expected ~{}ms with min_delay={}ms applied)",
        (min_delay + transport_timeout).as_millis(),
        min_delay.as_millis(),
    );
}

/// Sanity counter-check: with no rate-limiting axis set, two concurrent
/// requests drain in roughly the transport timeout. This pins down the
/// "no layer" regime so the throttled test's threshold is meaningful — if
/// some future change starts inserting a layer unconditionally, this test
/// will catch it.
#[tokio::test(flavor = "current_thread")]
async fn typed_provider_without_rate_limit_does_not_throttle() {
    let url = spawn_stalled_listener().await;

    let transport_timeout = Duration::from_millis(150);

    let provider = create_typed_http_provider::<Ethereum>(
        ProviderConfig::new(&url).with_timeout(transport_timeout),
    )
    .expect("provider built");

    let start = Instant::now();
    let (a, b) = tokio::join!(provider.get_block_number(), provider.get_block_number());
    let elapsed = start.elapsed();

    assert!(a.is_err());
    assert!(b.is_err());

    // No layer means both futures should drain in roughly the transport
    // timeout, well under the throttled-test threshold.
    let upper_bound = Duration::from_millis(300);
    assert!(
        elapsed < upper_bound,
        "expected no throttling, elapsed {elapsed:?} >= {upper_bound:?}"
    );
}
