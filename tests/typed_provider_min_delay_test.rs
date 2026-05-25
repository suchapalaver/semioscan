// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Behaviour guard for the issue #45 regression: a `ProviderConfig` whose only
//! rate-limiting axis is `min_delay` must produce a typed HTTP provider that
//! actually throttles requests.
//!
//! The check is observable, not structural. A stalled TCP listener accepts
//! every connection without ever responding, so every request will eventually
//! time out on the transport. The test captures two regimes back-to-back in
//! the same process — once with `min_delay` set, once without — and compares
//! how long two concurrent requests take to drain in each. With the layer
//! installed, the second request is held for at least `min_delay` before it
//! ever reaches the transport, so the layered run is meaningfully slower
//! than the baseline. With the layer missing (the pre-fix bug for the typed
//! factory), the two regimes collapse together.
//!
//! Comparing the two regimes against each other rather than against absolute
//! thresholds keeps the test resilient to CI scheduler stalls and network
//! stack jitter — both regimes pay the same overhead, which cancels in the
//! difference.

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

/// Build a typed provider with the given delay/timeout, fire two concurrent
/// `get_block_number` calls against a freshly bound stalled listener, and
/// return how long both futures took to drain. Used by the layered and the
/// no-layer cases so we can compare the two regimes against each other
/// rather than against absolute thresholds.
async fn drain_two_concurrent_requests(
    min_delay: Option<Duration>,
    transport_timeout: Duration,
) -> Duration {
    let url = spawn_stalled_listener().await;

    let mut config = ProviderConfig::new(&url).with_timeout(transport_timeout);
    if let Some(d) = min_delay {
        config = config.with_min_delay(d);
    }
    let provider = create_typed_http_provider::<Ethereum>(config).expect("provider built");

    let start = Instant::now();
    let (a, b) = tokio::join!(provider.get_block_number(), provider.get_block_number());
    let elapsed = start.elapsed();

    assert!(a.is_err(), "first request should time out: {a:?}");
    assert!(b.is_err(), "second request should time out: {b:?}");
    elapsed
}

/// With `min_delay` set as the only rate-limiting axis, two concurrent calls
/// against a stalled transport must serialise through the layer: the layered
/// regime has to drain meaningfully slower than an otherwise-identical
/// no-layer baseline measured on the same runtime.
///
/// The test compares two regimes captured in the same process so shared
/// scheduler / network-stack jitter cancels out. A relative gap of at least
/// `min_delay / 2` between the two timings is the contract — if the layer
/// is missing, the two regimes collapse to roughly the same elapsed window
/// and the assertion fires. Multi-threaded flavour avoids the
/// cooperative-scheduler trap where two futures awaiting the same single
/// runtime thread would serialise even without a rate-limit layer present.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_provider_min_delay_is_observable_in_request_pacing() {
    let min_delay = Duration::from_millis(400);
    let transport_timeout = Duration::from_millis(150);

    let baseline = drain_two_concurrent_requests(None, transport_timeout).await;
    let throttled = drain_two_concurrent_requests(Some(min_delay), transport_timeout).await;

    // The layer holds the second request for ~min_delay before the transport
    // ever sees it. Even with generous CI jitter on both regimes, the
    // layered run must outpace the baseline by at least half the configured
    // delay — anything smaller means the layer is not installed.
    let minimum_gap = min_delay / 2;
    let actual_gap = throttled.saturating_sub(baseline);
    assert!(
        actual_gap >= minimum_gap,
        "min_delay layer not throttling: baseline={baseline:?}, throttled={throttled:?}, \
         gap={actual_gap:?} < required {minimum_gap:?} (min_delay={min_delay:?})"
    );
}
