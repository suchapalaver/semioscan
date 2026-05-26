// SPDX-FileCopyrightText: 2026 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Behaviour guard for issue #54: a `ProviderConfig` carrying either
//! rate-limiting axis must produce a WebSocket provider that actually
//! paces requests on the wire, not just at the type level.
//!
//! The shape-check `create_ws_provider_accepts_full_dispatch_matrix`
//! covers the dispatch surface; the helper unit test
//! `rate_limit_layer_for_covers_full_matrix` covers which layer the
//! shared dispatcher returns for each input. Neither catches a future
//! refactor that reintroduces inline dispatch in `create_ws_provider`
//! and silently drops a layer — the original #46 failure mode, just
//! re-routed through the WS factory.
//!
//! These tests close that gap by observing real pacing on the wire. A
//! real WebSocket server completes the handshake and then records the
//! arrival timestamp of every inbound JSON-RPC frame without ever
//! responding. Two concurrent `get_block_number` calls are fired and
//! the gap between the first and second observed arrivals is the
//! contract — that gap is the only thing the rate-limit layer can
//! influence, since both calls share a single transport.
//!
//! As with `typed_provider_min_delay_test`, the check compares two
//! regimes captured back-to-back in the same process — once with the
//! axis under test set, once without — so shared scheduler / network
//! stack jitter cancels in the difference rather than having to be
//! absorbed by absolute thresholds.

#![cfg(feature = "ws")]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use alloy_provider::Provider;
use futures::StreamExt;
use semioscan::provider::{create_ws_provider, ProviderConfig};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// Spawn a localhost WebSocket server that:
///
/// 1. accepts inbound TCP connections,
/// 2. completes the WebSocket handshake per RFC 6455, and
/// 3. records the wall-clock arrival of every inbound text or binary
///    frame on the returned channel, then silently drops it.
///
/// Staying silent on the response is the trick: a real handshake is
/// needed so the alloy WS transport considers the connection live and
/// will pump dispatched requests through the rate-limit layer, but no
/// response is ever needed to observe the layer's effect — pacing
/// shows up directly in the arrival timestamps. Decoupling the check
/// from request/response semantics keeps the test resilient to any
/// future change in JSON-RPC framing.
async fn spawn_arrival_recording_server() -> (String, mpsc::UnboundedReceiver<Instant>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let url = format!("ws://{addr}");

    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                break;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                while let Some(Ok(msg)) = ws.next().await {
                    if matches!(msg, Message::Text(_) | Message::Binary(_)) {
                        let _ = tx.send(Instant::now());
                    }
                }
            });
        }
    });

    (url, rx)
}

/// Build a WS provider against the recording server using the caller's
/// config tweak, fire two concurrent `get_block_number` calls, and
/// return the wall-clock gap between when the server observed the
/// first and second JSON-RPC frames.
///
/// The futures are spawned and intentionally not joined: the server
/// never responds, so each call would hang forever. The layered
/// behaviour we care about is fully observable in the inbound frame
/// timestamps, which the layer slows down before the WS dispatch ever
/// sees them.
async fn measure_inbound_frame_gap(
    configure: impl FnOnce(ProviderConfig) -> ProviderConfig,
) -> Duration {
    let (url, mut arrivals) = spawn_arrival_recording_server().await;
    let config = configure(ProviderConfig::new(&url));
    let provider = create_ws_provider(config)
        .await
        .expect("ws provider connects");

    let p1 = provider.clone();
    tokio::spawn(async move {
        let _ = p1.get_block_number().await;
    });
    let p2 = provider.clone();
    tokio::spawn(async move {
        let _ = p2.get_block_number().await;
    });

    let first = tokio::time::timeout(Duration::from_secs(5), arrivals.recv())
        .await
        .expect("first frame within 5s")
        .expect("recording channel open");
    let second = tokio::time::timeout(Duration::from_secs(5), arrivals.recv())
        .await
        .expect("second frame within 5s")
        .expect("recording channel open");

    second.duration_since(first)
}

/// With `min_delay` set as the only rate-limiting axis, two concurrent
/// WS RPC calls must reach the wire serialised through the layer: the
/// gap between the first and second inbound frames at the recording
/// server has to grow by at least `min_delay / 2` relative to a
/// no-layer baseline measured the same way.
///
/// Comparing the two regimes captured in the same process keeps the
/// test resilient to CI scheduler stalls and OS network jitter — both
/// pay the same overhead, which cancels in the difference. A regression
/// that drops the `min_delay` arm in `create_ws_provider` would collapse
/// the throttled gap back to the baseline and trip the assertion.
/// Multi-threaded flavour avoids the cooperative-scheduler trap where
/// two futures awaiting the same single runtime thread would serialise
/// even without a rate-limit layer present.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_provider_min_delay_is_observable_in_request_pacing() {
    let min_delay = Duration::from_millis(400);

    let baseline = measure_inbound_frame_gap(|c| c).await;
    let throttled = measure_inbound_frame_gap(|c| c.with_min_delay(min_delay)).await;

    let minimum_gap = min_delay / 2;
    let actual_gap = throttled.saturating_sub(baseline);
    assert!(
        actual_gap >= minimum_gap,
        "min_delay layer not throttling WS provider: baseline={baseline:?}, \
         throttled={throttled:?}, gap={actual_gap:?} < required {minimum_gap:?} \
         (min_delay={min_delay:?})"
    );
}

/// With `rate_limit_per_second` set as the only rate-limiting axis,
/// the layer's token bucket has to throttle the second of two
/// concurrent WS RPC calls. Pinning the rate to 1 request/second
/// guarantees a one-second wait between consecutive calls — the
/// bucket starts with one token and refills at one token per second,
/// so the second call cannot acquire until ~1s after the first.
///
/// The expected gap is half a second — well above any plausible CI
/// jitter, well below the one-second wait the layer should be
/// imposing. A regression that drops the per-second arm in
/// `create_ws_provider` would collapse the throttled gap back to the
/// baseline and trip the assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_provider_rate_limit_per_second_is_observable_in_request_pacing() {
    let rps = 1u32;
    let minimum_gap = Duration::from_millis(500);

    let baseline = measure_inbound_frame_gap(|c| c).await;
    let throttled = measure_inbound_frame_gap(|c| c.with_rate_limit(rps)).await;

    let actual_gap = throttled.saturating_sub(baseline);
    assert!(
        actual_gap >= minimum_gap,
        "rate_limit_per_second layer not throttling WS provider: baseline={baseline:?}, \
         throttled={throttled:?}, gap={actual_gap:?} < required {minimum_gap:?} \
         (rate_limit_per_second={rps})"
    );
}
