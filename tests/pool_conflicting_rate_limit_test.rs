// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Behaviour guard for issue #50: a provider pool that ends up with both a
//! requests-per-second budget and a minimum-delay gap on the same chain must
//! fail loudly at `ProviderPoolBuilder::build()` rather than silently dropping
//! one axis on the wire.
//!
//! Each test composes the conflict through a different operator-facing entry
//! point — pool-wide rate limit + policy delay, endpoint-level rate limit +
//! policy delay, endpoint-level rate limit + endpoint-level min delay — so a
//! regression that quietly re-introduced precedence in any single dispatch
//! arm would surface as one of these tests flipping.

use std::time::Duration;

use alloy_chains::NamedChain;
use semioscan::provider::{ChainEndpoint, ProviderPoolBuilder};
use semioscan::{RpcError, SemioscanConfigBuilder};

fn assert_conflicting_rate_limit(err: RpcError, expected_rps: u32, expected_delay: Duration) {
    match err {
        RpcError::ConflictingRateLimit {
            rate_limit_per_second,
            min_delay,
        } => {
            assert_eq!(
                rate_limit_per_second, expected_rps,
                "rate_limit_per_second in error must reflect the offending value",
            );
            assert_eq!(
                min_delay, expected_delay,
                "min_delay in error must reflect the offending value",
            );
        }
        other => panic!("expected RpcError::ConflictingRateLimit, got {other:?}"),
    }
}

/// The pool-wide `with_rate_limit` and a policy's per-chain
/// `rate_limit_delay` describe the same axis: each endpoint installs at
/// most one rate-limit layer. Combining them on the same chain previously
/// dropped the policy delay silently with a `tracing::warn!`; the builder
/// must now refuse the combination at construction time. This is the
/// exact composition called out in the issue body, so a regression in the
/// pool builder's overlay logic would land here first.
///
/// The test uses `NamedChain::Mainnet`, which has no `with_common_defaults`
/// rate-limit-delay entry, and a non-default value, so the assertion only
/// passes if the explicit `.chain_rate_limit(...)` call actually wrote the
/// delay onto the policy.
#[test]
fn pool_with_rate_limit_plus_policy_delay_errors() {
    let url = "http://localhost:8545";
    let rps: u32 = 10;
    let delay = Duration::from_millis(400);

    let config = SemioscanConfigBuilder::with_defaults()
        .chain_rate_limit(NamedChain::Mainnet, delay)
        .build();

    let err = ProviderPoolBuilder::new()
        .add_chain(NamedChain::Mainnet, url)
        .with_rate_limit(rps)
        .with_rpc_policy(&config)
        .build()
        .expect_err("conflicting rate-limit axes must be rejected at build()");

    assert_conflicting_rate_limit(err, rps, delay);
}

/// The pool-wide `with_rate_limit` and an endpoint-level `with_min_delay`
/// describe the same axis through yet another composition path: the rps
/// comes from the pool-wide default, the min-delay from the endpoint
/// itself, no policy involved. This guards against a refactor that
/// special-cased policy-supplied delays while leaving endpoint-supplied
/// delays composed with the pool default unchecked.
#[test]
fn pool_with_rate_limit_plus_endpoint_min_delay_errors() {
    let url = "http://localhost:8545";
    let rps: u32 = 7;
    let delay = Duration::from_millis(300);

    let endpoint = ChainEndpoint::new(NamedChain::Polygon, url).with_min_delay(delay);

    let err = ProviderPoolBuilder::new()
        .add_endpoint(endpoint)
        .with_rate_limit(rps)
        .build()
        .expect_err("conflicting rate-limit axes must be rejected at build()");

    assert_conflicting_rate_limit(err, rps, delay);
}

/// An endpoint-level `with_rate_limit` plus a policy's `rate_limit_delay`
/// on the same chain reaches the same conflict through a different
/// composition path — the pool-wide default is never touched.
#[test]
fn pool_endpoint_rate_limit_plus_policy_delay_errors() {
    let url = "http://localhost:8545";
    let rps: u32 = 5;
    let delay = Duration::from_millis(500);

    let config = SemioscanConfigBuilder::with_defaults()
        .chain_rate_limit(NamedChain::Mainnet, delay)
        .build();

    let endpoint = ChainEndpoint::new(NamedChain::Mainnet, url).with_rate_limit(rps);

    let err = ProviderPoolBuilder::new()
        .add_endpoint(endpoint)
        .with_rpc_policy(&config)
        .build()
        .expect_err("conflicting rate-limit axes must be rejected at build()");

    assert_conflicting_rate_limit(err, rps, delay);
}

/// An endpoint that sets both axes directly (no policy involved) hits the
/// same rejection. This guards against a future refactor that moved the
/// conflict check to the policy-merge step and missed the endpoint-only
/// path.
#[test]
fn pool_endpoint_with_rate_limit_and_min_delay_errors() {
    let url = "http://localhost:8545";
    let rps: u32 = 8;
    let delay = Duration::from_millis(125);

    let endpoint = ChainEndpoint::new(NamedChain::Optimism, url)
        .with_rate_limit(rps)
        .with_min_delay(delay);

    let err = ProviderPoolBuilder::new()
        .add_endpoint(endpoint)
        .build()
        .expect_err("conflicting rate-limit axes must be rejected at build()");

    assert_conflicting_rate_limit(err, rps, delay);
}

/// A chain that resolves to only one rate-limit axis (per-second only, via
/// the pool-wide default; min-delay only, via the policy) must still build
/// cleanly. This is the negative control for the rejection tests above —
/// if a refactor over-tightened the check and started rejecting any
/// rate-limit configuration, this test would catch it.
#[test]
fn pool_single_axis_configurations_build_cleanly() {
    let url = "http://localhost:8545";

    ProviderPoolBuilder::new()
        .add_chain(NamedChain::Mainnet, url)
        .with_rate_limit(10)
        .build()
        .expect("pool-wide rate-limit alone must build");

    let policy_only = SemioscanConfigBuilder::with_defaults()
        .chain_rate_limit(NamedChain::Mainnet, Duration::from_millis(250))
        .build();

    ProviderPoolBuilder::new()
        .add_chain(NamedChain::Mainnet, url)
        .with_rpc_policy(&policy_only)
        .build()
        .expect("policy min-delay alone must build");
}
