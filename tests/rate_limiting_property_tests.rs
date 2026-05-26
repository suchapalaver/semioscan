// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Property-based tests for rate limiting
//!
//! These tests use proptest to validate invariants about rate limiting behavior
//! across a wide range of configurations and scenarios.

use alloy_chains::NamedChain;
use proptest::prelude::*;
use semioscan::{ChainConfig, MaxBlockRange, SemioscanConfig, SemioscanConfigBuilder};
use std::time::Duration;

// Helper to generate arbitrary NamedChain variants for testing
fn arb_chain() -> impl Strategy<Value = NamedChain> {
    prop_oneof![
        Just(NamedChain::Mainnet),
        Just(NamedChain::Arbitrum),
        Just(NamedChain::Base),
        Just(NamedChain::Optimism),
        Just(NamedChain::Polygon),
        Just(NamedChain::Avalanche),
        Just(NamedChain::BinanceSmartChain),
        Just(NamedChain::Sonic),
    ]
}

// Helper to generate arbitrary Duration for rate limiting (1-5000ms).
//
// The lower bound is `1` rather than `0` because the config builder rejects
// `Duration::ZERO` as a rate-limit delay — see
// `SemioscanConfigBuilder::rate_limit_delay`'s `# Panics` section. The
// rejection contract itself is pinned by `prop_zero_delay_rejected` below.
fn arb_duration() -> impl Strategy<Value = Duration> {
    (1u64..=5000).prop_map(Duration::from_millis)
}

proptest! {
    /// Property: Chain-specific rate limit should always override global rate limit
    #[test]
    fn prop_chain_override_always_wins(
        global_delay_ms in 1u64..=5000,
        chain_delay_ms in 1u64..=5000,
        chain in arb_chain(),
    ) {
        let global_delay = Duration::from_millis(global_delay_ms);
        let chain_delay = Duration::from_millis(chain_delay_ms);

        let config = SemioscanConfigBuilder::new()
            .rate_limit_delay(global_delay)
            .chain_rate_limit(chain, chain_delay)
            .build();

        // Chain-specific delay must override global
        prop_assert_eq!(
            config.get_rate_limit_delay(chain),
            Some(chain_delay),
            "Chain-specific delay must override global delay"
        );
    }

    /// Property: If no chain-specific override, global delay should apply
    #[test]
    fn prop_global_applies_without_override(
        global_delay_ms in 1u64..=5000,
        chain in arb_chain(),
    ) {
        let global_delay = Duration::from_millis(global_delay_ms);

        let config = SemioscanConfigBuilder::new()
            .rate_limit_delay(global_delay)
            .build();

        // All chains should inherit global delay
        prop_assert_eq!(
            config.get_rate_limit_delay(chain),
            Some(global_delay),
            "Chain without override must use global delay"
        );
    }

    /// Property: Minimal config always returns None for any chain
    #[test]
    fn prop_minimal_always_none(chain in arb_chain()) {
        let config = SemioscanConfig::minimal();

        prop_assert_eq!(
            config.get_rate_limit_delay(chain),
            None,
            "Minimal config must never have rate limits"
        );
    }

    /// Property: Default config must have delays for Base and Sonic specifically
    #[test]
    fn prop_default_has_strict_chains(_any in 0u64..1) {
        let config = SemioscanConfig::default();

        // These two chains MUST have delays in default config
        prop_assert!(
            config.get_rate_limit_delay(NamedChain::Base).is_some(),
            "Default config must rate-limit Base"
        );
        prop_assert!(
            config.get_rate_limit_delay(NamedChain::Sonic).is_some(),
            "Default config must rate-limit Sonic"
        );

        // Both should have same delay (250ms)
        prop_assert_eq!(
            config.get_rate_limit_delay(NamedChain::Base),
            config.get_rate_limit_delay(NamedChain::Sonic),
            "Base and Sonic should have same default delay"
        );
    }

    /// Property: Multiple chain overrides should be independent
    #[test]
    fn prop_multiple_overrides_independent(
        chain1 in arb_chain(),
        chain2 in arb_chain(),
        delay1 in arb_duration(),
        delay2 in arb_duration(),
    ) {
        prop_assume!(chain1 != chain2); // Only test different chains

        let config = SemioscanConfigBuilder::new()
            .chain_rate_limit(chain1, delay1)
            .chain_rate_limit(chain2, delay2)
            .build();

        // Each chain should have its own delay
        prop_assert_eq!(
            config.get_rate_limit_delay(chain1),
            Some(delay1),
            "Chain 1 must have its configured delay"
        );
        prop_assert_eq!(
            config.get_rate_limit_delay(chain2),
            Some(delay2),
            "Chain 2 must have its configured delay"
        );
    }

    /// Property: Starting with defaults should preserve built-in limits when adding new ones
    #[test]
    fn prop_with_defaults_preserves_builtins(
        chain in arb_chain(),
        delay in arb_duration(),
    ) {
        // Skip Base and Sonic since we're testing they're preserved
        prop_assume!(chain != NamedChain::Base && chain != NamedChain::Sonic);

        let config = SemioscanConfigBuilder::with_defaults()
            .chain_rate_limit(chain, delay)
            .build();

        // Built-in defaults must still be present
        prop_assert_eq!(
            config.get_rate_limit_delay(NamedChain::Base),
            Some(Duration::from_millis(250)),
            "Base default must be preserved when adding new overrides"
        );
        prop_assert_eq!(
            config.get_rate_limit_delay(NamedChain::Sonic),
            Some(Duration::from_millis(250)),
            "Sonic default must be preserved when adding new overrides"
        );

        // New override should be applied
        prop_assert_eq!(
            config.get_rate_limit_delay(chain),
            Some(delay),
            "New chain override must be applied"
        );
    }

    /// Property: Config should be clonable and preserve all settings
    #[test]
    fn prop_clone_preserves_all_settings(
        global_delay_ms in 1u64..=5000,
        chain in arb_chain(),
        chain_delay_ms in 1u64..=5000,
    ) {
        let original = SemioscanConfigBuilder::new()
            .rate_limit_delay(Duration::from_millis(global_delay_ms))
            .chain_rate_limit(chain, Duration::from_millis(chain_delay_ms))
            .build();

        let cloned = original.clone();

        // Cloned config must have identical settings
        prop_assert_eq!(
            original.get_rate_limit_delay(chain),
            cloned.get_rate_limit_delay(chain),
            "Cloned config must preserve rate limits"
        );

        // Test a different chain to verify global delay
        let other_chain = NamedChain::Mainnet;
        prop_assert_eq!(
            original.get_rate_limit_delay(other_chain),
            cloned.get_rate_limit_delay(other_chain),
            "Cloned config must preserve global rate limits"
        );
    }

    /// Property: Very large delays should be supported
    #[test]
    fn prop_large_delays_supported(delay_seconds in 1u64..=3600) {
        let large_delay = Duration::from_secs(delay_seconds);
        let config = SemioscanConfigBuilder::new()
            .rate_limit_delay(large_delay)
            .build();

        prop_assert_eq!(
            config.get_rate_limit_delay(NamedChain::Arbitrum),
            Some(large_delay),
            "Large delays should be supported"
        );
    }

    /// Property: Overriding a chain multiple times should use the last value
    #[test]
    fn prop_last_override_wins(
        chain in arb_chain(),
        delay1_ms in 1u64..=5000,
        delay2_ms in 1u64..=5000,
        delay3_ms in 1u64..=5000,
    ) {
        let final_delay = Duration::from_millis(delay3_ms);

        let config = SemioscanConfigBuilder::new()
            .chain_rate_limit(chain, Duration::from_millis(delay1_ms))
            .chain_rate_limit(chain, Duration::from_millis(delay2_ms))
            .chain_rate_limit(chain, final_delay)
            .build();

        // Last override should win
        prop_assert_eq!(
            config.get_rate_limit_delay(chain),
            Some(final_delay),
            "Last override must take precedence"
        );
    }
}

proptest! {
    /// Property: Max block range should never be zero (unless explicitly set)
    #[test]
    fn prop_max_block_range_positive(chain in arb_chain()) {
        let config = SemioscanConfig::default();

        let max_blocks = config.get_max_block_range(chain);
        prop_assert!(max_blocks.as_u64() > 0, "Max block range must be positive");
    }

    /// Property: Chain-specific max block range should override global
    #[test]
    fn prop_chain_max_blocks_overrides_global(
        global_max in 100u64..=10000,
        chain_max in 100u64..=10000,
        chain in arb_chain(),
    ) {
        prop_assume!(global_max != chain_max); // Only test when different

        let config = SemioscanConfigBuilder::new()
            .max_block_range(global_max)
            .chain_max_blocks(chain, chain_max)
            .build();

        // Chain-specific max blocks should override global
        prop_assert_eq!(
            config.get_max_block_range(chain).as_u64(),
            chain_max,
            "Chain-specific max blocks must override global"
        );
    }

    /// Property: Setting both rate limit and max blocks for a chain should preserve both
    #[test]
    fn prop_independent_chain_settings(
        chain in arb_chain(),
        delay_ms in 1u64..=5000,
        max_blocks in 100u64..=10000,
    ) {
        let delay = Duration::from_millis(delay_ms);

        let config = SemioscanConfigBuilder::new()
            .chain_rate_limit(chain, delay)
            .chain_max_blocks(chain, max_blocks)
            .build();

        // Both settings should be preserved independently
        prop_assert_eq!(
            config.get_rate_limit_delay(chain),
            Some(delay),
            "Rate limit should be preserved"
        );
        prop_assert_eq!(
            config.get_max_block_range(chain).as_u64(),
            max_blocks,
            "Max block range should be preserved"
        );
    }
}

// Additional unit tests for edge cases not covered by property tests

/// Zero-duration rate-limit delay is rejected by the builder. Pins the
/// rejection contract referenced in [`arb_duration`]'s `1..=5000` range:
/// the strategy starts at 1ms because the builder panics on zero, not
/// because zero is otherwise meaningful.
#[test]
#[should_panic(expected = "rate_limit_delay must be > 0")]
fn prop_zero_delay_rejected() {
    let _ = SemioscanConfigBuilder::new()
        .rate_limit_delay(Duration::ZERO)
        .build();
}

#[test]
fn test_chain_config_with_only_rate_limit() {
    let config = ChainConfig {
        max_block_range: None,
        rate_limit_delay: Some(Duration::from_millis(250)),
        rpc_timeout: None,
        serial_lookup_fallback_attempts: None,
    };

    assert!(config.rate_limit_delay.is_some());
    assert!(config.max_block_range.is_none());
    assert!(config.rpc_timeout.is_none());
}

#[test]
fn test_chain_config_with_only_max_blocks() {
    let config = ChainConfig {
        max_block_range: Some(MaxBlockRange::new(1000)),
        rate_limit_delay: None,
        rpc_timeout: None,
        serial_lookup_fallback_attempts: None,
    };

    assert!(config.max_block_range.is_some());
    assert!(config.rate_limit_delay.is_none());
    assert!(config.rpc_timeout.is_none());
}

#[test]
fn test_chain_config_with_both_settings() {
    let config = ChainConfig {
        max_block_range: Some(MaxBlockRange::new(1000)),
        rate_limit_delay: Some(Duration::from_millis(250)),
        rpc_timeout: None,
        serial_lookup_fallback_attempts: None,
    };

    assert_eq!(config.max_block_range, Some(MaxBlockRange::new(1000)));
    assert_eq!(config.rate_limit_delay, Some(Duration::from_millis(250)));
    assert!(config.rpc_timeout.is_none());
}

#[test]
fn test_builder_order_independence() {
    // Test that builder methods can be called in any order
    let config1 = SemioscanConfigBuilder::new()
        .max_block_range(1000)
        .rate_limit_delay(Duration::from_millis(500))
        .build();

    let config2 = SemioscanConfigBuilder::new()
        .rate_limit_delay(Duration::from_millis(500))
        .max_block_range(1000)
        .build();

    // Both configs should be equivalent
    assert_eq!(config1.max_block_range, config2.max_block_range);
    assert_eq!(config1.rate_limit_delay, config2.rate_limit_delay);
}
