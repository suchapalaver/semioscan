// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Pure normalisation of raw [`SwapData`] amounts.
//!
//! The helpers here turn raw `U256` token amounts into [`NormalizedAmount`]
//! and [`UsdValue`] using already-known [`TokenDecimals`]. No provider or
//! network access — callers are expected to populate decimals beforehand
//! via [`crate::price::decimals::TokenMetadataProvider`].

use alloy_primitives::Address;

use crate::price::SwapData;
use crate::{NormalizedAmount, TokenAmount, TokenDecimals, UsdValue};

/// Normalised target-token amount paired with the USDC counterpart for a
/// single swap.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SwapAmounts {
    pub token_amount: NormalizedAmount,
    pub usdc_amount: UsdValue,
}

/// Normalise both legs of a swap using the supplied decimals.
///
/// Returned values mirror the input amounts and are useful when the caller
/// wants per-swap detail rather than a price-against-USDC view.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NormalizedSwap {
    pub token_in_amount: NormalizedAmount,
    pub token_out_amount: NormalizedAmount,
    pub token_in_decimals: TokenDecimals,
    pub token_out_decimals: TokenDecimals,
}

/// `true` when one leg of `swap` is `target_token` and the other is `usdc`,
/// in either direction.
pub(crate) fn involves_pair(swap: &SwapData, target_token: Address, usdc: Address) -> bool {
    (swap.token_in == target_token && swap.token_out == usdc)
        || (swap.token_in == usdc && swap.token_out == target_token)
}

/// Normalise a swap that pairs `target_token` against `usdc` in either
/// direction.
///
/// Returns `Some` when one leg of the swap is `target_token` and the other
/// is `usdc`; `None` otherwise. The returned `token_amount` is always the
/// target-token leg (in/out depending on direction) and `usdc_amount`
/// always the stablecoin leg.
pub(crate) fn normalize_against_pair(
    swap: &SwapData,
    target_token: Address,
    usdc: Address,
    target_decimals: TokenDecimals,
    usdc_decimals: TokenDecimals,
) -> Option<SwapAmounts> {
    if swap.token_in == target_token && swap.token_out == usdc {
        let token_amount = TokenAmount::new(swap.token_in_amount).normalize(target_decimals);
        let usdc_amount = TokenAmount::new(swap.token_out_amount).normalize(usdc_decimals);
        return Some(SwapAmounts {
            token_amount,
            usdc_amount: UsdValue::new(usdc_amount.as_f64()),
        });
    }

    if swap.token_in == usdc && swap.token_out == target_token {
        let token_amount = TokenAmount::new(swap.token_out_amount).normalize(target_decimals);
        let usdc_amount = TokenAmount::new(swap.token_in_amount).normalize(usdc_decimals);
        return Some(SwapAmounts {
            token_amount,
            usdc_amount: UsdValue::new(usdc_amount.as_f64()),
        });
    }

    None
}

/// Normalise both legs of `swap` using the supplied decimals.
pub(crate) fn normalize_swap(
    swap: &SwapData,
    token_in_decimals: TokenDecimals,
    token_out_decimals: TokenDecimals,
) -> NormalizedSwap {
    NormalizedSwap {
        token_in_amount: TokenAmount::new(swap.token_in_amount).normalize(token_in_decimals),
        token_out_amount: TokenAmount::new(swap.token_out_amount).normalize(token_out_decimals),
        token_in_decimals,
        token_out_decimals,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, U256};

    fn token() -> Address {
        address!("1111111111111111111111111111111111111111")
    }

    fn usdc() -> Address {
        address!("2222222222222222222222222222222222222222")
    }

    fn other() -> Address {
        address!("3333333333333333333333333333333333333333")
    }

    fn swap(token_in: Address, in_amt: u128, token_out: Address, out_amt: u128) -> SwapData {
        SwapData {
            token_in,
            token_in_amount: U256::from(in_amt),
            token_out,
            token_out_amount: U256::from(out_amt),
            sender: None,
            tx_hash: None,
            block_number: None,
        }
    }

    #[test]
    fn pair_normalises_sell_direction() {
        // Selling 1.5 target (18 decimals) for 3.0 USDC (6 decimals)
        let s = swap(
            token(),
            1_500_000_000_000_000_000u128,
            usdc(),
            3_000_000u128,
        );

        let amounts = normalize_against_pair(
            &s,
            token(),
            usdc(),
            TokenDecimals::new(18),
            TokenDecimals::new(6),
        )
        .expect("pair-relevant swap normalises");

        assert!((amounts.token_amount.as_f64() - 1.5).abs() < 1e-9);
        assert!((amounts.usdc_amount.as_f64() - 3.0).abs() < 1e-9);
    }

    #[test]
    fn pair_normalises_buy_direction() {
        // Buying 0.5 target for 2.0 USDC (USDC -> target)
        let s = swap(usdc(), 2_000_000u128, token(), 500_000_000_000_000_000u128);

        let amounts = normalize_against_pair(
            &s,
            token(),
            usdc(),
            TokenDecimals::new(18),
            TokenDecimals::new(6),
        )
        .expect("reverse-direction swap normalises");

        assert!((amounts.token_amount.as_f64() - 0.5).abs() < 1e-9);
        assert!((amounts.usdc_amount.as_f64() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn pair_returns_none_for_irrelevant_swap() {
        let s = swap(token(), 1, other(), 1);

        assert!(normalize_against_pair(
            &s,
            token(),
            usdc(),
            TokenDecimals::new(18),
            TokenDecimals::new(6),
        )
        .is_none());

        let s2 = swap(other(), 1, other(), 1);
        assert!(normalize_against_pair(
            &s2,
            token(),
            usdc(),
            TokenDecimals::new(18),
            TokenDecimals::new(6),
        )
        .is_none());
    }

    #[test]
    fn involves_pair_matches_both_directions_only() {
        let sell = swap(token(), 1, usdc(), 1);
        let buy = swap(usdc(), 1, token(), 1);
        let unrelated = swap(token(), 1, other(), 1);

        assert!(involves_pair(&sell, token(), usdc()));
        assert!(involves_pair(&buy, token(), usdc()));
        assert!(!involves_pair(&unrelated, token(), usdc()));
    }

    #[test]
    fn normalize_swap_normalises_both_legs() {
        let s = swap(
            token(),
            1_000_000_000_000_000_000u128,
            usdc(),
            2_000_000u128,
        );

        let n = normalize_swap(&s, TokenDecimals::new(18), TokenDecimals::new(6));

        assert!((n.token_in_amount.as_f64() - 1.0).abs() < 1e-9);
        assert!((n.token_out_amount.as_f64() - 2.0).abs() < 1e-9);
        assert_eq!(n.token_in_decimals, TokenDecimals::new(18));
        assert_eq!(n.token_out_decimals, TokenDecimals::new(6));
    }
}
