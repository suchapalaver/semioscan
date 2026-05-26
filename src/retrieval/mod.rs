// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Data retrieval orchestration.
//!
//! This module handles coordinated retrieval of blockchain data including:
//! - Combined gas and price data extraction
//! - Transfer amount calculations
//! - Decimal precision handling
//! - Batch balance fetching

// Combined retrieval sub-modules
pub mod balance;
mod calculator;
mod failure;
mod gas_calculation;
mod gas_extractor;
mod transfer_log_scanner;
mod tx_receipt_enricher;
mod types;
mod utils;

// Re-export public API
pub use balance::{
    batch_fetch_balances, batch_fetch_eth_balances, BalanceError, BalanceQuery, BalanceResult,
};
pub use calculator::CombinedCalculator;
pub use types::{
    CombinedDataLookupAttempt, CombinedDataLookupFailure, CombinedDataLookupPass,
    CombinedDataLookupStage, CombinedDataResult, CombinedDataRetrievalMetadata, GasAndAmountForTx,
};
pub use utils::{get_token_decimal_precision, u256_to_bigdecimal};
