// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Strong types for configuration values
//!
//! These types ensure configuration values are not confused with
//! blockchain values (block numbers, gas amounts, etc.).

use serde::{Deserialize, Serialize};
use std::ops::{Add, AddAssign};

/// Maximum block range for RPC queries
///
/// This prevents overloading RPC nodes with queries that are too large.
/// Different chains have different limits based on their RPC infrastructure.
///
/// Typical values:
/// - Conservative: 2000 blocks (works on most chains)
/// - Moderate: 5000 blocks
/// - Generous: 10000 blocks (chains with robust RPC like Base)
///
/// # Examples
///
/// ```
/// use semioscan::MaxBlockRange;
///
/// let conservative = MaxBlockRange::DEFAULT;
/// assert_eq!(conservative.as_u64(), 2000);
///
/// let generous = MaxBlockRange::GENEROUS;
/// assert_eq!(generous.as_u64(), 10000);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MaxBlockRange(u64);

impl MaxBlockRange {
    /// Conservative default (works on most chains)
    pub const DEFAULT: Self = Self(2000);

    /// Moderate range for chains with good RPC support
    pub const MODERATE: Self = Self(5000);

    /// For chains with generous RPC limits (e.g., Base)
    pub const GENEROUS: Self = Self(10000);

    /// Very conservative for rate-limited RPCs
    pub const CONSERVATIVE: Self = Self(1000);

    /// Create a new max block range
    ///
    /// # Examples
    ///
    /// ```
    /// use semioscan::MaxBlockRange;
    ///
    /// let range = MaxBlockRange::new(3000);
    /// assert_eq!(range.as_u64(), 3000);
    /// ```
    pub const fn new(blocks: u64) -> Self {
        Self(blocks)
    }

    /// Get the inner u64 value
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for MaxBlockRange {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for MaxBlockRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} blocks", self.0)
    }
}

/// Represents a count of blockchain transactions
///
/// This type prevents confusion between transaction counts and other numeric values.
/// Using `usize` internally ensures counts cannot be negative.
///
/// # Examples
///
/// ```
/// use semioscan::TransactionCount;
///
/// let count = TransactionCount::new(5);
/// assert_eq!(count.as_usize(), 5);
///
/// let total = count + TransactionCount::new(3);
/// assert_eq!(total.as_usize(), 8);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct TransactionCount(usize);

impl TransactionCount {
    /// Zero transactions
    pub const ZERO: Self = Self(0);

    /// Create a new transaction count
    ///
    /// # Examples
    ///
    /// ```
    /// use semioscan::TransactionCount;
    ///
    /// let count = TransactionCount::new(10);
    /// assert_eq!(count.as_usize(), 10);
    /// ```
    pub const fn new(count: usize) -> Self {
        Self(count)
    }

    /// Get the inner usize value
    pub const fn as_usize(&self) -> usize {
        self.0
    }

    /// Increment the count by one
    ///
    /// Uses saturating addition to prevent overflow.
    pub fn increment(&mut self) {
        self.0 = self.0.saturating_add(1);
    }

    /// Check if count is zero
    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl From<usize> for TransactionCount {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

impl Add for TransactionCount {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0.saturating_add(rhs.0))
    }
}

impl AddAssign for TransactionCount {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0.saturating_add(rhs.0);
    }
}

impl std::fmt::Display for TransactionCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} transactions", self.0)
    }
}

/// Represents a count of blocks (not a block number)
///
/// This type prevents confusion between block counts (a quantity of blocks)
/// and block numbers (a position in the blockchain). For example, a block
/// window from block 100 to 110 has a count of 11 blocks, not 10.
///
/// # Examples
///
/// ```
/// use semioscan::BlockCount;
///
/// let count = BlockCount::new(100);
/// assert_eq!(count.as_u64(), 100);
///
/// let total = count + BlockCount::new(50);
/// assert_eq!(total.as_u64(), 150);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct BlockCount(u64);

impl BlockCount {
    /// Zero blocks
    pub const ZERO: Self = Self(0);

    /// Create a new block count
    ///
    /// # Examples
    ///
    /// ```
    /// use semioscan::BlockCount;
    ///
    /// let count = BlockCount::new(1000);
    /// assert_eq!(count.as_u64(), 1000);
    /// ```
    pub const fn new(count: u64) -> Self {
        Self(count)
    }

    /// Get the inner u64 value
    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    /// Check if count is zero
    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl From<u64> for BlockCount {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Add for BlockCount {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0.saturating_add(rhs.0))
    }
}

impl AddAssign for BlockCount {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0.saturating_add(rhs.0);
    }
}

impl std::fmt::Display for BlockCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 == 1 {
            write!(f, "1 block")
        } else {
            write!(f, "{} blocks", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_block_range_creation() {
        let range = MaxBlockRange::new(2000);
        assert_eq!(range.as_u64(), 2000);
    }

    #[test]
    fn test_max_block_range_constants() {
        assert_eq!(MaxBlockRange::CONSERVATIVE.as_u64(), 1000);
        assert_eq!(MaxBlockRange::DEFAULT.as_u64(), 2000);
        assert_eq!(MaxBlockRange::MODERATE.as_u64(), 5000);
        assert_eq!(MaxBlockRange::GENEROUS.as_u64(), 10000);
    }

    #[test]
    fn test_display() {
        let range = MaxBlockRange::new(2000);
        assert_eq!(format!("{}", range), "2000 blocks");
    }

    #[test]
    fn test_serialization() {
        let range = MaxBlockRange::new(2000);
        let json = serde_json::to_string(&range).unwrap();
        let deserialized: MaxBlockRange = serde_json::from_str(&json).unwrap();
        assert_eq!(range, deserialized);
    }

    #[test]
    fn test_conversions() {
        let u64_val = 2000u64;
        let range: MaxBlockRange = u64_val.into();
        let back: u64 = range.as_u64();
        assert_eq!(u64_val, back);
    }

    #[test]
    fn test_ordering() {
        let small = MaxBlockRange::CONSERVATIVE;
        let medium = MaxBlockRange::DEFAULT;
        let large = MaxBlockRange::GENEROUS;

        assert!(small < medium);
        assert!(medium < large);
        assert!(small < large);
    }

    // TransactionCount tests
    #[test]
    fn test_transaction_count_creation() {
        let count = TransactionCount::new(5);
        assert_eq!(count.as_usize(), 5);
    }

    #[test]
    fn test_transaction_count_zero() {
        assert!(TransactionCount::ZERO.is_zero());
        assert_eq!(TransactionCount::ZERO.as_usize(), 0);
    }

    #[test]
    fn test_transaction_count_addition() {
        let a = TransactionCount::new(5);
        let b = TransactionCount::new(3);
        let sum = a + b;
        assert_eq!(sum.as_usize(), 8);
    }

    #[test]
    fn test_transaction_count_increment() {
        let mut count = TransactionCount::new(5);
        count.increment();
        assert_eq!(count.as_usize(), 6);
    }

    #[test]
    fn test_transaction_count_saturating_addition() {
        let max_count = TransactionCount::new(usize::MAX);
        let small_count = TransactionCount::new(1);
        let result = max_count + small_count;
        assert_eq!(result.as_usize(), usize::MAX);
    }

    #[test]
    fn test_transaction_count_saturating_increment() {
        let mut count = TransactionCount::new(usize::MAX);
        count.increment();
        assert_eq!(count.as_usize(), usize::MAX);
    }

    #[test]
    fn test_transaction_count_display() {
        let count = TransactionCount::new(42);
        assert_eq!(format!("{}", count), "42 transactions");
    }

    #[test]
    fn test_transaction_count_serialization() {
        let count = TransactionCount::new(10);
        let json = serde_json::to_string(&count).unwrap();
        let deserialized: TransactionCount = serde_json::from_str(&json).unwrap();
        assert_eq!(count, deserialized);
    }

    #[test]
    fn test_transaction_count_conversions() {
        let usize_val = 42usize;
        let count: TransactionCount = usize_val.into();
        let back: usize = count.as_usize();
        assert_eq!(usize_val, back);
    }

    #[test]
    fn test_transaction_count_ordering() {
        let small = TransactionCount::new(5);
        let medium = TransactionCount::new(10);
        let large = TransactionCount::new(20);

        assert!(small < medium);
        assert!(medium < large);
        assert!(small < large);
    }
}
