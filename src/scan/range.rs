// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
// SPDX-FileCopyrightText: 2026 Joseph Livesey <jlivesey@gmail.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Finite block-range normalization for chunked scans.

use alloy_primitives::BlockNumber;
use std::fmt;
use std::ops::{Bound, RangeBounds};

/// Inclusive numeric block range accepted by chunked log scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FiniteBlockRange {
    pub(crate) start_block: BlockNumber,
    pub(crate) end_block: BlockNumber,
}

/// Error returned when a [`RangeBounds<BlockNumber>`] cannot be normalized into
/// a finite non-empty inclusive block range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockRangeError {
    MissingStart,
    MissingEnd,
    ExcludedStartOverflow,
    ExcludedEndUnderflow,
    EmptyOrInverted {
        start_block: BlockNumber,
        end_block: BlockNumber,
    },
}

impl fmt::Display for BlockRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingStart => {
                write!(f, "block range must have a finite numeric start bound")
            }
            Self::MissingEnd => write!(f, "block range must have a finite numeric end bound"),
            Self::ExcludedStartOverflow => write!(
                f,
                "block range excluded start bound cannot advance past u64::MAX"
            ),
            Self::ExcludedEndUnderflow => {
                write!(f, "block range excluded end bound cannot retreat below 0")
            }
            Self::EmptyOrInverted {
                start_block,
                end_block,
            } => write!(
                f,
                "block range must be non-empty and ordered: start_block {start_block} > end_block {end_block}"
            ),
        }
    }
}

impl std::error::Error for BlockRangeError {}

/// Normalize any finite [`RangeBounds<BlockNumber>`] into the inclusive
/// `(start_block, end_block)` shape used by the chunking loop.
pub(crate) fn finite_block_range<R>(range: R) -> Result<FiniteBlockRange, BlockRangeError>
where
    R: RangeBounds<BlockNumber>,
{
    let start_block = match range.start_bound() {
        Bound::Included(start) => *start,
        Bound::Excluded(start) => start
            .checked_add(1)
            .ok_or(BlockRangeError::ExcludedStartOverflow)?,
        Bound::Unbounded => return Err(BlockRangeError::MissingStart),
    };

    let end_block = match range.end_bound() {
        Bound::Included(end) => *end,
        Bound::Excluded(end) => end
            .checked_sub(1)
            .ok_or(BlockRangeError::ExcludedEndUnderflow)?,
        Bound::Unbounded => return Err(BlockRangeError::MissingEnd),
    };

    if start_block > end_block {
        return Err(BlockRangeError::EmptyOrInverted {
            start_block,
            end_block,
        });
    }

    Ok(FiniteBlockRange {
        start_block,
        end_block,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inclusive_range_preserves_bounds() {
        assert_eq!(
            finite_block_range(10..=20).expect("inclusive range"),
            FiniteBlockRange {
                start_block: 10,
                end_block: 20,
            }
        );
    }

    #[test]
    fn exclusive_bounds_normalize_to_inclusive_window() {
        assert_eq!(
            finite_block_range(10..20).expect("exclusive end range"),
            FiniteBlockRange {
                start_block: 10,
                end_block: 19,
            }
        );
        assert_eq!(
            finite_block_range((std::ops::Bound::Excluded(10), std::ops::Bound::Included(20)))
                .expect("excluded start range"),
            FiniteBlockRange {
                start_block: 11,
                end_block: 20,
            }
        );
    }

    #[test]
    fn open_ended_ranges_are_rejected() {
        assert_eq!(finite_block_range(10..), Err(BlockRangeError::MissingEnd));
        assert_eq!(
            finite_block_range(..=20),
            Err(BlockRangeError::MissingStart)
        );
        assert_eq!(finite_block_range(..), Err(BlockRangeError::MissingStart));
    }

    #[test]
    fn empty_or_inverted_ranges_are_rejected() {
        let inverted_start = 20;
        let inverted_end = 10;

        assert_eq!(
            finite_block_range(inverted_start..=inverted_end),
            Err(BlockRangeError::EmptyOrInverted {
                start_block: 20,
                end_block: 10,
            })
        );
        assert_eq!(
            finite_block_range(10..10),
            Err(BlockRangeError::EmptyOrInverted {
                start_block: 10,
                end_block: 9,
            })
        );
    }

    #[test]
    fn excluded_bound_overflow_and_underflow_are_rejected() {
        assert_eq!(
            finite_block_range((
                std::ops::Bound::Excluded(u64::MAX),
                std::ops::Bound::Included(u64::MAX),
            )),
            Err(BlockRangeError::ExcludedStartOverflow)
        );
        assert_eq!(
            finite_block_range((std::ops::Bound::Included(0), std::ops::Bound::Excluded(0))),
            Err(BlockRangeError::ExcludedEndUnderflow)
        );
    }
}
