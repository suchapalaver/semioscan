// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Neutral block-range log scanning primitives.
//!
//! Provides a single canonical implementation of "iterate a block range in
//! chain-sized chunks, fetch logs for each chunk, apply a rate-limit delay
//! between calls". Gas, price, and combined retrieval all share this primitive
//! so the chunking, rate limiting, and tracing behavior cannot drift between
//! domains.
//!
//! The error policy is supplied by the caller as a closure that maps each
//! per-chunk transport error to either `None` (continue with the next chunk)
//! or `Some(domain_error)` (fail the whole scan with that error). This lets
//! the same primitive serve `EventScanner`'s continue-on-error semantics and
//! the fail-fast semantics needed by gas and retrieval, without locking the
//! scanner to a shared error type.

pub mod logs;

pub use logs::LogScanner;
