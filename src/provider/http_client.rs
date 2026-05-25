// SPDX-FileCopyrightText: 2025 Semiotic AI, Inc.
//
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for constructing the underlying HTTP transport client.

use std::time::Duration;

use alloy_transport_http::reqwest;

use crate::errors::RpcError;

/// Build a `reqwest::Client` with the requested per-request timeout.
pub(super) fn reqwest_client_with_timeout(timeout: Duration) -> Result<reqwest::Client, RpcError> {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| {
            RpcError::ProviderConnectionFailed(format!("reqwest client build failed: {e}"))
        })
}
