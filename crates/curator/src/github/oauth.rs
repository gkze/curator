//! GitHub OAuth Device Flow implementation.
//!
//! This module provides functionality for authenticating users via GitHub's
//! OAuth Device Flow, which is ideal for CLI applications.
//!
//! # Device Flow Overview
//!
//! 1. Request a device code from GitHub
//! 2. Display the user code and verification URL to the user
//! 3. User visits the URL and enters the code
//! 4. Poll GitHub until the user completes authorization
//! 5. Receive an access token
//!
//! # Example
//!
//! ```ignore
//! use curator::github::oauth::{request_device_code, poll_for_token};
//!
//! // Step 1: Request device code
//! let device_code = request_device_code("public_repo").await?;
//!
//! // Step 2: Show user the code
//! println!("Go to: {}", device_code.verification_uri);
//! println!("Enter code: {}", device_code.user_code);
//!
//! // Step 3: Poll for token
//! let token = poll_for_token(&device_code).await?;
//! println!("Access token: {}", token.access_token);
//! ```

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

/// The Client ID for the Curator OAuth App.
///
/// This is a public identifier and is safe to embed in the source code.
/// It identifies the Curator application to GitHub during OAuth flows.
pub const CLIENT_ID: &str = "Ov23liN0721EfoUpRrLl";

/// GitHub's device authorization endpoint.
const DEVICE_CODE_URL: &str = "https://github.com/login/device/code";

/// GitHub's OAuth token endpoint.
const TOKEN_URL: &str = "https://github.com/login/oauth/access_token";

/// Errors that can occur during OAuth Device Flow.
#[derive(Debug, Error)]
pub enum OAuthError {
    /// HTTP request failed.
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// Failed to parse response from GitHub.
    #[error("Failed to parse response: {0}")]
    Parse(String),

    /// User did not authorize in time.
    #[error("Authorization expired. Please try again.")]
    Expired,

    /// User denied the authorization request.
    #[error("Authorization was denied by the user.")]
    AccessDenied,

    /// The device code was not recognized (may have been used already).
    #[error("Invalid device code. Please restart the login process.")]
    InvalidDeviceCode,

    /// Too many polling requests (should not happen with proper interval).
    #[error("Too many requests. Please wait and try again.")]
    SlowDown,

    /// Unexpected error from GitHub.
    #[error("GitHub error: {0}")]
    GitHub(String),
}

/// Response from GitHub's device code endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct DeviceCodeResponse {
    /// The device verification code (sent to GitHub during polling).
    pub device_code: String,

    /// The code the user enters at the verification URL.
    pub user_code: String,

    /// The URL where the user enters the code.
    pub verification_uri: String,

    /// How long until the device code expires (in seconds).
    pub expires_in: u64,

    /// Minimum interval between polling requests (in seconds).
    pub interval: u64,
}

/// Successful access token response from GitHub.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AccessTokenResponse {
    /// The OAuth access token.
    pub access_token: String,

    /// The token type (usually "bearer").
    pub token_type: String,

    /// The granted scopes (space-separated).
    pub scope: String,
}

/// Error response during token polling.
#[derive(Debug, Deserialize)]
struct TokenErrorResponse {
    error: String,
    error_description: Option<String>,
    #[serde(default)]
    interval: Option<u64>,
}

/// Combined response type for token endpoint (can be success or error).
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TokenResponse {
    Success(AccessTokenResponse),
    Error(TokenErrorResponse),
}

/// Request a device code from GitHub to start the OAuth Device Flow.
///
/// # Arguments
///
/// * `scope` - The OAuth scope(s) to request (e.g., "public_repo" for starring public repos).
///
/// # Returns
///
/// A [`DeviceCodeResponse`] containing the user code and verification URL.
///
/// # Example
///
/// ```ignore
/// let device_code = request_device_code("public_repo").await?;
/// println!("Visit {} and enter code: {}", device_code.verification_uri, device_code.user_code);
/// ```
pub async fn request_device_code(scope: &str) -> Result<DeviceCodeResponse, OAuthError> {
    let client = Client::new();

    let response = client
        .post(DEVICE_CODE_URL)
        .header("Accept", "application/json")
        .form(&[("client_id", CLIENT_ID), ("scope", scope)])
        .send()
        .await?;

    if !response.status().is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(OAuthError::GitHub(format!(
            "Failed to get device code: {}",
            text
        )));
    }

    response
        .json::<DeviceCodeResponse>()
        .await
        .map_err(|e| OAuthError::Parse(e.to_string()))
}

/// Poll GitHub for an access token after the user has been shown the device code.
///
/// This function will poll GitHub at the specified interval until:
/// - The user authorizes the application (returns the access token)
/// - The device code expires (returns [`OAuthError::Expired`])
/// - The user denies authorization (returns [`OAuthError::AccessDenied`])
///
/// # Arguments
///
/// * `device_code` - The device code response from [`request_device_code`].
///
/// # Returns
///
/// An [`AccessTokenResponse`] containing the access token on success.
///
/// # Example
///
/// ```ignore
/// let token = poll_for_token(&device_code).await?;
/// // Save token.access_token for future API calls
/// ```
pub async fn poll_for_token(
    device_code: &DeviceCodeResponse,
) -> Result<AccessTokenResponse, OAuthError> {
    let client = Client::new();
    let mut interval = Duration::from_secs(device_code.interval);
    let deadline = std::time::Instant::now() + Duration::from_secs(device_code.expires_in);

    loop {
        // Check if we've exceeded the expiration time
        if std::time::Instant::now() >= deadline {
            return Err(OAuthError::Expired);
        }

        // Wait before polling
        tokio::time::sleep(interval).await;

        let response = client
            .post(TOKEN_URL)
            .header("Accept", "application/json")
            .form(&[
                ("client_id", CLIENT_ID),
                ("device_code", device_code.device_code.as_str()),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ])
            .send()
            .await?;

        if !response.status().is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(OAuthError::GitHub(format!(
                "Token request failed: {}",
                text
            )));
        }

        let token_response: TokenResponse = response
            .json()
            .await
            .map_err(|e| OAuthError::Parse(e.to_string()))?;

        match token_response {
            TokenResponse::Success(token) => return Ok(token),
            TokenResponse::Error(err) => match err.error.as_str() {
                "authorization_pending" => {
                    // User hasn't authorized yet, keep polling
                    continue;
                }
                "slow_down" => {
                    // Increase interval as requested
                    if let Some(new_interval) = err.interval {
                        interval = Duration::from_secs(new_interval);
                    } else {
                        interval += Duration::from_secs(5);
                    }
                    continue;
                }
                "expired_token" => return Err(OAuthError::Expired),
                "access_denied" => return Err(OAuthError::AccessDenied),
                "incorrect_device_code" => return Err(OAuthError::InvalidDeviceCode),
                _ => {
                    return Err(OAuthError::GitHub(
                        err.error_description.unwrap_or(err.error),
                    ));
                }
            },
        }
    }
}

/// Request a device code with the default scope for Curator (public_repo).
///
/// This is a convenience function that requests the minimal scope needed
/// for Curator's functionality (starring public repositories).
pub async fn request_device_code_default() -> Result<DeviceCodeResponse, OAuthError> {
    request_device_code("public_repo").await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_id_is_set() {
        assert!(!CLIENT_ID.is_empty());
        assert_eq!(CLIENT_ID, "Ov23liN0721EfoUpRrLl");
    }

    #[test]
    fn test_device_code_response_deserialize() {
        let json = r#"{
            "device_code": "abc123",
            "user_code": "ABCD-1234",
            "verification_uri": "https://github.com/login/device",
            "expires_in": 900,
            "interval": 5
        }"#;

        let response: DeviceCodeResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.device_code, "abc123");
        assert_eq!(response.user_code, "ABCD-1234");
        assert_eq!(response.verification_uri, "https://github.com/login/device");
        assert_eq!(response.expires_in, 900);
        assert_eq!(response.interval, 5);
    }

    #[test]
    fn test_access_token_response_deserialize() {
        let json = r#"{
            "access_token": "gho_xxxxxxxxxxxx",
            "token_type": "bearer",
            "scope": "public_repo"
        }"#;

        let response: AccessTokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.access_token, "gho_xxxxxxxxxxxx");
        assert_eq!(response.token_type, "bearer");
        assert_eq!(response.scope, "public_repo");
    }
}
