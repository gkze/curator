//! GitLab OAuth Device Flow implementation.
//!
//! This module provides functionality for authenticating users via GitLab's
//! Device Authorization Grant (RFC 8628), which is ideal for CLI applications.
//!
//! Requires GitLab 17.2+ (generally available since 17.9).
//!
//! # Device Flow Overview
//!
//! 1. Request a device code from GitLab
//! 2. Display the user code and verification URL to the user
//! 3. User visits the URL and enters the code on a device with browser access
//! 4. Poll GitLab until the user completes authorization
//! 5. Receive an access token
//!
//! # Example
//!
//! ```ignore
//! use curator::gitlab::oauth::{request_device_code, poll_for_token};
//!
//! // Step 1: Request device code
//! let device_code = request_device_code("https://gitlab.com", "read_api").await?;
//!
//! // Step 2: Show user the code
//! println!("Go to: {}", device_code.verification_uri);
//! println!("Enter code: {}", device_code.user_code);
//!
//! // Step 3: Poll for token
//! let token = poll_for_token("https://gitlab.com", &device_code).await?;
//! println!("Access token: {}", token.access_token);
//! ```

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

/// The Client ID for the Curator OAuth App on gitlab.com.
///
/// This is a public identifier and is safe to embed in the source code.
/// It identifies the Curator application to GitLab during OAuth flows.
pub const CLIENT_ID: &str = "eba8ea9cbb5e8ddd455a3b3db35871963d8aa6b0a344a4b8c8e34ae8d71f336f";

/// Errors that can occur during OAuth Device Flow.
#[derive(Debug, Error)]
pub enum OAuthError {
    /// HTTP request failed.
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// Failed to parse response from GitLab.
    #[error("Failed to parse response: {0}")]
    Parse(String),

    /// User did not authorize in time.
    #[error("Authorization expired. Please try again.")]
    Expired,

    /// User denied the authorization request.
    #[error("Authorization was denied by the user.")]
    AccessDenied,

    /// Too many polling requests (should not happen with proper interval).
    #[error("Too many requests. Please wait and try again.")]
    SlowDown,

    /// Unexpected error from GitLab.
    #[error("GitLab error: {0}")]
    GitLab(String),
}

/// Response from GitLab's device authorization endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct DeviceCodeResponse {
    /// The device verification code (sent to GitLab during polling).
    pub device_code: String,

    /// The code the user enters at the verification URL.
    pub user_code: String,

    /// The URL where the user enters the code.
    pub verification_uri: String,

    /// The complete URL with user_code pre-filled.
    #[serde(default)]
    pub verification_uri_complete: Option<String>,

    /// How long until the device code expires (in seconds).
    pub expires_in: u64,

    /// Minimum interval between polling requests (in seconds).
    pub interval: u64,
}

/// Successful access token response from GitLab.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AccessTokenResponse {
    /// The OAuth access token.
    pub access_token: String,

    /// The token type (usually "Bearer").
    pub token_type: String,

    /// How long until the token expires (in seconds).
    #[serde(default)]
    pub expires_in: Option<u64>,

    /// The granted scope.
    #[serde(default)]
    pub scope: Option<String>,

    /// When the token was created (Unix timestamp).
    #[serde(default)]
    pub created_at: Option<u64>,

    /// The refresh token for obtaining new access tokens.
    #[serde(default)]
    pub refresh_token: Option<String>,
}

/// Error response during token polling.
#[derive(Debug, Deserialize)]
struct TokenErrorResponse {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

/// Combined response type for token endpoint (can be success or error).
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TokenResponse {
    Success(AccessTokenResponse),
    Error(TokenErrorResponse),
}

/// Normalize a GitLab host into a base URL with scheme.
///
/// Accepts `"gitlab.com"`, `"https://gitlab.com"`, etc.
fn base_url(host: &str) -> String {
    let host = host.trim_end_matches('/');
    if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("https://{}", host)
    }
}

/// Request a device code from GitLab to start the OAuth Device Flow.
///
/// # Arguments
///
/// * `host` - The GitLab host (e.g., "gitlab.com" or "https://gitlab.example.com").
/// * `scope` - The OAuth scope(s) to request (e.g., "read_api").
///
/// # Returns
///
/// A [`DeviceCodeResponse`] containing the user code and verification URL.
pub async fn request_device_code(
    host: &str,
    scope: &str,
) -> Result<DeviceCodeResponse, OAuthError> {
    let client = Client::new();
    let url = format!("{}/oauth/authorize_device", base_url(host));

    let response = client
        .post(&url)
        .header("Accept", "application/json")
        .form(&[("client_id", CLIENT_ID), ("scope", scope)])
        .send()
        .await?;

    if !response.status().is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(OAuthError::GitLab(format!(
            "Failed to get device code: {}",
            text
        )));
    }

    response
        .json::<DeviceCodeResponse>()
        .await
        .map_err(|e| OAuthError::Parse(e.to_string()))
}

/// Poll GitLab for an access token after the user has been shown the device code.
///
/// This function will poll GitLab at the specified interval until:
/// - The user authorizes the application (returns the access token)
/// - The device code expires (returns [`OAuthError::Expired`])
/// - The user denies authorization (returns [`OAuthError::AccessDenied`])
///
/// # Arguments
///
/// * `host` - The GitLab host (e.g., "gitlab.com").
/// * `device_code` - The device code response from [`request_device_code`].
///
/// # Returns
///
/// An [`AccessTokenResponse`] containing the access token on success.
pub async fn poll_for_token(
    host: &str,
    device_code: &DeviceCodeResponse,
) -> Result<AccessTokenResponse, OAuthError> {
    let client = Client::new();
    let url = format!("{}/oauth/token", base_url(host));
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
            .post(&url)
            .header("Accept", "application/json")
            .form(&[
                ("client_id", CLIENT_ID),
                ("device_code", device_code.device_code.as_str()),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ])
            .send()
            .await?;

        if !response.status().is_success() {
            // GitLab returns 400 for pending/slow_down, parse the body
            let text = response.text().await.unwrap_or_default();
            // Try to parse as error response
            if let Ok(err) = serde_json::from_str::<TokenErrorResponse>(&text) {
                match err.error.as_str() {
                    "authorization_pending" => continue,
                    "slow_down" => {
                        interval += Duration::from_secs(5);
                        continue;
                    }
                    "expired_token" => return Err(OAuthError::Expired),
                    "access_denied" => return Err(OAuthError::AccessDenied),
                    _ => {
                        return Err(OAuthError::GitLab(
                            err.error_description.unwrap_or(err.error),
                        ));
                    }
                }
            }
            return Err(OAuthError::GitLab(format!(
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
                "authorization_pending" => continue,
                "slow_down" => {
                    interval += Duration::from_secs(5);
                    continue;
                }
                "expired_token" => return Err(OAuthError::Expired),
                "access_denied" => return Err(OAuthError::AccessDenied),
                _ => {
                    return Err(OAuthError::GitLab(
                        err.error_description.unwrap_or(err.error),
                    ));
                }
            },
        }
    }
}

/// Refresh an expired access token using a refresh token.
///
/// GitLab OAuth tokens expire after 2 hours. Use the `refresh_token` from
/// a previous [`AccessTokenResponse`] to obtain new tokens without requiring
/// user interaction.
///
/// # Arguments
///
/// * `host` - The GitLab host (e.g., "gitlab.com").
/// * `refresh_token` - The refresh token from a previous token response.
///
/// # Returns
///
/// A new [`AccessTokenResponse`] with fresh `access_token` and `refresh_token`.
/// The old tokens are invalidated.
pub async fn refresh_access_token(
    host: &str,
    refresh_token: &str,
) -> Result<AccessTokenResponse, OAuthError> {
    let client = Client::new();
    let url = format!("{}/oauth/token", base_url(host));

    let response = client
        .post(&url)
        .header("Accept", "application/json")
        .form(&[
            ("client_id", CLIENT_ID),
            ("refresh_token", refresh_token),
            ("grant_type", "refresh_token"),
        ])
        .send()
        .await?;

    if !response.status().is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(OAuthError::GitLab(format!(
            "Token refresh failed: {}",
            text
        )));
    }

    let token_response: TokenResponse = response
        .json()
        .await
        .map_err(|e| OAuthError::Parse(e.to_string()))?;

    match token_response {
        TokenResponse::Success(token) => Ok(token),
        TokenResponse::Error(err) => Err(OAuthError::GitLab(
            err.error_description.unwrap_or(err.error),
        )),
    }
}

/// Compute the expiry Unix timestamp from an [`AccessTokenResponse`].
///
/// Returns `None` if `created_at` or `expires_in` are missing.
pub fn token_expires_at(token: &AccessTokenResponse) -> Option<u64> {
    match (token.created_at, token.expires_in) {
        (Some(created), Some(expires)) => Some(created + expires),
        _ => None,
    }
}

/// Check if a token has expired (or will expire within the given buffer).
///
/// Returns `true` if the token is expired or the expiry time is unknown.
pub fn token_is_expired(expires_at: Option<u64>, buffer_secs: u64) -> bool {
    match expires_at {
        Some(exp) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            now + buffer_secs >= exp
        }
        // No expiry info — assume it might be a PAT (never expires)
        None => false,
    }
}

/// Request a device code with the default scope for Curator (api).
///
/// Uses the provided host. For gitlab.com, pass `"gitlab.com"`.
pub async fn request_device_code_default(host: &str) -> Result<DeviceCodeResponse, OAuthError> {
    request_device_code(host, "api").await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_id_is_set() {
        assert!(!CLIENT_ID.is_empty());
    }

    #[test]
    fn test_base_url_plain_host() {
        assert_eq!(base_url("gitlab.com"), "https://gitlab.com");
    }

    #[test]
    fn test_base_url_with_scheme() {
        assert_eq!(
            base_url("https://gitlab.example.com"),
            "https://gitlab.example.com"
        );
    }

    #[test]
    fn test_base_url_trailing_slash() {
        assert_eq!(base_url("gitlab.com/"), "https://gitlab.com");
    }

    #[test]
    fn test_device_code_response_deserialize() {
        let json = r#"{
            "device_code": "GmRhmhcxhwAzkoEqiMEg_DnyEysNkuNhszIySk9eS",
            "user_code": "0A44L90H",
            "verification_uri": "https://gitlab.com/oauth/device",
            "verification_uri_complete": "https://gitlab.com/oauth/device?user_code=0A44L90H",
            "expires_in": 300,
            "interval": 5
        }"#;

        let response: DeviceCodeResponse = serde_json::from_str(json).unwrap();
        assert_eq!(
            response.device_code,
            "GmRhmhcxhwAzkoEqiMEg_DnyEysNkuNhszIySk9eS"
        );
        assert_eq!(response.user_code, "0A44L90H");
        assert_eq!(response.verification_uri, "https://gitlab.com/oauth/device");
        assert_eq!(
            response.verification_uri_complete.as_deref(),
            Some("https://gitlab.com/oauth/device?user_code=0A44L90H")
        );
        assert_eq!(response.expires_in, 300);
        assert_eq!(response.interval, 5);
    }

    #[test]
    fn test_access_token_response_deserialize() {
        let json = r#"{
            "access_token": "TOKEN",
            "token_type": "Bearer",
            "expires_in": 7200,
            "scope": "api",
            "created_at": 1593096829
        }"#;

        let response: AccessTokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.access_token, "TOKEN");
        assert_eq!(response.token_type, "Bearer");
        assert_eq!(response.expires_in, Some(7200));
        assert_eq!(response.scope.as_deref(), Some("api"));
        assert_eq!(response.created_at, Some(1593096829));
        assert_eq!(response.refresh_token, None);
    }

    #[test]
    fn test_access_token_response_with_refresh_token() {
        let json = r#"{
            "access_token": "TOKEN",
            "token_type": "Bearer",
            "expires_in": 7200,
            "refresh_token": "REFRESH",
            "scope": "api",
            "created_at": 1593096829
        }"#;

        let response: AccessTokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.access_token, "TOKEN");
        assert_eq!(response.refresh_token.as_deref(), Some("REFRESH"));
    }

    #[test]
    fn test_token_expires_at_calculation() {
        let token = AccessTokenResponse {
            access_token: "t".into(),
            token_type: "Bearer".into(),
            expires_in: Some(7200),
            scope: Some("api".into()),
            created_at: Some(1000000),
            refresh_token: None,
        };
        assert_eq!(token_expires_at(&token), Some(1007200));
    }

    #[test]
    fn test_token_expires_at_missing_fields() {
        let token = AccessTokenResponse {
            access_token: "t".into(),
            token_type: "Bearer".into(),
            expires_in: None,
            scope: None,
            created_at: None,
            refresh_token: None,
        };
        assert_eq!(token_expires_at(&token), None);
    }

    #[test]
    fn test_token_is_expired_no_expiry() {
        // No expiry info — assume PAT, not expired
        assert!(!token_is_expired(None, 0));
    }

    #[test]
    fn test_token_is_expired_past() {
        // Expired long ago
        assert!(token_is_expired(Some(1000000), 0));
    }

    #[test]
    fn test_token_is_expired_future() {
        // Expires far in the future
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 100_000;
        assert!(!token_is_expired(Some(future), 0));
    }

    #[test]
    fn test_token_error_response_deserialize() {
        let json = r#"{
            "error": "authorization_pending",
            "error_description": "The authorization request is still pending."
        }"#;

        let response: TokenErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.error, "authorization_pending");
        assert_eq!(
            response.error_description.as_deref(),
            Some("The authorization request is still pending.")
        );
    }
}
