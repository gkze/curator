//! Shared OAuth error types.
//!
//! This module provides a unified error type for OAuth operations across
//! all supported platforms (GitHub, GitLab, Gitea/Codeberg).

use thiserror::Error;

/// Errors that can occur during OAuth flows.
#[derive(Debug, Error)]
pub enum OAuthError {
    /// HTTP request failed.
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// Failed to parse response from the OAuth provider.
    #[error("Failed to parse response: {0}")]
    Parse(String),

    /// User did not authorize in time (device flow) or callback timed out (PKCE).
    #[error("Authorization expired. Please try again.")]
    Expired,

    /// User denied the authorization request.
    #[error("Authorization was denied by the user.")]
    AccessDenied,

    /// The device code was not recognized (device flow).
    #[error("Invalid device code. Please restart the login process.")]
    InvalidDeviceCode,

    /// Invalid state parameter (CSRF protection failed).
    #[error("Invalid state parameter. This may be a CSRF attack.")]
    InvalidState,

    /// Too many polling requests (should not happen with proper interval).
    #[error("Too many requests. Please wait and try again.")]
    SlowDown,

    /// The callback server failed.
    #[error("Callback server error: {0}")]
    Server(String),

    /// OAuth configuration error.
    #[error("OAuth configuration error: {0}")]
    Configuration(String),

    /// Unexpected error from the OAuth provider.
    #[error("{provider} error: {message}")]
    Provider {
        /// The platform name (e.g., "GitHub", "GitLab", "Gitea").
        provider: &'static str,
        /// The error message.
        message: String,
    },
}

impl OAuthError {
    /// Create a provider-specific error.
    pub fn provider(provider: &'static str, message: impl Into<String>) -> Self {
        Self::Provider {
            provider,
            message: message.into(),
        }
    }

    /// Create a GitHub-specific error.
    pub fn github(message: impl Into<String>) -> Self {
        Self::provider("GitHub", message)
    }

    /// Create a GitLab-specific error.
    pub fn gitlab(message: impl Into<String>) -> Self {
        Self::provider("GitLab", message)
    }

    /// Create a Gitea-specific error.
    pub fn gitea(message: impl Into<String>) -> Self {
        Self::provider("Gitea", message)
    }

    /// Create a Codeberg-specific error.
    pub fn codeberg(message: impl Into<String>) -> Self {
        Self::provider("Codeberg", message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_helpers_set_provider_and_message() {
        let err = OAuthError::provider("GitHub", "nope");
        match err {
            OAuthError::Provider { provider, message } => {
                assert_eq!(provider, "GitHub");
                assert_eq!(message, "nope");
            }
            other => panic!("expected Provider variant, got {other:?}"),
        }

        let err = OAuthError::github("bad");
        assert_eq!(err.to_string(), "GitHub error: bad");

        let err = OAuthError::gitlab("bad");
        assert_eq!(err.to_string(), "GitLab error: bad");

        let err = OAuthError::gitea("bad");
        assert_eq!(err.to_string(), "Gitea error: bad");

        let err = OAuthError::codeberg("bad");
        assert_eq!(err.to_string(), "Codeberg error: bad");
    }

    #[test]
    fn display_messages_cover_simple_variants() {
        assert_eq!(
            OAuthError::Expired.to_string(),
            "Authorization expired. Please try again."
        );
        assert_eq!(
            OAuthError::AccessDenied.to_string(),
            "Authorization was denied by the user."
        );
        assert_eq!(
            OAuthError::InvalidDeviceCode.to_string(),
            "Invalid device code. Please restart the login process."
        );
        assert_eq!(
            OAuthError::InvalidState.to_string(),
            "Invalid state parameter. This may be a CSRF attack."
        );
        assert_eq!(
            OAuthError::SlowDown.to_string(),
            "Too many requests. Please wait and try again."
        );
        assert_eq!(
            OAuthError::Server("boom".to_string()).to_string(),
            "Callback server error: boom"
        );
        assert_eq!(
            OAuthError::Configuration("missing".to_string()).to_string(),
            "OAuth configuration error: missing"
        );
        assert_eq!(
            OAuthError::Parse("bad json".to_string()).to_string(),
            "Failed to parse response: bad json"
        );
    }

    #[tokio::test]
    async fn http_variant_formats_as_http_request_failed() {
        // Use an invalid URL so we fail immediately without real network I/O.
        let err = reqwest::Client::new()
            .get("http://")
            .send()
            .await
            .expect_err("invalid URL should error");
        let oauth: OAuthError = err.into();
        assert!(oauth.to_string().contains("HTTP request failed:"));
    }
}
