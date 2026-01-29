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
