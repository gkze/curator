//! Shared OAuth utilities for all platforms.
//!
//! This module provides common OAuth functionality used across GitHub, GitLab,
//! and Gitea/Codeberg platforms:
//!
//! - [`error::OAuthError`] - Unified error type for OAuth operations
//! - [`callback`] - Local HTTP callback server for PKCE flows
//! - Token utilities (expiry checking, refresh)
//!
//! # OAuth Flows Supported
//!
//! | Platform     | Flow Type                    | Module              |
//! |--------------|------------------------------|---------------------|
//! | GitHub       | Device Authorization Grant   | [`crate::github::oauth`] |
//! | GitLab       | Device Authorization Grant   | [`crate::gitlab::oauth`] |
//! | Gitea        | Authorization Code + PKCE    | [`crate::gitea::oauth`]  |
//! | Codeberg     | Authorization Code + PKCE    | [`crate::gitea::oauth`]  |
//!
//! # Example: PKCE Flow (Gitea/Codeberg)
//!
//! ```ignore
//! use curator::oauth::{CallbackServer, DEFAULT_CALLBACK_PORT, redirect_uri};
//! use oauth2::{PkceCodeChallenge, CsrfToken};
//! use std::time::Duration;
//!
//! // Generate PKCE challenge
//! let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
//! let state = CsrfToken::new_random();
//!
//! // Build authorization URL and open in browser
//! let auth_url = build_auth_url(&pkce_challenge, state.secret());
//! open::that(&auth_url)?;
//!
//! // Wait for callback
//! let server = CallbackServer::new(DEFAULT_CALLBACK_PORT, state.secret());
//! let code = server.wait_for_code(Duration::from_secs(300)).await?;
//!
//! // Exchange code for token
//! let token = exchange_code(&code, &pkce_verifier).await?;
//! ```

mod error;

#[cfg(feature = "gitea")]
pub mod callback;

pub use error::OAuthError;

#[cfg(feature = "gitea")]
pub use callback::{CallbackServer, DEFAULT_CALLBACK_PORT, redirect_uri};

/// Check if a token has expired (or will expire within the given buffer).
///
/// Returns `true` if the token is expired or will expire within `buffer_secs`.
/// Returns `false` if `expires_at` is `None` (assumes it's a long-lived token like a PAT).
///
/// # Arguments
///
/// * `expires_at` - Unix timestamp when the token expires, or `None` for PATs.
/// * `buffer_secs` - Number of seconds before expiry to consider the token expired.
///
/// # Example
///
/// ```
/// use curator::oauth::token_is_expired;
///
/// // Token expires in 10 minutes, check with 5 minute buffer
/// let expires_at = std::time::SystemTime::now()
///     .duration_since(std::time::UNIX_EPOCH)
///     .unwrap()
///     .as_secs() + 600;
///
/// assert!(!token_is_expired(Some(expires_at), 300)); // Still valid
/// assert!(token_is_expired(Some(expires_at), 700));  // Within buffer
/// ```
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

/// Compute the expiry Unix timestamp from created_at and expires_in.
///
/// Returns `None` if either value is `None`.
///
/// # Example
///
/// ```
/// use curator::oauth::compute_expires_at;
///
/// let expires_at = compute_expires_at(Some(1000000), Some(7200));
/// assert_eq!(expires_at, Some(1007200));
/// ```
pub fn compute_expires_at(created_at: Option<u64>, expires_in: Option<u64>) -> Option<u64> {
    match (created_at, expires_in) {
        (Some(created), Some(expires)) => Some(created + expires),
        _ => None,
    }
}

/// Normalize a host string into a base URL with HTTPS scheme.
///
/// Accepts bare hostnames, hostnames with scheme, and URLs with trailing slashes.
///
/// # Example
///
/// ```
/// use curator::oauth::normalize_host;
///
/// assert_eq!(normalize_host("gitlab.com"), "https://gitlab.com");
/// assert_eq!(normalize_host("https://gitlab.com/"), "https://gitlab.com");
/// assert_eq!(normalize_host("http://localhost:3000"), "http://localhost:3000");
/// ```
pub fn normalize_host(host: &str) -> String {
    let host = host.trim_end_matches('/');
    if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("https://{}", host)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_is_expired_no_expiry() {
        // No expiry info — assume PAT, not expired
        assert!(!token_is_expired(None, 0));
        assert!(!token_is_expired(None, 300));
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
        assert!(!token_is_expired(Some(future), 300));
    }

    #[test]
    fn test_token_is_expired_within_buffer() {
        // Expires in 60 seconds, but buffer is 300
        let soon = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        assert!(token_is_expired(Some(soon), 300));
    }

    #[test]
    fn test_compute_expires_at() {
        assert_eq!(compute_expires_at(Some(1000000), Some(7200)), Some(1007200));
        assert_eq!(compute_expires_at(None, Some(7200)), None);
        assert_eq!(compute_expires_at(Some(1000000), None), None);
        assert_eq!(compute_expires_at(None, None), None);
    }

    #[test]
    fn test_normalize_host_plain() {
        assert_eq!(normalize_host("gitlab.com"), "https://gitlab.com");
        assert_eq!(normalize_host("codeberg.org"), "https://codeberg.org");
    }

    #[test]
    fn test_normalize_host_with_scheme() {
        assert_eq!(
            normalize_host("https://gitlab.example.com"),
            "https://gitlab.example.com"
        );
        assert_eq!(
            normalize_host("http://localhost:3000"),
            "http://localhost:3000"
        );
    }

    #[test]
    fn test_normalize_host_trailing_slash() {
        assert_eq!(normalize_host("gitlab.com/"), "https://gitlab.com");
        assert_eq!(normalize_host("https://gitlab.com/"), "https://gitlab.com");
    }
}
