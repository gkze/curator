//! GitHub API error types.

use chrono::{DateTime, Utc};
use thiserror::Error;

/// Errors that can occur when interacting with the GitHub API.
#[derive(Debug, Error)]
pub enum GitHubError {
    #[error("GitHub API error: {0}")]
    Api(#[from] octocrab::Error),

    #[error("Rate limit exceeded. Resets at {reset_at}")]
    RateLimited { reset_at: DateTime<Utc> },

    #[error("Authentication required")]
    AuthRequired,

    #[error("Organization not found: {0}")]
    OrgNotFound(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

// Re-export the shared short_error_message function from platform module
pub use crate::platform::short_error_message;

/// Check if an error indicates a rate limit (403/429 or JSON parse error from empty response).
pub fn is_rate_limit_error(e: &octocrab::Error) -> bool {
    match e {
        octocrab::Error::GitHub { source, .. } => {
            let status = source.status_code.as_u16();
            status == 403 || status == 429
        }
        // Empty response body (EOF) often indicates rate limiting
        octocrab::Error::Json { .. } => true,
        _ => false,
    }
}

/// Check if a GitHubError indicates rate limiting.
pub fn is_rate_limit_error_from_github(e: &GitHubError) -> bool {
    match e {
        GitHubError::Api(octocrab_err) => is_rate_limit_error(octocrab_err),
        GitHubError::RateLimited { .. } => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_rate_limit_error_from_github() {
        let rate_limited = GitHubError::RateLimited {
            reset_at: Utc::now(),
        };
        assert!(is_rate_limit_error_from_github(&rate_limited));

        let org_not_found = GitHubError::OrgNotFound("test".to_string());
        assert!(!is_rate_limit_error_from_github(&org_not_found));

        let auth_required = GitHubError::AuthRequired;
        assert!(!is_rate_limit_error_from_github(&auth_required));
    }
}
