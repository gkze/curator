//! GitLab API error types.

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::platform::PlatformError;

/// Errors that can occur when interacting with the GitLab API.
#[derive(Debug, Error)]
pub enum GitLabError {
    #[error("GitLab API error: {0}")]
    Api(String),

    #[error("Rate limit exceeded. Resets at {reset_at}")]
    RateLimited { reset_at: DateTime<Utc> },

    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Group not found: {0}")]
    GroupNotFound(String),

    #[error("Project not found: {0}")]
    ProjectNotFound(String),

    #[error("Builder error: {0}")]
    Builder(String),
}

impl GitLabError {
    /// Create an API error from a message.
    pub fn api(msg: impl Into<String>) -> Self {
        Self::Api(msg.into())
    }
}

/// Convert from gitlab crate's ApiError.
impl<E: std::error::Error + Send + Sync + 'static> From<gitlab::api::ApiError<E>> for GitLabError {
    fn from(err: gitlab::api::ApiError<E>) -> Self {
        let msg = err.to_string();

        // Try to detect specific error types from the message
        if msg.contains("401") || msg.contains("Unauthorized") {
            Self::Auth(msg)
        } else if msg.contains("404") || msg.contains("Not Found") {
            Self::Api(msg)
        } else if msg.contains("429") || msg.contains("rate limit") {
            Self::RateLimited {
                reset_at: Utc::now() + chrono::Duration::minutes(1),
            }
        } else {
            Self::Api(msg)
        }
    }
}

/// Convert from gitlab crate's RestError.
impl From<gitlab::RestError> for GitLabError {
    fn from(err: gitlab::RestError) -> Self {
        let msg = err.to_string();
        if msg.contains("401") || msg.contains("Unauthorized") {
            Self::Auth(msg)
        } else {
            Self::Api(msg)
        }
    }
}

/// Convert from gitlab crate's AuthError.
impl From<gitlab::AuthError> for GitLabError {
    fn from(err: gitlab::AuthError) -> Self {
        Self::Auth(err.to_string())
    }
}

/// Convert GitLabError to platform-agnostic PlatformError.
impl From<GitLabError> for PlatformError {
    fn from(err: GitLabError) -> Self {
        match err {
            GitLabError::RateLimited { reset_at } => PlatformError::RateLimited { reset_at },
            GitLabError::Auth(_) => PlatformError::AuthRequired,
            GitLabError::GroupNotFound(g) => PlatformError::not_found(format!("group: {}", g)),
            GitLabError::ProjectNotFound(p) => PlatformError::not_found(format!("project: {}", p)),
            GitLabError::Api(msg) => PlatformError::api(msg),
            GitLabError::Builder(msg) => PlatformError::internal(msg),
        }
    }
}

// Re-export the shared short_error_message function from platform module
pub use crate::platform::short_error_message;

/// Check if an error indicates a rate limit.
pub fn is_rate_limit_error(e: &GitLabError) -> bool {
    match e {
        GitLabError::RateLimited { .. } => true,
        GitLabError::Api(msg) => msg.contains("429") || msg.contains("rate limit"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_rate_limit_error() {
        let rate_limited = GitLabError::RateLimited {
            reset_at: Utc::now(),
        };
        assert!(is_rate_limit_error(&rate_limited));

        let api_429 = GitLabError::Api("429 Too Many Requests".to_string());
        assert!(is_rate_limit_error(&api_429));

        let group_not_found = GitLabError::GroupNotFound("test".to_string());
        assert!(!is_rate_limit_error(&group_not_found));

        let auth_error = GitLabError::Auth("invalid token".to_string());
        assert!(!is_rate_limit_error(&auth_error));
    }

    #[test]
    fn test_platform_error_conversion() {
        let rate_limited = GitLabError::RateLimited {
            reset_at: Utc::now(),
        };
        let platform_err: PlatformError = rate_limited.into();
        assert!(matches!(platform_err, PlatformError::RateLimited { .. }));

        let auth_err = GitLabError::Auth("bad token".to_string());
        let platform_err: PlatformError = auth_err.into();
        assert!(matches!(platform_err, PlatformError::AuthRequired));
    }
}
