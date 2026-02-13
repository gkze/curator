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

    #[error("HTTP request error: {0}")]
    Http(String),

    #[error("JSON deserialization error: {0}")]
    Deserialize(String),
}

impl GitLabError {
    /// Create an API error from a message.
    pub fn api(msg: impl Into<String>) -> Self {
        Self::Api(msg.into())
    }

    /// Classify an HTTP status code and response body into a typed error.
    pub fn from_status_code(status: u16, body: &str) -> Self {
        match status {
            401 | 403 => Self::Auth(format!("{}: {}", status, body)),
            404 => Self::Api(format!("Not found: {}", body)),
            429 => Self::RateLimited {
                reset_at: Utc::now() + chrono::Duration::minutes(1),
            },
            _ => Self::Api(format!("{}: {}", status, body)),
        }
    }
}

/// Convert from reqwest::Error.
impl From<reqwest::Error> for GitLabError {
    fn from(err: reqwest::Error) -> Self {
        let msg = err.to_string();
        if msg.contains("401") || msg.contains("Unauthorized") {
            Self::Auth(msg)
        } else if msg.contains("429") || msg.contains("rate limit") {
            Self::RateLimited {
                reset_at: Utc::now() + chrono::Duration::minutes(1),
            }
        } else {
            Self::Http(msg)
        }
    }
}

/// Convert from progenitor_client::Error.
impl<T: std::fmt::Debug> From<progenitor_client::Error<T>> for GitLabError {
    fn from(err: progenitor_client::Error<T>) -> Self {
        let msg = format!("{:?}", err);
        match &err {
            progenitor_client::Error::InvalidRequest(s) => Self::Api(s.clone()),
            progenitor_client::Error::CommunicationError(e) => Self::Http(e.to_string()),
            progenitor_client::Error::ErrorResponse(rv) => {
                Self::from_status_code(rv.status().as_u16(), &msg)
            }
            _ => Self::Api(msg),
        }
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
            GitLabError::Http(msg) => PlatformError::api(msg),
            GitLabError::Deserialize(msg) => PlatformError::internal(msg),
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
        // Http wraps transport-level errors (timeouts, DNS, etc.) — never a rate limit.
        // Actual 429 responses are classified as RateLimited by from_status_code.
        GitLabError::Http(_) => false,
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

    #[test]
    fn test_from_status_code() {
        let err = GitLabError::from_status_code(401, "bad token");
        assert!(matches!(err, GitLabError::Auth(_)));

        let err = GitLabError::from_status_code(403, "forbidden");
        assert!(matches!(err, GitLabError::Auth(_)));

        let err = GitLabError::from_status_code(429, "slow down");
        assert!(matches!(err, GitLabError::RateLimited { .. }));

        let err = GitLabError::from_status_code(404, "no such thing");
        assert!(matches!(err, GitLabError::Api(_)));

        let err = GitLabError::from_status_code(500, "internal server error");
        assert!(matches!(err, GitLabError::Api(_)));
    }
}
