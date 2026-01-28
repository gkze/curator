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
    pub fn from_status(status: reqwest::StatusCode, body: &str) -> Self {
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            Self::Auth(format!("{}: {}", status, body))
        } else if status == reqwest::StatusCode::NOT_FOUND {
            Self::Api(format!("Not found: {}", body))
        } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            Self::RateLimited {
                reset_at: Utc::now() + chrono::Duration::minutes(1),
            }
        } else {
            Self::Api(format!("{}: {}", status, body))
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
                let status = rv.status();
                if status == reqwest::StatusCode::UNAUTHORIZED
                    || status == reqwest::StatusCode::FORBIDDEN
                {
                    Self::Auth(msg)
                } else if status == reqwest::StatusCode::NOT_FOUND {
                    Self::Api(format!("Not found: {}", msg))
                } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    Self::RateLimited {
                        reset_at: Utc::now() + chrono::Duration::minutes(1),
                    }
                } else {
                    Self::Api(msg)
                }
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
        GitLabError::Api(msg) | GitLabError::Http(msg) => {
            msg.contains("429") || msg.contains("rate limit")
        }
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
    fn test_from_status() {
        let err = GitLabError::from_status(reqwest::StatusCode::UNAUTHORIZED, "bad token");
        assert!(matches!(err, GitLabError::Auth(_)));

        let err = GitLabError::from_status(reqwest::StatusCode::TOO_MANY_REQUESTS, "slow down");
        assert!(matches!(err, GitLabError::RateLimited { .. }));

        let err = GitLabError::from_status(reqwest::StatusCode::NOT_FOUND, "no such thing");
        assert!(matches!(err, GitLabError::Api(_)));
    }
}
