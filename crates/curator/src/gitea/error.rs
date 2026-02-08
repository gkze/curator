//! Error types for Gitea API operations.

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::platform::PlatformError;

/// Errors that can occur when interacting with the Gitea API.
#[derive(Debug, Error)]
pub enum GiteaError {
    /// HTTP request failed.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON parsing failed.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// API returned an error response.
    #[error("API error ({status}): {message}")]
    Api { status: u16, message: String },

    /// Rate limit exceeded.
    #[error("Rate limit exceeded. Resets at {reset_at}")]
    RateLimited { reset_at: DateTime<Utc> },

    /// Authentication failed or token invalid.
    #[error("Authentication failed: {0}")]
    Auth(String),

    /// Organization not found.
    #[error("Organization not found: {0}")]
    OrgNotFound(String),

    /// Repository not found.
    #[error("Repository not found: {0}")]
    RepoNotFound(String),

    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    Config(String),
}

impl From<GiteaError> for PlatformError {
    fn from(err: GiteaError) -> Self {
        match err {
            GiteaError::Http(e) => PlatformError::Network {
                message: e.to_string(),
            },
            GiteaError::Json(e) => PlatformError::Internal {
                message: format!("JSON parse error: {}", e),
            },
            GiteaError::Api { status, message } => {
                if status == 401 || status == 403 {
                    PlatformError::AuthRequired
                } else if status == 404 {
                    PlatformError::NotFound { resource: message }
                } else if status == 429 {
                    PlatformError::RateLimited {
                        reset_at: Utc::now() + chrono::Duration::minutes(1),
                    }
                } else {
                    PlatformError::Api { message }
                }
            }
            GiteaError::RateLimited { reset_at } => PlatformError::RateLimited { reset_at },
            GiteaError::Auth(_) => PlatformError::AuthRequired,
            GiteaError::OrgNotFound(org) => PlatformError::NotFound {
                resource: format!("organization: {}", org),
            },
            GiteaError::RepoNotFound(repo) => PlatformError::NotFound {
                resource: format!("repository: {}", repo),
            },
            GiteaError::Config(msg) => PlatformError::Internal { message: msg },
        }
    }
}

/// Check if an error is a rate limit error.
pub fn is_rate_limit_error(err: &GiteaError) -> bool {
    match err {
        GiteaError::RateLimited { .. } => true,
        GiteaError::Api { status: 429, .. } => true,
        GiteaError::Http(e) => e.status().map(|s| s.as_u16()) == Some(429),
        _ => false,
    }
}

/// Get a short error message suitable for display.
pub fn short_error_message(err: &GiteaError) -> String {
    match err {
        GiteaError::Http(e) => {
            if let Some(status) = e.status() {
                format!("HTTP {}", status)
            } else {
                "Network error".to_string()
            }
        }
        GiteaError::Json(_) => "JSON parse error".to_string(),
        GiteaError::Api { status, message } => {
            if message.len() > 50 {
                // Use char_indices to avoid panicking on multi-byte UTF-8
                let truncated: String = message.chars().take(47).collect();
                format!("HTTP {}: {}...", status, truncated)
            } else {
                format!("HTTP {}: {}", status, message)
            }
        }
        GiteaError::RateLimited { .. } => "Rate limited".to_string(),
        GiteaError::Auth(_) => "Authentication failed".to_string(),
        GiteaError::OrgNotFound(org) => format!("Org not found: {}", org),
        GiteaError::RepoNotFound(repo) => format!("Repo not found: {}", repo),
        GiteaError::Config(msg) => format!("Config: {}", msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_error_to_platform_error() {
        let err = GiteaError::Api {
            status: 404,
            message: "not found".to_string(),
        };
        let platform_err: PlatformError = err.into();
        assert!(matches!(platform_err, PlatformError::NotFound { .. }));
    }

    #[test]
    fn test_auth_error_to_platform_error() {
        let err = GiteaError::Auth("invalid token".to_string());
        let platform_err: PlatformError = err.into();
        assert!(matches!(platform_err, PlatformError::AuthRequired));
    }

    #[test]
    fn test_rate_limit_error_to_platform_error() {
        let err = GiteaError::RateLimited {
            reset_at: Utc::now(),
        };
        let platform_err: PlatformError = err.into();
        assert!(matches!(platform_err, PlatformError::RateLimited { .. }));
    }

    #[test]
    fn test_is_rate_limit_error() {
        let rate_limited = GiteaError::RateLimited {
            reset_at: Utc::now(),
        };
        assert!(is_rate_limit_error(&rate_limited));

        let api_429 = GiteaError::Api {
            status: 429,
            message: "too many requests".to_string(),
        };
        assert!(is_rate_limit_error(&api_429));

        let api_500 = GiteaError::Api {
            status: 500,
            message: "server error".to_string(),
        };
        assert!(!is_rate_limit_error(&api_500));
    }

    #[test]
    fn test_short_error_message() {
        let err = GiteaError::RateLimited {
            reset_at: Utc::now(),
        };
        assert_eq!(short_error_message(&err), "Rate limited");

        let err = GiteaError::OrgNotFound("test-org".to_string());
        assert_eq!(short_error_message(&err), "Org not found: test-org");
    }
}
