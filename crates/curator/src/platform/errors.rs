use chrono::{DateTime, Utc};
use thiserror::Error;

/// Errors that can occur when interacting with a code platform.
#[derive(Debug, Error)]
pub enum PlatformError {
    /// API error from the platform.
    #[error("API error: {message}")]
    Api { message: String },

    /// Rate limit exceeded.
    #[error("Rate limit exceeded. Resets at {reset_at}")]
    RateLimited { reset_at: DateTime<Utc> },

    /// Authentication required or failed.
    #[error("Authentication required")]
    AuthRequired,

    /// Resource not found (org, repo, etc.).
    #[error("Not found: {resource}")]
    NotFound { resource: String },

    /// Network or connection error.
    #[error("Network error: {message}")]
    Network { message: String },

    /// Unexpected/internal error.
    #[error("Internal error: {message}")]
    Internal { message: String },
}

impl PlatformError {
    /// Create an API error.
    #[inline]
    pub fn api(message: impl Into<String>) -> Self {
        Self::Api {
            message: message.into(),
        }
    }

    /// Create a not found error.
    #[inline]
    pub fn not_found(resource: impl Into<String>) -> Self {
        Self::NotFound {
            resource: resource.into(),
        }
    }

    /// Create a network error.
    #[inline]
    pub fn network(message: impl Into<String>) -> Self {
        Self::Network {
            message: message.into(),
        }
    }

    /// Create an internal error.
    #[inline]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    /// Check if this error is a rate limit error (retryable).
    #[inline]
    pub fn is_rate_limited(&self) -> bool {
        matches!(self, Self::RateLimited { .. })
    }
}

/// Extract a short error message suitable for display.
///
/// Takes the first line of an error message, which is useful for errors
/// that include backtraces or multi-line details. This provides a concise
/// message for progress reporting and logging.
///
/// # Example
///
/// ```ignore
/// use curator::platform::short_error_message;
/// let error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
/// assert_eq!(short_error_message(&error), "file not found");
/// ```
#[inline]
pub fn short_error_message(e: &impl std::error::Error) -> String {
    let full = e.to_string();
    full.lines().next().unwrap_or(&full).to_string()
}

/// Result type for platform operations.
pub type Result<T> = std::result::Result<T, PlatformError>;
