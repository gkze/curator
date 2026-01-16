//! Shared retry utilities for platform operations.
//!
//! This module provides common retry configuration and utilities used across
//! all platform integrations (GitHub, GitLab, Gitea).

use std::time::Duration;

use backon::ExponentialBuilder;

use crate::sync::{INITIAL_BACKOFF_MS, MAX_BACKOFF_MS, MAX_STAR_RETRIES};

/// Configuration for retry operations.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Minimum delay between retries.
    pub min_delay: Duration,
    /// Maximum delay between retries.
    pub max_delay: Duration,
    /// Maximum number of retry attempts.
    pub max_retries: usize,
    /// Whether to add jitter to delays.
    pub with_jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_millis(INITIAL_BACKOFF_MS),
            max_delay: Duration::from_millis(MAX_BACKOFF_MS),
            max_retries: MAX_STAR_RETRIES as usize,
            with_jitter: true,
        }
    }
}

impl RetryConfig {
    /// Create a new retry configuration with custom values.
    #[must_use]
    pub fn new(min_delay: Duration, max_delay: Duration, max_retries: usize) -> Self {
        Self {
            min_delay,
            max_delay,
            max_retries,
            with_jitter: true,
        }
    }

    /// Set whether to use jitter.
    #[must_use]
    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.with_jitter = jitter;
        self
    }

    /// Build an exponential backoff strategy from this configuration.
    #[must_use]
    pub fn into_backoff(self) -> ExponentialBuilder {
        let mut builder = ExponentialBuilder::default()
            .with_min_delay(self.min_delay)
            .with_max_delay(self.max_delay)
            .with_max_times(self.max_retries);

        if self.with_jitter {
            builder = builder.with_jitter();
        }

        builder
    }
}

/// Build the default exponential backoff strategy for platform operations.
///
/// This is the standard backoff configuration used across all platforms:
/// - Initial delay: 1 second
/// - Maximum delay: 60 seconds
/// - Maximum retries: 5
/// - Jitter: enabled
///
/// # Example
///
/// ```ignore
/// use backon::Retryable;
/// use curator::retry::default_backoff;
///
/// let result = operation
///     .retry(default_backoff())
///     .when(|e| is_retryable(e))
///     .await;
/// ```
#[must_use]
pub fn default_backoff() -> ExponentialBuilder {
    RetryConfig::default().into_backoff()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();

        assert_eq!(config.min_delay, Duration::from_millis(INITIAL_BACKOFF_MS));
        assert_eq!(config.max_delay, Duration::from_millis(MAX_BACKOFF_MS));
        assert_eq!(config.max_retries, MAX_STAR_RETRIES as usize);
        assert!(config.with_jitter);
    }

    #[test]
    fn test_retry_config_custom() {
        let config = RetryConfig::new(Duration::from_secs(2), Duration::from_secs(30), 3);

        assert_eq!(config.min_delay, Duration::from_secs(2));
        assert_eq!(config.max_delay, Duration::from_secs(30));
        assert_eq!(config.max_retries, 3);
        assert!(config.with_jitter);
    }

    #[test]
    fn test_retry_config_without_jitter() {
        let config = RetryConfig::default().with_jitter(false);
        assert!(!config.with_jitter);
    }

    #[test]
    fn test_default_backoff_creates_builder() {
        // Just verify it compiles and returns an ExponentialBuilder
        let _backoff = default_backoff();
    }

    #[test]
    fn test_into_backoff() {
        let config = RetryConfig::default();
        let _backoff = config.into_backoff();
    }
}
