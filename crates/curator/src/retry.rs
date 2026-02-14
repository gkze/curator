//! Shared retry utilities for platform operations.
//!
//! This module provides common retry configuration and utilities used across
//! all platform integrations (GitHub, GitLab, Gitea).

use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};

use crate::platform::ProgressCallback;
use crate::sync::{INITIAL_BACKOFF_MS, MAX_BACKOFF_MS, MAX_STAR_RETRIES, SyncProgress};

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

/// Execute an operation with automatic retry on rate limit errors.
///
/// This function provides a common retry pattern used across all platform clients:
/// - Tracks retry attempts with an atomic counter
/// - Uses exponential backoff with jitter
/// - Reports progress via callback on each retry
/// - Logs retry attempts with debug-level tracing
///
/// # Arguments
///
/// * `operation` - The async operation to retry. Must be a closure that returns a `Future`.
/// * `is_rate_limit` - A function that checks if an error is a rate limit error (retryable).
/// * `short_message` - A function that extracts a short error message for logging.
/// * `owner` - The owner/namespace for progress reporting.
/// * `name` - The repository name for progress reporting.
/// * `on_progress` - Optional callback for reporting retry progress.
///
/// # Example
///
/// ```ignore
/// use curator::retry::with_retry;
/// use curator::github::error::{GitHubError, is_rate_limit_error_from_github, short_error_message};
///
/// let result = with_retry(
///     || async { client.star_repo(owner, name).await },
///     is_rate_limit_error_from_github,
///     short_error_message,
///     "owner",
///     "repo",
///     Some(&progress_callback),
/// ).await;
/// ```
pub async fn with_retry<T, E, F, Fut, IsRateLimit, ShortMsg>(
    mut operation: F,
    is_rate_limit: IsRateLimit,
    short_message: ShortMsg,
    owner: &str,
    name: &str,
    on_progress: Option<&ProgressCallback>,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
    IsRateLimit: Fn(&E) -> bool + Send + Sync + 'static,
    ShortMsg: Fn(&E) -> String + Send + Sync + 'static,
{
    let owner_str = owner.to_string();
    let name_str = name.to_string();

    // Track attempt number for progress reporting
    let attempt = AtomicU32::new(0);

    let retry_op = || {
        attempt.fetch_add(1, Ordering::SeqCst);
        operation()
    };

    retry_op
        .retry(default_backoff())
        .notify(|err, dur| {
            let current_attempt = attempt.load(Ordering::SeqCst);
            if let Some(cb) = on_progress {
                cb(SyncProgress::RateLimitBackoff {
                    owner: owner_str.clone(),
                    name: name_str.clone(),
                    retry_after_ms: dur.as_millis() as u64,
                    attempt: current_attempt,
                });
            }
            tracing::debug!(
                "Rate limited on {}/{}, retrying in {:?} (attempt {}): {}",
                owner_str,
                name_str,
                dur,
                current_attempt,
                short_message(err)
            );
        })
        .when(is_rate_limit)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

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

    #[derive(Debug, Clone)]
    struct TestError {
        message: &'static str,
        rate_limited: bool,
    }

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.message)
        }
    }

    impl std::error::Error for TestError {}

    #[tokio::test(start_paused = true)]
    async fn with_retry_retries_rate_limit_errors_and_emits_progress() {
        let calls = Arc::new(AtomicU32::new(0));

        let events: Arc<Mutex<Vec<SyncProgress>>> = Arc::new(Mutex::new(Vec::new()));
        let events_capture = Arc::clone(&events);
        let callback: ProgressCallback = Box::new(move |event| {
            events_capture
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(event);
        });

        // Operation: fail twice with a rate-limit error, then succeed.
        let calls_capture = Arc::clone(&calls);
        let mut operation = move || {
            let calls_capture = Arc::clone(&calls_capture);
            async move {
                let n = calls_capture.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(TestError {
                        message: "rate limited",
                        rate_limited: true,
                    })
                } else {
                    Ok(42u32)
                }
            }
        };

        let advancer = tokio::spawn(async {
            // Advance time repeatedly so any backoff sleeps complete.
            for _ in 0..30 {
                tokio::time::advance(Duration::from_secs(60)).await;
                tokio::task::yield_now().await;
            }
        });

        let result = with_retry(
            &mut operation,
            |e: &TestError| e.rate_limited,
            |e: &TestError| e.to_string(),
            "org",
            "repo",
            Some(&callback),
        )
        .await;

        advancer.await.expect("advancer task");

        assert_eq!(result.unwrap(), 42);
        assert!(calls.load(Ordering::SeqCst) >= 3);

        let events = events.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            events
                .iter()
                .any(|e| matches!(e, SyncProgress::RateLimitBackoff { .. }))
        );
    }

    #[tokio::test]
    async fn with_retry_does_not_retry_non_rate_limit_errors() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_capture = Arc::clone(&calls);

        let mut operation = move || {
            let calls_capture = Arc::clone(&calls_capture);
            async move {
                calls_capture.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(TestError {
                    message: "boom",
                    rate_limited: false,
                })
            }
        };

        let err = with_retry(
            &mut operation,
            |e: &TestError| e.rate_limited,
            |e: &TestError| e.to_string(),
            "org",
            "repo",
            None,
        )
        .await
        .expect_err("expected error");

        assert_eq!(err.to_string(), "boom");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
