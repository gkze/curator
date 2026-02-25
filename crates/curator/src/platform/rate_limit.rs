use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};

use crate::entity::platform_type::PlatformType;

use super::types::RateLimitInfo;

/// Default rate limits per platform (requests per second).
pub mod rate_limits {
    use crate::entity::platform_type::PlatformType;

    /// GitHub: 5000 requests/hour = ~1.4/sec, we use 10/sec to allow bursts.
    pub const GITHUB_DEFAULT_RPS: u32 = 10;
    /// GitLab: 2000 requests/minute = ~33/sec, we use 5/sec for safety.
    pub const GITLAB_DEFAULT_RPS: u32 = 5;
    /// Gitea/Codeberg: varies by instance, conservative default.
    pub const GITEA_DEFAULT_RPS: u32 = 5;

    /// Get the default rate limit for a platform type.
    pub fn default_rps_for_platform(platform_type: PlatformType) -> u32 {
        match platform_type {
            PlatformType::GitHub => GITHUB_DEFAULT_RPS,
            PlatformType::GitLab => GITLAB_DEFAULT_RPS,
            PlatformType::Gitea => GITEA_DEFAULT_RPS,
        }
    }
}

/// Get the default rate limit for a platform type.
#[allow(dead_code)]
pub fn default_rps_for_platform(platform_type: PlatformType) -> u32 {
    rate_limits::default_rps_for_platform(platform_type)
}

/// Minimum requests per second floor to avoid stalling.
const MIN_RPS: f64 = 0.5;

/// Maximum requests per second ceiling.
const MAX_RPS: f64 = 50.0;

/// Internal adaptive limiter state.
struct AdaptiveState {
    /// Governor-backed direct limiter.
    limiter: Arc<DefaultDirectRateLimiter>,
    /// Current target requests-per-second.
    current_rps: f64,
    /// Server-mandated cooldown (from Retry-After), if any.
    retry_until: Option<Instant>,
}

/// An adaptive rate limiter using the `governor` crate.
///
/// Unlike a fixed-rate limiter, this adjusts its pacing based on actual API
/// rate limit state reported via [`AdaptiveRateLimiter::update`]. After each
/// API response, callers feed back the `RateLimitInfo` from response headers,
/// and the limiter recalculates the optimal request spacing.
///
/// # Algorithm
///
/// Requests are paced by a `governor::RateLimiter` configured with a quota that
/// is recalculated from `remaining / seconds_until_reset`. On updates, we swap
/// in a freshly configured limiter for subsequent requests. `Retry-After` is
/// handled by tracking a cooldown timestamp that is honored before limiter checks.
///
/// # Example
///
/// ```ignore
/// use curator::platform::AdaptiveRateLimiter;
///
/// let limiter = AdaptiveRateLimiter::new(10); // Start at 10 req/sec
///
/// // Before each API call:
/// limiter.wait().await;
/// let response = client.some_api_call().await?;
///
/// // After each API call, feed back rate limit headers:
/// if let Some(info) = extract_rate_limit(&response) {
///     limiter.update(&info);
/// }
/// ```
#[derive(Clone)]
pub struct AdaptiveRateLimiter {
    state: Arc<Mutex<AdaptiveState>>,
}

impl AdaptiveRateLimiter {
    fn quota_for_rps(rps: f64) -> Quota {
        let replenish_period = Duration::from_secs_f64((1.0 / rps).max(0.001));
        let burst = NonZeroU32::new(3).expect("non-zero burst");
        Quota::with_period(replenish_period)
            .unwrap_or_else(|| Quota::per_second(NonZeroU32::new(1).expect("non-zero quota")))
            .allow_burst(burst)
    }

    fn limiter_for_rps(rps: f64) -> Arc<DefaultDirectRateLimiter> {
        Arc::new(RateLimiter::direct(Self::quota_for_rps(rps)))
    }

    /// Create a new adaptive rate limiter starting at the given requests per second.
    ///
    /// The initial RPS is used until the first `update()` call provides actual
    /// rate limit state from the API.
    ///
    /// # Arguments
    ///
    /// * `initial_rps` - Starting requests per second (must be > 0, defaults to 1 if 0)
    pub fn new(initial_rps: u32) -> Self {
        let rps = (initial_rps.max(1) as f64).clamp(MIN_RPS, MAX_RPS);

        Self {
            state: Arc::new(Mutex::new(AdaptiveState {
                limiter: Self::limiter_for_rps(rps),
                current_rps: rps,
                retry_until: None,
            })),
        }
    }

    /// Wait until a request is allowed by the rate limiter.
    ///
    /// This method first honors any active `Retry-After` cooldown, then waits
    /// on the governor limiter quota.
    pub async fn wait(&self) {
        let (limiter, retry_until) = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            (Arc::clone(&state.limiter), state.retry_until)
        };

        if let Some(until) = retry_until {
            let now = Instant::now();
            if until > now {
                tokio::time::sleep(until - now).await;
            }

            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state
                .retry_until
                .is_some_and(|current_until| current_until <= Instant::now())
            {
                state.retry_until = None;
            }
        }

        limiter.until_ready().await;
    }

    /// Update the rate limiter with fresh rate limit information from an API response.
    ///
    /// Calculates optimal pacing as `remaining / seconds_until_reset` and swaps in
    /// a new governor quota for future requests.
    ///
    /// If the response includes a `retry_after` duration (from a 429 response),
    /// the cooldown timestamp is extended to honor the server's requested wait.
    pub fn update(&self, info: &RateLimitInfo) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());

        // Handle Retry-After cooldown
        if let Some(retry_after) = info.retry_after {
            let until = Instant::now() + retry_after;
            state.retry_until = Some(
                state
                    .retry_until
                    .map_or(until, |current| current.max(until)),
            );
        }

        // Calculate new RPS from remaining budget
        let now = chrono::Utc::now();
        let seconds_until_reset = (info.reset_at - now).num_seconds().max(1) as f64;
        let remaining = info.remaining as f64;

        let target_rps = (remaining / seconds_until_reset).clamp(MIN_RPS, MAX_RPS);
        if (state.current_rps - target_rps).abs() > f64::EPSILON {
            state.current_rps = target_rps;
            state.limiter = Self::limiter_for_rps(target_rps);
        }
    }

    /// Get the current effective requests per second.
    ///
    /// Useful for diagnostics and logging.
    pub fn current_rps(&self) -> f64 {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.current_rps
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration as ChronoDuration, Utc};

    use super::*;

    #[test]
    fn default_rps_helpers_match_platform_defaults() {
        assert_eq!(
            rate_limits::default_rps_for_platform(PlatformType::GitHub),
            rate_limits::GITHUB_DEFAULT_RPS
        );
        assert_eq!(
            rate_limits::default_rps_for_platform(PlatformType::GitLab),
            rate_limits::GITLAB_DEFAULT_RPS
        );
        assert_eq!(
            rate_limits::default_rps_for_platform(PlatformType::Gitea),
            rate_limits::GITEA_DEFAULT_RPS
        );

        assert_eq!(
            default_rps_for_platform(PlatformType::GitHub),
            rate_limits::GITHUB_DEFAULT_RPS
        );
        assert_eq!(
            default_rps_for_platform(PlatformType::GitLab),
            rate_limits::GITLAB_DEFAULT_RPS
        );
        assert_eq!(
            default_rps_for_platform(PlatformType::Gitea),
            rate_limits::GITEA_DEFAULT_RPS
        );
    }

    #[test]
    fn adaptive_rate_limiter_new_clamps_zero_to_one_rps() {
        let limiter = AdaptiveRateLimiter::new(0);
        let rps = limiter.current_rps();
        assert!(
            (rps - 1.0).abs() < 1e-9,
            "expected ~1 rps when constructed with 0, got {rps}"
        );
    }

    #[test]
    fn adaptive_rate_limiter_update_applies_remaining_over_time_and_clamps() {
        let limiter = AdaptiveRateLimiter::new(10);

        // Ensure we fall well below MIN_RPS regardless of timestamp truncation.
        let info_min = RateLimitInfo {
            limit: 5000,
            remaining: 1,
            reset_at: Utc::now() + ChronoDuration::hours(24),
            retry_after: None,
        };
        limiter.update(&info_min);
        let rps_min = limiter.current_rps();
        assert!(
            (rps_min - 0.5).abs() < 1e-6,
            "expected ~0.5 rps at MIN_RPS clamp, got {rps_min}"
        );

        // Ensure we exceed MAX_RPS.
        let info_max = RateLimitInfo {
            limit: 5000,
            remaining: 10_000,
            reset_at: Utc::now() + ChronoDuration::seconds(1),
            retry_after: None,
        };
        limiter.update(&info_max);
        let rps_max = limiter.current_rps();
        assert!(
            (rps_max - 50.0).abs() < 1e-6,
            "expected ~50 rps at MAX_RPS clamp, got {rps_max}"
        );

        // A normal value within bounds. Use a large window so truncation noise is tiny.
        let info_mid = RateLimitInfo {
            limit: 5000,
            remaining: 5000,
            reset_at: Utc::now() + ChronoDuration::seconds(1000),
            retry_after: None,
        };
        limiter.update(&info_mid);
        let rps_mid = limiter.current_rps();
        assert!(
            (4.8..=5.3).contains(&rps_mid),
            "expected ~5 rps (5000/1000), got {rps_mid}"
        );
    }

    #[tokio::test]
    async fn adaptive_rate_limiter_handles_poisoned_mutex_state() {
        let limiter = AdaptiveRateLimiter::new(1);

        // Poison the mutex by panicking while holding the lock.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = limiter.state.lock().unwrap();
            panic!("poison");
        }));

        // These methods use `unwrap_or_else(|e| e.into_inner())` to recover.
        let _ = limiter.current_rps();
        limiter.update(&RateLimitInfo {
            limit: 5000,
            remaining: 1,
            reset_at: Utc::now() + ChronoDuration::hours(1),
            retry_after: None,
        });
        limiter.wait().await;
    }
}
