use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

/// Internal GCRA state with a mutable cell size for adaptive pacing.
struct GcraState {
    /// Theoretical arrival time — when the next request is allowed.
    tat: Instant,
    /// Time between requests (1/rps). This is the mutable part.
    cell_size: Duration,
    /// Maximum burst tolerance — how far ahead of schedule we allow.
    burst_tolerance: Duration,
}

/// An adaptive rate limiter using a custom GCRA (Generic Cell Rate Algorithm).
///
/// Unlike a fixed-rate limiter, this adjusts its pacing based on actual API
/// rate limit state reported via [`AdaptiveRateLimiter::update`]. After each
/// API response, callers feed back the `RateLimitInfo` from response headers,
/// and the limiter recalculates the optimal request spacing.
///
/// # Algorithm
///
/// GCRA tracks a single timestamp (TAT — theoretical arrival time). Each
/// request advances the TAT by `cell_size` (= 1/rps). If `now < TAT`,
/// the caller sleeps until TAT. A `burst_tolerance` allows some requests
/// to proceed immediately even if TAT is slightly in the future.
///
/// When `update()` is called with new rate limit info, only `cell_size`
/// changes — the TAT is preserved, so pacing adjusts smoothly without
/// resetting state or risking accidental bursts.
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
    state: Arc<Mutex<GcraState>>,
}

impl AdaptiveRateLimiter {
    /// Create a new adaptive rate limiter starting at the given requests per second.
    ///
    /// The initial RPS is used until the first `update()` call provides actual
    /// rate limit state from the API.
    ///
    /// # Arguments
    ///
    /// * `initial_rps` - Starting requests per second (must be > 0, defaults to 1 if 0)
    pub fn new(initial_rps: u32) -> Self {
        let rps = initial_rps.max(1) as f64;
        let cell_size = Duration::from_secs_f64(1.0 / rps);
        // Allow a small burst: up to 3 requests can proceed without waiting
        let burst_tolerance = cell_size.saturating_mul(3);

        Self {
            state: Arc::new(Mutex::new(GcraState {
                tat: Instant::now(),
                cell_size,
                burst_tolerance,
            })),
        }
    }

    /// Wait until a request is allowed by the rate limiter.
    ///
    /// This method will sleep (asynchronously) if the current time is before
    /// the theoretical arrival time, respecting burst tolerance.
    pub async fn wait(&self) {
        let sleep_duration = {
            let mut state = self.state.lock().expect("rate limiter lock poisoned");
            let now = Instant::now();
            let cell_size = state.cell_size;
            let burst_tolerance = state.burst_tolerance;

            if now >= state.tat {
                // We're at or past the TAT — request is allowed immediately.
                // Advance TAT by one cell.
                state.tat = now + cell_size;
                None
            } else {
                let wait = state.tat - now;
                if wait <= burst_tolerance {
                    // Within burst tolerance — allow but advance TAT.
                    state.tat += cell_size;
                    None
                } else {
                    // Must wait. Advance TAT for our future slot.
                    state.tat += cell_size;
                    Some(wait - burst_tolerance)
                }
            }
        };

        if let Some(duration) = sleep_duration {
            tokio::time::sleep(duration).await;
        }
    }

    /// Update the rate limiter with fresh rate limit information from an API response.
    ///
    /// Calculates optimal pacing as `remaining / seconds_until_reset` and adjusts
    /// the cell size accordingly. The TAT (theoretical arrival time) is preserved,
    /// so the transition is smooth — only future request spacing changes.
    ///
    /// If the response includes a `retry_after` duration (from a 429 response),
    /// the TAT is pushed forward to honor the server's requested wait time.
    pub fn update(&self, info: &RateLimitInfo) {
        let mut state = self.state.lock().expect("rate limiter lock poisoned");

        // Handle Retry-After: push TAT forward
        if let Some(retry_after) = info.retry_after {
            let earliest = Instant::now() + retry_after;
            if earliest > state.tat {
                state.tat = earliest;
            }
        }

        // Calculate new RPS from remaining budget
        let now = chrono::Utc::now();
        let seconds_until_reset = (info.reset_at - now).num_seconds().max(1) as f64;
        let remaining = info.remaining as f64;

        let target_rps = (remaining / seconds_until_reset).clamp(MIN_RPS, MAX_RPS);
        state.cell_size = Duration::from_secs_f64(1.0 / target_rps);
        // Recalculate burst tolerance proportionally
        state.burst_tolerance = state.cell_size.saturating_mul(3);
    }

    /// Get the current effective requests per second.
    ///
    /// Useful for diagnostics and logging.
    pub fn current_rps(&self) -> f64 {
        let state = self.state.lock().expect("rate limiter lock poisoned");
        1.0 / state.cell_size.as_secs_f64()
    }
}
