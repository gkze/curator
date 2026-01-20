use crate::platform::ApiRateLimiter;

pub(super) async fn wait_for_rate_limit(rate_limiter: Option<&ApiRateLimiter>) {
    if let Some(limiter) = rate_limiter {
        limiter.wait().await;
    }
}
