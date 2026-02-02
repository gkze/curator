use std::num::NonZeroU32;
use std::sync::Arc;

use async_trait::async_trait;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use sea_orm::DatabaseConnection;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::platform_type::PlatformType;

use super::errors::Result;
use super::types::{
    OrgInfo, PlatformClient, PlatformRepo, ProgressCallback, RateLimitInfo, UserInfo,
};

/// Type alias for the governor rate limiter.
type GovernorRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// Default rate limits per platform (requests per second).
pub mod rate_limits {
    /// GitHub: 5000 requests/hour = ~1.4/sec, we use 10/sec to allow bursts.
    pub const GITHUB_DEFAULT_RPS: u32 = 10;
    /// GitLab: 2000 requests/minute = ~33/sec, we use 5/sec for safety.
    pub const GITLAB_DEFAULT_RPS: u32 = 5;
    /// Gitea/Codeberg: varies by instance, conservative default.
    pub const GITEA_DEFAULT_RPS: u32 = 5;
}

/// Get the default rate limit for a platform type.
#[allow(dead_code)] // Reserved for future use in dynamic rate limiting
pub fn default_rps_for_platform(platform_type: PlatformType) -> u32 {
    match platform_type {
        PlatformType::GitHub => rate_limits::GITHUB_DEFAULT_RPS,
        PlatformType::GitLab => rate_limits::GITLAB_DEFAULT_RPS,
        PlatformType::Gitea => rate_limits::GITEA_DEFAULT_RPS,
    }
}

/// A standalone API rate limiter using the governor crate.
///
/// This can be used independently of the `RateLimitedClient` wrapper,
/// for cases where you need to rate-limit operations that don't go
/// through the `PlatformClient` trait (e.g., sync functions that use
/// raw platform clients).
///
/// # Example
///
/// ```ignore
/// use curator::platform::ApiRateLimiter;
///
/// let limiter = ApiRateLimiter::new(10); // 10 requests per second
///
/// // Before each API call:
/// limiter.wait().await;
/// client.some_api_call().await?;
/// ```
#[derive(Clone)]
pub struct ApiRateLimiter {
    inner: Arc<GovernorRateLimiter>,
}

impl ApiRateLimiter {
    /// Create a new rate limiter with the specified requests per second.
    ///
    /// # Arguments
    ///
    /// * `requests_per_second` - Maximum requests per second (must be > 0, defaults to 1 if 0)
    pub fn new(requests_per_second: u32) -> Self {
        let rps = NonZeroU32::new(requests_per_second).unwrap_or(NonZeroU32::new(1).unwrap());
        let rate_limiter = RateLimiter::direct(Quota::per_second(rps));

        Self {
            inner: Arc::new(rate_limiter),
        }
    }

    /// Wait until a request is allowed by the rate limiter.
    ///
    /// This method will block (asynchronously) until the rate limit allows
    /// another request to proceed.
    pub async fn wait(&self) {
        self.inner.until_ready().await;
    }
}

/// A rate-limited wrapper around any `PlatformClient`.
///
/// This decorator applies proactive rate limiting using the `governor` crate
/// to prevent hitting API rate limits. All trait methods wait for the rate
/// limiter before delegating to the inner client.
///
/// # Example
///
/// ```ignore
/// use curator::platform::{RateLimitedClient, rate_limits};
/// use curator::github::GitHubClient;
///
/// let client = GitHubClient::new(token, instance_id)?;
/// let client = RateLimitedClient::new(client, rate_limits::GITHUB_DEFAULT_RPS);
///
/// // All operations are now rate-limited
/// let repos = client.list_org_repos("rust-lang", None, None).await?;
/// ```
pub struct RateLimitedClient<C> {
    inner: C,
    rate_limiter: Arc<GovernorRateLimiter>,
}

impl<C> RateLimitedClient<C> {
    /// Create a new rate-limited client wrapper.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying platform client to wrap
    /// * `requests_per_second` - Maximum requests per second (must be > 0)
    pub fn new(inner: C, requests_per_second: u32) -> Self {
        let rps = NonZeroU32::new(requests_per_second).unwrap_or(NonZeroU32::new(1).unwrap());
        let rate_limiter = RateLimiter::direct(Quota::per_second(rps));

        Self {
            inner,
            rate_limiter: Arc::new(rate_limiter),
        }
    }

    /// Get a reference to the inner client.
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Wait for the rate limiter before making a request.
    async fn wait(&self) {
        self.rate_limiter.until_ready().await;
    }
}

// Implement Clone if the inner client is Clone
impl<C: Clone> Clone for RateLimitedClient<C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            rate_limiter: Arc::clone(&self.rate_limiter),
        }
    }
}

#[async_trait]
impl<C: PlatformClient> PlatformClient for RateLimitedClient<C> {
    fn platform_type(&self) -> PlatformType {
        self.inner.platform_type()
    }

    fn instance_id(&self) -> Uuid {
        self.inner.instance_id()
    }

    async fn get_rate_limit(&self) -> Result<RateLimitInfo> {
        self.wait().await;
        self.inner.get_rate_limit().await
    }

    async fn get_org_info(&self, org: &str) -> Result<OrgInfo> {
        self.wait().await;
        self.inner.get_org_info(org).await
    }

    async fn get_authenticated_user(&self) -> Result<UserInfo> {
        self.wait().await;
        self.inner.get_authenticated_user().await
    }

    async fn get_repo(
        &self,
        owner: &str,
        name: &str,
        db: Option<&DatabaseConnection>,
    ) -> Result<PlatformRepo> {
        self.wait().await;
        self.inner.get_repo(owner, name, db).await
    }

    async fn list_org_repos(
        &self,
        org: &str,
        db: Option<&DatabaseConnection>,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<Vec<PlatformRepo>> {
        self.wait().await;
        self.inner.list_org_repos(org, db, on_progress).await
    }

    async fn list_user_repos(
        &self,
        username: &str,
        db: Option<&DatabaseConnection>,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<Vec<PlatformRepo>> {
        self.wait().await;
        self.inner.list_user_repos(username, db, on_progress).await
    }

    async fn is_repo_starred(&self, owner: &str, name: &str) -> Result<bool> {
        self.wait().await;
        self.inner.is_repo_starred(owner, name).await
    }

    async fn star_repo(&self, owner: &str, name: &str) -> Result<bool> {
        self.wait().await;
        self.inner.star_repo(owner, name).await
    }

    async fn star_repo_with_retry(
        &self,
        owner: &str,
        name: &str,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<bool> {
        self.wait().await;
        self.inner
            .star_repo_with_retry(owner, name, on_progress)
            .await
    }

    async fn unstar_repo(&self, owner: &str, name: &str) -> Result<bool> {
        self.wait().await;
        self.inner.unstar_repo(owner, name).await
    }

    async fn list_starred_repos(
        &self,
        db: Option<&DatabaseConnection>,
        concurrency: usize,
        skip_rate_checks: bool,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<Vec<PlatformRepo>> {
        self.wait().await;
        self.inner
            .list_starred_repos(db, concurrency, skip_rate_checks, on_progress)
            .await
    }

    async fn list_starred_repos_streaming(
        &self,
        repo_tx: mpsc::Sender<PlatformRepo>,
        db: Option<&DatabaseConnection>,
        concurrency: usize,
        skip_rate_checks: bool,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<usize> {
        self.wait().await;
        self.inner
            .list_starred_repos_streaming(repo_tx, db, concurrency, skip_rate_checks, on_progress)
            .await
    }

    fn to_active_model(&self, repo: &PlatformRepo) -> CodeRepositoryActiveModel {
        self.inner.to_active_model(repo)
    }
}
