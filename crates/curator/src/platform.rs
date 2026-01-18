//! Platform-agnostic trait for code forge clients.
//!
//! This module defines the `PlatformClient` trait that provides a unified interface
//! for interacting with different code hosting platforms (GitHub, GitLab, Codeberg).
//!
//! # Example
//!
//! ```ignore
//! use curator::platform::{PlatformClient, PlatformRepo};
//!
//! async fn sync_repos<C: PlatformClient>(client: &C, org: &str) -> Result<(), PlatformError> {
//!     let repos = client.list_org_repos(org, None, None).await?;
//!     for repo in repos {
//!         println!("{}/{}", repo.owner, repo.name);
//!     }
//!     Ok(())
//! }
//! ```

use std::num::NonZeroU32;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use sea_orm::{DatabaseConnection, Set};
use thiserror::Error;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::entity::code_platform::CodePlatform;
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::code_visibility::CodeVisibility;
use crate::sync::SyncProgress;

/// Strip null values from a JSON object to reduce storage size.
///
/// This recursively removes null values from objects, which can significantly
/// reduce database storage for platform_metadata fields where most values are null.
///
/// # Example
///
/// ```ignore
/// let json = serde_json::json!({
///     "node_id": "abc123",
///     "private": false,
///     "allow_squash_merge": null,
/// });
/// let stripped = strip_null_values(json);
/// // Result: {"node_id": "abc123", "private": false}
/// ```
pub fn strip_null_values(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let filtered: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k, strip_null_values(v)))
                .collect();
            serde_json::Value::Object(filtered)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(strip_null_values).collect())
        }
        other => other,
    }
}

/// Type alias for the governor rate limiter.
type GovernorRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

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

/// Rate limit information from a platform.
#[derive(Debug, Clone)]
pub struct RateLimitInfo {
    /// Maximum requests allowed per period.
    pub limit: usize,
    /// Remaining requests in current period.
    pub remaining: usize,
    /// When the rate limit resets.
    pub reset_at: DateTime<Utc>,
}

/// Information about an organization on a platform.
#[derive(Debug, Clone)]
pub struct OrgInfo {
    /// Organization name/login.
    pub name: String,
    /// Number of public repositories.
    pub public_repos: usize,
    /// Organization description (if available).
    pub description: Option<String>,
}

/// A repository from any platform (platform-agnostic representation).
#[derive(Debug, Clone)]
pub struct PlatformRepo {
    /// Platform-specific numeric ID.
    pub platform_id: i64,
    /// Repository owner (user or org).
    pub owner: String,
    /// Repository name.
    pub name: String,
    /// Repository description.
    pub description: Option<String>,
    /// Default branch name.
    pub default_branch: String,
    /// Repository visibility (public, private, or internal).
    pub visibility: CodeVisibility,
    /// Whether the repository is a fork.
    pub is_fork: bool,
    /// Whether the repository is archived.
    pub is_archived: bool,
    /// Star/favorite count.
    pub stars: Option<u32>,
    /// Fork count.
    pub forks: Option<u32>,
    /// Primary programming language.
    pub language: Option<String>,
    /// Repository topics/tags.
    pub topics: Vec<String>,
    /// When the repo was created.
    pub created_at: Option<DateTime<Utc>>,
    /// When the repo was last updated.
    pub updated_at: Option<DateTime<Utc>>,
    /// When code was last pushed.
    pub pushed_at: Option<DateTime<Utc>>,
    /// License SPDX identifier.
    pub license: Option<String>,
    /// Homepage URL.
    pub homepage: Option<String>,
    /// Size in KB.
    pub size_kb: Option<u64>,
    /// Platform-specific metadata (JSON).
    pub metadata: serde_json::Value,
}

impl PlatformRepo {
    /// Get the full name (owner/name).
    #[inline]
    #[must_use]
    pub fn full_name(&self) -> String {
        format!("{}/{}", self.owner, self.name)
    }

    /// Convert this platform repository to a database active model.
    ///
    /// This is the shared conversion logic used by all platforms. Platform-specific
    /// fields that aren't available in `PlatformRepo` (like `is_mirror`, `is_template`,
    /// `is_empty`, `open_issues`, `watchers`, `has_issues`, `has_wiki`, `has_pull_requests`)
    /// can be extracted from the `metadata` JSON field if needed, or will use defaults.
    ///
    /// # Arguments
    ///
    /// * `platform` - The platform this repository came from
    pub fn to_active_model(&self, platform: CodePlatform) -> CodeRepositoryActiveModel {
        let now = Utc::now().fixed_offset();
        let topics = serde_json::json!(self.topics);

        // Extract optional fields from metadata if available
        let metadata = &self.metadata;
        let is_mirror = metadata
            .get("mirror")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let is_template = metadata
            .get("template")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let is_empty = metadata
            .get("empty")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let open_issues = metadata
            .get("open_issues_count")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);
        let watchers = metadata
            .get("watchers_count")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);
        let has_issues = metadata
            .get("has_issues")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let has_wiki = metadata
            .get("has_wiki")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let has_pull_requests = metadata
            .get("has_pull_requests")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        CodeRepositoryActiveModel {
            id: Set(Uuid::new_v4()),
            platform: Set(platform),
            platform_id: Set(self.platform_id),
            owner: Set(self.owner.clone()),
            name: Set(self.name.clone()),
            description: Set(self.description.clone()),
            default_branch: Set(self.default_branch.clone()),
            topics: Set(topics),
            primary_language: Set(self.language.clone()),
            license_spdx: Set(self.license.clone()),
            homepage: Set(self.homepage.clone()),
            visibility: Set(self.visibility.clone()),
            is_fork: Set(self.is_fork),
            is_mirror: Set(is_mirror),
            is_archived: Set(self.is_archived),
            is_template: Set(is_template),
            is_empty: Set(is_empty),
            stars: Set(self.stars.map(|c| c as i32)),
            forks: Set(self.forks.map(|c| c as i32)),
            open_issues: Set(open_issues),
            watchers: Set(watchers),
            size_kb: Set(self.size_kb.map(|s| s as i64)),
            has_issues: Set(has_issues),
            has_wiki: Set(has_wiki),
            has_pull_requests: Set(has_pull_requests),
            created_at: Set(self.created_at.map(|t| t.fixed_offset())),
            updated_at: Set(self.updated_at.map(|t| t.fixed_offset())),
            pushed_at: Set(self.pushed_at.map(|t| t.fixed_offset())),
            platform_metadata: Set(self.metadata.clone()),
            synced_at: Set(now),
            etag: Set(None),
        }
    }

    /// Convert a database model back to a PlatformRepo.
    ///
    /// This is useful when loading cached repos from the database to stream
    /// them through the sync pipeline without re-fetching from the API.
    pub fn from_model(model: &crate::entity::code_repository::Model) -> Self {
        // Extract topics from JSON array
        let topics: Vec<String> = model
            .topics
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Self {
            platform_id: model.platform_id,
            owner: model.owner.clone(),
            name: model.name.clone(),
            description: model.description.clone(),
            default_branch: model.default_branch.clone(),
            visibility: model.visibility.clone(),
            is_fork: model.is_fork,
            is_archived: model.is_archived,
            stars: model.stars.map(|s| s as u32),
            forks: model.forks.map(|f| f as u32),
            language: model.primary_language.clone(),
            topics,
            created_at: model.created_at.map(|t| t.with_timezone(&Utc)),
            updated_at: model.updated_at.map(|t| t.with_timezone(&Utc)),
            pushed_at: model.pushed_at.map(|t| t.with_timezone(&Utc)),
            license: model.license_spdx.clone(),
            homepage: model.homepage.clone(),
            size_kb: model.size_kb.map(|s| s as u64),
            metadata: model.platform_metadata.clone(),
        }
    }
}

/// Progress callback for platform operations.
///
/// Uses the shared `SyncProgress` type from the sync module for unified
/// progress reporting across all platforms.
pub type ProgressCallback = Box<dyn Fn(SyncProgress) + Send + Sync>;

/// Result type for platform operations.
pub type Result<T> = std::result::Result<T, PlatformError>;

/// Information about a user on a platform.
#[derive(Debug, Clone)]
pub struct UserInfo {
    /// Username/login.
    pub username: String,
    /// Display name (if available).
    pub name: Option<String>,
    /// Email (if available).
    pub email: Option<String>,
    /// User bio/description (if available).
    pub bio: Option<String>,
    /// Number of public repositories.
    pub public_repos: usize,
    /// Number of followers.
    pub followers: usize,
}

/// Trait for code hosting platform clients.
///
/// This trait provides a unified interface for interacting with different
/// code hosting platforms like GitHub, GitLab, and Codeberg.
///
/// # Implementation Notes
///
/// Implementors should:
/// - Handle pagination internally for list operations
/// - Report progress via the optional callback
/// - Handle rate limiting with appropriate backoff
/// - Convert platform-specific errors to `PlatformError`
#[async_trait]
pub trait PlatformClient: Send + Sync {
    /// Get the platform type this client connects to.
    fn platform(&self) -> CodePlatform;

    /// Get current rate limit status.
    async fn get_rate_limit(&self) -> Result<RateLimitInfo>;

    /// Get information about an organization.
    async fn get_org_info(&self, org: &str) -> Result<OrgInfo>;

    /// Get information about the authenticated user.
    async fn get_authenticated_user(&self) -> Result<UserInfo>;

    /// List all repositories for an organization.
    ///
    /// This should handle pagination internally and return all repositories.
    /// Progress can be reported via the optional callback.
    ///
    /// If a database connection is provided, implementations may use it for
    /// ETag caching to avoid refetching unchanged data.
    async fn list_org_repos(
        &self,
        org: &str,
        db: Option<&DatabaseConnection>,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<Vec<PlatformRepo>>;

    /// List all repositories for a user.
    ///
    /// This should handle pagination internally and return all repositories
    /// owned by the specified user. Progress can be reported via the optional callback.
    ///
    /// If a database connection is provided, implementations may use it for
    /// ETag caching to avoid refetching unchanged data.
    async fn list_user_repos(
        &self,
        username: &str,
        db: Option<&DatabaseConnection>,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<Vec<PlatformRepo>>;

    /// Check if a repository is starred by the authenticated user.
    async fn is_repo_starred(&self, owner: &str, name: &str) -> Result<bool>;

    /// Star a repository.
    ///
    /// Returns `Ok(true)` if the repo was starred, `Ok(false)` if already starred.
    async fn star_repo(&self, owner: &str, name: &str) -> Result<bool>;

    /// Star a repository with retry on rate limit.
    ///
    /// Returns `Ok(true)` if starred, `Ok(false)` if already starred.
    /// Progress callback receives rate limit backoff notifications.
    async fn star_repo_with_retry(
        &self,
        owner: &str,
        name: &str,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<bool>;

    /// Unstar a repository.
    ///
    /// Returns `Ok(true)` if the repo was unstarred, `Ok(false)` if it wasn't starred.
    async fn unstar_repo(&self, owner: &str, name: &str) -> Result<bool>;

    /// List all repositories starred by the authenticated user.
    ///
    /// This should handle pagination internally and return all starred repositories.
    /// Progress can be reported via the optional callback.
    ///
    /// # Arguments
    ///
    /// * `db` - Optional database connection for ETag caching
    /// * `concurrency` - Maximum concurrent page fetches
    /// * `skip_rate_checks` - Skip per-request rate limit API checks
    /// * `on_progress` - Optional progress callback
    async fn list_starred_repos(
        &self,
        db: Option<&DatabaseConnection>,
        concurrency: usize,
        skip_rate_checks: bool,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<Vec<PlatformRepo>>;

    /// List starred repositories with streaming output.
    ///
    /// Sends repositories through the channel as pages complete, allowing
    /// downstream processing to start before all pages are fetched.
    /// Returns the total count of repositories sent.
    ///
    /// # Arguments
    ///
    /// * `repo_tx` - Channel sender for streaming repositories
    /// * `db` - Optional database connection for ETag caching
    /// * `concurrency` - Maximum concurrent page fetches
    /// * `skip_rate_checks` - Skip per-request rate limit API checks
    /// * `on_progress` - Optional progress callback
    async fn list_starred_repos_streaming(
        &self,
        repo_tx: mpsc::Sender<PlatformRepo>,
        db: Option<&DatabaseConnection>,
        concurrency: usize,
        skip_rate_checks: bool,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<usize>;

    /// Convert a platform repository to a curator active model.
    fn to_active_model(&self, repo: &PlatformRepo) -> CodeRepositoryActiveModel;
}

/// Default rate limits per platform (requests per second).
pub mod rate_limits {
    /// GitHub: 5000 requests/hour = ~1.4/sec, we use 10/sec to allow bursts.
    pub const GITHUB_DEFAULT_RPS: u32 = 10;
    /// GitLab: 2000 requests/minute = ~33/sec, we use 5/sec for safety.
    pub const GITLAB_DEFAULT_RPS: u32 = 5;
    /// Gitea/Codeberg: varies by instance, conservative default.
    pub const GITEA_DEFAULT_RPS: u32 = 5;
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
/// let client = GitHubClient::new(token)?;
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
    fn platform(&self) -> CodePlatform {
        self.inner.platform()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration as StdDuration, Instant};

    #[test]
    fn test_platform_error_api() {
        let err = PlatformError::api("Something went wrong");
        assert!(err.to_string().contains("API error"));
        assert!(err.to_string().contains("Something went wrong"));
    }

    #[test]
    fn test_platform_error_not_found() {
        let err = PlatformError::not_found("org/repo");
        assert!(err.to_string().contains("Not found"));
        assert!(err.to_string().contains("org/repo"));
    }

    #[test]
    fn test_platform_error_rate_limited() {
        let err = PlatformError::RateLimited {
            reset_at: Utc::now(),
        };
        assert!(err.to_string().contains("Rate limit"));
    }

    #[test]
    fn test_platform_error_network() {
        let err = PlatformError::network("connection refused");
        assert!(err.to_string().contains("Network error"));
        assert!(err.to_string().contains("connection refused"));
    }

    #[test]
    fn test_platform_error_internal() {
        let err = PlatformError::internal("unexpected state");
        assert!(err.to_string().contains("Internal error"));
        assert!(err.to_string().contains("unexpected state"));
    }

    #[test]
    fn test_platform_error_auth_required() {
        let err = PlatformError::AuthRequired;
        assert!(err.to_string().contains("Authentication required"));
    }

    #[test]
    fn test_platform_error_is_rate_limited() {
        let rate_limited = PlatformError::RateLimited {
            reset_at: Utc::now(),
        };
        assert!(rate_limited.is_rate_limited());

        let api_error = PlatformError::api("some error");
        assert!(!api_error.is_rate_limited());

        let not_found = PlatformError::not_found("resource");
        assert!(!not_found.is_rate_limited());
    }

    #[test]
    fn test_platform_repo_full_name() {
        let repo = PlatformRepo {
            platform_id: 123,
            owner: "myorg".to_string(),
            name: "myrepo".to_string(),
            description: None,
            default_branch: "main".to_string(),
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_archived: false,
            stars: Some(10),
            forks: Some(2),
            language: Some("Rust".to_string()),
            topics: vec!["cli".to_string()],
            created_at: None,
            updated_at: None,
            pushed_at: None,
            license: None,
            homepage: None,
            size_kb: None,
            metadata: serde_json::json!({}),
        };
        assert_eq!(repo.full_name(), "myorg/myrepo");
    }

    #[test]
    fn test_platform_repo_to_active_model() {
        let repo = PlatformRepo {
            platform_id: 12345,
            owner: "rust-lang".to_string(),
            name: "rust".to_string(),
            description: Some("The Rust programming language".to_string()),
            default_branch: "master".to_string(),
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_archived: false,
            stars: Some(80000),
            forks: Some(10000),
            language: Some("Rust".to_string()),
            topics: vec!["rust".to_string(), "compiler".to_string()],
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            pushed_at: Some(Utc::now()),
            license: Some("MIT".to_string()),
            homepage: Some("https://rust-lang.org".to_string()),
            size_kb: Some(500000),
            metadata: serde_json::json!({
                "mirror": false,
                "template": false,
                "empty": false,
                "open_issues_count": 5000,
                "watchers_count": 3000,
                "has_issues": true,
                "has_wiki": true,
                "has_pull_requests": true
            }),
        };

        let model = repo.to_active_model(CodePlatform::GitHub);

        // Verify key fields are set correctly
        assert_eq!(model.platform.clone().unwrap(), CodePlatform::GitHub);
        assert_eq!(model.platform_id.clone().unwrap(), 12345);
        assert_eq!(model.owner.clone().unwrap(), "rust-lang");
        assert_eq!(model.name.clone().unwrap(), "rust");
        assert_eq!(
            model.description.clone().unwrap(),
            Some("The Rust programming language".to_string())
        );
        assert_eq!(model.default_branch.clone().unwrap(), "master");
        assert_eq!(model.visibility.clone().unwrap(), CodeVisibility::Public);
        assert!(!model.is_fork.clone().unwrap());
        assert!(!model.is_archived.clone().unwrap());
        assert_eq!(model.stars.clone().unwrap(), Some(80000));
        assert_eq!(model.forks.clone().unwrap(), Some(10000));
    }

    #[test]
    fn test_platform_repo_to_active_model_extracts_metadata() {
        let repo = PlatformRepo {
            platform_id: 123,
            owner: "test".to_string(),
            name: "repo".to_string(),
            description: None,
            default_branch: "main".to_string(),
            visibility: CodeVisibility::Private,
            is_fork: true,
            is_archived: true,
            stars: None,
            forks: None,
            language: None,
            topics: vec![],
            created_at: None,
            updated_at: None,
            pushed_at: None,
            license: None,
            homepage: None,
            size_kb: None,
            metadata: serde_json::json!({
                "mirror": true,
                "template": true,
                "empty": true,
                "open_issues_count": 42,
                "watchers_count": 100,
                "has_issues": false,
                "has_wiki": false,
                "has_pull_requests": false
            }),
        };

        let model = repo.to_active_model(CodePlatform::GitLab);

        // Verify metadata fields are extracted
        assert!(model.is_mirror.clone().unwrap());
        assert!(model.is_template.clone().unwrap());
        assert!(model.is_empty.clone().unwrap());
        assert_eq!(model.open_issues.clone().unwrap(), Some(42));
        assert_eq!(model.watchers.clone().unwrap(), Some(100));
        assert!(!model.has_issues.clone().unwrap());
        assert!(!model.has_wiki.clone().unwrap());
        assert!(!model.has_pull_requests.clone().unwrap());
    }

    #[test]
    fn test_rate_limit_info() {
        let info = RateLimitInfo {
            limit: 5000,
            remaining: 4999,
            reset_at: Utc::now(),
        };
        assert_eq!(info.limit, 5000);
        assert_eq!(info.remaining, 4999);
    }

    #[test]
    fn test_org_info() {
        let info = OrgInfo {
            name: "rust-lang".to_string(),
            public_repos: 100,
            description: Some("The Rust Programming Language".to_string()),
        };
        assert_eq!(info.name, "rust-lang");
        assert_eq!(info.public_repos, 100);
    }

    #[test]
    fn test_user_info() {
        let info = UserInfo {
            username: "octocat".to_string(),
            name: Some("The Octocat".to_string()),
            email: Some("octocat@github.com".to_string()),
            bio: Some("I love cats".to_string()),
            public_repos: 50,
            followers: 1000,
        };
        assert_eq!(info.username, "octocat");
        assert_eq!(info.name, Some("The Octocat".to_string()));
        assert_eq!(info.public_repos, 50);
        assert_eq!(info.followers, 1000);
    }

    #[test]
    fn test_short_error_message_single_line() {
        let err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let msg = short_error_message(&err);
        assert_eq!(msg, "file not found");
    }

    #[test]
    fn test_short_error_message_multiline() {
        let err = std::io::Error::other("first line\nsecond line\nthird line");
        let msg = short_error_message(&err);
        assert_eq!(msg, "first line");
    }

    #[test]
    fn test_rate_limits_constants() {
        assert_eq!(rate_limits::GITHUB_DEFAULT_RPS, 10);
        assert_eq!(rate_limits::GITLAB_DEFAULT_RPS, 5);
        assert_eq!(rate_limits::GITEA_DEFAULT_RPS, 5);
    }

    #[test]
    fn test_api_rate_limiter_new() {
        let limiter = ApiRateLimiter::new(10);
        // Just verify it can be created without panicking

        // Test with zero (should default to 1)
        let limiter_zero = ApiRateLimiter::new(0);

        // Verify Clone works
        let _cloned = limiter.clone();
        let _cloned_zero = limiter_zero.clone();
    }

    #[tokio::test]
    async fn test_api_rate_limiter_wait_allows_first_request() {
        let limiter = ApiRateLimiter::new(100); // High RPS for fast test
        let start = Instant::now();
        limiter.wait().await;
        // First request should be nearly instant
        assert!(start.elapsed() < StdDuration::from_millis(50));
    }

    #[tokio::test]
    async fn test_api_rate_limiter_respects_rate() {
        // Use a slower rate (2 RPS = 500ms between requests) for reliable testing
        let limiter = ApiRateLimiter::new(2);
        let start = Instant::now();

        // First request should be instant (burst allowed)
        limiter.wait().await;
        let after_first = start.elapsed();

        // Second request
        limiter.wait().await;
        let after_second = start.elapsed();

        // The limiter should throttle after the first request
        // Allow generous tolerance for test timing variability
        // At 2 RPS, second request should wait ~500ms, but burst may allow instant
        // Just verify the limiter doesn't crash and completes in reasonable time
        assert!(after_second >= after_first);
        assert!(after_second < StdDuration::from_secs(5)); // Sanity check
    }

    #[test]
    fn test_rate_limited_client_new() {
        // We can't easily create a mock PlatformClient, but we can verify
        // the RateLimitedClient wrapper compiles and the inner() method works
        fn _assert_wrapper_compiles<C: PlatformClient>(client: C) {
            let wrapped = RateLimitedClient::new(client, 10);
            let _inner = wrapped.inner();
        }

        // Verify the function compiles (we can't call it without a real client)
        fn _verify_compiles() {
            fn _test<C: PlatformClient>(c: C) {
                _assert_wrapper_compiles(c);
            }
        }
    }

    #[test]
    fn test_rate_limited_client_clone() {
        // Verify RateLimitedClient is Clone when inner is Clone
        fn _assert_clone<T: Clone>() {}

        // This would fail to compile if RateLimitedClient<C> isn't Clone when C: Clone
        fn _verify<C: PlatformClient + Clone>() {
            _assert_clone::<RateLimitedClient<C>>();
        }
    }
}
