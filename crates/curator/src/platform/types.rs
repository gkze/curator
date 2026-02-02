use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sea_orm::DatabaseConnection;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::code_visibility::CodeVisibility;
use crate::entity::platform_type::PlatformType;
use crate::sync::SyncProgress;

use super::errors::Result;

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
}

/// Progress callback for platform operations.
///
/// Uses the shared `SyncProgress` type from the sync module for unified
/// progress reporting across all platforms.
pub type ProgressCallback = Box<dyn Fn(SyncProgress) + Send + Sync>;

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
/// code hosting platforms like GitHub, GitLab, and Gitea instances.
///
/// # Instance Model
///
/// Each platform client is associated with a specific instance (e.g., github.com,
/// gitlab.mycompany.com, codeberg.org). The client knows its instance_id and
/// platform_type, which are used when storing repositories in the database.
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
    /// Get the platform type this client connects to (github, gitlab, gitea).
    fn platform_type(&self) -> PlatformType;

    /// Get the instance ID this client is configured for.
    fn instance_id(&self) -> Uuid;

    /// Get current rate limit status.
    async fn get_rate_limit(&self) -> Result<RateLimitInfo>;

    /// Get information about an organization.
    async fn get_org_info(&self, org: &str) -> Result<OrgInfo>;

    /// Get information about the authenticated user.
    async fn get_authenticated_user(&self) -> Result<UserInfo>;

    /// Fetch a single repository by owner and name.
    ///
    /// Implementations may use the optional database connection for ETag caching
    /// and cache-hit fallbacks.
    async fn get_repo(
        &self,
        owner: &str,
        name: &str,
        db: Option<&DatabaseConnection>,
    ) -> Result<PlatformRepo>;

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
    ///
    /// The implementation should use `self.instance_id()` to set the instance_id field.
    fn to_active_model(&self, repo: &PlatformRepo) -> CodeRepositoryActiveModel;
}
