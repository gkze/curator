//! GitLab API client creation and management.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use backon::Retryable;
use chrono::Utc;
use gitlab::api::{self, AsyncQuery, Pagination};
use gitlab::{AsyncGitlab, GitlabBuilder};
use tokio::sync::Mutex;

use super::convert::to_platform_repo;
use super::error::{GitLabError, is_rate_limit_error, short_error_message};
use super::types::{GitLabGroup, GitLabProject, GitLabUser};
use crate::entity::code_platform::CodePlatform;
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::platform::{
    self, OrgInfo, PlatformClient, PlatformError, PlatformRepo, RateLimitInfo, UserInfo,
};
use crate::retry::default_backoff;
use crate::sync::SyncProgress;

/// GitLab API client wrapper with Arc<Mutex<>> for cloneability.
///
/// Wraps the `AsyncGitlab` client from the gitlab crate in an `Arc<Mutex<>>`
/// to enable cloning and parallel operations. While `AsyncGitlab` doesn't
/// implement `Clone`, this wrapper does.
///
/// The mutex overhead is acceptable because:
/// - GitLab has lower rate limits (300/min for gitlab.com)
/// - Most time is spent on network I/O, not holding the lock
/// - This is simpler than connection pooling
#[derive(Clone)]
pub struct GitLabClient {
    inner: Arc<Mutex<AsyncGitlab>>,
    host: String,
    starred_projects_cache: Arc<Mutex<Option<StarredProjectsCache>>>,
}

#[derive(Debug, Clone)]
struct StarredProjectsCache {
    user_id: u64,
    paths: HashSet<String>,
}

impl GitLabClient {
    /// Create a new GitLab client.
    ///
    /// # Arguments
    ///
    /// * `host` - GitLab host (e.g., "gitlab.com" or "https://gitlab.example.com")
    /// * `token` - Personal access token
    ///
    /// # Example
    ///
    /// ```ignore
    /// let client = GitLabClient::new("gitlab.com", "your-token").await?;
    /// ```
    pub async fn new(host: &str, token: &str) -> Result<Self, GitLabError> {
        // The gitlab crate's GitlabBuilder expects just the hostname without scheme.
        // It will add https:// internally. Strip any scheme if provided.
        let host_only = host
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/');

        let inner = GitlabBuilder::new(host_only, token)
            .build_async()
            .await
            .map_err(|e| GitLabError::Auth(e.to_string()))?;

        // Store the full URL for display purposes
        let normalized_host = format!("https://{}", host_only);

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            host: normalized_host,
            starred_projects_cache: Arc::new(Mutex::new(None)),
        })
    }

    /// Get the host URL.
    pub fn host(&self) -> &str {
        &self.host
    }

    fn build_starred_paths(projects: &[GitLabProject]) -> HashSet<String> {
        projects
            .iter()
            .map(|project| project.path_with_namespace.clone())
            .collect()
    }

    async fn cached_starred_status(&self, user_id: u64, full_path: &str) -> Option<bool> {
        let cache = self.starred_projects_cache.lock().await;
        let cached = cache.as_ref()?;
        if cached.user_id != user_id {
            return None;
        }
        Some(cached.paths.contains(full_path))
    }

    async fn update_starred_cache(&self, user_id: u64, projects: &[GitLabProject]) {
        let paths = Self::build_starred_paths(projects);
        let mut cache = self.starred_projects_cache.lock().await;
        *cache = Some(StarredProjectsCache { user_id, paths });
    }

    async fn clear_starred_cache(&self) {
        let mut cache = self.starred_projects_cache.lock().await;
        *cache = None;
    }

    /// Get rate limit information.
    ///
    /// GitLab doesn't have a dedicated rate limit endpoint, so we return
    /// approximate values. The actual rate limits are included in response
    /// headers which we don't currently track.
    pub fn get_rate_limit(&self) -> RateLimitInfo {
        // GitLab.com defaults: 2000 requests per minute for authenticated users
        // Self-hosted instances may vary
        RateLimitInfo {
            limit: 2000,
            remaining: 2000,
            reset_at: Utc::now() + chrono::Duration::minutes(1),
        }
    }

    /// Get information about a group.
    pub async fn get_group_info(&self, group: &str) -> Result<OrgInfo, GitLabError> {
        let endpoint = gitlab::api::groups::Group::builder()
            .group(group)
            .build()
            .map_err(|e| GitLabError::Builder(e.to_string()))?;

        let client = self.inner.lock().await;
        let group_data: GitLabGroup = endpoint
            .query_async(&*client)
            .await
            .map_err(GitLabError::from)?;

        Ok(OrgInfo {
            name: group_data.name,
            public_repos: 0, // Unknown without additional API call
            description: group_data.description,
        })
    }

    /// List all projects for a group with automatic pagination.
    pub async fn list_group_projects(
        &self,
        group: &str,
        include_subgroups: bool,
    ) -> Result<Vec<GitLabProject>, GitLabError> {
        let endpoint = gitlab::api::groups::projects::GroupProjects::builder()
            .group(group)
            .include_subgroups(include_subgroups)
            .build()
            .map_err(|e| GitLabError::Builder(e.to_string()))?;

        // Use paged() for automatic pagination
        let paged = api::paged(endpoint, Pagination::All);

        let client = self.inner.lock().await;
        let projects: Vec<GitLabProject> = paged
            .query_async(&*client)
            .await
            .map_err(GitLabError::from)?;

        Ok(projects)
    }

    /// Get information about the authenticated user.
    pub async fn get_user_info(&self) -> Result<GitLabUser, GitLabError> {
        let endpoint = gitlab::api::users::CurrentUser::builder()
            .build()
            .map_err(|e| GitLabError::Builder(e.to_string()))?;

        let client = self.inner.lock().await;
        let user: GitLabUser = endpoint
            .query_async(&*client)
            .await
            .map_err(GitLabError::from)?;

        Ok(user)
    }

    /// List all projects for a specific user with automatic pagination.
    pub async fn list_user_projects(
        &self,
        username: &str,
    ) -> Result<Vec<GitLabProject>, GitLabError> {
        let endpoint = gitlab::api::users::UserProjects::builder()
            .user(username)
            .build()
            .map_err(|e| GitLabError::Builder(e.to_string()))?;

        // Use paged() for automatic pagination
        let paged = api::paged(endpoint, Pagination::All);

        let client = self.inner.lock().await;
        let projects: Vec<GitLabProject> = paged
            .query_async(&*client)
            .await
            .map_err(GitLabError::from)?;

        Ok(projects)
    }

    /// Star a project by ID.
    ///
    /// Returns Ok(true) if the project was starred, Ok(false) if already starred.
    pub async fn star_project(&self, project_id: u64) -> Result<bool, GitLabError> {
        let endpoint = gitlab::api::projects::star::StarProject::builder()
            .project(project_id)
            .build()
            .map_err(|e| GitLabError::Builder(e.to_string()))?;

        let client = self.inner.lock().await;
        match api::ignore(endpoint).query_async(&*client).await {
            Ok(()) => Ok(true), // Newly starred
            Err(e) => {
                let msg = e.to_string();
                // GitLab returns 304 Not Modified if already starred
                if msg.contains("304") || msg.contains("Not Modified") {
                    Ok(false) // Already starred
                } else {
                    Err(GitLabError::from(e))
                }
            }
        }
    }

    /// Unstar a project by ID.
    ///
    /// Returns Ok(true) if the project was unstarred, Ok(false) if wasn't starred.
    pub async fn unstar_project(&self, project_id: u64) -> Result<bool, GitLabError> {
        let endpoint = gitlab::api::projects::star::UnstarProject::builder()
            .project(project_id)
            .build()
            .map_err(
                |e: gitlab::api::projects::star::UnstarProjectBuilderError| {
                    GitLabError::Builder(e.to_string())
                },
            )?;

        let client = self.inner.lock().await;
        match api::ignore(endpoint).query_async(&*client).await {
            Ok(()) => Ok(true), // Unstarred
            Err(e) => {
                let msg = e.to_string();
                // GitLab returns 304 Not Modified if wasn't starred
                if msg.contains("304") || msg.contains("Not Modified") {
                    Ok(false) // Wasn't starred
                } else {
                    Err(GitLabError::from(e))
                }
            }
        }
    }

    /// List all projects starred by a user with automatic pagination.
    pub async fn list_starred_projects(
        &self,
        user_id: u64,
    ) -> Result<Vec<GitLabProject>, GitLabError> {
        let endpoint = gitlab::api::users::UserProjects::builder()
            .user(user_id)
            .starred(true)
            .build()
            .map_err(|e: gitlab::api::users::UserProjectsBuilderError| {
                GitLabError::Builder(e.to_string())
            })?;

        // Use paged() for automatic pagination
        let paged = api::paged(endpoint, Pagination::All);

        let client = self.inner.lock().await;
        let projects: Vec<GitLabProject> = paged
            .query_async(&*client)
            .await
            .map_err(GitLabError::from)?;

        Ok(projects)
    }
}

#[async_trait]
impl PlatformClient for GitLabClient {
    fn platform(&self) -> CodePlatform {
        CodePlatform::GitLab
    }

    async fn get_rate_limit(&self) -> platform::Result<RateLimitInfo> {
        Ok(GitLabClient::get_rate_limit(self))
    }

    async fn get_org_info(&self, org: &str) -> platform::Result<OrgInfo> {
        self.get_group_info(org).await.map_err(PlatformError::from)
    }

    async fn get_authenticated_user(&self) -> platform::Result<UserInfo> {
        let user = self.get_user_info().await?;

        Ok(UserInfo {
            username: user.username,
            name: user.name,
            email: user.email.or(user.public_email),
            bio: user.bio,
            public_repos: 0, // GitLab doesn't provide this in user endpoint
            followers: 0,    // GitLab doesn't provide this in user endpoint
        })
    }

    async fn list_org_repos(
        &self,
        org: &str,
        _db: Option<&sea_orm::DatabaseConnection>,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        // TODO: Implement ETag caching for GitLab
        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchingRepos {
                namespace: org.to_string(),
                total_repos: None,
                expected_pages: None,
            });
        }

        let projects = self.list_group_projects(org, true).await?;

        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchComplete {
                total: projects.len(),
            });
        }

        // Convert to PlatformRepo
        let repos: Vec<PlatformRepo> = projects.iter().map(to_platform_repo).collect();

        Ok(repos)
    }

    async fn list_user_repos(
        &self,
        username: &str,
        _db: Option<&sea_orm::DatabaseConnection>,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        // TODO: Implement ETag caching for GitLab
        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchingRepos {
                namespace: username.to_string(),
                total_repos: None,
                expected_pages: None,
            });
        }

        let projects = self.list_user_projects(username).await?;

        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchComplete {
                total: projects.len(),
            });
        }

        // Convert to PlatformRepo
        let repos: Vec<PlatformRepo> = projects.iter().map(to_platform_repo).collect();

        Ok(repos)
    }

    async fn is_repo_starred(&self, owner: &str, name: &str) -> platform::Result<bool> {
        // GitLab doesn't provide a direct "is starred" endpoint. We fetch the
        // authenticated user's starred projects and cache the list.
        let full_path = format!("{}/{}", owner, name);
        let user = self.get_user_info().await?;

        if let Some(cached) = self.cached_starred_status(user.id, &full_path).await {
            return Ok(cached);
        }

        let projects = self.list_starred_projects(user.id).await?;
        let paths = Self::build_starred_paths(&projects);
        let is_starred = paths.contains(&full_path);

        let mut cache = self.starred_projects_cache.lock().await;
        *cache = Some(StarredProjectsCache {
            user_id: user.id,
            paths,
        });

        Ok(is_starred)
    }

    async fn star_repo(&self, owner: &str, name: &str) -> platform::Result<bool> {
        // GitLab stars by project ID, not owner/name
        // We need to look up the project first or extract ID from metadata
        // For now, we'll search for the project
        let full_path = format!("{}/{}", owner, name);

        let endpoint = gitlab::api::projects::Project::builder()
            .project(&full_path)
            .build()
            .map_err(|e| PlatformError::internal(e.to_string()))?;

        let client = self.inner.lock().await;
        let project: GitLabProject = endpoint
            .query_async(&*client)
            .await
            .map_err(|e| PlatformError::api(e.to_string()))?;
        drop(client); // Release lock before starring

        let result = self
            .star_project(project.id)
            .await
            .map_err(PlatformError::from)?;
        self.clear_starred_cache().await;
        Ok(result)
    }

    async fn star_repo_with_retry(
        &self,
        owner: &str,
        name: &str,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<bool> {
        let full_path = format!("{}/{}", owner, name);

        // Look up project ID first
        let endpoint = gitlab::api::projects::Project::builder()
            .project(&full_path)
            .build()
            .map_err(|e| PlatformError::internal(e.to_string()))?;

        let client = self.inner.lock().await;
        let project: GitLabProject = endpoint
            .query_async(&*client)
            .await
            .map_err(|e| PlatformError::api(e.to_string()))?;
        drop(client); // Release lock

        let project_id = project.id;
        let client = self.clone();

        // Track attempt number for progress reporting
        let attempt = std::sync::atomic::AtomicU32::new(0);

        let star_op = || async {
            attempt.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            client.star_project(project_id).await
        };

        let result = star_op
            .retry(default_backoff())
            .notify(|err, dur| {
                let current_attempt = attempt.load(std::sync::atomic::Ordering::SeqCst);
                if let Some(cb) = on_progress {
                    cb(SyncProgress::RateLimitBackoff {
                        owner: full_path.clone(),
                        name: String::new(),
                        retry_after_ms: dur.as_millis() as u64,
                        attempt: current_attempt,
                    });
                }
                tracing::debug!(
                    "Rate limited on {}, retrying in {:?} (attempt {}): {}",
                    full_path,
                    dur,
                    current_attempt,
                    short_error_message(err)
                );
            })
            .when(is_rate_limit_error)
            .await;

        let result = result.map_err(PlatformError::from)?;
        self.clear_starred_cache().await;
        Ok(result)
    }

    async fn unstar_repo(&self, owner: &str, name: &str) -> platform::Result<bool> {
        // GitLab unstars by project ID, not owner/name
        // We need to look up the project first
        let full_path = format!("{}/{}", owner, name);

        let endpoint = gitlab::api::projects::Project::builder()
            .project(&full_path)
            .build()
            .map_err(|e| PlatformError::internal(e.to_string()))?;

        let client = self.inner.lock().await;
        let project: GitLabProject = endpoint
            .query_async(&*client)
            .await
            .map_err(|e| PlatformError::api(e.to_string()))?;
        drop(client); // Release lock before unstarring

        let result = self
            .unstar_project(project.id)
            .await
            .map_err(PlatformError::from)?;
        self.clear_starred_cache().await;
        Ok(result)
    }

    async fn list_starred_repos(
        &self,
        _db: Option<&sea_orm::DatabaseConnection>,
        _concurrency: usize,
        _skip_rate_checks: bool,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        // LIMITATION: The gitlab crate handles pagination internally, so `concurrency` is ignored.
        // TODO: Implement ETag caching for GitLab (see beads issue curator-vvy)
        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchingRepos {
                namespace: "starred".to_string(),
                total_repos: None,
                expected_pages: None,
            });
        }

        // First get the current user's ID
        let user = self.get_user_info().await?;

        // Then fetch their starred projects
        let projects = self.list_starred_projects(user.id).await?;

        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchComplete {
                total: projects.len(),
            });
        }

        self.update_starred_cache(user.id, &projects).await;

        // Convert to PlatformRepo
        let repos: Vec<PlatformRepo> = projects.iter().map(to_platform_repo).collect();

        Ok(repos)
    }

    async fn list_starred_repos_streaming(
        &self,
        repo_tx: tokio::sync::mpsc::Sender<PlatformRepo>,
        _db: Option<&sea_orm::DatabaseConnection>,
        _concurrency: usize,
        _skip_rate_checks: bool,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<usize> {
        // LIMITATION: The gitlab crate's `api::paged()` fetches all pages before returning,
        // so this isn't truly streaming. The `concurrency` parameter is ignored.
        // See beads issue curator-0tg for tracking.
        // TODO: Implement ETag caching for GitLab (see beads issue curator-vvy)
        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchingRepos {
                namespace: "starred".to_string(),
                total_repos: None,
                expected_pages: None,
            });
        }

        // First get the current user's ID
        let user = self.get_user_info().await?;

        // Fetch starred projects (uses library pagination internally)
        let projects = self.list_starred_projects(user.id).await?;

        self.update_starred_cache(user.id, &projects).await;

        // Stream repos as we convert them
        let mut sent = 0usize;
        for project in &projects {
            let platform_repo = to_platform_repo(project);
            if repo_tx.send(platform_repo).await.is_ok() {
                sent += 1;
            }
        }

        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchComplete { total: sent });
        }

        Ok(sent)
    }

    fn to_active_model(&self, repo: &PlatformRepo) -> CodeRepositoryActiveModel {
        repo.to_active_model(self.platform())
    }
}

/// Create a GitLab client (convenience function).
///
/// # Arguments
///
/// * `host` - GitLab host (e.g., "gitlab.com")
/// * `token` - Personal access token
pub async fn create_client(host: &str, token: &str) -> Result<GitLabClient, GitLabError> {
    GitLabClient::new(host, token).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_info() {
        let info = RateLimitInfo {
            limit: 2000,
            remaining: 1999,
            reset_at: Utc::now(),
        };
        assert_eq!(info.limit, 2000);
        assert_eq!(info.remaining, 1999);
    }

    #[test]
    fn test_gitlab_error_to_platform_error() {
        let rate_limited = GitLabError::RateLimited {
            reset_at: Utc::now(),
        };
        let platform_err: PlatformError = rate_limited.into();
        assert!(matches!(platform_err, PlatformError::RateLimited { .. }));

        let not_found = GitLabError::GroupNotFound("test-group".to_string());
        let platform_err: PlatformError = not_found.into();
        assert!(matches!(platform_err, PlatformError::NotFound { .. }));
    }

    #[test]
    fn test_gitlab_client_is_clone() {
        // Verify that GitLabClient implements Clone (compile-time check)
        fn assert_clone<T: Clone>() {}
        assert_clone::<GitLabClient>();
    }

    #[test]
    fn test_gitlab_client_platform() {
        // Verify the trait implementation compiles
        fn assert_platform_client<T: PlatformClient>() {}
        assert_platform_client::<GitLabClient>();
    }
}
