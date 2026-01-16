//! Gitea API client creation and management.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use backon::Retryable;
use chrono::Utc;
use reqwest::{Client, StatusCode, header};
use tokio::sync::Semaphore;

use super::convert::to_platform_repo;
use super::error::{GiteaError, is_rate_limit_error, short_error_message};
use super::types::{GiteaAuthUser, GiteaOrg, GiteaRepo};
use crate::entity::code_platform::CodePlatform;
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::platform::{
    self, OrgInfo, PlatformClient, PlatformError, PlatformRepo, RateLimitInfo, UserInfo,
};
use crate::retry::default_backoff;
use crate::sync::SyncProgress;

/// Default Codeberg host.
pub const CODEBERG_HOST: &str = "https://codeberg.org";

/// Default page size for API requests.
const PAGE_SIZE: u32 = 50;

/// Gitea API client.
///
/// This client uses reqwest to interact with the Gitea API, which is
/// compatible with Codeberg, Forgejo, and other Gitea-based forges.
#[derive(Clone)]
pub struct GiteaClient {
    client: Client,
    host: String,
    token: String,
    platform: CodePlatform,
}

impl GiteaClient {
    /// Create a new Gitea client.
    ///
    /// # Arguments
    ///
    /// * `host` - Gitea host URL (e.g., "https://codeberg.org")
    /// * `token` - Personal access token
    /// * `platform` - The platform type (Codeberg or Gitea)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use curator::entity::code_platform::CodePlatform;
    ///
    /// // For Codeberg
    /// let client = GiteaClient::new("https://codeberg.org", "token", CodePlatform::Codeberg)?;
    ///
    /// // For self-hosted Gitea/Forgejo
    /// let client = GiteaClient::new("https://git.example.com", "token", CodePlatform::Gitea)?;
    /// ```
    pub fn new(host: &str, token: &str, platform: CodePlatform) -> Result<Self, GiteaError> {
        // Normalize host URL
        let host = host.trim_end_matches('/').to_string();

        // Build HTTP client with default headers
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            header::USER_AGENT,
            header::HeaderValue::from_static("curator/0.1.0"),
        );

        let client = Client::builder()
            .default_headers(headers)
            .timeout(StdDuration::from_secs(30))
            .build()
            .map_err(GiteaError::Http)?;

        Ok(Self {
            client,
            host,
            token: token.to_string(),
            platform,
        })
    }

    /// Get the host URL.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Get the platform type.
    pub fn platform_type(&self) -> CodePlatform {
        self.platform.clone()
    }

    /// Make an authenticated GET request.
    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, GiteaError> {
        let url = format!("{}/api/v1{}", self.host, path);

        let response = self
            .client
            .get(&url)
            .header(header::AUTHORIZATION, format!("token {}", self.token))
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            return Err(GiteaError::Api {
                status: status.as_u16(),
                message,
            });
        }

        let body = response.text().await?;
        serde_json::from_str(&body).map_err(GiteaError::Json)
    }

    /// Make an authenticated PUT request (for starring).
    async fn put(&self, path: &str) -> Result<StatusCode, GiteaError> {
        let url = format!("{}/api/v1{}", self.host, path);

        let response = self
            .client
            .put(&url)
            .header(header::AUTHORIZATION, format!("token {}", self.token))
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() && status != StatusCode::NO_CONTENT {
            let message = response.text().await.unwrap_or_default();
            return Err(GiteaError::Api {
                status: status.as_u16(),
                message,
            });
        }

        Ok(status)
    }

    /// Make an authenticated DELETE request (for unstarring).
    async fn delete(&self, path: &str) -> Result<StatusCode, GiteaError> {
        let url = format!("{}/api/v1{}", self.host, path);

        let response = self
            .client
            .delete(&url)
            .header(header::AUTHORIZATION, format!("token {}", self.token))
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() && status != StatusCode::NO_CONTENT {
            let message = response.text().await.unwrap_or_default();
            return Err(GiteaError::Api {
                status: status.as_u16(),
                message,
            });
        }

        Ok(status)
    }

    /// Get rate limit information.
    ///
    /// Gitea doesn't have a dedicated rate limit endpoint, so we return
    /// approximate values. The actual rate limits depend on the instance
    /// configuration.
    pub fn get_rate_limit(&self) -> RateLimitInfo {
        // Codeberg/Gitea defaults vary by instance
        // Codeberg has relatively generous limits
        RateLimitInfo {
            limit: 1000,
            remaining: 1000,
            reset_at: Utc::now() + chrono::Duration::minutes(1),
        }
    }

    /// Get information about an organization.
    pub async fn get_org_info_internal(&self, org: &str) -> Result<OrgInfo, GiteaError> {
        let org_data: GiteaOrg = self.get(&format!("/orgs/{}", org)).await.map_err(|e| {
            if matches!(e, GiteaError::Api { status: 404, .. }) {
                GiteaError::OrgNotFound(org.to_string())
            } else {
                e
            }
        })?;

        Ok(OrgInfo {
            name: org_data.full_name.unwrap_or(org_data.username),
            public_repos: 0, // Unknown without additional API call
            description: org_data.description,
        })
    }

    /// List all repositories for an organization with pagination.
    pub async fn list_org_repos(&self, org: &str) -> Result<Vec<GiteaRepo>, GiteaError> {
        let mut all_repos = Vec::new();
        let mut page = 1u32;

        loop {
            let repos: Vec<GiteaRepo> = self
                .get(&format!(
                    "/orgs/{}/repos?page={}&limit={}",
                    org, page, PAGE_SIZE
                ))
                .await?;

            let count = repos.len();
            all_repos.extend(repos);

            // If we got fewer than PAGE_SIZE, we've reached the end
            if count < PAGE_SIZE as usize {
                break;
            }

            page += 1;
        }

        Ok(all_repos)
    }

    /// Check if a repository is starred by the authenticated user.
    pub async fn is_repo_starred(&self, owner: &str, repo: &str) -> Result<bool, GiteaError> {
        let url = format!("{}/api/v1/user/starred/{}/{}", self.host, owner, repo);

        let response = self
            .client
            .get(&url)
            .header(header::AUTHORIZATION, format!("token {}", self.token))
            .send()
            .await?;

        match response.status() {
            StatusCode::NO_CONTENT => Ok(true), // 204 = starred
            StatusCode::NOT_FOUND => Ok(false), // 404 = not starred
            status => Err(GiteaError::Api {
                status: status.as_u16(),
                message: response.text().await.unwrap_or_default(),
            }),
        }
    }

    /// Star a repository.
    ///
    /// Returns Ok(true) if the repo was starred, Ok(false) if already starred.
    pub async fn star_repo(&self, owner: &str, repo: &str) -> Result<bool, GiteaError> {
        // Check if already starred
        if self.is_repo_starred(owner, repo).await? {
            return Ok(false);
        }

        // PUT /user/starred/{owner}/{repo}
        self.put(&format!("/user/starred/{}/{}", owner, repo))
            .await?;
        Ok(true)
    }

    /// Get information about the authenticated user.
    pub async fn get_user_info(&self) -> Result<GiteaAuthUser, GiteaError> {
        self.get("/user").await
    }

    /// List all repositories for a specific user with pagination.
    pub async fn list_user_repos_internal(
        &self,
        username: &str,
    ) -> Result<Vec<GiteaRepo>, GiteaError> {
        let mut all_repos = Vec::new();
        let mut page = 1u32;

        loop {
            let repos: Vec<GiteaRepo> = self
                .get(&format!(
                    "/users/{}/repos?page={}&limit={}",
                    username, page, PAGE_SIZE
                ))
                .await?;

            let count = repos.len();
            all_repos.extend(repos);

            // If we got fewer than PAGE_SIZE, we've reached the end
            if count < PAGE_SIZE as usize {
                break;
            }

            page += 1;
        }

        Ok(all_repos)
    }

    /// Unstar a repository.
    ///
    /// Returns Ok(true) if the repo was unstarred, Ok(false) if it wasn't starred.
    pub async fn unstar_repo_internal(&self, owner: &str, repo: &str) -> Result<bool, GiteaError> {
        // Check if starred first
        if !self.is_repo_starred(owner, repo).await? {
            return Ok(false);
        }

        // DELETE /user/starred/{owner}/{repo}
        self.delete(&format!("/user/starred/{}/{}", owner, repo))
            .await?;
        Ok(true)
    }

    /// List all repositories starred by the authenticated user with concurrent pagination.
    pub async fn list_starred_repos_internal(
        &self,
        concurrency: usize,
    ) -> Result<Vec<GiteaRepo>, GiteaError> {
        // Fetch first page to check if there are more
        let first_page: Vec<GiteaRepo> = self
            .get(&format!("/user/starred?page=1&limit={}", PAGE_SIZE))
            .await?;

        let first_page_count = first_page.len();
        let mut all_repos = first_page;

        // If first page is not full, we're done
        if first_page_count < PAGE_SIZE as usize {
            return Ok(all_repos);
        }

        // Fetch remaining pages concurrently using semaphore for rate control
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut page = 2u32;
        let mut handles = Vec::new();

        // Spawn page fetches - semaphore limits concurrency, no artificial caps
        loop {
            let task_semaphore = Arc::clone(&semaphore);
            let client = self.clone();
            let current_page = page;

            let handle = tokio::spawn(async move {
                let _permit = task_semaphore.acquire().await.ok()?;

                let repos: Result<Vec<GiteaRepo>, GiteaError> = client
                    .get(&format!(
                        "/user/starred?page={}&limit={}",
                        current_page, PAGE_SIZE
                    ))
                    .await;

                match repos {
                    Ok(repos) => Some((current_page, repos)),
                    Err(e) => {
                        tracing::warn!(
                            "Failed to fetch starred repos page {}: {}",
                            current_page,
                            short_error_message(&e)
                        );
                        None
                    }
                }
            });

            handles.push(handle);
            page += 1;

            // Process in batches to detect early termination
            if handles.len() >= concurrency {
                let mut got_partial = false;
                for handle in handles.drain(..) {
                    if let Ok(Some((_page_num, repos))) = handle.await {
                        let count = repos.len();
                        all_repos.extend(repos);
                        if count < PAGE_SIZE as usize {
                            got_partial = true;
                        }
                    }
                }
                if got_partial {
                    break;
                }
            }
        }

        // Collect any remaining results from handles
        for handle in handles {
            if let Ok(Some((_page_num, repos))) = handle.await {
                all_repos.extend(repos);
            }
        }

        Ok(all_repos)
    }
}

#[async_trait]
impl PlatformClient for GiteaClient {
    fn platform(&self) -> CodePlatform {
        self.platform.clone()
    }

    async fn get_rate_limit(&self) -> platform::Result<RateLimitInfo> {
        Ok(GiteaClient::get_rate_limit(self))
    }

    async fn get_org_info(&self, org: &str) -> platform::Result<OrgInfo> {
        self.get_org_info_internal(org)
            .await
            .map_err(PlatformError::from)
    }

    async fn get_authenticated_user(&self) -> platform::Result<UserInfo> {
        let user = self.get_user_info().await.map_err(PlatformError::from)?;

        Ok(UserInfo {
            username: user.login,
            name: user.full_name,
            email: user.email,
            bio: user.description,
            public_repos: 0, // Gitea doesn't provide this in user endpoint
            followers: user.followers_count,
        })
    }

    async fn list_org_repos(
        &self,
        org: &str,
        _db: Option<&sea_orm::DatabaseConnection>,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        // TODO: Implement ETag caching for Gitea
        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchingRepos {
                namespace: org.to_string(),
                total_repos: None,
                expected_pages: None,
            });
        }

        let repos = self.list_org_repos(org).await?;

        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchComplete { total: repos.len() });
        }

        // Convert to PlatformRepo
        let platform_repos: Vec<PlatformRepo> = repos.iter().map(to_platform_repo).collect();

        Ok(platform_repos)
    }

    async fn list_user_repos(
        &self,
        username: &str,
        _db: Option<&sea_orm::DatabaseConnection>,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        // TODO: Implement ETag caching for Gitea
        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchingRepos {
                namespace: username.to_string(),
                total_repos: None,
                expected_pages: None,
            });
        }

        let repos = self
            .list_user_repos_internal(username)
            .await
            .map_err(PlatformError::from)?;

        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchComplete { total: repos.len() });
        }

        // Convert to PlatformRepo
        let platform_repos: Vec<PlatformRepo> = repos.iter().map(to_platform_repo).collect();

        Ok(platform_repos)
    }

    async fn is_repo_starred(&self, owner: &str, name: &str) -> platform::Result<bool> {
        self.is_repo_starred(owner, name)
            .await
            .map_err(PlatformError::from)
    }

    async fn star_repo(&self, owner: &str, name: &str) -> platform::Result<bool> {
        self.star_repo(owner, name)
            .await
            .map_err(PlatformError::from)
    }

    async fn star_repo_with_retry(
        &self,
        owner: &str,
        name: &str,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<bool> {
        let owner_str = owner.to_string();
        let name_str = name.to_string();
        let client = self.clone();

        // Track attempt number for progress reporting
        let attempt = std::sync::atomic::AtomicU32::new(0);

        let star_op = || async {
            attempt.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            client.star_repo(&owner_str, &name_str).await
        };

        let result = star_op
            .retry(default_backoff())
            .notify(|err, dur| {
                let current_attempt = attempt.load(std::sync::atomic::Ordering::SeqCst);
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
                    short_error_message(err)
                );
            })
            .when(is_rate_limit_error)
            .await;

        result.map_err(PlatformError::from)
    }

    async fn unstar_repo(&self, owner: &str, name: &str) -> platform::Result<bool> {
        self.unstar_repo_internal(owner, name)
            .await
            .map_err(PlatformError::from)
    }

    async fn list_starred_repos(
        &self,
        _db: Option<&sea_orm::DatabaseConnection>,
        concurrency: usize,
        _skip_rate_checks: bool,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        // TODO: Implement ETag caching for Gitea
        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchingRepos {
                namespace: "starred".to_string(),
                total_repos: None,
                expected_pages: None,
            });
        }

        let repos = self
            .list_starred_repos_internal(concurrency)
            .await
            .map_err(PlatformError::from)?;

        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchComplete { total: repos.len() });
        }

        // Convert to PlatformRepo
        let platform_repos: Vec<PlatformRepo> = repos.iter().map(to_platform_repo).collect();

        Ok(platform_repos)
    }

    async fn list_starred_repos_streaming(
        &self,
        repo_tx: tokio::sync::mpsc::Sender<PlatformRepo>,
        _db: Option<&sea_orm::DatabaseConnection>,
        concurrency: usize,
        _skip_rate_checks: bool,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<usize> {
        // TODO: Implement ETag caching for Gitea
        use std::sync::atomic::{AtomicUsize, Ordering};

        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchingRepos {
                namespace: "starred".to_string(),
                total_repos: None,
                expected_pages: None,
            });
        }

        let total_sent = Arc::new(AtomicUsize::new(0));

        // Fetch first page
        let first_page: Vec<GiteaRepo> = self
            .get(&format!("/user/starred?page=1&limit={}", PAGE_SIZE))
            .await
            .map_err(PlatformError::from)?;

        let first_page_count = first_page.len();

        // Send first page repos immediately
        for repo in &first_page {
            let platform_repo = to_platform_repo(repo);
            if repo_tx.send(platform_repo).await.is_ok() {
                total_sent.fetch_add(1, Ordering::Relaxed);
            }
        }

        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchedPage {
                page: 1,
                count: first_page_count,
                total_so_far: total_sent.load(Ordering::Relaxed),
                expected_pages: None,
            });
        }

        // If first page is not full, we're done
        if first_page_count < PAGE_SIZE as usize {
            if let Some(cb) = on_progress {
                cb(SyncProgress::FetchComplete {
                    total: total_sent.load(Ordering::Relaxed),
                });
            }
            return Ok(total_sent.load(Ordering::Relaxed));
        }

        // Fetch remaining pages concurrently using semaphore for rate control
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut page = 2u32;
        let mut handles = Vec::new();

        // Spawn page fetches - semaphore limits concurrency, no artificial caps
        loop {
            let task_semaphore = Arc::clone(&semaphore);
            let client = self.clone();
            let task_repo_tx = repo_tx.clone();
            let task_total_sent = Arc::clone(&total_sent);
            let current_page = page;

            let handle = tokio::spawn(async move {
                let _permit = task_semaphore.acquire().await.ok()?;

                let repos: Result<Vec<GiteaRepo>, GiteaError> = client
                    .get(&format!(
                        "/user/starred?page={}&limit={}",
                        current_page, PAGE_SIZE
                    ))
                    .await;

                match repos {
                    Ok(repos) => {
                        let count = repos.len();
                        for repo in &repos {
                            let platform_repo = to_platform_repo(repo);
                            if task_repo_tx.send(platform_repo).await.is_ok() {
                                task_total_sent.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Some((current_page, count))
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to fetch starred repos page {}: {}",
                            current_page,
                            crate::gitea::error::short_error_message(&e)
                        );
                        None
                    }
                }
            });

            handles.push(handle);
            page += 1;

            // Process in batches to detect early termination
            if handles.len() >= concurrency {
                let mut got_partial = false;
                for handle in handles.drain(..) {
                    if let Ok(Some((page_num, count))) = handle.await {
                        if let Some(cb) = on_progress {
                            cb(SyncProgress::FetchedPage {
                                page: page_num,
                                count,
                                total_so_far: total_sent.load(Ordering::Relaxed),
                                expected_pages: None,
                            });
                        }
                        if count < PAGE_SIZE as usize {
                            got_partial = true;
                        }
                    }
                }
                if got_partial {
                    break;
                }
            }
        }

        // Collect any remaining results from handles
        for handle in handles {
            if let Ok(Some((page_num, count))) = handle.await
                && let Some(cb) = on_progress
            {
                cb(SyncProgress::FetchedPage {
                    page: page_num,
                    count,
                    total_so_far: total_sent.load(Ordering::Relaxed),
                    expected_pages: None,
                });
            }
        }

        if let Some(cb) = on_progress {
            cb(SyncProgress::FetchComplete {
                total: total_sent.load(Ordering::Relaxed),
            });
        }

        Ok(total_sent.load(Ordering::Relaxed))
    }

    fn to_active_model(&self, repo: &PlatformRepo) -> CodeRepositoryActiveModel {
        repo.to_active_model(self.platform())
    }
}

/// Create a Gitea client (convenience function).
///
/// # Arguments
///
/// * `host` - Gitea host URL (e.g., "https://codeberg.org")
/// * `token` - Personal access token
/// * `platform` - The platform type (Codeberg or Gitea)
pub fn create_client(
    host: &str,
    token: &str,
    platform: CodePlatform,
) -> Result<GiteaClient, GiteaError> {
    GiteaClient::new(host, token, platform)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_info() {
        let info = RateLimitInfo {
            limit: 1000,
            remaining: 999,
            reset_at: Utc::now(),
        };
        assert_eq!(info.limit, 1000);
        assert_eq!(info.remaining, 999);
    }

    #[test]
    fn test_gitea_error_to_platform_error() {
        let rate_limited = GiteaError::RateLimited {
            reset_at: Utc::now(),
        };
        let platform_err: PlatformError = rate_limited.into();
        assert!(matches!(platform_err, PlatformError::RateLimited { .. }));

        let not_found = GiteaError::OrgNotFound("test-org".to_string());
        let platform_err: PlatformError = not_found.into();
        assert!(matches!(platform_err, PlatformError::NotFound { .. }));
    }

    #[test]
    fn test_gitea_client_is_clone() {
        // Verify that GiteaClient implements Clone (compile-time check)
        fn assert_clone<T: Clone>() {}
        assert_clone::<GiteaClient>();
    }

    #[test]
    fn test_gitea_client_platform() {
        // Verify the trait implementation compiles
        fn assert_platform_client<T: PlatformClient>() {}
        assert_platform_client::<GiteaClient>();
    }

    #[test]
    fn test_codeberg_host() {
        assert_eq!(CODEBERG_HOST, "https://codeberg.org");
    }
}
