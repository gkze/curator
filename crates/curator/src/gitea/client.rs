//! Gitea API client creation and management.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::Utc;
use reqwest::{Client, StatusCode, header};
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::convert::to_platform_repo;
use super::error::{GiteaError, is_rate_limit_error, short_error_message};
use super::types::{GiteaAuthUser, GiteaOrg, GiteaRepo};
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::platform_type::PlatformType;
use crate::platform::{
    self, AdaptiveRateLimiter, OrgInfo, PlatformClient, PlatformError, PlatformRepo, RateLimitInfo,
    UserInfo,
};
use crate::retry::with_retry;
use crate::sync::{SyncProgress, emit};

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
    /// The instance ID this client is configured for.
    instance_id: Uuid,
    /// Optional adaptive rate limiter for pacing API requests.
    rate_limiter: Option<AdaptiveRateLimiter>,
}

impl GiteaClient {
    /// Create a new Gitea client.
    ///
    /// # Arguments
    ///
    /// * `host` - Gitea host URL (e.g., "https://codeberg.org")
    /// * `token` - Personal access token
    /// * `instance_id` - The instance ID this client is configured for
    ///
    /// # Example
    ///
    /// ```ignore
    /// use uuid::Uuid;
    ///
    /// // For Codeberg
    /// let instance_id = Uuid::new_v4();
    /// let client = GiteaClient::new("https://codeberg.org", "token", instance_id)?;
    ///
    /// // For self-hosted Gitea/Forgejo
    /// let client = GiteaClient::new("https://git.example.com", "token", instance_id)?;
    /// ```
    pub fn new(
        host: &str,
        token: &str,
        instance_id: Uuid,
        rate_limiter: Option<AdaptiveRateLimiter>,
    ) -> Result<Self, GiteaError> {
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
            instance_id,
            rate_limiter,
        })
    }

    /// Wait for rate limiter if one is configured.
    async fn wait_for_rate_limit(&self) {
        if let Some(ref limiter) = self.rate_limiter {
            limiter.wait().await;
        }
    }

    /// Update the rate limiter with rate limit info from response headers, if available.
    fn update_rate_limit(&self, headers: &reqwest::header::HeaderMap) {
        if let Some(ref limiter) = self.rate_limiter
            && let Some(info) = Self::parse_rate_limit_headers(headers)
        {
            limiter.update(&info);
        }
    }

    /// Extract rate limit info from Gitea response headers.
    fn parse_rate_limit_headers(headers: &reqwest::header::HeaderMap) -> Option<RateLimitInfo> {
        let limit = headers
            .get("x-ratelimit-limit")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())?;
        let remaining = headers
            .get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())?;
        let reset_epoch = headers
            .get("x-ratelimit-reset")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<i64>().ok())?;
        let reset_at = chrono::DateTime::from_timestamp(reset_epoch, 0)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        Some(RateLimitInfo {
            limit,
            remaining,
            reset_at,
            retry_after: None,
        })
    }

    /// Get the host URL.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Get the instance ID.
    pub fn get_instance_id(&self) -> Uuid {
        self.instance_id
    }

    /// Make an authenticated GET request.
    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, GiteaError> {
        self.wait_for_rate_limit().await;
        let url = format!("{}/api/v1{}", self.host, path);

        let response = self
            .client
            .get(&url)
            .header(header::AUTHORIZATION, format!("token {}", self.token))
            .send()
            .await?;

        let status = response.status();
        let headers = response.headers().clone();
        self.update_rate_limit(&headers);

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
        self.wait_for_rate_limit().await;
        let url = format!("{}/api/v1{}", self.host, path);

        let response = self
            .client
            .put(&url)
            .header(header::AUTHORIZATION, format!("token {}", self.token))
            .send()
            .await?;

        let status = response.status();
        let headers = response.headers().clone();
        self.update_rate_limit(&headers);

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
        self.wait_for_rate_limit().await;
        let url = format!("{}/api/v1{}", self.host, path);

        let response = self
            .client
            .delete(&url)
            .header(header::AUTHORIZATION, format!("token {}", self.token))
            .send()
            .await?;

        let status = response.status();
        let headers = response.headers().clone();
        self.update_rate_limit(&headers);

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
            retry_after: None,
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
    fn platform_type(&self) -> PlatformType {
        PlatformType::Gitea
    }

    fn instance_id(&self) -> Uuid {
        self.instance_id
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

    async fn get_repo(
        &self,
        owner: &str,
        name: &str,
        _db: Option<&sea_orm::DatabaseConnection>,
    ) -> platform::Result<PlatformRepo> {
        let repo = self
            .get::<GiteaRepo>(&format!("/repos/{}/{}", owner, name))
            .await
            .map_err(PlatformError::from)?;

        Ok(to_platform_repo(&repo))
    }

    async fn list_org_repos(
        &self,
        org: &str,
        _db: Option<&sea_orm::DatabaseConnection>,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        // TODO: Implement ETag caching for Gitea
        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: org.to_string(),
                total_repos: None,
                expected_pages: None,
            },
        );

        let repos = self.list_org_repos(org).await?;

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: org.to_string(),
                total: repos.len(),
            },
        );

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
        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: username.to_string(),
                total_repos: None,
                expected_pages: None,
            },
        );

        let repos = self
            .list_user_repos_internal(username)
            .await
            .map_err(PlatformError::from)?;

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: username.to_string(),
                total: repos.len(),
            },
        );

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
        let client = self.clone();
        let owner_str = owner.to_string();
        let name_str = name.to_string();

        with_retry(
            || async { client.star_repo(&owner_str, &name_str).await },
            is_rate_limit_error,
            short_error_message,
            owner,
            name,
            on_progress,
        )
        .await
        .map_err(PlatformError::from)
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
        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: "starred".to_string(),
                total_repos: None,
                expected_pages: None,
            },
        );

        let repos = self
            .list_starred_repos_internal(concurrency)
            .await
            .map_err(PlatformError::from)?;

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: "starred".to_string(),
                total: repos.len(),
            },
        );

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

        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: "starred".to_string(),
                total_repos: None,
                expected_pages: None,
            },
        );

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

        emit(
            on_progress,
            SyncProgress::FetchedPage {
                namespace: "starred".to_string(),
                page: 1,
                count: first_page_count,
                total_so_far: total_sent.load(Ordering::Relaxed),
                expected_pages: None,
            },
        );

        // If first page is not full, we're done
        if first_page_count < PAGE_SIZE as usize {
            emit(
                on_progress,
                SyncProgress::FetchComplete {
                    namespace: "starred".to_string(),
                    total: total_sent.load(Ordering::Relaxed),
                },
            );
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
                        emit(
                            on_progress,
                            SyncProgress::FetchedPage {
                                namespace: "starred".to_string(),
                                page: page_num,
                                count,
                                total_so_far: total_sent.load(Ordering::Relaxed),
                                expected_pages: None,
                            },
                        );
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
            if let Ok(Some((page_num, count))) = handle.await {
                emit(
                    on_progress,
                    SyncProgress::FetchedPage {
                        namespace: "starred".to_string(),
                        page: page_num,
                        count,
                        total_so_far: total_sent.load(Ordering::Relaxed),
                        expected_pages: None,
                    },
                );
            }
        }

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: "starred".to_string(),
                total: total_sent.load(Ordering::Relaxed),
            },
        );

        Ok(total_sent.load(Ordering::Relaxed))
    }

    fn to_active_model(&self, repo: &PlatformRepo) -> CodeRepositoryActiveModel {
        repo.to_active_model(self.instance_id)
    }
}

/// Create a Gitea client (convenience function).
///
/// # Arguments
///
/// * `host` - Gitea host URL (e.g., "https://codeberg.org")
/// * `token` - Personal access token
/// * `instance_id` - The instance ID this client is configured for
pub fn create_client(
    host: &str,
    token: &str,
    instance_id: Uuid,
) -> Result<GiteaClient, GiteaError> {
    GiteaClient::new(host, token, instance_id, None)
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
            retry_after: None,
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
