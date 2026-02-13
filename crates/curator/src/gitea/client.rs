//! Gitea API client creation and management.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::convert::to_platform_repo;
use super::error::{GiteaError, is_rate_limit_error, short_error_message};
use super::types::{GiteaAuthUser, GiteaOrg, GiteaRepo};
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::platform_type::PlatformType;
use crate::platform::{
    self, AdaptiveRateLimiter, OrgInfo, PlatformClient, PlatformError, PlatformRepo, RateLimitInfo,
    UserInfo, handle_streaming_cache_hit_fallback, load_repos_by_instance,
};
use crate::retry::with_retry;
use crate::sync::{SyncProgress, emit};

use crate::http::reqwest_transport::ReqwestTransport;
use crate::http::{HttpHeaders, HttpMethod, HttpRequest, HttpResponse, HttpTransport, header_get};

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
    transport: Arc<dyn HttpTransport>,
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
        let host = host.trim_end_matches('/');
        let transport = ReqwestTransport::with_timeout(StdDuration::from_secs(30))
            .map_err(|e| GiteaError::Config(e.to_string()))?;

        Ok(Self::new_with_transport(
            host,
            token,
            instance_id,
            rate_limiter,
            Arc::new(transport),
        ))
    }

    pub fn new_with_transport(
        host: &str,
        token: &str,
        instance_id: Uuid,
        rate_limiter: Option<AdaptiveRateLimiter>,
        transport: Arc<dyn HttpTransport>,
    ) -> Self {
        let host = host.trim_end_matches('/').to_string();
        Self {
            transport,
            host,
            token: token.to_string(),
            instance_id,
            rate_limiter,
        }
    }

    /// Wait for rate limiter if one is configured.
    async fn wait_for_rate_limit(&self) {
        if let Some(ref limiter) = self.rate_limiter {
            limiter.wait().await;
        }
    }

    /// Update the rate limiter with rate limit info from response headers, if available.
    fn update_rate_limit(&self, headers: &HttpHeaders) {
        if let Some(ref limiter) = self.rate_limiter
            && let Some(info) = Self::parse_rate_limit_headers(headers)
        {
            limiter.update(&info);
        }
    }

    /// Extract rate limit info from Gitea response headers.
    fn parse_rate_limit_headers(headers: &HttpHeaders) -> Option<RateLimitInfo> {
        let limit = header_get(headers, "x-ratelimit-limit")?
            .parse::<usize>()
            .ok()?;
        let remaining = header_get(headers, "x-ratelimit-remaining")?
            .parse::<usize>()
            .ok()?;
        let reset_epoch = header_get(headers, "x-ratelimit-reset")?
            .parse::<i64>()
            .ok()?;
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

        let request = HttpRequest {
            method: HttpMethod::Get,
            url,
            headers: vec![
                ("Accept".to_string(), "application/json".to_string()),
                ("User-Agent".to_string(), "curator".to_string()),
                ("Authorization".to_string(), format!("token {}", self.token)),
            ],
            body: Vec::new(),
        };

        let response: HttpResponse = self
            .transport
            .send(request)
            .await
            .map_err(|e| GiteaError::Http(e.to_string()))?;

        self.update_rate_limit(&response.headers);

        if !(200..300).contains(&response.status) {
            let message = String::from_utf8_lossy(&response.body).to_string();
            return Err(GiteaError::Api {
                status: response.status,
                message,
            });
        }

        serde_json::from_slice(&response.body).map_err(GiteaError::Json)
    }

    /// Make a conditional GET request with optional `If-None-Match` header.
    ///
    /// If `cached_etag` is provided the server may return 304 Not Modified,
    /// avoiding the cost of transferring and deserializing the response body.
    async fn get_conditional<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        cached_etag: Option<&str>,
    ) -> Result<platform::FetchResult<T>, GiteaError> {
        use crate::platform::PaginationInfo;

        self.wait_for_rate_limit().await;
        let url = format!("{}/api/v1{}", self.host, path);

        let mut headers = vec![
            ("Accept".to_string(), "application/json".to_string()),
            ("User-Agent".to_string(), "curator".to_string()),
            ("Authorization".to_string(), format!("token {}", self.token)),
        ];
        if let Some(etag) = cached_etag {
            headers.push(("If-None-Match".to_string(), etag.to_string()));
        }

        let request = HttpRequest {
            method: HttpMethod::Get,
            url,
            headers,
            body: Vec::new(),
        };

        let response = self
            .transport
            .send(request)
            .await
            .map_err(|e| GiteaError::Http(e.to_string()))?;

        self.update_rate_limit(&response.headers);

        match response.status {
            304 => Ok(platform::FetchResult::NotModified),
            s if (200..300).contains(&s) => {
                let etag = header_get(&response.headers, "etag").map(|s| s.to_string());
                let data: T = serde_json::from_slice(&response.body).map_err(GiteaError::Json)?;
                Ok(platform::FetchResult::Fetched {
                    data,
                    etag,
                    pagination: PaginationInfo::default(),
                })
            }
            _ => {
                let message = String::from_utf8_lossy(&response.body).to_string();
                Err(GiteaError::Api {
                    status: response.status,
                    message,
                })
            }
        }
    }

    /// Make an authenticated PUT request (for starring).
    async fn put(&self, path: &str) -> Result<u16, GiteaError> {
        self.wait_for_rate_limit().await;
        let url = format!("{}/api/v1{}", self.host, path);

        let request = HttpRequest {
            method: HttpMethod::Put,
            url,
            headers: vec![
                ("Accept".to_string(), "application/json".to_string()),
                ("User-Agent".to_string(), "curator".to_string()),
                ("Authorization".to_string(), format!("token {}", self.token)),
            ],
            body: Vec::new(),
        };

        let response = self
            .transport
            .send(request)
            .await
            .map_err(|e| GiteaError::Http(e.to_string()))?;

        self.update_rate_limit(&response.headers);

        if !(200..300).contains(&response.status) && response.status != 204 {
            let message = String::from_utf8_lossy(&response.body).to_string();
            return Err(GiteaError::Api {
                status: response.status,
                message,
            });
        }

        Ok(response.status)
    }

    /// Make an authenticated DELETE request (for unstarring).
    async fn delete(&self, path: &str) -> Result<u16, GiteaError> {
        self.wait_for_rate_limit().await;
        let url = format!("{}/api/v1{}", self.host, path);

        let request = HttpRequest {
            method: HttpMethod::Delete,
            url,
            headers: vec![
                ("Accept".to_string(), "application/json".to_string()),
                ("User-Agent".to_string(), "curator".to_string()),
                ("Authorization".to_string(), format!("token {}", self.token)),
            ],
            body: Vec::new(),
        };

        let response = self
            .transport
            .send(request)
            .await
            .map_err(|e| GiteaError::Http(e.to_string()))?;

        self.update_rate_limit(&response.headers);

        if !(200..300).contains(&response.status) && response.status != 204 {
            let message = String::from_utf8_lossy(&response.body).to_string();
            return Err(GiteaError::Api {
                status: response.status,
                message,
            });
        }

        Ok(response.status)
    }

    /// Get rate limit information.
    ///
    /// Gitea doesn't have a dedicated rate limit endpoint. Returns the current
    /// effective RPS from the adaptive rate limiter when available, otherwise
    /// returns conservative defaults.
    pub fn get_rate_limit(&self) -> RateLimitInfo {
        if let Some(ref limiter) = self.rate_limiter {
            let rps = limiter.current_rps();
            // Approximate remaining from current pacing
            let remaining = (rps * 60.0) as usize;
            RateLimitInfo {
                limit: remaining,
                remaining,
                reset_at: Utc::now() + chrono::Duration::minutes(1),
                retry_after: None,
            }
        } else {
            RateLimitInfo {
                limit: 1000,
                remaining: 1000,
                reset_at: Utc::now() + chrono::Duration::minutes(1),
                retry_after: None,
            }
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
            public_repos: org_data.repo_count,
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
        self.wait_for_rate_limit().await;
        let url = format!("{}/api/v1/user/starred/{}/{}", self.host, owner, repo);

        let request = HttpRequest {
            method: HttpMethod::Get,
            url,
            headers: vec![
                ("Accept".to_string(), "application/json".to_string()),
                ("User-Agent".to_string(), "curator".to_string()),
                ("Authorization".to_string(), format!("token {}", self.token)),
            ],
            body: Vec::new(),
        };

        let response = self
            .transport
            .send(request)
            .await
            .map_err(|e| GiteaError::Http(e.to_string()))?;

        self.update_rate_limit(&response.headers);

        match response.status {
            204 => Ok(true),
            404 => Ok(false),
            status => Err(GiteaError::Api {
                status,
                message: String::from_utf8_lossy(&response.body).to_string(),
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
        // NOTE: ETag caching not yet implemented for org/user repos (only starred).
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
        // NOTE: ETag caching not yet implemented for org/user repos (only starred).
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
        db: Option<&sea_orm::DatabaseConnection>,
        concurrency: usize,
        _skip_rate_checks: bool,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<usize> {
        use crate::api_cache;
        use crate::entity::api_cache::{EndpointType, Model as ApiCacheModel};
        use std::sync::atomic::{AtomicUsize, Ordering};

        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: "starred".to_string(),
                total_repos: None,
                expected_pages: None,
            },
        );

        // Resolve the authenticated username for cache key scoping
        let cache_username = self
            .get_user_info()
            .await
            .map(|u| u.login)
            .unwrap_or_else(|_| "starred".to_string());

        let total_sent = Arc::new(AtomicUsize::new(0));

        // Fetch first page with conditional ETag request
        let cached_etag = if let Some(db) = db {
            let ck = ApiCacheModel::starred_key(&cache_username, 1);
            api_cache::get_etag(db, self.instance_id, EndpointType::Starred, &ck)
                .await
                .ok()
                .flatten()
        } else {
            None
        };

        let first_result: platform::FetchResult<Vec<GiteaRepo>> = self
            .get_conditional(
                &format!("/user/starred?page=1&limit={}", PAGE_SIZE),
                cached_etag.as_deref(),
            )
            .await
            .map_err(PlatformError::from)?;

        let mut all_cache_hits;

        match first_result {
            platform::FetchResult::NotModified => {
                // Cache hit — load from DB if available
                let instance_id = self.instance_id;
                handle_streaming_cache_hit_fallback(
                    db,
                    1, // one cache hit (this page)
                    |db| load_repos_by_instance(db, instance_id),
                    "starred",
                    &repo_tx,
                    &total_sent,
                    on_progress,
                )
                .await?;

                // First page was NotModified — no pagination info, can't determine total pages
                let sent = total_sent.load(Ordering::Relaxed);
                emit(
                    on_progress,
                    SyncProgress::FetchComplete {
                        namespace: "starred".to_string(),
                        total: sent,
                    },
                );
                return Ok(sent);
            }
            platform::FetchResult::Fetched {
                data: first_page,
                etag,
                ..
            } => {
                all_cache_hits = false;
                let first_page_count = first_page.len();

                // Store ETag for page 1
                if let Some(db) = db {
                    let ck = ApiCacheModel::starred_key(&cache_username, 1);
                    if let Err(e) = api_cache::upsert_with_pagination(
                        db,
                        self.instance_id,
                        EndpointType::Starred,
                        &ck,
                        etag,
                        None,
                    )
                    .await
                    {
                        tracing::debug!("api cache upsert failed: {e}");
                    }
                }

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
            }
        }

        // Fetch remaining pages concurrently using semaphore for rate control.
        // Safety cap: Gitea doesn't expose total pages, so we cap at a generous
        // upper bound to prevent unbounded loops.
        const MAX_PAGES: u32 = 500;
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut page = 2u32;
        let mut handles = Vec::new();

        loop {
            if page > MAX_PAGES {
                tracing::warn!(
                    "Reached maximum page limit ({}) for starred repos pagination",
                    MAX_PAGES
                );
                break;
            }

            let task_semaphore = Arc::clone(&semaphore);
            let client = self.clone();
            let task_repo_tx = repo_tx.clone();
            let task_total_sent = Arc::clone(&total_sent);
            let current_page = page;

            // Pre-fetch ETag outside the spawn so we can borrow db
            let task_cached_etag = if let Some(db) = db {
                let ck = ApiCacheModel::starred_key(&cache_username, page);
                api_cache::get_etag(db, self.instance_id, EndpointType::Starred, &ck)
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            };

            let handle = tokio::spawn(async move {
                let _permit = task_semaphore.acquire().await.ok()?;

                let result: Result<platform::FetchResult<Vec<GiteaRepo>>, GiteaError> = client
                    .get_conditional(
                        &format!("/user/starred?page={}&limit={}", current_page, PAGE_SIZE),
                        task_cached_etag.as_deref(),
                    )
                    .await;

                match result {
                    Ok(platform::FetchResult::NotModified) => {
                        Some((current_page, 0usize, true, None))
                    }
                    Ok(platform::FetchResult::Fetched {
                        data: repos, etag, ..
                    }) => {
                        let count = repos.len();
                        for repo in &repos {
                            let platform_repo = to_platform_repo(repo);
                            if task_repo_tx.send(platform_repo).await.is_ok() {
                                task_total_sent.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Some((current_page, count, false, etag))
                    }
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
                let mut batch_all_cache_hits = true;
                for handle in handles.drain(..) {
                    if let Ok(Some((page_num, count, was_cache_hit, etag))) = handle.await {
                        if !was_cache_hit {
                            all_cache_hits = false;
                            batch_all_cache_hits = false;

                            // Store ETag for fetched pages
                            if let Some(db) = db {
                                let ck = ApiCacheModel::starred_key(&cache_username, page_num);
                                if let Err(e) = api_cache::upsert_with_pagination(
                                    db,
                                    self.instance_id,
                                    EndpointType::Starred,
                                    &ck,
                                    etag,
                                    None,
                                )
                                .await
                                {
                                    tracing::debug!("api cache upsert failed: {e}");
                                }
                            }

                            if count < PAGE_SIZE as usize {
                                got_partial = true;
                            }
                        }

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
                // Terminate if we got a partial page OR if every page in this
                // batch was a cache hit (no new data to discover).
                if got_partial || batch_all_cache_hits {
                    break;
                }
            }
        }

        // Collect any remaining results from handles
        for handle in handles {
            if let Ok(Some((page_num, count, was_cache_hit, etag))) = handle.await {
                if !was_cache_hit {
                    all_cache_hits = false;

                    if let Some(db) = db {
                        let ck = ApiCacheModel::starred_key(&cache_username, page_num);
                        if let Err(e) = api_cache::upsert_with_pagination(
                            db,
                            self.instance_id,
                            EndpointType::Starred,
                            &ck,
                            etag,
                            None,
                        )
                        .await
                        {
                            tracing::debug!("api cache upsert failed: {e}");
                        }
                    }
                }

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

        // Cache-hit fallback: if all pages were 304 and nothing was sent
        if all_cache_hits && total_sent.load(Ordering::Relaxed) == 0 {
            let instance_id = self.instance_id;
            handle_streaming_cache_hit_fallback(
                db,
                1, // all pages were cache hits
                |db| load_repos_by_instance(db, instance_id),
                "starred",
                &repo_tx,
                &total_sent,
                on_progress,
            )
            .await?;
        }

        let sent = total_sent.load(Ordering::Relaxed);
        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: "starred".to_string(),
                total: sent,
            },
        );

        Ok(sent)
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
    use crate::http::{HttpMethod, HttpResponse, MockTransport};
    use std::sync::Arc;

    fn to_headers(pairs: Vec<(&str, &str)>) -> HttpHeaders {
        pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn response(status: u16, headers: Vec<(&str, &str)>, body: impl AsRef<[u8]>) -> HttpResponse {
        HttpResponse {
            status,
            headers: to_headers(headers),
            body: body.as_ref().to_vec(),
        }
    }

    fn repo_json(id: i64) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "name": format!("repo-{id}"),
            "full_name": format!("owner/repo-{id}"),
            "description": null,
            "default_branch": "main",
            "private": false,
            "fork": false,
            "archived": false,
            "mirror": false,
            "empty": false,
            "template": false,
            "stars_count": 0,
            "forks_count": 0,
            "open_issues_count": 0,
            "watchers_count": 0,
            "size": 1,
            "language": "Rust",
            "topics": [],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "pushed_at": "2024-01-03T00:00:00Z",
            "owner": {
                "id": 1,
                "login": "owner",
                "full_name": "Owner",
                "avatar_url": null
            },
            "html_url": format!("https://forge.test/owner/repo-{id}"),
            "ssh_url": format!("ssh://git@forge.test/owner/repo-{id}.git"),
            "clone_url": format!("https://forge.test/owner/repo-{id}.git"),
            "website": null,
            "has_issues": true,
            "has_wiki": true,
            "has_pull_requests": true
        })
    }

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

    #[test]
    fn test_new_normalizes_host_and_sets_instance_id() {
        let instance_id = Uuid::new_v4();
        let transport = MockTransport::new();
        let client = GiteaClient::new_with_transport(
            "https://codeberg.org/",
            "token",
            instance_id,
            None,
            Arc::new(transport),
        );

        assert_eq!(client.host(), "https://codeberg.org");
        assert_eq!(client.get_instance_id(), instance_id);
    }

    #[test]
    fn test_new_normalizes_host_with_multiple_trailing_slashes() {
        let transport = MockTransport::new();
        let client = GiteaClient::new_with_transport(
            "https://forge.example///",
            "token",
            Uuid::new_v4(),
            None,
            Arc::new(transport),
        );

        assert_eq!(client.host(), "https://forge.example");
    }

    #[test]
    fn test_parse_rate_limit_headers_success() {
        let headers = to_headers(vec![
            ("x-ratelimit-limit", "1000"),
            ("x-ratelimit-remaining", "777"),
            ("x-ratelimit-reset", "1706400000"),
        ]);

        let info = GiteaClient::parse_rate_limit_headers(&headers).expect("headers should parse");
        assert_eq!(info.limit, 1000);
        assert_eq!(info.remaining, 777);
        assert!(info.retry_after.is_none());
    }

    #[test]
    fn test_parse_rate_limit_headers_missing_values() {
        let headers: HttpHeaders = Vec::new();
        assert!(GiteaClient::parse_rate_limit_headers(&headers).is_none());
    }

    #[test]
    fn test_parse_rate_limit_headers_invalid_number() {
        let headers = to_headers(vec![
            ("x-ratelimit-limit", "invalid"),
            ("x-ratelimit-remaining", "10"),
            ("x-ratelimit-reset", "1706400000"),
        ]);

        assert!(GiteaClient::parse_rate_limit_headers(&headers).is_none());
    }

    #[test]
    fn test_parse_rate_limit_headers_invalid_remaining_returns_none() {
        let headers = to_headers(vec![
            ("x-ratelimit-limit", "1000"),
            ("x-ratelimit-remaining", "bad"),
            ("x-ratelimit-reset", "1706400000"),
        ]);

        assert!(GiteaClient::parse_rate_limit_headers(&headers).is_none());
    }

    #[test]
    fn test_parse_rate_limit_headers_invalid_reset_number_returns_none() {
        let headers = to_headers(vec![
            ("x-ratelimit-limit", "1000"),
            ("x-ratelimit-remaining", "10"),
            ("x-ratelimit-reset", "not-a-number"),
        ]);

        assert!(GiteaClient::parse_rate_limit_headers(&headers).is_none());
    }

    #[test]
    fn test_parse_rate_limit_headers_invalid_reset_falls_back_to_now() {
        let headers = to_headers(vec![
            ("x-ratelimit-limit", "1000"),
            ("x-ratelimit-remaining", "10"),
            ("x-ratelimit-reset", "9223372036854775807"),
        ]);

        let before = Utc::now();
        let info = GiteaClient::parse_rate_limit_headers(&headers).expect("headers should parse");
        assert_eq!(info.limit, 1000);
        assert_eq!(info.remaining, 10);
        assert!(info.reset_at >= before);
    }

    #[test]
    fn test_parse_rate_limit_headers_zero_reset_epoch() {
        let headers = to_headers(vec![
            ("x-ratelimit-limit", "1000"),
            ("x-ratelimit-remaining", "250"),
            ("x-ratelimit-reset", "0"),
        ]);

        let info = GiteaClient::parse_rate_limit_headers(&headers).expect("headers should parse");
        assert_eq!(info.limit, 1000);
        assert_eq!(info.remaining, 250);
        assert_eq!(info.reset_at.timestamp(), 0);
    }

    #[test]
    fn test_get_rate_limit_without_limiter_uses_defaults() {
        let transport = MockTransport::new();
        let client = GiteaClient::new_with_transport(
            "https://codeberg.org",
            "token",
            Uuid::new_v4(),
            None,
            Arc::new(transport),
        );

        let info = client.get_rate_limit();
        assert_eq!(info.limit, 1000);
        assert_eq!(info.remaining, 1000);
        assert!(info.retry_after.is_none());
    }

    #[test]
    fn test_get_rate_limit_with_limiter_uses_current_rps() {
        let limiter = AdaptiveRateLimiter::new(5);
        let transport = MockTransport::new();
        let client = GiteaClient::new_with_transport(
            "https://codeberg.org",
            "token",
            Uuid::new_v4(),
            Some(limiter),
            Arc::new(transport),
        );

        let info = client.get_rate_limit();
        assert_eq!(info.limit, 300);
        assert_eq!(info.remaining, 300);
        assert!(info.retry_after.is_none());
    }

    #[tokio::test]
    async fn test_get_conditional_not_modified_returns_cache_hit() {
        let transport = MockTransport::new();
        let host = "https://forge.test";
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/user/starred?page=1&limit=50"),
            response(304, vec![], ""),
        );

        let client = GiteaClient::new_with_transport(
            host,
            "token",
            Uuid::new_v4(),
            None,
            Arc::new(transport.clone()),
        );

        let result = client
            .get_conditional::<Vec<GiteaRepo>>("/user/starred?page=1&limit=50", Some("etag-1"))
            .await
            .expect("request should succeed");

        assert!(matches!(result, platform::FetchResult::NotModified));
        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0]
                .headers
                .iter()
                .any(|(k, v)| k.eq_ignore_ascii_case("if-none-match") && v == "etag-1")
        );
    }

    #[tokio::test]
    async fn test_get_conditional_error_status_maps_to_api_error() {
        let transport = MockTransport::new();
        let host = "https://forge.test";
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/user/starred?page=1&limit=50"),
            response(429, vec![], "slow down"),
        );

        let client = GiteaClient::new_with_transport(
            host,
            "token",
            Uuid::new_v4(),
            None,
            Arc::new(transport),
        );

        let err = client
            .get_conditional::<Vec<GiteaRepo>>("/user/starred?page=1&limit=50", None)
            .await
            .expect_err("429 should return an API error");

        match err {
            GiteaError::Api { status, message } => {
                assert_eq!(status, 429);
                assert_eq!(message, "slow down");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_list_org_repos_paginates_until_partial_page() {
        let first_page = serde_json::to_string(&vec![repo_json(1); PAGE_SIZE as usize])
            .expect("first page should serialize");
        let second_page = serde_json::to_string(&vec![repo_json(2), repo_json(3)])
            .expect("second page should serialize");

        let transport = MockTransport::new();
        let host = "https://forge.test";
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/orgs/acme/repos?page=1&limit=50"),
            response(200, vec![("Content-Type", "application/json")], first_page),
        );
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/orgs/acme/repos?page=2&limit=50"),
            response(200, vec![("Content-Type", "application/json")], second_page),
        );

        let client = GiteaClient::new_with_transport(
            host,
            "token",
            Uuid::new_v4(),
            None,
            Arc::new(transport.clone()),
        );

        let repos = client
            .list_org_repos("acme")
            .await
            .expect("paginated fetch should succeed");

        assert_eq!(repos.len(), PAGE_SIZE as usize + 2);

        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].url,
            format!("{host}/api/v1/orgs/acme/repos?page=1&limit=50")
        );
        assert_eq!(
            requests[1].url,
            format!("{host}/api/v1/orgs/acme/repos?page=2&limit=50")
        );
    }

    #[tokio::test]
    async fn test_list_starred_repos_internal_stops_after_partial_batch() {
        let first_page = serde_json::to_string(&vec![repo_json(10); PAGE_SIZE as usize])
            .expect("first page should serialize");
        let second_page =
            serde_json::to_string(&vec![repo_json(20)]).expect("second page should serialize");

        let transport = MockTransport::new();
        let host = "https://forge.test";
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/user/starred?page=1&limit=50"),
            response(200, vec![("Content-Type", "application/json")], first_page),
        );
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/user/starred?page=2&limit=50"),
            response(200, vec![("Content-Type", "application/json")], second_page),
        );

        let client = GiteaClient::new_with_transport(
            host,
            "token",
            Uuid::new_v4(),
            None,
            Arc::new(transport.clone()),
        );

        let repos = client
            .list_starred_repos_internal(1)
            .await
            .expect("starred pagination should succeed");

        assert_eq!(repos.len(), PAGE_SIZE as usize + 1);

        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].url,
            format!("{host}/api/v1/user/starred?page=1&limit=50")
        );
        assert_eq!(
            requests[1].url,
            format!("{host}/api/v1/user/starred?page=2&limit=50")
        );
    }

    #[tokio::test]
    async fn test_list_starred_repos_internal_continues_after_error_until_partial_page() {
        let first_page = serde_json::to_string(&vec![repo_json(10); PAGE_SIZE as usize])
            .expect("first page should serialize");
        let third_page =
            serde_json::to_string(&vec![repo_json(30)]).expect("third page should serialize");

        let transport = MockTransport::new();
        let host = "https://forge.test";
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/user/starred?page=1&limit=50"),
            response(200, vec![("Content-Type", "application/json")], first_page),
        );
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/user/starred?page=2&limit=50"),
            response(500, vec![], "boom"),
        );
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/user/starred?page=3&limit=50"),
            response(200, vec![("Content-Type", "application/json")], third_page),
        );

        let client = GiteaClient::new_with_transport(
            host,
            "token",
            Uuid::new_v4(),
            None,
            Arc::new(transport.clone()),
        );

        let repos = client
            .list_starred_repos_internal(1)
            .await
            .expect("starred pagination should succeed despite one failed page");

        assert_eq!(repos.len(), PAGE_SIZE as usize + 1);

        let requests = transport.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[1].url,
            format!("{host}/api/v1/user/starred?page=2&limit=50")
        );
        assert_eq!(
            requests[2].url,
            format!("{host}/api/v1/user/starred?page=3&limit=50")
        );
    }

    #[tokio::test]
    async fn test_is_repo_starred_returns_true_for_no_content() {
        let transport = MockTransport::new();
        let host = "https://forge.test";
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/user/starred/owner/repo"),
            response(204, vec![], ""),
        );

        let client = GiteaClient::new_with_transport(
            host,
            "token",
            Uuid::new_v4(),
            None,
            Arc::new(transport),
        );

        let is_starred = client
            .is_repo_starred("owner", "repo")
            .await
            .expect("204 should map to starred");

        assert!(is_starred);
    }

    #[tokio::test]
    async fn test_is_repo_starred_unexpected_status_returns_api_error() {
        let transport = MockTransport::new();
        let host = "https://forge.test";
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/user/starred/owner/repo"),
            response(500, vec![], "boom"),
        );

        let client = GiteaClient::new_with_transport(
            host,
            "token",
            Uuid::new_v4(),
            None,
            Arc::new(transport),
        );

        let err = client
            .is_repo_starred("owner", "repo")
            .await
            .expect_err("500 should map to API error");

        match err {
            GiteaError::Api { status, message } => {
                assert_eq!(status, 500);
                assert_eq!(message, "boom");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_star_repo_returns_false_without_put_when_already_starred() {
        let transport = MockTransport::new();
        let host = "https://forge.test";
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/user/starred/owner/repo"),
            response(204, vec![], ""),
        );

        let client = GiteaClient::new_with_transport(
            host,
            "token",
            Uuid::new_v4(),
            None,
            Arc::new(transport.clone()),
        );

        let changed = client
            .star_repo("owner", "repo")
            .await
            .expect("already-starred branch should succeed");

        assert!(!changed);
        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].url,
            format!("{host}/api/v1/user/starred/owner/repo")
        );
    }

    #[tokio::test]
    async fn test_unstar_repo_internal_returns_false_without_delete_when_not_starred() {
        let transport = MockTransport::new();
        let host = "https://forge.test";
        transport.push_response(
            HttpMethod::Get,
            format!("{host}/api/v1/user/starred/owner/repo"),
            response(404, vec![], "not starred"),
        );

        let client = GiteaClient::new_with_transport(
            host,
            "token",
            Uuid::new_v4(),
            None,
            Arc::new(transport.clone()),
        );

        let changed = client
            .unstar_repo_internal("owner", "repo")
            .await
            .expect("not-starred branch should succeed");

        assert!(!changed);
        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].url,
            format!("{host}/api/v1/user/starred/owner/repo")
        );
    }
}
