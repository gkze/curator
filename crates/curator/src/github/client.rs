//! GitHub API client creation and rate limit management.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use octocrab::Octocrab;
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, IF_NONE_MATCH};
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;

use uuid::Uuid;

use super::convert::to_platform_repo;
use super::error::{GitHubError, is_rate_limit_error_from_github, short_error_message};
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::platform_type::PlatformType;
use crate::platform::{
    self, AdaptiveRateLimiter, CacheStats, FetchResult, OrgInfo, PaginationInfo, PlatformClient,
    PlatformError, PlatformRepo, RateLimitInfo, UserInfo, handle_cache_hit_fallback,
    load_repos_by_instance, load_repos_by_instance_and_owner,
};
use crate::retry::with_retry;
use crate::sync::{SyncProgress, emit};

/// Extract ETag from response headers.
///
/// Returns the ETag value if present, handling both strong and weak ETags.
pub fn extract_etag(headers: &HeaderMap) -> Option<String> {
    headers
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
}

/// Pagination information extracted from GitHub's Link header.
///
/// This is a GitHub-specific type for parsing the Link header format.
/// Use `to_pagination_info()` to convert to the shared `PaginationInfo` type.
#[derive(Debug, Clone, Default)]
pub struct LinkPagination {
    /// The last page number (from rel="last" link).
    pub last_page: Option<u32>,
    /// The next page number (from rel="next" link).
    pub next_page: Option<u32>,
}

impl LinkPagination {
    /// Returns the total number of pages if known.
    pub fn total_pages(&self) -> Option<u32> {
        self.last_page
    }

    /// Convert to the shared PaginationInfo type.
    pub fn to_pagination_info(&self) -> PaginationInfo {
        PaginationInfo {
            total_pages: self.last_page,
            next_page: self.next_page,
        }
    }
}

/// Parse the Link header to extract pagination info.
///
/// GitHub Link headers look like:
/// `<https://api.github.com/organizations/123/repos?per_page=100&page=2>; rel="next", <...&page=3>; rel="last"`
pub fn parse_link_header(link_header: &str) -> LinkPagination {
    let mut info = LinkPagination::default();

    for part in link_header.split(',') {
        let part = part.trim();

        // Extract URL and rel type
        let mut url = None;
        let mut rel = None;

        for segment in part.split(';') {
            let segment = segment.trim();
            if segment.starts_with('<') && segment.ends_with('>') {
                url = Some(&segment[1..segment.len() - 1]);
            } else if let Some(rel_value) = segment.strip_prefix("rel=") {
                rel = Some(rel_value.trim_matches('"'));
            }
        }

        if let (Some(url), Some(rel_type)) = (url, rel) {
            // Extract page number from URL query string
            if let Some(page_num) = extract_page_from_url(url) {
                match rel_type {
                    "last" => info.last_page = Some(page_num),
                    "next" => info.next_page = Some(page_num),
                    _ => {}
                }
            }
        }
    }

    info
}

/// Extract the page parameter from a URL.
fn extract_page_from_url(url: &str) -> Option<u32> {
    // Find the query string
    let query_start = url.find('?')?;
    let query = &url[query_start + 1..];

    // Parse query parameters
    for param in query.split('&') {
        if let Some(value) = param.strip_prefix("page=") {
            return value.parse().ok();
        }
    }

    None
}

/// Create an authenticated Octocrab instance from a GitHub token.
pub fn create_client(token: &str) -> Result<Octocrab, GitHubError> {
    Octocrab::builder()
        .personal_token(token.to_string())
        .build()
        .map_err(GitHubError::Api)
}

/// Check rate limit and return error if exceeded.
pub async fn check_rate_limit(client: &Octocrab) -> Result<(), GitHubError> {
    let rate_limit = client.ratelimit().get().await?;
    let core = &rate_limit.resources.core;

    if core.remaining == 0 {
        let reset_at = DateTime::from_timestamp(core.reset as i64, 0).unwrap_or_else(Utc::now);
        return Err(GitHubError::RateLimited { reset_at });
    }

    Ok(())
}

/// Get current rate limit status (core API only).
pub async fn get_rate_limit(client: &Octocrab) -> Result<RateLimitInfo, GitHubError> {
    let rate_limit = client.ratelimit().get().await?;
    let core = &rate_limit.resources.core;

    Ok(RateLimitInfo {
        limit: core.limit,
        remaining: core.remaining,
        reset_at: DateTime::from_timestamp(core.reset as i64, 0).unwrap_or_else(Utc::now),
        retry_after: None,
    })
}

/// Get full rate limit status for all resources.
pub async fn get_rate_limits(
    client: &Octocrab,
) -> Result<super::types::GitHubRateLimitResponse, GitHubError> {
    // Fetch raw JSON to get all fields including those octocrab may not expose
    let response: super::types::GitHubRateLimitResponse = client
        .get("/rate_limit", None::<&()>)
        .await
        .map_err(GitHubError::Api)?;
    Ok(response)
}

/// Get organization info including repo count.
pub async fn get_org_info(client: &Octocrab, org: &str) -> Result<OrgInfo, GitHubError> {
    let org_data: serde_json::Value = client
        .get(format!("/orgs/{}", org), None::<&()>)
        .await
        .map_err(GitHubError::Api)?;

    let public_repos = org_data
        .get("public_repos")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;

    let name = org_data
        .get("login")
        .and_then(|v| v.as_str())
        .unwrap_or(org)
        .to_string();

    let description = org_data
        .get("description")
        .and_then(|v| v.as_str())
        .map(String::from);

    Ok(OrgInfo {
        name,
        public_repos,
        description,
    })
}

/// GitHub API client implementing the PlatformClient trait.
///
/// This wraps an `Octocrab` instance and provides a unified interface
/// for interacting with GitHub repositories.
#[derive(Clone)]
pub struct GitHubClient {
    inner: Arc<Octocrab>,
    /// The authentication token, stored for making raw conditional requests.
    token: Arc<String>,
    /// Shared HTTP client for conditional (ETag) requests.
    http_client: reqwest::Client,
    /// The instance ID this client is configured for.
    instance_id: Uuid,
    /// Optional adaptive rate limiter for pacing API requests.
    rate_limiter: Option<AdaptiveRateLimiter>,
}

impl GitHubClient {
    /// Create a new GitHub client from an authentication token.
    pub fn new(
        token: &str,
        instance_id: Uuid,
        rate_limiter: Option<AdaptiveRateLimiter>,
    ) -> Result<Self, GitHubError> {
        let client = create_client(token)?;
        Ok(Self {
            inner: Arc::new(client),
            token: Arc::new(token.to_string()),
            http_client: reqwest::Client::new(),
            instance_id,
            rate_limiter,
        })
    }

    /// Create a GitHub client from an existing Octocrab instance.
    ///
    /// Note: This constructor doesn't have access to the token, so conditional
    /// requests with ETags won't work. Use `new()` instead for full functionality.
    pub fn from_octocrab(client: Octocrab, instance_id: Uuid) -> Self {
        Self {
            inner: Arc::new(client),
            token: Arc::new(String::new()),
            http_client: reqwest::Client::new(),
            instance_id,
            rate_limiter: None,
        }
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

    /// Extract rate limit info from GitHub response headers.
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

    /// Get a reference to the inner Octocrab client.
    pub fn inner(&self) -> &Octocrab {
        &self.inner
    }

    /// Make a GET request with optional conditional ETag.
    ///
    /// If `cached_etag` is provided, sends an `If-None-Match` header.
    /// Returns `FetchResult::NotModified` if the server returns 304.
    /// Also extracts pagination info from the Link header.
    pub async fn get_conditional<T: DeserializeOwned>(
        &self,
        route: &str,
        cached_etag: Option<&str>,
    ) -> Result<FetchResult<T>, GitHubError> {
        self.wait_for_rate_limit().await;

        // Build the request URL
        let url = format!("https://api.github.com{}", route);

        // Build the request with authentication (reuses shared connection pool)
        let mut request = self
            .http_client
            .get(&url)
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "curator")
            .header("Authorization", format!("Bearer {}", self.token.as_str()));

        // Add conditional header if we have a cached ETag
        if let Some(etag) = cached_etag {
            request = request.header(IF_NONE_MATCH, etag);
        }

        let response = request
            .send()
            .await
            .map_err(|e| GitHubError::Internal(format!("HTTP request failed: {}", e)))?;

        let status = response.status();
        let headers = response.headers().clone();
        self.update_rate_limit(&headers);

        match status {
            StatusCode::NOT_MODIFIED => Ok(FetchResult::NotModified),
            StatusCode::OK => {
                let etag = extract_etag(&headers);

                // Parse pagination from Link header and convert to shared type
                let pagination = headers
                    .get("link")
                    .and_then(|v| v.to_str().ok())
                    .map(|h| parse_link_header(h).to_pagination_info())
                    .unwrap_or_default();

                let data: T = response
                    .json()
                    .await
                    .map_err(|e| GitHubError::Internal(format!("JSON parse error: {}", e)))?;
                Ok(FetchResult::Fetched {
                    data,
                    etag,
                    pagination,
                })
            }
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(GitHubError::AuthRequired),
            StatusCode::NOT_FOUND => Err(GitHubError::OrgNotFound(route.to_string())),
            _ => Err(GitHubError::Internal(format!(
                "Unexpected HTTP status: {}",
                status
            ))),
        }
    }

    /// Get the authentication token.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// List organization repositories with ETag caching support.
    ///
    /// This method supports conditional requests using ETags to avoid
    /// refetching unchanged data. When a database connection is provided,
    /// it will:
    /// - Check for cached ETags before each page fetch
    /// - Skip fetching if the server returns 304 Not Modified
    /// - Store new ETags and pagination info after successful fetches
    /// - Continue checking all pages using stored pagination metadata
    ///
    /// # Arguments
    ///
    /// * `org` - The organization name
    /// * `db` - Optional database connection for ETag caching
    /// * `on_progress` - Optional progress callback
    ///
    /// # Returns
    ///
    /// A tuple of `(repos, cache_stats)` where `cache_stats` indicates
    /// how many pages were cache hits vs fetched.
    pub async fn list_org_repos_cached(
        &self,
        org: &str,
        db: Option<&sea_orm::DatabaseConnection>,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<(Vec<PlatformRepo>, CacheStats)> {
        use super::pagination::PaginatedFetchConfig;

        // Get org info to know total repos upfront
        let org_info = self.get_org_info(org).await.ok();
        let total_repos = org_info.as_ref().map(|i| i.public_repos);

        let config = PaginatedFetchConfig::org_repos(org, total_repos);

        let result = self
            .fetch_pages::<octocrab::models::Repository>(&config, db, on_progress)
            .await?;

        // Convert to PlatformRepo
        let repos: Vec<PlatformRepo> = result.items.iter().map(to_platform_repo).collect();

        // If all pages were cache hits and we got no repos, load from database
        if result.all_cache_hits
            && repos.is_empty()
            && let Some(models) = handle_cache_hit_fallback(
                db,
                result.stats.cache_hits,
                |db| load_repos_by_instance_and_owner(db, self.instance_id, org),
                org,
                on_progress,
            )
            .await?
        {
            let repos: Vec<PlatformRepo> = models.iter().map(PlatformRepo::from_model).collect();
            return Ok((repos, result.stats));
        }

        Ok((repos, result.stats))
    }

    /// List starred repositories with ETag caching support.
    ///
    /// This method supports conditional requests using ETags to avoid
    /// refetching unchanged data. When a database connection is provided,
    /// it will:
    /// - Check for cached ETags before each page fetch
    /// - Skip fetching if the server returns 304 Not Modified
    /// - Store new ETags and pagination info after successful fetches
    /// - Continue checking all pages using stored pagination metadata
    ///
    /// # Arguments
    ///
    /// * `username` - The username whose starred repos to fetch (used as cache key)
    /// * `db` - Optional database connection for ETag caching
    /// * `skip_rate_checks` - Skip rate limit checks if true
    /// * `on_progress` - Optional progress callback
    ///
    /// # Returns
    ///
    /// A tuple of `(repos, cache_stats)` where `cache_stats` indicates
    /// how many pages were cache hits vs fetched.
    pub async fn list_starred_repos_cached(
        &self,
        username: &str,
        db: Option<&sea_orm::DatabaseConnection>,
        skip_rate_checks: bool,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<(Vec<PlatformRepo>, CacheStats)> {
        use super::pagination::PaginatedFetchConfig;

        let config =
            PaginatedFetchConfig::starred(username).with_skip_rate_checks(skip_rate_checks);

        let result = self
            .fetch_pages::<octocrab::models::Repository>(&config, db, on_progress)
            .await?;

        // Convert to PlatformRepo
        let repos: Vec<PlatformRepo> = result.items.iter().map(to_platform_repo).collect();

        // If all pages were cache hits and we got no repos, load from database
        if result.all_cache_hits
            && repos.is_empty()
            && let Some(models) = handle_cache_hit_fallback(
                db,
                result.stats.cache_hits,
                |db| load_repos_by_instance(db, self.instance_id),
                "starred",
                on_progress,
            )
            .await?
        {
            let repos: Vec<PlatformRepo> = models.iter().map(PlatformRepo::from_model).collect();
            return Ok((repos, result.stats));
        }

        Ok((repos, result.stats))
    }

    /// List user repositories with ETag caching support.
    ///
    /// This method supports conditional requests using ETags to avoid
    /// refetching unchanged data. When a database connection is provided,
    /// it will:
    /// - Check for cached ETags before each page fetch
    /// - Skip fetching if the server returns 304 Not Modified
    /// - Store new ETags and pagination info after successful fetches
    /// - Continue checking all pages using stored pagination metadata
    ///
    /// # Arguments
    ///
    /// * `username` - The username whose repositories to fetch
    /// * `db` - Optional database connection for ETag caching
    /// * `on_progress` - Optional progress callback
    ///
    /// # Returns
    ///
    /// A tuple of `(repos, cache_stats)` where `cache_stats` indicates
    /// how many pages were cache hits vs fetched.
    pub async fn list_user_repos_cached(
        &self,
        username: &str,
        db: Option<&sea_orm::DatabaseConnection>,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<(Vec<PlatformRepo>, CacheStats)> {
        use super::pagination::PaginatedFetchConfig;

        // Get user info to know total repos upfront
        let user_info: Option<serde_json::Value> = self
            .inner
            .get(format!("/users/{}", username), None::<&()>)
            .await
            .ok();
        let total_repos = user_info
            .as_ref()
            .and_then(|u| u.get("public_repos"))
            .and_then(|v| v.as_u64())
            .map(|v| v as usize);

        let config = PaginatedFetchConfig::user_repos(username, total_repos);

        let result = self
            .fetch_pages::<octocrab::models::Repository>(&config, db, on_progress)
            .await?;

        // Convert to PlatformRepo
        let repos: Vec<PlatformRepo> = result.items.iter().map(to_platform_repo).collect();

        // If all pages were cache hits and we got no repos, load from database
        if result.all_cache_hits
            && repos.is_empty()
            && let Some(models) = handle_cache_hit_fallback(
                db,
                result.stats.cache_hits,
                |db| load_repos_by_instance_and_owner(db, self.instance_id, username),
                username,
                on_progress,
            )
            .await?
        {
            let repos: Vec<PlatformRepo> = models.iter().map(PlatformRepo::from_model).collect();
            return Ok((repos, result.stats));
        }

        Ok((repos, result.stats))
    }
}

impl From<GitHubError> for PlatformError {
    fn from(err: GitHubError) -> Self {
        match err {
            GitHubError::RateLimited { reset_at } => PlatformError::RateLimited { reset_at },
            GitHubError::AuthRequired => PlatformError::AuthRequired,
            GitHubError::OrgNotFound(org) => PlatformError::not_found(format!("org: {}", org)),
            GitHubError::Api(e) => PlatformError::api(e.to_string()),
            GitHubError::Internal(msg) => PlatformError::internal(msg),
        }
    }
}

#[async_trait]
impl PlatformClient for GitHubClient {
    fn platform_type(&self) -> PlatformType {
        PlatformType::GitHub
    }

    fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    async fn get_rate_limit(&self) -> platform::Result<RateLimitInfo> {
        get_rate_limit(&self.inner)
            .await
            .map_err(PlatformError::from)
    }

    async fn get_org_info(&self, org: &str) -> platform::Result<OrgInfo> {
        get_org_info(&self.inner, org)
            .await
            .map_err(PlatformError::from)
    }

    async fn get_authenticated_user(&self) -> platform::Result<UserInfo> {
        // GET /user
        let user: serde_json::Value = self
            .inner
            .get("/user", None::<&()>)
            .await
            .map_err(|e| PlatformError::api(e.to_string()))?;

        Ok(UserInfo {
            username: user
                .get("login")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            name: user.get("name").and_then(|v| v.as_str()).map(String::from),
            email: user.get("email").and_then(|v| v.as_str()).map(String::from),
            bio: user.get("bio").and_then(|v| v.as_str()).map(String::from),
            public_repos: user
                .get("public_repos")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize,
            followers: user.get("followers").and_then(|v| v.as_u64()).unwrap_or(0) as usize,
        })
    }

    async fn get_repo(
        &self,
        owner: &str,
        name: &str,
        db: Option<&sea_orm::DatabaseConnection>,
    ) -> platform::Result<PlatformRepo> {
        use crate::api_cache;
        use crate::entity::api_cache::{EndpointType, Model as ApiCacheModel};
        use crate::repository;

        let cache_key = ApiCacheModel::single_repo_key(owner, name);
        let cached_etag = if let Some(db) = db {
            api_cache::get_etag(db, self.instance_id, EndpointType::SingleRepo, &cache_key)
                .await
                .ok()
                .flatten()
        } else {
            None
        };

        let route = format!("/repos/{}/{}", owner, name);
        let fetch_result: FetchResult<octocrab::models::Repository> = self
            .get_conditional(&route, cached_etag.as_deref())
            .await
            .map_err(PlatformError::from)?;

        match fetch_result {
            FetchResult::NotModified => {
                if let Some(db) = db {
                    let cached = repository::find_by_natural_key(db, self.instance_id, owner, name)
                        .await
                        .map_err(|e| PlatformError::internal(e.to_string()))?;
                    if let Some(model) = cached {
                        return Ok(PlatformRepo::from_model(&model));
                    }
                }

                let refresh: FetchResult<octocrab::models::Repository> = self
                    .get_conditional(&route, None)
                    .await
                    .map_err(PlatformError::from)?;

                match refresh {
                    FetchResult::Fetched { data, etag, .. } => {
                        if let Some(db) = db
                            && let Err(e) = api_cache::upsert(
                                db,
                                self.instance_id,
                                EndpointType::SingleRepo,
                                &cache_key,
                                etag,
                            )
                            .await
                        {
                            tracing::debug!("api cache upsert failed: {e}");
                        }
                        Ok(to_platform_repo(&data))
                    }
                    FetchResult::NotModified => Err(PlatformError::internal(format!(
                        "Cache hit for {}/{} but repository not found in database",
                        owner, name
                    ))),
                }
            }
            FetchResult::Fetched { data, etag, .. } => {
                if let Some(db) = db
                    && let Err(e) = api_cache::upsert(
                        db,
                        self.instance_id,
                        EndpointType::SingleRepo,
                        &cache_key,
                        etag,
                    )
                    .await
                {
                    tracing::debug!("api cache upsert failed: {e}");
                }
                Ok(to_platform_repo(&data))
            }
        }
    }

    async fn list_org_repos(
        &self,
        org: &str,
        db: Option<&sea_orm::DatabaseConnection>,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        // Use cached version if database connection is provided
        if let Some(db) = db {
            let (repos, _stats) = self
                .list_org_repos_cached(org, Some(db), on_progress)
                .await?;
            return Ok(repos);
        }

        // Non-cached fallback
        let mut all_repos = Vec::new();
        let mut page = 1u32;

        // Get org info to know total repos upfront
        let org_info = self.get_org_info(org).await.ok();
        let total_repos = org_info.as_ref().map(|i| i.public_repos);

        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: org.to_string(),
                total_repos,
                expected_pages: total_repos.map(|t| t.div_ceil(100) as u32),
            },
        );

        loop {
            // Check rate limit before making request
            check_rate_limit(&self.inner)
                .await
                .map_err(PlatformError::from)?;

            let page_result = self
                .inner
                .orgs(org)
                .list_repos()
                .per_page(100)
                .page(page)
                .send()
                .await
                .map_err(|e| PlatformError::api(e.to_string()))?;

            let repos: Vec<_> = page_result.items;
            let count = repos.len();

            // Convert to PlatformRepo
            let platform_repos: Vec<PlatformRepo> = repos.iter().map(to_platform_repo).collect();
            all_repos.extend(platform_repos);

            emit(
                on_progress,
                SyncProgress::FetchedPage {
                    namespace: org.to_string(),
                    page,
                    count,
                    total_so_far: all_repos.len(),
                    expected_pages: None,
                },
            );

            // If we got fewer than 100, we've reached the end
            if count < 100 {
                break;
            }

            page += 1;
        }

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: org.to_string(),
                total: all_repos.len(),
            },
        );

        Ok(all_repos)
    }

    async fn list_user_repos(
        &self,
        username: &str,
        db: Option<&sea_orm::DatabaseConnection>,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        // Use cached version if database connection is provided
        if let Some(db) = db {
            let (repos, _stats) = self
                .list_user_repos_cached(username, Some(db), on_progress)
                .await?;
            return Ok(repos);
        }

        // Non-cached fallback
        let mut all_repos = Vec::new();
        let mut page = 1u32;

        // Get user info to know total repos upfront
        let user_info: Option<serde_json::Value> = self
            .inner
            .get(format!("/users/{}", username), None::<&()>)
            .await
            .ok();
        let total_repos = user_info
            .as_ref()
            .and_then(|u| u.get("public_repos"))
            .and_then(|v| v.as_u64())
            .map(|v| v as usize);

        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: username.to_string(),
                total_repos,
                expected_pages: total_repos.map(|t| t.div_ceil(100) as u32),
            },
        );

        loop {
            // Check rate limit before making request
            check_rate_limit(&self.inner)
                .await
                .map_err(PlatformError::from)?;

            // GET /users/{username}/repos - lists public repos for the user
            let route = format!("/users/{}/repos?per_page=100&page={}", username, page);
            let repos: Vec<octocrab::models::Repository> = self
                .inner
                .get(&route, None::<&()>)
                .await
                .map_err(|e| PlatformError::api(e.to_string()))?;

            let count = repos.len();

            // Convert to PlatformRepo
            let platform_repos: Vec<PlatformRepo> = repos.iter().map(to_platform_repo).collect();
            all_repos.extend(platform_repos);

            emit(
                on_progress,
                SyncProgress::FetchedPage {
                    namespace: username.to_string(),
                    page,
                    count,
                    total_so_far: all_repos.len(),
                    expected_pages: None,
                },
            );

            // If we got fewer than 100, we've reached the end
            if count < 100 {
                break;
            }

            page += 1;
        }

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: username.to_string(),
                total: all_repos.len(),
            },
        );

        Ok(all_repos)
    }

    async fn is_repo_starred(&self, owner: &str, name: &str) -> platform::Result<bool> {
        // GET /user/starred/{owner}/{repo} returns 204 if starred, 404 if not
        // We use _get instead of get because 204 returns an empty body which
        // causes JSON deserialization to fail
        let route = format!("/user/starred/{}/{}", owner, name);
        let response = self
            .inner
            ._get(&route)
            .await
            .map_err(|e| PlatformError::api(e.to_string()))?;

        match response.status().as_u16() {
            204 => Ok(true),
            404 => Ok(false),
            status => Err(PlatformError::api(format!(
                "Unexpected status {} checking starred status for {}/{}",
                status, owner, name
            ))),
        }
    }

    async fn star_repo(&self, owner: &str, name: &str) -> platform::Result<bool> {
        // Check if already starred
        if self.is_repo_starred(owner, name).await? {
            return Ok(false);
        }

        // PUT /user/starred/{owner}/{repo}
        let route = format!("/user/starred/{}/{}", owner, name);
        self.inner
            ._put(&route, None::<&()>)
            .await
            .map_err(|e| PlatformError::api(e.to_string()))?;

        Ok(true)
    }

    async fn star_repo_with_retry(
        &self,
        owner: &str,
        name: &str,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<bool> {
        // Check if already starred first (before any delays)
        match self.is_repo_starred(owner, name).await {
            Ok(true) => return Ok(false),
            Ok(false) => {}
            Err(PlatformError::RateLimited { .. }) => {
                // Will retry below
            }
            Err(e) => return Err(e),
        }

        let owner_str = owner.to_string();
        let name_str = name.to_string();
        let client = self.inner.clone();

        with_retry(
            || async {
                // PUT /user/starred/{owner}/{repo}
                let route = format!("/user/starred/{}/{}", owner_str, name_str);
                client
                    ._put(&route, None::<&()>)
                    .await
                    .map_err(GitHubError::Api)
            },
            is_rate_limit_error_from_github,
            short_error_message,
            owner,
            name,
            on_progress,
        )
        .await
        .map(|_| true)
        .map_err(PlatformError::from)
    }

    async fn unstar_repo(&self, owner: &str, name: &str) -> platform::Result<bool> {
        // Check if starred first
        if !self.is_repo_starred(owner, name).await? {
            return Ok(false);
        }

        // DELETE /user/starred/{owner}/{repo}
        let route = format!("/user/starred/{}/{}", owner, name);
        self.inner
            ._delete(&route, None::<&()>)
            .await
            .map_err(|e| PlatformError::api(e.to_string()))?;

        Ok(true)
    }

    async fn list_starred_repos(
        &self,
        db: Option<&sea_orm::DatabaseConnection>,
        concurrency: usize,
        skip_rate_checks: bool,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        // Use cached version if database connection is provided
        if let Some(db) = db {
            // Get username for cache key
            let user = self.get_authenticated_user().await?;
            let (repos, _stats) = self
                .list_starred_repos_cached(&user.username, Some(db), skip_rate_checks, on_progress)
                .await?;
            return Ok(repos);
        }

        // Non-cached fallback
        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: "starred".to_string(),
                total_repos: None,
                expected_pages: None,
            },
        );

        // Fetch first page
        if !skip_rate_checks {
            check_rate_limit(&self.inner)
                .await
                .map_err(PlatformError::from)?;
        }

        let first_page_route = "/user/starred?per_page=100&page=1";
        let first_page_repos: Vec<octocrab::models::Repository> = self
            .inner
            .get(first_page_route, None::<&()>)
            .await
            .map_err(|e| PlatformError::api(e.to_string()))?;

        let first_page_count = first_page_repos.len();
        let mut all_repos: Vec<PlatformRepo> =
            first_page_repos.iter().map(to_platform_repo).collect();

        emit(
            on_progress,
            SyncProgress::FetchedPage {
                namespace: "starred".to_string(),
                page: 1,
                count: first_page_count,
                total_so_far: all_repos.len(),
                expected_pages: None,
            },
        );

        // If first page is not full, we're done
        if first_page_count < 100 {
            emit(
                on_progress,
                SyncProgress::FetchComplete {
                    namespace: "starred".to_string(),
                    total: all_repos.len(),
                },
            );
            return Ok(all_repos);
        }

        // Fetch remaining pages concurrently using semaphore for rate control
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let client = self.inner.clone();

        let mut page = 2u32;
        let mut handles = Vec::new();

        // Spawn page fetches - semaphore limits concurrency, no artificial caps
        loop {
            let task_semaphore = Arc::clone(&semaphore);
            let task_client = client.clone();
            let current_page = page;

            let handle = tokio::spawn(async move {
                let _permit = task_semaphore.acquire().await.ok()?;

                // Check rate limit unless skipping
                if !skip_rate_checks && check_rate_limit(&task_client).await.is_err() {
                    return None;
                }

                let route = format!("/user/starred?per_page=100&page={}", current_page);
                let repos: Result<Vec<octocrab::models::Repository>, _> =
                    task_client.get(&route, None::<&()>).await;

                match repos {
                    Ok(repos) => Some((current_page, repos)),
                    Err(_) => None,
                }
            });

            handles.push(handle);
            page += 1;

            // Process in batches to detect early termination
            if handles.len() >= concurrency {
                let mut got_partial = false;
                for handle in handles.drain(..) {
                    if let Ok(Some((page_num, repos))) = handle.await {
                        let count = repos.len();
                        let platform_repos: Vec<PlatformRepo> =
                            repos.iter().map(to_platform_repo).collect();
                        all_repos.extend(platform_repos);

                        emit(
                            on_progress,
                            SyncProgress::FetchedPage {
                                namespace: "starred".to_string(),
                                page: page_num,
                                count,
                                total_so_far: all_repos.len(),
                                expected_pages: None,
                            },
                        );

                        if count < 100 {
                            got_partial = true;
                        }
                    }
                }
                if got_partial {
                    break;
                }
            }
        }

        // Collect any remaining results
        for handle in handles {
            if let Ok(Some((page_num, repos))) = handle.await {
                let count = repos.len();
                let platform_repos: Vec<PlatformRepo> =
                    repos.iter().map(to_platform_repo).collect();
                all_repos.extend(platform_repos);

                emit(
                    on_progress,
                    SyncProgress::FetchedPage {
                        namespace: "starred".to_string(),
                        page: page_num,
                        count,
                        total_so_far: all_repos.len(),
                        expected_pages: None,
                    },
                );
            }
        }

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: "starred".to_string(),
                total: all_repos.len(),
            },
        );

        Ok(all_repos)
    }

    async fn list_starred_repos_streaming(
        &self,
        repo_tx: tokio::sync::mpsc::Sender<PlatformRepo>,
        db: Option<&sea_orm::DatabaseConnection>,
        concurrency: usize,
        skip_rate_checks: bool,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<usize> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let total_sent = Arc::new(AtomicUsize::new(0));
        if let Some(db) = db {
            use crate::api_cache;
            use crate::entity::api_cache::{EndpointType, Model as ApiCacheModel};

            let username = self.get_authenticated_user().await?.username;
            let mut expected_pages =
                api_cache::get_starred_total_pages(db, self.instance_id, &username)
                    .await
                    .ok()
                    .flatten()
                    .map(|t| t as u32);
            let mut all_cache_hits = true;
            let mut cache_hits = 0u32;

            if !skip_rate_checks {
                check_rate_limit(&self.inner)
                    .await
                    .map_err(PlatformError::from)?;
            }

            let first_route = "/user/starred?per_page=100&page=1";
            let first_cache_key = ApiCacheModel::starred_key(&username, 1);
            let first_etag = api_cache::get_etag(
                db,
                self.instance_id,
                EndpointType::Starred,
                &first_cache_key,
            )
            .await
            .ok()
            .flatten();

            let first_result: FetchResult<Vec<octocrab::models::Repository>> = self
                .get_conditional(first_route, first_etag.as_deref())
                .await
                .map_err(PlatformError::from)?;

            match first_result {
                FetchResult::NotModified => {
                    cache_hits += 1;
                    // Emit FetchingRepos with cached expected_pages so fetch bar is created
                    emit(
                        on_progress,
                        SyncProgress::FetchingRepos {
                            namespace: "starred".to_string(),
                            total_repos: expected_pages.map(|p| (p * 100) as usize),
                            expected_pages,
                        },
                    );
                    emit(
                        on_progress,
                        SyncProgress::FetchedPage {
                            namespace: "starred".to_string(),
                            page: 1,
                            count: 0,
                            total_so_far: total_sent.load(Ordering::Relaxed),
                            expected_pages,
                        },
                    );
                }
                FetchResult::Fetched {
                    data: repos,
                    etag,
                    pagination,
                } => {
                    all_cache_hits = false;
                    let count = repos.len();

                    if let Some(total) = pagination.total_pages {
                        expected_pages = Some(total);
                    }

                    let total_pages_to_store = expected_pages.map(|t| t as i32);
                    if let Err(e) = api_cache::upsert_with_pagination(
                        db,
                        self.instance_id,
                        EndpointType::Starred,
                        &first_cache_key,
                        etag,
                        total_pages_to_store,
                    )
                    .await
                    {
                        tracing::debug!("api cache upsert failed: {e}");
                    }

                    // Emit FetchingRepos with expected_pages from Link header
                    emit(
                        on_progress,
                        SyncProgress::FetchingRepos {
                            namespace: "starred".to_string(),
                            total_repos: expected_pages.map(|p| (p * 100) as usize),
                            expected_pages,
                        },
                    );

                    for repo in &repos {
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
                            count,
                            total_so_far: total_sent.load(Ordering::Relaxed),
                            expected_pages,
                        },
                    );

                    if expected_pages.is_none() && count < 100 {
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

            if expected_pages.is_none() {
                if all_cache_hits && total_sent.load(Ordering::Relaxed) == 0 && cache_hits > 0 {
                    let cached_repos = load_repos_by_instance(db, self.instance_id).await?;

                    if !cached_repos.is_empty() {
                        let cached_count = cached_repos.len();

                        emit(
                            on_progress,
                            SyncProgress::CacheHit {
                                namespace: "starred".to_string(),
                                cached_count,
                            },
                        );

                        for model in &cached_repos {
                            let platform_repo = PlatformRepo::from_model(model);
                            if repo_tx.send(platform_repo).await.is_ok() {
                                total_sent.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }

                emit(
                    on_progress,
                    SyncProgress::FetchComplete {
                        namespace: "starred".to_string(),
                        total: total_sent.load(Ordering::Relaxed),
                    },
                );

                return Ok(total_sent.load(Ordering::Relaxed));
            }

            let last_page = expected_pages.unwrap_or(1);
            if last_page > 1 {
                // Pre-fetch ETags for all pages 2..=last_page
                let page_cache_keys: Vec<String> = (2..=last_page)
                    .map(|p| ApiCacheModel::starred_key(&username, p))
                    .collect();

                let etags_map = api_cache::get_etags_batch(
                    db,
                    self.instance_id,
                    EndpointType::Starred,
                    &page_cache_keys,
                )
                .await
                .unwrap_or_default();

                let semaphore = Arc::new(Semaphore::new(concurrency));
                let mut handles = Vec::new();
                let client = self.clone(); // Clone GitHubClient for spawned tasks

                // Result type: (page, count, new_etag, was_modified)
                type PageResult = (u32, usize, Option<String>, bool);

                for page in 2..=last_page {
                    let task_semaphore = Arc::clone(&semaphore);
                    let task_client = client.clone();
                    let task_repo_tx = repo_tx.clone();
                    let task_total_sent = Arc::clone(&total_sent);
                    let cache_key = ApiCacheModel::starred_key(&username, page);
                    let page_etag = etags_map.get(&cache_key).cloned().flatten();

                    let handle: tokio::task::JoinHandle<Option<PageResult>> =
                        tokio::spawn(async move {
                            let _permit = task_semaphore.acquire().await.ok()?;

                            let route = format!("/user/starred?per_page=100&page={}", page);
                            let result: Result<FetchResult<Vec<octocrab::models::Repository>>, _> =
                                task_client
                                    .get_conditional(&route, page_etag.as_deref())
                                    .await;

                            match result {
                                Ok(FetchResult::NotModified) => {
                                    // 304 - data unchanged, no repos to send
                                    Some((page, 0, None, false))
                                }
                                Ok(FetchResult::Fetched {
                                    data: repos, etag, ..
                                }) => {
                                    let count = repos.len();
                                    for repo in &repos {
                                        let platform_repo = to_platform_repo(repo);
                                        if task_repo_tx.send(platform_repo).await.is_ok() {
                                            task_total_sent.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                    Some((page, count, etag, true))
                                }
                                Err(_) => None,
                            }
                        });

                    handles.push((page, handle));
                }

                // Collect results and track which pages need ETag updates
                let mut etag_updates: Vec<(String, Option<String>)> = Vec::new();

                for (_page, handle) in handles {
                    if let Ok(Some((page_num, count, new_etag, was_modified))) = handle.await {
                        if was_modified {
                            all_cache_hits = false;
                            // Queue ETag update for this page
                            let cache_key = ApiCacheModel::starred_key(&username, page_num);
                            etag_updates.push((cache_key, new_etag));
                        } else {
                            // 304 Not Modified - this page was a cache hit
                            cache_hits += 1;
                        }

                        emit(
                            on_progress,
                            SyncProgress::FetchedPage {
                                namespace: "starred".to_string(),
                                page: page_num,
                                count,
                                total_so_far: total_sent.load(Ordering::Relaxed),
                                expected_pages,
                            },
                        );
                    }
                }

                // Batch update ETags for modified pages
                for (cache_key, etag) in etag_updates {
                    if let Err(e) = api_cache::upsert(
                        db,
                        self.instance_id,
                        EndpointType::Starred,
                        &cache_key,
                        etag,
                    )
                    .await
                    {
                        tracing::debug!("api cache upsert failed: {e}");
                    }
                }
            }

            if all_cache_hits && total_sent.load(Ordering::Relaxed) == 0 && cache_hits > 0 {
                let cached_repos = load_repos_by_instance(db, self.instance_id).await?;

                if !cached_repos.is_empty() {
                    let cached_count = cached_repos.len();

                    emit(
                        on_progress,
                        SyncProgress::CacheHit {
                            namespace: "starred".to_string(),
                            cached_count,
                        },
                    );

                    for model in &cached_repos {
                        let platform_repo = PlatformRepo::from_model(model);
                        if repo_tx.send(platform_repo).await.is_ok() {
                            total_sent.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }

            emit(
                on_progress,
                SyncProgress::FetchComplete {
                    namespace: "starred".to_string(),
                    total: total_sent.load(Ordering::Relaxed),
                },
            );

            return Ok(total_sent.load(Ordering::Relaxed));
        }

        if !skip_rate_checks {
            check_rate_limit(&self.inner)
                .await
                .map_err(PlatformError::from)?;
        }

        // Use get_conditional to get pagination info from Link header
        let first_page_route = "/user/starred?per_page=100&page=1";
        let first_page_result: FetchResult<Vec<octocrab::models::Repository>> = self
            .get_conditional(first_page_route, None)
            .await
            .map_err(PlatformError::from)?;

        // Without db, we can't get NotModified (no ETag sent), so this should always be Fetched
        let FetchResult::Fetched {
            data: first_page_repos,
            pagination,
            ..
        } = first_page_result
        else {
            // Shouldn't happen without ETag
            return Ok(0);
        };

        let expected_pages = pagination.total_pages;
        let first_page_count = first_page_repos.len();

        // Emit FetchingRepos with expected_pages from Link header
        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: "starred".to_string(),
                total_repos: expected_pages.map(|p| (p * 100) as usize),
                expected_pages,
            },
        );

        for repo in &first_page_repos {
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
                expected_pages,
            },
        );

        // If first page is not full and we don't know total pages, we're done
        if first_page_count < 100 && expected_pages.is_none() {
            emit(
                on_progress,
                SyncProgress::FetchComplete {
                    namespace: "starred".to_string(),
                    total: total_sent.load(Ordering::Relaxed),
                },
            );
            return Ok(total_sent.load(Ordering::Relaxed));
        }

        // Determine last page: use expected_pages if known, otherwise fetch until partial page
        let known_last_page = expected_pages;

        let semaphore = Arc::new(Semaphore::new(concurrency));
        let client = self.inner.clone();

        let mut page = 2u32;
        let mut handles = Vec::new();

        loop {
            // Stop if we've reached the known last page
            if let Some(last) = known_last_page
                && page > last
            {
                break;
            }

            let task_semaphore = Arc::clone(&semaphore);
            let task_client = client.clone();
            let task_repo_tx = repo_tx.clone();
            let task_total_sent = Arc::clone(&total_sent);
            let current_page = page;

            let handle = tokio::spawn(async move {
                let _permit = task_semaphore.acquire().await.ok()?;

                if !skip_rate_checks && check_rate_limit(&task_client).await.is_err() {
                    return None;
                }

                let route = format!("/user/starred?per_page=100&page={}", current_page);
                let repos: Result<Vec<octocrab::models::Repository>, _> =
                    task_client.get(&route, None::<&()>).await;

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
                    Err(_) => None,
                }
            });

            handles.push(handle);
            page += 1;

            // Process in batches to detect early termination (fallback if no Link header)
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
                                expected_pages,
                            },
                        );
                        if count < 100 {
                            got_partial = true;
                        }
                    }
                }
                if got_partial {
                    break;
                }
            }
        }

        // Collect any remaining results
        for handle in handles {
            if let Ok(Some((page_num, count))) = handle.await {
                emit(
                    on_progress,
                    SyncProgress::FetchedPage {
                        namespace: "starred".to_string(),
                        page: page_num,
                        count,
                        total_so_far: total_sent.load(Ordering::Relaxed),
                        expected_pages,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_github_error_to_platform_error() {
        let rate_limited = GitHubError::RateLimited {
            reset_at: Utc::now(),
        };
        let platform_err: PlatformError = rate_limited.into();
        assert!(matches!(platform_err, PlatformError::RateLimited { .. }));

        let auth_required = GitHubError::AuthRequired;
        let platform_err: PlatformError = auth_required.into();
        assert!(matches!(platform_err, PlatformError::AuthRequired));

        let not_found = GitHubError::OrgNotFound("test-org".to_string());
        let platform_err: PlatformError = not_found.into();
        assert!(matches!(platform_err, PlatformError::NotFound { .. }));
    }

    #[test]
    fn test_github_client_platform() {
        // We can't easily test the full client without a token,
        // but we can verify the trait implementation compiles
        fn assert_platform_client<T: PlatformClient>() {}
        assert_platform_client::<GitHubClient>();
    }

    #[test]
    fn test_parse_link_header_full() {
        // Real GitHub Link header format
        let header = r#"<https://api.github.com/organizations/5430905/repos?per_page=100&page=2>; rel="next", <https://api.github.com/organizations/5430905/repos?per_page=100&page=3>; rel="last""#;

        let info = parse_link_header(header);
        assert_eq!(info.next_page, Some(2));
        assert_eq!(info.last_page, Some(3));
        assert_eq!(info.total_pages(), Some(3));
    }

    #[test]
    fn test_parse_link_header_only_next() {
        let header =
            r#"<https://api.github.com/organizations/123/repos?per_page=100&page=2>; rel="next""#;

        let info = parse_link_header(header);
        assert_eq!(info.next_page, Some(2));
        assert_eq!(info.last_page, None);
        assert_eq!(info.total_pages(), None);
    }

    #[test]
    fn test_parse_link_header_only_last() {
        let header =
            r#"<https://api.github.com/organizations/123/repos?per_page=100&page=5>; rel="last""#;

        let info = parse_link_header(header);
        assert_eq!(info.next_page, None);
        assert_eq!(info.last_page, Some(5));
        assert_eq!(info.total_pages(), Some(5));
    }

    #[test]
    fn test_parse_link_header_empty() {
        let info = parse_link_header("");
        assert_eq!(info.next_page, None);
        assert_eq!(info.last_page, None);
    }

    #[test]
    fn test_extract_page_from_url() {
        assert_eq!(
            extract_page_from_url("https://api.github.com/repos?page=5"),
            Some(5)
        );
        assert_eq!(
            extract_page_from_url("https://api.github.com/repos?per_page=100&page=3"),
            Some(3)
        );
        assert_eq!(
            extract_page_from_url("https://api.github.com/repos?per_page=100"),
            None
        );
        assert_eq!(extract_page_from_url("https://api.github.com/repos"), None);
    }

    #[test]
    fn test_link_pagination_total_pages() {
        let info = LinkPagination {
            last_page: Some(10),
            next_page: Some(2),
        };
        assert_eq!(info.total_pages(), Some(10));

        let info = LinkPagination {
            last_page: None,
            next_page: Some(2),
        };
        assert_eq!(info.total_pages(), None);
    }

    #[test]
    fn test_link_pagination_to_pagination_info() {
        let link = LinkPagination {
            last_page: Some(5),
            next_page: Some(2),
        };
        let pagination = link.to_pagination_info();
        assert_eq!(pagination.total_pages, Some(5));
        assert_eq!(pagination.next_page, Some(2));
    }
}
