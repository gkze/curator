//! GitLab API client creation and management.
//!
//! Uses the [`HttpTransport`] trait for all HTTP I/O, with pagination,
//! rate-limit header parsing, and starred-project caching.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use sea_orm::DatabaseConnection;
use tokio::sync::Mutex;

use uuid::Uuid;

use super::convert::to_platform_repo;
use super::error::{GitLabError, is_rate_limit_error, short_error_message};
use super::types::{GitLabGroup, GitLabProject, GitLabUser};
use crate::api_cache;
use crate::entity::api_cache::{EndpointType, Model as ApiCacheModel};
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::platform_type::PlatformType;
use crate::platform::{
    self, AdaptiveRateLimiter, CacheStats, FetchResult, OrgInfo, PaginationInfo, PlatformClient,
    PlatformError, PlatformRepo, RateLimitInfo, UserInfo, handle_cache_hit_fallback,
    handle_streaming_cache_hit_fallback, load_repos_by_instance, load_repos_by_instance_and_owner,
};
use crate::retry::with_retry;
use crate::sync::{SyncProgress, emit};

use crate::http::reqwest_transport::ReqwestTransport;
use crate::http::{HttpHeaders, HttpMethod, HttpRequest, HttpTransport, header_get};

/// GitLab API client backed by the [`HttpTransport`] abstraction.
///
/// All HTTP I/O goes through the injected transport, enabling unit tests
/// via [`MockTransport`](crate::MockTransport) without a running server.
///
/// Key capabilities:
/// - Automatic pagination with ETag caching per page
/// - Rate-limit header parsing and adaptive pacing
/// - Starred-project caching for efficient `is_repo_starred` checks
#[derive(Clone)]
pub struct GitLabClient {
    transport: Arc<dyn HttpTransport>,
    /// Normalised host URL (e.g. `https://gitlab.com`).
    host: String,
    /// The authentication token (PAT or OAuth access token).
    token: Arc<String>,
    /// Starred projects cache to avoid re-fetching.
    starred_projects_cache: Arc<Mutex<Option<StarredProjectsCache>>>,
    /// The instance ID this client is configured for.
    instance_id: Uuid,
    /// Optional adaptive rate limiter for pacing API requests.
    rate_limiter: Option<AdaptiveRateLimiter>,
}

#[derive(Debug, Clone)]
struct StarredProjectsCache {
    user_id: u64,
    paths: HashSet<String>,
}

impl GitLabClient {
    /// Create a new GitLab client.
    ///
    /// Builds a real HTTP transport for GitLab API calls.
    pub async fn new(
        host: &str,
        token: &str,
        instance_id: Uuid,
        rate_limiter: Option<AdaptiveRateLimiter>,
    ) -> Result<Self, GitLabError> {
        let host_only = host
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/');

        let base_url = format!("https://{}", host_only);
        let _ = reqwest::header::HeaderValue::from_str(token)
            .map_err(|e| GitLabError::Auth(format!("Invalid token: {}", e)))?;

        let http = reqwest::Client::builder()
            .user_agent("curator")
            .build()
            .map_err(|e| GitLabError::Http(format!("Failed to build HTTP client: {}", e)))?;
        let transport = Arc::new(ReqwestTransport::new(http));

        let client =
            Self::new_with_transport(&base_url, token, instance_id, rate_limiter, transport);

        // Validate credentials with a lightweight call.
        let _ = client.get_user_info().await?;

        Ok(client)
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
            token: Arc::new(token.to_string()),
            starred_projects_cache: Arc::new(Mutex::new(None)),
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

    fn default_headers(&self) -> HttpHeaders {
        vec![
            ("PRIVATE-TOKEN".to_string(), self.token.as_str().to_string()),
            ("Accept".to_string(), "application/json".to_string()),
            ("User-Agent".to_string(), "curator".to_string()),
        ]
    }

    fn url_encode_component(input: &str) -> String {
        let mut out = String::new();
        for &b in input.as_bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char);
                }
                _ => {
                    use std::fmt::Write;
                    let _ = write!(&mut out, "%{:02X}", b);
                }
            }
        }
        out
    }

    /// Update the rate limiter with rate limit info from response headers, if available.
    fn update_rate_limit(&self, headers: &HttpHeaders) {
        if let Some(ref limiter) = self.rate_limiter
            && let Some(info) = Self::parse_rate_limit_headers(headers)
        {
            limiter.update(&info);
        }
    }

    /// Get the host URL.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Make a conditional GET request with optional `If-None-Match` header.
    ///
    /// If `cached_etag` is provided the server may return 304 Not Modified,
    /// avoiding the cost of transferring and deserializing the response body.
    async fn get_conditional<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        cached_etag: Option<&str>,
    ) -> Result<FetchResult<T>, GitLabError> {
        self.wait_for_rate_limit().await;

        let mut headers = self.default_headers();
        if let Some(etag) = cached_etag {
            headers.push(("If-None-Match".to_string(), etag.to_string()));
        }

        let request = HttpRequest {
            method: HttpMethod::Get,
            url: url.to_string(),
            headers,
            body: Vec::new(),
        };

        let response = self
            .transport
            .send(request)
            .await
            .map_err(|e| GitLabError::Http(format!("HTTP request failed: {}", e)))?;

        self.update_rate_limit(&response.headers);

        match response.status {
            304 => Ok(FetchResult::NotModified),
            status if (200..=299).contains(&status) => {
                let etag = header_get(&response.headers, "etag").map(|s| s.to_string());
                let total_pages: Option<i32> = header_get(&response.headers, "x-total-pages")
                    .and_then(|v| v.parse::<i32>().ok());

                let data: T = serde_json::from_slice(&response.body)
                    .map_err(|e| GitLabError::Deserialize(format!("JSON parse error: {}", e)))?;

                Ok(FetchResult::Fetched {
                    data,
                    etag,
                    pagination: PaginationInfo::from_total_pages(total_pages),
                })
            }
            401 | 403 => Err(GitLabError::Auth("Authentication failed".to_string())),
            404 => Err(GitLabError::Api(format!("Not found: {}", url))),
            status => {
                let body = String::from_utf8_lossy(&response.body).to_string();
                Err(GitLabError::from_status_code(status, &body))
            }
        }
    }

    /// Extract rate limit info from response headers.
    fn parse_rate_limit_headers(headers: &HttpHeaders) -> Option<RateLimitInfo> {
        let limit = header_get(headers, "ratelimit-limit")?
            .parse::<usize>()
            .ok()?;
        let remaining = header_get(headers, "ratelimit-remaining")?
            .parse::<usize>()
            .ok()?;
        let reset_epoch = header_get(headers, "ratelimit-reset")?
            .parse::<i64>()
            .ok()?;

        let reset_at = chrono::DateTime::from_timestamp(reset_epoch, 0)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|| Utc::now() + chrono::Duration::minutes(1));

        Some(RateLimitInfo {
            limit,
            remaining,
            reset_at,
            retry_after: None,
        })
    }

    // ------------------------------------------------------------------
    // Paginated helpers — call generated methods in a loop
    // ------------------------------------------------------------------

    /// Generic paginated fetch with ETag caching.
    ///
    /// Fetches all pages of a paginated endpoint, using cached ETags when
    /// available. Returns the collected items and cache statistics.
    ///
    /// # Type Parameters
    /// - `T`: The item type to deserialize (must be `DeserializeOwned`)
    ///
    /// # Parameters
    /// - `endpoint_type`: The endpoint type for cache key lookups
    /// - `cache_key_prefix`: The prefix used to load stored total_pages from DB
    /// - `url_fn`: Builds the URL for a given page number
    /// - `cache_key_fn`: Builds the cache key for a given page number
    /// - `db`: Optional database connection for ETag caching
    async fn paginated_fetch<T: serde::de::DeserializeOwned>(
        &self,
        endpoint_type: EndpointType,
        cache_key_prefix: &str,
        url_fn: impl Fn(u32) -> String,
        cache_key_fn: impl Fn(u32) -> String,
        db: Option<&DatabaseConnection>,
    ) -> Result<(Vec<T>, CacheStats), GitLabError> {
        let mut all: Vec<T> = Vec::new();
        let mut page = 1u32;
        let mut stats = CacheStats::default();
        let mut known_total_pages: Option<u32> = None;

        // Try to load stored total_pages from the DB
        if let Some(db) = db
            && let Ok(Some(stored)) =
                api_cache::get_total_pages(db, self.instance_id, endpoint_type, cache_key_prefix)
                    .await
        {
            known_total_pages = Some(stored as u32);
        }

        loop {
            let url = url_fn(page);
            let cache_key = cache_key_fn(page);

            // Look up cached ETag
            let cached_etag = if let Some(db) = db {
                api_cache::get_etag(db, self.instance_id, endpoint_type, &cache_key)
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            };

            let result: FetchResult<Vec<T>> =
                self.get_conditional(&url, cached_etag.as_deref()).await?;

            match result {
                FetchResult::NotModified => {
                    stats.record_hit();
                    if let Some(total) = known_total_pages
                        && page < total
                    {
                        page += 1;
                        continue;
                    }
                    break;
                }
                FetchResult::Fetched {
                    data: items,
                    etag,
                    pagination,
                } => {
                    stats.record_fetch();
                    let is_empty = items.is_empty();

                    if let Some(tp) = pagination.total_pages {
                        known_total_pages = Some(tp);
                    }

                    // Store ETag
                    if let Some(db) = db {
                        let tp_to_store = if page == 1 {
                            known_total_pages.map(|t| t as i32)
                        } else {
                            None
                        };
                        if let Err(e) = api_cache::upsert_with_pagination(
                            db,
                            self.instance_id,
                            endpoint_type,
                            &cache_key,
                            etag,
                            tp_to_store,
                        )
                        .await
                        {
                            tracing::debug!("api cache upsert failed: {e}");
                        }
                    }

                    all.extend(items);

                    if is_empty {
                        break;
                    }
                    if let Some(total) = known_total_pages
                        && page >= total
                    {
                        break;
                    }
                    page += 1;
                }
            }
        }

        Ok((all, stats))
    }

    /// List all projects for a group with automatic pagination and optional
    /// ETag caching.
    ///
    /// When a `db` is provided, each page request checks for a cached ETag and
    /// sends an `If-None-Match` header.  Pages that return 304 are skipped.
    pub async fn list_group_projects(
        &self,
        group: &str,
        include_subgroups: bool,
        db: Option<&DatabaseConnection>,
    ) -> Result<(Vec<GitLabProject>, CacheStats), GitLabError> {
        let include_sub = if include_subgroups { "true" } else { "false" };
        let encoded_group = group.replace('/', "%2F");
        let host = self.host.clone();

        self.paginated_fetch(
            EndpointType::OrgRepos,
            group,
            |page| {
                format!(
                    "{}/api/v4/groups/{}/projects?include_subgroups={}&per_page=100&page={}",
                    host, encoded_group, include_sub, page,
                )
            },
            |page| ApiCacheModel::org_repos_key(group, page),
            db,
        )
        .await
    }

    /// Get information about the authenticated user.
    pub async fn get_user_info(&self) -> Result<GitLabUser, GitLabError> {
        self.wait_for_rate_limit().await;

        let url = format!("{}/api/v4/user", self.host);
        let request = HttpRequest {
            method: HttpMethod::Get,
            url,
            headers: self.default_headers(),
            body: Vec::new(),
        };

        let response = self
            .transport
            .send(request)
            .await
            .map_err(|e| GitLabError::Http(format!("HTTP request failed: {}", e)))?;

        self.update_rate_limit(&response.headers);

        if !(200..=299).contains(&response.status) {
            let body = String::from_utf8_lossy(&response.body).to_string();
            return Err(GitLabError::from_status_code(response.status, &body));
        }

        serde_json::from_slice::<GitLabUser>(&response.body)
            .map_err(|e| GitLabError::Deserialize(format!("JSON parse error: {}", e)))
    }

    /// List all projects for a specific user with automatic pagination and
    /// optional ETag caching.
    pub async fn list_user_projects(
        &self,
        username: &str,
        db: Option<&DatabaseConnection>,
    ) -> Result<(Vec<GitLabProject>, CacheStats), GitLabError> {
        let user_id = self.resolve_user_id(username).await?;
        let host = self.host.clone();

        self.paginated_fetch(
            EndpointType::UserRepos,
            username,
            |page| {
                format!(
                    "{}/api/v4/users/{}/projects?per_page=100&page={}",
                    host, user_id, page,
                )
            },
            |page| ApiCacheModel::user_repos_key(username, page),
            db,
        )
        .await
    }

    /// Resolve a username to a user ID.
    ///
    /// The trimmed OpenAPI spec doesn't include `/users?username=X`, so we
    /// fall back to the inner reqwest client for this one lookup.
    async fn resolve_user_id(&self, username: &str) -> Result<u64, GitLabError> {
        self.wait_for_rate_limit().await;

        let encoded_username = Self::url_encode_component(username);
        let url = format!("{}/api/v4/users?username={}", self.host, encoded_username);

        let request = HttpRequest {
            method: HttpMethod::Get,
            url,
            headers: self.default_headers(),
            body: Vec::new(),
        };
        let response = self
            .transport
            .send(request)
            .await
            .map_err(|e| GitLabError::Http(format!("HTTP request failed: {}", e)))?;

        self.update_rate_limit(&response.headers);

        if !(200..=299).contains(&response.status) {
            let body = String::from_utf8_lossy(&response.body).to_string();
            return Err(GitLabError::from_status_code(response.status, &body));
        }

        let users: Vec<serde_json::Value> = serde_json::from_slice(&response.body)
            .map_err(|e| GitLabError::Deserialize(format!("Failed to parse users list: {}", e)))?;

        users
            .first()
            .and_then(|u| u.get("id"))
            .and_then(|id| id.as_u64())
            .ok_or_else(|| GitLabError::Api(format!("User not found: {}", username)))
    }

    /// Star a project by ID.
    ///
    /// Returns `Ok(true)` if the project was starred, `Ok(false)` if already starred.
    pub async fn star_project(&self, project_id: u64) -> Result<bool, GitLabError> {
        self.wait_for_rate_limit().await;

        let url = format!("{}/api/v4/projects/{}/star", self.host, project_id);
        let request = HttpRequest {
            method: HttpMethod::Post,
            url,
            headers: self.default_headers(),
            body: Vec::new(),
        };

        let response = self
            .transport
            .send(request)
            .await
            .map_err(|e| GitLabError::Http(format!("HTTP request failed: {}", e)))?;

        self.update_rate_limit(&response.headers);

        match response.status {
            304 => Ok(false),
            status if (200..=299).contains(&status) => Ok(true),
            status => {
                let body = String::from_utf8_lossy(&response.body).to_string();
                Err(GitLabError::from_status_code(status, &body))
            }
        }
    }

    /// Unstar a project by ID.
    ///
    /// Returns `Ok(true)` if the project was unstarred, `Ok(false)` if wasn't starred.
    pub async fn unstar_project(&self, project_id: u64) -> Result<bool, GitLabError> {
        self.wait_for_rate_limit().await;

        let url = format!("{}/api/v4/projects/{}/unstar", self.host, project_id);
        let request = HttpRequest {
            method: HttpMethod::Post,
            url,
            headers: self.default_headers(),
            body: Vec::new(),
        };

        let response = self
            .transport
            .send(request)
            .await
            .map_err(|e| GitLabError::Http(format!("HTTP request failed: {}", e)))?;

        self.update_rate_limit(&response.headers);

        match response.status {
            304 => Ok(false),
            status if (200..=299).contains(&status) => Ok(true),
            status => {
                let body = String::from_utf8_lossy(&response.body).to_string();
                Err(GitLabError::from_status_code(status, &body))
            }
        }
    }

    /// List all projects starred by a user with automatic pagination and
    /// optional ETag caching.
    pub async fn list_starred_projects(
        &self,
        user_id: u64,
        username: &str,
        db: Option<&DatabaseConnection>,
    ) -> Result<(Vec<GitLabProject>, CacheStats), GitLabError> {
        let host = self.host.clone();
        // Cache key prefix for starred uses "{username}/starred" format
        // so get_total_pages looks for "{username}/starred/page/1"
        let cache_key_prefix = format!("{}/starred", username);

        self.paginated_fetch(
            EndpointType::Starred,
            &cache_key_prefix,
            |page| {
                format!(
                    "{}/api/v4/users/{}/starred_projects?per_page=100&page={}",
                    host, user_id, page,
                )
            },
            |page| ApiCacheModel::starred_key(username, page),
            db,
        )
        .await
    }

    /// Get information about a group.
    pub async fn get_group_info(&self, group: &str) -> Result<OrgInfo, GitLabError> {
        self.wait_for_rate_limit().await;

        let encoded_group = group.replace('/', "%2F");
        let url = format!("{}/api/v4/groups/{}", self.host, encoded_group);
        let request = HttpRequest {
            method: HttpMethod::Get,
            url,
            headers: self.default_headers(),
            body: Vec::new(),
        };

        let response = self
            .transport
            .send(request)
            .await
            .map_err(|e| GitLabError::Http(format!("HTTP request failed: {}", e)))?;
        self.update_rate_limit(&response.headers);

        match response.status {
            404 => Err(GitLabError::GroupNotFound(group.to_string())),
            status if (200..=299).contains(&status) => {
                let detail: GitLabGroup = serde_json::from_slice(&response.body)
                    .map_err(|e| GitLabError::Deserialize(format!("JSON parse error: {}", e)))?;

                let repo_count = self.get_group_project_count(&encoded_group).await;

                Ok(OrgInfo {
                    name: detail.name,
                    public_repos: repo_count,
                    description: detail.description,
                })
            }
            status => {
                let body = String::from_utf8_lossy(&response.body).to_string();
                Err(GitLabError::from_status_code(status, &body))
            }
        }
    }

    /// Get the total project count for a group via a lightweight API request.
    ///
    /// Makes a `per_page=1` request to the group projects endpoint and reads
    /// the `x-total` header to avoid fetching all projects.
    async fn get_group_project_count(&self, encoded_group: &str) -> usize {
        self.wait_for_rate_limit().await;

        let url = format!(
            "{}/api/v4/groups/{}/projects?per_page=1&page=1&include_subgroups=false",
            self.host, encoded_group,
        );

        let request = HttpRequest {
            method: HttpMethod::Get,
            url,
            headers: self.default_headers(),
            body: Vec::new(),
        };

        let Ok(response) = self.transport.send(request).await else {
            return 0;
        };

        self.update_rate_limit(&response.headers);

        if !(200..=299).contains(&response.status) {
            return 0;
        }

        header_get(&response.headers, "x-total")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0)
    }

    /// Look up a project by its full path (owner/name).
    async fn get_project_by_path(&self, full_path: &str) -> Result<GitLabProject, GitLabError> {
        self.wait_for_rate_limit().await;

        let encoded = full_path.replace('/', "%2F");
        let url = format!("{}/api/v4/projects/{}", self.host, encoded);
        let request = HttpRequest {
            method: HttpMethod::Get,
            url,
            headers: self.default_headers(),
            body: Vec::new(),
        };

        let response = self
            .transport
            .send(request)
            .await
            .map_err(|e| GitLabError::Http(format!("HTTP request failed: {}", e)))?;
        self.update_rate_limit(&response.headers);

        match response.status {
            404 => Err(GitLabError::ProjectNotFound(full_path.to_string())),
            status if (200..=299).contains(&status) => {
                serde_json::from_slice::<GitLabProject>(&response.body)
                    .map_err(|e| GitLabError::Deserialize(format!("JSON parse error: {}", e)))
            }
            status => {
                let body = String::from_utf8_lossy(&response.body).to_string();
                Err(GitLabError::from_status_code(status, &body))
            }
        }
    }

    // ------------------------------------------------------------------
    // Starred-project cache helpers
    // ------------------------------------------------------------------

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

    /// Get rate limit information from the API.
    ///
    /// Makes a lightweight request to extract rate limit headers. Falls back to
    /// hardcoded defaults if headers aren't present.
    pub async fn get_rate_limit(&self) -> RateLimitInfo {
        let url = format!("{}/api/v4/user", self.host);
        let request = HttpRequest {
            method: HttpMethod::Get,
            url,
            headers: self.default_headers(),
            body: Vec::new(),
        };

        if let Ok(resp) = self.transport.send(request).await
            && let Some(info) = Self::parse_rate_limit_headers(&resp.headers)
        {
            return info;
        }

        // Fallback: GitLab.com defaults (2000 req/min for authenticated users)
        RateLimitInfo {
            limit: 2000,
            remaining: 2000,
            reset_at: Utc::now() + chrono::Duration::minutes(1),
            retry_after: None,
        }
    }
}

// ---------------------------------------------------------------------------
// PlatformClient trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl PlatformClient for GitLabClient {
    fn platform_type(&self) -> PlatformType {
        PlatformType::GitLab
    }

    fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    async fn get_rate_limit(&self) -> platform::Result<RateLimitInfo> {
        Ok(GitLabClient::get_rate_limit(self).await)
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
            public_repos: 0,
            followers: user.followers,
        })
    }

    async fn get_repo(
        &self,
        owner: &str,
        name: &str,
        _db: Option<&sea_orm::DatabaseConnection>,
    ) -> platform::Result<PlatformRepo> {
        let full_path = format!("{}/{}", owner, name);
        let project = self
            .get_project_by_path(&full_path)
            .await
            .map_err(PlatformError::from)?;

        Ok(to_platform_repo(&project))
    }

    async fn list_org_repos(
        &self,
        org: &str,
        db: Option<&sea_orm::DatabaseConnection>,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: org.to_string(),
                total_repos: None,
                expected_pages: None,
            },
        );

        let (projects, stats) = self.list_group_projects(org, true, db).await?;

        // Cache-hit fallback: if every page was 304, load from the local DB.
        if stats.all_cached()
            && projects.is_empty()
            && let Some(models) = handle_cache_hit_fallback(
                db,
                stats.cache_hits,
                |db| load_repos_by_instance_and_owner(db, self.instance_id, org),
                org,
                on_progress,
            )
            .await?
        {
            return Ok(models.iter().map(PlatformRepo::from_model).collect());
        }

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: org.to_string(),
                total: projects.len(),
            },
        );

        let repos: Vec<PlatformRepo> = projects.iter().map(to_platform_repo).collect();
        Ok(repos)
    }

    async fn list_user_repos(
        &self,
        username: &str,
        db: Option<&sea_orm::DatabaseConnection>,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: username.to_string(),
                total_repos: None,
                expected_pages: None,
            },
        );

        let (projects, stats) = self.list_user_projects(username, db).await?;

        // Cache-hit fallback
        if stats.all_cached()
            && projects.is_empty()
            && let Some(models) = handle_cache_hit_fallback(
                db,
                stats.cache_hits,
                |db| load_repos_by_instance_and_owner(db, self.instance_id, username),
                username,
                on_progress,
            )
            .await?
        {
            return Ok(models.iter().map(PlatformRepo::from_model).collect());
        }

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: username.to_string(),
                total: projects.len(),
            },
        );

        let repos: Vec<PlatformRepo> = projects.iter().map(to_platform_repo).collect();
        Ok(repos)
    }

    async fn is_repo_starred(&self, owner: &str, name: &str) -> platform::Result<bool> {
        let full_path = format!("{}/{}", owner, name);
        let user = self.get_user_info().await?;

        if let Some(cached) = self.cached_starred_status(user.id, &full_path).await {
            return Ok(cached);
        }

        // No db available here, pass None for ETag caching
        let (projects, _stats) = self
            .list_starred_projects(user.id, &user.username, None)
            .await?;
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
        let full_path = format!("{}/{}", owner, name);

        if self.is_repo_starred(owner, name).await? {
            return Ok(false);
        }

        let project: GitLabProject = self
            .get_project_by_path(&full_path)
            .await
            .map_err(PlatformError::from)?;

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

        match self.is_repo_starred(owner, name).await {
            Ok(true) => return Ok(false),
            Ok(false) => {}
            Err(PlatformError::RateLimited { .. }) => {}
            Err(error) => return Err(error),
        }

        let project: GitLabProject = self
            .get_project_by_path(&full_path)
            .await
            .map_err(PlatformError::from)?;

        let project_id = project.id;
        let client = self.clone();

        let result = with_retry(
            || async { client.star_project(project_id).await },
            is_rate_limit_error,
            short_error_message,
            &full_path,
            "",
            on_progress,
        )
        .await
        .map_err(PlatformError::from)?;

        self.clear_starred_cache().await;
        Ok(result)
    }

    async fn unstar_repo(&self, owner: &str, name: &str) -> platform::Result<bool> {
        let full_path = format!("{}/{}", owner, name);
        let project: GitLabProject = self
            .get_project_by_path(&full_path)
            .await
            .map_err(PlatformError::from)?;

        let result = self
            .unstar_project(project.id)
            .await
            .map_err(PlatformError::from)?;
        self.clear_starred_cache().await;
        Ok(result)
    }

    async fn list_starred_repos(
        &self,
        db: Option<&sea_orm::DatabaseConnection>,
        concurrency: usize,
        _skip_rate_checks: bool,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<Vec<PlatformRepo>> {
        use tokio::sync::Semaphore;

        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: "starred".to_string(),
                total_repos: None,
                expected_pages: None,
            },
        );

        let user = self.get_user_info().await?;
        let host = self.host.clone();
        let username = user.username.clone();
        let cache_key_prefix = format!("{}/starred", &username);

        // Try to load known total_pages from DB
        let mut known_total_pages: Option<u32> = None;
        if let Some(db) = db
            && let Ok(Some(stored)) = api_cache::get_total_pages(
                db,
                self.instance_id,
                EndpointType::Starred,
                &cache_key_prefix,
            )
            .await
        {
            known_total_pages = Some(stored as u32);
        }

        // ── Page 1 (sequential) ─────────────────────────────────────
        let url = format!(
            "{}/api/v4/users/{}/starred_projects?per_page=100&page=1",
            host, user.id,
        );
        let cache_key = ApiCacheModel::starred_key(&username, 1);
        let cached_etag = if let Some(db) = db {
            api_cache::get_etag(db, self.instance_id, EndpointType::Starred, &cache_key)
                .await
                .ok()
                .flatten()
        } else {
            None
        };

        let first_result: FetchResult<Vec<GitLabProject>> =
            self.get_conditional(&url, cached_etag.as_deref()).await?;

        let mut all_projects: Vec<GitLabProject> = Vec::new();
        let mut all_cache_hits;

        match first_result {
            FetchResult::NotModified => {
                all_cache_hits = true;
                if known_total_pages.is_none() || known_total_pages == Some(1) {
                    // All cached, fall back to DB
                    if let Some(models) = handle_cache_hit_fallback(
                        db,
                        1,
                        |db| load_repos_by_instance(db, self.instance_id),
                        "starred",
                        on_progress,
                    )
                    .await?
                    {
                        let repos: Vec<PlatformRepo> =
                            models.iter().map(PlatformRepo::from_model).collect();
                        return Ok(repos);
                    }
                }
            }
            FetchResult::Fetched {
                data: items,
                etag,
                pagination,
            } => {
                all_cache_hits = false;
                if let Some(tp) = pagination.total_pages {
                    known_total_pages = Some(tp);
                }

                // Store ETag + pagination for page 1
                if let Some(db) = db
                    && let Err(e) = api_cache::upsert_with_pagination(
                        db,
                        self.instance_id,
                        EndpointType::Starred,
                        &cache_key,
                        etag,
                        known_total_pages.map(|t| t as i32),
                    )
                    .await
                {
                    tracing::debug!("api cache upsert failed: {e}");
                }

                emit(
                    on_progress,
                    SyncProgress::FetchedPage {
                        namespace: "starred".to_string(),
                        page: 1,
                        count: items.len(),
                        total_so_far: items.len(),
                        expected_pages: known_total_pages,
                    },
                );

                all_projects.extend(items);
            }
        }

        // ── Remaining pages (concurrent) ────────────────────────────
        let total_pages = known_total_pages.unwrap_or(1);
        if total_pages > 1 {
            let semaphore = Arc::new(Semaphore::new(concurrency));
            let mut handles = Vec::new();
            let user_id = user.id;

            for page in 2..=total_pages {
                let client = self.clone();
                let task_semaphore = Arc::clone(&semaphore);
                let task_host = host.clone();

                let task_cached_etag = if let Some(db) = db {
                    let ck = ApiCacheModel::starred_key(&username, page);
                    api_cache::get_etag(db, self.instance_id, EndpointType::Starred, &ck)
                        .await
                        .ok()
                        .flatten()
                } else {
                    None
                };

                let handle = tokio::spawn(async move {
                    let _permit = task_semaphore.acquire().await.ok()?;

                    let url = format!(
                        "{}/api/v4/users/{}/starred_projects?per_page=100&page={}",
                        task_host, user_id, page,
                    );

                    let result: Result<FetchResult<Vec<GitLabProject>>, GitLabError> = client
                        .get_conditional(&url, task_cached_etag.as_deref())
                        .await;

                    match result {
                        Ok(FetchResult::NotModified) => {
                            Some((page, Vec::<GitLabProject>::new(), true, None))
                        }
                        Ok(FetchResult::Fetched {
                            data: items, etag, ..
                        }) => Some((page, items, false, etag)),
                        Err(e) => {
                            tracing::warn!(
                                "Failed to fetch starred repos page {}: {}",
                                page,
                                short_error_message(&e),
                            );
                            None
                        }
                    }
                });

                handles.push(handle);
            }

            for handle in handles {
                if let Ok(Some((page_num, items, was_cache_hit, etag))) = handle.await {
                    if !was_cache_hit {
                        all_cache_hits = false;
                    }

                    if let Some(db) = db
                        && !was_cache_hit
                    {
                        let ck = ApiCacheModel::starred_key(&username, page_num);
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

                    emit(
                        on_progress,
                        SyncProgress::FetchedPage {
                            namespace: "starred".to_string(),
                            page: page_num,
                            count: items.len(),
                            total_so_far: all_projects.len() + items.len(),
                            expected_pages: known_total_pages,
                        },
                    );

                    all_projects.extend(items);
                }
            }
        }

        // ── Cache-hit fallback ──────────────────────────────────────
        if all_cache_hits
            && all_projects.is_empty()
            && let Some(models) = handle_cache_hit_fallback(
                db,
                1,
                |db| load_repos_by_instance(db, self.instance_id),
                "starred",
                on_progress,
            )
            .await?
        {
            let repos: Vec<PlatformRepo> = models.iter().map(PlatformRepo::from_model).collect();
            return Ok(repos);
        }

        self.update_starred_cache(user.id, &all_projects).await;

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: "starred".to_string(),
                total: all_projects.len(),
            },
        );

        let repos: Vec<PlatformRepo> = all_projects.iter().map(to_platform_repo).collect();
        Ok(repos)
    }

    async fn list_starred_repos_streaming(
        &self,
        repo_tx: tokio::sync::mpsc::Sender<PlatformRepo>,
        db: Option<&sea_orm::DatabaseConnection>,
        concurrency: usize,
        _skip_rate_checks: bool,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<usize> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::Semaphore;

        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: "starred".to_string(),
                total_repos: None,
                expected_pages: None,
            },
        );

        let user = self.get_user_info().await?;
        let host = self.host.clone();
        let username = user.username.clone();
        let cache_key_prefix = format!("{}/starred", &username);

        // Try to load known total_pages from DB for cache-hit continuation
        let mut known_total_pages: Option<u32> = None;
        if let Some(db) = db
            && let Ok(Some(stored)) = api_cache::get_total_pages(
                db,
                self.instance_id,
                EndpointType::Starred,
                &cache_key_prefix,
            )
            .await
        {
            known_total_pages = Some(stored as u32);
        }

        // ── Page 1 (sequential) ─────────────────────────────────────
        let url = format!(
            "{}/api/v4/users/{}/starred_projects?per_page=100&page=1",
            host, user.id,
        );
        let cache_key = ApiCacheModel::starred_key(&username, 1);
        let cached_etag = if let Some(db) = db {
            api_cache::get_etag(db, self.instance_id, EndpointType::Starred, &cache_key)
                .await
                .ok()
                .flatten()
        } else {
            None
        };

        let first_result: FetchResult<Vec<GitLabProject>> =
            self.get_conditional(&url, cached_etag.as_deref()).await?;

        let total_sent = Arc::new(AtomicUsize::new(0));
        let mut all_projects: Vec<GitLabProject> = Vec::new();
        let mut all_cache_hits;

        match first_result {
            FetchResult::NotModified => {
                all_cache_hits = true;
                // If we don't know total_pages, we can't continue paginating
                if known_total_pages.is_none() || known_total_pages == Some(1) {
                    let instance_id = self.instance_id;
                    let sent = handle_streaming_cache_hit_fallback(
                        db,
                        1, // one cache hit (this page)
                        |db| load_repos_by_instance(db, instance_id),
                        "starred",
                        &repo_tx,
                        &total_sent,
                        on_progress,
                    )
                    .await?;

                    if sent > 0 {
                        let total = total_sent.load(Ordering::Relaxed);
                        emit(
                            on_progress,
                            SyncProgress::FetchComplete {
                                namespace: "starred".to_string(),
                                total,
                            },
                        );
                        return Ok(total);
                    }
                }
            }
            FetchResult::Fetched {
                data: items,
                etag,
                pagination,
            } => {
                all_cache_hits = false;
                if let Some(tp) = pagination.total_pages {
                    known_total_pages = Some(tp);
                }

                // Store ETag + pagination for page 1
                if let Some(db) = db
                    && let Err(e) = api_cache::upsert_with_pagination(
                        db,
                        self.instance_id,
                        EndpointType::Starred,
                        &cache_key,
                        etag,
                        known_total_pages.map(|t| t as i32),
                    )
                    .await
                {
                    tracing::debug!("api cache upsert failed: {e}");
                }

                let page_count = items.len();

                // Stream page 1 repos immediately
                for project in &items {
                    let platform_repo = to_platform_repo(project);
                    if repo_tx.send(platform_repo).await.is_ok() {
                        total_sent.fetch_add(1, Ordering::Relaxed);
                    }
                }

                emit(
                    on_progress,
                    SyncProgress::FetchedPage {
                        namespace: "starred".to_string(),
                        page: 1,
                        count: page_count,
                        total_so_far: total_sent.load(Ordering::Relaxed),
                        expected_pages: known_total_pages,
                    },
                );

                all_projects.extend(items);
            }
        }

        // ── Remaining pages (concurrent) ────────────────────────────
        let total_pages = known_total_pages.unwrap_or(1);
        if total_pages > 1 {
            let semaphore = Arc::new(Semaphore::new(concurrency));
            let mut handles = Vec::new();

            let user_id = user.id;

            for page in 2..=total_pages {
                let client = self.clone();
                let task_repo_tx = repo_tx.clone();
                let task_total_sent = Arc::clone(&total_sent);
                let task_semaphore = Arc::clone(&semaphore);
                let task_host = host.clone();

                // Pre-fetch ETag outside the spawn so we can borrow db
                let task_cached_etag = if let Some(db) = db {
                    let ck = ApiCacheModel::starred_key(&username, page);
                    api_cache::get_etag(db, self.instance_id, EndpointType::Starred, &ck)
                        .await
                        .ok()
                        .flatten()
                } else {
                    None
                };

                let handle = tokio::spawn(async move {
                    let _permit = task_semaphore.acquire().await.ok()?;

                    let url = format!(
                        "{}/api/v4/users/{}/starred_projects?per_page=100&page={}",
                        task_host, user_id, page,
                    );

                    let result: Result<FetchResult<Vec<GitLabProject>>, GitLabError> = client
                        .get_conditional(&url, task_cached_etag.as_deref())
                        .await;

                    match result {
                        Ok(FetchResult::NotModified) => {
                            // Cache hit — no items to stream
                            Some((page, Vec::<GitLabProject>::new(), true, None))
                        }
                        Ok(FetchResult::Fetched {
                            data: items, etag, ..
                        }) => {
                            for project in &items {
                                let platform_repo = to_platform_repo(project);
                                if task_repo_tx.send(platform_repo).await.is_ok() {
                                    task_total_sent.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Some((page, items, false, etag))
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to fetch starred repos page {}: {}",
                                page,
                                short_error_message(&e),
                            );
                            None
                        }
                    }
                });

                handles.push(handle);
            }

            // Collect results from all spawned tasks
            for handle in handles {
                if let Ok(Some((page_num, items, was_cache_hit, etag))) = handle.await {
                    if !was_cache_hit {
                        all_cache_hits = false;
                    }

                    // Store ETag for fetched pages
                    if let Some(db) = db
                        && !was_cache_hit
                    {
                        let ck = ApiCacheModel::starred_key(&username, page_num);
                        if let Err(e) = api_cache::upsert_with_pagination(
                            db,
                            self.instance_id,
                            EndpointType::Starred,
                            &ck,
                            etag,
                            None, // total_pages already stored on page 1
                        )
                        .await
                        {
                            tracing::debug!("api cache upsert failed: {e}");
                        }
                    }

                    emit(
                        on_progress,
                        SyncProgress::FetchedPage {
                            namespace: "starred".to_string(),
                            page: page_num,
                            count: items.len(),
                            total_so_far: total_sent.load(Ordering::Relaxed),
                            expected_pages: known_total_pages,
                        },
                    );

                    all_projects.extend(items);
                }
            }
        }

        // ── Cache-hit fallback ──────────────────────────────────────
        // If every page returned 304 and we got no projects, load from DB
        if all_cache_hits && all_projects.is_empty() {
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

        // Update starred cache with all projects we collected
        self.update_starred_cache(user.id, &all_projects).await;

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

/// Create a GitLab client (convenience function).
pub async fn create_client(
    host: &str,
    token: &str,
    instance_id: Uuid,
) -> Result<GitLabClient, GitLabError> {
    GitLabClient::new(host, token, instance_id, None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use chrono::Duration;
    #[cfg(feature = "migrate")]
    use sea_orm::ActiveValue::Set;
    #[cfg(feature = "migrate")]
    use sea_orm::EntityTrait;

    use crate::{HttpHeaders, HttpMethod, HttpRequest, HttpResponse, HttpTransport, MockTransport};

    const TEST_BASE_URL: &str = "http://local.test";

    fn push_response(
        transport: &MockTransport,
        method: HttpMethod,
        url: &str,
        status: u16,
        headers: HttpHeaders,
        body: &str,
    ) {
        transport.push_response(
            method,
            url.to_string(),
            HttpResponse {
                status,
                headers,
                body: body.as_bytes().to_vec(),
            },
        );
    }

    fn request_header(req: &HttpRequest, name: &str) -> Option<String> {
        crate::header_get(&req.headers, name).map(|v| v.to_string())
    }

    fn project_json(path_with_namespace: &str) -> serde_json::Value {
        let path = path_with_namespace
            .rsplit('/')
            .next()
            .unwrap_or(path_with_namespace);
        let namespace = path_with_namespace
            .split('/')
            .next()
            .unwrap_or("owner")
            .to_string();

        serde_json::json!({
            "id": 1,
            "name": path,
            "path": path,
            "path_with_namespace": path_with_namespace,
            "description": null,
            "default_branch": "main",
            "visibility": "public",
            "archived": false,
            "topics": [],
            "star_count": 0,
            "forks_count": 0,
            "open_issues_count": 0,
            "created_at": "2024-01-01T00:00:00Z",
            "last_activity_at": "2024-01-02T00:00:00Z",
            "namespace": {
                "id": 1,
                "name": namespace.clone(),
                "path": namespace.clone(),
                "full_path": namespace,
                "kind": "group"
            },
            "forked_from_project": null,
            "mirror": false,
            "issues_enabled": true,
            "wiki_enabled": true,
            "merge_requests_enabled": true,
            "web_url": format!("https://gitlab.test/{path_with_namespace}"),
            "ssh_url_to_repo": format!("git@gitlab.test:{path_with_namespace}.git"),
            "http_url_to_repo": format!("https://gitlab.test/{path_with_namespace}.git")
        })
    }

    fn test_client(
        host: &str,
        instance_id: Uuid,
        transport: Arc<dyn HttpTransport>,
    ) -> GitLabClient {
        GitLabClient::new_with_transport(host, "test-token", instance_id, None, transport)
    }

    fn mock_project(path_with_namespace: &str) -> GitLabProject {
        GitLabProject {
            id: 1,
            name: "repo".to_string(),
            path: "repo".to_string(),
            path_with_namespace: path_with_namespace.to_string(),
            description: None,
            default_branch: Some("main".to_string()),
            visibility: "public".to_string(),
            archived: false,
            topics: vec![],
            star_count: 0,
            forks_count: 0,
            open_issues_count: Some(0),
            created_at: Utc::now() - Duration::days(10),
            last_activity_at: Utc::now() - Duration::days(1),
            namespace: crate::gitlab::types::GitLabNamespace {
                id: 1,
                name: "owner".to_string(),
                path: "owner".to_string(),
                full_path: "owner".to_string(),
                kind: "group".to_string(),
            },
            forked_from_project: None,
            mirror: Some(false),
            issues_enabled: Some(true),
            wiki_enabled: Some(true),
            merge_requests_enabled: Some(true),
            web_url: "https://gitlab.example.com/owner/repo".to_string(),
            ssh_url_to_repo: Some("git@gitlab.example.com:owner/repo.git".to_string()),
            http_url_to_repo: Some("https://gitlab.example.com/owner/repo.git".to_string()),
        }
    }

    #[cfg(feature = "migrate")]
    async fn setup_db(instance_id: Uuid) -> sea_orm::DatabaseConnection {
        let db = crate::connect_and_migrate("sqlite::memory:")
            .await
            .expect("test db should migrate");

        let now = chrono::Utc::now().fixed_offset();
        crate::entity::instance::Entity::insert(crate::entity::instance::ActiveModel {
            id: Set(instance_id),
            name: Set("gitlab-test".to_string()),
            platform_type: Set(crate::entity::platform_type::PlatformType::GitLab),
            host: Set("gitlab.test".to_string()),
            oauth_client_id: Set(None),
            oauth_flow: Set("auto".to_string()),
            created_at: Set(now),
        })
        .exec(&db)
        .await
        .expect("instance should insert");

        db
    }

    #[test]
    fn test_rate_limit_info() {
        let info = RateLimitInfo {
            limit: 2000,
            remaining: 1999,
            reset_at: Utc::now(),
            retry_after: None,
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
        fn assert_clone<T: Clone>() {}
        assert_clone::<GitLabClient>();
    }

    #[test]
    fn test_gitlab_client_platform() {
        fn assert_platform_client<T: PlatformClient>() {}
        assert_platform_client::<GitLabClient>();
    }

    #[test]
    fn test_parse_rate_limit_headers() {
        let headers: HttpHeaders = vec![
            ("ratelimit-limit".to_string(), "2000".to_string()),
            ("ratelimit-remaining".to_string(), "1999".to_string()),
            ("ratelimit-reset".to_string(), "1706400000".to_string()),
        ];

        let info = GitLabClient::parse_rate_limit_headers(&headers).unwrap();
        assert_eq!(info.limit, 2000);
        assert_eq!(info.remaining, 1999);
    }

    #[test]
    fn test_parse_rate_limit_headers_missing() {
        let headers: HttpHeaders = Vec::new();
        assert!(GitLabClient::parse_rate_limit_headers(&headers).is_none());
    }

    #[test]
    fn test_parse_rate_limit_headers_invalid_numbers() {
        let headers: HttpHeaders = vec![
            ("ratelimit-limit".to_string(), "abc".to_string()),
            ("ratelimit-remaining".to_string(), "100".to_string()),
            ("ratelimit-reset".to_string(), "1706400000".to_string()),
        ];

        assert!(GitLabClient::parse_rate_limit_headers(&headers).is_none());
    }

    #[test]
    fn test_parse_rate_limit_headers_invalid_remaining_returns_none() {
        let headers: HttpHeaders = vec![
            ("ratelimit-limit".to_string(), "2000".to_string()),
            ("ratelimit-remaining".to_string(), "oops".to_string()),
            ("ratelimit-reset".to_string(), "1706400000".to_string()),
        ];

        assert!(GitLabClient::parse_rate_limit_headers(&headers).is_none());
    }

    #[test]
    fn test_parse_rate_limit_headers_invalid_timestamp_falls_back() {
        let headers: HttpHeaders = vec![
            ("ratelimit-limit".to_string(), "2000".to_string()),
            ("ratelimit-remaining".to_string(), "1999".to_string()),
            (
                "ratelimit-reset".to_string(),
                "9223372036854775807".to_string(),
            ),
        ];

        let now = Utc::now();
        let info = GitLabClient::parse_rate_limit_headers(&headers).unwrap();
        assert_eq!(info.limit, 2000);
        assert_eq!(info.remaining, 1999);
        assert!(info.reset_at >= now);
    }

    #[test]
    fn test_parse_rate_limit_headers_invalid_reset_number_returns_none() {
        let headers: HttpHeaders = vec![
            ("ratelimit-limit".to_string(), "2000".to_string()),
            ("ratelimit-remaining".to_string(), "1999".to_string()),
            ("ratelimit-reset".to_string(), "not-a-number".to_string()),
        ];

        assert!(GitLabClient::parse_rate_limit_headers(&headers).is_none());
    }

    #[test]
    fn test_parse_rate_limit_headers_valid_timestamp_round_trip() {
        let headers: HttpHeaders = vec![
            ("ratelimit-limit".to_string(), "120".to_string()),
            ("ratelimit-remaining".to_string(), "60".to_string()),
            ("ratelimit-reset".to_string(), "1706400000".to_string()),
        ];

        let info = GitLabClient::parse_rate_limit_headers(&headers).unwrap();
        assert_eq!(info.reset_at.timestamp(), 1706400000);
    }

    #[test]
    fn test_parse_rate_limit_headers_zero_reset_timestamp() {
        let headers: HttpHeaders = vec![
            ("ratelimit-limit".to_string(), "7".to_string()),
            ("ratelimit-remaining".to_string(), "3".to_string()),
            ("ratelimit-reset".to_string(), "0".to_string()),
        ];

        let info = GitLabClient::parse_rate_limit_headers(&headers).unwrap();
        assert_eq!(info.limit, 7);
        assert_eq!(info.remaining, 3);
        assert_eq!(info.reset_at.timestamp(), 0);
    }

    #[test]
    fn test_build_starred_paths_deduplicates_entries() {
        let projects = vec![
            mock_project("group/repo-a"),
            mock_project("group/repo-b"),
            mock_project("group/repo-a"),
        ];

        let paths = GitLabClient::build_starred_paths(&projects);
        assert_eq!(paths.len(), 2);
        assert!(paths.contains("group/repo-a"));
        assert!(paths.contains("group/repo-b"));
    }

    #[test]
    fn test_build_starred_paths_empty_input() {
        let paths = GitLabClient::build_starred_paths(&[]);
        assert!(paths.is_empty());
    }

    #[test]
    fn test_parse_rate_limit_headers_negative_reset_timestamp() {
        let headers: HttpHeaders = vec![
            ("ratelimit-limit".to_string(), "10".to_string()),
            ("ratelimit-remaining".to_string(), "1".to_string()),
            ("ratelimit-reset".to_string(), "-1".to_string()),
        ];

        let info = GitLabClient::parse_rate_limit_headers(&headers).unwrap();
        assert_eq!(info.limit, 10);
        assert_eq!(info.remaining, 1);
        assert_eq!(info.reset_at.timestamp(), -1);
        assert!(info.retry_after.is_none());
    }

    #[tokio::test]
    async fn test_cached_starred_status_guard_without_cache() {
        let transport: Arc<dyn HttpTransport> = Arc::new(MockTransport::new());
        let client = test_client("https://gitlab.example.com", Uuid::new_v4(), transport);

        let result = client.cached_starred_status(42, "group/repo-a").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_cached_starred_status_guard_different_user_id() {
        let transport: Arc<dyn HttpTransport> = Arc::new(MockTransport::new());
        let client = test_client("https://gitlab.example.com", Uuid::new_v4(), transport);
        client
            .update_starred_cache(7, &[mock_project("group/repo-a")])
            .await;

        let result = client.cached_starred_status(8, "group/repo-a").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_cached_starred_status_exact_path_selection() {
        let transport: Arc<dyn HttpTransport> = Arc::new(MockTransport::new());
        let client = test_client("https://gitlab.example.com", Uuid::new_v4(), transport);
        client
            .update_starred_cache(7, &[mock_project("group/repo")])
            .await;

        let exact = client.cached_starred_status(7, "group/repo").await;
        let different = client.cached_starred_status(7, "group/repo-extra").await;

        assert_eq!(exact, Some(true));
        assert_eq!(different, Some(false));
    }

    #[tokio::test]
    async fn test_clear_starred_cache_removes_cached_entries() {
        let transport: Arc<dyn HttpTransport> = Arc::new(MockTransport::new());
        let client = test_client("https://gitlab.example.com", Uuid::new_v4(), transport);
        client
            .update_starred_cache(99, &[mock_project("group/repo-a")])
            .await;

        assert_eq!(
            client.cached_starred_status(99, "group/repo-a").await,
            Some(true)
        );

        client.clear_starred_cache().await;

        assert!(
            client
                .cached_starred_status(99, "group/repo-a")
                .await
                .is_none()
        );
    }

    #[test]
    fn test_host_and_instance_id_accessors_return_configured_values() {
        let instance_id = Uuid::new_v4();
        let transport: Arc<dyn HttpTransport> = Arc::new(MockTransport::new());
        let client = test_client("https://self-managed.gitlab", instance_id, transport);

        assert_eq!(client.host(), "https://self-managed.gitlab");
        assert_eq!(client.instance_id(), instance_id);
    }

    #[tokio::test]
    async fn test_get_conditional_not_modified_returns_cache_hit_and_uses_if_none_match() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());
        let client = test_client(TEST_BASE_URL, Uuid::new_v4(), transport_arc);

        let url = format!("{TEST_BASE_URL}/api/v4/groups/acme/projects?page=1");
        push_response(&transport, HttpMethod::Get, &url, 304, Vec::new(), "");

        let result = client
            .get_conditional::<Vec<serde_json::Value>>(&url, Some("W/\"etag-1\""))
            .await
            .expect("request should succeed");

        assert!(matches!(result, FetchResult::NotModified));

        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, url);
        assert_eq!(
            request_header(&requests[0], "If-None-Match"),
            Some("W/\"etag-1\"".to_string())
        );
    }

    #[tokio::test]
    async fn test_get_conditional_unauthorized_maps_to_auth_error() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());
        let client = test_client(TEST_BASE_URL, Uuid::new_v4(), transport_arc);
        let url = format!("{TEST_BASE_URL}/api/v4/projects");
        push_response(
            &transport,
            HttpMethod::Get,
            &url,
            401,
            Vec::new(),
            "unauthorized",
        );

        let err = client
            .get_conditional::<Vec<serde_json::Value>>(&url, None)
            .await
            .expect_err("401 should map to auth error");

        assert!(matches!(err, GitLabError::Auth(_)));
    }

    #[tokio::test]
    async fn test_get_conditional_too_many_requests_maps_to_rate_limited() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());
        let client = test_client(TEST_BASE_URL, Uuid::new_v4(), transport_arc);
        let url = format!("{TEST_BASE_URL}/api/v4/projects");
        push_response(
            &transport,
            HttpMethod::Get,
            &url,
            429,
            Vec::new(),
            "slow down",
        );

        let err = client
            .get_conditional::<Vec<serde_json::Value>>(&url, None)
            .await
            .expect_err("429 should map to rate-limited error");

        assert!(matches!(err, GitLabError::RateLimited { .. }));
    }

    #[tokio::test]
    async fn test_get_conditional_success_returns_data_etag_and_total_pages() {
        let body = serde_json::to_string(&vec![serde_json::json!({"id": 1, "name": "repo"})])
            .expect("body should serialize");
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());
        let client = test_client(TEST_BASE_URL, Uuid::new_v4(), transport_arc);

        let url = format!("{TEST_BASE_URL}/api/v4/projects");
        push_response(
            &transport,
            HttpMethod::Get,
            &url,
            200,
            vec![
                ("Content-Type".to_string(), "application/json".to_string()),
                ("ETag".to_string(), "W/\"etag-abc\"".to_string()),
                ("x-total-pages".to_string(), "3".to_string()),
            ],
            &body,
        );

        let result = client
            .get_conditional::<Vec<serde_json::Value>>(&url, None)
            .await
            .expect("200 response should be parsed");

        match result {
            FetchResult::Fetched {
                data,
                etag,
                pagination,
            } => {
                assert_eq!(data.len(), 1);
                assert_eq!(etag.as_deref(), Some("W/\"etag-abc\""));
                assert_eq!(pagination.total_pages, Some(3));
            }
            other => panic!("expected fetched result, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_get_conditional_not_found_embeds_requested_url() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());
        let client = test_client(TEST_BASE_URL, Uuid::new_v4(), transport_arc);

        let request_url = format!("{TEST_BASE_URL}/api/v4/projects/does/not/exist");
        push_response(
            &transport,
            HttpMethod::Get,
            &request_url,
            404,
            Vec::new(),
            "missing",
        );

        let err = client
            .get_conditional::<Vec<serde_json::Value>>(&request_url, None)
            .await
            .expect_err("404 should map to API not-found error");

        match err {
            GitLabError::Api(message) => {
                assert!(message.contains("Not found:"));
                assert!(message.contains(&request_url));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_list_group_projects_stops_on_empty_second_page() {
        let page_one = serde_json::to_string(&vec![project_json("acme/repo-one")])
            .expect("first page should serialize");

        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());

        let url_page_1 = format!(
            "{TEST_BASE_URL}/api/v4/groups/acme/projects?include_subgroups=false&per_page=100&page=1"
        );
        let url_page_2 = format!(
            "{TEST_BASE_URL}/api/v4/groups/acme/projects?include_subgroups=false&per_page=100&page=2"
        );

        push_response(
            &transport,
            HttpMethod::Get,
            &url_page_1,
            200,
            vec![
                ("Content-Type".to_string(), "application/json".to_string()),
                ("x-total-pages".to_string(), "2".to_string()),
            ],
            &page_one,
        );
        push_response(
            &transport,
            HttpMethod::Get,
            &url_page_2,
            200,
            vec![("Content-Type".to_string(), "application/json".to_string())],
            "[]",
        );

        let client = test_client(TEST_BASE_URL, Uuid::new_v4(), transport_arc);
        let (projects, stats) = client
            .list_group_projects("acme", false, None)
            .await
            .expect("group pagination should succeed");

        assert_eq!(projects.len(), 1);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.pages_fetched, 2);

        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].url, url_page_1);
        assert!(request_header(&requests[0], "If-None-Match").is_none());
        assert_eq!(requests[1].url, url_page_2);
    }

    #[cfg(feature = "migrate")]
    #[tokio::test]
    async fn test_list_group_projects_continues_from_cached_total_after_page_one_304() {
        let page_two = serde_json::to_string(&vec![project_json("acme/repo-two")])
            .expect("second page should serialize");

        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());

        let url_page_1 = format!(
            "{TEST_BASE_URL}/api/v4/groups/acme/projects?include_subgroups=false&per_page=100&page=1"
        );
        let url_page_2 = format!(
            "{TEST_BASE_URL}/api/v4/groups/acme/projects?include_subgroups=false&per_page=100&page=2"
        );

        push_response(
            &transport,
            HttpMethod::Get,
            &url_page_1,
            304,
            Vec::new(),
            "",
        );
        push_response(
            &transport,
            HttpMethod::Get,
            &url_page_2,
            200,
            vec![("Content-Type".to_string(), "application/json".to_string())],
            &page_two,
        );

        let instance_id = Uuid::new_v4();
        let db = setup_db(instance_id).await;
        api_cache::upsert_with_pagination(
            &db,
            instance_id,
            EndpointType::OrgRepos,
            &ApiCacheModel::org_repos_key("acme", 1),
            Some("W/\"cached-p1\"".to_string()),
            Some(2),
        )
        .await
        .expect("cache seed should succeed");

        let client = test_client(TEST_BASE_URL, instance_id, transport_arc);
        let (projects, stats) = client
            .list_group_projects("acme", false, Some(&db))
            .await
            .expect("group pagination should succeed");

        assert_eq!(projects.len(), 1);
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.pages_fetched, 1);

        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].url, url_page_1);
        assert_eq!(
            request_header(&requests[0], "If-None-Match"),
            Some("W/\"cached-p1\"".to_string())
        );
        assert_eq!(requests[1].url, url_page_2);
    }

    #[tokio::test]
    async fn test_resolve_user_id_returns_api_error_when_user_not_found() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());
        let client = test_client(TEST_BASE_URL, Uuid::new_v4(), transport_arc);

        let url = format!("{TEST_BASE_URL}/api/v4/users?username=missing-user");
        push_response(
            &transport,
            HttpMethod::Get,
            &url,
            200,
            vec![("Content-Type".to_string(), "application/json".to_string())],
            "[]",
        );
        let err = client
            .resolve_user_id("missing-user")
            .await
            .expect_err("empty users list should fail");

        match err {
            GitLabError::Api(message) => assert!(message.contains("User not found: missing-user")),
            other => panic!("unexpected error variant: {other:?}"),
        }

        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, url);
    }
}
