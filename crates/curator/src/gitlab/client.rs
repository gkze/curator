//! GitLab API client creation and management.
//!
//! Wraps the Progenitor-generated client from [`super::api`] with pagination,
//! rate-limit header parsing, and starred-project caching.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use reqwest::header::{self, HeaderMap, HeaderValue};
use sea_orm::DatabaseConnection;
use tokio::sync::Mutex;

use uuid::Uuid;

use super::api;
use super::convert::to_platform_repo;
use super::error::{GitLabError, is_rate_limit_error, short_error_message};
use super::types::{GitLabGroup, GitLabProject, GitLabUser};
use crate::api_cache;
use crate::entity::api_cache::{EndpointType, Model as ApiCacheModel};
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::platform_type::PlatformType;
use crate::platform::{
    self, CacheStats, FetchResult, OrgInfo, PaginationInfo, PlatformClient, PlatformError,
    PlatformRepo, RateLimitInfo, UserInfo,
};
use crate::repository;
use crate::retry::with_retry;
use crate::sync::{SyncProgress, emit};

/// GitLab API client backed by the Progenitor-generated typed client.
///
/// The inner [`api::Client`] provides typed methods for each endpoint defined
/// in the trimmed OpenAPI spec.  This wrapper adds:
/// - Automatic pagination (the generated methods return a single page)
/// - Rate-limit header parsing via [`progenitor_client::ResponseValue::headers`]
/// - Starred-project caching for efficient `is_repo_starred` checks
#[derive(Clone)]
pub struct GitLabClient {
    /// Progenitor-generated typed client.
    api: api::Client,
    /// Normalised host URL (e.g. `https://gitlab.com`).
    host: String,
    /// Starred projects cache to avoid re-fetching.
    starred_projects_cache: Arc<Mutex<Option<StarredProjectsCache>>>,
    /// The instance ID this client is configured for.
    instance_id: Uuid,
}

#[derive(Debug, Clone)]
struct StarredProjectsCache {
    user_id: u64,
    paths: HashSet<String>,
}

impl GitLabClient {
    /// Create a new GitLab client.
    ///
    /// Builds a [`reqwest::Client`] with the supplied token and passes it to
    /// the Progenitor-generated client via [`api::Client::new_with_client`].
    pub async fn new(host: &str, token: &str, instance_id: Uuid) -> Result<Self, GitLabError> {
        let host_only = host
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/');

        let base_url = format!("https://{}", host_only);
        let normalized_host = base_url.clone();

        let mut headers = HeaderMap::new();
        headers.insert(
            "PRIVATE-TOKEN",
            HeaderValue::from_str(token)
                .map_err(|e| GitLabError::Auth(format!("Invalid token: {}", e)))?,
        );
        headers.insert(header::ACCEPT, HeaderValue::from_static("application/json"));

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .user_agent("curator")
            .build()
            .map_err(|e| GitLabError::Http(format!("Failed to build HTTP client: {}", e)))?;

        let api = api::Client::new_with_client(&base_url, http);

        // Validate the token with a typed call
        let resp = api.get_api_v4_user().await.map_err(GitLabError::from)?;
        // Discard the body; we only needed to check auth succeeds
        let _ = resp.into_inner();

        Ok(Self {
            api,
            host: normalized_host,
            starred_projects_cache: Arc::new(Mutex::new(None)),
            instance_id,
        })
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
        let mut request = self.api.client.get(url);

        if let Some(etag) = cached_etag {
            request = request.header(reqwest::header::IF_NONE_MATCH, etag);
        }

        let response = request
            .send()
            .await
            .map_err(|e| GitLabError::Http(format!("HTTP request failed: {}", e)))?;

        let status = response.status();
        let headers = response.headers().clone();

        match status {
            reqwest::StatusCode::NOT_MODIFIED => Ok(FetchResult::NotModified),
            s if s.is_success() => {
                let etag = headers
                    .get(reqwest::header::ETAG)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());

                let total_pages: Option<i32> = headers
                    .get("x-total-pages")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok());

                let data: T = response
                    .json()
                    .await
                    .map_err(|e| GitLabError::Deserialize(format!("JSON parse error: {}", e)))?;

                Ok(FetchResult::Fetched {
                    data,
                    etag,
                    pagination: PaginationInfo::from_total_pages(total_pages),
                })
            }
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
                Err(GitLabError::Auth("Authentication failed".to_string()))
            }
            reqwest::StatusCode::NOT_FOUND => Err(GitLabError::Api(format!("Not found: {}", url))),
            _ => {
                let body = response.text().await.unwrap_or_default();
                Err(GitLabError::from_status(status, &body))
            }
        }
    }

    /// Extract rate limit info from response headers.
    fn parse_rate_limit_headers(headers: &HeaderMap) -> Option<RateLimitInfo> {
        let limit = headers
            .get("ratelimit-limit")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())?;
        let remaining = headers
            .get("ratelimit-remaining")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())?;
        let reset_epoch = headers
            .get("ratelimit-reset")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<i64>().ok())?;

        let reset_at = chrono::DateTime::from_timestamp(reset_epoch, 0)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|| Utc::now() + chrono::Duration::minutes(1));

        Some(RateLimitInfo {
            limit,
            remaining,
            reset_at,
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
                        let _ = api_cache::upsert_with_pagination(
                            db,
                            self.instance_id,
                            endpoint_type,
                            &cache_key,
                            etag,
                            tp_to_store,
                        )
                        .await;
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
        let resp = self
            .api
            .get_api_v4_user()
            .await
            .map_err(GitLabError::from)?;
        Ok(GitLabUser::from(resp.into_inner()))
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
        let url = format!("{}/api/v4/users", self.host);
        let resp = self
            .api
            .client
            .get(&url)
            .query(&[("username", username)])
            .send()
            .await
            .map_err(GitLabError::from)?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(GitLabError::from_status(status, &body));
        }

        let users: Vec<serde_json::Value> = resp
            .json()
            .await
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
        let id_str = project_id.to_string();
        match self.api.post_api_v4_projects_id_star(&id_str).await {
            Ok(_) => Ok(true),
            Err(progenitor_client::Error::ErrorResponse(rv))
                if rv.status() == reqwest::StatusCode::NOT_MODIFIED =>
            {
                Ok(false)
            }
            Err(e) => Err(GitLabError::from(e)),
        }
    }

    /// Unstar a project by ID.
    ///
    /// Returns `Ok(true)` if the project was unstarred, `Ok(false)` if wasn't starred.
    pub async fn unstar_project(&self, project_id: u64) -> Result<bool, GitLabError> {
        let id_str = project_id.to_string();
        match self.api.post_api_v4_projects_id_unstar(&id_str).await {
            Ok(_) => Ok(true),
            Err(progenitor_client::Error::ErrorResponse(rv))
                if rv.status() == reqwest::StatusCode::NOT_MODIFIED =>
            {
                Ok(false)
            }
            Err(e) => Err(GitLabError::from(e)),
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
        let resp = self
            .api
            .get_api_v4_groups_id(
                group, None, // custom_attributes
                None, // with_projects
            )
            .await;

        match resp {
            Ok(rv) => {
                let detail: GitLabGroup = rv.into_inner().into();
                Ok(OrgInfo {
                    name: detail.name,
                    public_repos: 0,
                    description: detail.description,
                })
            }
            Err(progenitor_client::Error::ErrorResponse(rv))
                if rv.status() == reqwest::StatusCode::NOT_FOUND =>
            {
                Err(GitLabError::GroupNotFound(group.to_string()))
            }
            Err(e) => Err(GitLabError::from(e)),
        }
    }

    /// Look up a project by its full path (owner/name).
    async fn get_project_by_path(&self, full_path: &str) -> Result<GitLabProject, GitLabError> {
        let resp = self
            .api
            .get_api_v4_projects_id(
                full_path, None, // license
                None, // statistics
                None, // with_custom_attributes
            )
            .await;

        match resp {
            Ok(rv) => Ok(GitLabProject::from(rv.into_inner())),
            Err(progenitor_client::Error::ErrorResponse(rv))
                if rv.status() == reqwest::StatusCode::NOT_FOUND =>
            {
                Err(GitLabError::ProjectNotFound(full_path.to_string()))
            }
            Err(progenitor_client::Error::UnexpectedResponse(rv))
                if rv.status() == reqwest::StatusCode::NOT_FOUND =>
            {
                Err(GitLabError::ProjectNotFound(full_path.to_string()))
            }
            Err(e) => Err(GitLabError::from(e)),
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
        if let Ok(resp) = self.api.get_api_v4_user().await
            && let Some(info) = Self::parse_rate_limit_headers(resp.headers())
        {
            return info;
        }

        // Fallback: GitLab.com defaults (2000 req/min for authenticated users)
        RateLimitInfo {
            limit: 2000,
            remaining: 2000,
            reset_at: Utc::now() + chrono::Duration::minutes(1),
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
            followers: 0,
        })
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
            && let Some(db) = db
        {
            let cached_repos =
                repository::find_all_by_instance_and_owner(db, self.instance_id, org)
                    .await
                    .map_err(|e| PlatformError::internal(e.to_string()))?;

            if !cached_repos.is_empty() {
                emit(
                    on_progress,
                    SyncProgress::CacheHit {
                        namespace: org.to_string(),
                        cached_count: cached_repos.len(),
                    },
                );
                emit(
                    on_progress,
                    SyncProgress::FetchComplete {
                        namespace: org.to_string(),
                        total: cached_repos.len(),
                    },
                );
                return Ok(cached_repos.iter().map(PlatformRepo::from_model).collect());
            }
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
            && let Some(db) = db
        {
            let cached_repos =
                repository::find_all_by_instance_and_owner(db, self.instance_id, username)
                    .await
                    .map_err(|e| PlatformError::internal(e.to_string()))?;

            if !cached_repos.is_empty() {
                emit(
                    on_progress,
                    SyncProgress::CacheHit {
                        namespace: username.to_string(),
                        cached_count: cached_repos.len(),
                    },
                );
                emit(
                    on_progress,
                    SyncProgress::FetchComplete {
                        namespace: username.to_string(),
                        total: cached_repos.len(),
                    },
                );
                return Ok(cached_repos.iter().map(PlatformRepo::from_model).collect());
            }
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
        _concurrency: usize,
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

        let user = self.get_user_info().await?;
        let (projects, stats) = self
            .list_starred_projects(user.id, &user.username, db)
            .await?;

        // Cache-hit fallback
        if stats.all_cached()
            && projects.is_empty()
            && let Some(db) = db
        {
            let cached_repos = repository::find_all_by_instance(db, self.instance_id)
                .await
                .map_err(|e| PlatformError::internal(e.to_string()))?;

            if !cached_repos.is_empty() {
                emit(
                    on_progress,
                    SyncProgress::CacheHit {
                        namespace: "starred".to_string(),
                        cached_count: cached_repos.len(),
                    },
                );
                emit(
                    on_progress,
                    SyncProgress::FetchComplete {
                        namespace: "starred".to_string(),
                        total: cached_repos.len(),
                    },
                );

                // Still update the in-memory starred cache from DB results
                let repos: Vec<PlatformRepo> =
                    cached_repos.iter().map(PlatformRepo::from_model).collect();
                return Ok(repos);
            }
        }

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: "starred".to_string(),
                total: projects.len(),
            },
        );

        self.update_starred_cache(user.id, &projects).await;

        let repos: Vec<PlatformRepo> = projects.iter().map(to_platform_repo).collect();
        Ok(repos)
    }

    async fn list_starred_repos_streaming(
        &self,
        repo_tx: tokio::sync::mpsc::Sender<PlatformRepo>,
        db: Option<&sea_orm::DatabaseConnection>,
        _concurrency: usize,
        _skip_rate_checks: bool,
        on_progress: Option<&platform::ProgressCallback>,
    ) -> platform::Result<usize> {
        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: "starred".to_string(),
                total_repos: None,
                expected_pages: None,
            },
        );

        let user = self.get_user_info().await?;
        let (projects, stats) = self
            .list_starred_projects(user.id, &user.username, db)
            .await?;

        self.update_starred_cache(user.id, &projects).await;

        // Cache-hit fallback for streaming
        if stats.all_cached()
            && projects.is_empty()
            && let Some(db) = db
        {
            let cached_repos = repository::find_all_by_instance(db, self.instance_id)
                .await
                .map_err(|e| PlatformError::internal(e.to_string()))?;

            if !cached_repos.is_empty() {
                emit(
                    on_progress,
                    SyncProgress::CacheHit {
                        namespace: "starred".to_string(),
                        cached_count: cached_repos.len(),
                    },
                );

                let mut sent = 0usize;
                for model in &cached_repos {
                    let repo = PlatformRepo::from_model(model);
                    if repo_tx.send(repo).await.is_ok() {
                        sent += 1;
                    }
                }

                emit(
                    on_progress,
                    SyncProgress::FetchComplete {
                        namespace: "starred".to_string(),
                        total: sent,
                    },
                );
                return Ok(sent);
            }
        }

        let mut sent = 0usize;
        for project in &projects {
            let platform_repo = to_platform_repo(project);
            if repo_tx.send(platform_repo).await.is_ok() {
                sent += 1;
            }
        }

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
    GitLabClient::new(host, token, instance_id).await
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
        let mut headers = HeaderMap::new();
        headers.insert("ratelimit-limit", HeaderValue::from_static("2000"));
        headers.insert("ratelimit-remaining", HeaderValue::from_static("1999"));
        headers.insert("ratelimit-reset", HeaderValue::from_static("1706400000"));

        let info = GitLabClient::parse_rate_limit_headers(&headers).unwrap();
        assert_eq!(info.limit, 2000);
        assert_eq!(info.remaining, 1999);
    }

    #[test]
    fn test_parse_rate_limit_headers_missing() {
        let headers = HeaderMap::new();
        assert!(GitLabClient::parse_rate_limit_headers(&headers).is_none());
    }
}
