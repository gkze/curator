//! Paginated API fetching with ETag caching support.
//!
//! This module provides a generic abstraction for paginated GitHub API requests
//! with ETag-based caching to avoid refetching unchanged data.

use sea_orm::DatabaseConnection;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};

use crate::api_cache;
use crate::entity::api_cache::EndpointType;
use crate::platform::{self, CacheStats, FetchResult, PlatformClient, ProgressCallback};
use crate::sync::{SyncProgress, emit};

use super::client::{GitHubClient, check_rate_limit};

use super::error::GitHubError;

/// Configuration for a paginated fetch operation.
pub struct PaginatedFetchConfig<'a> {
    /// The endpoint type for cache lookups.
    pub endpoint_type: EndpointType,
    /// Namespace identifier (used for progress reporting and cache keys).
    pub namespace: &'a str,
    /// Function to build the API route for a given page number.
    pub route_fn: Box<dyn Fn(u32) -> String + Send + Sync + 'a>,
    /// Function to build the cache key for a given page number.
    pub cache_key_fn: Box<dyn Fn(u32) -> String + Send + Sync + 'a>,
    /// Expected total repos (for progress bar estimation).
    pub expected_total_repos: Option<usize>,
    /// Skip rate limit checks between requests.
    pub skip_rate_checks: bool,
}

impl<'a> PaginatedFetchConfig<'a> {
    /// Create config for fetching organization repositories.
    pub fn org_repos(org: &'a str, expected_repos: Option<usize>) -> Self {
        Self {
            endpoint_type: EndpointType::OrgRepos,
            namespace: org,
            route_fn: Box::new(move |page| {
                format!("/orgs/{}/repos?per_page=100&page={}", org, page)
            }),
            cache_key_fn: Box::new(move |page| format!("{}/page/{}", org, page)),
            expected_total_repos: expected_repos,
            skip_rate_checks: false,
        }
    }

    /// Create config for fetching starred repositories.
    pub fn starred(username: &'a str) -> Self {
        use crate::entity::api_cache::Model as ApiCacheModel;

        Self {
            endpoint_type: EndpointType::Starred,
            namespace: "starred",
            route_fn: Box::new(move |page| format!("/user/starred?per_page=100&page={}", page)),
            cache_key_fn: Box::new(move |page| ApiCacheModel::starred_key(username, page)),
            expected_total_repos: None,
            skip_rate_checks: false,
        }
    }

    /// Create config for fetching user repositories.
    pub fn user_repos(username: &'a str, expected_repos: Option<usize>) -> Self {
        Self {
            endpoint_type: EndpointType::UserRepos,
            namespace: username,
            route_fn: Box::new(move |page| {
                format!("/users/{}/repos?per_page=100&page={}", username, page)
            }),
            cache_key_fn: Box::new(move |page| format!("{}/page/{}", username, page)),
            expected_total_repos: expected_repos,
            skip_rate_checks: false,
        }
    }

    /// Set whether to skip rate limit checks.
    pub fn with_skip_rate_checks(mut self, skip: bool) -> Self {
        self.skip_rate_checks = skip;
        self
    }
}

/// Result of a paginated fetch operation.
pub struct PaginatedFetchResult<T> {
    /// The fetched items.
    pub items: Vec<T>,
    /// Cache statistics.
    pub stats: CacheStats,
    /// Whether all pages were cache hits.
    pub all_cache_hits: bool,
}

impl GitHubClient {
    /// Fetch all pages of a paginated endpoint with ETag caching.
    ///
    /// This method:
    /// - Checks for cached ETags before each request
    /// - Skips fetching if the server returns 304 Not Modified
    /// - Stores new ETags and pagination info after successful fetches
    /// - Continues through all pages using stored pagination metadata
    ///
    /// If all pages are cache hits and db is provided, attempts to load
    /// cached data from the database.
    pub async fn fetch_pages<T: DeserializeOwned>(
        &self,
        config: &PaginatedFetchConfig<'_>,
        db: Option<&DatabaseConnection>,
        on_progress: Option<&ProgressCallback>,
    ) -> platform::Result<PaginatedFetchResult<T>> {
        let mut all_items: Vec<T> = Vec::new();
        let mut page = 1u32;
        let mut stats = CacheStats::default();
        let mut known_total_pages: Option<u32> = None;
        let mut all_cache_hits = true;

        // Try to get stored total_pages from the database
        if let Some(db) = db
            && let Ok(Some(stored_total)) = api_cache::get_total_pages(
                db,
                self.instance_id(),
                config.endpoint_type,
                config.namespace,
            )
            .await
        {
            known_total_pages = Some(stored_total as u32);
        }

        // Calculate expected pages
        let expected_pages = known_total_pages
            .or_else(|| config.expected_total_repos.map(|t| t.div_ceil(100) as u32));

        emit(
            on_progress,
            SyncProgress::FetchingRepos {
                namespace: config.namespace.to_string(),
                total_repos: config.expected_total_repos,
                expected_pages,
            },
        );

        loop {
            // Check rate limit before making request
            if !config.skip_rate_checks {
                check_rate_limit(self.inner())
                    .await
                    .map_err(platform::PlatformError::from)?;
            }

            let route = (config.route_fn)(page);
            let cache_key = (config.cache_key_fn)(page);

            // Try to get cached ETag
            let cached_etag = if let Some(db) = db {
                api_cache::get_etag(db, self.instance_id(), config.endpoint_type, &cache_key)
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            };

            // Make conditional request
            let result: FetchResult<Vec<T>> = self
                .get_conditional(&route, cached_etag.as_deref())
                .await
                .map_err(platform::PlatformError::from)?;

            match result {
                FetchResult::NotModified => {
                    stats.cache_hits += 1;

                    emit(
                        on_progress,
                        SyncProgress::FetchedPage {
                            namespace: config.namespace.to_string(),
                            page,
                            count: 0,
                            total_so_far: all_items.len(),
                            expected_pages: known_total_pages,
                        },
                    );

                    // Continue to next page if we know there are more
                    if let Some(total) = known_total_pages
                        && page < total
                    {
                        page += 1;
                        continue;
                    }
                    // If we don't know total pages and hit cache, we're done
                    break;
                }
                FetchResult::Fetched {
                    data: items,
                    etag,
                    pagination,
                } => {
                    all_cache_hits = false;
                    stats.pages_fetched += 1;
                    let count = items.len();

                    // Update known total pages from Link header
                    if let Some(total) = pagination.total_pages {
                        known_total_pages = Some(total);
                    }

                    // Store new ETag and pagination info
                    if let Some(db) = db {
                        let total_pages_to_store = if page == 1 {
                            known_total_pages.map(|t| t as i32)
                        } else {
                            None
                        };

                        if let Err(e) = api_cache::upsert_with_pagination(
                            db,
                            self.instance_id(),
                            config.endpoint_type,
                            &cache_key,
                            etag,
                            total_pages_to_store,
                        )
                        .await
                        {
                            tracing::debug!("api cache upsert failed: {e}");
                        }
                    }

                    all_items.extend(items);

                    emit(
                        on_progress,
                        SyncProgress::FetchedPage {
                            namespace: config.namespace.to_string(),
                            page,
                            count,
                            total_so_far: all_items.len(),
                            expected_pages: known_total_pages,
                        },
                    );

                    // Check if we should continue
                    if let Some(total) = known_total_pages {
                        if page < total {
                            page += 1;
                            continue;
                        }
                        break;
                    }

                    // No pagination info - use count-based heuristic
                    if count < 100 {
                        break;
                    }

                    page += 1;
                }
            }
        }

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: config.namespace.to_string(),
                total: all_items.len(),
            },
        );

        Ok(PaginatedFetchResult {
            items: all_items,
            stats,
            all_cache_hits,
        })
    }

    /// Fetch pages with streaming output through a channel.
    ///
    /// Similar to `fetch_pages`, but sends items through the channel as pages
    /// complete, allowing downstream processing to start immediately.
    ///
    /// Returns the total count of items sent.
    pub async fn fetch_pages_streaming<T, U, F>(
        &self,
        config: &PaginatedFetchConfig<'_>,
        db: Option<&DatabaseConnection>,
        tx: mpsc::Sender<U>,
        convert: F,
        concurrency: usize,
        on_progress: Option<&ProgressCallback>,
    ) -> platform::Result<(usize, CacheStats)>
    where
        T: DeserializeOwned + Send + Sync + 'static,
        U: Send + 'static,
        F: Fn(&T) -> U + Send + Sync + Clone + 'static,
    {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let total_sent = Arc::new(AtomicUsize::new(0));
        let mut stats = CacheStats::default();
        let mut known_total_pages: Option<u32> = None;

        // Try to get stored total_pages from the database
        if let Some(db) = db
            && let Ok(Some(stored_total)) = api_cache::get_total_pages(
                db,
                self.instance_id(),
                config.endpoint_type,
                config.namespace,
            )
            .await
        {
            known_total_pages = Some(stored_total as u32);
        }

        // Check rate limit before first page
        if !config.skip_rate_checks {
            check_rate_limit(self.inner())
                .await
                .map_err(platform::PlatformError::from)?;
        }

        // Fetch first page to get pagination info
        let first_route = (config.route_fn)(1);
        let first_cache_key = (config.cache_key_fn)(1);
        let first_etag = if let Some(db) = db {
            api_cache::get_etag(
                db,
                self.instance_id(),
                config.endpoint_type,
                &first_cache_key,
            )
            .await
            .ok()
            .flatten()
        } else {
            None
        };

        let first_result: FetchResult<Vec<T>> = self
            .get_conditional(&first_route, first_etag.as_deref())
            .await
            .map_err(platform::PlatformError::from)?;

        match first_result {
            FetchResult::NotModified => {
                stats.cache_hits += 1;
                emit(
                    on_progress,
                    SyncProgress::FetchingRepos {
                        namespace: config.namespace.to_string(),
                        total_repos: known_total_pages.map(|p| (p * 100) as usize),
                        expected_pages: known_total_pages,
                    },
                );
                emit(
                    on_progress,
                    SyncProgress::FetchedPage {
                        namespace: config.namespace.to_string(),
                        page: 1,
                        count: 0,
                        total_so_far: 0,
                        expected_pages: known_total_pages,
                    },
                );
            }
            FetchResult::Fetched {
                data: items,
                etag,
                pagination,
            } => {
                let count = items.len();
                stats.pages_fetched += 1;

                if let Some(total) = pagination.total_pages {
                    known_total_pages = Some(total);
                }

                // Store ETag
                if let Some(db) = db
                    && let Err(e) = api_cache::upsert_with_pagination(
                        db,
                        self.instance_id(),
                        config.endpoint_type,
                        &first_cache_key,
                        etag,
                        known_total_pages.map(|t| t as i32),
                    )
                    .await
                {
                    tracing::debug!("api cache upsert failed: {e}");
                }

                emit(
                    on_progress,
                    SyncProgress::FetchingRepos {
                        namespace: config.namespace.to_string(),
                        total_repos: known_total_pages.map(|p| (p * 100) as usize),
                        expected_pages: known_total_pages,
                    },
                );

                // Send first page items
                for item in &items {
                    if tx.send(convert(item)).await.is_ok() {
                        total_sent.fetch_add(1, Ordering::Relaxed);
                    }
                }

                emit(
                    on_progress,
                    SyncProgress::FetchedPage {
                        namespace: config.namespace.to_string(),
                        page: 1,
                        count,
                        total_so_far: total_sent.load(Ordering::Relaxed),
                        expected_pages: known_total_pages,
                    },
                );

                // If first page wasn't full and we don't know total, we're done
                if known_total_pages.is_none() && count < 100 {
                    emit(
                        on_progress,
                        SyncProgress::FetchComplete {
                            namespace: config.namespace.to_string(),
                            total: total_sent.load(Ordering::Relaxed),
                        },
                    );
                    return Ok((total_sent.load(Ordering::Relaxed), stats));
                }
            }
        }

        // Fetch remaining pages concurrently
        let last_page = known_total_pages.unwrap_or(1);
        if last_page > 1 {
            // Pre-fetch ETags for all remaining pages
            let page_cache_keys: Vec<String> =
                (2..=last_page).map(|p| (config.cache_key_fn)(p)).collect();

            let etags_map = if let Some(db) = db {
                api_cache::get_etags_batch(
                    db,
                    self.instance_id(),
                    config.endpoint_type,
                    &page_cache_keys,
                )
                .await
                .unwrap_or_default()
            } else {
                std::collections::HashMap::new()
            };

            let semaphore = Arc::new(Semaphore::new(concurrency));
            let mut handles = Vec::new();
            let client = self.clone();
            let channel_closed = Arc::new(AtomicBool::new(false));

            for page in 2..=last_page {
                let task_semaphore = Arc::clone(&semaphore);
                let task_client = client.clone();
                let task_tx = tx.clone();
                let task_total_sent = Arc::clone(&total_sent);
                let task_channel_closed = Arc::clone(&channel_closed);
                let route = (config.route_fn)(page);
                let cache_key = (config.cache_key_fn)(page);
                let cached_etag = etags_map.get(&cache_key).cloned().flatten();
                let convert_fn = convert.clone();
                let skip_rate_checks = config.skip_rate_checks;

                let handle = tokio::spawn(async move {
                    let _permit = match task_semaphore.acquire().await {
                        Ok(permit) => permit,
                        Err(_) => return (page, 0, None, false, true),
                    };

                    if !skip_rate_checks && check_rate_limit(task_client.inner()).await.is_err() {
                        return (page, 0, None, false, false);
                    }

                    let result: Result<FetchResult<Vec<T>>, GitHubError> = task_client
                        .get_conditional(&route, cached_etag.as_deref())
                        .await;

                    match result {
                        Ok(FetchResult::NotModified) => (page, 0, None, false, false),
                        Ok(FetchResult::Fetched { data, etag, .. }) => {
                            let count = data.len();
                            let mut sent = 0usize;

                            if !task_channel_closed.load(Ordering::Relaxed) {
                                for item in &data {
                                    if task_tx.send(convert_fn(item)).await.is_ok() {
                                        task_total_sent.fetch_add(1, Ordering::Relaxed);
                                        sent += 1;
                                    } else {
                                        task_channel_closed.store(true, Ordering::Relaxed);
                                        break;
                                    }
                                }
                            }

                            (page, count, etag, true, sent < count)
                        }
                        Err(_) => (page, 0, None, false, false),
                    }
                });

                handles.push(handle);
            }

            // Collect results and update stats
            for handle in handles {
                if let Ok((page, count, new_etag, was_fetched, _channel_err)) = handle.await {
                    if was_fetched {
                        stats.pages_fetched += 1;

                        // Store new ETag
                        if let Some(db) = db {
                            let cache_key = (config.cache_key_fn)(page);
                            if let Err(e) = api_cache::upsert_with_pagination(
                                db,
                                self.instance_id(),
                                config.endpoint_type,
                                &cache_key,
                                new_etag,
                                None,
                            )
                            .await
                            {
                                tracing::debug!("api cache upsert failed: {e}");
                            }
                        }
                    } else if count == 0 {
                        stats.cache_hits += 1;
                    }

                    emit(
                        on_progress,
                        SyncProgress::FetchedPage {
                            namespace: config.namespace.to_string(),
                            page,
                            count,
                            total_so_far: total_sent.load(Ordering::Relaxed),
                            expected_pages: known_total_pages,
                        },
                    );
                }
            }
        }

        // Note: All-cache-hits case (loading from DB) is handled by the caller
        // since the streaming function can't use the convert function with DB models

        emit(
            on_progress,
            SyncProgress::FetchComplete {
                namespace: config.namespace.to_string(),
                total: total_sent.load(Ordering::Relaxed),
            },
        );

        Ok((total_sent.load(Ordering::Relaxed), stats))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use octocrab::Octocrab;
    #[cfg(feature = "migrate")]
    use sea_orm::ActiveValue::Set;
    #[cfg(feature = "migrate")]
    use sea_orm::EntityTrait;
    use std::sync::Arc;
    use uuid::Uuid;

    use crate::{HttpHeaders, HttpMethod, HttpRequest, HttpResponse, HttpTransport, MockTransport};

    const TEST_BASE_URL: &str = "http://local.test";

    fn push_response(
        transport: &MockTransport,
        url: &str,
        status: u16,
        headers: HttpHeaders,
        body: &str,
    ) {
        transport.push_response(
            HttpMethod::Get,
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

    fn item_array_json(count: usize) -> String {
        let entries: Vec<serde_json::Value> = (0..count)
            .map(|idx| serde_json::json!({"id": idx as u64}))
            .collect();
        serde_json::Value::Array(entries).to_string()
    }

    #[cfg(feature = "migrate")]
    async fn setup_db(instance_id: Uuid) -> sea_orm::DatabaseConnection {
        let db = crate::connect_and_migrate("sqlite::memory:")
            .await
            .expect("test db should migrate");

        let now = chrono::Utc::now().fixed_offset();
        crate::entity::instance::Entity::insert(crate::entity::instance::ActiveModel {
            id: Set(instance_id),
            name: Set("pagination-test".to_string()),
            platform_type: Set(crate::entity::platform_type::PlatformType::GitHub),
            host: Set("pagination.test".to_string()),
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
    fn test_org_repos_config() {
        let config = PaginatedFetchConfig::org_repos("rust-lang", Some(500));
        assert_eq!(config.endpoint_type, EndpointType::OrgRepos);
        assert_eq!(config.namespace, "rust-lang");
        assert_eq!(config.expected_total_repos, Some(500));
        assert!(!config.skip_rate_checks);

        let route = (config.route_fn)(1);
        assert_eq!(route, "/orgs/rust-lang/repos?per_page=100&page=1");

        let cache_key = (config.cache_key_fn)(2);
        assert_eq!(cache_key, "rust-lang/page/2");
    }

    #[test]
    fn test_starred_config() {
        let config = PaginatedFetchConfig::starred("gkze");
        assert_eq!(config.endpoint_type, EndpointType::Starred);
        assert_eq!(config.namespace, "starred");
        assert!(config.expected_total_repos.is_none());

        let route = (config.route_fn)(3);
        assert_eq!(route, "/user/starred?per_page=100&page=3");

        let cache_key = (config.cache_key_fn)(1);
        assert_eq!(cache_key, "gkze/starred/page/1");
    }

    #[test]
    fn test_user_repos_config() {
        let config = PaginatedFetchConfig::user_repos("octocat", Some(100));
        assert_eq!(config.endpoint_type, EndpointType::UserRepos);
        assert_eq!(config.namespace, "octocat");

        let route = (config.route_fn)(2);
        assert_eq!(route, "/users/octocat/repos?per_page=100&page=2");

        let cache_key = (config.cache_key_fn)(4);
        assert_eq!(cache_key, "octocat/page/4");
    }

    #[test]
    fn test_org_repos_config_without_expected_total() {
        let config = PaginatedFetchConfig::org_repos("rust-lang", None);
        assert_eq!(config.endpoint_type, EndpointType::OrgRepos);
        assert_eq!(config.namespace, "rust-lang");
        assert!(config.expected_total_repos.is_none());
        assert_eq!(
            config.route_fn.as_ref()(10),
            "/orgs/rust-lang/repos?per_page=100&page=10"
        );
        assert_eq!(config.cache_key_fn.as_ref()(10), "rust-lang/page/10");
    }

    #[test]
    fn test_with_skip_rate_checks() {
        let config = PaginatedFetchConfig::starred("user").with_skip_rate_checks(true);
        assert!(config.skip_rate_checks);
    }

    #[test]
    fn test_with_skip_rate_checks_preserves_other_fields() {
        let config = PaginatedFetchConfig::org_repos("rust-lang", Some(320));
        let updated = config.with_skip_rate_checks(true);

        assert_eq!(updated.endpoint_type, EndpointType::OrgRepos);
        assert_eq!(updated.namespace, "rust-lang");
        assert_eq!(updated.expected_total_repos, Some(320));
        assert!(updated.skip_rate_checks);
        assert_eq!(
            updated.route_fn.as_ref()(3),
            "/orgs/rust-lang/repos?per_page=100&page=3"
        );
        assert_eq!(updated.cache_key_fn.as_ref()(3), "rust-lang/page/3");
    }

    #[test]
    fn test_with_skip_rate_checks_can_toggle_back_to_false() {
        let config = PaginatedFetchConfig::starred("user")
            .with_skip_rate_checks(true)
            .with_skip_rate_checks(false);
        assert!(!config.skip_rate_checks);
    }

    #[test]
    fn test_starred_config_cache_key_scopes_to_username() {
        let config = PaginatedFetchConfig::starred("alice");
        let cache_key = (config.cache_key_fn)(9);
        assert_eq!(cache_key, "alice/starred/page/9");
    }

    #[test]
    fn test_starred_configs_keep_same_route_but_isolated_cache_keys() {
        let alice = PaginatedFetchConfig::starred("alice");
        let bob = PaginatedFetchConfig::starred("bob");

        assert_eq!((alice.route_fn)(4), (bob.route_fn)(4));
        assert_ne!((alice.cache_key_fn)(4), (bob.cache_key_fn)(4));
    }

    #[test]
    fn test_user_repos_config_preserves_namespace_with_slashes() {
        let config = PaginatedFetchConfig::user_repos("team/user", Some(1));

        assert_eq!(config.namespace, "team/user");
        assert_eq!(
            config.route_fn.as_ref()(1),
            "/users/team/user/repos?per_page=100&page=1"
        );
        assert_eq!(config.cache_key_fn.as_ref()(1), "team/user/page/1");
    }

    #[test]
    fn test_expected_total_repos_preserves_zero_value() {
        let config = PaginatedFetchConfig::org_repos("empty-org", Some(0));
        assert_eq!(config.expected_total_repos, Some(0));
    }

    #[test]
    fn test_starred_config_namespace_is_constant_across_usernames() {
        let alice = PaginatedFetchConfig::starred("alice");
        let bob = PaginatedFetchConfig::starred("bob");

        assert_eq!(alice.namespace, "starred");
        assert_eq!(bob.namespace, "starred");
    }

    #[test]
    fn test_starred_config_with_empty_username_keeps_namespace_and_builds_cache_key() {
        let config = PaginatedFetchConfig::starred("");

        assert_eq!(config.namespace, "starred");
        assert_eq!((config.route_fn)(2), "/user/starred?per_page=100&page=2");
        assert_eq!((config.cache_key_fn)(2), "/starred/page/2");
    }

    #[test]
    fn test_org_and_user_configs_keep_distinct_endpoint_types() {
        let org_config = PaginatedFetchConfig::org_repos("same-name", Some(10));
        let user_config = PaginatedFetchConfig::user_repos("same-name", Some(10));

        assert_eq!(org_config.endpoint_type, EndpointType::OrgRepos);
        assert_eq!(user_config.endpoint_type, EndpointType::UserRepos);
    }

    #[test]
    fn test_org_config_preserves_namespace_with_slashes() {
        let config = PaginatedFetchConfig::org_repos("parent/team", Some(42));

        assert_eq!(config.namespace, "parent/team");
        assert_eq!(
            config.route_fn.as_ref()(2),
            "/orgs/parent/team/repos?per_page=100&page=2"
        );
        assert_eq!(config.cache_key_fn.as_ref()(2), "parent/team/page/2");
    }

    #[test]
    fn test_org_config_supports_page_zero_route_and_cache_key() {
        let config = PaginatedFetchConfig::org_repos("rust-lang", Some(1));

        assert_eq!(
            config.route_fn.as_ref()(0),
            "/orgs/rust-lang/repos?per_page=100&page=0"
        );
        assert_eq!(config.cache_key_fn.as_ref()(0), "rust-lang/page/0");
    }

    #[test]
    fn test_user_config_supports_max_u32_page_value() {
        let config = PaginatedFetchConfig::user_repos("octocat", None);

        assert_eq!(
            config.route_fn.as_ref()(u32::MAX),
            "/users/octocat/repos?per_page=100&page=4294967295"
        );
        assert_eq!(
            config.cache_key_fn.as_ref()(u32::MAX),
            "octocat/page/4294967295"
        );
    }

    #[tokio::test]
    async fn test_fetch_pages_collects_multiple_pages_from_link_header() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());

        push_response(
            &transport,
            "http://local.test/repos?page=1",
            200,
            vec![
                ("content-type".to_string(), "application/json".to_string()),
                (
                    "link".to_string(),
                    "<http://local.test/repos?page=2>; rel=\"next\", <http://local.test/repos?page=2>; rel=\"last\""
                        .to_string(),
                ),
                ("etag".to_string(), "\"p1\"".to_string()),
            ],
            "[{\"id\":1}]",
        );
        push_response(
            &transport,
            "http://local.test/repos?page=2",
            200,
            vec![
                ("content-type".to_string(), "application/json".to_string()),
                ("etag".to_string(), "\"p2\"".to_string()),
            ],
            "[{\"id\":2}]",
        );

        let octocrab = Octocrab::builder()
            .build()
            .expect("octocrab client should build");
        let client = GitHubClient::from_octocrab_with_transport_and_base_url(
            octocrab,
            Uuid::new_v4(),
            transport_arc,
            TEST_BASE_URL,
        );

        let config = PaginatedFetchConfig {
            endpoint_type: EndpointType::UserRepos,
            namespace: "octocat",
            route_fn: Box::new(|page| format!("/repos?page={}", page)),
            cache_key_fn: Box::new(|page| format!("octocat/page/{}", page)),
            expected_total_repos: None,
            skip_rate_checks: true,
        };

        let result = client
            .fetch_pages::<serde_json::Value>(&config, None, None)
            .await
            .expect("fetch should succeed");

        assert_eq!(result.items.len(), 2);
        assert_eq!(result.stats.pages_fetched, 2);
        assert_eq!(result.stats.cache_hits, 0);
        assert!(!result.all_cache_hits);
    }

    #[tokio::test]
    async fn test_fetch_pages_stops_by_count_heuristic_without_pagination() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());

        push_response(
            &transport,
            "http://local.test/repos?page=1",
            200,
            vec![
                ("content-type".to_string(), "application/json".to_string()),
                ("etag".to_string(), "\"p1\"".to_string()),
            ],
            &item_array_json(100),
        );
        push_response(
            &transport,
            "http://local.test/repos?page=2",
            200,
            vec![("content-type".to_string(), "application/json".to_string())],
            &item_array_json(20),
        );

        let octocrab = Octocrab::builder()
            .build()
            .expect("octocrab client should build");
        let client = GitHubClient::from_octocrab_with_transport_and_base_url(
            octocrab,
            Uuid::new_v4(),
            transport_arc,
            TEST_BASE_URL,
        );

        let config = PaginatedFetchConfig {
            endpoint_type: EndpointType::UserRepos,
            namespace: "octocat",
            route_fn: Box::new(|page| format!("/repos?page={}", page)),
            cache_key_fn: Box::new(|page| format!("octocat/page/{}", page)),
            expected_total_repos: None,
            skip_rate_checks: true,
        };

        let result = client
            .fetch_pages::<serde_json::Value>(&config, None, None)
            .await
            .expect("fetch should stop on count heuristic");

        assert_eq!(result.items.len(), 120);
        assert_eq!(result.stats.pages_fetched, 2);
        assert_eq!(result.stats.cache_hits, 0);
        assert!(!result.all_cache_hits);

        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].url, "http://local.test/repos?page=1");
        assert_eq!(
            request_header(&requests[0], "Accept").as_deref(),
            Some("application/vnd.github+json")
        );
        assert_eq!(
            request_header(&requests[0], "User-Agent").as_deref(),
            Some("curator")
        );
        assert!(
            request_header(&requests[0], "Authorization")
                .unwrap_or_default()
                .starts_with("Bearer")
        );
        assert_eq!(requests[1].url, "http://local.test/repos?page=2");
    }

    #[tokio::test]
    async fn test_fetch_pages_not_modified_without_known_total_stops_after_first_page() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());
        push_response(
            &transport,
            "http://local.test/orgs/acme/repos?page=1",
            304,
            Vec::new(),
            "",
        );

        let octocrab = Octocrab::builder()
            .build()
            .expect("octocrab client should build");
        let client = GitHubClient::from_octocrab_with_transport_and_base_url(
            octocrab,
            Uuid::new_v4(),
            transport_arc,
            TEST_BASE_URL,
        );

        let config = PaginatedFetchConfig {
            endpoint_type: EndpointType::OrgRepos,
            namespace: "acme",
            route_fn: Box::new(|page| format!("/orgs/acme/repos?page={}", page)),
            cache_key_fn: Box::new(|page| format!("acme/page/{}", page)),
            expected_total_repos: None,
            skip_rate_checks: true,
        };

        let result = client
            .fetch_pages::<serde_json::Value>(&config, None, None)
            .await
            .expect("cache hit should be handled");

        assert!(result.items.is_empty());
        assert_eq!(result.stats.pages_fetched, 0);
        assert_eq!(result.stats.cache_hits, 1);
        assert!(result.all_cache_hits);
    }

    #[cfg(feature = "migrate")]
    #[tokio::test]
    async fn test_fetch_pages_streaming_not_modified_first_page_completes_early() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());

        push_response(
            &transport,
            "http://local.test/repos?page=1",
            304,
            Vec::new(),
            "",
        );

        let octocrab = Octocrab::builder()
            .build()
            .expect("octocrab client should build");
        let client = GitHubClient::from_octocrab_with_transport_and_base_url(
            octocrab,
            Uuid::new_v4(),
            transport_arc,
            TEST_BASE_URL,
        );

        let (tx, _rx) = tokio::sync::mpsc::channel::<serde_json::Value>(128);
        let config = PaginatedFetchConfig {
            endpoint_type: EndpointType::UserRepos,
            namespace: "octocat",
            route_fn: Box::new(|page| format!("/repos?page={}", page)),
            cache_key_fn: Box::new(|page| format!("octocat/page/{}", page)),
            expected_total_repos: None,
            skip_rate_checks: true,
        };

        let (count, stats) = client
            .fetch_pages_streaming::<serde_json::Value, serde_json::Value, _>(
                &config,
                None,
                tx,
                |item| item.clone(),
                4,
                None,
            )
            .await
            .expect("streaming fetch should return from not-modified first page");

        assert_eq!(count, 0);
        assert_eq!(stats.pages_fetched, 0);
        assert_eq!(stats.cache_hits, 1);
    }

    #[tokio::test]
    async fn test_fetch_pages_streaming_first_page_short_count_returns_early() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());

        push_response(
            &transport,
            "http://local.test/repos?page=1",
            200,
            vec![
                ("content-type".to_string(), "application/json".to_string()),
                ("etag".to_string(), "\"p1\"".to_string()),
            ],
            &item_array_json(42),
        );

        let octocrab = Octocrab::builder()
            .build()
            .expect("octocrab client should build");
        let client = GitHubClient::from_octocrab_with_transport_and_base_url(
            octocrab,
            Uuid::new_v4(),
            transport_arc,
            TEST_BASE_URL,
        );

        let (tx, _rx) = tokio::sync::mpsc::channel::<serde_json::Value>(128);
        let config = PaginatedFetchConfig {
            endpoint_type: EndpointType::UserRepos,
            namespace: "octocat",
            route_fn: Box::new(|page| format!("/repos?page={}", page)),
            cache_key_fn: Box::new(|page| format!("octocat/page/{}", page)),
            expected_total_repos: None,
            skip_rate_checks: true,
        };

        let (count, stats) = client
            .fetch_pages_streaming::<serde_json::Value, serde_json::Value, _>(
                &config,
                None,
                tx,
                |item| item.clone(),
                4,
                None,
            )
            .await
            .expect("streaming fetch should return from short first page");

        assert_eq!(count, 42);
        assert_eq!(stats.pages_fetched, 1);
        assert_eq!(stats.cache_hits, 0);

        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "http://local.test/repos?page=1");
        assert_eq!(
            request_header(&requests[0], "Accept").as_deref(),
            Some("application/vnd.github+json")
        );
    }

    #[cfg(feature = "migrate")]
    #[tokio::test]
    async fn test_fetch_pages_uses_cached_total_pages_to_continue_after_cache_hit() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());
        push_response(
            &transport,
            "http://local.test/repos?page=1",
            304,
            Vec::new(),
            "",
        );
        push_response(
            &transport,
            "http://local.test/repos?page=2",
            200,
            vec![
                ("content-type".to_string(), "application/json".to_string()),
                ("etag".to_string(), "\"p2\"".to_string()),
            ],
            "[{\"id\":2}]",
        );

        let instance_id = Uuid::new_v4();
        let db = setup_db(instance_id).await;
        crate::api_cache::upsert_with_pagination(
            &db,
            instance_id,
            EndpointType::UserRepos,
            "octocat/page/1",
            Some("\"cached-p1\"".to_string()),
            Some(2),
        )
        .await
        .expect("cache seed should succeed");

        let octocrab = Octocrab::builder()
            .build()
            .expect("octocrab client should build");
        let client = GitHubClient::from_octocrab_with_transport_and_base_url(
            octocrab,
            instance_id,
            transport_arc,
            TEST_BASE_URL,
        );

        let config = PaginatedFetchConfig {
            endpoint_type: EndpointType::UserRepos,
            namespace: "octocat",
            route_fn: Box::new(|page| format!("/repos?page={}", page)),
            cache_key_fn: Box::new(|page| format!("octocat/page/{}", page)),
            expected_total_repos: None,
            skip_rate_checks: true,
        };

        let result = client
            .fetch_pages::<serde_json::Value>(&config, Some(&db), None)
            .await
            .expect("fetch should succeed");

        assert_eq!(result.items.len(), 1);
        assert_eq!(result.stats.cache_hits, 1);
        assert_eq!(result.stats.pages_fetched, 1);
        assert!(!result.all_cache_hits);

        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].url, "http://local.test/repos?page=1");
        assert_eq!(
            request_header(&requests[0], "If-None-Match"),
            Some("\"cached-p1\"".to_string())
        );
        assert_eq!(requests[1].url, "http://local.test/repos?page=2");
    }
}
