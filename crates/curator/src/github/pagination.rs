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
use crate::entity::code_platform::CodePlatform;
use crate::platform::{self, ProgressCallback};
use crate::sync::{SyncProgress, emit};

use super::client::{CacheStats, FetchResult, GitHubClient, check_rate_limit};

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
                CodePlatform::GitHub,
                config.endpoint_type.clone(),
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
                api_cache::get_etag(
                    db,
                    CodePlatform::GitHub,
                    config.endpoint_type.clone(),
                    &cache_key,
                )
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
                    if let Some(total) = pagination.total_pages() {
                        known_total_pages = Some(total);
                    }

                    // Store new ETag and pagination info
                    if let Some(db) = db {
                        let total_pages_to_store = if page == 1 {
                            known_total_pages.map(|t| t as i32)
                        } else {
                            None
                        };

                        let _ = api_cache::upsert_with_pagination(
                            db,
                            CodePlatform::GitHub,
                            config.endpoint_type.clone(),
                            &cache_key,
                            etag,
                            total_pages_to_store,
                        )
                        .await;
                    }

                    all_items.extend(items);

                    emit(
                        on_progress,
                        SyncProgress::FetchedPage {
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
                CodePlatform::GitHub,
                config.endpoint_type.clone(),
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
                CodePlatform::GitHub,
                config.endpoint_type.clone(),
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

                if let Some(total) = pagination.total_pages() {
                    known_total_pages = Some(total);
                }

                // Store ETag
                if let Some(db) = db {
                    let _ = api_cache::upsert_with_pagination(
                        db,
                        CodePlatform::GitHub,
                        config.endpoint_type.clone(),
                        &first_cache_key,
                        etag,
                        known_total_pages.map(|t| t as i32),
                    )
                    .await;
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
                    CodePlatform::GitHub,
                    config.endpoint_type.clone(),
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
                            let _ = api_cache::upsert_with_pagination(
                                db,
                                CodePlatform::GitHub,
                                config.endpoint_type.clone(),
                                &cache_key,
                                new_etag,
                                None,
                            )
                            .await;
                        }
                    } else if count == 0 {
                        stats.cache_hits += 1;
                    }

                    emit(
                        on_progress,
                        SyncProgress::FetchedPage {
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
                total: total_sent.load(Ordering::Relaxed),
            },
        );

        Ok((total_sent.load(Ordering::Relaxed), stats))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[test]
    fn test_with_skip_rate_checks() {
        let config = PaginatedFetchConfig::starred("user").with_skip_rate_checks(true);
        assert!(config.skip_rate_checks);
    }
}
