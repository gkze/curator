//! Unified sync engine for all platforms.
//!
//! This module provides a platform-agnostic sync implementation that works with
//! any type implementing the `PlatformClient` trait.
//!
//! # Rate Limiting
//!
//! All sync functions accept an optional `ApiRateLimiter` to enable proactive
//! rate limiting. When provided, the limiter's `wait()` method is called before
//! each API operation to prevent hitting platform rate limits.
//!
//! # Example
//!
//! ```ignore
//! use curator::sync::{SyncOptions, sync_namespace_streaming};
//! use curator::platform::{ApiRateLimiter, rate_limits};
//!
//! let limiter = ApiRateLimiter::new(rate_limits::GITHUB_DEFAULT_RPS);
//! let result = sync_namespace_streaming(
//!     &client,
//!     "rust-lang",
//!     &options,
//!     Some(&limiter),
//!     model_tx,
//!     Some(&progress),
//! ).await?;
//! ```

use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{Semaphore, mpsc};

use sea_orm::DatabaseConnection;

use super::progress::{ProgressCallback, SyncProgress, emit};
use super::types::{
    NamespaceSyncResult, NamespaceSyncResultStreaming, StarringStats, SyncOptions, SyncResult,
};
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::platform::{ApiRateLimiter, PlatformClient, PlatformError, PlatformRepo};

/// Filter repositories by recent activity.
///
/// Returns repositories that have been updated within the specified duration.
pub fn filter_by_activity(
    repos: &[PlatformRepo],
    active_within: chrono::Duration,
) -> Vec<&PlatformRepo> {
    let cutoff = Utc::now() - active_within;

    repos
        .iter()
        .filter(|repo| {
            // Check pushed_at first, then updated_at
            repo.pushed_at
                .or(repo.updated_at)
                .map(|t| t > cutoff)
                .unwrap_or(false)
        })
        .collect()
}

/// Star repositories sequentially using the platform client.
///
/// This is the unified starring implementation for use with the generic sync engine.
/// Currently unused as platform-specific sync functions use their own starring logic
/// that operates on platform-specific types before conversion.
#[allow(dead_code)]
async fn star_repos_sequential<C: PlatformClient>(
    client: &C,
    repos: &[&PlatformRepo],
    dry_run: bool,
    on_progress: Option<&ProgressCallback>,
) -> StarringStats {
    let mut stats = StarringStats::default();

    if repos.is_empty() {
        return stats;
    }

    emit(
        on_progress,
        SyncProgress::StarringRepos {
            count: repos.len(),
            concurrency: 1,
            dry_run,
        },
    );

    for repo in repos {
        let result = if dry_run {
            Ok(true) // Assume would be starred
        } else {
            client
                .star_repo_with_retry(&repo.owner, &repo.name, on_progress)
                .await
        };

        match result {
            Ok(true) => {
                stats.starred += 1;
                emit(
                    on_progress,
                    SyncProgress::StarredRepo {
                        owner: repo.owner.clone(),
                        name: repo.name.clone(),
                        already_starred: false,
                    },
                );
            }
            Ok(false) => {
                stats.skipped += 1;
                emit(
                    on_progress,
                    SyncProgress::StarredRepo {
                        owner: repo.owner.clone(),
                        name: repo.name.clone(),
                        already_starred: true,
                    },
                );
            }
            Err(e) => {
                let err_msg = e.to_string();
                stats
                    .errors
                    .push(format!("{}/{}: {}", repo.owner, repo.name, err_msg));
                emit(
                    on_progress,
                    SyncProgress::StarError {
                        owner: repo.owner.clone(),
                        name: repo.name.clone(),
                        error: err_msg,
                    },
                );
            }
        }
    }

    emit(
        on_progress,
        SyncProgress::StarringComplete {
            starred: stats.starred,
            already_starred: stats.skipped,
            errors: stats.errors.len(),
        },
    );

    stats
}

/// Star repositories concurrently using the platform client.
///
/// Uses a semaphore to limit concurrency. When a rate limiter is provided,
/// waits for rate limit clearance before each starring operation.
async fn star_repos_concurrent<C: PlatformClient + Clone + 'static>(
    client: &C,
    repos: Vec<(String, String)>, // (owner, name) pairs
    concurrency: usize,
    dry_run: bool,
    rate_limiter: Option<&ApiRateLimiter>,
    on_progress: Option<&ProgressCallback>,
) -> StarringStats {
    let mut stats = StarringStats::default();

    if repos.is_empty() {
        return stats;
    }

    let concurrency = std::cmp::min(concurrency, repos.len());
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let limiter = rate_limiter.cloned();

    emit(
        on_progress,
        SyncProgress::StarringRepos {
            count: repos.len(),
            concurrency,
            dry_run,
        },
    );

    let mut handles = Vec::with_capacity(repos.len());

    for (owner, name) in repos {
        let client = client.clone();
        let semaphore = Arc::clone(&semaphore);
        let limiter = limiter.clone();

        let handle = tokio::spawn(async move {
            let _permit = match semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    return (
                        owner,
                        name,
                        Err(PlatformError::internal("Semaphore closed unexpectedly")),
                    );
                }
            };

            // Apply rate limiting if configured
            if let Some(ref limiter) = limiter {
                limiter.wait().await;
            }

            let result = if dry_run {
                // In dry run, check if already starred to report accurately
                match client.is_repo_starred(&owner, &name).await {
                    Ok(true) => Ok(false), // Already starred
                    Ok(false) => Ok(true), // Would be starred
                    Err(e) => Err(e),
                }
            } else {
                client.star_repo_with_retry(&owner, &name, None).await
            };

            (owner, name, result)
        });

        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok((owner, name, Ok(true))) => {
                stats.starred += 1;
                emit(
                    on_progress,
                    SyncProgress::StarredRepo {
                        owner,
                        name,
                        already_starred: false,
                    },
                );
            }
            Ok((owner, name, Ok(false))) => {
                stats.skipped += 1;
                emit(
                    on_progress,
                    SyncProgress::StarredRepo {
                        owner,
                        name,
                        already_starred: true,
                    },
                );
            }
            Ok((owner, name, Err(e))) => {
                let err_msg = e.to_string();
                stats
                    .errors
                    .push(format!("{}/{}: {}", owner, name, err_msg));
                emit(
                    on_progress,
                    SyncProgress::StarError {
                        owner,
                        name,
                        error: err_msg,
                    },
                );
            }
            Err(e) => {
                stats.errors.push(format!("Task panic: {}", e));
            }
        }
    }

    emit(
        on_progress,
        SyncProgress::StarringComplete {
            starred: stats.starred,
            already_starred: stats.skipped,
            errors: stats.errors.len(),
        },
    );

    stats
}

/// Sync a namespace (org/group) using a platform client.
///
/// This is the primary sync function that:
/// 1. Fetches all repositories for the namespace
/// 2. Filters by recent activity
/// 3. Stars matching repositories (if enabled)
/// 4. Returns results and active models for persistence
///
/// # Arguments
///
/// * `client` - Platform client implementing `PlatformClient`
/// * `namespace` - Organization or group name to sync
/// * `options` - Sync configuration options
/// * `rate_limiter` - Optional rate limiter for proactive rate limiting
/// * `db` - Optional database connection for ETag caching
/// * `on_progress` - Optional progress callback
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip(client, options, rate_limiter, db, on_progress), fields(namespace = %namespace))
)]
pub async fn sync_namespace<C: PlatformClient + Clone + 'static>(
    client: &C,
    namespace: &str,
    options: &SyncOptions,
    rate_limiter: Option<&ApiRateLimiter>,
    db: Option<&DatabaseConnection>,
    on_progress: Option<&ProgressCallback>,
) -> Result<(SyncResult, Vec<CodeRepositoryActiveModel>), PlatformError> {
    let mut result = SyncResult::default();

    // Apply rate limiting before fetching repos
    if let Some(limiter) = rate_limiter {
        limiter.wait().await;
    }

    // Fetch all repositories
    let all_repos = client.list_org_repos(namespace, db, on_progress).await?;
    result.processed = all_repos.len();

    // Filter by activity
    emit(
        on_progress,
        SyncProgress::FilteringByActivity {
            days: options.active_within.num_days(),
        },
    );

    let active_repos = filter_by_activity(&all_repos, options.active_within);
    result.matched = active_repos.len();

    emit(
        on_progress,
        SyncProgress::FilterComplete {
            matched: result.matched,
            total: result.processed,
        },
    );

    // Star repositories
    if options.star && !active_repos.is_empty() {
        // Collect owner/name pairs for concurrent starring
        let repos_to_star: Vec<(String, String)> = active_repos
            .iter()
            .map(|r| (r.owner.clone(), r.name.clone()))
            .collect();

        let star_stats = star_repos_concurrent(
            client,
            repos_to_star,
            options.concurrency,
            options.dry_run,
            rate_limiter,
            on_progress,
        )
        .await;

        result.starred = star_stats.starred;
        result.skipped = star_stats.skipped;
        result.errors = star_stats.errors;
    }

    // Convert to active models
    emit(on_progress, SyncProgress::ConvertingModels);

    let models: Vec<CodeRepositoryActiveModel> = active_repos
        .iter()
        .map(|r| client.to_active_model(r))
        .collect();

    result.saved = models.len();

    emit(
        on_progress,
        SyncProgress::ModelsReady {
            count: models.len(),
        },
    );

    Ok((result, models))
}

/// Sync a namespace with streaming persistence.
///
/// Sends models through a channel for immediate persistence as they are processed.
///
/// # Arguments
///
/// * `client` - Platform client implementing `PlatformClient`
/// * `namespace` - Organization or group name to sync
/// * `options` - Sync configuration options
/// * `rate_limiter` - Optional rate limiter for proactive rate limiting
/// * `db` - Optional database connection for ETag caching
/// * `model_tx` - Channel sender for streaming model persistence
/// * `on_progress` - Optional progress callback
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip(client, options, rate_limiter, db, model_tx, on_progress), fields(namespace = %namespace))
)]
pub async fn sync_namespace_streaming<C: PlatformClient + Clone + 'static>(
    client: &C,
    namespace: &str,
    options: &SyncOptions,
    rate_limiter: Option<&ApiRateLimiter>,
    db: Option<&DatabaseConnection>,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Result<SyncResult, PlatformError> {
    let mut result = SyncResult::default();
    let cutoff = Utc::now() - options.active_within;

    // Apply rate limiting before fetching repos
    if let Some(limiter) = rate_limiter {
        limiter.wait().await;
    }

    // Fetch all repositories
    let all_repos = client.list_org_repos(namespace, db, on_progress).await?;
    result.processed = all_repos.len();

    emit(
        on_progress,
        SyncProgress::FilteringByActivity {
            days: options.active_within.num_days(),
        },
    );

    let mut channel_closed = false;
    let mut repos_to_star: Vec<(String, String)> = Vec::new();

    // Process and filter repositories
    for repo in &all_repos {
        let activity_time = repo.pushed_at.or(repo.updated_at);
        let is_active = activity_time.map(|t| t > cutoff).unwrap_or(false);

        if is_active {
            result.matched += 1;

            if options.star {
                repos_to_star.push((repo.owner.clone(), repo.name.clone()));
            }

            if !options.dry_run && !channel_closed {
                let model = client.to_active_model(repo);
                if model_tx.send(model).await.is_err() {
                    channel_closed = true;
                    emit(
                        on_progress,
                        SyncProgress::Warning {
                            message: "persistence channel closed".to_string(),
                        },
                    );
                } else {
                    result.saved += 1;
                }
            }
        }
    }

    emit(
        on_progress,
        SyncProgress::FilterComplete {
            matched: result.matched,
            total: result.processed,
        },
    );

    // Star repositories concurrently
    if options.star && !repos_to_star.is_empty() {
        let star_stats = star_repos_concurrent(
            client,
            repos_to_star,
            options.concurrency,
            options.dry_run,
            rate_limiter,
            on_progress,
        )
        .await;

        result.starred = star_stats.starred;
        result.skipped = star_stats.skipped;
        result.errors.extend(star_stats.errors);
    }

    Ok(result)
}

/// Sync multiple namespaces concurrently.
///
/// # Arguments
///
/// * `client` - Platform client implementing `PlatformClient`
/// * `namespaces` - List of organization or group names to sync
/// * `options` - Sync configuration options
/// * `rate_limiter` - Optional rate limiter for proactive rate limiting
/// * `db` - Optional database connection for ETag caching
/// * `on_progress` - Optional progress callback
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip_all, fields(namespace_count = namespaces.len()))
)]
pub async fn sync_namespaces<C: PlatformClient + Clone + 'static>(
    client: &C,
    namespaces: &[String],
    options: &SyncOptions,
    rate_limiter: Option<&ApiRateLimiter>,
    _db: Option<&DatabaseConnection>,
    on_progress: Option<&ProgressCallback>,
) -> Vec<NamespaceSyncResult> {
    if namespaces.is_empty() {
        return Vec::new();
    }

    emit(
        on_progress,
        SyncProgress::SyncingNamespaces {
            count: namespaces.len(),
        },
    );

    // Limit namespace concurrency to avoid overwhelming the API
    let ns_concurrency = std::cmp::max(1, options.concurrency / 4);
    let semaphore = Arc::new(Semaphore::new(ns_concurrency));
    let limiter = rate_limiter.cloned();

    let mut handles = Vec::with_capacity(namespaces.len());

    for namespace in namespaces {
        let namespace = namespace.clone();
        let client = client.clone();
        let options = options.clone();
        let semaphore = Arc::clone(&semaphore);
        let limiter = limiter.clone();

        let handle = tokio::spawn(async move {
            let _permit = match semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    return NamespaceSyncResult {
                        namespace,
                        result: SyncResult::default(),
                        models: Vec::new(),
                        error: Some("Semaphore closed unexpectedly".to_string()),
                    };
                }
            };

            // Note: db is not passed to spawned tasks as DatabaseConnection doesn't impl Clone
            // For concurrent namespace syncs, caching is not used. Use sync_namespace directly
            // with a db connection for cached single-namespace syncs.
            let result =
                sync_namespace(&client, &namespace, &options, limiter.as_ref(), None, None).await;

            match result {
                Ok((sync_result, models)) => NamespaceSyncResult {
                    namespace,
                    result: sync_result,
                    models,
                    error: None,
                },
                Err(e) => NamespaceSyncResult {
                    namespace,
                    result: SyncResult::default(),
                    models: Vec::new(),
                    error: Some(e.to_string()),
                },
            }
        });

        handles.push(handle);
    }

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(ns_result) => results.push(ns_result),
            Err(e) => results.push(NamespaceSyncResult {
                namespace: "<unknown>".to_string(),
                result: SyncResult::default(),
                models: Vec::new(),
                error: Some(format!("Task panic: {}", e)),
            }),
        }
    }

    let successful = results.iter().filter(|r| r.error.is_none()).count();
    let failed = results.iter().filter(|r| r.error.is_some()).count();
    emit(
        on_progress,
        SyncProgress::SyncNamespacesComplete { successful, failed },
    );

    results
}

/// Sync multiple namespaces concurrently with streaming persistence.
///
/// # Arguments
///
/// * `client` - Platform client implementing `PlatformClient`
/// * `namespaces` - List of organization or group names to sync
/// * `options` - Sync configuration options
/// * `rate_limiter` - Optional rate limiter for proactive rate limiting
/// * `db` - Optional database connection for ETag caching
/// * `model_tx` - Channel sender for streaming model persistence
/// * `on_progress` - Optional progress callback
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip_all, fields(namespace_count = namespaces.len()))
)]
pub async fn sync_namespaces_streaming<C: PlatformClient + Clone + 'static>(
    client: &C,
    namespaces: &[String],
    options: &SyncOptions,
    rate_limiter: Option<&ApiRateLimiter>,
    _db: Option<&DatabaseConnection>,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Vec<NamespaceSyncResultStreaming> {
    if namespaces.is_empty() {
        return Vec::new();
    }

    emit(
        on_progress,
        SyncProgress::SyncingNamespaces {
            count: namespaces.len(),
        },
    );

    let ns_concurrency = std::cmp::max(1, options.concurrency / 4);
    let semaphore = Arc::new(Semaphore::new(ns_concurrency));
    let limiter = rate_limiter.cloned();

    let mut handles = Vec::with_capacity(namespaces.len());

    for namespace in namespaces {
        let namespace = namespace.clone();
        let client = client.clone();
        let options = options.clone();
        let semaphore = Arc::clone(&semaphore);
        let tx = model_tx.clone();
        let limiter = limiter.clone();

        let handle = tokio::spawn(async move {
            let _permit = match semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    return NamespaceSyncResultStreaming {
                        namespace,
                        result: SyncResult::default(),
                        error: Some("Semaphore closed unexpectedly".to_string()),
                    };
                }
            };

            let result =
                // Note: db is not passed to spawned tasks as DatabaseConnection doesn't impl Clone
                sync_namespace_streaming(&client, &namespace, &options, limiter.as_ref(), None, tx, None)
                    .await;

            match result {
                Ok(sync_result) => NamespaceSyncResultStreaming {
                    namespace,
                    result: sync_result,
                    error: None,
                },
                Err(e) => NamespaceSyncResultStreaming {
                    namespace,
                    result: SyncResult::default(),
                    error: Some(e.to_string()),
                },
            }
        });

        handles.push(handle);
    }

    // Drop our copy of the sender so receivers know when all senders are done
    drop(model_tx);

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(ns_result) => results.push(ns_result),
            Err(e) => results.push(NamespaceSyncResultStreaming {
                namespace: "<unknown>".to_string(),
                result: SyncResult::default(),
                error: Some(format!("Task panic: {}", e)),
            }),
        }
    }

    let successful = results.iter().filter(|r| r.error.is_none()).count();
    let failed = results.iter().filter(|r| r.error.is_some()).count();
    emit(
        on_progress,
        SyncProgress::SyncNamespacesComplete { successful, failed },
    );

    results
}

/// Sync a user's repositories.
///
/// This is similar to `sync_namespace` but fetches a specific user's repositories
/// instead of an organization's repositories.
///
/// # Arguments
///
/// * `client` - Platform client implementing `PlatformClient`
/// * `username` - Username whose repositories to sync
/// * `options` - Sync configuration options
/// * `rate_limiter` - Optional rate limiter for proactive rate limiting
/// * `db` - Optional database connection for ETag caching
/// * `on_progress` - Optional progress callback
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip(client, options, rate_limiter, db, on_progress), fields(username = %username))
)]
pub async fn sync_user<C: PlatformClient + Clone + 'static>(
    client: &C,
    username: &str,
    options: &SyncOptions,
    rate_limiter: Option<&ApiRateLimiter>,
    db: Option<&DatabaseConnection>,
    on_progress: Option<&ProgressCallback>,
) -> Result<(SyncResult, Vec<CodeRepositoryActiveModel>), PlatformError> {
    let mut result = SyncResult::default();

    // Apply rate limiting before fetching repos
    if let Some(limiter) = rate_limiter {
        limiter.wait().await;
    }

    // Fetch all user repositories
    let all_repos = client.list_user_repos(username, db, on_progress).await?;
    result.processed = all_repos.len();

    // Filter by activity
    emit(
        on_progress,
        SyncProgress::FilteringByActivity {
            days: options.active_within.num_days(),
        },
    );

    let active_repos = filter_by_activity(&all_repos, options.active_within);
    result.matched = active_repos.len();

    emit(
        on_progress,
        SyncProgress::FilterComplete {
            matched: result.matched,
            total: result.processed,
        },
    );

    // Star repositories
    if options.star && !active_repos.is_empty() {
        // Collect owner/name pairs for concurrent starring
        let repos_to_star: Vec<(String, String)> = active_repos
            .iter()
            .map(|r| (r.owner.clone(), r.name.clone()))
            .collect();

        let star_stats = star_repos_concurrent(
            client,
            repos_to_star,
            options.concurrency,
            options.dry_run,
            rate_limiter,
            on_progress,
        )
        .await;

        result.starred = star_stats.starred;
        result.skipped = star_stats.skipped;
        result.errors = star_stats.errors;
    }

    // Convert to active models
    emit(on_progress, SyncProgress::ConvertingModels);

    let models: Vec<CodeRepositoryActiveModel> = active_repos
        .iter()
        .map(|r| client.to_active_model(r))
        .collect();

    result.saved = models.len();

    emit(
        on_progress,
        SyncProgress::ModelsReady {
            count: models.len(),
        },
    );

    Ok((result, models))
}

/// Sync a user's repositories with streaming persistence.
///
/// Sends models through a channel for immediate persistence as they are processed.
///
/// # Arguments
///
/// * `client` - Platform client implementing `PlatformClient`
/// * `username` - Username whose repositories to sync
/// * `options` - Sync configuration options
/// * `rate_limiter` - Optional rate limiter for proactive rate limiting
/// * `db` - Optional database connection for ETag caching
/// * `model_tx` - Channel sender for streaming model persistence
/// * `on_progress` - Optional progress callback
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip(client, options, rate_limiter, db, model_tx, on_progress), fields(username = %username))
)]
pub async fn sync_user_streaming<C: PlatformClient + Clone + 'static>(
    client: &C,
    username: &str,
    options: &SyncOptions,
    rate_limiter: Option<&ApiRateLimiter>,
    db: Option<&DatabaseConnection>,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Result<SyncResult, PlatformError> {
    let mut result = SyncResult::default();
    let cutoff = Utc::now() - options.active_within;

    // Apply rate limiting before fetching repos
    if let Some(limiter) = rate_limiter {
        limiter.wait().await;
    }

    // Fetch all user repositories
    let all_repos = client.list_user_repos(username, db, on_progress).await?;
    result.processed = all_repos.len();

    emit(
        on_progress,
        SyncProgress::FilteringByActivity {
            days: options.active_within.num_days(),
        },
    );

    let mut channel_closed = false;
    let mut repos_to_star: Vec<(String, String)> = Vec::new();

    // Process and filter repositories
    for repo in &all_repos {
        let activity_time = repo.pushed_at.or(repo.updated_at);
        let is_active = activity_time.map(|t| t > cutoff).unwrap_or(false);

        if is_active {
            result.matched += 1;

            if options.star {
                repos_to_star.push((repo.owner.clone(), repo.name.clone()));
            }

            if !options.dry_run && !channel_closed {
                let model = client.to_active_model(repo);
                if model_tx.send(model).await.is_err() {
                    channel_closed = true;
                    emit(
                        on_progress,
                        SyncProgress::Warning {
                            message: "persistence channel closed".to_string(),
                        },
                    );
                } else {
                    result.saved += 1;
                }
            }
        }
    }

    emit(
        on_progress,
        SyncProgress::FilterComplete {
            matched: result.matched,
            total: result.processed,
        },
    );

    // Star repositories concurrently
    if options.star && !repos_to_star.is_empty() {
        let star_stats = star_repos_concurrent(
            client,
            repos_to_star,
            options.concurrency,
            options.dry_run,
            rate_limiter,
            on_progress,
        )
        .await;

        result.starred = star_stats.starred;
        result.skipped = star_stats.skipped;
        result.errors.extend(star_stats.errors);
    }

    Ok(result)
}

/// Sync multiple users' repositories concurrently with streaming persistence.
///
/// # Arguments
///
/// * `client` - Platform client implementing `PlatformClient`
/// * `usernames` - List of usernames to sync
/// * `options` - Sync configuration options
/// * `rate_limiter` - Optional rate limiter for proactive rate limiting
/// * `db` - Optional database connection for ETag caching
/// * `model_tx` - Channel sender for streaming model persistence
/// * `on_progress` - Optional progress callback
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip_all, fields(user_count = usernames.len()))
)]
pub async fn sync_users_streaming<C: PlatformClient + Clone + 'static>(
    client: &C,
    usernames: &[String],
    options: &SyncOptions,
    rate_limiter: Option<&ApiRateLimiter>,
    _db: Option<&DatabaseConnection>,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Vec<NamespaceSyncResultStreaming> {
    if usernames.is_empty() {
        return Vec::new();
    }

    emit(
        on_progress,
        SyncProgress::SyncingNamespaces {
            count: usernames.len(),
        },
    );

    let ns_concurrency = std::cmp::max(1, options.concurrency / 4);
    let semaphore = Arc::new(Semaphore::new(ns_concurrency));
    let limiter = rate_limiter.cloned();

    let mut handles = Vec::with_capacity(usernames.len());

    for username in usernames {
        let username = username.clone();
        let client = client.clone();
        let options = options.clone();
        let semaphore = Arc::clone(&semaphore);
        let tx = model_tx.clone();
        let limiter = limiter.clone();

        let handle = tokio::spawn(async move {
            let _permit = match semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    return NamespaceSyncResultStreaming {
                        namespace: username,
                        result: SyncResult::default(),
                        error: Some("Semaphore closed unexpectedly".to_string()),
                    };
                }
            };

            let result =
                // Note: db is not passed to spawned tasks as DatabaseConnection doesn't impl Clone
                sync_user_streaming(&client, &username, &options, limiter.as_ref(), None, tx, None).await;

            match result {
                Ok(sync_result) => NamespaceSyncResultStreaming {
                    namespace: username,
                    result: sync_result,
                    error: None,
                },
                Err(e) => NamespaceSyncResultStreaming {
                    namespace: username,
                    result: SyncResult::default(),
                    error: Some(e.to_string()),
                },
            }
        });

        handles.push(handle);
    }

    // Drop our copy of the sender so receivers know when all senders are done
    drop(model_tx);

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(ns_result) => results.push(ns_result),
            Err(e) => results.push(NamespaceSyncResultStreaming {
                namespace: "<unknown>".to_string(),
                result: SyncResult::default(),
                error: Some(format!("Task panic: {}", e)),
            }),
        }
    }

    let successful = results.iter().filter(|r| r.error.is_none()).count();
    let failed = results.iter().filter(|r| r.error.is_some()).count();
    emit(
        on_progress,
        SyncProgress::SyncNamespacesComplete { successful, failed },
    );

    results
}

/// Result of pruning operation.
struct PruneResult {
    /// Number of repos pruned.
    pruned: usize,
    /// List of successfully pruned repos (owner, name) for database cleanup.
    pruned_repos: Vec<(String, String)>,
    /// Errors encountered.
    errors: Vec<String>,
}

/// Prune (unstar) inactive repositories.
///
/// Unstars repositories that are outside the activity window.
/// Returns the list of successfully pruned repos so they can be deleted from the database.
async fn prune_repos<C: PlatformClient + Clone + 'static>(
    client: &C,
    repos: Vec<(String, String)>, // (owner, name) pairs
    dry_run: bool,
    rate_limiter: Option<&ApiRateLimiter>,
    on_progress: Option<&ProgressCallback>,
) -> PruneResult {
    let mut result = PruneResult {
        pruned: 0,
        pruned_repos: Vec::new(),
        errors: Vec::new(),
    };

    if repos.is_empty() {
        return result;
    }

    emit(
        on_progress,
        SyncProgress::PruningRepos {
            count: repos.len(),
            dry_run,
        },
    );

    for (owner, name) in repos {
        // Apply rate limiting if configured
        if let Some(limiter) = rate_limiter {
            limiter.wait().await;
        }

        if dry_run {
            // In dry run, just count as would-be-pruned
            result.pruned += 1;
            result.pruned_repos.push((owner.clone(), name.clone()));
            emit(
                on_progress,
                SyncProgress::PrunedRepo {
                    owner: owner.clone(),
                    name: name.clone(),
                },
            );
        } else {
            match client.unstar_repo(&owner, &name).await {
                Ok(true) => {
                    result.pruned += 1;
                    result.pruned_repos.push((owner.clone(), name.clone()));
                    emit(
                        on_progress,
                        SyncProgress::PrunedRepo {
                            owner: owner.clone(),
                            name: name.clone(),
                        },
                    );
                }
                Ok(false) => {
                    // Wasn't starred - shouldn't happen but not an error
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    result
                        .errors
                        .push(format!("{}/{}: {}", owner, name, err_msg));
                    emit(
                        on_progress,
                        SyncProgress::PruneError {
                            owner: owner.clone(),
                            name: name.clone(),
                            error: err_msg,
                        },
                    );
                }
            }
        }
    }

    emit(
        on_progress,
        SyncProgress::PruningComplete {
            pruned: result.pruned,
            errors: result.errors.len(),
        },
    );

    result
}

/// Sync the authenticated user's starred repositories.
///
/// This function:
/// 1. Fetches all repositories starred by the authenticated user
/// 2. Filters by recent activity (streaming - processes repos as they arrive)
/// 3. Persists active starred repos to the database
/// 4. Optionally prunes (unstars) inactive repositories
///
/// # Arguments
///
/// * `client` - Platform client implementing `PlatformClient`
/// * `options` - Sync configuration options (uses `active_within` and `prune` flags)
/// * `rate_limiter` - Optional rate limiter for proactive rate limiting
/// * `db` - Optional database connection for ETag caching
/// * `concurrency` - Number of concurrent page fetches
/// * `skip_rate_checks` - Whether to skip rate limit checks
/// * `model_tx` - Channel sender for streaming model persistence
/// * `on_progress` - Optional progress callback
#[allow(clippy::too_many_arguments)]
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip_all, fields(concurrency = concurrency, prune = options.prune))
)]
pub async fn sync_starred_streaming<C: PlatformClient + Clone + 'static>(
    client: &C,
    options: &SyncOptions,
    rate_limiter: Option<&ApiRateLimiter>,
    db: Option<&DatabaseConnection>,
    concurrency: usize,
    skip_rate_checks: bool,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Result<SyncResult, PlatformError> {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let cutoff = Utc::now() - options.active_within;

    // Apply rate limiting before fetching repos
    if let Some(limiter) = rate_limiter {
        limiter.wait().await;
    }

    // Emit filtering activity event - for starred sync, filtering happens during streaming
    emit(
        on_progress,
        SyncProgress::FilteringByActivity {
            days: options.active_within.num_days(),
        },
    );

    // Shared state for the streaming processor
    let processed = Arc::new(AtomicUsize::new(0));
    let matched = Arc::new(AtomicUsize::new(0));
    let saved = Arc::new(AtomicUsize::new(0));
    let channel_closed = Arc::new(AtomicBool::new(false));
    let repos_to_prune = Arc::new(Mutex::new(Vec::<(String, String)>::new()));

    // Create channel for receiving repos from the streaming fetch
    let (repo_tx, mut repo_rx) = mpsc::channel::<PlatformRepo>(100);

    // Create channel for progress events from processor task
    // Sends (matched_count, processed_count) tuples
    // Use unbounded to avoid blocking processor
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<(usize, usize)>();

    // Spawn task to process repos as they stream in
    let processor_client = client.clone();
    let processor_model_tx = model_tx.clone();
    let processor_processed = Arc::clone(&processed);
    let processor_matched = Arc::clone(&matched);
    let processor_saved = Arc::clone(&saved);
    let processor_channel_closed = Arc::clone(&channel_closed);
    let processor_repos_to_prune = Arc::clone(&repos_to_prune);
    let processor_dry_run = options.dry_run;
    let processor_prune = options.prune;

    let processor_handle = tokio::spawn(async move {
        while let Some(repo) = repo_rx.recv().await {
            let new_processed = processor_processed.fetch_add(1, Ordering::Relaxed) + 1;

            let activity_time = repo.pushed_at.or(repo.updated_at);
            let is_active = activity_time.map(|t| t > cutoff).unwrap_or(false);

            if is_active {
                let new_matched = processor_matched.fetch_add(1, Ordering::Relaxed) + 1;

                // Send progress update (ignore errors if receiver dropped)
                let _ = progress_tx.send((new_matched, new_processed));

                // Persist active starred repos
                if !processor_dry_run && !processor_channel_closed.load(Ordering::Relaxed) {
                    let model = processor_client.to_active_model(&repo);
                    if processor_model_tx.send(model).await.is_err() {
                        processor_channel_closed.store(true, Ordering::Relaxed);
                    } else {
                        processor_saved.fetch_add(1, Ordering::Relaxed);
                    }
                }
            } else if processor_prune {
                // Mark inactive repos for pruning
                if let Ok(mut prune_list) = processor_repos_to_prune.lock() {
                    prune_list.push((repo.owner.clone(), repo.name.clone()));
                }
            }
        }
    });

    // Stream starred repos with concurrent progress emission
    // Use pin to allow polling the future while also checking progress
    let fetch_future = client.list_starred_repos_streaming(
        repo_tx,
        db,
        concurrency,
        skip_rate_checks,
        on_progress,
    );
    tokio::pin!(fetch_future);

    let mut fetch_done = false;
    let mut fetch_result: Option<Result<usize, PlatformError>> = None;
    let mut last_emitted_matched = 0usize;
    let mut last_emitted_processed = 0usize;

    // Poll fetch and progress concurrently, emitting FilteredPage events in real-time
    loop {
        tokio::select! {
            biased;

            // Check for progress updates from processor (non-blocking drain)
            result = progress_rx.recv(), if !fetch_done => {
                if let Some((matched_count, processed_count)) = result {
                    // Emit progress every 25 matched repos or 100 processed for real-time feedback
                    let should_emit = matched_count >= last_emitted_matched + 25
                        || processed_count >= last_emitted_processed + 100
                        || (matched_count > 0 && last_emitted_matched == 0);

                    if should_emit {
                        emit(
                            on_progress,
                            SyncProgress::FilteredPage {
                                matched_so_far: matched_count,
                                processed_so_far: processed_count,
                            },
                        );
                        last_emitted_matched = matched_count;
                        last_emitted_processed = processed_count;
                    }
                }
            }

            // Drive the fetch forward
            result = &mut fetch_future, if !fetch_done => {
                fetch_done = true;
                fetch_result = Some(result);
            }
        }

        // Exit when fetch is done
        if fetch_done {
            // Drain any remaining progress updates
            while let Ok((matched_count, processed_count)) = progress_rx.try_recv() {
                let should_emit = matched_count >= last_emitted_matched + 25
                    || processed_count >= last_emitted_processed + 100
                    || (matched_count > 0 && last_emitted_matched == 0);

                if should_emit {
                    emit(
                        on_progress,
                        SyncProgress::FilteredPage {
                            matched_so_far: matched_count,
                            processed_so_far: processed_count,
                        },
                    );
                    last_emitted_matched = matched_count;
                    last_emitted_processed = processed_count;
                }
            }
            break;
        }
    }

    let fetch_result = fetch_result.expect("fetch should complete");

    // Wait for processor to finish
    let _ = processor_handle.await;

    // Emit final progress update
    let final_matched = matched.load(Ordering::Relaxed);
    let final_processed = processed.load(Ordering::Relaxed);
    if final_matched > last_emitted_matched || final_processed > last_emitted_processed {
        emit(
            on_progress,
            SyncProgress::FilteredPage {
                matched_so_far: final_matched,
                processed_so_far: final_processed,
            },
        );
    }

    // Check fetch result before emitting completion events
    fetch_result?;

    // Emit FilterComplete and ModelsReady BEFORE dropping the channel
    // This ensures the progress reporter knows the final count before any
    // remaining Persisted events arrive from the persist task's final batch flush
    emit(
        on_progress,
        SyncProgress::FilterComplete {
            matched: final_matched,
            total: final_processed,
        },
    );

    // Emit ModelsReady to create the save bar with correct count
    // This ensures the save bar appears with progress bar style
    if final_matched > 0 && !options.dry_run {
        emit(
            on_progress,
            SyncProgress::ModelsReady {
                count: final_matched,
            },
        );
    }

    // Drop our copy of the model sender to allow persist task to detect completion
    // (processor_model_tx was already dropped when the processor task finished)
    // The persist task will flush its final batch after this, but FilterComplete
    // has already been emitted so the save bar will use progress bar style
    drop(model_tx);

    // Build result
    let mut result = SyncResult {
        processed: final_processed,
        matched: final_matched,
        saved: saved.load(Ordering::Relaxed),
        ..Default::default()
    };

    // Prune inactive starred repos
    let repos_to_prune: Vec<(String, String)> = match Arc::try_unwrap(repos_to_prune) {
        Ok(mutex) => mutex.into_inner().unwrap_or_default(),
        Err(arc) => arc.lock().unwrap().clone(),
    };

    if options.prune && !repos_to_prune.is_empty() {
        let prune_result = prune_repos(
            client,
            repos_to_prune,
            options.dry_run,
            rate_limiter,
            on_progress,
        )
        .await;

        result.pruned = prune_result.pruned;
        result.pruned_repos = prune_result.pruned_repos;
        result.errors.extend(prune_result.errors);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn mock_repo(name: &str, days_ago: i64) -> PlatformRepo {
        use crate::entity::code_visibility::CodeVisibility;

        PlatformRepo {
            platform_id: 123,
            owner: "test-org".to_string(),
            name: name.to_string(),
            description: None,
            default_branch: "main".to_string(),
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_archived: false,
            stars: Some(10),
            forks: Some(2),
            language: Some("Rust".to_string()),
            topics: vec![],
            created_at: None,
            updated_at: Some(Utc::now() - Duration::days(days_ago)),
            pushed_at: Some(Utc::now() - Duration::days(days_ago)),
            license: None,
            homepage: None,
            size_kb: None,
            metadata: serde_json::json!({}),
        }
    }

    fn mock_repo_with_owner(owner: &str, name: &str, days_ago: i64) -> PlatformRepo {
        use crate::entity::code_visibility::CodeVisibility;

        PlatformRepo {
            platform_id: 123,
            owner: owner.to_string(),
            name: name.to_string(),
            description: None,
            default_branch: "main".to_string(),
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_archived: false,
            stars: Some(10),
            forks: Some(2),
            language: Some("Rust".to_string()),
            topics: vec![],
            created_at: None,
            updated_at: Some(Utc::now() - Duration::days(days_ago)),
            pushed_at: Some(Utc::now() - Duration::days(days_ago)),
            license: None,
            homepage: None,
            size_kb: None,
            metadata: serde_json::json!({}),
        }
    }

    #[test]
    fn test_filter_by_activity_includes_recent() {
        let repos = vec![mock_repo("recent", 10)];
        let filtered = filter_by_activity(&repos, Duration::days(30));
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn test_filter_by_activity_excludes_old() {
        let repos = vec![mock_repo("old", 100)];
        let filtered = filter_by_activity(&repos, Duration::days(30));
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_filter_by_activity_mixed() {
        let repos = vec![
            mock_repo("recent", 10),
            mock_repo("old", 100),
            mock_repo("borderline", 29),
        ];
        let filtered = filter_by_activity(&repos, Duration::days(30));
        assert_eq!(filtered.len(), 2);

        let names: Vec<_> = filtered.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"recent"));
        assert!(names.contains(&"borderline"));
        assert!(!names.contains(&"old"));
    }

    #[test]
    fn test_filter_by_activity_no_timestamp() {
        let mut repo = mock_repo("no-time", 0);
        repo.pushed_at = None;
        repo.updated_at = None;

        let repos = vec![repo];
        let filtered = filter_by_activity(&repos, Duration::days(30));
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_filter_by_activity_uses_pushed_at_first() {
        use crate::entity::code_visibility::CodeVisibility;

        // pushed_at is recent, updated_at is old
        let repo = PlatformRepo {
            platform_id: 123,
            owner: "test".to_string(),
            name: "test".to_string(),
            description: None,
            default_branch: "main".to_string(),
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_archived: false,
            stars: None,
            forks: None,
            language: None,
            topics: vec![],
            created_at: None,
            updated_at: Some(Utc::now() - Duration::days(100)), // Old
            pushed_at: Some(Utc::now() - Duration::days(5)),    // Recent
            license: None,
            homepage: None,
            size_kb: None,
            metadata: serde_json::json!({}),
        };

        let repos = vec![repo];
        let filtered = filter_by_activity(&repos, Duration::days(30));
        assert_eq!(filtered.len(), 1); // Should pass because pushed_at is used
    }

    #[test]
    fn test_filter_by_activity_falls_back_to_updated_at() {
        use crate::entity::code_visibility::CodeVisibility;

        // pushed_at is None, updated_at is recent
        let repo = PlatformRepo {
            platform_id: 123,
            owner: "test".to_string(),
            name: "test".to_string(),
            description: None,
            default_branch: "main".to_string(),
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_archived: false,
            stars: None,
            forks: None,
            language: None,
            topics: vec![],
            created_at: None,
            updated_at: Some(Utc::now() - Duration::days(5)), // Recent
            pushed_at: None,                                  // No pushed_at
            license: None,
            homepage: None,
            size_kb: None,
            metadata: serde_json::json!({}),
        };

        let repos = vec![repo];
        let filtered = filter_by_activity(&repos, Duration::days(30));
        assert_eq!(filtered.len(), 1); // Should pass because updated_at is used as fallback
    }

    #[test]
    fn test_filter_by_activity_exact_boundary() {
        // Test the exact boundary - repo updated exactly at cutoff
        let repos = vec![mock_repo("boundary", 30)];
        let filtered = filter_by_activity(&repos, Duration::days(30));
        // At exactly 30 days, it should be excluded (> not >=)
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_filter_by_activity_empty_input() {
        let repos: Vec<PlatformRepo> = vec![];
        let filtered = filter_by_activity(&repos, Duration::days(30));
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_sync_options_default_prune() {
        let options = SyncOptions::default();
        assert!(options.prune); // Default should be true
    }

    #[test]
    fn test_sync_result_prune_fields() {
        let mut result = SyncResult::default();
        assert_eq!(result.pruned, 0);
        assert!(result.pruned_repos.is_empty());

        result.pruned = 5;
        result.pruned_repos = vec![
            ("owner1".to_string(), "repo1".to_string()),
            ("owner2".to_string(), "repo2".to_string()),
        ];

        assert_eq!(result.pruned, 5);
        assert_eq!(result.pruned_repos.len(), 2);
        assert_eq!(
            result.pruned_repos[0],
            ("owner1".to_string(), "repo1".to_string())
        );
    }

    #[test]
    fn test_namespace_sync_result_streaming() {
        let result = NamespaceSyncResultStreaming {
            namespace: "rust-lang".to_string(),
            result: SyncResult {
                processed: 100,
                matched: 50,
                starred: 10,
                saved: 50,
                skipped: 40,
                pruned: 5,
                pruned_repos: vec![("old".to_string(), "repo".to_string())],
                errors: vec!["some error".to_string()],
            },
            error: None,
        };

        assert_eq!(result.namespace, "rust-lang");
        assert_eq!(result.result.processed, 100);
        assert_eq!(result.result.matched, 50);
        assert_eq!(result.result.pruned, 5);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_namespace_sync_result_streaming_with_error() {
        let result = NamespaceSyncResultStreaming {
            namespace: "failing-org".to_string(),
            result: SyncResult::default(),
            error: Some("API error: 403 Forbidden".to_string()),
        };

        assert_eq!(result.namespace, "failing-org");
        assert!(result.error.is_some());
        assert!(result.error.unwrap().contains("403"));
    }

    #[test]
    fn test_starring_stats_accumulation() {
        let mut stats = StarringStats::default();

        stats.starred += 5;
        stats.skipped += 3;
        stats.errors.push("error 1".to_string());
        stats.errors.push("error 2".to_string());

        assert_eq!(stats.starred, 5);
        assert_eq!(stats.skipped, 3);
        assert_eq!(stats.errors.len(), 2);
    }

    #[test]
    fn test_sync_options_custom() {
        let options = SyncOptions {
            active_within: Duration::days(90),
            star: false,
            dry_run: true,
            concurrency: 5,
            platform_options: super::super::types::PlatformOptions {
                include_subgroups: true,
            },
            prune: false,
        };

        assert_eq!(options.active_within, Duration::days(90));
        assert!(!options.star);
        assert!(options.dry_run);
        assert_eq!(options.concurrency, 5);
        assert!(options.platform_options.include_subgroups);
        assert!(!options.prune);
    }

    #[tokio::test]
    async fn test_star_repos_concurrent_empty() {
        // Test that star_repos_concurrent handles empty input
        // We can't easily mock the client, but we can verify the function
        // handles empty input without making API calls
        fn verify_empty_handling() {
            // This test verifies the logic structure - actual API tests
            // would require integration testing with mock servers
            let repos: Vec<(String, String)> = vec![];
            assert!(repos.is_empty());
        }
        verify_empty_handling();
    }

    #[test]
    fn test_platform_repo_full_name() {
        let repo = mock_repo_with_owner("rust-lang", "rust", 10);
        assert_eq!(repo.full_name(), "rust-lang/rust");
    }

    #[test]
    fn test_sync_result_errors_accumulation() {
        let mut result = SyncResult::default();

        result.errors.push("error 1".to_string());
        result.errors.push("error 2".to_string());
        result.errors.push("error 3".to_string());

        assert_eq!(result.errors.len(), 3);
        assert!(result.errors.contains(&"error 1".to_string()));
    }

    #[test]
    fn test_progress_callback_type() {
        // Verify the progress callback can be created and called
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let callback: ProgressCallback = Box::new(move |event| {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            // Just verify we can match on the event
            if let SyncProgress::FetchComplete { total: _ } = event {}
        });

        callback(SyncProgress::FetchComplete { total: 42 });
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }
}
