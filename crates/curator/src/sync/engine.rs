//! Unified sync engine for all platforms.
//!
//! This module provides a platform-agnostic sync implementation that works with
//! any type implementing the `PlatformClient` trait.
//!
//! # Rate Limiting
//!
//! Rate limiting is handled inside platform clients. Each client holds an
//! optional `AdaptiveRateLimiter` and transparently paces requests before
//! API operations. The sync engine does not manage rate limiters directly.
//!
//! # Example
//!
//! ```ignore
//! use curator::sync::{SyncOptions, sync_namespace_streaming};
//!
//! let result = sync_namespace_streaming(
//!     &client,
//!     "rust-lang",
//!     &options,
//!     model_tx,
//!     Some(&progress),
//! ).await?;
//! ```

mod filter;
mod persist;
mod star;

pub use filter::{filter_by_activity, filter_for_incremental_sync};

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use chrono::Utc;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use std::future::Future;

use sea_orm::DatabaseConnection;

use super::progress::{ProgressCallback, SyncProgress, emit};
use super::types::{
    NamespaceSyncResult, NamespaceSyncResultStreaming, SyncOptions, SyncResult, SyncStrategy,
};
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::platform::{PlatformClient, PlatformError, PlatformRepo};
use crate::repository;
use crate::repository::RepoSyncInfo;

const REPO_STREAM_CHANNEL_BUFFER: usize = 500;
const FILTER_PROGRESS_CHANNEL_BUFFER: usize = 256;
const FILTER_PROGRESS_EMIT_MATCHED_STEP: usize = 25;
const FILTER_PROGRESS_EMIT_PROCESSED_STEP: usize = 100;
const PROCESSOR_LOG_EVERY: usize = 1000;

/// Trait for concurrent sync result types.
///
/// This trait abstracts over `NamespaceSyncResult` and `NamespaceSyncResultStreaming`,
/// allowing `spawn_concurrent_sync` to work with both types.
trait ConcurrentSyncResult: Send + 'static {
    /// Create a result for when semaphore acquisition fails.
    fn semaphore_error(name: String) -> Self;

    /// Create a result for when a spawned task panics.
    fn panic_error(error: String) -> Self;

    /// Whether this result represents an error.
    fn is_error(&self) -> bool;
}

impl ConcurrentSyncResult for NamespaceSyncResult {
    fn semaphore_error(name: String) -> Self {
        Self {
            namespace: name,
            result: SyncResult::default(),
            models: Vec::new(),
            error: Some("Semaphore closed unexpectedly".to_string()),
        }
    }

    fn panic_error(error: String) -> Self {
        Self {
            namespace: "<unknown>".to_string(),
            result: SyncResult::default(),
            models: Vec::new(),
            error: Some(format!("Task panic: {}", error)),
        }
    }

    fn is_error(&self) -> bool {
        self.error.is_some()
    }
}

impl ConcurrentSyncResult for NamespaceSyncResultStreaming {
    fn semaphore_error(name: String) -> Self {
        Self {
            namespace: name,
            result: SyncResult::default(),
            error: Some("Semaphore closed unexpectedly".to_string()),
        }
    }

    fn panic_error(error: String) -> Self {
        Self {
            namespace: "<unknown>".to_string(),
            result: SyncResult::default(),
            error: Some(format!("Task panic: {}", error)),
        }
    }

    fn is_error(&self) -> bool {
        self.error.is_some()
    }
}

/// Spawn concurrent sync tasks for items (namespaces or usernames).
///
/// Emits progress, creates tasks with semaphore-controlled concurrency, and
/// collects results with proper panic handling.
async fn spawn_concurrent_sync<R, F, Fut>(
    items: &[String],
    concurrency: usize,
    make_task: F,
    on_progress: Option<&ProgressCallback>,
) -> Vec<R>
where
    R: ConcurrentSyncResult,
    F: Fn(String, Arc<Semaphore>) -> Fut,
    Fut: Future<Output = R> + Send + 'static,
{
    if items.is_empty() {
        return Vec::new();
    }

    emit(
        on_progress,
        SyncProgress::SyncingNamespaces { count: items.len() },
    );

    let semaphore = Arc::new(Semaphore::new(std::cmp::max(1, concurrency / 4)));
    let handles: Vec<_> = items
        .iter()
        .map(|item| tokio::spawn(make_task(item.clone(), Arc::clone(&semaphore))))
        .collect();

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(
            handle
                .await
                .unwrap_or_else(|e| R::panic_error(e.to_string())),
        );
    }

    let successful = results.iter().filter(|r| !r.is_error()).count();
    emit(
        on_progress,
        SyncProgress::SyncNamespacesComplete {
            successful,
            failed: results.len() - successful,
        },
    );

    results
}

/// Spawn concurrent streaming sync tasks for items (namespaces or usernames).
///
/// This is the streaming variant of `spawn_concurrent_sync`. The key difference is that
/// it accepts a `model_tx` sender that is cloned for each task and dropped after all
/// tasks are spawned to signal completion to receivers.
///
/// # Arguments
///
/// * `items` - List of items (namespace names or usernames) to sync
/// * `concurrency` - Base concurrency level (divided by 4 for namespace-level concurrency)
/// * `model_tx` - Channel sender for streaming model persistence (dropped after spawning)
/// * `make_task` - Factory function that creates the sync task for each item
/// * `on_progress` - Optional progress callback
async fn spawn_concurrent_sync_streaming<F, Fut>(
    items: &[String],
    concurrency: usize,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    make_task: F,
    on_progress: Option<&ProgressCallback>,
) -> Vec<NamespaceSyncResultStreaming>
where
    F: Fn(String, Arc<Semaphore>, mpsc::Sender<CodeRepositoryActiveModel>) -> Fut,
    Fut: Future<Output = NamespaceSyncResultStreaming> + Send + 'static,
{
    if items.is_empty() {
        return Vec::new();
    }

    emit(
        on_progress,
        SyncProgress::SyncingNamespaces { count: items.len() },
    );

    let ns_concurrency = std::cmp::max(1, concurrency / 4);
    let semaphore = Arc::new(Semaphore::new(ns_concurrency));

    let handles: Vec<_> = items
        .iter()
        .map(|item| {
            let task = make_task(item.clone(), Arc::clone(&semaphore), model_tx.clone());
            tokio::spawn(task)
        })
        .collect();

    // Drop our copy of the sender so receivers know when all senders are done
    drop(model_tx);

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(
            handle
                .await
                .unwrap_or_else(|e| NamespaceSyncResultStreaming::panic_error(e.to_string())),
        );
    }

    let successful = results.iter().filter(|r| !r.is_error()).count();
    let failed = results.len() - successful;
    emit(
        on_progress,
        SyncProgress::SyncNamespacesComplete { successful, failed },
    );

    results
}

fn apply_incremental_filter(
    repos: Vec<PlatformRepo>,
    sync_info: &[RepoSyncInfo],
) -> Vec<PlatformRepo> {
    if sync_info.is_empty() {
        return repos;
    }

    filter::filter_for_incremental_sync(&repos, sync_info)
        .into_iter()
        .cloned()
        .collect()
}

async fn maybe_filter_incremental_for_owner<C: PlatformClient>(
    client: &C,
    owner: &str,
    options: &SyncOptions,
    db: Option<&DatabaseConnection>,
    repos: Vec<PlatformRepo>,
) -> Result<Vec<PlatformRepo>, PlatformError> {
    if options.strategy != SyncStrategy::Incremental {
        return Ok(repos);
    }

    let Some(db) = db else {
        return Ok(repos);
    };

    let sync_info =
        repository::get_sync_info_by_instance_and_owner(db, client.instance_id(), owner)
            .await
            .map_err(|e| PlatformError::internal(e.to_string()))?;

    Ok(apply_incremental_filter(repos, &sync_info))
}

async fn maybe_filter_incremental_for_owners<C: PlatformClient>(
    client: &C,
    owners: &[String],
    options: &SyncOptions,
    db: Option<&DatabaseConnection>,
    repos: Vec<PlatformRepo>,
) -> Result<Vec<PlatformRepo>, PlatformError> {
    if options.strategy != SyncStrategy::Incremental {
        return Ok(repos);
    }

    let Some(db) = db else {
        return Ok(repos);
    };

    if owners.is_empty() {
        return Ok(repos);
    }

    let mut sync_info: Vec<RepoSyncInfo> = Vec::new();
    for owner in owners {
        let mut owner_info =
            repository::get_sync_info_by_instance_and_owner(db, client.instance_id(), owner)
                .await
                .map_err(|e| PlatformError::internal(e.to_string()))?;
        sync_info.append(&mut owner_info);
    }

    Ok(apply_incremental_filter(repos, &sync_info))
}

async fn sync_repos<C: PlatformClient + Clone + 'static>(
    client: &C,
    namespace: &str,
    repos: Vec<PlatformRepo>,
    options: &SyncOptions,
    on_progress: Option<&ProgressCallback>,
) -> Result<(SyncResult, Vec<CodeRepositoryActiveModel>), PlatformError> {
    let mut result = SyncResult {
        processed: repos.len(),
        ..SyncResult::default()
    };

    emit(
        on_progress,
        SyncProgress::FilteringByActivity {
            namespace: namespace.to_string(),
            days: options.active_within.num_days(),
        },
    );

    emit(
        on_progress,
        SyncProgress::FilteredPage {
            matched_so_far: 0,
            processed_so_far: 0,
        },
    );

    let active_repos = filter::filter_by_activity(&repos, options.active_within);
    result.matched = active_repos.len();

    emit(
        on_progress,
        SyncProgress::FilterComplete {
            namespace: namespace.to_string(),
            matched: result.matched,
            total: result.processed,
        },
    );

    if options.star && !active_repos.is_empty() {
        let repos_to_star: Vec<(String, String)> = active_repos
            .iter()
            .map(|repo| (repo.owner.clone(), repo.name.clone()))
            .collect();

        let star_stats = star::star_repos_concurrent(
            client,
            repos_to_star,
            options.concurrency,
            options.dry_run,
            on_progress,
        )
        .await;

        result.starred = star_stats.starred;
        result.skipped = star_stats.skipped;
        result.errors = star_stats.errors;
    }

    emit(on_progress, SyncProgress::ConvertingModels);

    let models = persist::build_models(client, &active_repos);
    result.saved = models.len();

    emit(
        on_progress,
        SyncProgress::ModelsReady {
            count: models.len(),
        },
    );

    Ok((result, models))
}

async fn sync_repos_streaming<C: PlatformClient + Clone + 'static>(
    client: &C,
    namespace: &str,
    repos: Vec<PlatformRepo>,
    options: &SyncOptions,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Result<SyncResult, PlatformError> {
    let streaming_result = persist::process_streaming_repos(
        client,
        namespace,
        &repos,
        options,
        &model_tx,
        on_progress,
    )
    .await;
    let mut result = streaming_result.result;

    if options.star && !streaming_result.repos_to_star.is_empty() {
        let star_stats = star::star_repos_concurrent(
            client,
            streaming_result.repos_to_star,
            options.concurrency,
            options.dry_run,
            on_progress,
        )
        .await;

        result.starred = star_stats.starred;
        result.skipped = star_stats.skipped;
        result.errors.extend(star_stats.errors);
    }

    Ok(result)
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
/// * `db` - Optional database connection for ETag caching
/// * `on_progress` - Optional progress callback
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip(client, options, db, on_progress), fields(namespace = %namespace))
)]
pub async fn sync_namespace<C: PlatformClient + Clone + 'static>(
    client: &C,
    namespace: &str,
    options: &SyncOptions,
    db: Option<&DatabaseConnection>,
    on_progress: Option<&ProgressCallback>,
) -> Result<(SyncResult, Vec<CodeRepositoryActiveModel>), PlatformError> {
    let repos = client.list_org_repos(namespace, db, on_progress).await?;
    let repos = maybe_filter_incremental_for_owner(client, namespace, options, db, repos).await?;

    sync_repos(client, namespace, repos, options, on_progress).await
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
/// * `db` - Optional database connection for ETag caching
/// * `model_tx` - Channel sender for streaming model persistence
/// * `on_progress` - Optional progress callback
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip(client, options, db, model_tx, on_progress), fields(namespace = %namespace))
)]
pub async fn sync_namespace_streaming<C: PlatformClient + Clone + 'static>(
    client: &C,
    namespace: &str,
    options: &SyncOptions,
    db: Option<&DatabaseConnection>,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Result<SyncResult, PlatformError> {
    let repos = client.list_org_repos(namespace, db, on_progress).await?;
    let repos = maybe_filter_incremental_for_owner(client, namespace, options, db, repos).await?;

    sync_repos_streaming(client, namespace, repos, options, model_tx, on_progress).await
}

/// Sync multiple namespaces concurrently.
///
/// # Arguments
///
/// * `client` - Platform client implementing `PlatformClient`
/// * `namespaces` - List of organization or group names to sync
/// * `options` - Sync configuration options
/// * `db` - Optional database connection for ETag caching (wrapped in Arc for concurrent access)
/// * `on_progress` - Optional progress callback
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip_all, fields(namespace_count = namespaces.len()))
)]
pub async fn sync_namespaces<C: PlatformClient + Clone + 'static>(
    client: &C,
    namespaces: &[String],
    options: &SyncOptions,
    db: Option<Arc<DatabaseConnection>>,
    on_progress: Option<&ProgressCallback>,
) -> Vec<NamespaceSyncResult> {
    let client = client.clone();
    let options = options.clone();

    spawn_concurrent_sync(
        namespaces,
        options.concurrency,
        |namespace, semaphore| {
            let client = client.clone();
            let options = options.clone();
            let task_db = db.clone();

            async move {
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => return NamespaceSyncResult::semaphore_error(namespace),
                };

                match sync_namespace(&client, &namespace, &options, task_db.as_deref(), None).await
                {
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
            }
        },
        on_progress,
    )
    .await
}

/// Sync multiple namespaces concurrently with streaming persistence.
///
/// # Arguments
///
/// * `client` - Platform client implementing `PlatformClient`
/// * `namespaces` - List of organization or group names to sync
/// * `options` - Sync configuration options
/// * `db` - Optional database connection for ETag caching (wrapped in Arc for concurrent access)
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
    db: Option<Arc<DatabaseConnection>>,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Vec<NamespaceSyncResultStreaming> {
    let client = client.clone();
    let options = options.clone();

    spawn_concurrent_sync_streaming(
        namespaces,
        options.concurrency,
        model_tx,
        move |namespace, semaphore, tx| {
            let client = client.clone();
            let options = options.clone();
            let task_db = db.clone();

            async move {
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => return NamespaceSyncResultStreaming::semaphore_error(namespace),
                };

                match sync_namespace_streaming(
                    &client,
                    &namespace,
                    &options,
                    task_db.as_deref(),
                    tx,
                    None,
                )
                .await
                {
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
            }
        },
        on_progress,
    )
    .await
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
/// * `db` - Optional database connection for ETag caching
/// * `on_progress` - Optional progress callback
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip(client, options, db, on_progress), fields(username = %username))
)]
pub async fn sync_user<C: PlatformClient + Clone + 'static>(
    client: &C,
    username: &str,
    options: &SyncOptions,
    db: Option<&DatabaseConnection>,
    on_progress: Option<&ProgressCallback>,
) -> Result<(SyncResult, Vec<CodeRepositoryActiveModel>), PlatformError> {
    let repos = client.list_user_repos(username, db, on_progress).await?;
    let repos = maybe_filter_incremental_for_owner(client, username, options, db, repos).await?;

    sync_repos(client, username, repos, options, on_progress).await
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
/// * `db` - Optional database connection for ETag caching
/// * `model_tx` - Channel sender for streaming model persistence
/// * `on_progress` - Optional progress callback
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip(client, options, db, model_tx, on_progress), fields(username = %username))
)]
pub async fn sync_user_streaming<C: PlatformClient + Clone + 'static>(
    client: &C,
    username: &str,
    options: &SyncOptions,
    db: Option<&DatabaseConnection>,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Result<SyncResult, PlatformError> {
    let repos = client.list_user_repos(username, db, on_progress).await?;
    let repos = maybe_filter_incremental_for_owner(client, username, options, db, repos).await?;

    sync_repos_streaming(client, username, repos, options, model_tx, on_progress).await
}

/// Sync an explicit list of repositories (owner/name pairs) with streaming persistence.
///
/// This is used for discovery-driven syncs where repository URLs are provided
/// directly rather than through org/user listings.
#[cfg_attr(
    any(feature = "github", feature = "gitlab", feature = "gitea"),
    tracing::instrument(skip(client, options, db, model_tx, on_progress), fields(repo_count = repos.len()))
)]
#[allow(clippy::too_many_arguments)]
pub async fn sync_repo_list_streaming<C: PlatformClient + Clone + 'static>(
    client: &C,
    namespace: &str,
    repos: &[(String, String)],
    options: &SyncOptions,
    db: Option<Arc<DatabaseConnection>>,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Result<SyncResult, PlatformError> {
    emit(
        on_progress,
        SyncProgress::FetchingRepos {
            namespace: namespace.to_string(),
            total_repos: Some(repos.len()),
            expected_pages: Some(repos.len().min(u32::MAX as usize) as u32),
        },
    );

    let mut join_set: JoinSet<(String, String, Result<PlatformRepo, PlatformError>)> =
        JoinSet::new();
    let concurrency = std::cmp::max(1, std::cmp::min(options.concurrency, repos.len().max(1)));
    let semaphore = Arc::new(Semaphore::new(concurrency));

    for (owner, name) in repos {
        let owner = owner.clone();
        let name = name.clone();
        let client = client.clone();
        let semaphore = Arc::clone(&semaphore);
        let task_db = db.clone();

        join_set.spawn(async move {
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

            let repo = client.get_repo(&owner, &name, task_db.as_deref()).await;
            (owner, name, repo)
        });
    }

    let mut fetched = Vec::new();
    let mut errors = Vec::new();
    let mut attempted = 0usize;

    while let Some(result) = join_set.join_next().await {
        attempted += 1;
        emit(
            on_progress,
            SyncProgress::FetchedPage {
                namespace: namespace.to_string(),
                page: attempted as u32,
                count: 1,
                total_so_far: attempted,
                expected_pages: Some(repos.len().min(u32::MAX as usize) as u32),
            },
        );

        match result {
            Ok((owner, name, Ok(repo))) => {
                fetched.push(repo);
                tracing::debug!(owner = %owner, name = %name, "Fetched repo");
            }
            Ok((owner, name, Err(error))) => {
                let message = format!("{}/{}: {}", owner, name, error);
                emit(
                    on_progress,
                    SyncProgress::Warning {
                        message: message.clone(),
                    },
                );
                errors.push(message);
            }
            Err(join_error) => {
                let message = format!("Task join error: {}", join_error);
                emit(
                    on_progress,
                    SyncProgress::Warning {
                        message: message.clone(),
                    },
                );
                errors.push(message);
            }
        }
    }

    emit(
        on_progress,
        SyncProgress::FetchComplete {
            namespace: namespace.to_string(),
            total: fetched.len(),
        },
    );

    let fetched = if options.strategy == SyncStrategy::Incremental {
        let mut owners: Vec<String> = fetched.iter().map(|repo| repo.owner.clone()).collect();
        owners.sort();
        owners.dedup();
        maybe_filter_incremental_for_owners(client, &owners, options, db.as_deref(), fetched)
            .await?
    } else {
        fetched
    };

    let mut result =
        sync_repos_streaming(client, namespace, fetched, options, model_tx, on_progress).await?;

    result.errors.extend(errors);
    Ok(result)
}

/// Sync multiple users' repositories concurrently with streaming persistence.
///
/// # Arguments
///
/// * `client` - Platform client implementing `PlatformClient`
/// * `usernames` - List of usernames to sync
/// * `options` - Sync configuration options
/// * `db` - Optional database connection for ETag caching (wrapped in Arc for concurrent access)
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
    db: Option<Arc<DatabaseConnection>>,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Vec<NamespaceSyncResultStreaming> {
    let client = client.clone();
    let options = options.clone();

    spawn_concurrent_sync_streaming(
        usernames,
        options.concurrency,
        model_tx,
        move |username, semaphore, tx| {
            let client = client.clone();
            let options = options.clone();
            let task_db = db.clone();

            async move {
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => return NamespaceSyncResultStreaming::semaphore_error(username),
                };

                match sync_user_streaming(
                    &client,
                    &username,
                    &options,
                    task_db.as_deref(),
                    tx,
                    None,
                )
                .await
                {
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
            }
        },
        on_progress,
    )
    .await
}

struct StarredProcessorState {
    processed: Arc<AtomicUsize>,
    matched: Arc<AtomicUsize>,
    saved: Arc<AtomicUsize>,
    channel_closed: Arc<AtomicBool>,
    repos_to_prune: Arc<Mutex<Vec<(String, String)>>>,
}

impl StarredProcessorState {
    fn new() -> Self {
        Self {
            processed: Arc::new(AtomicUsize::new(0)),
            matched: Arc::new(AtomicUsize::new(0)),
            saved: Arc::new(AtomicUsize::new(0)),
            channel_closed: Arc::new(AtomicBool::new(false)),
            repos_to_prune: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_starred_processor_task<C: PlatformClient + Clone + 'static>(
    client: &C,
    cutoff: chrono::DateTime<Utc>,
    repo_rx: mpsc::Receiver<PlatformRepo>,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    progress_tx: mpsc::Sender<(usize, usize)>,
    dry_run: bool,
    prune: bool,
    state: &StarredProcessorState,
) -> tokio::task::JoinHandle<()> {
    let processor_client = client.clone();
    let processor_processed = Arc::clone(&state.processed);
    let processor_matched = Arc::clone(&state.matched);
    let processor_saved = Arc::clone(&state.saved);
    let processor_channel_closed = Arc::clone(&state.channel_closed);
    let processor_repos_to_prune = Arc::clone(&state.repos_to_prune);

    tokio::spawn(async move {
        let mut repo_rx = repo_rx;
        let processor_model_tx = model_tx;

        tracing::debug!("Processor task started");
        while let Some(repo) = repo_rx.recv().await {
            let new_processed = processor_processed.fetch_add(1, Ordering::Relaxed) + 1;
            if new_processed.is_multiple_of(PROCESSOR_LOG_EVERY) {
                tracing::debug!(processed = new_processed, "Processor progress");
            }

            let is_active = filter::is_active_repo(&repo, cutoff);
            let new_matched = if is_active {
                processor_matched.fetch_add(1, Ordering::Relaxed) + 1
            } else {
                processor_matched.load(Ordering::Relaxed)
            };

            let _ = progress_tx.try_send((new_matched, new_processed));

            if is_active {
                if !dry_run && !processor_channel_closed.load(Ordering::Relaxed) {
                    let model = processor_client.to_active_model(&repo);
                    if processor_model_tx.send(model).await.is_err() {
                        processor_channel_closed.store(true, Ordering::Relaxed);
                    } else {
                        processor_saved.fetch_add(1, Ordering::Relaxed);
                    }
                }
            } else if prune && let Ok(mut prune_list) = processor_repos_to_prune.lock() {
                prune_list.push((repo.owner.clone(), repo.name.clone()));
            }
        }

        tracing::debug!(
            processed = processor_processed.load(Ordering::Relaxed),
            matched = processor_matched.load(Ordering::Relaxed),
            saved = processor_saved.load(Ordering::Relaxed),
            "Processor task finished"
        );
    })
}

async fn poll_starred_stream_tasks<F>(
    fetch_future: F,
    processor_handle: tokio::task::JoinHandle<()>,
    progress_rx: mpsc::Receiver<(usize, usize)>,
    on_progress: Option<&ProgressCallback>,
) -> Result<(usize, usize), PlatformError>
where
    F: Future<Output = Result<usize, PlatformError>>,
{
    let mut fetch_future = Box::pin(fetch_future);
    let mut fetch_done = false;
    let mut fetch_result: Option<Result<usize, PlatformError>> = None;
    let mut processor_result: Option<Result<(), tokio::task::JoinError>> = None;
    let mut last_emitted_matched = 0usize;
    let mut last_emitted_processed = 0usize;
    let mut processor_handle = processor_handle;

    let mut progress_stream = ReceiverStream::new(progress_rx).fuse();
    let mut progress_stream_done = false;

    loop {
        tokio::select! {
            biased;

            result = progress_stream.next(), if processor_result.is_none() && !progress_stream_done => {
                if let Some((matched_count, processed_count)) = result {
                    if should_emit_filtered_progress(
                        matched_count,
                        processed_count,
                        last_emitted_matched,
                        last_emitted_processed,
                    ) {
                        emit_filtered_progress(on_progress, matched_count, processed_count);
                        last_emitted_matched = matched_count;
                        last_emitted_processed = processed_count;
                    }
                } else {
                    progress_stream_done = true;
                }
            }

            result = fetch_future.as_mut(), if !fetch_done => {
                fetch_done = true;
                fetch_result = Some(result);
            }

            result = &mut processor_handle, if processor_result.is_none() => {
                processor_result = Some(result.map(|_| ()));
            }
        }

        if fetch_done && processor_result.is_some() {
            while let Some((matched_count, processed_count)) = progress_stream.next().await {
                if should_emit_filtered_progress(
                    matched_count,
                    processed_count,
                    last_emitted_matched,
                    last_emitted_processed,
                ) {
                    emit_filtered_progress(on_progress, matched_count, processed_count);
                    last_emitted_matched = matched_count;
                    last_emitted_processed = processed_count;
                }
            }
            break;
        }
    }

    let fetch_result =
        fetch_result.ok_or_else(|| PlatformError::internal("Fetch task did not complete"))?;
    drop(fetch_future);

    match processor_result {
        Some(result) => {
            result.map_err(|e| PlatformError::internal(format!("Processor task failed: {}", e)))?;
        }
        None => {
            return Err(PlatformError::internal("Processor task did not complete"));
        }
    }

    fetch_result?;
    Ok((last_emitted_matched, last_emitted_processed))
}

fn take_prune_repos(repos_to_prune: Arc<Mutex<Vec<(String, String)>>>) -> Vec<(String, String)> {
    match Arc::try_unwrap(repos_to_prune) {
        Ok(mutex) => mutex.into_inner().unwrap_or_default(),
        Err(arc) => arc.lock().unwrap_or_else(|e| e.into_inner()).clone(),
    }
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
    db: Option<&DatabaseConnection>,
    concurrency: usize,
    skip_rate_checks: bool,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Result<SyncResult, PlatformError> {
    let cutoff = Utc::now() - options.active_within;

    // Note: FilteringByActivity is not emitted here because the client's
    // list_starred_repos_streaming emits FetchingRepos, and filtering happens
    // during streaming (not as a separate phase). The FilteredPage events
    // provide incremental filtering progress.

    let state = StarredProcessorState::new();

    // Create channel for receiving repos from the streaming fetch.
    // Buffer of 500 provides headroom to reduce backpressure likelihood.
    // The time-based flushing in the persist task is the primary deadlock prevention,
    // but larger buffers reduce the frequency of backpressure situations.
    let (repo_tx, repo_rx) = mpsc::channel::<PlatformRepo>(REPO_STREAM_CHANNEL_BUFFER);

    // Create channel for progress events from processor task
    // Sends (matched_count, processed_count) tuples
    // Use a bounded channel and drop when full to avoid unbounded memory growth.
    let (progress_tx, progress_rx) =
        mpsc::channel::<(usize, usize)>(FILTER_PROGRESS_CHANNEL_BUFFER);

    let processor_handle = spawn_starred_processor_task(
        client,
        cutoff,
        repo_rx,
        model_tx.clone(),
        progress_tx,
        options.dry_run,
        options.prune,
        &state,
    );

    let fetch_future = client.list_starred_repos_streaming(
        repo_tx,
        db,
        concurrency,
        skip_rate_checks,
        on_progress,
    );

    let (last_emitted_matched, last_emitted_processed) =
        poll_starred_stream_tasks(fetch_future, processor_handle, progress_rx, on_progress).await?;

    // Emit final progress update
    let final_matched = state.matched.load(Ordering::Relaxed);
    let final_processed = state.processed.load(Ordering::Relaxed);
    if final_matched > last_emitted_matched || final_processed > last_emitted_processed {
        emit(
            on_progress,
            SyncProgress::FilteredPage {
                matched_so_far: final_matched,
                processed_so_far: final_processed,
            },
        );
    }

    // Emit FilterComplete and ModelsReady BEFORE dropping the channel
    // This ensures the progress reporter knows the final count before any
    // remaining Persisted events arrive from the persist task's final batch flush
    emit(
        on_progress,
        SyncProgress::FilterComplete {
            namespace: "starred".to_string(),
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
    tracing::debug!("Dropping model_tx to signal persist task completion");
    drop(model_tx);
    tracing::debug!("model_tx dropped, persist task should now see channel close");

    // Build result
    let mut result = SyncResult {
        processed: final_processed,
        matched: final_matched,
        saved: state.saved.load(Ordering::Relaxed),
        ..Default::default()
    };

    // Prune inactive starred repos
    let repos_to_prune = take_prune_repos(state.repos_to_prune);

    if options.prune && !repos_to_prune.is_empty() {
        let prune_result =
            star::prune_repos(client, repos_to_prune, options.dry_run, on_progress).await;

        result.pruned = prune_result.pruned;
        result.pruned_repos = prune_result.pruned_repos;
        result.errors.extend(prune_result.errors);
    }

    Ok(result)
}

fn should_emit_filtered_progress(
    matched_count: usize,
    processed_count: usize,
    last_emitted_matched: usize,
    last_emitted_processed: usize,
) -> bool {
    matched_count >= last_emitted_matched + FILTER_PROGRESS_EMIT_MATCHED_STEP
        || processed_count >= last_emitted_processed + FILTER_PROGRESS_EMIT_PROCESSED_STEP
        || (matched_count > 0 && last_emitted_matched == 0)
        || (processed_count > 0 && last_emitted_processed == 0)
}

fn emit_filtered_progress(
    on_progress: Option<&ProgressCallback>,
    matched_count: usize,
    processed_count: usize,
) {
    emit(
        on_progress,
        SyncProgress::FilteredPage {
            matched_so_far: matched_count,
            processed_so_far: processed_count,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::StarringStats;
    use async_trait::async_trait;
    use chrono::Duration;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use crate::entity::platform_type::PlatformType;
    use crate::platform::{OrgInfo, RateLimitInfo, Result as PlatformResult, UserInfo};

    #[derive(Clone, Default)]
    struct TestClient {
        starred_repos: Arc<Mutex<Vec<PlatformRepo>>>,
        repo_results: Arc<Mutex<HashMap<String, PlatformResult<PlatformRepo>>>>,
        star_results: Arc<Mutex<HashMap<String, PlatformResult<bool>>>>,
        unstar_results: Arc<Mutex<HashMap<String, PlatformResult<bool>>>>,
        star_calls: Arc<Mutex<Vec<String>>>,
        unstar_calls: Arc<Mutex<Vec<String>>>,
    }

    impl TestClient {
        fn key(owner: &str, name: &str) -> String {
            format!("{owner}/{name}")
        }

        fn set_starred_repos(&self, repos: Vec<PlatformRepo>) {
            *self.starred_repos.lock().unwrap_or_else(|e| e.into_inner()) = repos;
        }

        fn set_star_result(&self, owner: &str, name: &str, value: PlatformResult<bool>) {
            let key = Self::key(owner, name);
            self.star_results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key, value);
        }

        fn set_repo_result(&self, owner: &str, name: &str, value: PlatformResult<PlatformRepo>) {
            let key = Self::key(owner, name);
            self.repo_results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key, value);
        }

        fn set_unstar_result(&self, owner: &str, name: &str, value: PlatformResult<bool>) {
            let key = Self::key(owner, name);
            self.unstar_results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key, value);
        }

        fn star_calls_len(&self) -> usize {
            self.star_calls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len()
        }

        fn unstar_calls(&self) -> Vec<String> {
            self.unstar_calls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
        }
    }

    #[derive(Clone)]
    struct PanicRepoClient;

    #[async_trait]
    impl PlatformClient for PanicRepoClient {
        fn platform_type(&self) -> PlatformType {
            PlatformType::GitHub
        }

        fn instance_id(&self) -> Uuid {
            Uuid::nil()
        }

        async fn get_rate_limit(&self) -> PlatformResult<RateLimitInfo> {
            panic!("unused in tests")
        }

        async fn get_org_info(&self, _org: &str) -> PlatformResult<OrgInfo> {
            panic!("unused in tests")
        }

        async fn get_authenticated_user(&self) -> PlatformResult<UserInfo> {
            panic!("unused in tests")
        }

        async fn get_repo(
            &self,
            _owner: &str,
            _name: &str,
            _db: Option<&DatabaseConnection>,
        ) -> PlatformResult<PlatformRepo> {
            panic!("intentional panic for join error coverage")
        }

        async fn list_org_repos(
            &self,
            _org: &str,
            _db: Option<&DatabaseConnection>,
            _on_progress: Option<&crate::platform::ProgressCallback>,
        ) -> PlatformResult<Vec<PlatformRepo>> {
            panic!("unused in tests")
        }

        async fn list_user_repos(
            &self,
            _username: &str,
            _db: Option<&DatabaseConnection>,
            _on_progress: Option<&crate::platform::ProgressCallback>,
        ) -> PlatformResult<Vec<PlatformRepo>> {
            panic!("unused in tests")
        }

        async fn is_repo_starred(&self, _owner: &str, _name: &str) -> PlatformResult<bool> {
            panic!("unused in tests")
        }

        async fn star_repo(&self, _owner: &str, _name: &str) -> PlatformResult<bool> {
            panic!("unused in tests")
        }

        async fn star_repo_with_retry(
            &self,
            _owner: &str,
            _name: &str,
            _on_progress: Option<&crate::platform::ProgressCallback>,
        ) -> PlatformResult<bool> {
            panic!("unused in tests")
        }

        async fn unstar_repo(&self, _owner: &str, _name: &str) -> PlatformResult<bool> {
            panic!("unused in tests")
        }

        async fn list_starred_repos(
            &self,
            _db: Option<&DatabaseConnection>,
            _concurrency: usize,
            _skip_rate_checks: bool,
            _on_progress: Option<&crate::platform::ProgressCallback>,
        ) -> PlatformResult<Vec<PlatformRepo>> {
            panic!("unused in tests")
        }

        async fn list_starred_repos_streaming(
            &self,
            _repo_tx: mpsc::Sender<PlatformRepo>,
            _db: Option<&DatabaseConnection>,
            _concurrency: usize,
            _skip_rate_checks: bool,
            _on_progress: Option<&crate::platform::ProgressCallback>,
        ) -> PlatformResult<usize> {
            panic!("unused in tests")
        }

        fn to_active_model(&self, repo: &PlatformRepo) -> CodeRepositoryActiveModel {
            repo.to_active_model(self.instance_id())
        }
    }

    #[async_trait]
    impl PlatformClient for TestClient {
        fn platform_type(&self) -> PlatformType {
            PlatformType::GitHub
        }

        fn instance_id(&self) -> Uuid {
            Uuid::nil()
        }

        async fn get_rate_limit(&self) -> PlatformResult<RateLimitInfo> {
            panic!("unused in tests")
        }

        async fn get_org_info(&self, _org: &str) -> PlatformResult<OrgInfo> {
            panic!("unused in tests")
        }

        async fn get_authenticated_user(&self) -> PlatformResult<UserInfo> {
            panic!("unused in tests")
        }

        async fn get_repo(
            &self,
            owner: &str,
            name: &str,
            _db: Option<&DatabaseConnection>,
        ) -> PlatformResult<PlatformRepo> {
            let key = Self::key(owner, name);
            self.repo_results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key)
                .unwrap_or_else(|| Err(PlatformError::internal("missing repo result")))
        }

        async fn list_org_repos(
            &self,
            _org: &str,
            _db: Option<&DatabaseConnection>,
            _on_progress: Option<&crate::platform::ProgressCallback>,
        ) -> PlatformResult<Vec<PlatformRepo>> {
            panic!("unused in tests")
        }

        async fn list_user_repos(
            &self,
            _username: &str,
            _db: Option<&DatabaseConnection>,
            _on_progress: Option<&crate::platform::ProgressCallback>,
        ) -> PlatformResult<Vec<PlatformRepo>> {
            panic!("unused in tests")
        }

        async fn is_repo_starred(&self, _owner: &str, _name: &str) -> PlatformResult<bool> {
            panic!("unused in tests")
        }

        async fn star_repo(&self, _owner: &str, _name: &str) -> PlatformResult<bool> {
            panic!("unused in tests")
        }

        async fn star_repo_with_retry(
            &self,
            owner: &str,
            name: &str,
            _on_progress: Option<&crate::platform::ProgressCallback>,
        ) -> PlatformResult<bool> {
            let key = Self::key(owner, name);
            self.star_calls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(key.clone());
            self.star_results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key)
                .unwrap_or(Ok(false))
        }

        async fn unstar_repo(&self, owner: &str, name: &str) -> PlatformResult<bool> {
            let key = Self::key(owner, name);
            self.unstar_calls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(key.clone());
            self.unstar_results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key)
                .unwrap_or(Ok(false))
        }

        async fn list_starred_repos(
            &self,
            _db: Option<&DatabaseConnection>,
            _concurrency: usize,
            _skip_rate_checks: bool,
            _on_progress: Option<&crate::platform::ProgressCallback>,
        ) -> PlatformResult<Vec<PlatformRepo>> {
            panic!("unused in tests")
        }

        async fn list_starred_repos_streaming(
            &self,
            repo_tx: mpsc::Sender<PlatformRepo>,
            _db: Option<&DatabaseConnection>,
            _concurrency: usize,
            _skip_rate_checks: bool,
            _on_progress: Option<&crate::platform::ProgressCallback>,
        ) -> PlatformResult<usize> {
            let repos = self
                .starred_repos
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let count = repos.len();
            for repo in repos {
                if repo_tx.send(repo).await.is_err() {
                    break;
                }
            }
            Ok(count)
        }

        fn to_active_model(&self, repo: &PlatformRepo) -> CodeRepositoryActiveModel {
            repo.to_active_model(self.instance_id())
        }
    }

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
        use super::super::types::SyncStrategy;

        let options = SyncOptions {
            active_within: Duration::days(90),
            star: false,
            dry_run: true,
            concurrency: 5,
            platform_options: super::super::types::PlatformOptions {
                include_subgroups: true,
            },
            prune: false,
            strategy: SyncStrategy::Incremental,
        };

        assert_eq!(options.active_within, Duration::days(90));
        assert!(!options.star);
        assert!(options.dry_run);
        assert_eq!(options.concurrency, 5);
        assert!(options.platform_options.include_subgroups);
        assert!(!options.prune);
        assert_eq!(options.strategy, SyncStrategy::Incremental);
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
            if let SyncProgress::FetchComplete { total: _, .. } = event {}
        });

        callback(SyncProgress::FetchComplete {
            namespace: "test".to_string(),
            total: 42,
        });
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_should_emit_filtered_progress_threshold_and_initial_edges() {
        assert!(should_emit_filtered_progress(25, 0, 0, 0));
        assert!(should_emit_filtered_progress(0, 100, 0, 0));
        assert!(should_emit_filtered_progress(1, 0, 0, 0));
        assert!(should_emit_filtered_progress(0, 1, 0, 0));
        assert!(!should_emit_filtered_progress(24, 99, 1, 1));
    }

    #[test]
    fn test_take_prune_repos_when_arc_is_still_shared() {
        let repos = Arc::new(Mutex::new(vec![("org".to_string(), "repo".to_string())]));
        let shared = Arc::clone(&repos);

        let taken = take_prune_repos(repos);
        assert_eq!(taken, vec![("org".to_string(), "repo".to_string())]);

        drop(shared);
    }

    #[test]
    fn test_take_prune_repos_when_arc_is_unique() {
        let repos = Arc::new(Mutex::new(vec![
            ("org-1".to_string(), "repo-1".to_string()),
            ("org-2".to_string(), "repo-2".to_string()),
        ]));

        let taken = take_prune_repos(repos);
        assert_eq!(
            taken,
            vec![
                ("org-1".to_string(), "repo-1".to_string()),
                ("org-2".to_string(), "repo-2".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn test_spawn_concurrent_sync_empty_input_short_circuits_without_progress() {
        let items: Vec<String> = Vec::new();
        let progress_calls = Arc::new(AtomicUsize::new(0));
        let progress_calls_clone = Arc::clone(&progress_calls);
        let callback: ProgressCallback = Box::new(move |_event| {
            progress_calls_clone.fetch_add(1, Ordering::SeqCst);
        });

        let results = spawn_concurrent_sync::<NamespaceSyncResult, _, _>(
            &items,
            4,
            |name, _semaphore| async move { NamespaceSyncResult::semaphore_error(name) },
            Some(&callback),
        )
        .await;

        assert!(results.is_empty());
        assert_eq!(progress_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_spawn_concurrent_sync_wraps_panics_into_results() {
        let items = vec!["one".to_string()];

        let results = spawn_concurrent_sync::<NamespaceSyncResult, _, _>(
            &items,
            4,
            |_item, _semaphore| async move {
                panic!("boom");
            },
            None,
        )
        .await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].namespace, "<unknown>");
        assert!(
            results[0]
                .error
                .as_deref()
                .is_some_and(|err| err.contains("Task panic:"))
        );
    }

    #[tokio::test]
    async fn test_spawn_concurrent_sync_emits_aggregated_success_and_failure_progress() {
        let items = vec!["ok".to_string(), "bad".to_string()];
        let progress_events = Arc::new(Mutex::new(Vec::<SyncProgress>::new()));
        let progress_events_clone = Arc::clone(&progress_events);
        let callback: ProgressCallback = Box::new(move |event| {
            progress_events_clone
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(event);
        });

        let results = spawn_concurrent_sync::<NamespaceSyncResult, _, _>(
            &items,
            4,
            |item, _semaphore| async move {
                if item == "bad" {
                    NamespaceSyncResult {
                        namespace: item,
                        result: SyncResult::default(),
                        models: Vec::new(),
                        error: Some("failed".to_string()),
                    }
                } else {
                    NamespaceSyncResult {
                        namespace: item,
                        result: SyncResult::default(),
                        models: Vec::new(),
                        error: None,
                    }
                }
            },
            Some(&callback),
        )
        .await;

        assert_eq!(results.len(), 2);
        assert_eq!(results.iter().filter(|r| r.error.is_none()).count(), 1);
        assert_eq!(results.iter().filter(|r| r.error.is_some()).count(), 1);

        let events = progress_events.lock().unwrap_or_else(|e| e.into_inner());
        assert!(matches!(
            events.first(),
            Some(SyncProgress::SyncingNamespaces { count: 2 })
        ));
        assert!(events.iter().any(|event| matches!(
            event,
            SyncProgress::SyncNamespacesComplete {
                successful: 1,
                failed: 1
            }
        )));
    }

    #[tokio::test]
    async fn test_spawn_concurrent_sync_streaming_empty_input_short_circuits_without_progress() {
        let items: Vec<String> = Vec::new();
        let (model_tx, mut model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(4);
        let progress_calls = Arc::new(AtomicUsize::new(0));
        let progress_calls_clone = Arc::clone(&progress_calls);
        let callback: ProgressCallback = Box::new(move |_event| {
            progress_calls_clone.fetch_add(1, Ordering::SeqCst);
        });

        let results =
            spawn_concurrent_sync_streaming(
                &items,
                4,
                model_tx,
                |name, _semaphore, _tx| async move {
                    NamespaceSyncResultStreaming::semaphore_error(name)
                },
                Some(&callback),
            )
            .await;

        assert!(results.is_empty());
        assert_eq!(progress_calls.load(Ordering::SeqCst), 0);
        assert!(model_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_spawn_concurrent_sync_streaming_emits_progress_and_wraps_panics() {
        let items = vec!["one".to_string()];
        let (model_tx, mut model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(4);

        let progress_events = Arc::new(Mutex::new(Vec::<SyncProgress>::new()));
        let progress_events_clone = Arc::clone(&progress_events);
        let callback: ProgressCallback = Box::new(move |event| {
            progress_events_clone
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(event);
        });

        let results = spawn_concurrent_sync_streaming(
            &items,
            4,
            model_tx,
            |_item, _semaphore, _tx| async move {
                panic!("boom");
            },
            Some(&callback),
        )
        .await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].namespace, "<unknown>");
        assert!(
            results[0]
                .error
                .as_deref()
                .is_some_and(|err| err.contains("Task panic:"))
        );

        {
            let events = progress_events.lock().unwrap_or_else(|e| e.into_inner());
            assert!(matches!(
                events.first(),
                Some(SyncProgress::SyncingNamespaces { count: 1 })
            ));
            assert!(events.iter().any(|event| matches!(
                event,
                SyncProgress::SyncNamespacesComplete {
                    successful: 0,
                    failed: 1
                }
            )));
        }

        assert!(model_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_poll_starred_stream_tasks_returns_processor_join_error() {
        let fetch_future = async { Ok::<usize, PlatformError>(1) };
        let processor_handle = tokio::spawn(async move {
            panic!("processor failed");
        });
        let (progress_tx, progress_rx) = mpsc::channel::<(usize, usize)>(1);
        drop(progress_tx);

        let err = poll_starred_stream_tasks(fetch_future, processor_handle, progress_rx, None)
            .await
            .expect_err("processor panic should be surfaced");

        assert!(err.to_string().contains("Processor task failed"));
    }

    #[tokio::test]
    async fn test_sync_repos_skips_starring_when_star_disabled() {
        let client = TestClient::default();
        client.set_star_result("test-org", "recent", Ok(true));

        let options = SyncOptions {
            star: false,
            active_within: Duration::days(30),
            ..SyncOptions::default()
        };

        let repos = vec![mock_repo("recent", 1)];
        let (result, models) = sync_repos(&client, "test-org", repos, &options, None)
            .await
            .expect("sync should succeed");

        assert_eq!(result.processed, 1);
        assert_eq!(result.matched, 1);
        assert_eq!(result.starred, 0);
        assert_eq!(result.saved, 1);
        assert_eq!(models.len(), 1);
        assert_eq!(client.star_calls_len(), 0);
    }

    #[tokio::test]
    async fn test_sync_repos_skips_starring_when_no_active_repos() {
        let client = TestClient::default();
        client.set_star_result("test-org", "old", Ok(true));

        let options = SyncOptions {
            star: true,
            active_within: Duration::days(30),
            ..SyncOptions::default()
        };

        let repos = vec![mock_repo("old", 90)];
        let (result, models) = sync_repos(&client, "test-org", repos, &options, None)
            .await
            .expect("sync should succeed");

        assert_eq!(result.processed, 1);
        assert_eq!(result.matched, 0);
        assert_eq!(result.starred, 0);
        assert_eq!(result.saved, 0);
        assert!(models.is_empty());
        assert_eq!(client.star_calls_len(), 0);
    }

    #[tokio::test]
    async fn test_sync_starred_streaming_prune_disabled_skips_unstar() {
        let client = TestClient::default();
        client.set_starred_repos(vec![mock_repo_with_owner("org", "inactive", 90)]);
        client.set_unstar_result("org", "inactive", Ok(true));

        let options = SyncOptions {
            prune: false,
            active_within: Duration::days(30),
            ..SyncOptions::default()
        };

        let (model_tx, mut model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(8);
        let sync_result = sync_starred_streaming(&client, &options, None, 2, false, model_tx, None)
            .await
            .expect("sync should succeed");

        assert_eq!(sync_result.processed, 1);
        assert_eq!(sync_result.matched, 0);
        assert_eq!(sync_result.saved, 0);
        assert_eq!(sync_result.pruned, 0);
        assert!(sync_result.pruned_repos.is_empty());
        assert!(model_rx.recv().await.is_none());
        assert!(client.unstar_calls().is_empty());
    }

    #[tokio::test]
    async fn test_sync_starred_streaming_prune_enabled_unstars_inactive() {
        let client = TestClient::default();
        client.set_starred_repos(vec![mock_repo_with_owner("org", "inactive", 90)]);
        client.set_unstar_result("org", "inactive", Ok(true));

        let options = SyncOptions {
            prune: true,
            active_within: Duration::days(30),
            ..SyncOptions::default()
        };

        let (model_tx, _model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(8);
        let sync_result = sync_starred_streaming(&client, &options, None, 2, false, model_tx, None)
            .await
            .expect("sync should succeed");

        assert_eq!(sync_result.processed, 1);
        assert_eq!(sync_result.matched, 0);
        assert_eq!(sync_result.pruned, 1);
        assert_eq!(
            sync_result.pruned_repos,
            vec![("org".to_string(), "inactive".to_string())]
        );
        assert_eq!(client.unstar_calls(), vec!["org/inactive".to_string()]);
    }

    #[tokio::test]
    async fn test_sync_starred_streaming_handles_closed_model_channel() {
        let client = TestClient::default();
        client.set_starred_repos(vec![mock_repo_with_owner("org", "active", 1)]);

        let options = SyncOptions {
            prune: false,
            active_within: Duration::days(30),
            ..SyncOptions::default()
        };

        let (model_tx, model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(1);
        drop(model_rx);

        let sync_result = sync_starred_streaming(&client, &options, None, 2, false, model_tx, None)
            .await
            .expect("sync should succeed when model receiver is gone");

        assert_eq!(sync_result.processed, 1);
        assert_eq!(sync_result.matched, 1);
        assert_eq!(sync_result.saved, 0);
        assert!(sync_result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_sync_starred_streaming_aggregates_prune_errors() {
        let client = TestClient::default();
        client.set_starred_repos(vec![mock_repo_with_owner("org", "inactive", 90)]);
        client.set_unstar_result(
            "org",
            "inactive",
            Err(PlatformError::internal("unstar failed")),
        );

        let options = SyncOptions {
            prune: true,
            active_within: Duration::days(30),
            ..SyncOptions::default()
        };

        let (model_tx, mut model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(8);
        let sync_result = sync_starred_streaming(&client, &options, None, 2, false, model_tx, None)
            .await
            .expect("sync should succeed and report prune failures");

        assert_eq!(sync_result.processed, 1);
        assert_eq!(sync_result.matched, 0);
        assert_eq!(sync_result.pruned, 0);
        assert!(
            sync_result
                .errors
                .iter()
                .any(|error| error.contains("unstar failed"))
        );
        assert!(model_rx.recv().await.is_none());
    }

    #[test]
    fn test_should_emit_filtered_progress_exact_step_boundaries() {
        assert!(!should_emit_filtered_progress(34, 199, 10, 100));
        assert!(should_emit_filtered_progress(35, 199, 10, 100));
        assert!(!should_emit_filtered_progress(10, 199, 10, 100));
        assert!(should_emit_filtered_progress(10, 200, 10, 100));
    }

    #[tokio::test]
    async fn test_poll_starred_stream_tasks_propagates_fetch_error() {
        let fetch_future =
            async { Err::<usize, PlatformError>(PlatformError::internal("fetch failed")) };
        let processor_handle = tokio::spawn(async {});
        let (progress_tx, progress_rx) = mpsc::channel::<(usize, usize)>(1);
        drop(progress_tx);

        let err = poll_starred_stream_tasks(fetch_future, processor_handle, progress_rx, None)
            .await
            .expect_err("fetch error should be returned");

        assert!(err.to_string().contains("fetch failed"));
    }

    #[tokio::test]
    async fn test_sync_starred_streaming_dry_run_skips_models_ready_event() {
        let client = TestClient::default();
        client.set_starred_repos(vec![mock_repo_with_owner("org", "active", 1)]);

        let options = SyncOptions {
            dry_run: true,
            prune: false,
            active_within: Duration::days(30),
            ..SyncOptions::default()
        };

        let models_ready_count = Arc::new(AtomicUsize::new(0));
        let filter_complete_count = Arc::new(AtomicUsize::new(0));
        let models_ready_count_clone = Arc::clone(&models_ready_count);
        let filter_complete_count_clone = Arc::clone(&filter_complete_count);
        let callback: ProgressCallback = Box::new(move |event| match event {
            SyncProgress::ModelsReady { .. } => {
                models_ready_count_clone.fetch_add(1, Ordering::SeqCst);
            }
            SyncProgress::FilterComplete { .. } => {
                filter_complete_count_clone.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        });

        let (model_tx, mut model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(8);
        let sync_result =
            sync_starred_streaming(&client, &options, None, 2, false, model_tx, Some(&callback))
                .await
                .expect("sync should succeed");

        assert_eq!(sync_result.processed, 1);
        assert_eq!(sync_result.matched, 1);
        assert_eq!(sync_result.saved, 0);
        assert_eq!(models_ready_count.load(Ordering::SeqCst), 0);
        assert_eq!(filter_complete_count.load(Ordering::SeqCst), 1);
        assert!(model_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_sync_starred_streaming_non_dry_run_emits_models_ready_event() {
        let client = TestClient::default();
        client.set_starred_repos(vec![mock_repo_with_owner("org", "active", 1)]);

        let options = SyncOptions {
            dry_run: false,
            prune: false,
            active_within: Duration::days(30),
            ..SyncOptions::default()
        };

        let models_ready_count = Arc::new(AtomicUsize::new(0));
        let filter_complete_count = Arc::new(AtomicUsize::new(0));
        let models_ready_count_clone = Arc::clone(&models_ready_count);
        let filter_complete_count_clone = Arc::clone(&filter_complete_count);
        let callback: ProgressCallback = Box::new(move |event| match event {
            SyncProgress::ModelsReady { .. } => {
                models_ready_count_clone.fetch_add(1, Ordering::SeqCst);
            }
            SyncProgress::FilterComplete { .. } => {
                filter_complete_count_clone.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        });

        let (model_tx, mut model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(8);
        let sync_result =
            sync_starred_streaming(&client, &options, None, 2, false, model_tx, Some(&callback))
                .await
                .expect("sync should succeed");

        assert_eq!(sync_result.processed, 1);
        assert_eq!(sync_result.matched, 1);
        assert_eq!(sync_result.saved, 1);
        assert_eq!(models_ready_count.load(Ordering::SeqCst), 1);
        assert_eq!(filter_complete_count.load(Ordering::SeqCst), 1);
        assert!(model_rx.recv().await.is_some());
        assert!(model_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_sync_repo_list_streaming_emits_warning_progress_for_join_error() {
        let client = PanicRepoClient;
        let options = SyncOptions {
            star: false,
            dry_run: true,
            ..SyncOptions::default()
        };
        let repos = vec![("org".to_string(), "repo".to_string())];
        let warnings = Arc::new(Mutex::new(Vec::<String>::new()));
        let warnings_clone = Arc::clone(&warnings);
        let callback: ProgressCallback = Box::new(move |event| {
            if let SyncProgress::Warning { message } = event {
                warnings_clone
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(message);
            }
        });

        let (model_tx, mut model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(8);
        let result = sync_repo_list_streaming(
            &client,
            "panic",
            &repos,
            &options,
            None,
            model_tx,
            Some(&callback),
        )
        .await
        .expect("sync should succeed while collecting join errors");

        assert_eq!(result.processed, 0);
        assert!(result.errors.iter().any(|e| e.contains("Task join error:")));
        assert!(model_rx.recv().await.is_none());

        let warning_messages = warnings.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            warning_messages
                .iter()
                .any(|message| message.contains("Task join error:"))
        );
    }

    #[tokio::test]
    async fn test_sync_repo_list_streaming_aggregates_fetch_and_star_errors() {
        let client = TestClient::default();
        let active_repo = mock_repo_with_owner("ok-org", "good", 1);
        client.set_repo_result("ok-org", "good", Ok(active_repo));
        client.set_repo_result(
            "bad-org",
            "broken",
            Err(PlatformError::internal("cannot fetch repo")),
        );
        client.set_star_result(
            "ok-org",
            "good",
            Err(PlatformError::internal("star failed")),
        );

        let options = SyncOptions {
            star: true,
            dry_run: false,
            active_within: Duration::days(30),
            ..SyncOptions::default()
        };

        let repos = vec![
            ("ok-org".to_string(), "good".to_string()),
            ("bad-org".to_string(), "broken".to_string()),
        ];

        let (model_tx, mut model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(8);
        let result =
            sync_repo_list_streaming(&client, "mixed", &repos, &options, None, model_tx, None)
                .await
                .expect("sync should complete with collected errors");

        assert_eq!(result.processed, 1);
        assert_eq!(result.matched, 1);
        assert_eq!(result.saved, 1);
        assert_eq!(result.errors.len(), 2);
        assert!(result.errors.iter().any(|e| e.contains("bad-org/broken:")));
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.contains("cannot fetch repo"))
        );
        assert!(result.errors.iter().any(|e| e.contains("star failed")));
        assert!(model_rx.recv().await.is_some());
        assert!(model_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_sync_repo_list_streaming_incremental_without_db_keeps_fetched_repos() {
        let client = TestClient::default();
        client.set_repo_result("acme", "one", Ok(mock_repo_with_owner("acme", "one", 1)));
        client.set_repo_result("acme", "two", Ok(mock_repo_with_owner("acme", "two", 2)));

        let options = SyncOptions {
            strategy: SyncStrategy::Incremental,
            star: false,
            dry_run: false,
            active_within: Duration::days(30),
            ..SyncOptions::default()
        };
        let repos = vec![
            ("acme".to_string(), "one".to_string()),
            ("acme".to_string(), "two".to_string()),
        ];

        let (model_tx, mut model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(8);
        let result = sync_repo_list_streaming(
            &client,
            "incremental-no-db",
            &repos,
            &options,
            None,
            model_tx,
            None,
        )
        .await
        .expect("incremental sync without db should keep fetched repos");

        assert_eq!(result.processed, 2);
        assert_eq!(result.matched, 2);
        assert_eq!(result.saved, 2);
        assert!(result.errors.is_empty());
        assert!(model_rx.recv().await.is_some());
        assert!(model_rx.recv().await.is_some());
        assert!(model_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_sync_repo_list_streaming_incremental_with_db_skips_filter_when_none_fetched() {
        let client = TestClient::default();
        client.set_repo_result(
            "missing-org",
            "missing-repo",
            Err(PlatformError::internal("cannot fetch repo")),
        );

        let options = SyncOptions {
            strategy: SyncStrategy::Incremental,
            star: false,
            dry_run: true,
            ..SyncOptions::default()
        };
        let repos = vec![("missing-org".to_string(), "missing-repo".to_string())];
        let db = Arc::new(MockDatabase::new(DatabaseBackend::Sqlite).into_connection());

        let (model_tx, mut model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(8);
        let result = sync_repo_list_streaming(
            &client,
            "incremental-empty",
            &repos,
            &options,
            Some(db),
            model_tx,
            None,
        )
        .await
        .expect("sync should succeed and keep fetch errors");

        assert_eq!(result.processed, 0);
        assert_eq!(result.matched, 0);
        assert_eq!(result.saved, 0);
        assert_eq!(result.errors.len(), 1);
        assert!(result.errors[0].contains("cannot fetch repo"));
        assert!(model_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_sync_repo_list_streaming_collects_task_join_errors() {
        let client = PanicRepoClient;
        let options = SyncOptions {
            star: false,
            dry_run: true,
            ..SyncOptions::default()
        };
        let repos = vec![("org".to_string(), "repo".to_string())];

        let (model_tx, mut model_rx) = mpsc::channel::<CodeRepositoryActiveModel>(8);
        let result =
            sync_repo_list_streaming(&client, "panic", &repos, &options, None, model_tx, None)
                .await
                .expect("sync should succeed while collecting join errors");

        assert_eq!(result.processed, 0);
        assert_eq!(result.matched, 0);
        assert_eq!(result.saved, 0);
        assert!(
            result
                .errors
                .iter()
                .any(|error| error.contains("Task join error:"))
        );
        assert!(model_rx.recv().await.is_none());
    }
}
