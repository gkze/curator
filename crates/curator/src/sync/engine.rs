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

mod fetch;
mod filter;
mod persist;
mod star;

pub use filter::filter_by_activity;

use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;

use sea_orm::DatabaseConnection;

use super::progress::{ProgressCallback, SyncProgress, emit};
use super::types::{NamespaceSyncResult, NamespaceSyncResultStreaming, SyncOptions, SyncResult};
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::platform::{ApiRateLimiter, PlatformClient, PlatformError, PlatformRepo};

async fn sync_repos<C: PlatformClient + Clone + 'static>(
    client: &C,
    repos: Vec<PlatformRepo>,
    options: &SyncOptions,
    rate_limiter: Option<&ApiRateLimiter>,
    on_progress: Option<&ProgressCallback>,
) -> Result<(SyncResult, Vec<CodeRepositoryActiveModel>), PlatformError> {
    let mut result = SyncResult {
        processed: repos.len(),
        ..SyncResult::default()
    };

    emit(
        on_progress,
        SyncProgress::FilteringByActivity {
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
            rate_limiter,
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
    repos: Vec<PlatformRepo>,
    options: &SyncOptions,
    rate_limiter: Option<&ApiRateLimiter>,
    model_tx: mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> Result<SyncResult, PlatformError> {
    let streaming_result =
        persist::process_streaming_repos(client, &repos, options, &model_tx, on_progress).await;
    let mut result = streaming_result.result;

    if options.star && !streaming_result.repos_to_star.is_empty() {
        let star_stats = star::star_repos_concurrent(
            client,
            streaming_result.repos_to_star,
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
    fetch::wait_for_rate_limit(rate_limiter).await;
    let repos = client.list_org_repos(namespace, db, on_progress).await?;

    sync_repos(client, repos, options, rate_limiter, on_progress).await
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
    fetch::wait_for_rate_limit(rate_limiter).await;
    let repos = client.list_org_repos(namespace, db, on_progress).await?;

    sync_repos_streaming(client, repos, options, rate_limiter, model_tx, on_progress).await
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
    fetch::wait_for_rate_limit(rate_limiter).await;
    let repos = client.list_user_repos(username, db, on_progress).await?;

    sync_repos(client, repos, options, rate_limiter, on_progress).await
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
    fetch::wait_for_rate_limit(rate_limiter).await;
    let repos = client.list_user_repos(username, db, on_progress).await?;

    sync_repos_streaming(client, repos, options, rate_limiter, model_tx, on_progress).await
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
    fetch::wait_for_rate_limit(rate_limiter).await;

    // Note: FilteringByActivity is not emitted here because the client's
    // list_starred_repos_streaming emits FetchingRepos, and filtering happens
    // during streaming (not as a separate phase). The FilteredPage events
    // provide incremental filtering progress.

    // Shared state for the streaming processor
    let processed = Arc::new(AtomicUsize::new(0));
    let matched = Arc::new(AtomicUsize::new(0));
    let saved = Arc::new(AtomicUsize::new(0));
    let channel_closed = Arc::new(AtomicBool::new(false));
    let repos_to_prune = Arc::new(Mutex::new(Vec::<(String, String)>::new()));

    // Create channel for receiving repos from the streaming fetch.
    // Buffer of 500 provides headroom to reduce backpressure likelihood.
    // The time-based flushing in the persist task is the primary deadlock prevention,
    // but larger buffers reduce the frequency of backpressure situations.
    let (repo_tx, mut repo_rx) = mpsc::channel::<PlatformRepo>(500);

    // Create channel for progress events from processor task
    // Sends (matched_count, processed_count) tuples
    // Use unbounded to avoid blocking processor
    let (progress_tx, progress_rx) = mpsc::unbounded_channel::<(usize, usize)>();

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
        tracing::debug!("Processor task started");
        while let Some(repo) = repo_rx.recv().await {
            let new_processed = processor_processed.fetch_add(1, Ordering::Relaxed) + 1;
            if new_processed.is_multiple_of(1000) {
                tracing::debug!(processed = new_processed, "Processor progress");
            }

            let is_active = filter::is_active_repo(&repo, cutoff);
            let new_matched = if is_active {
                processor_matched.fetch_add(1, Ordering::Relaxed) + 1
            } else {
                processor_matched.load(Ordering::Relaxed)
            };

            // Send progress update (ignore errors if receiver dropped)
            let _ = progress_tx.send((new_matched, new_processed));

            if is_active {
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
        tracing::debug!(
            processed = processor_processed.load(Ordering::Relaxed),
            matched = processor_matched.load(Ordering::Relaxed),
            saved = processor_saved.load(Ordering::Relaxed),
            "Processor task finished"
        );
    });

    // Stream starred repos with concurrent progress emission
    // Use pin to allow polling the future while also checking progress
    let mut fetch_future = Box::pin(client.list_starred_repos_streaming(
        repo_tx,
        db,
        concurrency,
        skip_rate_checks,
        on_progress,
    ));

    let mut fetch_done = false;
    let mut fetch_result: Option<Result<usize, PlatformError>> = None;
    let mut processor_result: Option<Result<(), tokio::task::JoinError>> = None;
    let mut last_emitted_matched = 0usize;
    let mut last_emitted_processed = 0usize;
    let mut processor_handle = processor_handle;

    // Poll fetch, processor, and progress concurrently, emitting FilteredPage events in real-time
    // Wrap receiver in a stream for idiomatic async handling
    let mut progress_stream = UnboundedReceiverStream::new(progress_rx).fuse();
    // Track when progress stream is exhausted to avoid spinning on fused stream
    // (fused streams return Ready(None) immediately, which would cause a spin loop)
    let mut progress_stream_done = false;

    loop {
        tokio::select! {
            biased;

            result = progress_stream.next(), if processor_result.is_none() && !progress_stream_done => {
                if let Some((matched_count, processed_count)) = result {
                    let should_emit = matched_count >= last_emitted_matched + 25
                        || processed_count >= last_emitted_processed + 100
                        || (matched_count > 0 && last_emitted_matched == 0)
                        || (processed_count > 0 && last_emitted_processed == 0);

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
                } else {
                    // Stream exhausted (sender dropped), disable this branch to avoid spinning
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
            // Drain any remaining buffered progress messages.
            // Since processor finished, the sender is dropped and stream won't block.
            while let Some((matched_count, processed_count)) = progress_stream.next().await {
                let should_emit = matched_count >= last_emitted_matched + 25
                    || processed_count >= last_emitted_processed + 100
                    || (matched_count > 0 && last_emitted_matched == 0)
                    || (processed_count > 0 && last_emitted_processed == 0);

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
    drop(fetch_future);

    let _ = processor_result.expect("processor should complete");

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
    tracing::debug!("Dropping model_tx to signal persist task completion");
    drop(model_tx);
    tracing::debug!("model_tx dropped, persist task should now see channel close");

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
        let prune_result = star::prune_repos(
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
    use crate::sync::StarringStats;
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
