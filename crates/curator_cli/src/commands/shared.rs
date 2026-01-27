//! Shared utilities for CLI command handlers.
//!
//! This module provides CLI-specific wrappers around the library's sync infrastructure,
//! injecting CLI concerns like the global shutdown flag.
//!
//! # SyncRunner
//!
//! The [`SyncRunner`] struct provides a unified interface for running sync operations
//! across all platforms (GitHub, GitLab, Gitea). It handles:
//!
//! - Progress reporter setup (auto-detects TTY)
//! - Rate limiter configuration
//! - Persist task management (channel creation, spawning, awaiting)
//! - Result aggregation for multi-namespace syncs
//! - Output formatting (TTY vs non-TTY)
//! - Shutdown handling

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize},
};

use console::Term;
use curator::entity::code_platform::CodePlatform;
use curator::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use curator::platform::{ApiRateLimiter, PlatformClient};
use curator::repository;
use curator::sync::{
    NamespaceSyncResultStreaming, ProgressCallback, SyncOptions, SyncResult,
    sync_namespace_streaming, sync_namespaces_streaming, sync_starred_streaming,
    sync_user_streaming, sync_users_streaming,
};
use sea_orm::DatabaseConnection;
use tokio::sync::mpsc;

use crate::progress::ProgressReporter;
use crate::shutdown::{SHUTDOWN_FLAG, is_shutdown_requested};

// Re-export types from the library for convenience
pub use curator::sync::{
    MODEL_CHANNEL_BUFFER_SIZE, PersistTaskResult, await_persist_task, display_persist_errors,
};

/// Spawn a persist task with the CLI's global shutdown flag.
///
/// This is a thin wrapper around the library's `spawn_persist_task` that injects
/// the CLI's global shutdown flag for graceful shutdown handling.
pub(crate) fn spawn_persist_task(
    db: Arc<DatabaseConnection>,
    rx: mpsc::Receiver<CodeRepositoryActiveModel>,
    on_progress: Option<Arc<ProgressCallback>>,
) -> (tokio::task::JoinHandle<PersistTaskResult>, Arc<AtomicUsize>) {
    // Get the global shutdown flag
    let shutdown_flag: Arc<AtomicBool> = Arc::clone(&SHUTDOWN_FLAG);

    curator::sync::spawn_persist_task(db, rx, Some(shutdown_flag), on_progress)
}

/// Create a rate limiter if rate limiting is enabled.
/// Returns None if no_rate_limit is true, Some(limiter) otherwise.
pub(crate) fn maybe_rate_limiter(no_rate_limit: bool, rps: u32) -> Option<ApiRateLimiter> {
    if no_rate_limit {
        None
    } else {
        Some(ApiRateLimiter::new(rps))
    }
}

/// Print a warning when rate limiting is disabled (TTY only).
pub(crate) fn warn_no_rate_limit(is_tty: bool) {
    if is_tty {
        eprintln!("Warning: Rate limiting disabled - you may experience API throttling\n");
    }
}

/// Display final rate limit status with a timeout to avoid hangs.
pub(crate) async fn display_final_rate_limit<C: PlatformClient>(
    client: &C,
    is_tty: bool,
    no_rate_limit: bool,
) {
    if no_rate_limit {
        return;
    }

    let rate_limit =
        tokio::time::timeout(std::time::Duration::from_secs(5), client.get_rate_limit()).await;

    match rate_limit {
        Ok(Ok(final_rate)) => {
            if is_tty {
                println!(
                    "\nRate limit after sync: {}/{} remaining",
                    final_rate.remaining, final_rate.limit
                );
            } else {
                tracing::info!(
                    remaining = final_rate.remaining,
                    limit = final_rate.limit,
                    "Rate limit after sync"
                );
            }
        }
        Ok(Err(error)) => {
            if is_tty {
                eprintln!("Warning: Failed to fetch rate limit after sync: {error}");
            } else {
                tracing::warn!(error = %error, "Failed to fetch rate limit after sync");
            }
        }
        Err(_) => {
            if is_tty {
                eprintln!("Warning: Timed out fetching rate limit after sync");
            } else {
                tracing::warn!("Timed out fetching rate limit after sync");
            }
        }
    }
}

/// The type of sync operation being performed.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // Starred variant reserved for future starred sync support
pub enum SyncKind {
    /// Sync organization/group repositories.
    Namespace,
    /// Sync user repositories.
    User,
    /// Sync starred repositories.
    Starred,
}

/// Aggregated results from a sync operation.
#[derive(Debug, Default)]
pub struct AggregatedSyncResult {
    /// Total repositories processed across all namespaces.
    pub total_processed: usize,
    /// Total repositories matched (active) across all namespaces.
    pub total_matched: usize,
    /// Total repositories starred across all namespaces.
    pub total_starred: usize,
    /// Total repositories skipped (already starred) across all namespaces.
    pub total_skipped: usize,
    /// Total repositories pruned (for starred sync).
    pub total_pruned: usize,
    /// Pruned repos for database deletion.
    pub pruned_repos: Vec<(String, String)>,
    /// All errors encountered during sync.
    pub all_errors: Vec<String>,
    /// Persist task result.
    pub persist_result: PersistTaskResult,
    /// Number of records deleted from database (for pruned repos).
    pub deleted: usize,
}

impl AggregatedSyncResult {
    /// Create from a single sync result.
    pub fn from_single(result: SyncResult, persist_result: PersistTaskResult) -> Self {
        Self {
            total_processed: result.processed,
            total_matched: result.matched,
            total_starred: result.starred,
            total_skipped: result.skipped,
            total_pruned: result.pruned,
            pruned_repos: result.pruned_repos,
            all_errors: result.errors,
            persist_result,
            deleted: 0,
        }
    }

    /// Create from multiple namespace results.
    pub fn from_multiple(
        results: &[NamespaceSyncResultStreaming],
        persist_result: PersistTaskResult,
    ) -> Self {
        let mut agg = Self {
            persist_result,
            ..Default::default()
        };

        for ns_result in results {
            if let Some(err) = &ns_result.error {
                agg.all_errors
                    .push(format!("{}: {}", ns_result.namespace, err));
                continue;
            }

            let result = &ns_result.result;
            agg.total_processed += result.processed;
            agg.total_matched += result.matched;
            agg.total_starred += result.starred;
            agg.total_skipped += result.skipped;
            agg.total_pruned += result.pruned;
            agg.pruned_repos.extend(result.pruned_repos.iter().cloned());

            for err in &result.errors {
                agg.all_errors
                    .push(format!("{}: {}", ns_result.namespace, err));
            }
        }

        agg
    }
}

/// A unified sync runner that handles the common sync workflow.
///
/// This struct encapsulates the boilerplate code shared across all CLI handlers:
/// - Progress reporter creation
/// - Rate limiter setup
/// - Persist task management
/// - Result aggregation
/// - Output formatting
pub struct SyncRunner {
    /// Database connection.
    db: Arc<DatabaseConnection>,
    /// Sync options.
    options: SyncOptions,
    /// Rate limiter (if enabled).
    rate_limiter: Option<ApiRateLimiter>,
    /// Progress reporter.
    reporter: Arc<ProgressReporter>,
    /// Progress callback for the library.
    progress: Arc<ProgressCallback>,
    /// Whether we're in a TTY.
    is_tty: bool,
    /// Active within days (for display).
    active_within_days: u64,
    /// Whether rate limiting is disabled.
    no_rate_limit: bool,
}

impl SyncRunner {
    /// Create a new SyncRunner with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `db` - Database connection
    /// * `options` - Sync options
    /// * `no_rate_limit` - Whether rate limiting is disabled
    /// * `default_rps` - Default requests per second for rate limiting
    /// * `active_within_days` - Number of days for activity filter (for display)
    pub fn new(
        db: Arc<DatabaseConnection>,
        options: SyncOptions,
        no_rate_limit: bool,
        default_rps: u32,
        active_within_days: u64,
    ) -> Self {
        let is_tty = Term::stdout().is_term();
        let reporter = Arc::new(ProgressReporter::new());
        let progress = reporter.as_callback();
        let rate_limiter = maybe_rate_limiter(no_rate_limit, default_rps);

        if options.dry_run && is_tty {
            println!("DRY RUN - no changes will be made\n");
        }

        if no_rate_limit {
            warn_no_rate_limit(is_tty);
        }

        Self {
            db,
            options,
            rate_limiter,
            reporter,
            progress,
            is_tty,
            active_within_days,
            no_rate_limit,
        }
    }

    /// Get whether we're running in a TTY.
    pub fn is_tty(&self) -> bool {
        self.is_tty
    }

    /// Get whether rate limiting is disabled.
    pub fn no_rate_limit(&self) -> bool {
        self.no_rate_limit
    }

    /// Run a single namespace sync (org/group).
    pub async fn run_namespace<C: PlatformClient + Clone + 'static>(
        &self,
        client: &C,
        namespace: &str,
    ) -> Result<AggregatedSyncResult, Box<dyn std::error::Error>> {
        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);
        let persist_handle = if !self.options.dry_run {
            let (handle, _counter) =
                spawn_persist_task(Arc::clone(&self.db), rx, Some(Arc::clone(&self.progress)));
            Some(handle)
        } else {
            drop(rx);
            None
        };

        let result = sync_namespace_streaming(
            client,
            namespace,
            &self.options,
            self.rate_limiter.as_ref(),
            Some(&*self.db),
            tx,
            Some(&*self.progress),
        )
        .await?;

        let persist_result = if let Some(handle) = persist_handle {
            await_persist_task(handle).await
        } else {
            PersistTaskResult::default()
        };

        self.reporter.finish();

        Ok(AggregatedSyncResult::from_single(result, persist_result))
    }

    /// Run multiple namespace syncs concurrently.
    pub async fn run_namespaces<C: PlatformClient + Clone + 'static>(
        &self,
        client: &C,
        namespaces: &[String],
    ) -> AggregatedSyncResult {
        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);
        let persist_handle = if !self.options.dry_run {
            let (handle, _counter) =
                spawn_persist_task(Arc::clone(&self.db), rx, Some(Arc::clone(&self.progress)));
            Some(handle)
        } else {
            drop(rx);
            None
        };

        let results = sync_namespaces_streaming(
            client,
            namespaces,
            &self.options,
            self.rate_limiter.as_ref(),
            Some(Arc::clone(&self.db)), // Pass db for ETag caching in concurrent syncs
            tx,
            Some(&*self.progress),
        )
        .await;

        let persist_result = if let Some(handle) = persist_handle {
            await_persist_task(handle).await
        } else {
            PersistTaskResult::default()
        };

        self.reporter.finish();

        AggregatedSyncResult::from_multiple(&results, persist_result)
    }

    /// Run a single user sync.
    pub async fn run_user<C: PlatformClient + Clone + 'static>(
        &self,
        client: &C,
        user: &str,
    ) -> Result<AggregatedSyncResult, Box<dyn std::error::Error>> {
        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);
        let persist_handle = if !self.options.dry_run {
            let (handle, _counter) =
                spawn_persist_task(Arc::clone(&self.db), rx, Some(Arc::clone(&self.progress)));
            Some(handle)
        } else {
            drop(rx);
            None
        };

        let result = sync_user_streaming(
            client,
            user,
            &self.options,
            self.rate_limiter.as_ref(),
            Some(&*self.db),
            tx,
            Some(&*self.progress),
        )
        .await?;

        let persist_result = if let Some(handle) = persist_handle {
            await_persist_task(handle).await
        } else {
            PersistTaskResult::default()
        };

        self.reporter.finish();

        Ok(AggregatedSyncResult::from_single(result, persist_result))
    }

    /// Run multiple user syncs concurrently.
    pub async fn run_users<C: PlatformClient + Clone + 'static>(
        &self,
        client: &C,
        users: &[String],
    ) -> AggregatedSyncResult {
        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);
        let persist_handle = if !self.options.dry_run {
            let (handle, _counter) =
                spawn_persist_task(Arc::clone(&self.db), rx, Some(Arc::clone(&self.progress)));
            Some(handle)
        } else {
            drop(rx);
            None
        };

        let results = sync_users_streaming(
            client,
            users,
            &self.options,
            self.rate_limiter.as_ref(),
            Some(Arc::clone(&self.db)), // Pass db for ETag caching in concurrent syncs
            tx,
            Some(&*self.progress),
        )
        .await;

        let persist_result = if let Some(handle) = persist_handle {
            await_persist_task(handle).await
        } else {
            PersistTaskResult::default()
        };

        self.reporter.finish();

        AggregatedSyncResult::from_multiple(&results, persist_result)
    }

    /// Run a starred repositories sync.
    pub async fn run_starred<C: PlatformClient + Clone + 'static>(
        &self,
        client: &C,
        platform: CodePlatform,
    ) -> Result<AggregatedSyncResult, Box<dyn std::error::Error>> {
        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);
        let persist_handle = if !self.options.dry_run {
            let (handle, _counter) =
                spawn_persist_task(Arc::clone(&self.db), rx, Some(Arc::clone(&self.progress)));
            Some(handle)
        } else {
            drop(rx);
            None
        };

        let result = sync_starred_streaming(
            client,
            &self.options,
            self.rate_limiter.as_ref(),
            Some(&*self.db),
            self.options.concurrency,
            self.no_rate_limit,
            tx,
            Some(&*self.progress),
        )
        .await?;

        let persist_result = if let Some(handle) = persist_handle {
            await_persist_task(handle).await
        } else {
            PersistTaskResult::default()
        };

        // Delete pruned repos from database (unless dry-run)
        let deleted = if !self.options.dry_run && !result.pruned_repos.is_empty() {
            repository::delete_by_owner_name(&self.db, platform, &result.pruned_repos)
                .await
                .unwrap_or(0) as usize
        } else {
            0
        };

        self.reporter.finish();

        let mut agg = AggregatedSyncResult::from_single(result, persist_result);
        agg.deleted = deleted;
        Ok(agg)
    }

    /// Print results for a single namespace/user sync.
    pub fn print_single_result(&self, name: &str, result: &AggregatedSyncResult, kind: SyncKind) {
        let was_interrupted = is_shutdown_requested();
        let saved = result.persist_result.saved_count;

        if self.is_tty {
            if was_interrupted {
                println!("\n(Interrupted by user - partial results below)");
            }

            let entity = match kind {
                SyncKind::Namespace => "repositories",
                SyncKind::User => "repositories",
                SyncKind::Starred => "starred",
            };

            println!("\nSync results for '{}':", name);
            println!("  Total {}:    {}", entity, result.total_processed);
            println!(
                "  Active (last {} days): {}",
                self.active_within_days, result.total_matched
            );

            if self.options.star {
                if self.options.dry_run {
                    println!("  Would star:            {}", result.total_starred);
                    println!("  Already starred:       {}", result.total_skipped);
                } else {
                    println!("  Starred:               {}", result.total_starred);
                    println!("  Already starred:       {}", result.total_skipped);
                }
            }

            if !self.options.dry_run {
                println!("  Saved to database:     {}", saved);
                if result.persist_result.has_errors() {
                    println!(
                        "  Failed to save:        {}",
                        result.persist_result.failed_count()
                    );
                }
            } else {
                println!("  Would save:            {}", result.total_matched);
            }

            if !result.all_errors.is_empty() {
                println!("\nSync errors:");
                for err in &result.all_errors {
                    println!("  - {}", err);
                }
            }

            display_persist_errors(&result.persist_result, self.is_tty);
        } else {
            tracing::info!(
                name = %name,
                processed = result.total_processed,
                matched = result.total_matched,
                starred = result.total_starred,
                skipped = result.total_skipped,
                saved = saved,
                persist_errors = result.persist_result.failed_count(),
                errors = result.all_errors.len(),
                "Sync complete"
            );
            display_persist_errors(&result.persist_result, self.is_tty);
        }
    }

    /// Print aggregated results for multiple namespace/user syncs.
    pub fn print_multi_result(&self, count: usize, result: &AggregatedSyncResult, kind: SyncKind) {
        let was_interrupted = is_shutdown_requested();
        let total_saved = result.persist_result.saved_count;

        if self.is_tty {
            if was_interrupted {
                println!("\n(Interrupted by user - partial results below)");
            }

            let entity = match kind {
                SyncKind::Namespace => "repositories",
                SyncKind::User => "repositories",
                SyncKind::Starred => "starred",
            };

            println!("\n=== SUMMARY ===");
            println!("Total {} processed: {}", entity, result.total_processed);
            println!(
                "Total active (last {} days):  {}",
                self.active_within_days, result.total_matched
            );

            if self.options.star {
                if self.options.dry_run {
                    println!("Total would star:             {}", result.total_starred);
                    println!("Total already starred:        {}", result.total_skipped);
                } else {
                    println!("Total starred:                {}", result.total_starred);
                    println!("Total already starred:        {}", result.total_skipped);
                }
            }

            if !self.options.dry_run {
                println!("Total saved to database:      {}", total_saved);
                if result.persist_result.has_errors() {
                    println!(
                        "Total failed to save:         {}",
                        result.persist_result.failed_count()
                    );
                }
            } else {
                println!("Total would save:             {}", result.total_matched);
            }

            if !result.all_errors.is_empty() {
                println!("\nSync errors ({}):", result.all_errors.len());
                for err in &result.all_errors {
                    println!("  - {}", err);
                }
            }

            display_persist_errors(&result.persist_result, self.is_tty);
        } else {
            tracing::info!(
                count = count,
                processed = result.total_processed,
                matched = result.total_matched,
                starred = result.total_starred,
                skipped = result.total_skipped,
                saved = total_saved,
                persist_errors = result.persist_result.failed_count(),
                errors = result.all_errors.len(),
                "Sync complete"
            );
            display_persist_errors(&result.persist_result, self.is_tty);
        }
    }

    /// Print results for a starred sync.
    pub fn print_starred_result(&self, result: &AggregatedSyncResult, prune: bool) {
        let was_interrupted = is_shutdown_requested();
        let saved = result.persist_result.saved_count;

        if self.is_tty {
            if was_interrupted {
                println!("\n(Interrupted by user - partial results below)");
            }

            println!("\nSync results for starred repositories:");
            println!("  Total starred:         {}", result.total_processed);
            println!(
                "  Active (last {} days): {}",
                self.active_within_days, result.total_matched
            );

            if self.options.dry_run {
                println!("  Would save:            {}", result.total_matched);
                if prune {
                    println!("  Would prune:           {}", result.total_pruned);
                }
            } else {
                println!("  Saved to database:     {}", saved);
                if result.persist_result.has_errors() {
                    println!(
                        "  Failed to save:        {}",
                        result.persist_result.failed_count()
                    );
                }
                if prune {
                    println!("  Pruned (unstarred):    {}", result.total_pruned);
                    if result.deleted > 0 {
                        println!("  Deleted from database: {}", result.deleted);
                    }
                }
            }

            if !result.all_errors.is_empty() {
                println!("\nSync errors:");
                for err in &result.all_errors {
                    println!("  - {}", err);
                }
            }

            display_persist_errors(&result.persist_result, self.is_tty);
        } else {
            tracing::info!(
                processed = result.total_processed,
                matched = result.total_matched,
                saved = saved,
                persist_errors = result.persist_result.failed_count(),
                pruned = result.total_pruned,
                deleted = result.deleted,
                errors = result.all_errors.len(),
                "Starred sync complete"
            );
            display_persist_errors(&result.persist_result, self.is_tty);
        }
    }
}
