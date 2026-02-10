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

use std::sync::{Arc, atomic::AtomicBool};

use console::Term;
use curator::PlatformType;
use curator::entity::instance::Model as InstanceModel;
use curator::platform::PlatformClient;
use curator::rate_limits;
use curator::repository;
#[cfg(feature = "discovery")]
use curator::sync::{
    NamespaceSyncResultStreaming, ProgressCallback, SyncContext, SyncOptions, SyncResult,
};
use sea_orm::DatabaseConnection;

use crate::config::Config;
use crate::progress::ProgressReporter;
use crate::shutdown::{SHUTDOWN_FLAG, is_shutdown_requested};

/// Buffer time (in seconds) before token expiry to trigger refresh.
/// We refresh 5 minutes early to avoid race conditions.
#[cfg(feature = "gitea")]
const TOKEN_REFRESH_BUFFER_SECS: u64 = 300;

// Re-export types from the library for convenience
pub use curator::sync::{PersistTaskResult, display_persist_errors};

/// Build the platform rate limiter unless disabled.
pub(crate) fn build_rate_limiter(
    platform_type: PlatformType,
    no_rate_limit: bool,
) -> Option<curator::AdaptiveRateLimiter> {
    if no_rate_limit {
        None
    } else {
        Some(curator::AdaptiveRateLimiter::new(
            rate_limits::default_rps_for_platform(platform_type),
        ))
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
    /// Progress reporter.
    reporter: Arc<ProgressReporter>,
    /// Progress callback for the library.
    progress: Arc<ProgressCallback>,
    /// Whether we're in a TTY.
    is_tty: bool,
    /// Active within days (for display).
    active_within_days: u64,
    /// Whether rate limiting is disabled (used for display and skip_rate_checks).
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
    /// * `active_within_days` - Number of days for activity filter (for display)
    pub fn new(
        db: Arc<DatabaseConnection>,
        options: SyncOptions,
        no_rate_limit: bool,
        active_within_days: u64,
    ) -> Self {
        let is_tty = Term::stdout().is_term();
        let reporter = Arc::new(ProgressReporter::new());
        let progress = reporter.as_callback();

        if options.dry_run && is_tty {
            println!("DRY RUN - no changes will be made\n");
        }

        if no_rate_limit && is_tty {
            eprintln!("Warning: Rate limiting disabled - you may experience API throttling\n");
        }

        Self {
            db,
            options,
            reporter,
            progress,
            is_tty,
            active_within_days,
            no_rate_limit,
        }
    }

    /// Get whether we're running in a TTY.
    #[allow(dead_code)]
    pub fn is_tty(&self) -> bool {
        self.is_tty
    }

    /// Get whether rate limiting is disabled.
    #[allow(dead_code)]
    pub fn no_rate_limit(&self) -> bool {
        self.no_rate_limit
    }

    fn sync_context<C: PlatformClient + Clone + 'static>(&self, client: C) -> SyncContext<C> {
        let shutdown_flag: Arc<AtomicBool> = Arc::clone(&SHUTDOWN_FLAG);

        SyncContext::builder()
            .client(client)
            .options(self.options.clone())
            .database(Arc::clone(&self.db))
            .progress(Arc::clone(&self.progress))
            .shutdown_flag(shutdown_flag)
            .build()
            .expect("SyncRunner always provides required SyncContext fields")
    }

    /// Run a single namespace sync (org/group).
    pub async fn run_namespace<C: PlatformClient + Clone + 'static>(
        &self,
        client: &C,
        namespace: &str,
    ) -> Result<AggregatedSyncResult, Box<dyn std::error::Error>> {
        let result = self
            .sync_context(client.clone())
            .sync_namespace_streaming(namespace)
            .await?;

        self.reporter.finish();

        Ok(AggregatedSyncResult::from_single(
            result.sync,
            result.persist,
        ))
    }

    /// Run multiple namespace syncs concurrently.
    pub async fn run_namespaces<C: PlatformClient + Clone + 'static>(
        &self,
        client: &C,
        namespaces: &[String],
    ) -> AggregatedSyncResult {
        let (results, persist_result) = self
            .sync_context(client.clone())
            .sync_namespaces_streaming(namespaces)
            .await;

        self.reporter.finish();

        AggregatedSyncResult::from_multiple(&results, persist_result)
    }

    /// Run a single user sync.
    pub async fn run_user<C: PlatformClient + Clone + 'static>(
        &self,
        client: &C,
        user: &str,
    ) -> Result<AggregatedSyncResult, Box<dyn std::error::Error>> {
        let result = self
            .sync_context(client.clone())
            .sync_user_streaming(user)
            .await?;

        self.reporter.finish();

        Ok(AggregatedSyncResult::from_single(
            result.sync,
            result.persist,
        ))
    }

    /// Run a sync for an explicit repository list.
    #[cfg(feature = "discovery")]
    pub async fn run_repo_list<C: PlatformClient + Clone + 'static>(
        &self,
        client: &C,
        label: &str,
        repos: &[(String, String)],
    ) -> Result<AggregatedSyncResult, Box<dyn std::error::Error>> {
        let result = self
            .sync_context(client.clone())
            .sync_repo_list_streaming(label, repos)
            .await?;

        self.reporter.finish();

        Ok(AggregatedSyncResult::from_single(
            result.sync,
            result.persist,
        ))
    }

    /// Run multiple user syncs concurrently.
    pub async fn run_users<C: PlatformClient + Clone + 'static>(
        &self,
        client: &C,
        users: &[String],
    ) -> AggregatedSyncResult {
        let (results, persist_result) = self
            .sync_context(client.clone())
            .sync_users_streaming(users)
            .await;

        self.reporter.finish();

        AggregatedSyncResult::from_multiple(&results, persist_result)
    }

    /// Run a starred repositories sync.
    pub async fn run_starred<C: PlatformClient + Clone + 'static>(
        &self,
        client: &C,
    ) -> Result<AggregatedSyncResult, Box<dyn std::error::Error>> {
        let result = self
            .sync_context(client.clone())
            .sync_starred_streaming(self.no_rate_limit)
            .await?;

        let mut agg = AggregatedSyncResult::from_single(result.sync, result.persist);

        // Delete pruned repos from database (unless dry-run)
        if !self.options.dry_run && !agg.pruned_repos.is_empty() {
            match repository::delete_by_owner_name(
                &self.db,
                client.instance_id(),
                &agg.pruned_repos,
            )
            .await
            {
                Ok(deleted) => {
                    agg.deleted = deleted as usize;
                }
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        pruned = agg.pruned_repos.len(),
                        "Failed to delete pruned repositories from database"
                    );
                    agg.all_errors.push(format!(
                        "Failed to delete pruned repositories from database: {err}"
                    ));
                }
            }
        }

        self.reporter.finish();
        Ok(agg)
    }

    /// Print results for a single namespace/user sync.
    pub fn print_single_result(&self, name: &str, result: &AggregatedSyncResult, kind: SyncKind) {
        let was_interrupted = is_shutdown_requested();
        let saved = result.persist_result.saved_count;
        let incremental = self.options.strategy == curator::sync::SyncStrategy::Incremental;

        if self.is_tty {
            if was_interrupted {
                println!("\n(Interrupted by user - partial results below)");
            }

            let total_label = match kind {
                SyncKind::Starred => "Total starred",
                _ if incremental => "Updated since last sync",
                _ => "Total repositories",
            };
            let active_label = if incremental {
                format!("Active & updated (last {} days)", self.active_within_days)
            } else {
                format!("Active (last {} days)", self.active_within_days)
            };

            println!("\nSync results for '{}':", name);
            println!("  {}:    {}", total_label, result.total_processed);
            println!("  {}: {}", active_label, result.total_matched);

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
        let incremental = self.options.strategy == curator::sync::SyncStrategy::Incremental;

        if self.is_tty {
            if was_interrupted {
                println!("\n(Interrupted by user - partial results below)");
            }

            let entity = match kind {
                SyncKind::Namespace => "repositories",
                SyncKind::User => "repositories",
                SyncKind::Starred => "starred",
            };
            let total_label = if incremental {
                "Total updated since last sync"
            } else {
                "Total repositories processed"
            };
            let active_label = if incremental {
                format!(
                    "Total active & updated (last {} days)",
                    self.active_within_days
                )
            } else {
                format!("Total active (last {} days)", self.active_within_days)
            };

            println!("\n=== SUMMARY ===");
            if incremental {
                println!("{}: {}", total_label, result.total_processed);
            } else {
                println!("Total {} processed: {}", entity, result.total_processed);
            }
            println!("{}:  {}", active_label, result.total_matched);

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

/// Get the token for an instance, automatically refreshing OAuth tokens if needed.
///
/// For Codeberg (OAuth), this checks if the token is expired or near expiry and
/// attempts to refresh it using the stored refresh token. The new tokens are
/// saved to the config file.
///
/// For other platforms or non-OAuth tokens (PATs), this simply returns the
/// configured token.
///
/// # Arguments
///
/// * `instance` - The instance to get the token for
/// * `config` - The CLI configuration containing tokens
///
/// # Returns
///
/// The valid access token, or an error if no token is configured or refresh fails.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub async fn get_token_for_instance(
    instance: &InstanceModel,
    config: &Config,
) -> Result<String, Box<dyn std::error::Error>> {
    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => config.github_token().ok_or_else(|| {
            format!(
                "No GitHub token configured. Run 'curator login {}' or set CURATOR_GITHUB_TOKEN.",
                instance.name
            )
            .into()
        }),
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => config.gitlab_token().ok_or_else(|| {
            format!(
                "No GitLab token configured. Run 'curator login {}' or set CURATOR_GITLAB_TOKEN.",
                instance.name
            )
            .into()
        }),
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            if instance.is_codeberg() {
                // Check if we need to refresh the Codeberg OAuth token
                get_codeberg_token_with_refresh(config).await
            } else {
                // Self-hosted Gitea uses PAT (no refresh needed)
                config.gitea_token().ok_or_else(|| {
                    format!(
                        "No Gitea token configured for '{}'. Run 'curator login {}' or set CURATOR_GITEA_TOKEN.",
                        instance.name, instance.name
                    )
                    .into()
                })
            }
        }
        #[allow(unreachable_patterns)]
        _ => Err(format!(
            "Platform type '{}' not supported. Enable the appropriate feature.",
            instance.platform_type
        )
        .into()),
    }
    .or_else(|e| {
        // Fallback: try ~/.netrc for the instance host
        if let Some(token) = read_netrc_token(&instance.host) {
            Ok(token)
        } else {
            Err(e)
        }
    })
}

/// Read a token from ~/.netrc for the given host.
///
/// Parses the standard netrc format used by git, curl, and other tools:
/// ```text
/// machine github.com
///   login user
///   password ghp_xxx
/// ```
///
/// Returns the `password` value for the matching `machine` entry, or `None`.
fn read_netrc_token(host: &str) -> Option<String> {
    let home = std::env::var("HOME").ok()?;
    let path = std::path::Path::new(&home).join(".netrc");
    let content = std::fs::read_to_string(path).ok()?;

    let mut in_machine = false;
    let mut tokens = content.split_whitespace().peekable();

    while let Some(token) = tokens.next() {
        match token {
            "machine" => {
                if let Some(&machine) = tokens.peek() {
                    in_machine = machine == host;
                    tokens.next();
                }
            }
            "password" if in_machine => {
                return tokens.next().map(|s| s.to_string());
            }
            // Also support "default" entry as a last resort
            "default" if !in_machine => {
                // Continue scanning for password in the default block
                in_machine = true;
            }
            _ => {}
        }
    }

    None
}

/// Get the Codeberg token, refreshing if expired or near expiry.
///
/// This function:
/// 1. Checks if a token exists
/// 2. If a refresh token exists, checks if the access token is expired/near expiry
/// 3. If refresh is needed and possible, refreshes and saves new tokens
/// 4. Returns the (possibly refreshed) access token
#[cfg(feature = "gitea")]
async fn get_codeberg_token_with_refresh(
    config: &Config,
) -> Result<String, Box<dyn std::error::Error>> {
    use curator::gitea::oauth::{CodebergAuth, refresh_access_token, token_expires_at};
    use curator::oauth::token_is_expired;

    // Get the current token (if any)
    let current_token = config.codeberg_token();
    let refresh_token = config.codeberg_refresh_token();
    let expires_at = config.codeberg_token_expires_at();

    // If we have a refresh token and the access token is expired/near expiry, try to refresh
    if let Some(ref rt) = refresh_token
        && token_is_expired(expires_at, TOKEN_REFRESH_BUFFER_SECS)
    {
        tracing::info!("Codeberg token expired or near expiry, attempting refresh...");

        match refresh_access_token(&CodebergAuth::new(), rt).await {
            Ok(new_tokens) => {
                // Calculate new expiry
                let new_expires_at = token_expires_at(&new_tokens);

                // Save the new tokens
                Config::save_codeberg_oauth_tokens(
                    &new_tokens.access_token,
                    new_tokens.refresh_token.as_deref(),
                    new_expires_at,
                )?;

                tracing::info!("Successfully refreshed Codeberg token");
                return Ok(new_tokens.access_token);
            }
            Err(e) => {
                // If refresh fails and we have a current token, warn but try to use it
                // (it might still work if the expiry check was overly aggressive)
                if current_token.is_some() {
                    tracing::warn!(
                        "Failed to refresh Codeberg token: {}. Trying existing token...",
                        e
                    );
                } else {
                    return Err(format!(
                        "Codeberg token expired and refresh failed: {}. Run 'curator login codeberg' to re-authenticate.",
                        e
                    )
                    .into());
                }
            }
        }
    }

    // Return the current token if we have one
    current_token.ok_or_else(|| {
        "No Codeberg token configured. Run 'curator login codeberg' or set CURATOR_CODEBERG_TOKEN."
            .into()
    })
}
