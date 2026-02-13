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
    let mut in_default = false;
    let mut default_password: Option<String> = None;
    let mut tokens = content.split_whitespace().peekable();

    while let Some(token) = tokens.next() {
        match token {
            "machine" => {
                if let Some(&machine) = tokens.peek() {
                    in_machine = machine == host;
                    in_default = false;
                    tokens.next();
                }
            }
            "password" if in_machine => {
                return tokens.next().map(|s| s.to_string());
            }
            "password" if in_default => {
                if default_password.is_none() {
                    default_password = tokens.next().map(|s| s.to_string());
                } else {
                    tokens.next();
                }
            }
            // Also support "default" entry as a last resort
            "default" if !in_machine => {
                // Continue scanning for password in the default block
                in_default = true;
            }
            _ => {}
        }
    }

    default_password
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

#[cfg(test)]
#[allow(clippy::await_holding_lock)] // env_lock guards are intentionally held across awaits to serialise env-mutating tests
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use curator::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
    use curator::platform::{OrgInfo, PlatformError, PlatformRepo, RateLimitInfo, UserInfo};
    use curator::sync::{NamespaceSyncResultStreaming, SyncResult};
    use sea_orm::Database;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, OnceLock};
    use tokio::sync::mpsc;
    use uuid::Uuid;

    #[derive(Clone)]
    enum RateLimitBehavior {
        Ok,
        Err,
        Never,
    }

    #[derive(Clone)]
    struct FakeRateLimitClient {
        behavior: RateLimitBehavior,
        calls: Arc<AtomicUsize>,
    }

    impl FakeRateLimitClient {
        fn new(behavior: RateLimitBehavior) -> Self {
            Self {
                behavior,
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl PlatformClient for FakeRateLimitClient {
        fn platform_type(&self) -> PlatformType {
            PlatformType::GitHub
        }

        fn instance_id(&self) -> Uuid {
            Uuid::nil()
        }

        async fn get_rate_limit(&self) -> curator::platform::Result<RateLimitInfo> {
            self.calls.fetch_add(1, Ordering::SeqCst);

            match self.behavior {
                RateLimitBehavior::Ok => Ok(RateLimitInfo {
                    limit: 5000,
                    remaining: 4999,
                    reset_at: Utc::now(),
                    retry_after: None,
                }),
                RateLimitBehavior::Err => Err(PlatformError::api("boom")),
                RateLimitBehavior::Never => std::future::pending().await,
            }
        }

        async fn get_org_info(&self, _org: &str) -> curator::platform::Result<OrgInfo> {
            unimplemented!()
        }

        async fn get_authenticated_user(&self) -> curator::platform::Result<UserInfo> {
            unimplemented!()
        }

        async fn get_repo(
            &self,
            _owner: &str,
            _name: &str,
            _db: Option<&DatabaseConnection>,
        ) -> curator::platform::Result<PlatformRepo> {
            unimplemented!()
        }

        async fn list_org_repos(
            &self,
            _org: &str,
            _db: Option<&DatabaseConnection>,
            _on_progress: Option<&curator::sync::ProgressCallback>,
        ) -> curator::platform::Result<Vec<PlatformRepo>> {
            unimplemented!()
        }

        async fn list_user_repos(
            &self,
            _username: &str,
            _db: Option<&DatabaseConnection>,
            _on_progress: Option<&curator::sync::ProgressCallback>,
        ) -> curator::platform::Result<Vec<PlatformRepo>> {
            unimplemented!()
        }

        async fn is_repo_starred(
            &self,
            _owner: &str,
            _name: &str,
        ) -> curator::platform::Result<bool> {
            unimplemented!()
        }

        async fn star_repo(&self, _owner: &str, _name: &str) -> curator::platform::Result<bool> {
            unimplemented!()
        }

        async fn star_repo_with_retry(
            &self,
            _owner: &str,
            _name: &str,
            _on_progress: Option<&curator::sync::ProgressCallback>,
        ) -> curator::platform::Result<bool> {
            unimplemented!()
        }

        async fn unstar_repo(&self, _owner: &str, _name: &str) -> curator::platform::Result<bool> {
            unimplemented!()
        }

        async fn list_starred_repos(
            &self,
            _db: Option<&DatabaseConnection>,
            _concurrency: usize,
            _skip_rate_checks: bool,
            _on_progress: Option<&curator::sync::ProgressCallback>,
        ) -> curator::platform::Result<Vec<PlatformRepo>> {
            unimplemented!()
        }

        async fn list_starred_repos_streaming(
            &self,
            _repo_tx: mpsc::Sender<PlatformRepo>,
            _db: Option<&DatabaseConnection>,
            _concurrency: usize,
            _skip_rate_checks: bool,
            _on_progress: Option<&curator::sync::ProgressCallback>,
        ) -> curator::platform::Result<usize> {
            unimplemented!()
        }

        fn to_active_model(&self, _repo: &PlatformRepo) -> CodeRepositoryActiveModel {
            unimplemented!()
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn sample_sync_result() -> SyncResult {
        SyncResult {
            processed: 10,
            matched: 8,
            starred: 3,
            saved: 0,
            skipped: 5,
            pruned: 2,
            pruned_repos: vec![("owner".to_string(), "repo".to_string())],
            errors: vec!["boom".to_string()],
        }
    }

    fn sample_instance(name: &str, platform: PlatformType, host: &str) -> InstanceModel {
        InstanceModel {
            id: Uuid::new_v4(),
            name: name.to_string(),
            platform_type: platform,
            host: host.to_string(),
            oauth_client_id: None,
            oauth_flow: "auto".to_string(),
            created_at: Utc::now().fixed_offset(),
        }
    }

    #[test]
    fn build_rate_limiter_respects_disable_flag() {
        assert!(build_rate_limiter(PlatformType::GitHub, true).is_none());
        assert!(build_rate_limiter(PlatformType::GitHub, false).is_some());
    }

    #[test]
    fn aggregated_sync_result_from_single_copies_fields() {
        let persist = PersistTaskResult {
            saved_count: 7,
            errors: Vec::new(),
            panic_info: None,
        };
        let sync = sample_sync_result();

        let agg = AggregatedSyncResult::from_single(sync, persist);

        assert_eq!(agg.total_processed, 10);
        assert_eq!(agg.total_matched, 8);
        assert_eq!(agg.total_starred, 3);
        assert_eq!(agg.total_skipped, 5);
        assert_eq!(agg.total_pruned, 2);
        assert_eq!(agg.pruned_repos.len(), 1);
        assert_eq!(agg.all_errors, vec!["boom".to_string()]);
        assert_eq!(agg.persist_result.saved_count, 7);
    }

    #[test]
    fn aggregated_sync_result_from_multiple_aggregates_successes_and_errors() {
        let ok_result = NamespaceSyncResultStreaming {
            namespace: "good".to_string(),
            result: SyncResult {
                processed: 2,
                matched: 1,
                starred: 1,
                saved: 0,
                skipped: 0,
                pruned: 0,
                pruned_repos: vec![],
                errors: vec!["minor".to_string()],
            },
            error: None,
        };
        let failed_result = NamespaceSyncResultStreaming {
            namespace: "bad".to_string(),
            result: SyncResult::default(),
            error: Some("network failed".to_string()),
        };

        let agg = AggregatedSyncResult::from_multiple(
            &[ok_result, failed_result],
            PersistTaskResult::default(),
        );

        assert_eq!(agg.total_processed, 2);
        assert_eq!(agg.total_matched, 1);
        assert_eq!(agg.total_starred, 1);
        assert!(agg.all_errors.iter().any(|e| e.contains("good: minor")));
        assert!(
            agg.all_errors
                .iter()
                .any(|e| e.contains("bad: network failed"))
        );
    }

    #[test]
    fn aggregated_sync_result_from_multiple_skips_counts_for_failed_namespace_entries() {
        let failed_with_counts = NamespaceSyncResultStreaming {
            namespace: "bad".to_string(),
            result: SyncResult {
                processed: 99,
                matched: 88,
                starred: 77,
                saved: 0,
                skipped: 66,
                pruned: 55,
                pruned_repos: vec![("o".to_string(), "r".to_string())],
                errors: vec!["should-not-be-included".to_string()],
            },
            error: Some("hard failure".to_string()),
        };

        let agg = AggregatedSyncResult::from_multiple(
            &[failed_with_counts],
            PersistTaskResult::default(),
        );

        assert_eq!(agg.total_processed, 0);
        assert_eq!(agg.total_matched, 0);
        assert_eq!(agg.total_starred, 0);
        assert_eq!(agg.total_skipped, 0);
        assert_eq!(agg.total_pruned, 0);
        assert!(agg.pruned_repos.is_empty());
        assert_eq!(agg.all_errors, vec!["bad: hard failure".to_string()]);
    }

    #[test]
    fn aggregated_sync_result_from_multiple_accumulates_pruned_and_skipped_counts() {
        let first = NamespaceSyncResultStreaming {
            namespace: "first".to_string(),
            result: SyncResult {
                processed: 3,
                matched: 2,
                starred: 1,
                saved: 0,
                skipped: 4,
                pruned: 1,
                pruned_repos: vec![("org-a".to_string(), "repo-a".to_string())],
                errors: vec![],
            },
            error: None,
        };
        let second = NamespaceSyncResultStreaming {
            namespace: "second".to_string(),
            result: SyncResult {
                processed: 5,
                matched: 4,
                starred: 2,
                saved: 0,
                skipped: 6,
                pruned: 2,
                pruned_repos: vec![("org-b".to_string(), "repo-b".to_string())],
                errors: vec![],
            },
            error: None,
        };

        let agg =
            AggregatedSyncResult::from_multiple(&[first, second], PersistTaskResult::default());

        assert_eq!(agg.total_processed, 8);
        assert_eq!(agg.total_matched, 6);
        assert_eq!(agg.total_starred, 3);
        assert_eq!(agg.total_skipped, 10);
        assert_eq!(agg.total_pruned, 3);
        assert_eq!(
            agg.pruned_repos,
            vec![
                ("org-a".to_string(), "repo-a".to_string()),
                ("org-b".to_string(), "repo-b".to_string())
            ]
        );
    }

    #[test]
    fn read_netrc_token_reads_machine_and_default_entries() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("curator-netrc-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let netrc = dir.join(".netrc");
        std::fs::write(
            &netrc,
            "machine github.com login octo password ghp_machine\ndefault login fallback password ghp_default\n",
        )
        .expect("netrc should be written");

        let original_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
        }

        let machine_token = read_netrc_token("github.com");
        let default_token = read_netrc_token("unknown.example");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        assert_eq!(machine_token.as_deref(), Some("ghp_machine"));
        assert_eq!(default_token.as_deref(), Some("ghp_default"));
    }

    #[test]
    fn read_netrc_token_prefers_machine_when_default_appears_first() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("curator-netrc-order-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let netrc = dir.join(".netrc");
        std::fs::write(
            &netrc,
            "default login fallback password ghp_default\nmachine github.com login octo password ghp_machine\n",
        )
        .expect("netrc should be written");

        let original_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
        }

        let machine_token = read_netrc_token("github.com");
        let default_token = read_netrc_token("unknown.example");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        assert_eq!(machine_token.as_deref(), Some("ghp_machine"));
        assert_eq!(default_token.as_deref(), Some("ghp_default"));
    }

    #[test]
    fn read_netrc_token_keeps_first_default_password_when_multiple_exist() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("curator-netrc-default-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let netrc = dir.join(".netrc");
        std::fs::write(
            &netrc,
            "default login first password token_one password token_two\n",
        )
        .expect("netrc should be written");

        let original_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
        }

        let token = read_netrc_token("missing-host.example");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        assert_eq!(token.as_deref(), Some("token_one"));
    }

    #[test]
    fn read_netrc_token_handles_trailing_machine_keyword_without_host() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir =
            std::env::temp_dir().join(format!("curator-netrc-default-scope-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let netrc = dir.join(".netrc");
        std::fs::write(
            &netrc,
            "default login fallback password ghp_default\nmachine\n",
        )
        .expect("netrc should be written");

        let original_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
        }

        let token = read_netrc_token("unknown.example");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        assert_eq!(token.as_deref(), Some("ghp_default"));
    }

    #[test]
    fn read_netrc_token_ignores_default_keyword_inside_machine_entry() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!(
            "curator-netrc-default-inside-machine-{}",
            Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let netrc = dir.join(".netrc");
        std::fs::write(
            &netrc,
            "machine github.com login octo default password machine_token\n",
        )
        .expect("netrc should be written");

        let original_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
        }

        let token = read_netrc_token("github.com");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        assert_eq!(token.as_deref(), Some("machine_token"));
    }

    #[test]
    fn read_netrc_token_returns_none_for_machine_password_without_value() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir =
            std::env::temp_dir().join(format!("curator-netrc-missing-password-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let netrc = dir.join(".netrc");
        std::fs::write(&netrc, "machine github.com login octo password\n")
            .expect("netrc should be written");

        let original_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
        }

        let token = read_netrc_token("github.com");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        assert!(token.is_none());
    }

    #[test]
    fn read_netrc_token_returns_none_when_home_is_unset() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let original_home = std::env::var("HOME").ok();
        unsafe {
            std::env::remove_var("HOME");
        }

        let token = read_netrc_token("github.com");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        assert!(token.is_none());
    }

    #[test]
    fn read_netrc_token_returns_none_when_netrc_file_is_missing() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir =
            std::env::temp_dir().join(format!("curator-netrc-missing-file-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");

        let original_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
        }

        let token = read_netrc_token("github.com");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        assert!(token.is_none());
    }

    #[tokio::test]
    async fn get_token_for_instance_returns_platform_error_without_fallbacks() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("curator-netrc-missing-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        std::fs::write(dir.join(".netrc"), "").expect("netrc should be written");

        let original_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
            std::env::remove_var("CURATOR_GITHUB_TOKEN");
        }

        let config = Config::default();
        let instance = sample_instance("github", PlatformType::GitHub, "github.com");
        let err = get_token_for_instance(&instance, &config)
            .await
            .expect_err("missing token should return platform error");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        assert!(
            err.to_string().contains("No GitHub token configured"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn get_token_for_instance_prefers_configured_token() {
        let mut config = Config::default();
        config.github.token = Some("token-from-config".to_string());
        let instance = sample_instance("github", PlatformType::GitHub, "github.com");

        let token = get_token_for_instance(&instance, &config)
            .await
            .expect("token should resolve from config");

        assert_eq!(token, "token-from-config");
    }

    #[tokio::test]
    async fn get_token_for_instance_uses_netrc_fallback() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("curator-netrc-fallback-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let netrc = dir.join(".netrc");
        std::fs::write(
            &netrc,
            "machine github.com login octo password ghp_from_netrc\n",
        )
        .expect("netrc should be written");

        let original_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
            std::env::remove_var("CURATOR_GITHUB_TOKEN");
        }

        let config = Config::default();
        let instance = sample_instance("github", PlatformType::GitHub, "github.com");
        let token = get_token_for_instance(&instance, &config)
            .await
            .expect("token should fallback to netrc");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        assert_eq!(token, "ghp_from_netrc");
    }

    #[tokio::test]
    async fn get_token_for_instance_returns_platform_error_when_netrc_machine_does_not_match() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("curator-netrc-mismatch-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let netrc = dir.join(".netrc");
        std::fs::write(
            &netrc,
            "machine gitlab.com login octo password glpat_from_netrc\n",
        )
        .expect("netrc should be written");

        let original_home = std::env::var("HOME").ok();
        let original_github_token = std::env::var("CURATOR_GITHUB_TOKEN").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
            std::env::remove_var("CURATOR_GITHUB_TOKEN");
        }

        let config = Config::default();
        let instance = sample_instance("github", PlatformType::GitHub, "github.com");
        let err = get_token_for_instance(&instance, &config).await.expect_err(
            "missing github token should keep platform error when netrc does not match",
        );

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }
        match original_github_token {
            Some(token) => unsafe {
                std::env::set_var("CURATOR_GITHUB_TOKEN", token);
            },
            None => unsafe {
                std::env::remove_var("CURATOR_GITHUB_TOKEN");
            },
        }

        assert!(
            err.to_string().contains("No GitHub token configured"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn get_token_for_instance_uses_default_netrc_fallback_when_machine_missing() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir =
            std::env::temp_dir().join(format!("curator-netrc-default-fallback-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let netrc = dir.join(".netrc");
        std::fs::write(
            &netrc,
            "default login fallback password ghp_default_fallback\n",
        )
        .expect("netrc should be written");

        let original_home = std::env::var("HOME").ok();
        let original_github_token = std::env::var("CURATOR_GITHUB_TOKEN").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
            std::env::remove_var("CURATOR_GITHUB_TOKEN");
        }

        let config = Config::default();
        let instance =
            sample_instance("github-enterprise", PlatformType::GitHub, "ghe.example.com");
        let token = get_token_for_instance(&instance, &config)
            .await
            .expect("token should fallback to default netrc entry");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }
        match original_github_token {
            Some(token) => unsafe {
                std::env::set_var("CURATOR_GITHUB_TOKEN", token);
            },
            None => unsafe {
                std::env::remove_var("CURATOR_GITHUB_TOKEN");
            },
        }

        assert_eq!(token, "ghp_default_fallback");
    }

    #[tokio::test]
    async fn get_token_for_instance_ignores_platform_env_without_config_load() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir =
            std::env::temp_dir().join(format!("curator-netrc-env-precedence-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let netrc = dir.join(".netrc");
        std::fs::write(
            &netrc,
            "machine github.com login octo password ghp_from_netrc\ndefault login fallback password ghp_default\n",
        )
        .expect("netrc should be written");

        let original_home = std::env::var("HOME").ok();
        let original_github_token = std::env::var("CURATOR_GITHUB_TOKEN").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
            std::env::set_var("CURATOR_GITHUB_TOKEN", "token-from-env");
        }

        let config = Config::default();
        let instance = sample_instance("github", PlatformType::GitHub, "github.com");
        let token = get_token_for_instance(&instance, &config)
            .await
            .expect("netrc token should be used when config has no token");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }
        match original_github_token {
            Some(token) => unsafe {
                std::env::set_var("CURATOR_GITHUB_TOKEN", token);
            },
            None => unsafe {
                std::env::remove_var("CURATOR_GITHUB_TOKEN");
            },
        }

        assert_eq!(token, "ghp_from_netrc");
    }

    #[tokio::test]
    async fn get_token_for_instance_prefers_config_over_netrc_fallback() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("curator-netrc-precedence-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let netrc = dir.join(".netrc");
        std::fs::write(
            &netrc,
            "machine github.com login octo password ghp_from_netrc\n",
        )
        .expect("netrc should be written");

        let original_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
        }

        let mut config = Config::default();
        config.github.token = Some("token-from-config".to_string());
        let instance = sample_instance("github", PlatformType::GitHub, "github.com");
        let token = get_token_for_instance(&instance, &config)
            .await
            .expect("config token should have precedence");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        assert_eq!(token, "token-from-config");
    }

    #[tokio::test]
    async fn get_token_for_instance_reads_gitlab_and_gitea_tokens_from_config() {
        let mut config = Config::default();

        config.gitlab.token = Some("gitlab-config-token".to_string());
        let gitlab_instance = sample_instance("gitlab", PlatformType::GitLab, "gitlab.com");
        let gitlab_token = get_token_for_instance(&gitlab_instance, &config)
            .await
            .expect("gitlab token should resolve from config");
        assert_eq!(gitlab_token, "gitlab-config-token");

        config.gitea.token = Some("gitea-config-token".to_string());
        let gitea_instance = sample_instance(
            "self-hosted-gitea",
            PlatformType::Gitea,
            "gitea.example.com",
        );
        let gitea_token = get_token_for_instance(&gitea_instance, &config)
            .await
            .expect("self-hosted gitea token should resolve from config");
        assert_eq!(gitea_token, "gitea-config-token");

        config.codeberg.token = Some("codeberg-config-token".to_string());
        let codeberg_instance = sample_instance("codeberg", PlatformType::Gitea, "codeberg.org");
        let codeberg_token = get_token_for_instance(&codeberg_instance, &config)
            .await
            .expect("codeberg token should resolve from config without refresh token");
        assert_eq!(codeberg_token, "codeberg-config-token");
    }

    #[tokio::test]
    async fn get_token_for_instance_returns_gitea_error_without_config_or_netrc() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir =
            std::env::temp_dir().join(format!("curator-netrc-gitea-missing-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        std::fs::write(dir.join(".netrc"), "").expect("netrc should be writable");

        let original_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", &dir);
        }

        let config = Config::default();
        let instance = sample_instance("forgejo-local", PlatformType::Gitea, "forgejo.local");
        let err = get_token_for_instance(&instance, &config)
            .await
            .expect_err("missing gitea token should fail without netrc fallback");

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        assert!(
            err.to_string()
                .contains("No Gitea token configured for 'forgejo-local'"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn sync_runner_accessors_and_print_helpers_are_callable() {
        let path = std::env::temp_dir().join(format!("curator-sync-runner-{}.db", Uuid::new_v4()));
        let url = format!("sqlite://{}?mode=rwc", path.display());
        if let Some(parent) = Path::new(&path).parent() {
            std::fs::create_dir_all(parent).expect("parent should be creatable");
        }

        let db = Database::connect(&url).await.expect("db should connect");
        let runner = SyncRunner::new(Arc::new(db), SyncOptions::default(), true, 60);

        assert!(runner.no_rate_limit());
        // is_tty() may vary by environment; ensure method is callable.
        let _ = runner.is_tty();

        let mut agg =
            AggregatedSyncResult::from_single(sample_sync_result(), PersistTaskResult::default());
        agg.persist_result.saved_count = 2;

        runner.print_single_result("test", &agg, SyncKind::Namespace);
        runner.print_multi_result(1, &agg, SyncKind::User);
        runner.print_starred_result(&agg, true);
    }

    #[tokio::test]
    async fn sync_runner_print_helpers_cover_incremental_starred_and_error_branches() {
        let path = std::env::temp_dir().join(format!(
            "curator-sync-runner-branches-{}.db",
            Uuid::new_v4()
        ));
        let url = format!("sqlite://{}?mode=rwc", path.display());
        if let Some(parent) = Path::new(&path).parent() {
            std::fs::create_dir_all(parent).expect("parent should be creatable");
        }

        let db = Database::connect(&url).await.expect("db should connect");
        let options = SyncOptions {
            star: true,
            dry_run: true,
            strategy: curator::sync::SyncStrategy::Incremental,
            ..SyncOptions::default()
        };

        let mut runner = SyncRunner::new(Arc::new(db), options, false, 14);
        runner.is_tty = true;

        let mut dry_run_result = AggregatedSyncResult::from_single(
            SyncResult {
                processed: 4,
                matched: 2,
                starred: 1,
                saved: 0,
                skipped: 3,
                pruned: 1,
                pruned_repos: vec![],
                errors: vec!["dry-run error".to_string()],
            },
            PersistTaskResult::default(),
        );
        dry_run_result
            .all_errors
            .push("namespace failed".to_string());

        runner.print_single_result("starred", &dry_run_result, SyncKind::Starred);
        runner.print_multi_result(2, &dry_run_result, SyncKind::Starred);
        runner.print_starred_result(&dry_run_result, true);

        runner.options.dry_run = false;
        let mut persisted_result = dry_run_result;
        persisted_result.deleted = 1;
        persisted_result.persist_result.saved_count = 2;
        persisted_result.persist_result.errors = vec![(
            "owner".to_string(),
            "repo".to_string(),
            "persist failed".to_string(),
        )];

        runner.print_single_result("starred", &persisted_result, SyncKind::Namespace);
        runner.print_multi_result(1, &persisted_result, SyncKind::User);
        runner.print_starred_result(&persisted_result, true);
    }

    #[tokio::test]
    async fn display_final_rate_limit_skips_fetch_when_disabled() {
        let client = FakeRateLimitClient::new(RateLimitBehavior::Ok);

        display_final_rate_limit(&client, false, true).await;

        assert_eq!(client.calls(), 0);
    }

    #[tokio::test]
    async fn display_final_rate_limit_fetches_for_success_and_error_results() {
        let ok_client = FakeRateLimitClient::new(RateLimitBehavior::Ok);
        display_final_rate_limit(&ok_client, false, false).await;
        assert_eq!(ok_client.calls(), 1);

        let err_client = FakeRateLimitClient::new(RateLimitBehavior::Err);
        display_final_rate_limit(&err_client, false, false).await;
        assert_eq!(err_client.calls(), 1);
    }

    #[tokio::test]
    async fn display_final_rate_limit_times_out_for_slow_clients() {
        let slow_client = FakeRateLimitClient::new(RateLimitBehavior::Never);

        display_final_rate_limit(&slow_client, false, false).await;

        assert_eq!(slow_client.calls(), 1);
    }
}
