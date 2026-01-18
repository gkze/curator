//! Curator CLI - command-line interface for the repository tracker.

mod config;
mod progress;

use std::path::PathBuf;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use std::sync::Arc;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use clap::ValueEnum;
use clap::{CommandFactory, Parser, Subcommand};
use console::Term;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use curator::entity::code_platform::CodePlatform;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use curator::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
#[cfg(feature = "gitea")]
use curator::gitea::{self, GiteaClient};
#[cfg(feature = "github")]
use curator::github::GitHubClient;
#[cfg(feature = "gitlab")]
use curator::gitlab::GitLabClient;
use curator::migration::{Migrator, MigratorTrait};
#[cfg(any(feature = "github", feature = "gitea"))]
use curator::platform::PlatformClient;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use curator::repository;
#[cfg(any(feature = "github", feature = "gitea"))]
use curator::sync::sync_namespaces_streaming;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use curator::sync::{
    PlatformOptions, ProgressCallback, SyncOptions, SyncProgress, sync_namespace_streaming,
    sync_starred_streaming, sync_user_streaming, sync_users_streaming,
};
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use curator::{ApiRateLimiter, rate_limits};
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use progress::ProgressReporter;
use sea_orm::Database;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use sea_orm::DatabaseConnection;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use serde::Serialize;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use tabled::{Table, Tabled, settings::Style};
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

/// Global shutdown flag for graceful termination.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Check if shutdown has been requested.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[inline]
fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::Acquire)
}

/// Request shutdown.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[inline]
fn request_shutdown() {
    SHUTDOWN_REQUESTED.store(true, Ordering::Release);
}

/// Set up the Ctrl+C handler for graceful shutdown.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
fn setup_shutdown_handler() {
    tokio::spawn(async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");

        let is_tty = Term::stdout().is_term();
        if is_tty {
            eprintln!("\n\nShutdown requested, finishing current operations...");
            eprintln!("Press Ctrl+C again to force quit.");
        } else {
            tracing::warn!("Shutdown requested, finishing current operations");
        }

        request_shutdown();

        // Wait for second Ctrl+C for force quit
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install second Ctrl+C handler");

        if is_tty {
            eprintln!("Force quit!");
        }
        std::process::exit(130); // Standard exit code for Ctrl+C
    });
}

#[derive(Parser)]
#[command(name = "curator")]
#[command(version)]
#[command(about = "A multi-platform repository tracker")]
#[command(
    long_about = "Curator synchronizes and tracks repositories across multiple code hosting \
platforms (GitHub, GitLab, Gitea/Codeberg). It maintains a local database of \
repositories, can star active repos, and prune inactive ones."
)]
#[command(after_long_help = r#"EXAMPLES
    Sync all repos from a GitHub organization:
        $ curator github org rust-lang

    Sync starred repos and prune inactive ones:
        $ curator github stars

    Sync from multiple GitLab groups:
        $ curator gitlab group my-company/team-a my-company/team-b

    Dry run to see what would happen:
        $ curator github org kubernetes --dry-run

    Generate shell completions:
        $ curator completions bash > ~/.local/share/bash-completion/completions/curator

CONFIGURATION
    Curator reads configuration from:
      1. ~/.config/curator/config.toml (or $XDG_CONFIG_HOME/curator/config.toml)
      2. Environment variables (CURATOR_* prefix, e.g., CURATOR_GITHUB_TOKEN)
      3. .env file in current directory

ENVIRONMENT VARIABLES
    CURATOR_DATABASE_URL      Database connection string (default: ~/.local/state/curator/curator.db)
    CURATOR_GITHUB_TOKEN      GitHub personal access token
    CURATOR_GITLAB_TOKEN      GitLab personal access token
    CURATOR_GITLAB_HOST       GitLab host (default: gitlab.com)
    CURATOR_GITEA_TOKEN       Gitea/Forgejo personal access token
    CURATOR_GITEA_HOST        Gitea/Forgejo host URL
    CURATOR_CODEBERG_TOKEN    Codeberg personal access token
"#)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run database migrations
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
    },
    /// GitHub operations
    #[cfg(feature = "github")]
    Github {
        #[command(subcommand)]
        action: GithubAction,
    },
    /// GitLab operations
    #[cfg(feature = "gitlab")]
    Gitlab {
        #[command(subcommand)]
        action: GitlabAction,
    },
    /// Codeberg operations (codeberg.org)
    #[cfg(feature = "gitea")]
    Codeberg {
        #[command(subcommand)]
        action: CodebergAction,
    },
    /// Gitea operations (self-hosted Gitea/Forgejo)
    #[cfg(feature = "gitea")]
    Gitea {
        #[command(subcommand)]
        action: GiteaAction,
    },
    /// Generate shell completion scripts
    Completions {
        /// Shell to generate completions for
        shell: clap_complete::Shell,
    },
    /// Generate man page(s)
    Man {
        /// Output directory for man pages (prints to stdout if not specified)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum MigrateAction {
    /// Apply all pending migrations
    Up,
    /// Rollback the last migration
    Down,
    /// Show migration status
    Status,
    /// Fresh install - drop all tables and reapply migrations
    Fresh,
}

/// Output format for rate limit display.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum OutputFormat {
    /// Display as a formatted table (default)
    #[default]
    Table,
    /// Display as JSON
    Json,
}

/// Rate limit information for display.
#[cfg(feature = "github")]
#[derive(Debug, Clone, Serialize, Tabled)]
struct RateLimitDisplay {
    #[tabled(rename = "Resource")]
    #[serde(rename = "resource")]
    pub resource: String,
    #[tabled(rename = "Limit")]
    pub limit: String,
    #[tabled(rename = "Used")]
    pub used: String,
    #[tabled(rename = "Remaining")]
    pub remaining: String,
    #[tabled(rename = "Usage %")]
    pub usage_percent: String,
    #[tabled(rename = "Resets At")]
    pub reset_at: String,
    #[tabled(rename = "Resets In")]
    pub reset_in: String,
}

#[cfg(feature = "github")]
impl RateLimitDisplay {
    fn from_github_resource(name: &str, resource: &curator::github::RateLimitResource) -> Self {
        let usage_percent = if resource.limit > 0 {
            (resource.used as f64 / resource.limit as f64) * 100.0
        } else {
            0.0
        };
        let now = chrono::Utc::now();
        let reset_at = resource.reset_at();
        let reset_duration = reset_at.signed_duration_since(now);
        let reset_in = if reset_duration.num_seconds() > 0 {
            format_duration(reset_duration)
        } else {
            "now".to_string()
        };

        Self {
            resource: name.to_string(),
            limit: resource.limit.to_string(),
            used: resource.used.to_string(),
            remaining: resource.remaining.to_string(),
            usage_percent: format!("{:.1}%", usage_percent),
            reset_at: reset_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
            reset_in,
        }
    }

    fn print_many(mut items: Vec<Self>, format: OutputFormat) {
        // Sort by resource name for consistent output
        items.sort_by(|a, b| a.resource.cmp(&b.resource));

        match format {
            OutputFormat::Table => {
                let mut table = Table::new(items);
                table.with(Style::rounded());
                println!("{}", table);
            }
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&items).unwrap());
            }
        }
    }
}

/// Build a vector of all GitHub rate limit resources for display.
#[cfg(feature = "github")]
fn github_rate_limits_to_display(
    limits: &curator::github::GitHubRateLimits,
) -> Vec<RateLimitDisplay> {
    let mut items = Vec::new();

    // Required resources
    items.push(RateLimitDisplay::from_github_resource("core", &limits.core));
    items.push(RateLimitDisplay::from_github_resource(
        "search",
        &limits.search,
    ));

    // Optional resources - add if present
    if let Some(ref r) = limits.graphql {
        items.push(RateLimitDisplay::from_github_resource("graphql", r));
    }
    if let Some(ref r) = limits.code_search {
        items.push(RateLimitDisplay::from_github_resource("code_search", r));
    }
    if let Some(ref r) = limits.integration_manifest {
        items.push(RateLimitDisplay::from_github_resource(
            "integration_manifest",
            r,
        ));
    }
    if let Some(ref r) = limits.source_import {
        items.push(RateLimitDisplay::from_github_resource("source_import", r));
    }
    if let Some(ref r) = limits.code_scanning_upload {
        items.push(RateLimitDisplay::from_github_resource(
            "code_scanning_upload",
            r,
        ));
    }
    if let Some(ref r) = limits.actions_runner_registration {
        items.push(RateLimitDisplay::from_github_resource(
            "actions_runner_registration",
            r,
        ));
    }
    if let Some(ref r) = limits.scim {
        items.push(RateLimitDisplay::from_github_resource("scim", r));
    }
    if let Some(ref r) = limits.dependency_snapshots {
        items.push(RateLimitDisplay::from_github_resource(
            "dependency_snapshots",
            r,
        ));
    }
    if let Some(ref r) = limits.audit_log {
        items.push(RateLimitDisplay::from_github_resource("audit_log", r));
    }
    if let Some(ref r) = limits.code_scanning_autofix {
        items.push(RateLimitDisplay::from_github_resource(
            "code_scanning_autofix",
            r,
        ));
    }

    items
}

/// Rate limit informational message for platforms without dedicated endpoints.
#[cfg(any(feature = "gitlab", feature = "gitea"))]
#[derive(Debug, Clone, Serialize, Tabled)]
struct RateLimitInfoMessage {
    #[tabled(rename = "Platform")]
    pub platform: String,
    #[tabled(rename = "Rate Limiting")]
    pub message: String,
    #[tabled(rename = "Default Limit")]
    pub default_limit: String,
    #[tabled(rename = "Note")]
    pub note: String,
    #[tabled(rename = "Documentation")]
    pub docs_url: String,
}

#[cfg(any(feature = "gitlab", feature = "gitea"))]
impl RateLimitInfoMessage {
    fn print(self, format: OutputFormat) {
        match format {
            OutputFormat::Table => {
                let mut table = Table::new(vec![self]);
                table.with(Style::rounded());
                println!("{}", table);
            }
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&self).unwrap());
            }
        }
    }
}

/// Format a duration in a human-readable way.
#[cfg(feature = "github")]
fn format_duration(duration: chrono::Duration) -> String {
    let total_secs = duration.num_seconds();
    if total_secs < 60 {
        format!("{}s", total_secs)
    } else if total_secs < 3600 {
        let mins = total_secs / 60;
        let secs = total_secs % 60;
        if secs > 0 {
            format!("{}m {}s", mins, secs)
        } else {
            format!("{}m", mins)
        }
    } else {
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        if mins > 0 {
            format!("{}h {}m", hours, mins)
        } else {
            format!("{}h", hours)
        }
    }
}

/// Common sync options shared across all platforms and sync commands.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[derive(Debug, Clone, clap::Args)]
struct CommonSyncOptions {
    /// Only include repos active within this many days (default from config or 60)
    #[arg(short = 'a', long)]
    active_within_days: Option<u64>,

    /// Don't star repositories (overrides config; starring is on by default)
    #[arg(short = 'S', long)]
    no_star: bool,

    /// Dry run - show what would be done without making changes
    #[arg(short = 'n', long)]
    dry_run: bool,

    /// Maximum concurrent API requests (default from config or 20)
    #[arg(short = 'c', long)]
    concurrency: Option<usize>,

    /// Disable proactive rate limiting (may cause API throttling)
    #[arg(short = 'R', long)]
    no_rate_limit: bool,
}

/// Options for syncing starred repositories.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[derive(Debug, Clone, clap::Args)]
struct StarredSyncOptions {
    /// Only include repos active within this many days (default from config or 60)
    #[arg(short = 'a', long)]
    active_within_days: Option<u64>,

    /// Don't prune (unstar) inactive repositories
    #[arg(long)]
    no_prune: bool,

    /// Dry run - show what would be done without making changes
    #[arg(short = 'n', long)]
    dry_run: bool,

    /// Maximum concurrent API requests (default from config or 20)
    #[arg(short = 'c', long)]
    concurrency: Option<usize>,

    /// Disable proactive rate limiting (may cause API throttling)
    #[arg(short = 'R', long)]
    no_rate_limit: bool,
}

#[cfg(feature = "github")]
#[derive(Subcommand)]
enum GithubAction {
    /// Authenticate with GitHub using OAuth Device Flow
    ///
    /// Opens your browser to authorize Curator with GitHub.
    /// The token is saved to your config file for future use.
    Login,
    /// Sync repositories from a GitHub organization
    Org {
        /// Organization name(s) - can specify multiple
        #[arg(required = true)]
        orgs: Vec<String>,

        #[command(flatten)]
        sync_opts: CommonSyncOptions,
    },
    /// Sync repositories from a GitHub user
    User {
        /// Username(s) - can specify multiple
        #[arg(required = true)]
        users: Vec<String>,

        #[command(flatten)]
        sync_opts: CommonSyncOptions,
    },
    /// Sync your starred repositories (and prune inactive ones)
    Stars {
        #[command(flatten)]
        sync_opts: StarredSyncOptions,
    },
    /// Show current rate limit status
    Limits {
        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
}

#[cfg(feature = "gitlab")]
#[derive(Subcommand)]
enum GitlabAction {
    /// Sync projects from a GitLab group
    Group {
        /// Group path(s) - can specify multiple (e.g., "gitlab-org" or "my-company/team")
        #[arg(required = true)]
        groups: Vec<String>,

        /// GitLab host (default: gitlab.com, or from config/env)
        #[arg(short = 'H', long)]
        host: Option<String>,

        /// Don't include projects from subgroups
        #[arg(short = 's', long)]
        no_subgroups: bool,

        #[command(flatten)]
        sync_opts: CommonSyncOptions,
    },
    /// Sync projects from a GitLab user
    User {
        /// Username(s) - can specify multiple
        #[arg(required = true)]
        users: Vec<String>,

        /// GitLab host (default: gitlab.com, or from config/env)
        #[arg(short = 'H', long)]
        host: Option<String>,

        #[command(flatten)]
        sync_opts: CommonSyncOptions,
    },
    /// Sync your starred projects (and prune inactive ones)
    Stars {
        /// GitLab host (default: gitlab.com, or from config/env)
        #[arg(short = 'H', long)]
        host: Option<String>,

        #[command(flatten)]
        sync_opts: StarredSyncOptions,
    },
    /// Show current rate limit status (approximate)
    Limits {
        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
}

#[cfg(feature = "gitea")]
#[derive(Subcommand)]
enum CodebergAction {
    /// Sync repositories from a Codeberg organization
    Org {
        /// Organization name(s) - can specify multiple
        #[arg(required = true)]
        orgs: Vec<String>,

        #[command(flatten)]
        sync_opts: CommonSyncOptions,
    },
    /// Sync repositories from a Codeberg user
    User {
        /// Username(s) - can specify multiple
        #[arg(required = true)]
        users: Vec<String>,

        #[command(flatten)]
        sync_opts: CommonSyncOptions,
    },
    /// Sync your starred repositories (and prune inactive ones)
    Stars {
        #[command(flatten)]
        sync_opts: StarredSyncOptions,
    },
    /// Show current rate limit status (approximate)
    Limits {
        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
}

#[cfg(feature = "gitea")]
#[derive(Subcommand)]
enum GiteaAction {
    /// Sync repositories from a Gitea/Forgejo organization
    Org {
        /// Organization name(s) - can specify multiple
        #[arg(required = true)]
        orgs: Vec<String>,

        /// Gitea host URL (required, or from CURATOR_GITEA_HOST env/config)
        #[arg(short = 'H', long)]
        host: Option<String>,

        #[command(flatten)]
        sync_opts: CommonSyncOptions,
    },
    /// Sync repositories from a Gitea user
    User {
        /// Username(s) - can specify multiple
        #[arg(required = true)]
        users: Vec<String>,

        /// Gitea host URL (required, or from CURATOR_GITEA_HOST env/config)
        #[arg(short = 'H', long)]
        host: Option<String>,

        #[command(flatten)]
        sync_opts: CommonSyncOptions,
    },
    /// Sync your starred repositories (and prune inactive ones)
    Stars {
        /// Gitea host URL (required, or from CURATOR_GITEA_HOST env/config)
        #[arg(short = 'H', long)]
        host: Option<String>,

        #[command(flatten)]
        sync_opts: StarredSyncOptions,
    },
    /// Show current rate limit status (approximate)
    Limits {
        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
}

/// Batch size for bulk database upserts.
/// Balances memory usage with database round-trip efficiency.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
const PERSIST_BATCH_SIZE: usize = 100;

/// Number of retry attempts for database writes.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
const PERSIST_RETRY_ATTEMPTS: u32 = 3;

/// Initial backoff delay in milliseconds for database write retries.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
const PERSIST_RETRY_BACKOFF_MS: u64 = 100;

/// Result of a persist task, including saved count and any errors encountered.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[derive(Debug, Default)]
pub struct PersistTaskResult {
    /// Number of repositories successfully saved to database.
    pub saved_count: usize,
    /// Accumulated errors: (owner, name, error_message).
    pub errors: Vec<(String, String, String)>,
    /// Panic message if the task panicked.
    pub panic_info: Option<String>,
}

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
impl PersistTaskResult {
    /// Check if there were any errors during persistence.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty() || self.panic_info.is_some()
    }

    /// Get the total number of failed items.
    pub fn failed_count(&self) -> usize {
        self.errors.len() + usize::from(self.panic_info.is_some())
    }
}

/// Spawn a task that persists repository models from a channel using batch upserts.
///
/// Models are collected into batches and persisted using bulk upsert with ON CONFLICT,
/// which is significantly faster than individual upserts (1 query per batch vs 2n queries).
///
/// Features:
/// - Automatic retry with exponential backoff for transient database errors
/// - Accumulates all errors for reporting after sync completes
/// - Captures panics and includes them in the result
///
/// The task respects the global shutdown flag:
/// - On shutdown, it flushes any pending batch before exiting
/// - This ensures no data loss during graceful shutdown
///
/// Returns the task handle and a counter for tracking saved count in real-time.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
fn spawn_persist_task(
    db: Arc<DatabaseConnection>,
    mut rx: mpsc::Receiver<CodeRepositoryActiveModel>,
    on_progress: Option<Arc<ProgressCallback>>,
) -> (tokio::task::JoinHandle<PersistTaskResult>, Arc<AtomicUsize>) {
    let saved_count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&saved_count);

    let handle = tokio::spawn(async move {
        let mut result = PersistTaskResult::default();
        let mut batch: Vec<CodeRepositoryActiveModel> = Vec::with_capacity(PERSIST_BATCH_SIZE);
        let mut batch_names: Vec<(String, String)> = Vec::with_capacity(PERSIST_BATCH_SIZE);

        // Macro to flush a batch with retry logic (avoids lifetime issues with closures)
        macro_rules! flush_batch {
            ($models:expr, $names:expr) => {{
                match repository::bulk_upsert_with_retry(
                    &db,
                    $models,
                    PERSIST_RETRY_ATTEMPTS,
                    PERSIST_RETRY_BACKOFF_MS,
                )
                .await
                {
                    Ok(rows_affected) => {
                        let count = rows_affected as usize;
                        result.saved_count += count;
                        saved_count.fetch_add(count, Ordering::Relaxed);
                        // Report progress for actually persisted items
                        if let Some(cb) = &on_progress {
                            for (owner, name) in $names.into_iter().take(count) {
                                cb(SyncProgress::Persisted { owner, name });
                            }
                        }
                    }
                    Err(e) => {
                        let error = e.to_string();
                        // Accumulate errors for later reporting
                        for (owner, name) in &$names {
                            result
                                .errors
                                .push((owner.clone(), name.clone(), error.clone()));
                        }
                        // Also emit progress events for real-time feedback
                        if let Some(cb) = &on_progress {
                            for (owner, name) in $names {
                                cb(SyncProgress::PersistError {
                                    owner,
                                    name,
                                    error: error.clone(),
                                });
                            }
                        }
                    }
                }
            }};
        }

        loop {
            // Check for shutdown - if requested, drain remaining items and exit
            if is_shutdown_requested() {
                // Drain any remaining items from the channel
                while let Ok(model) = rx.try_recv() {
                    // SAFETY: ActiveModel always has owner/name set by to_active_model()
                    let owner = model.owner.clone().unwrap();
                    let name = model.name.clone().unwrap();
                    batch_names.push((owner, name));
                    batch.push(model);
                }
                // Flush final batch
                if !batch.is_empty() {
                    let names = std::mem::take(&mut batch_names);
                    let models = std::mem::take(&mut batch);
                    flush_batch!(models, names);
                }
                break;
            }

            let model = rx.recv().await;
            let channel_closed = model.is_none();

            // Add to batch if we received a model
            if let Some(model) = model {
                // SAFETY: ActiveModel always has owner/name set by to_active_model()
                let owner = model.owner.clone().unwrap();
                let name = model.name.clone().unwrap();
                batch_names.push((owner, name));
                batch.push(model);
            }

            // Flush when batch is full OR channel is closed
            let should_flush = batch.len() >= PERSIST_BATCH_SIZE || channel_closed;

            if should_flush && !batch.is_empty() {
                let names = std::mem::take(&mut batch_names);
                let models = std::mem::take(&mut batch);
                flush_batch!(models, names);
            }

            // Exit loop when channel is closed
            if channel_closed {
                break;
            }
        }

        result
    });

    (handle, counter)
}

/// Await the persist task handle and capture any panic information.
///
/// If the task panicked, returns a PersistTaskResult with panic_info set.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
async fn await_persist_task(
    handle: tokio::task::JoinHandle<PersistTaskResult>,
) -> PersistTaskResult {
    match handle.await {
        Ok(result) => result,
        Err(e) => {
            // Task panicked or was cancelled
            let panic_info = if e.is_panic() {
                // Try to extract panic message
                let panic_payload = e.into_panic();
                if let Some(s) = panic_payload.downcast_ref::<&str>() {
                    Some((*s).to_string())
                } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                    Some(s.clone())
                } else {
                    Some("Unknown panic".to_string())
                }
            } else if e.is_cancelled() {
                Some("Task was cancelled".to_string())
            } else {
                Some(format!("Task failed: {}", e))
            };

            PersistTaskResult {
                saved_count: 0,
                errors: Vec::new(),
                panic_info,
            }
        }
    }
}

/// Display persist errors and panic info to the user.
///
/// This function outputs accumulated errors in a visible way so users
/// know exactly what failed to save to the database.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
fn display_persist_errors(result: &PersistTaskResult, is_tty: bool) {
    if result.panic_info.is_some() || !result.errors.is_empty() {
        if is_tty {
            println!();
        }

        // Display panic info first (most severe)
        if let Some(ref panic) = result.panic_info {
            if is_tty {
                eprintln!("\x1b[1;31mPersist task crashed: {}\x1b[0m", panic);
                eprintln!("  Some repositories may not have been saved to the database.");
            } else {
                tracing::error!(panic = %panic, "Persist task crashed - some repos may not be saved");
            }
        }

        // Display individual errors (limited to first 10 to avoid flooding)
        if !result.errors.is_empty() {
            let total_errors = result.errors.len();
            let display_count = std::cmp::min(10, total_errors);

            if is_tty {
                eprintln!(
                    "\x1b[1;33mDatabase write errors ({} total):\x1b[0m",
                    total_errors
                );
                for (owner, name, error) in result.errors.iter().take(display_count) {
                    eprintln!("  - {}/{}: {}", owner, name, error);
                }
                if total_errors > display_count {
                    eprintln!("  ... and {} more errors", total_errors - display_count);
                }
            } else {
                for (owner, name, error) in result.errors.iter().take(display_count) {
                    tracing::error!(
                        repo = %format!("{}/{}", owner, name),
                        error = %error,
                        "Failed to save to database"
                    );
                }
                if total_errors > display_count {
                    tracing::error!(
                        additional_errors = total_errors - display_count,
                        "Additional database write errors occurred"
                    );
                }
            }
        }
    }
}

/// Create a rate limiter if rate limiting is enabled.
/// Returns None if no_rate_limit is true, Some(limiter) otherwise.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
fn maybe_rate_limiter(no_rate_limit: bool, rps: u32) -> Option<ApiRateLimiter> {
    if no_rate_limit {
        None
    } else {
        Some(ApiRateLimiter::new(rps))
    }
}

/// Print a warning when rate limiting is disabled (TTY only).
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
fn warn_no_rate_limit(is_tty: bool) {
    if is_tty {
        eprintln!("Warning: Rate limiting disabled - you may experience API throttling\n");
    }
}

/// Run a Gitea-based sync (shared by Codeberg and Gitea subcommands).
#[cfg(feature = "gitea")]
async fn run_gitea_sync(
    client: &GiteaClient,
    orgs: &[String],
    options: &SyncOptions,
    db: Arc<DatabaseConnection>,
    active_within_days: u64,
    platform_name: &str,
    rate_limiter: Option<&ApiRateLimiter>,
) -> Result<(), Box<dyn std::error::Error>> {
    let is_tty = Term::stdout().is_term();

    if options.dry_run && is_tty {
        println!("DRY RUN - no changes will be made\n");
    }

    // Create progress reporter (auto-detects TTY)
    let reporter = Arc::new(ProgressReporter::new());
    let progress = reporter.as_callback();

    // Single org or multiple?
    if orgs.len() == 1 {
        let org = &orgs[0];

        // Set up streaming persistence (unless dry-run)
        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
        let persist_handle = if !options.dry_run {
            let (handle, _counter) =
                spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
            Some(handle)
        } else {
            drop(rx); // Don't need receiver in dry-run
            None
        };

        let result = sync_namespace_streaming(
            client,
            org,
            options,
            rate_limiter,
            Some(&*db),
            tx,
            Some(&*progress),
        )
        .await?;

        // Wait for persistence to complete
        let persist_result = if let Some(handle) = persist_handle {
            await_persist_task(handle).await
        } else {
            PersistTaskResult::default()
        };
        let saved = persist_result.saved_count;

        // Finish progress bars before printing summary
        reporter.finish();

        // Check if shutdown was requested
        let was_interrupted = is_shutdown_requested();

        if is_tty {
            if was_interrupted {
                println!("\n(Interrupted by user - partial results below)");
            }
            println!("\nSync results for '{}' ({}):", org, platform_name);
            println!("  Total repositories:    {}", result.processed);
            println!(
                "  Active (last {} days): {}",
                active_within_days, result.matched
            );

            if options.star {
                if options.dry_run {
                    println!("  Would star:            {}", result.starred);
                    println!("  Already starred:       {}", result.skipped);
                } else {
                    println!("  Starred:               {}", result.starred);
                    println!("  Already starred:       {}", result.skipped);
                }
            }

            if !options.dry_run {
                println!("  Saved to database:     {}", saved);
                if persist_result.has_errors() {
                    println!("  Failed to save:        {}", persist_result.failed_count());
                }
            } else {
                println!("  Would save:            {}", result.matched);
            }

            // Report sync errors
            if !result.errors.is_empty() {
                println!("\nSync errors:");
                for err in &result.errors {
                    println!("  - {}", err);
                }
            }

            // Report persist errors
            display_persist_errors(&persist_result, is_tty);
        } else {
            tracing::info!(
                org = %org,
                platform = %platform_name,
                processed = result.processed,
                matched = result.matched,
                starred = result.starred,
                skipped = result.skipped,
                saved = saved,
                errors = result.errors.len(),
                "Sync complete"
            );
        }
    } else {
        // Multiple orgs - sync concurrently with streaming persistence

        // Set up streaming persistence (unless dry-run)
        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
        let persist_handle = if !options.dry_run {
            let (handle, _counter) =
                spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
            Some(handle)
        } else {
            drop(rx);
            None
        };

        let org_results = sync_namespaces_streaming(
            client,
            orgs,
            options,
            rate_limiter,
            None,
            tx,
            Some(&*progress),
        )
        .await;

        // Wait for persistence to complete
        let persist_result = if let Some(handle) = persist_handle {
            await_persist_task(handle).await
        } else {
            PersistTaskResult::default()
        };
        let total_saved = persist_result.saved_count;

        // Finish progress bars before printing summary
        reporter.finish();

        let mut total_processed = 0;
        let mut total_matched = 0;
        let mut total_starred = 0;
        let mut total_skipped = 0;
        let mut all_errors = Vec::new();

        for org_result in &org_results {
            if let Some(err) = &org_result.error {
                all_errors.push(format!("{}: {}", org_result.namespace, err));
                continue;
            }

            let result = &org_result.result;
            total_processed += result.processed;
            total_matched += result.matched;
            total_starred += result.starred;
            total_skipped += result.skipped;

            // Collect errors from this org
            for err in &result.errors {
                all_errors.push(format!("{}: {}", org_result.namespace, err));
            }
        }

        // Check if shutdown was requested
        let was_interrupted = is_shutdown_requested();

        if is_tty {
            // Summary
            if was_interrupted {
                println!("\n(Interrupted by user - partial results below)");
            }
            println!("\n=== SUMMARY ({}) ===", platform_name);
            println!("Total repositories processed: {}", total_processed);
            println!(
                "Total active (last {} days):  {}",
                active_within_days, total_matched
            );
            if options.star {
                if options.dry_run {
                    println!("Total would star:             {}", total_starred);
                    println!("Total already starred:        {}", total_skipped);
                } else {
                    println!("Total starred:                {}", total_starred);
                    println!("Total already starred:        {}", total_skipped);
                }
            }
            if !options.dry_run {
                println!("Total saved to database:      {}", total_saved);
                if persist_result.has_errors() {
                    println!(
                        "Total failed to save:         {}",
                        persist_result.failed_count()
                    );
                }
            } else {
                println!("Total would save:             {}", total_matched);
            }

            // Report sync errors
            if !all_errors.is_empty() {
                println!("\nSync errors ({}):", all_errors.len());
                for err in &all_errors {
                    println!("  - {}", err);
                }
            }

            // Report persist errors
            display_persist_errors(&persist_result, is_tty);
        } else {
            tracing::info!(
                orgs = orgs.len(),
                platform = %platform_name,
                processed = total_processed,
                matched = total_matched,
                starred = total_starred,
                skipped = total_skipped,
                saved = total_saved,
                persist_errors = persist_result.failed_count(),
                errors = all_errors.len(),
                "Sync complete"
            );
            display_persist_errors(&persist_result, is_tty);
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    // Set up graceful shutdown handler (Ctrl+C)
    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    setup_shutdown_handler();

    // Initialize tracing for non-TTY mode (structured logging)
    // Only initialize if not connected to a TTY
    if !Term::stdout().is_term() {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive("curator=info".parse().unwrap())
                    .add_directive("curator_cli=info".parse().unwrap()),
            )
            .with_target(false)
            .init();
    }

    // Load configuration (config file -> env vars -> defaults)
    let config = config::Config::load();

    let cli = Cli::parse();

    // Handle commands that don't require database access first
    match &cli.command {
        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            clap_complete::generate(*shell, &mut cmd, "curator", &mut std::io::stdout());
            return Ok(());
        }
        Commands::Man { output } => {
            let cmd = Cli::command();

            match output {
                Some(dir) => {
                    // Generate all man pages (main + subcommands) to directory
                    std::fs::create_dir_all(dir)?;
                    clap_mangen::generate_to(cmd, dir)?;
                    println!("Generated man pages in: {}", dir.display());
                }
                None => {
                    // Print main man page to stdout
                    let man = clap_mangen::Man::new(cmd);
                    man.render(&mut std::io::stdout())?;
                }
            }
            return Ok(());
        }
        _ => {}
    }

    let database_url = config
        .database_url()
        .expect("Failed to determine database URL - this should not happen");

    // Ensure the database directory exists for SQLite
    if database_url.starts_with("sqlite://") {
        let db_path = database_url.trim_start_matches("sqlite://");
        // Strip query parameters (e.g., ?mode=rwc) before path operations
        let db_path = db_path.split('?').next().unwrap_or(db_path);
        let db_path = std::path::Path::new(db_path);

        // Warn if using a relative path (can cause issues depending on cwd)
        if db_path.is_relative() && !db_path.as_os_str().is_empty() {
            tracing::warn!(
                "Database path '{}' is relative - behavior depends on current directory. \
                 Consider using an absolute path.",
                db_path.display()
            );
        }

        if let Some(parent) = db_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
    }

    match cli.command {
        Commands::Migrate { action } => {
            let db = Database::connect(&database_url).await?;

            match action {
                MigrateAction::Up => {
                    println!("Applying migrations...");
                    Migrator::up(&db, None).await?;
                    println!("Migrations applied successfully.");
                }
                MigrateAction::Down => {
                    println!("Rolling back last migration...");
                    Migrator::down(&db, Some(1)).await?;
                    println!("Rollback complete.");
                }
                MigrateAction::Status => {
                    println!("Migration status:");
                    Migrator::status(&db).await?;
                }
                MigrateAction::Fresh => {
                    println!("Dropping all tables and reapplying migrations...");
                    Migrator::fresh(&db).await?;
                    println!("Fresh migration complete.");
                }
            }
        }
        #[cfg(feature = "github")]
        Commands::Github { action } => {
            match action {
                GithubAction::Login => {
                    use curator::github::oauth::{poll_for_token, request_device_code_default};

                    let is_tty = Term::stdout().is_term();

                    if is_tty {
                        println!("Authenticating with GitHub...\n");
                    }

                    // Request device code
                    let device_code = request_device_code_default().await.map_err(|e| {
                        Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error>
                    })?;

                    // Try to copy code to clipboard
                    let clipboard_success = match arboard::Clipboard::new() {
                        Ok(mut clipboard) => clipboard.set_text(&device_code.user_code).is_ok(),
                        Err(_) => false,
                    };

                    // Display instructions
                    if is_tty {
                        println!("Please visit: {}", device_code.verification_uri);
                        println!();
                        if clipboard_success {
                            println!("Your code: {} (copied to clipboard)", device_code.user_code);
                        } else {
                            println!("Your code: {}", device_code.user_code);
                        }
                        println!();
                        println!(
                            "Waiting for authorization (expires in {} seconds)...",
                            device_code.expires_in
                        );
                    } else {
                        tracing::info!(
                            verification_uri = %device_code.verification_uri,
                            user_code = %device_code.user_code,
                            "Please authorize the application"
                        );
                    }

                    // Try to open browser
                    let _ = open::that(&device_code.verification_uri);

                    // Poll for token
                    let token_response = poll_for_token(&device_code).await.map_err(|e| {
                        Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error>
                    })?;

                    // Save token to config
                    let config_path =
                        config::Config::save_github_token(&token_response.access_token)?;

                    if is_tty {
                        println!();
                        println!("Success! GitHub token saved to: {}", config_path.display());
                        println!();
                        println!("You can now use curator commands like:");
                        println!("  curator github stars");
                        println!("  curator github org <org-name>");
                    } else {
                        tracing::info!(
                            config_path = %config_path.display(),
                            "GitHub authentication successful"
                        );
                    }
                }
                GithubAction::Org { orgs, sync_opts } => {
                    let github_token = config.github_token().expect(
                        "No GitHub token configured. Run 'curator github login' to authenticate, or set CURATOR_GITHUB_TOKEN.",
                    );
                    let client = GitHubClient::new(&github_token)?;
                    // Merge CLI args with config defaults
                    let active_within_days = sync_opts
                        .active_within_days
                        .unwrap_or(config.sync.active_within_days);
                    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
                    // CLI --no-star overrides config; if not set, use config value
                    let star = if sync_opts.no_star {
                        false
                    } else {
                        config.sync.star
                    };
                    // CLI --no-rate-limit overrides config
                    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

                    let db = Arc::new(Database::connect(&database_url).await?);

                    let options = SyncOptions {
                        active_within: chrono::Duration::days(active_within_days as i64),
                        star,
                        dry_run: sync_opts.dry_run,
                        concurrency,
                        platform_options: PlatformOptions::default(),
                        prune: false,
                    };

                    let is_tty = Term::stdout().is_term();

                    if sync_opts.dry_run && is_tty {
                        println!("DRY RUN - no changes will be made\n");
                    }

                    // Check rate limit first
                    let rate_limit = client.get_rate_limit().await?;
                    if is_tty {
                        println!(
                            "Rate limit: {}/{} remaining (resets at {})\n",
                            rate_limit.remaining, rate_limit.limit, rate_limit.reset_at
                        );
                    } else {
                        tracing::info!(
                            remaining = rate_limit.remaining,
                            limit = rate_limit.limit,
                            "Rate limit status"
                        );
                    }

                    // Create progress reporter (auto-detects TTY)
                    let reporter = Arc::new(ProgressReporter::new());
                    let progress = reporter.as_callback();

                    // Create rate limiter for proactive rate limiting (if enabled)
                    let rate_limiter =
                        maybe_rate_limiter(no_rate_limit, rate_limits::GITHUB_DEFAULT_RPS);
                    if no_rate_limit {
                        warn_no_rate_limit(is_tty);
                    }

                    // Single org or multiple?
                    if orgs.len() == 1 {
                        let org = &orgs[0];

                        // Set up streaming persistence (unless dry-run)
                        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                        let persist_handle = if !options.dry_run {
                            let (handle, _counter) = spawn_persist_task(
                                Arc::clone(&db),
                                rx,
                                Some(Arc::clone(&progress)),
                            );
                            Some(handle)
                        } else {
                            drop(rx); // Don't need receiver in dry-run
                            None
                        };

                        let result = sync_namespace_streaming(
                            &client,
                            org,
                            &options,
                            rate_limiter.as_ref(),
                            Some(&*db),
                            tx,
                            Some(&*progress),
                        )
                        .await?;

                        // Wait for persistence to complete
                        let persist_result = if let Some(handle) = persist_handle {
                            await_persist_task(handle).await
                        } else {
                            PersistTaskResult::default()
                        };
                        let saved = persist_result.saved_count;

                        // Finish progress bars before printing summary
                        reporter.finish();

                        // Check if shutdown was requested
                        let was_interrupted = is_shutdown_requested();

                        if is_tty {
                            if was_interrupted {
                                println!("\n(Interrupted by user - partial results below)");
                            }
                            println!("\nSync results for '{}':", org);
                            println!("  Total repositories:    {}", result.processed);
                            println!(
                                "  Active (last {} days): {}",
                                active_within_days, result.matched
                            );

                            if options.star {
                                if options.dry_run {
                                    println!("  Would star:            {}", result.starred);
                                    println!("  Already starred:       {}", result.skipped);
                                } else {
                                    println!("  Starred:               {}", result.starred);
                                    println!("  Already starred:       {}", result.skipped);
                                }
                            }

                            if !options.dry_run {
                                println!("  Saved to database:     {}", saved);
                                if persist_result.has_errors() {
                                    println!(
                                        "  Failed to save:        {}",
                                        persist_result.failed_count()
                                    );
                                }
                            } else {
                                println!("  Would save:            {}", result.matched);
                            }

                            // Report sync errors
                            if !result.errors.is_empty() {
                                println!("\nSync errors:");
                                for err in &result.errors {
                                    println!("  - {}", err);
                                }
                            }

                            // Report persist errors
                            display_persist_errors(&persist_result, is_tty);
                        } else {
                            tracing::info!(
                                org = %org,
                                processed = result.processed,
                                matched = result.matched,
                                starred = result.starred,
                                skipped = result.skipped,
                                saved = saved,
                                persist_errors = persist_result.failed_count(),
                                errors = result.errors.len(),
                                "Sync complete"
                            );
                            display_persist_errors(&persist_result, is_tty);
                        }
                    } else {
                        // Multiple orgs - sync concurrently with streaming persistence

                        // Set up streaming persistence (unless dry-run)
                        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                        let persist_handle = if !options.dry_run {
                            let (handle, _counter) = spawn_persist_task(
                                Arc::clone(&db),
                                rx,
                                Some(Arc::clone(&progress)),
                            );
                            Some(handle)
                        } else {
                            drop(rx);
                            None
                        };

                        let org_results = sync_namespaces_streaming(
                            &client,
                            &orgs,
                            &options,
                            rate_limiter.as_ref(),
                            None, // db not passed to concurrent syncs
                            tx,
                            Some(&*progress),
                        )
                        .await;

                        // Wait for persistence to complete
                        let persist_result = if let Some(handle) = persist_handle {
                            await_persist_task(handle).await
                        } else {
                            PersistTaskResult::default()
                        };
                        let total_saved = persist_result.saved_count;

                        // Finish progress bars before printing summary
                        reporter.finish();

                        let mut total_processed = 0;
                        let mut total_matched = 0;
                        let mut total_starred = 0;
                        let mut total_skipped = 0;
                        let mut all_errors = Vec::new();

                        for org_result in &org_results {
                            if let Some(err) = &org_result.error {
                                all_errors.push(format!("{}: {}", org_result.namespace, err));
                                continue;
                            }

                            let result = &org_result.result;
                            total_processed += result.processed;
                            total_matched += result.matched;
                            total_starred += result.starred;
                            total_skipped += result.skipped;

                            // Collect errors from this org
                            for err in &result.errors {
                                all_errors.push(format!("{}: {}", org_result.namespace, err));
                            }
                        }

                        // Check if shutdown was requested
                        let was_interrupted = is_shutdown_requested();

                        if is_tty {
                            // Summary
                            if was_interrupted {
                                println!("\n(Interrupted by user - partial results below)");
                            }
                            println!("\n=== SUMMARY ===");
                            println!("Total repositories processed: {}", total_processed);
                            println!(
                                "Total active (last {} days):  {}",
                                active_within_days, total_matched
                            );
                            if options.star {
                                if options.dry_run {
                                    println!("Total would star:             {}", total_starred);
                                    println!("Total already starred:        {}", total_skipped);
                                } else {
                                    println!("Total starred:                {}", total_starred);
                                    println!("Total already starred:        {}", total_skipped);
                                }
                            }
                            if !options.dry_run {
                                println!("Total saved to database:      {}", total_saved);
                                if persist_result.has_errors() {
                                    println!(
                                        "Total failed to save:         {}",
                                        persist_result.failed_count()
                                    );
                                }
                            } else {
                                println!("Total would save:             {}", total_matched);
                            }

                            // Report sync errors
                            if !all_errors.is_empty() {
                                println!("\nSync errors ({}):", all_errors.len());
                                for err in &all_errors {
                                    println!("  - {}", err);
                                }
                            }

                            // Report persist errors
                            display_persist_errors(&persist_result, is_tty);
                        } else {
                            tracing::info!(
                                orgs = orgs.len(),
                                processed = total_processed,
                                matched = total_matched,
                                starred = total_starred,
                                skipped = total_skipped,
                                saved = total_saved,
                                persist_errors = persist_result.failed_count(),
                                errors = all_errors.len(),
                                "Sync complete"
                            );
                            display_persist_errors(&persist_result, is_tty);
                        }
                    }

                    // Final rate limit
                    let final_rate = client.get_rate_limit().await?;
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
                GithubAction::User { users, sync_opts } => {
                    let github_token = config.github_token().expect(
                        "No GitHub token configured. Run 'curator github login' to authenticate, or set CURATOR_GITHUB_TOKEN.",
                    );
                    let client = GitHubClient::new(&github_token)?;

                    // Merge CLI args with config defaults
                    let active_within_days = sync_opts
                        .active_within_days
                        .unwrap_or(config.sync.active_within_days);
                    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
                    let star = if sync_opts.no_star {
                        false
                    } else {
                        config.sync.star
                    };
                    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

                    let db = Arc::new(Database::connect(&database_url).await?);

                    let options = SyncOptions {
                        active_within: chrono::Duration::days(active_within_days as i64),
                        star,
                        dry_run: sync_opts.dry_run,
                        concurrency,
                        platform_options: PlatformOptions::default(),
                        prune: false,
                    };

                    let is_tty = Term::stdout().is_term();

                    if options.dry_run && is_tty {
                        println!("DRY RUN - no changes will be made\n");
                    }

                    // Check rate limit first
                    let rate_limit = client.get_rate_limit().await?;
                    if is_tty {
                        println!(
                            "Rate limit: {}/{} remaining (resets at {})\n",
                            rate_limit.remaining, rate_limit.limit, rate_limit.reset_at
                        );
                    }

                    // Create progress reporter
                    let reporter = Arc::new(ProgressReporter::new());
                    let progress = reporter.as_callback();

                    // Create rate limiter (if enabled)
                    let rate_limiter =
                        maybe_rate_limiter(no_rate_limit, rate_limits::GITHUB_DEFAULT_RPS);
                    if no_rate_limit {
                        warn_no_rate_limit(is_tty);
                    }

                    if users.len() == 1 {
                        // Single user - use simple path
                        let user = &users[0];

                        if is_tty {
                            println!("Syncing repositories for user '{}'...\n", user);
                        }

                        // Set up streaming persistence (unless dry-run)
                        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                        let persist_handle = if !options.dry_run {
                            let (handle, _counter) = spawn_persist_task(
                                Arc::clone(&db),
                                rx,
                                Some(Arc::clone(&progress)),
                            );
                            Some(handle)
                        } else {
                            drop(rx);
                            None
                        };

                        let result = sync_user_streaming(
                            &client,
                            user,
                            &options,
                            rate_limiter.as_ref(),
                            Some(&*db),
                            tx,
                            Some(&*progress),
                        )
                        .await?;

                        // Wait for persistence to complete
                        let persist_result = if let Some(handle) = persist_handle {
                            await_persist_task(handle).await
                        } else {
                            PersistTaskResult::default()
                        };
                        let saved = persist_result.saved_count;

                        // Finish progress bars before printing summary
                        reporter.finish();

                        if is_tty {
                            println!("\nSync results for user '{}':", user);
                            println!("  Total repositories:    {}", result.processed);
                            println!(
                                "  Active (last {} days): {}",
                                active_within_days, result.matched
                            );

                            if options.star {
                                if options.dry_run {
                                    println!("  Would star:            {}", result.starred);
                                    println!("  Already starred:       {}", result.skipped);
                                } else {
                                    println!("  Starred:               {}", result.starred);
                                    println!("  Already starred:       {}", result.skipped);
                                }
                            }

                            if !options.dry_run {
                                println!("  Saved to database:     {}", saved);
                                if persist_result.has_errors() {
                                    println!(
                                        "  Failed to save:        {}",
                                        persist_result.failed_count()
                                    );
                                }
                            } else {
                                println!("  Would save:            {}", result.matched);
                            }

                            if !result.errors.is_empty() {
                                println!("\nSync errors:");
                                for err in &result.errors {
                                    println!("  - {}", err);
                                }
                            }
                            display_persist_errors(&persist_result, is_tty);
                        } else {
                            tracing::info!(
                                user = %user,
                                processed = result.processed,
                                matched = result.matched,
                                starred = result.starred,
                                skipped = result.skipped,
                                saved = saved,
                                persist_errors = persist_result.failed_count(),
                                errors = result.errors.len(),
                                "User sync complete"
                            );
                            display_persist_errors(&persist_result, is_tty);
                        }
                    } else {
                        // Multiple users - sync concurrently with streaming persistence
                        if is_tty {
                            println!("Syncing repositories for {} users...\n", users.len());
                        }

                        // Set up streaming persistence (unless dry-run)
                        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                        let persist_handle = if !options.dry_run {
                            let (handle, _counter) = spawn_persist_task(
                                Arc::clone(&db),
                                rx,
                                Some(Arc::clone(&progress)),
                            );
                            Some(handle)
                        } else {
                            drop(rx);
                            None
                        };

                        let user_results = sync_users_streaming(
                            &client,
                            &users,
                            &options,
                            rate_limiter.as_ref(),
                            None, // db not passed to concurrent syncs
                            tx,
                            Some(&*progress),
                        )
                        .await;

                        // Wait for persistence to complete
                        let persist_result = if let Some(handle) = persist_handle {
                            await_persist_task(handle).await
                        } else {
                            PersistTaskResult::default()
                        };
                        let total_saved = persist_result.saved_count;

                        // Finish progress bars before printing summary
                        reporter.finish();

                        let mut total_processed = 0;
                        let mut total_matched = 0;
                        let mut total_starred = 0;
                        let mut total_skipped = 0;
                        let mut all_errors = Vec::new();

                        for user_result in &user_results {
                            if let Some(err) = &user_result.error {
                                all_errors.push(format!("{}: {}", user_result.namespace, err));
                                continue;
                            }

                            let result = &user_result.result;
                            total_processed += result.processed;
                            total_matched += result.matched;
                            total_starred += result.starred;
                            total_skipped += result.skipped;

                            for err in &result.errors {
                                all_errors.push(format!("{}: {}", user_result.namespace, err));
                            }
                        }

                        // Check if shutdown was requested
                        let was_interrupted = is_shutdown_requested();

                        if is_tty {
                            if was_interrupted {
                                println!("\n(Interrupted by user - partial results below)");
                            }
                            println!("\n=== SUMMARY ===");
                            println!("Total repositories processed: {}", total_processed);
                            println!(
                                "Total active (last {} days):  {}",
                                active_within_days, total_matched
                            );
                            if options.star {
                                if options.dry_run {
                                    println!("Total would star:             {}", total_starred);
                                    println!("Total already starred:        {}", total_skipped);
                                } else {
                                    println!("Total starred:                {}", total_starred);
                                    println!("Total already starred:        {}", total_skipped);
                                }
                            }
                            if !options.dry_run {
                                println!("Total saved to database:      {}", total_saved);
                                if persist_result.has_errors() {
                                    println!(
                                        "Total failed to save:         {}",
                                        persist_result.failed_count()
                                    );
                                }
                            } else {
                                println!("Total would save:             {}", total_matched);
                            }

                            if !all_errors.is_empty() {
                                println!("\nSync errors ({}):", all_errors.len());
                                for err in &all_errors {
                                    println!("  - {}", err);
                                }
                            }
                            display_persist_errors(&persist_result, is_tty);
                        } else {
                            tracing::info!(
                                users = users.len(),
                                processed = total_processed,
                                matched = total_matched,
                                starred = total_starred,
                                skipped = total_skipped,
                                saved = total_saved,
                                persist_errors = persist_result.failed_count(),
                                errors = all_errors.len(),
                                "User sync complete"
                            );
                            display_persist_errors(&persist_result, is_tty);
                        }
                    }

                    // Final rate limit
                    let final_rate = client.get_rate_limit().await?;
                    if is_tty {
                        println!(
                            "\nRate limit after sync: {}/{} remaining",
                            final_rate.remaining, final_rate.limit
                        );
                    }
                }
                GithubAction::Stars { sync_opts } => {
                    let github_token = config.github_token().expect(
                        "No GitHub token configured. Run 'curator github login' to authenticate, or set CURATOR_GITHUB_TOKEN.",
                    );
                    let client = GitHubClient::new(&github_token)?;

                    // Merge CLI args with config defaults
                    let active_within_days = sync_opts
                        .active_within_days
                        .unwrap_or(config.sync.active_within_days);
                    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
                    let prune = !sync_opts.no_prune;
                    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

                    let db = Arc::new(Database::connect(&database_url).await?);

                    let options = SyncOptions {
                        active_within: chrono::Duration::days(active_within_days as i64),
                        star: false, // Not used for starred sync
                        dry_run: sync_opts.dry_run,
                        concurrency,
                        platform_options: PlatformOptions::default(),
                        prune,
                    };

                    let is_tty = Term::stdout().is_term();

                    if options.dry_run && is_tty {
                        println!("DRY RUN - no changes will be made\n");
                    }

                    // Check rate limit first
                    let rate_limit = client.get_rate_limit().await?;
                    if is_tty {
                        println!(
                            "Rate limit: {}/{} remaining (resets at {})\n",
                            rate_limit.remaining, rate_limit.limit, rate_limit.reset_at
                        );
                        println!("Syncing starred repositories...\n");
                    }

                    // Create progress reporter
                    let reporter = Arc::new(ProgressReporter::new());
                    let progress = reporter.as_callback();

                    // Create rate limiter (if enabled)
                    let rate_limiter =
                        maybe_rate_limiter(no_rate_limit, rate_limits::GITHUB_DEFAULT_RPS);
                    if no_rate_limit {
                        warn_no_rate_limit(is_tty);
                    }

                    // Set up streaming persistence (unless dry-run)
                    let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                    let persist_handle = if !options.dry_run {
                        let (handle, _counter) =
                            spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
                        Some(handle)
                    } else {
                        drop(rx);
                        None
                    };

                    let result = sync_starred_streaming(
                        &client,
                        &options,
                        rate_limiter.as_ref(),
                        Some(&*db),
                        options.concurrency,
                        no_rate_limit,
                        tx,
                        Some(&*progress),
                    )
                    .await?;

                    // Wait for persistence to complete
                    let persist_result = if let Some(handle) = persist_handle {
                        await_persist_task(handle).await
                    } else {
                        PersistTaskResult::default()
                    };
                    let saved = persist_result.saved_count;

                    // Delete pruned repos from database (unless dry-run)
                    let deleted = if !options.dry_run && !result.pruned_repos.is_empty() {
                        repository::delete_by_owner_name(
                            &db,
                            CodePlatform::GitHub,
                            &result.pruned_repos,
                        )
                        .await
                        .unwrap_or(0)
                    } else {
                        0
                    };

                    // Finish progress bars before printing summary
                    reporter.finish();

                    if is_tty {
                        println!("\nSync results for starred repositories:");
                        println!("  Total starred:         {}", result.processed);
                        println!(
                            "  Active (last {} days): {}",
                            active_within_days, result.matched
                        );

                        if options.dry_run {
                            println!("  Would save:            {}", result.matched);
                            if prune {
                                println!("  Would prune:           {}", result.pruned);
                            }
                        } else {
                            println!("  Saved to database:     {}", saved);
                            if persist_result.has_errors() {
                                println!(
                                    "  Failed to save:        {}",
                                    persist_result.failed_count()
                                );
                            }
                            if prune {
                                println!("  Pruned (unstarred):    {}", result.pruned);
                                if deleted > 0 {
                                    println!("  Deleted from database: {}", deleted);
                                }
                            }
                        }

                        if !result.errors.is_empty() {
                            println!("\nSync errors:");
                            for err in &result.errors {
                                println!("  - {}", err);
                            }
                        }
                        display_persist_errors(&persist_result, is_tty);
                    } else {
                        tracing::info!(
                            processed = result.processed,
                            matched = result.matched,
                            saved = saved,
                            persist_errors = persist_result.failed_count(),
                            pruned = result.pruned,
                            deleted = deleted,
                            errors = result.errors.len(),
                            "Starred sync complete"
                        );
                        display_persist_errors(&persist_result, is_tty);
                    }

                    // Final rate limit
                    let final_rate = client.get_rate_limit().await?;
                    if is_tty {
                        println!(
                            "\nRate limit after sync: {}/{} remaining",
                            final_rate.remaining, final_rate.limit
                        );
                    }
                }
                GithubAction::Limits { output } => {
                    let github_token = config.github_token().expect(
                        "No GitHub token configured. Run 'curator github login' to authenticate, or set CURATOR_GITHUB_TOKEN.",
                    );
                    let client = GitHubClient::new(&github_token)?;

                    let rate_limits = curator::github::get_rate_limits(client.inner()).await?;
                    let items = github_rate_limits_to_display(&rate_limits.resources);
                    RateLimitDisplay::print_many(items, output);
                }
            }
        }
        #[cfg(feature = "gitlab")]
        Commands::Gitlab { action } => {
            match action {
                GitlabAction::Group {
                    groups,
                    host,
                    no_subgroups,
                    sync_opts,
                } => {
                    // Resolve host: CLI arg > env > config > default
                    let gitlab_host = host.unwrap_or_else(|| config.gitlab_host());

                    // Resolve token: env > config
                    let gitlab_token = config.gitlab_token().expect(
                        "CURATOR_GITLAB_TOKEN must be set in environment, .env file, or config file",
                    );

                    // Merge CLI args with config defaults
                    let active_within_days = sync_opts
                        .active_within_days
                        .unwrap_or(config.sync.active_within_days);
                    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
                    // CLI --no-star overrides config; if not set, use config value
                    let star = if sync_opts.no_star {
                        false
                    } else {
                        config.sync.star
                    };
                    // CLI --no-subgroups overrides config; if not set, use config value
                    let include_subgroups = if no_subgroups {
                        false
                    } else {
                        config.gitlab.include_subgroups
                    };
                    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

                    let client = GitLabClient::new(&gitlab_host, &gitlab_token).await?;
                    let db = Arc::new(Database::connect(&database_url).await?);

                    let options = SyncOptions {
                        active_within: chrono::Duration::days(active_within_days as i64),
                        star,
                        dry_run: sync_opts.dry_run,
                        concurrency,
                        platform_options: PlatformOptions { include_subgroups },
                        prune: false,
                    };

                    let is_tty = Term::stdout().is_term();

                    if options.dry_run && is_tty {
                        println!("DRY RUN - no changes will be made\n");
                    }

                    // Create progress reporter (auto-detects TTY)
                    let reporter = Arc::new(ProgressReporter::new());
                    let progress = reporter.as_callback();

                    // Create rate limiter (if enabled)
                    let rate_limiter =
                        maybe_rate_limiter(no_rate_limit, rate_limits::GITLAB_DEFAULT_RPS);
                    if no_rate_limit {
                        warn_no_rate_limit(is_tty);
                    }

                    // Single group or multiple?
                    if groups.len() == 1 {
                        let group = &groups[0];

                        // Set up streaming persistence (unless dry-run)
                        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                        let persist_handle = if !options.dry_run {
                            let (handle, _counter) = spawn_persist_task(
                                Arc::clone(&db),
                                rx,
                                Some(Arc::clone(&progress)),
                            );
                            Some(handle)
                        } else {
                            drop(rx); // Don't need receiver in dry-run
                            None
                        };

                        let result = sync_namespace_streaming(
                            &client,
                            group,
                            &options,
                            rate_limiter.as_ref(),
                            Some(&*db),
                            tx,
                            Some(&*progress),
                        )
                        .await?;

                        // Wait for persistence to complete
                        let persist_result = if let Some(handle) = persist_handle {
                            await_persist_task(handle).await
                        } else {
                            PersistTaskResult::default()
                        };
                        let saved = persist_result.saved_count;

                        // Finish progress bars before printing summary
                        reporter.finish();

                        // Check if shutdown was requested
                        let was_interrupted = is_shutdown_requested();

                        if is_tty {
                            if was_interrupted {
                                println!("\n(Interrupted by user - partial results below)");
                            }
                            println!("\nSync results for '{}':", group);
                            println!("  Total projects:        {}", result.processed);
                            println!(
                                "  Active (last {} days): {}",
                                active_within_days, result.matched
                            );

                            if options.star {
                                if options.dry_run {
                                    println!("  Would star:            {}", result.starred);
                                    println!("  Already starred:       {}", result.skipped);
                                } else {
                                    println!("  Starred:               {}", result.starred);
                                    println!("  Already starred:       {}", result.skipped);
                                }
                            }

                            if !options.dry_run {
                                println!("  Saved to database:     {}", saved);
                                if persist_result.has_errors() {
                                    println!(
                                        "  Failed to save:        {}",
                                        persist_result.failed_count()
                                    );
                                }
                            } else {
                                println!("  Would save:            {}", result.matched);
                            }

                            // Report sync errors
                            if !result.errors.is_empty() {
                                println!("\nSync errors:");
                                for err in &result.errors {
                                    println!("  - {}", err);
                                }
                            }

                            // Report persist errors
                            display_persist_errors(&persist_result, is_tty);
                        } else {
                            tracing::info!(
                                group = %group,
                                processed = result.processed,
                                matched = result.matched,
                                starred = result.starred,
                                skipped = result.skipped,
                                saved = saved,
                                persist_errors = persist_result.failed_count(),
                                errors = result.errors.len(),
                                "Sync complete"
                            );
                            display_persist_errors(&persist_result, is_tty);
                        }
                    } else {
                        // Multiple groups - sync sequentially with streaming persistence
                        // (GitLab client doesn't implement Clone, so we can't easily parallelize)

                        // Set up streaming persistence (unless dry-run)
                        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                        let persist_handle = if !options.dry_run {
                            let (handle, _counter) = spawn_persist_task(
                                Arc::clone(&db),
                                rx,
                                Some(Arc::clone(&progress)),
                            );
                            Some(handle)
                        } else {
                            drop(rx);
                            None
                        };

                        let mut total_processed = 0;
                        let mut total_matched = 0;
                        let mut total_starred = 0;
                        let mut total_skipped = 0;
                        let mut all_errors: Vec<String> = Vec::new();

                        for group in &groups {
                            let (group_tx, mut group_rx) =
                                mpsc::channel::<CodeRepositoryActiveModel>(100);

                            // Forward models to the main persistence channel
                            let tx_clone = tx.clone();
                            let forward_handle = tokio::spawn(async move {
                                while let Some(model) = group_rx.recv().await {
                                    if tx_clone.send(model).await.is_err() {
                                        break;
                                    }
                                }
                            });

                            match sync_namespace_streaming(
                                &client,
                                group,
                                &options,
                                rate_limiter.as_ref(),
                                Some(&*db),
                                group_tx,
                                Some(&*progress),
                            )
                            .await
                            {
                                Ok(result) => {
                                    total_processed += result.processed;
                                    total_matched += result.matched;
                                    total_starred += result.starred;
                                    total_skipped += result.skipped;
                                    for err in result.errors {
                                        all_errors.push(format!("{}: {}", group, err));
                                    }
                                }
                                Err(e) => {
                                    all_errors.push(format!("{}: {}", group, e));
                                }
                            }

                            // Wait for forwarding to complete
                            let _ = forward_handle.await;

                            // Check for shutdown
                            if is_shutdown_requested() {
                                break;
                            }
                        }

                        // Close the main channel to signal persistence task
                        drop(tx);

                        // Wait for persistence to complete
                        let persist_result = if let Some(handle) = persist_handle {
                            await_persist_task(handle).await
                        } else {
                            PersistTaskResult::default()
                        };
                        let total_saved = persist_result.saved_count;

                        // Finish progress bars before printing summary
                        reporter.finish();

                        // Check if shutdown was requested
                        let was_interrupted = is_shutdown_requested();

                        if is_tty {
                            // Summary
                            if was_interrupted {
                                println!("\n(Interrupted by user - partial results below)");
                            }
                            println!("\n=== SUMMARY ===");
                            println!("Total projects processed: {}", total_processed);
                            println!(
                                "Total active (last {} days):  {}",
                                active_within_days, total_matched
                            );
                            if options.star {
                                if options.dry_run {
                                    println!("Total would star:             {}", total_starred);
                                    println!("Total already starred:        {}", total_skipped);
                                } else {
                                    println!("Total starred:                {}", total_starred);
                                    println!("Total already starred:        {}", total_skipped);
                                }
                            }
                            if !options.dry_run {
                                println!("Total saved to database:      {}", total_saved);
                                if persist_result.has_errors() {
                                    println!(
                                        "Total failed to save:         {}",
                                        persist_result.failed_count()
                                    );
                                }
                            } else {
                                println!("Total would save:             {}", total_matched);
                            }

                            // Report sync errors
                            if !all_errors.is_empty() {
                                println!("\nSync errors ({}):", all_errors.len());
                                for err in &all_errors {
                                    println!("  - {}", err);
                                }
                            }

                            // Report persist errors
                            display_persist_errors(&persist_result, is_tty);
                        } else {
                            tracing::info!(
                                groups = groups.len(),
                                processed = total_processed,
                                matched = total_matched,
                                starred = total_starred,
                                skipped = total_skipped,
                                saved = total_saved,
                                persist_errors = persist_result.failed_count(),
                                errors = all_errors.len(),
                                "Sync complete"
                            );
                            display_persist_errors(&persist_result, is_tty);
                        }
                    }
                }
                GitlabAction::User {
                    users,
                    host,
                    sync_opts,
                } => {
                    // Resolve host: CLI arg > env > config > default
                    let gitlab_host = host.unwrap_or_else(|| config.gitlab_host());

                    // Resolve token: env > config
                    let gitlab_token = config.gitlab_token().expect(
                        "CURATOR_GITLAB_TOKEN must be set in environment, .env file, or config file",
                    );

                    // Merge CLI args with config defaults
                    let active_within_days = sync_opts
                        .active_within_days
                        .unwrap_or(config.sync.active_within_days);
                    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
                    let star = if sync_opts.no_star {
                        false
                    } else {
                        config.sync.star
                    };
                    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

                    let client = GitLabClient::new(&gitlab_host, &gitlab_token).await?;
                    let db = Arc::new(Database::connect(&database_url).await?);

                    let options = SyncOptions {
                        active_within: chrono::Duration::days(active_within_days as i64),
                        star,
                        dry_run: sync_opts.dry_run,
                        concurrency,
                        platform_options: PlatformOptions::default(),
                        prune: false,
                    };

                    let is_tty = Term::stdout().is_term();

                    if options.dry_run && is_tty {
                        println!("DRY RUN - no changes will be made\n");
                    }

                    // Create progress reporter
                    let reporter = Arc::new(ProgressReporter::new());
                    let progress = reporter.as_callback();

                    // Create rate limiter (if enabled)
                    let rate_limiter =
                        maybe_rate_limiter(no_rate_limit, rate_limits::GITLAB_DEFAULT_RPS);
                    if no_rate_limit {
                        warn_no_rate_limit(is_tty);
                    }

                    if users.len() == 1 {
                        let user = &users[0];

                        if is_tty {
                            println!("Syncing projects for user '{}'...\n", user);
                        }

                        // Set up streaming persistence (unless dry-run)
                        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                        let persist_handle = if !options.dry_run {
                            let (handle, _counter) = spawn_persist_task(
                                Arc::clone(&db),
                                rx,
                                Some(Arc::clone(&progress)),
                            );
                            Some(handle)
                        } else {
                            drop(rx);
                            None
                        };

                        let result = sync_user_streaming(
                            &client,
                            user,
                            &options,
                            rate_limiter.as_ref(),
                            Some(&*db),
                            tx,
                            Some(&*progress),
                        )
                        .await?;

                        // Wait for persistence to complete
                        let persist_result = if let Some(handle) = persist_handle {
                            await_persist_task(handle).await
                        } else {
                            PersistTaskResult::default()
                        };
                        let saved = persist_result.saved_count;

                        // Finish progress bars before printing summary
                        reporter.finish();

                        if is_tty {
                            println!("\nSync results for user '{}':", user);
                            println!("  Total projects:        {}", result.processed);
                            println!(
                                "  Active (last {} days): {}",
                                active_within_days, result.matched
                            );

                            if options.star {
                                if options.dry_run {
                                    println!("  Would star:            {}", result.starred);
                                    println!("  Already starred:       {}", result.skipped);
                                } else {
                                    println!("  Starred:               {}", result.starred);
                                    println!("  Already starred:       {}", result.skipped);
                                }
                            }

                            if !options.dry_run {
                                println!("  Saved to database:     {}", saved);
                                if persist_result.has_errors() {
                                    println!(
                                        "  Failed to save:        {}",
                                        persist_result.failed_count()
                                    );
                                }
                            } else {
                                println!("  Would save:            {}", result.matched);
                            }

                            // Report sync errors
                            if !result.errors.is_empty() {
                                println!("\nSync errors:");
                                for err in &result.errors {
                                    println!("  - {}", err);
                                }
                            }

                            // Report persist errors
                            display_persist_errors(&persist_result, is_tty);
                        } else {
                            tracing::info!(
                                user = %user,
                                processed = result.processed,
                                matched = result.matched,
                                starred = result.starred,
                                skipped = result.skipped,
                                saved = saved,
                                persist_errors = persist_result.failed_count(),
                                errors = result.errors.len(),
                                "User sync complete"
                            );
                            display_persist_errors(&persist_result, is_tty);
                        }
                    } else {
                        // Multiple users
                        if is_tty {
                            println!("Syncing projects for {} users...\n", users.len());
                        }

                        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                        let persist_handle = if !options.dry_run {
                            let (handle, _counter) = spawn_persist_task(
                                Arc::clone(&db),
                                rx,
                                Some(Arc::clone(&progress)),
                            );
                            Some(handle)
                        } else {
                            drop(rx);
                            None
                        };

                        let user_results = sync_users_streaming(
                            &client,
                            &users,
                            &options,
                            rate_limiter.as_ref(),
                            None, // db not passed to concurrent syncs
                            tx,
                            Some(&*progress),
                        )
                        .await;

                        let persist_result = if let Some(handle) = persist_handle {
                            await_persist_task(handle).await
                        } else {
                            PersistTaskResult::default()
                        };
                        let total_saved = persist_result.saved_count;

                        reporter.finish();

                        let mut total_processed = 0;
                        let mut total_matched = 0;
                        let mut total_starred = 0;
                        let mut total_skipped = 0;
                        let mut all_errors = Vec::new();

                        for user_result in &user_results {
                            if let Some(err) = &user_result.error {
                                all_errors.push(format!("{}: {}", user_result.namespace, err));
                                continue;
                            }

                            let result = &user_result.result;
                            total_processed += result.processed;
                            total_matched += result.matched;
                            total_starred += result.starred;
                            total_skipped += result.skipped;

                            for err in &result.errors {
                                all_errors.push(format!("{}: {}", user_result.namespace, err));
                            }
                        }

                        if is_tty {
                            println!("\n=== SUMMARY ===");
                            println!("Total projects processed: {}", total_processed);
                            println!(
                                "Total active (last {} days):  {}",
                                active_within_days, total_matched
                            );
                            if options.star {
                                if options.dry_run {
                                    println!("Total would star:             {}", total_starred);
                                    println!("Total already starred:        {}", total_skipped);
                                } else {
                                    println!("Total starred:                {}", total_starred);
                                    println!("Total already starred:        {}", total_skipped);
                                }
                            }
                            if !options.dry_run {
                                println!("Total saved to database:      {}", total_saved);
                                if persist_result.has_errors() {
                                    println!(
                                        "Total failed to save:         {}",
                                        persist_result.failed_count()
                                    );
                                }
                            } else {
                                println!("Total would save:             {}", total_matched);
                            }

                            // Report sync errors
                            if !all_errors.is_empty() {
                                println!("\nSync errors ({}):", all_errors.len());
                                for err in &all_errors {
                                    println!("  - {}", err);
                                }
                            }

                            // Report persist errors
                            display_persist_errors(&persist_result, is_tty);
                        } else {
                            tracing::info!(
                                users = users.len(),
                                processed = total_processed,
                                matched = total_matched,
                                starred = total_starred,
                                skipped = total_skipped,
                                saved = total_saved,
                                persist_errors = persist_result.failed_count(),
                                errors = all_errors.len(),
                                "User sync complete"
                            );
                            display_persist_errors(&persist_result, is_tty);
                        }
                    }
                }
                GitlabAction::Stars { host, sync_opts } => {
                    // Resolve host: CLI arg > env > config > default
                    let gitlab_host = host.unwrap_or_else(|| config.gitlab_host());

                    // Resolve token: env > config
                    let gitlab_token = config.gitlab_token().expect(
                        "CURATOR_GITLAB_TOKEN must be set in environment, .env file, or config file",
                    );

                    // Merge CLI args with config defaults
                    let active_within_days = sync_opts
                        .active_within_days
                        .unwrap_or(config.sync.active_within_days);
                    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
                    let prune = !sync_opts.no_prune;
                    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

                    let client = GitLabClient::new(&gitlab_host, &gitlab_token).await?;
                    let db = Arc::new(Database::connect(&database_url).await?);

                    let options = SyncOptions {
                        active_within: chrono::Duration::days(active_within_days as i64),
                        star: false, // Not used for starred sync
                        dry_run: sync_opts.dry_run,
                        concurrency,
                        platform_options: PlatformOptions::default(),
                        prune,
                    };

                    let is_tty = Term::stdout().is_term();

                    if options.dry_run && is_tty {
                        println!("DRY RUN - no changes will be made\n");
                    }

                    if is_tty {
                        println!("Syncing starred projects from {}...\n", gitlab_host);
                    }

                    // Create progress reporter
                    let reporter = Arc::new(ProgressReporter::new());
                    let progress = reporter.as_callback();

                    // Create rate limiter (if enabled)
                    let rate_limiter =
                        maybe_rate_limiter(no_rate_limit, rate_limits::GITLAB_DEFAULT_RPS);
                    if no_rate_limit {
                        warn_no_rate_limit(is_tty);
                    }

                    // Set up streaming persistence (unless dry-run)
                    let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                    let persist_handle = if !options.dry_run {
                        let (handle, _counter) =
                            spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
                        Some(handle)
                    } else {
                        drop(rx);
                        None
                    };

                    let result = sync_starred_streaming(
                        &client,
                        &options,
                        rate_limiter.as_ref(),
                        Some(&*db),
                        options.concurrency,
                        no_rate_limit,
                        tx,
                        Some(&*progress),
                    )
                    .await?;

                    // Wait for persistence to complete
                    let persist_result = if let Some(handle) = persist_handle {
                        await_persist_task(handle).await
                    } else {
                        PersistTaskResult::default()
                    };
                    let saved = persist_result.saved_count;

                    // Delete pruned repos from database (unless dry-run)
                    let deleted = if !options.dry_run && !result.pruned_repos.is_empty() {
                        repository::delete_by_owner_name(
                            &db,
                            CodePlatform::GitLab,
                            &result.pruned_repos,
                        )
                        .await
                        .unwrap_or(0)
                    } else {
                        0
                    };

                    // Finish progress bars before printing summary
                    reporter.finish();

                    if is_tty {
                        println!("\nSync results for starred projects (GitLab):");
                        println!("  Total starred:         {}", result.processed);
                        println!(
                            "  Active (last {} days): {}",
                            active_within_days, result.matched
                        );

                        if options.dry_run {
                            println!("  Would save:            {}", result.matched);
                            if prune {
                                println!("  Would prune:           {}", result.pruned);
                            }
                        } else {
                            println!("  Saved to database:     {}", saved);
                            if persist_result.has_errors() {
                                println!(
                                    "  Failed to save:        {}",
                                    persist_result.failed_count()
                                );
                            }
                            if prune {
                                println!("  Pruned (unstarred):    {}", result.pruned);
                                if deleted > 0 {
                                    println!("  Deleted from database: {}", deleted);
                                }
                            }
                        }

                        // Report sync errors
                        if !result.errors.is_empty() {
                            println!("\nSync errors:");
                            for err in &result.errors {
                                println!("  - {}", err);
                            }
                        }

                        // Report persist errors
                        display_persist_errors(&persist_result, is_tty);
                    } else {
                        tracing::info!(
                            processed = result.processed,
                            matched = result.matched,
                            saved = saved,
                            persist_errors = persist_result.failed_count(),
                            pruned = result.pruned,
                            deleted = deleted,
                            errors = result.errors.len(),
                            "Starred sync complete"
                        );
                        display_persist_errors(&persist_result, is_tty);
                    }
                }
                GitlabAction::Limits { output } => {
                    // GitLab doesn't have a dedicated rate limit endpoint like GitHub
                    let info = RateLimitInfoMessage {
                        platform: "GitLab".to_string(),
                        message: "Per-endpoint rate limiting via response headers".to_string(),
                        default_limit: "300 requests/minute (authenticated)".to_string(),
                        note: "Self-hosted instances may have different limits".to_string(),
                        docs_url: "https://docs.gitlab.com/ee/administration/settings/user_and_ip_rate_limits.html".to_string(),
                    };
                    info.print(output);
                }
            }
        }
        #[cfg(feature = "gitea")]
        Commands::Codeberg { action } => {
            match action {
                CodebergAction::Org { orgs, sync_opts } => {
                    // Codeberg always uses codeberg.org
                    let codeberg_host = gitea::CODEBERG_HOST;

                    // Resolve token: env > config
                    let codeberg_token = config.codeberg_token().expect(
                        "CURATOR_CODEBERG_TOKEN must be set in environment, .env file, or config file",
                    );

                    // Merge CLI args with config defaults
                    let active_within_days = sync_opts
                        .active_within_days
                        .unwrap_or(config.sync.active_within_days);
                    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
                    // CLI --no-star overrides config; if not set, use config value
                    let star = if sync_opts.no_star {
                        false
                    } else {
                        config.sync.star
                    };
                    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

                    let client =
                        GiteaClient::new(codeberg_host, &codeberg_token, CodePlatform::Codeberg)?;
                    let db = Arc::new(Database::connect(&database_url).await?);

                    let options = SyncOptions {
                        active_within: chrono::Duration::days(active_within_days as i64),
                        star,
                        dry_run: sync_opts.dry_run,
                        concurrency,
                        platform_options: PlatformOptions::default(),
                        prune: false,
                    };

                    // Create rate limiter (if enabled)
                    let rate_limiter =
                        maybe_rate_limiter(no_rate_limit, rate_limits::GITEA_DEFAULT_RPS);
                    let is_tty = Term::stdout().is_term();
                    if no_rate_limit {
                        warn_no_rate_limit(is_tty);
                    }

                    run_gitea_sync(
                        &client,
                        &orgs,
                        &options,
                        db,
                        active_within_days,
                        "Codeberg",
                        rate_limiter.as_ref(),
                    )
                    .await?;
                }
                CodebergAction::User { users, sync_opts } => {
                    // Codeberg always uses codeberg.org
                    let codeberg_host = gitea::CODEBERG_HOST;

                    // Resolve token: env > config
                    let codeberg_token = config.codeberg_token().expect(
                        "CURATOR_CODEBERG_TOKEN must be set in environment, .env file, or config file",
                    );

                    // Merge CLI args with config defaults
                    let active_within_days = sync_opts
                        .active_within_days
                        .unwrap_or(config.sync.active_within_days);
                    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
                    let star = if sync_opts.no_star {
                        false
                    } else {
                        config.sync.star
                    };
                    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

                    let client =
                        GiteaClient::new(codeberg_host, &codeberg_token, CodePlatform::Codeberg)?;
                    let db = Arc::new(Database::connect(&database_url).await?);

                    let options = SyncOptions {
                        active_within: chrono::Duration::days(active_within_days as i64),
                        star,
                        dry_run: sync_opts.dry_run,
                        concurrency,
                        platform_options: PlatformOptions::default(),
                        prune: false,
                    };

                    let is_tty = Term::stdout().is_term();

                    if options.dry_run && is_tty {
                        println!("DRY RUN - no changes will be made\n");
                    }

                    // Create progress reporter
                    let reporter = Arc::new(ProgressReporter::new());
                    let progress = reporter.as_callback();

                    // Create rate limiter (if enabled)
                    let rate_limiter =
                        maybe_rate_limiter(no_rate_limit, rate_limits::GITEA_DEFAULT_RPS);
                    if no_rate_limit {
                        warn_no_rate_limit(is_tty);
                    }

                    if users.len() == 1 {
                        let user = &users[0];

                        if is_tty {
                            println!("Syncing repositories for user '{}'...\n", user);
                        }

                        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                        let persist_handle = if !options.dry_run {
                            let (handle, _counter) = spawn_persist_task(
                                Arc::clone(&db),
                                rx,
                                Some(Arc::clone(&progress)),
                            );
                            Some(handle)
                        } else {
                            drop(rx);
                            None
                        };

                        let result = sync_user_streaming(
                            &client,
                            user,
                            &options,
                            rate_limiter.as_ref(),
                            Some(&*db),
                            tx,
                            Some(&*progress),
                        )
                        .await?;

                        let persist_result = if let Some(handle) = persist_handle {
                            await_persist_task(handle).await
                        } else {
                            PersistTaskResult::default()
                        };
                        let saved = persist_result.saved_count;

                        reporter.finish();

                        if is_tty {
                            println!("\nSync results for user '{}':", user);
                            println!("  Total repositories:    {}", result.processed);
                            println!(
                                "  Active (last {} days): {}",
                                active_within_days, result.matched
                            );

                            if options.star {
                                if options.dry_run {
                                    println!("  Would star:            {}", result.starred);
                                    println!("  Already starred:       {}", result.skipped);
                                } else {
                                    println!("  Starred:               {}", result.starred);
                                    println!("  Already starred:       {}", result.skipped);
                                }
                            }

                            if !options.dry_run {
                                println!("  Saved to database:     {}", saved);
                                if persist_result.has_errors() {
                                    println!(
                                        "  Failed to save:        {}",
                                        persist_result.failed_count()
                                    );
                                }
                            } else {
                                println!("  Would save:            {}", result.matched);
                            }

                            // Report sync errors
                            if !result.errors.is_empty() {
                                println!("\nSync errors:");
                                for err in &result.errors {
                                    println!("  - {}", err);
                                }
                            }

                            // Report persist errors
                            display_persist_errors(&persist_result, is_tty);
                        } else {
                            tracing::info!(
                                user = %user,
                                processed = result.processed,
                                matched = result.matched,
                                starred = result.starred,
                                skipped = result.skipped,
                                saved = saved,
                                persist_errors = persist_result.failed_count(),
                                errors = result.errors.len(),
                                "User sync complete"
                            );
                            display_persist_errors(&persist_result, is_tty);
                        }
                    } else {
                        // Multiple users
                        if is_tty {
                            println!("Syncing repositories for {} users...\n", users.len());
                        }

                        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                        let persist_handle = if !options.dry_run {
                            let (handle, _counter) = spawn_persist_task(
                                Arc::clone(&db),
                                rx,
                                Some(Arc::clone(&progress)),
                            );
                            Some(handle)
                        } else {
                            drop(rx);
                            None
                        };

                        let user_results = sync_users_streaming(
                            &client,
                            &users,
                            &options,
                            rate_limiter.as_ref(),
                            None, // db not passed to concurrent syncs
                            tx,
                            Some(&*progress),
                        )
                        .await;

                        let persist_result = if let Some(handle) = persist_handle {
                            await_persist_task(handle).await
                        } else {
                            PersistTaskResult::default()
                        };
                        let total_saved = persist_result.saved_count;

                        reporter.finish();

                        let mut total_processed = 0;
                        let mut total_matched = 0;
                        let mut total_starred = 0;
                        let mut total_skipped = 0;
                        let mut all_errors = Vec::new();

                        for user_result in &user_results {
                            if let Some(err) = &user_result.error {
                                all_errors.push(format!("{}: {}", user_result.namespace, err));
                                continue;
                            }

                            let result = &user_result.result;
                            total_processed += result.processed;
                            total_matched += result.matched;
                            total_starred += result.starred;
                            total_skipped += result.skipped;

                            for err in &result.errors {
                                all_errors.push(format!("{}: {}", user_result.namespace, err));
                            }
                        }

                        if is_tty {
                            println!("\n=== SUMMARY ===");
                            println!("Total repositories processed: {}", total_processed);
                            println!(
                                "Total active (last {} days):  {}",
                                active_within_days, total_matched
                            );
                            if options.star {
                                if options.dry_run {
                                    println!("Total would star:             {}", total_starred);
                                    println!("Total already starred:        {}", total_skipped);
                                } else {
                                    println!("Total starred:                {}", total_starred);
                                    println!("Total already starred:        {}", total_skipped);
                                }
                            }
                            if !options.dry_run {
                                println!("Total saved to database:      {}", total_saved);
                                if persist_result.has_errors() {
                                    println!(
                                        "Total failed to save:         {}",
                                        persist_result.failed_count()
                                    );
                                }
                            } else {
                                println!("Total would save:             {}", total_matched);
                            }

                            // Report sync errors
                            if !all_errors.is_empty() {
                                println!("\nSync errors ({}):", all_errors.len());
                                for err in &all_errors {
                                    println!("  - {}", err);
                                }
                            }

                            // Report persist errors
                            display_persist_errors(&persist_result, is_tty);
                        } else {
                            tracing::info!(
                                users = users.len(),
                                processed = total_processed,
                                matched = total_matched,
                                starred = total_starred,
                                skipped = total_skipped,
                                saved = total_saved,
                                persist_errors = persist_result.failed_count(),
                                errors = all_errors.len(),
                                "User sync complete"
                            );
                            display_persist_errors(&persist_result, is_tty);
                        }
                    }
                }
                CodebergAction::Stars { sync_opts } => {
                    // Codeberg always uses codeberg.org
                    let codeberg_host = gitea::CODEBERG_HOST;

                    // Resolve token: env > config
                    let codeberg_token = config.codeberg_token().expect(
                        "CURATOR_CODEBERG_TOKEN must be set in environment, .env file, or config file",
                    );

                    // Merge CLI args with config defaults
                    let active_within_days = sync_opts
                        .active_within_days
                        .unwrap_or(config.sync.active_within_days);
                    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
                    let prune = !sync_opts.no_prune;
                    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

                    let client =
                        GiteaClient::new(codeberg_host, &codeberg_token, CodePlatform::Codeberg)?;
                    let db = Arc::new(Database::connect(&database_url).await?);

                    let options = SyncOptions {
                        active_within: chrono::Duration::days(active_within_days as i64),
                        star: false, // Not used for starred sync
                        dry_run: sync_opts.dry_run,
                        concurrency,
                        platform_options: PlatformOptions::default(),
                        prune,
                    };

                    let is_tty = Term::stdout().is_term();

                    if options.dry_run && is_tty {
                        println!("DRY RUN - no changes will be made\n");
                    }

                    if is_tty {
                        println!("Syncing starred repositories from Codeberg...\n");
                    }

                    // Create progress reporter
                    let reporter = Arc::new(ProgressReporter::new());
                    let progress = reporter.as_callback();

                    // Create rate limiter (if enabled)
                    let rate_limiter =
                        maybe_rate_limiter(no_rate_limit, rate_limits::GITEA_DEFAULT_RPS);
                    if no_rate_limit {
                        warn_no_rate_limit(is_tty);
                    }

                    // Set up streaming persistence (unless dry-run)
                    let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                    let persist_handle = if !options.dry_run {
                        let (handle, _counter) =
                            spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
                        Some(handle)
                    } else {
                        drop(rx);
                        None
                    };

                    let result = sync_starred_streaming(
                        &client,
                        &options,
                        rate_limiter.as_ref(),
                        Some(&*db),
                        options.concurrency,
                        no_rate_limit,
                        tx,
                        Some(&*progress),
                    )
                    .await?;

                    // Wait for persistence to complete
                    let persist_result = if let Some(handle) = persist_handle {
                        await_persist_task(handle).await
                    } else {
                        PersistTaskResult::default()
                    };
                    let saved = persist_result.saved_count;

                    // Delete pruned repos from database (unless dry-run)
                    let deleted = if !options.dry_run && !result.pruned_repos.is_empty() {
                        repository::delete_by_owner_name(
                            &db,
                            CodePlatform::Codeberg,
                            &result.pruned_repos,
                        )
                        .await
                        .unwrap_or(0)
                    } else {
                        0
                    };

                    // Finish progress bars before printing summary
                    reporter.finish();

                    if is_tty {
                        println!("\nSync results for starred repositories (Codeberg):");
                        println!("  Total starred:         {}", result.processed);
                        println!(
                            "  Active (last {} days): {}",
                            active_within_days, result.matched
                        );

                        if options.dry_run {
                            println!("  Would save:            {}", result.matched);
                            if prune {
                                println!("  Would prune:           {}", result.pruned);
                            }
                        } else {
                            println!("  Saved to database:     {}", saved);
                            if persist_result.has_errors() {
                                println!(
                                    "  Failed to save:        {}",
                                    persist_result.failed_count()
                                );
                            }
                            if prune {
                                println!("  Pruned (unstarred):    {}", result.pruned);
                                if deleted > 0 {
                                    println!("  Deleted from database: {}", deleted);
                                }
                            }
                        }

                        // Report sync errors
                        if !result.errors.is_empty() {
                            println!("\nSync errors:");
                            for err in &result.errors {
                                println!("  - {}", err);
                            }
                        }

                        // Report persist errors
                        display_persist_errors(&persist_result, is_tty);
                    } else {
                        tracing::info!(
                            processed = result.processed,
                            matched = result.matched,
                            saved = saved,
                            persist_errors = persist_result.failed_count(),
                            pruned = result.pruned,
                            deleted = deleted,
                            errors = result.errors.len(),
                            "Starred sync complete"
                        );
                        display_persist_errors(&persist_result, is_tty);
                    }
                }
                CodebergAction::Limits { output } => {
                    let info = RateLimitInfoMessage {
                        platform: "Codeberg".to_string(),
                        message: "Uses Gitea's rate limiting approach".to_string(),
                        default_limit: "Generous limits for authenticated users".to_string(),
                        note: "Rate limits depend on instance configuration".to_string(),
                        docs_url: "https://codeberg.org/Codeberg/Community/wiki/API".to_string(),
                    };
                    info.print(output);
                }
            }
        }
        #[cfg(feature = "gitea")]
        Commands::Gitea { action } => {
            match action {
                GiteaAction::Org {
                    orgs,
                    host,
                    sync_opts,
                } => {
                    // Resolve host: CLI arg > env > config (required)
                    let gitea_host = host.or_else(|| config.gitea_host()).expect(
                        "Gitea host must be specified via --host, CURATOR_GITEA_HOST env var, or config file",
                    );

                    // Resolve token: env > config
                    let gitea_token = config.gitea_token().expect(
                        "CURATOR_GITEA_TOKEN must be set in environment, .env file, or config file",
                    );

                    // Merge CLI args with config defaults
                    let active_within_days = sync_opts
                        .active_within_days
                        .unwrap_or(config.sync.active_within_days);
                    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
                    // CLI --no-star overrides config; if not set, use config value
                    let star = if sync_opts.no_star {
                        false
                    } else {
                        config.sync.star
                    };
                    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

                    let client = GiteaClient::new(&gitea_host, &gitea_token, CodePlatform::Gitea)?;
                    let db = Arc::new(Database::connect(&database_url).await?);

                    let options = SyncOptions {
                        active_within: chrono::Duration::days(active_within_days as i64),
                        star,
                        dry_run: sync_opts.dry_run,
                        concurrency,
                        platform_options: PlatformOptions::default(),
                        prune: false,
                    };

                    // Create rate limiter (if enabled)
                    let rate_limiter =
                        maybe_rate_limiter(no_rate_limit, rate_limits::GITEA_DEFAULT_RPS);
                    let is_tty = Term::stdout().is_term();
                    if no_rate_limit {
                        warn_no_rate_limit(is_tty);
                    }

                    run_gitea_sync(
                        &client,
                        &orgs,
                        &options,
                        db,
                        active_within_days,
                        "Gitea",
                        rate_limiter.as_ref(),
                    )
                    .await?;
                }
                GiteaAction::User {
                    users,
                    host,
                    sync_opts,
                } => {
                    // Resolve host: CLI arg > env > config (required)
                    let gitea_host = host.or_else(|| config.gitea_host()).expect(
                        "Gitea host must be specified via --host, CURATOR_GITEA_HOST env var, or config file",
                    );

                    // Resolve token: env > config
                    let gitea_token = config.gitea_token().expect(
                        "CURATOR_GITEA_TOKEN must be set in environment, .env file, or config file",
                    );

                    // Merge CLI args with config defaults
                    let active_within_days = sync_opts
                        .active_within_days
                        .unwrap_or(config.sync.active_within_days);
                    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
                    let star = if sync_opts.no_star {
                        false
                    } else {
                        config.sync.star
                    };
                    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

                    let client = GiteaClient::new(&gitea_host, &gitea_token, CodePlatform::Gitea)?;
                    let db = Arc::new(Database::connect(&database_url).await?);

                    let options = SyncOptions {
                        active_within: chrono::Duration::days(active_within_days as i64),
                        star,
                        dry_run: sync_opts.dry_run,
                        concurrency,
                        platform_options: PlatformOptions::default(),
                        prune: false,
                    };

                    let is_tty = Term::stdout().is_term();

                    if options.dry_run && is_tty {
                        println!("DRY RUN - no changes will be made\n");
                    }

                    // Create progress reporter
                    let reporter = Arc::new(ProgressReporter::new());
                    let progress = reporter.as_callback();

                    // Create rate limiter (if enabled)
                    let rate_limiter =
                        maybe_rate_limiter(no_rate_limit, rate_limits::GITEA_DEFAULT_RPS);
                    if no_rate_limit {
                        warn_no_rate_limit(is_tty);
                    }

                    if users.len() == 1 {
                        let user = &users[0];

                        if is_tty {
                            println!(
                                "Syncing repositories for user '{}' on {}...\n",
                                user, gitea_host
                            );
                        }

                        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                        let persist_handle = if !options.dry_run {
                            let (handle, _counter) = spawn_persist_task(
                                Arc::clone(&db),
                                rx,
                                Some(Arc::clone(&progress)),
                            );
                            Some(handle)
                        } else {
                            drop(rx);
                            None
                        };

                        let result = sync_user_streaming(
                            &client,
                            user,
                            &options,
                            rate_limiter.as_ref(),
                            Some(&*db),
                            tx,
                            Some(&*progress),
                        )
                        .await?;

                        let persist_result = if let Some(handle) = persist_handle {
                            await_persist_task(handle).await
                        } else {
                            PersistTaskResult::default()
                        };
                        let saved = persist_result.saved_count;

                        reporter.finish();

                        if is_tty {
                            println!("\nSync results for user '{}':", user);
                            println!("  Total repositories:    {}", result.processed);
                            println!(
                                "  Active (last {} days): {}",
                                active_within_days, result.matched
                            );
                            if options.star {
                                if options.dry_run {
                                    println!("  Would star:            {}", result.starred);
                                    println!("  Already starred:       {}", result.skipped);
                                } else {
                                    println!("  Starred:               {}", result.starred);
                                    println!("  Already starred:       {}", result.skipped);
                                }
                            }
                            if !options.dry_run {
                                println!("  Saved to database:     {}", saved);
                                if persist_result.has_errors() {
                                    println!(
                                        "  Failed to save:        {}",
                                        persist_result.failed_count()
                                    );
                                }
                            }

                            // Report sync errors
                            if !result.errors.is_empty() {
                                println!("\nSync errors ({}):", result.errors.len());
                                for err in &result.errors {
                                    println!("  - {}", err);
                                }
                            }

                            // Report persist errors
                            display_persist_errors(&persist_result, is_tty);
                        } else {
                            tracing::info!(
                                user = %user,
                                processed = result.processed,
                                matched = result.matched,
                                starred = result.starred,
                                skipped = result.skipped,
                                saved = saved,
                                persist_errors = persist_result.failed_count(),
                                errors = result.errors.len(),
                                "User sync complete"
                            );
                            display_persist_errors(&persist_result, is_tty);
                        }
                    } else {
                        // Multiple users
                        if is_tty {
                            println!(
                                "Syncing repositories for {} users on {}...\n",
                                users.len(),
                                gitea_host
                            );
                        }

                        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                        let persist_handle = if !options.dry_run {
                            let (handle, _counter) = spawn_persist_task(
                                Arc::clone(&db),
                                rx,
                                Some(Arc::clone(&progress)),
                            );
                            Some(handle)
                        } else {
                            drop(rx);
                            None
                        };

                        let user_results = sync_users_streaming(
                            &client,
                            &users,
                            &options,
                            rate_limiter.as_ref(),
                            None, // db not passed to concurrent syncs
                            tx,
                            Some(&*progress),
                        )
                        .await;

                        let persist_result = if let Some(handle) = persist_handle {
                            await_persist_task(handle).await
                        } else {
                            PersistTaskResult::default()
                        };
                        let total_saved = persist_result.saved_count;

                        reporter.finish();

                        let mut total_processed = 0;
                        let mut total_matched = 0;
                        let mut total_starred = 0;
                        let mut total_skipped = 0;
                        let mut all_errors = Vec::new();

                        for user_result in &user_results {
                            if let Some(err) = &user_result.error {
                                all_errors.push(format!("{}: {}", user_result.namespace, err));
                                continue;
                            }

                            let result = &user_result.result;
                            total_processed += result.processed;
                            total_matched += result.matched;
                            total_starred += result.starred;
                            total_skipped += result.skipped;

                            for err in &result.errors {
                                all_errors.push(format!("{}: {}", user_result.namespace, err));
                            }
                        }

                        if is_tty {
                            println!("\n=== SUMMARY ===");
                            println!("Total repositories processed: {}", total_processed);
                            println!(
                                "Total active (last {} days):  {}",
                                active_within_days, total_matched
                            );
                            if options.star {
                                if options.dry_run {
                                    println!("Total would star:             {}", total_starred);
                                    println!("Total already starred:        {}", total_skipped);
                                } else {
                                    println!("Total starred:                {}", total_starred);
                                    println!("Total already starred:        {}", total_skipped);
                                }
                            }
                            if !options.dry_run {
                                println!("Total saved to database:      {}", total_saved);
                                if persist_result.has_errors() {
                                    println!(
                                        "Total failed to save:         {}",
                                        persist_result.failed_count()
                                    );
                                }
                            } else {
                                println!("Total would save:             {}", total_matched);
                            }

                            // Report sync errors
                            if !all_errors.is_empty() {
                                println!("\nSync errors ({}):", all_errors.len());
                                for err in &all_errors {
                                    println!("  - {}", err);
                                }
                            }

                            // Report persist errors
                            display_persist_errors(&persist_result, is_tty);
                        } else {
                            tracing::info!(
                                users = users.len(),
                                processed = total_processed,
                                matched = total_matched,
                                starred = total_starred,
                                skipped = total_skipped,
                                saved = total_saved,
                                persist_errors = persist_result.failed_count(),
                                errors = all_errors.len(),
                                "User sync complete"
                            );
                            display_persist_errors(&persist_result, is_tty);
                        }
                    }
                }
                GiteaAction::Stars { host, sync_opts } => {
                    // Resolve host: CLI arg > env > config (required)
                    let gitea_host = host.or_else(|| config.gitea_host()).expect(
                        "Gitea host must be specified via --host, CURATOR_GITEA_HOST env var, or config file",
                    );

                    // Resolve token: env > config
                    let gitea_token = config.gitea_token().expect(
                        "CURATOR_GITEA_TOKEN must be set in environment, .env file, or config file",
                    );

                    // Merge CLI args with config defaults
                    let active_within_days = sync_opts
                        .active_within_days
                        .unwrap_or(config.sync.active_within_days);
                    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
                    let prune = !sync_opts.no_prune;
                    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

                    let client = GiteaClient::new(&gitea_host, &gitea_token, CodePlatform::Gitea)?;
                    let db = Arc::new(Database::connect(&database_url).await?);

                    let options = SyncOptions {
                        active_within: chrono::Duration::days(active_within_days as i64),
                        star: false, // Not used for starred sync
                        dry_run: sync_opts.dry_run,
                        concurrency,
                        platform_options: PlatformOptions::default(),
                        prune,
                    };

                    let is_tty = Term::stdout().is_term();

                    if options.dry_run && is_tty {
                        println!("DRY RUN - no changes will be made\n");
                    }

                    if is_tty {
                        println!("Syncing starred repositories from {}...\n", gitea_host);
                    }

                    // Create progress reporter
                    let reporter = Arc::new(ProgressReporter::new());
                    let progress = reporter.as_callback();

                    // Create rate limiter (if enabled)
                    let rate_limiter =
                        maybe_rate_limiter(no_rate_limit, rate_limits::GITEA_DEFAULT_RPS);
                    if no_rate_limit {
                        warn_no_rate_limit(is_tty);
                    }

                    // Set up streaming persistence (unless dry-run)
                    let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                    let persist_handle = if !options.dry_run {
                        let (handle, _counter) =
                            spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
                        Some(handle)
                    } else {
                        drop(rx);
                        None
                    };

                    let result = sync_starred_streaming(
                        &client,
                        &options,
                        rate_limiter.as_ref(),
                        Some(&*db),
                        options.concurrency,
                        no_rate_limit,
                        tx,
                        Some(&*progress),
                    )
                    .await?;

                    // Wait for persistence to complete
                    let persist_result = if let Some(handle) = persist_handle {
                        await_persist_task(handle).await
                    } else {
                        PersistTaskResult::default()
                    };
                    let saved = persist_result.saved_count;

                    // Delete pruned repos from database (unless dry-run)
                    // Note: Gitea platform type depends on the host
                    let platform = client.platform();
                    let deleted = if !options.dry_run && !result.pruned_repos.is_empty() {
                        repository::delete_by_owner_name(&db, platform, &result.pruned_repos)
                            .await
                            .unwrap_or(0)
                    } else {
                        0
                    };

                    // Finish progress bars before printing summary
                    reporter.finish();

                    if is_tty {
                        println!("\nSync results for starred repositories (Gitea):");
                        println!("  Total starred:         {}", result.processed);
                        println!(
                            "  Active (last {} days): {}",
                            active_within_days, result.matched
                        );

                        if options.dry_run {
                            println!("  Would save:            {}", result.matched);
                            if prune {
                                println!("  Would prune:           {}", result.pruned);
                            }
                        } else {
                            println!("  Saved to database:     {}", saved);
                            if persist_result.has_errors() {
                                println!(
                                    "  Failed to save:        {}",
                                    persist_result.failed_count()
                                );
                            }
                            if prune {
                                println!("  Pruned (unstarred):    {}", result.pruned);
                                if deleted > 0 {
                                    println!("  Deleted from database: {}", deleted);
                                }
                            }
                        }

                        // Report sync errors
                        if !result.errors.is_empty() {
                            println!("\nSync errors:");
                            for err in &result.errors {
                                println!("  - {}", err);
                            }
                        }

                        // Report persist errors
                        display_persist_errors(&persist_result, is_tty);
                    } else {
                        tracing::info!(
                            processed = result.processed,
                            matched = result.matched,
                            saved = saved,
                            persist_errors = persist_result.failed_count(),
                            pruned = result.pruned,
                            deleted = deleted,
                            errors = result.errors.len(),
                            "Starred sync complete"
                        );
                        display_persist_errors(&persist_result, is_tty);
                    }
                }
                GiteaAction::Limits { output } => {
                    let info = RateLimitInfoMessage {
                        platform: "Gitea/Forgejo".to_string(),
                        message: "Instance-specific rate limiting configuration".to_string(),
                        default_limit: "Varies by instance".to_string(),
                        note: "Contact your instance administrator for details".to_string(),
                        docs_url: "https://docs.gitea.com/ | https://forgejo.org/docs/".to_string(),
                    };
                    info.print(output);
                }
            }
        }
        // Completions and Man are handled early (before database_url check)
        Commands::Completions { .. } | Commands::Man { .. } => unreachable!(),
    }

    Ok(())
}
