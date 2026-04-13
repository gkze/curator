//! Curator CLI - command-line interface for the repository tracker.

mod commands;
mod config;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
mod credentials;
mod progress;
mod shutdown;
#[cfg(test)]
mod test_support;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use console::Term;
use tracing_subscriber::EnvFilter;

use crate::commands::OutputFormat;

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
    Add a well-known instance (github.com):
        $ curator instance add github

    Add a self-hosted GitLab instance:
        $ curator instance add my-gitlab -t gitlab -H gitlab.mycompany.com

    Authenticate with an instance:
        $ curator auth login github

    Sync all repos from a GitHub organization:
        $ curator sync org github rust-lang

    Sync starred repos and prune inactive ones:
        $ curator sync stars github

    Sync starred repos for all configured instances:
        $ curator sync stars --all

    Sync from multiple GitLab groups:
        $ curator sync org gitlab gitlab-org my-company/team

    Dry run to see what would happen:
        $ curator sync org github kubernetes --dry-run

    Show rate limits for an instance:
        $ curator limits github

    Generate shell completions:
        $ curator completions bash > ~/.local/share/bash-completion/completions/curator

CONFIGURATION
    Curator reads configuration from:
      1. CLI flags
      2. Environment variables (CURATOR_* prefix, e.g., CURATOR_INSTANCE_WORK_GITLAB_TOKEN)
         .env in the current directory is loaded into environment variables when present
      3. Config files (./curator.toml, then ~/.config/curator/config.toml)
      4. Built-in defaults

ENVIRONMENT VARIABLES
    CURATOR_DATABASE_URL      Database connection string (default: sqlite://~/.local/state/curator/curator.db?mode=rwc)
    CURATOR_INSTANCE_<NAME>_TOKEN  Per-instance token override (blank values ignored)

AUTH STORAGE
    Per-instance credentials are stored using [auth].credential_store.
    Recommended: auto or keychain.
    The db backend stores secrets in the curator database in plaintext-at-rest.
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
    /// Manage platform instances (github.com, gitlab.com, self-hosted, etc.)
    Instance {
        #[command(subcommand)]
        action: commands::instance::InstanceAction,
    },
    /// Deprecated compatibility alias for `auth login`
    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[command(hide = true)]
    Login {
        /// Instance name (e.g., "github", "gitlab", "codeberg", or a custom name)
        instance: String,
    },
    /// Authenticate with instances and inspect stored credentials
    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    Auth {
        #[command(subcommand)]
        action: commands::auth::AuthAction,
    },
    /// Sync repositories from a platform instance
    ///
    /// Syncs repositories from organizations, groups, users, or starred lists.
    /// The instance must be added first with `curator instance add`.
    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    Sync {
        #[command(subcommand)]
        action: commands::sync::SyncAction,
    },
    /// Discover repositories by crawling a website URL
    #[cfg(all(
        feature = "discovery",
        any(feature = "github", feature = "gitlab", feature = "gitea")
    ))]
    Discover {
        /// URL to crawl for repository links
        url: String,

        #[command(flatten)]
        discover_opts: DiscoverOptions,

        #[command(flatten)]
        sync_opts: CommonSyncOptions,
    },
    /// Show rate limit status for a platform instance
    ///
    /// Displays current API rate limit information.
    /// For GitHub: shows detailed per-resource rate limits.
    /// For GitLab/Gitea: shows informational rate limiting guidance.
    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    Limits {
        /// Instance name (e.g., "github", "gitlab", "codeberg", or a custom name)
        instance: String,

        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
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

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|err| format!("invalid value '{value}': {err}"))?;

    if parsed == 0 {
        Err("value must be at least 1".to_string())
    } else {
        Ok(parsed)
    }
}

/// Common sync options shared across all platforms and sync commands.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[derive(Debug, Clone, clap::Args)]
struct CommonSyncOptions {
    /// Only include repos active within this many days (default from config or 60)
    #[arg(short = 'd', long = "days")]
    active_within_days: Option<u64>,

    /// Don't star repositories (overrides config; starring is on by default)
    #[arg(short = 'S', long)]
    no_star: bool,

    /// Dry run - show what would be done without making changes
    #[arg(short = 'n', long)]
    dry_run: bool,

    /// Maximum concurrent API requests (default from config or 20)
    #[arg(short = 'c', long, value_parser = parse_positive_usize)]
    concurrency: Option<usize>,

    /// Disable proactive rate limiting (may cause API throttling)
    #[arg(short = 'R', long)]
    no_rate_limit: bool,

    /// Incremental sync - only process repos that changed since last sync
    ///
    /// When enabled, compares the platform's updated_at/pushed_at timestamps
    /// with the stored synced_at to skip repositories that haven't changed.
    /// Falls back to full sync if no prior sync data exists.
    #[arg(short = 'i', long)]
    incremental: bool,
}

/// Options for syncing starred repositories.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[derive(Debug, Clone, clap::Args)]
struct StarredSyncOptions {
    /// Only include repos active within this many days (default from config or 60)
    #[arg(short = 'd', long = "days")]
    active_within_days: Option<u64>,

    /// Don't prune (unstar) inactive repositories
    #[arg(short = 'P', long)]
    no_prune: bool,

    /// Dry run - show what would be done without making changes
    #[arg(short = 'n', long)]
    dry_run: bool,

    /// Maximum concurrent API requests (default from config or 20)
    #[arg(short = 'c', long, value_parser = parse_positive_usize)]
    concurrency: Option<usize>,

    /// Disable proactive rate limiting (may cause API throttling)
    #[arg(short = 'R', long)]
    no_rate_limit: bool,
}

/// Options for discovery crawling.
#[cfg(all(
    feature = "discovery",
    any(feature = "github", feature = "gitlab", feature = "gitea")
))]
#[derive(Debug, Clone, clap::Args)]
struct DiscoverOptions {
    /// Maximum crawl depth (default: 2)
    #[arg(short = 'D', long, default_value_t = 2)]
    max_depth: usize,

    /// Maximum pages to fetch (default: 1000)
    #[arg(short = 'p', long, default_value_t = 1000)]
    max_pages: usize,

    /// Maximum concurrent crawl requests (default: 10)
    #[arg(short = 'C', long, default_value_t = 10, value_parser = parse_positive_usize)]
    crawl_concurrency: usize,

    /// Allow crawling external hosts (default: same host only)
    #[arg(short = 'x', long)]
    allow_external: bool,

    /// Include subdomains when crawling (default: false)
    #[arg(short = 's', long)]
    include_subdomains: bool,

    /// Disable sitemap discovery
    #[arg(short = 'm', long)]
    no_sitemaps: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    // Set up graceful shutdown handler (Ctrl+C)
    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    shutdown::setup_shutdown_handler();

    // Initialize tracing for non-TTY mode (structured logging)
    // Only initialize if not connected to a TTY
    if !Term::stdout().is_term() {
        let env_filter = match EnvFilter::try_from_default_env() {
            Ok(filter) => filter,
            Err(_) => EnvFilter::new("curator=info,curator_cli=info"),
        };

        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .init();
    }

    let cli = Cli::parse();

    // Handle commands that don't require database access first
    match &cli.command {
        Commands::Completions { shell } => {
            commands::meta::handle_completions(*shell)?;
            return Ok(());
        }
        Commands::Man { output } => {
            commands::meta::handle_man(output.clone())?;
            return Ok(());
        }
        _ => {}
    }

    // Load configuration (defaults -> config files -> environment variables).
    // `.env` was loaded above, so it participates as environment values.
    let config = config::Config::try_load().map_err(|e| {
        std::io::Error::other(format!(
            "Failed to load curator config: {e}. Fix config syntax or run with a clean environment."
        ))
    })?;

    let database_url = config
        .database_url()
        .ok_or_else(|| {
            std::io::Error::other(
                "Failed to determine database URL. Set CURATOR_DATABASE_URL or configure [database].url.",
            )
        })?;

    // Ensure the database directory exists for SQLite
    ensure_sqlite_database_dir(&database_url)?;

    match cli.command {
        Commands::Migrate { action } => {
            commands::migrate::handle_migrate(action, &database_url).await?;
        }
        Commands::Instance { action } => {
            let db = curator::db::connect_and_migrate(&database_url).await?;
            commands::instance::handle_instance(action, &db, &config).await?;
        }
        #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
        Commands::Login { instance } => {
            let db = curator::db::connect_and_migrate(&database_url).await?;
            commands::login::handle_login(&instance, &db, &config).await?;
        }
        #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
        Commands::Auth { action } => {
            let db = curator::db::connect_and_migrate(&database_url).await?;
            commands::auth::handle_auth(action, &db, &config).await?;
        }
        #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
        Commands::Sync { action } => {
            commands::sync::handle_sync(action, &config, &database_url).await?;
        }
        #[cfg(all(
            feature = "discovery",
            any(feature = "github", feature = "gitlab", feature = "gitea")
        ))]
        Commands::Discover {
            url,
            discover_opts,
            sync_opts,
        } => {
            commands::discover::handle_discover(
                url,
                discover_opts,
                sync_opts,
                &config,
                &database_url,
            )
            .await?;
        }
        #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
        Commands::Limits { instance, output } => {
            let db = curator::db::connect_and_migrate(&database_url).await?;
            commands::limits::handle_limits(&instance, output, &config, &db).await?;
        }
        Commands::Completions { .. } | Commands::Man { .. } => {}
    }

    Ok(())
}

fn sqlite_database_parent(database_url: &str) -> Option<(std::path::PathBuf, bool)> {
    config::sqlite_database_parent(database_url)
}

fn ensure_sqlite_database_dir(database_url: &str) -> Result<(), std::io::Error> {
    let Some((parent, warn_relative)) = sqlite_database_parent(database_url) else {
        return Ok(());
    };

    if warn_relative {
        tracing::warn!(
            "Database path '{}' is relative - behavior depends on current directory. Consider using an absolute path.",
            parent.display()
        );
    }

    if !parent.as_os_str().is_empty() {
        std::fs::create_dir_all(parent)?;
    }

    Ok(())
}

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
