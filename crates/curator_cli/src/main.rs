//! Curator CLI - command-line interface for the repository tracker.

mod commands;
mod config;
mod progress;
mod shutdown;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use console::Term;
use tracing_subscriber::EnvFilter;

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use crate::commands::limits::OutputFormat;

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
        $ curator login github

    Sync all repos from a GitHub organization:
        $ curator sync org github rust-lang

    Sync starred repos and prune inactive ones:
        $ curator sync stars github

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
      1. ~/.config/curator/config.toml (or $XDG_CONFIG_HOME/curator/config.toml)
      2. Environment variables (CURATOR_* prefix, e.g., CURATOR_GITHUB_TOKEN)
      3. .env file in current directory

ENVIRONMENT VARIABLES
    CURATOR_DATABASE_URL      Database connection string (default: sqlite://~/.local/state/curator/curator.db?mode=rwc)
    CURATOR_GITHUB_TOKEN      GitHub personal access token
    CURATOR_GITLAB_TOKEN      GitLab personal access token
    CURATOR_GITEA_TOKEN       Gitea/Forgejo personal access token
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
    /// Manage platform instances (github.com, gitlab.com, self-hosted, etc.)
    Instance {
        #[command(subcommand)]
        action: commands::instance::InstanceAction,
    },
    /// Authenticate with a platform instance
    ///
    /// Logs in to the specified instance using the appropriate OAuth flow.
    /// For well-known instances (github, gitlab, codeberg), the instance
    /// is auto-created if it doesn't exist.
    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    Login {
        /// Instance name (e.g., "github", "gitlab", "codeberg", or a custom name)
        instance: String,
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
    #[arg(short = 'a', long)]
    active_within_days: Option<u64>,

    /// Don't prune (unstar) inactive repositories
    #[arg(short = 'P', long)]
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

/// Options for discovery crawling.
#[cfg(all(
    feature = "discovery",
    any(feature = "github", feature = "gitlab", feature = "gitea")
))]
#[derive(Debug, Clone, clap::Args)]
struct DiscoverOptions {
    /// Maximum crawl depth (default: 2)
    #[arg(short = 'd', long, default_value_t = 2)]
    max_depth: usize,

    /// Maximum pages to fetch (default: 1000)
    #[arg(short = 'p', long, default_value_t = 1000)]
    max_pages: usize,

    /// Maximum concurrent crawl requests (default: 10)
    #[arg(short = 'C', long, default_value_t = 10)]
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

    // Load configuration (config file -> env vars -> defaults)
    let config = config::Config::load();

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

    let database_url = config
        .database_url()
        .ok_or_else(|| {
            std::io::Error::other(
                "Failed to determine database URL. Set CURATOR_DATABASE_URL or configure [database].url.",
            )
        })?;

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
            commands::migrate::handle_migrate(action, &database_url).await?;
        }
        Commands::Instance { action } => {
            let db = curator::db::connect_and_migrate(&database_url).await?;
            commands::instance::handle_instance(action, &db).await?;
        }
        #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
        Commands::Login { instance } => {
            let db = curator::db::connect_and_migrate(&database_url).await?;
            commands::login::handle_login(&instance, &db, &config).await?;
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
