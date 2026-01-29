//! Curator CLI - command-line interface for the repository tracker.

mod commands;
mod config;
mod progress;
mod shutdown;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use console::Term;
use tracing_subscriber::EnvFilter;

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
    /// Authenticate with GitLab using OAuth Device Flow
    ///
    /// Opens your browser to authorize Curator with GitLab.
    /// The token is saved to your config file for future use.
    /// Supports self-hosted GitLab instances via --host.
    Login {
        /// GitLab host (default: gitlab.com, or from config/env)
        #[arg(short = 'H', long)]
        host: Option<String>,
    },
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
    /// Authenticate with Codeberg using OAuth PKCE flow
    ///
    /// Opens your browser to authorize Curator with Codeberg.
    /// The token is saved to your config file for future use.
    Login,
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
            commands::migrate::handle_migrate(action, &database_url).await?;
        }
        #[cfg(feature = "github")]
        Commands::Github { action } => {
            commands::github::handle_github(action, &config, &database_url).await?;
        }
        #[cfg(feature = "gitlab")]
        Commands::Gitlab { action } => {
            commands::gitlab::handle_gitlab(action, &config, &database_url).await?;
        }
        #[cfg(feature = "gitea")]
        Commands::Codeberg { action } => {
            commands::gitea::handle_codeberg(action, &config, &database_url).await?;
        }
        #[cfg(feature = "gitea")]
        Commands::Gitea { action } => {
            commands::gitea::handle_gitea(action, &config, &database_url).await?;
        }
        Commands::Completions { .. } | Commands::Man { .. } => {}
    }

    Ok(())
}
