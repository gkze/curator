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
    CURATOR_INSTANCE_<NAME>_TOKEN  Per-instance token override
    CURATOR_GITHUB_TOKEN      GitHub personal access token (legacy global fallback)
    CURATOR_GITLAB_TOKEN      GitLab personal access token (legacy global fallback)
    CURATOR_GITEA_TOKEN       Gitea/Forgejo personal access token (legacy global fallback)
    CURATOR_CODEBERG_TOKEN    Codeberg personal access token (legacy global fallback)

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
    /// Inspect and manage stored credentials
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
    #[arg(short = 'd', long = "days")]
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
    #[arg(short = 'D', long, default_value_t = 2)]
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
    if !database_url.starts_with("sqlite://") {
        return None;
    }

    let db_path = database_url.trim_start_matches("sqlite://");
    let db_path = db_path.split('?').next().unwrap_or(db_path);
    let db_path = std::path::Path::new(db_path);

    let parent = db_path.parent()?.to_path_buf();
    let warn_relative = db_path.is_relative() && !db_path.as_os_str().is_empty();
    Some((parent, warn_relative))
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
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn parses_migrate_status_subcommand() {
        let cli = Cli::try_parse_from(["curator", "migrate", "status"])
            .expect("migrate status should parse");

        match cli.command {
            Commands::Migrate { action } => assert!(matches!(action, MigrateAction::Status)),
            _ => panic!("expected migrate command"),
        }
    }

    #[test]
    fn parses_other_migrate_subcommands() {
        for (arg, expected) in [("up", "up"), ("down", "down"), ("fresh", "fresh")] {
            let cli = Cli::try_parse_from(["curator", "migrate", arg])
                .expect("migrate subcommand should parse");

            match cli.command {
                Commands::Migrate { action } => match (expected, action) {
                    ("up", MigrateAction::Up)
                    | ("down", MigrateAction::Down)
                    | ("fresh", MigrateAction::Fresh) => {}
                    _ => panic!("unexpected migrate action"),
                },
                _ => panic!("expected migrate command"),
            }
        }
    }

    #[test]
    fn parses_completions_shell_argument() {
        let cli = Cli::try_parse_from(["curator", "completions", "bash"])
            .expect("completions should parse");

        match cli.command {
            Commands::Completions { shell } => assert_eq!(shell, clap_complete::Shell::Bash),
            _ => panic!("expected completions command"),
        }
    }

    #[test]
    fn parses_man_output_argument() {
        let cli = Cli::try_parse_from(["curator", "man", "--output", "/tmp/man"])
            .expect("man output should parse");

        match cli.command {
            Commands::Man { output } => assert_eq!(output, Some(PathBuf::from("/tmp/man"))),
            _ => panic!("expected man command"),
        }
    }

    #[test]
    fn rejects_invocation_without_subcommand() {
        let parsed = Cli::try_parse_from(["curator"]);
        assert!(parsed.is_err());
    }

    #[test]
    fn sqlite_database_parent_handles_sqlite_and_non_sqlite_urls() {
        assert!(sqlite_database_parent("postgres://localhost/db").is_none());

        let (parent, warn_relative) = sqlite_database_parent("sqlite:///tmp/curator.db?mode=rwc")
            .expect("sqlite path should parse");
        assert_eq!(parent, PathBuf::from("/tmp"));
        assert!(!warn_relative);

        let (parent, warn_relative) = sqlite_database_parent("sqlite://var/data/curator.db")
            .expect("relative sqlite path should parse");
        assert_eq!(parent, PathBuf::from("var/data"));
        assert!(warn_relative);
    }

    #[test]
    fn ensure_sqlite_database_dir_creates_parent_directory() {
        let root = std::env::temp_dir().join(format!("curator-main-{}", Uuid::new_v4()));
        let db_path = root.join("nested").join("curator.db");
        let url = format!("sqlite://{}?mode=rwc", db_path.display());

        ensure_sqlite_database_dir(&url).expect("sqlite dir creation should succeed");
        assert!(root.join("nested").exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_sqlite_database_dir_is_noop_for_bare_filename() {
        ensure_sqlite_database_dir("sqlite://curator.db").expect("bare filename should not fail");
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_stars_all_without_instance() {
        let cli = Cli::try_parse_from(["curator", "sync", "stars", "--all"])
            .expect("sync stars --all should parse");

        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Stars { instance, all, .. } => {
                    assert!(all);
                    assert!(instance.is_none());
                }
                _ => panic!("expected sync stars action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_status_subcommand() {
        let cli = Cli::try_parse_from(["curator", "auth", "status", "github"])
            .expect("auth status should parse");

        match cli.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::Status { instance, .. } => {
                    assert_eq!(instance.as_deref(), Some("github"));
                }
                _ => panic!("expected auth status action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_cleanup_legacy_subcommand() {
        let cli = Cli::try_parse_from(["curator", "auth", "cleanup-legacy"])
            .expect("auth cleanup-legacy should parse");

        match cli.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::CleanupLegacy { .. } => {}
                _ => panic!("expected auth cleanup-legacy action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_login_subcommand() {
        let cli = Cli::try_parse_from(["curator", "login", "github"]).expect("login should parse");

        match cli.command {
            Commands::Login { instance } => assert_eq!(instance, "github"),
            _ => panic!("expected login command"),
        }
    }

    #[test]
    fn parses_instance_add_subcommand() {
        let cli = Cli::try_parse_from([
            "curator",
            "instance",
            "add",
            "work-gitlab",
            "-t",
            "gitlab",
            "-H",
            "gitlab.work.test",
            "--oauth-flow",
            "pkce",
        ])
        .expect("instance add should parse");

        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Add {
                    name,
                    platform_type,
                    host,
                    oauth_flow,
                    ..
                } => {
                    assert_eq!(name, "work-gitlab");
                    assert_eq!(platform_type.as_deref(), Some("gitlab"));
                    assert_eq!(host.as_deref(), Some("gitlab.work.test"));
                    assert!(matches!(oauth_flow, commands::instance::OauthFlowArg::Pkce));
                }
                _ => panic!("expected instance add action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_instance_update_subcommand() {
        let cli = Cli::try_parse_from([
            "curator",
            "instance",
            "update",
            "github",
            "--clear-oauth-client-id",
            "--oauth-flow",
            "token",
        ])
        .expect("instance update should parse");

        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Update {
                    name,
                    clear_oauth_client_id,
                    oauth_flow,
                    ..
                } => {
                    assert_eq!(name, "github");
                    assert!(clear_oauth_client_id);
                    assert!(matches!(
                        oauth_flow,
                        Some(commands::instance::OauthFlowArg::Token)
                    ));
                }
                _ => panic!("expected instance update action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_instance_remove_subcommand() {
        let cli = Cli::try_parse_from(["curator", "instance", "remove", "github", "--yes"])
            .expect("instance remove should parse");

        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Remove { name, yes } => {
                    assert_eq!(name, "github");
                    assert!(yes);
                }
                _ => panic!("expected instance remove action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_instance_show_subcommand() {
        let cli =
            Cli::try_parse_from(["curator", "instance", "show", "github", "--output", "json"])
                .expect("instance show should parse");

        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Show { name, output } => {
                    assert_eq!(name, "github");
                    assert!(matches!(output, OutputFormat::Json));
                }
                _ => panic!("expected instance show action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_logout_and_migrate_subcommands() {
        let logout = Cli::try_parse_from(["curator", "auth", "logout", "github"])
            .expect("auth logout should parse");
        match logout.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::Logout { instance } => assert_eq!(instance, "github"),
                _ => panic!("expected auth logout action"),
            },
            _ => panic!("expected auth command"),
        }

        let migrate = Cli::try_parse_from(["curator", "auth", "migrate", "--output", "json"])
            .expect("auth migrate should parse");
        match migrate.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::Migrate { output } => {
                    assert!(matches!(output, OutputFormat::Json));
                }
                _ => panic!("expected auth migrate action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_limits_subcommand() {
        let cli = Cli::try_parse_from(["curator", "limits", "github", "--output", "json"])
            .expect("limits should parse");

        match cli.command {
            Commands::Limits { instance, output } => {
                assert_eq!(instance, "github");
                assert!(matches!(output, OutputFormat::Json));
            }
            _ => panic!("expected limits command"),
        }
    }

    #[cfg(all(
        feature = "discovery",
        any(feature = "github", feature = "gitlab", feature = "gitea")
    ))]
    #[test]
    fn parses_discover_command_with_flags() {
        let cli = Cli::try_parse_from([
            "curator",
            "discover",
            "https://example.com",
            "--max-pages",
            "15",
            "--max-depth",
            "3",
            "--include-subdomains",
            "--no-sitemaps",
            "--dry-run",
        ])
        .expect("discover should parse");

        match cli.command {
            Commands::Discover {
                url,
                discover_opts,
                sync_opts,
            } => {
                assert_eq!(url, "https://example.com");
                assert_eq!(discover_opts.max_pages, 15);
                assert_eq!(discover_opts.max_depth, 3);
                assert!(!discover_opts.allow_external);
                assert!(discover_opts.include_subdomains);
                assert!(discover_opts.no_sitemaps);
                assert!(sync_opts.dry_run);
            }
            _ => panic!("expected discover command"),
        }
    }

    #[cfg(all(
        feature = "discovery",
        any(feature = "github", feature = "gitlab", feature = "gitea")
    ))]
    #[test]
    fn parses_discover_command_with_external_and_concurrency_flags() {
        let cli = Cli::try_parse_from([
            "curator",
            "discover",
            "https://example.com",
            "--allow-external",
            "--crawl-concurrency",
            "25",
        ])
        .expect("discover with external flags should parse");

        match cli.command {
            Commands::Discover { discover_opts, .. } => {
                assert!(discover_opts.allow_external);
                assert_eq!(discover_opts.crawl_concurrency, 25);
            }
            _ => panic!("expected discover command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_stars_single_instance_without_all() {
        let cli = Cli::try_parse_from(["curator", "sync", "stars", "github"])
            .expect("sync stars <instance> should parse");

        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Stars { instance, all, .. } => {
                    assert!(!all);
                    assert_eq!(instance.as_deref(), Some("github"));
                }
                _ => panic!("expected sync stars action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn rejects_sync_stars_without_instance_or_all() {
        let parsed = Cli::try_parse_from(["curator", "sync", "stars"]);
        assert!(parsed.is_err());
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn rejects_sync_stars_with_instance_and_all_together() {
        let parsed = Cli::try_parse_from(["curator", "sync", "stars", "github", "--all"]);
        assert!(parsed.is_err());
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_org_days_flag() {
        let cli = Cli::try_parse_from([
            "curator",
            "sync",
            "org",
            "github",
            "rust-lang",
            "--days",
            "30",
        ])
        .expect("sync org --days should parse");

        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Org { sync_opts, .. } => {
                    assert_eq!(sync_opts.active_within_days, Some(30));
                }
                _ => panic!("expected sync org action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_user_days_flag() {
        let cli = Cli::try_parse_from([
            "curator", "sync", "user", "github", "octocat", "--days", "7",
        ])
        .expect("sync user --days should parse");

        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::User { sync_opts, .. } => {
                    assert_eq!(sync_opts.active_within_days, Some(7));
                }
                _ => panic!("expected sync user action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_stars_days_short_flag() {
        let cli = Cli::try_parse_from(["curator", "sync", "stars", "github", "-d", "14"])
            .expect("sync stars -d should parse");

        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Stars { sync_opts, .. } => {
                    assert_eq!(sync_opts.active_within_days, Some(14));
                }
                _ => panic!("expected sync stars action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_stars_all_with_days_flag() {
        let cli = Cli::try_parse_from(["curator", "sync", "stars", "--all", "-d", "30"])
            .expect("sync stars --all -d should parse");

        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Stars {
                    instance,
                    all,
                    sync_opts,
                } => {
                    assert!(all);
                    assert!(instance.is_none());
                    assert_eq!(sync_opts.active_within_days, Some(30));
                }
                _ => panic!("expected sync stars action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_org_and_stars_extra_flags() {
        let org = Cli::try_parse_from([
            "curator",
            "sync",
            "org",
            "github",
            "rust-lang",
            "--no-star",
            "--dry-run",
            "--concurrency",
            "12",
            "--no-rate-limit",
            "--incremental",
        ])
        .expect("sync org flags should parse");

        match org.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Org { sync_opts, .. } => {
                    assert!(sync_opts.no_star);
                    assert!(sync_opts.dry_run);
                    assert_eq!(sync_opts.concurrency, Some(12));
                    assert!(sync_opts.no_rate_limit);
                    assert!(sync_opts.incremental);
                }
                _ => panic!("expected sync org action"),
            },
            _ => panic!("expected sync command"),
        }

        let stars = Cli::try_parse_from([
            "curator",
            "sync",
            "stars",
            "github",
            "--no-prune",
            "--dry-run",
            "--concurrency",
            "9",
            "--no-rate-limit",
        ])
        .expect("sync stars flags should parse");

        match stars.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Stars { sync_opts, .. } => {
                    assert!(sync_opts.no_prune);
                    assert!(sync_opts.dry_run);
                    assert_eq!(sync_opts.concurrency, Some(9));
                    assert!(sync_opts.no_rate_limit);
                }
                _ => panic!("expected sync stars action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn parses_completions_for_bash() {
        let cli = Cli::try_parse_from(["curator", "completions", "bash"])
            .expect("completions bash should parse");
        match cli.command {
            Commands::Completions { shell } => assert!(matches!(shell, clap_complete::Shell::Bash)),
            _ => panic!("expected completions command"),
        }
    }

    #[test]
    fn parses_man_without_output() {
        let cli = Cli::try_parse_from(["curator", "man"]).expect("man should parse");
        match cli.command {
            Commands::Man { output } => assert!(output.is_none()),
            _ => panic!("expected man command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_status_default_output_and_cleanup_json() {
        let status = Cli::try_parse_from(["curator", "auth", "status"])
            .expect("auth status without instance should parse");
        match status.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::Status { instance, output } => {
                    assert!(instance.is_none());
                    assert!(matches!(output, OutputFormat::Table));
                }
                _ => panic!("expected auth status action"),
            },
            _ => panic!("expected auth command"),
        }

        let cleanup =
            Cli::try_parse_from(["curator", "auth", "cleanup-legacy", "--output", "json"])
                .expect("auth cleanup-legacy json should parse");
        match cleanup.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::CleanupLegacy { output } => {
                    assert!(matches!(output, OutputFormat::Json));
                }
                _ => panic!("expected auth cleanup action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[test]
    fn migrate_actions_parse_all_variants() {
        for (arg, expected_status) in [
            ("up", "up"),
            ("down", "down"),
            ("status", "status"),
            ("fresh", "fresh"),
        ] {
            let cli = Cli::try_parse_from(["curator", "migrate", arg]).unwrap();
            match cli.command {
                Commands::Migrate { action } => match (expected_status, action) {
                    ("up", MigrateAction::Up)
                    | ("down", MigrateAction::Down)
                    | ("status", MigrateAction::Status)
                    | ("fresh", MigrateAction::Fresh) => {}
                    _ => panic!("unexpected migrate variant"),
                },
                _ => panic!("expected migrate command"),
            }
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_org_user_variants() {
        let org = Cli::try_parse_from([
            "curator",
            "sync",
            "org",
            "gitlab",
            "group/sub",
            "--concurrency",
            "3",
        ])
        .expect("sync org should parse");
        match org.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Org {
                    instance,
                    names,
                    sync_opts,
                    ..
                } => {
                    assert_eq!(instance, "gitlab");
                    assert_eq!(names, vec!["group/sub"]);
                    assert_eq!(sync_opts.concurrency, Some(3));
                }
                _ => panic!("expected sync org action"),
            },
            _ => panic!("expected sync command"),
        }

        let user = Cli::try_parse_from([
            "curator",
            "sync",
            "user",
            "github",
            "octocat",
            "--incremental",
        ])
        .expect("sync user should parse");
        match user.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::User {
                    names, sync_opts, ..
                } => {
                    assert_eq!(names, vec!["octocat"]);
                    assert!(sync_opts.incremental);
                }
                _ => panic!("expected sync user action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn rejects_old_active_within_days_flag() {
        let parsed = Cli::try_parse_from([
            "curator",
            "sync",
            "org",
            "github",
            "rust-lang",
            "--active-within-days",
            "30",
        ]);
        assert!(parsed.is_err());
    }

    #[test]
    fn parses_instance_list_default_output() {
        let cli = Cli::try_parse_from(["curator", "instance", "list"]).unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::List { output } => {
                    assert!(matches!(output, OutputFormat::Table));
                }
                _ => panic!("expected instance list action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_instance_add_defaults_to_auto_flow() {
        let cli = Cli::try_parse_from([
            "curator",
            "instance",
            "add",
            "forgejo",
            "--platform-type",
            "gitea",
            "--host",
            "forgejo.example",
        ])
        .unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Add { oauth_flow, .. } => {
                    assert!(matches!(oauth_flow, commands::instance::OauthFlowArg::Auto));
                }
                _ => panic!("expected add action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_migrate_default_output() {
        let cli = Cli::try_parse_from(["curator", "auth", "migrate"]).unwrap();
        match cli.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::Migrate { output } => {
                    assert!(matches!(output, OutputFormat::Table));
                }
                _ => panic!("expected auth migrate action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_stars_all_with_extra_flags() {
        let cli = Cli::try_parse_from([
            "curator",
            "sync",
            "stars",
            "--all",
            "--dry-run",
            "--no-prune",
            "--concurrency",
            "5",
        ])
        .unwrap();
        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Stars { all, sync_opts, .. } => {
                    assert!(all);
                    assert!(sync_opts.dry_run);
                    assert!(sync_opts.no_prune);
                    assert_eq!(sync_opts.concurrency, Some(5));
                }
                _ => panic!("expected stars action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn parses_instance_list_json_output() {
        let cli = Cli::try_parse_from(["curator", "instance", "list", "--output", "json"]).unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::List { output } => {
                    assert!(matches!(output, OutputFormat::Json));
                }
                _ => panic!("expected list action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_instance_show_table_default() {
        let cli = Cli::try_parse_from(["curator", "instance", "show", "github"]).unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Show { name, output } => {
                    assert_eq!(name, "github");
                    assert!(matches!(output, OutputFormat::Table));
                }
                _ => panic!("expected show action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_instance_update_with_client_id() {
        let cli = Cli::try_parse_from([
            "curator",
            "instance",
            "update",
            "github",
            "--oauth-client-id",
            "client-id",
        ])
        .unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Update {
                    name,
                    oauth_client_id,
                    clear_oauth_client_id,
                    oauth_flow,
                } => {
                    assert_eq!(name, "github");
                    assert_eq!(oauth_client_id.as_deref(), Some("client-id"));
                    assert!(!clear_oauth_client_id);
                    assert!(oauth_flow.is_none());
                }
                _ => panic!("expected update action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_completions_for_zsh() {
        let cli = Cli::try_parse_from(["curator", "completions", "zsh"]).unwrap();
        match cli.command {
            Commands::Completions { shell } => assert!(matches!(shell, clap_complete::Shell::Zsh)),
            _ => panic!("expected completions command"),
        }
    }

    #[test]
    fn parses_man_output_directory() {
        let cli = Cli::try_parse_from(["curator", "man", "--output", "/tmp/man"]).unwrap();
        match cli.command {
            Commands::Man { output } => {
                assert_eq!(output.as_deref(), Some(std::path::Path::new("/tmp/man")))
            }
            _ => panic!("expected man command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_logout_and_status_json() {
        let logout = Cli::try_parse_from(["curator", "auth", "logout", "work-gitlab"]).unwrap();
        match logout.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::Logout { instance } => {
                    assert_eq!(instance, "work-gitlab")
                }
                _ => panic!("expected logout action"),
            },
            _ => panic!("expected auth command"),
        }

        let status = Cli::try_parse_from([
            "curator",
            "auth",
            "status",
            "work-gitlab",
            "--output",
            "json",
        ])
        .unwrap();
        match status.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::Status { instance, output } => {
                    assert_eq!(instance.as_deref(), Some("work-gitlab"));
                    assert!(matches!(output, OutputFormat::Json));
                }
                _ => panic!("expected status action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_cleanup_legacy_default_output() {
        let cli = Cli::try_parse_from(["curator", "auth", "cleanup-legacy"]).unwrap();
        match cli.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::CleanupLegacy { output } => {
                    assert!(matches!(output, OutputFormat::Table));
                }
                _ => panic!("expected cleanup action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_stars_single_instance_with_json_flags() {
        let cli = Cli::try_parse_from([
            "curator",
            "sync",
            "stars",
            "github",
            "--days",
            "20",
            "--dry-run",
            "--no-prune",
        ])
        .unwrap();
        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Stars {
                    instance,
                    all,
                    sync_opts,
                } => {
                    assert_eq!(instance.as_deref(), Some("github"));
                    assert!(!all);
                    assert_eq!(sync_opts.active_within_days, Some(20));
                    assert!(sync_opts.dry_run);
                    assert!(sync_opts.no_prune);
                }
                _ => panic!("expected stars action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn parses_instance_remove_without_yes() {
        let cli = Cli::try_parse_from(["curator", "instance", "remove", "github"]).unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Remove { name, yes } => {
                    assert_eq!(name, "github");
                    assert!(!yes);
                }
                _ => panic!("expected remove action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_completions_for_all_shell_examples() {
        for shell in ["bash", "fish", "powershell", "elvish"] {
            let cli = Cli::try_parse_from(["curator", "completions", shell]).unwrap();
            match cli.command {
                Commands::Completions { .. } => {}
                _ => panic!("expected completions command"),
            }
        }
    }

    #[test]
    fn parses_migrate_status_and_fresh_again() {
        let status = Cli::try_parse_from(["curator", "migrate", "status"]).unwrap();
        match status.command {
            Commands::Migrate { action } => assert!(matches!(action, MigrateAction::Status)),
            _ => panic!("expected migrate command"),
        }

        let fresh = Cli::try_parse_from(["curator", "migrate", "fresh"]).unwrap();
        match fresh.command {
            Commands::Migrate { action } => assert!(matches!(action, MigrateAction::Fresh)),
            _ => panic!("expected migrate command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_org_and_user_no_star_flags() {
        let org =
            Cli::try_parse_from(["curator", "sync", "org", "github", "rust-lang", "--no-star"])
                .unwrap();
        match org.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Org { sync_opts, .. } => assert!(sync_opts.no_star),
                _ => panic!("expected org action"),
            },
            _ => panic!("expected sync command"),
        }

        let user =
            Cli::try_parse_from(["curator", "sync", "user", "github", "octocat", "--no-star"])
                .unwrap();
        match user.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::User { sync_opts, .. } => assert!(sync_opts.no_star),
                _ => panic!("expected user action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_org_multiple_names() {
        let cli =
            Cli::try_parse_from(["curator", "sync", "org", "github", "rust-lang", "tokio-rs"])
                .unwrap();
        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Org { names, .. } => {
                    assert_eq!(names, vec!["rust-lang", "tokio-rs"]);
                }
                _ => panic!("expected org action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_user_multiple_names() {
        let cli =
            Cli::try_parse_from(["curator", "sync", "user", "github", "octocat", "hubot"]).unwrap();
        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::User { names, .. } => {
                    assert_eq!(names, vec!["octocat", "hubot"]);
                }
                _ => panic!("expected user action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn parses_limits_default_output() {
        let cli = Cli::try_parse_from(["curator", "limits", "github"]).unwrap();
        match cli.command {
            Commands::Limits { instance, output } => {
                assert_eq!(instance, "github");
                assert!(matches!(output, OutputFormat::Table));
            }
            _ => panic!("expected limits command"),
        }
    }

    #[cfg(all(
        feature = "discovery",
        any(feature = "github", feature = "gitlab", feature = "gitea")
    ))]
    #[test]
    fn parses_discover_minimal_defaults() {
        let cli = Cli::try_parse_from(["curator", "discover", "https://example.com"]).unwrap();
        match cli.command {
            Commands::Discover {
                url,
                discover_opts,
                sync_opts,
            } => {
                assert_eq!(url, "https://example.com");
                assert_eq!(discover_opts.max_depth, 2);
                assert_eq!(discover_opts.max_pages, 1000);
                assert_eq!(discover_opts.crawl_concurrency, 10);
                assert!(!discover_opts.allow_external);
                assert!(!discover_opts.include_subdomains);
                assert!(!discover_opts.no_sitemaps);
                assert!(!sync_opts.no_star);
                assert!(!sync_opts.dry_run);
                assert!(!sync_opts.no_rate_limit);
                assert!(!sync_opts.incremental);
            }
            _ => panic!("expected discover command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_status_with_instance_default_output() {
        let cli = Cli::try_parse_from(["curator", "auth", "status", "github"]).unwrap();
        match cli.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::Status { instance, output } => {
                    assert_eq!(instance.as_deref(), Some("github"));
                    assert!(matches!(output, OutputFormat::Table));
                }
                _ => panic!("expected auth status action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_login_minimal() {
        let cli = Cli::try_parse_from(["curator", "login", "github"]).unwrap();
        match cli.command {
            Commands::Login { instance } => assert_eq!(instance, "github"),
            _ => panic!("expected login command"),
        }
    }

    #[test]
    fn parses_migrate_up_and_down() {
        let up = Cli::try_parse_from(["curator", "migrate", "up"]).unwrap();
        match up.command {
            Commands::Migrate { action } => assert!(matches!(action, MigrateAction::Up)),
            _ => panic!("expected migrate command"),
        }

        let down = Cli::try_parse_from(["curator", "migrate", "down"]).unwrap();
        match down.command {
            Commands::Migrate { action } => assert!(matches!(action, MigrateAction::Down)),
            _ => panic!("expected migrate command"),
        }
    }

    #[test]
    fn parses_completions_for_fish_and_powershell() {
        for (arg, shell) in [
            ("fish", clap_complete::Shell::Fish),
            ("powershell", clap_complete::Shell::PowerShell),
        ] {
            let cli = Cli::try_parse_from(["curator", "completions", arg]).unwrap();
            match cli.command {
                Commands::Completions { shell: parsed } => assert_eq!(parsed, shell),
                _ => panic!("expected completions command"),
            }
        }
    }

    #[test]
    fn parses_limits_json_output() {
        let cli = Cli::try_parse_from(["curator", "limits", "github", "--output", "json"]).unwrap();
        match cli.command {
            Commands::Limits { instance, output } => {
                assert_eq!(instance, "github");
                assert!(matches!(output, OutputFormat::Json));
            }
            _ => panic!("expected limits command"),
        }
    }

    #[test]
    fn parses_instance_add_with_explicit_flow_and_client_id() {
        let cli = Cli::try_parse_from([
            "curator",
            "instance",
            "add",
            "custom",
            "--platform-type",
            "gitlab",
            "--host",
            "gitlab.custom.test",
            "--oauth-client-id",
            "cid-123",
            "--oauth-flow",
            "device",
        ])
        .unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Add {
                    name,
                    platform_type,
                    host,
                    oauth_client_id,
                    oauth_flow,
                } => {
                    assert_eq!(name, "custom");
                    assert_eq!(platform_type.as_deref(), Some("gitlab"));
                    assert_eq!(host.as_deref(), Some("gitlab.custom.test"));
                    assert_eq!(oauth_client_id.as_deref(), Some("cid-123"));
                    assert!(matches!(
                        oauth_flow,
                        commands::instance::OauthFlowArg::Device
                    ));
                }
                _ => panic!("expected add action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_instance_update_without_optional_flags() {
        let cli = Cli::try_parse_from(["curator", "instance", "update", "custom"]).unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Update {
                    name,
                    oauth_client_id,
                    clear_oauth_client_id,
                    oauth_flow,
                } => {
                    assert_eq!(name, "custom");
                    assert!(oauth_client_id.is_none());
                    assert!(!clear_oauth_client_id);
                    assert!(oauth_flow.is_none());
                }
                _ => panic!("expected update action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_man_short_output_flag() {
        let cli = Cli::try_parse_from(["curator", "man", "-o", "/tmp/manpages"]).unwrap();
        match cli.command {
            Commands::Man { output } => {
                assert_eq!(
                    output.as_deref(),
                    Some(std::path::Path::new("/tmp/manpages"))
                );
            }
            _ => panic!("expected man command"),
        }
    }

    #[test]
    fn parses_completions_for_bash_and_zsh() {
        for (arg, shell) in [
            ("bash", clap_complete::Shell::Bash),
            ("zsh", clap_complete::Shell::Zsh),
        ] {
            let cli = Cli::try_parse_from(["curator", "completions", arg]).unwrap();
            match cli.command {
                Commands::Completions { shell: parsed } => assert_eq!(parsed, shell),
                _ => panic!("expected completions command"),
            }
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_stars_all_default_flags() {
        let cli = Cli::try_parse_from(["curator", "sync", "stars", "--all"]).unwrap();
        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Stars {
                    instance,
                    all,
                    sync_opts,
                } => {
                    assert!(instance.is_none());
                    assert!(all);
                    assert!(sync_opts.active_within_days.is_none());
                    assert!(!sync_opts.no_prune);
                    assert!(!sync_opts.dry_run);
                    assert!(sync_opts.concurrency.is_none());
                    assert!(!sync_opts.no_rate_limit);
                }
                _ => panic!("expected stars action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_stars_single_instance_default_flags() {
        let cli = Cli::try_parse_from(["curator", "sync", "stars", "github"]).unwrap();
        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Stars {
                    instance,
                    all,
                    sync_opts,
                } => {
                    assert_eq!(instance.as_deref(), Some("github"));
                    assert!(!all);
                    assert!(sync_opts.active_within_days.is_none());
                    assert!(!sync_opts.no_prune);
                    assert!(!sync_opts.dry_run);
                    assert!(sync_opts.concurrency.is_none());
                }
                _ => panic!("expected stars action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_stars_single_instance_with_json_like_flags() {
        let cli = Cli::try_parse_from([
            "curator",
            "sync",
            "stars",
            "github",
            "--days",
            "9",
            "--concurrency",
            "4",
            "--dry-run",
            "--no-prune",
            "--no-rate-limit",
        ])
        .unwrap();
        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Stars {
                    instance,
                    all,
                    sync_opts,
                } => {
                    assert_eq!(instance.as_deref(), Some("github"));
                    assert!(!all);
                    assert_eq!(sync_opts.active_within_days, Some(9));
                    assert_eq!(sync_opts.concurrency, Some(4));
                    assert!(sync_opts.dry_run);
                    assert!(sync_opts.no_prune);
                    assert!(sync_opts.no_rate_limit);
                }
                _ => panic!("expected stars action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn parses_instance_list_short_json_flag() {
        let cli = Cli::try_parse_from(["curator", "instance", "list", "-o", "json"]).unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::List { output } => {
                    assert!(matches!(output, OutputFormat::Json));
                }
                _ => panic!("expected list action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_instance_show_short_json_flag() {
        let cli =
            Cli::try_parse_from(["curator", "instance", "show", "github", "-o", "json"]).unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Show { name, output } => {
                    assert_eq!(name, "github");
                    assert!(matches!(output, OutputFormat::Json));
                }
                _ => panic!("expected show action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_instance_add_short_flags() {
        let cli = Cli::try_parse_from([
            "curator",
            "instance",
            "add",
            "custom",
            "-t",
            "gitea",
            "-H",
            "forgejo.example",
            "-c",
            "cid",
            "-f",
            "pkce",
        ])
        .unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Add {
                    platform_type,
                    host,
                    oauth_client_id,
                    oauth_flow,
                    ..
                } => {
                    assert_eq!(platform_type.as_deref(), Some("gitea"));
                    assert_eq!(host.as_deref(), Some("forgejo.example"));
                    assert_eq!(oauth_client_id.as_deref(), Some("cid"));
                    assert!(matches!(oauth_flow, commands::instance::OauthFlowArg::Pkce));
                }
                _ => panic!("expected add action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_instance_update_short_flags() {
        let cli = Cli::try_parse_from([
            "curator", "instance", "update", "custom", "-c", "cid", "-f", "device",
        ])
        .unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Update {
                    oauth_client_id,
                    oauth_flow,
                    ..
                } => {
                    assert_eq!(oauth_client_id.as_deref(), Some("cid"));
                    assert!(matches!(
                        oauth_flow,
                        Some(commands::instance::OauthFlowArg::Device)
                    ));
                }
                _ => panic!("expected update action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_status_short_json_flag() {
        let cli =
            Cli::try_parse_from(["curator", "auth", "status", "github", "-o", "json"]).unwrap();
        match cli.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::Status { instance, output } => {
                    assert_eq!(instance.as_deref(), Some("github"));
                    assert!(matches!(output, OutputFormat::Json));
                }
                _ => panic!("expected status action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_cleanup_legacy_short_json_flag() {
        let cli = Cli::try_parse_from(["curator", "auth", "cleanup-legacy", "-o", "json"]).unwrap();
        match cli.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::CleanupLegacy { output } => {
                    assert!(matches!(output, OutputFormat::Json));
                }
                _ => panic!("expected cleanup action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_migrate_short_json_flag() {
        let cli = Cli::try_parse_from(["curator", "auth", "migrate", "-o", "json"]).unwrap();
        match cli.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::Migrate { output } => {
                    assert!(matches!(output, OutputFormat::Json));
                }
                _ => panic!("expected migrate action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_org_short_flags() {
        let cli = Cli::try_parse_from([
            "curator",
            "sync",
            "org",
            "github",
            "rust-lang",
            "-d",
            "11",
            "-c",
            "6",
        ])
        .unwrap();
        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Org { sync_opts, .. } => {
                    assert_eq!(sync_opts.active_within_days, Some(11));
                    assert_eq!(sync_opts.concurrency, Some(6));
                }
                _ => panic!("expected org action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_user_short_flags() {
        let cli = Cli::try_parse_from([
            "curator", "sync", "user", "github", "octocat", "-d", "3", "-c", "2",
        ])
        .unwrap();
        match cli.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::User { sync_opts, .. } => {
                    assert_eq!(sync_opts.active_within_days, Some(3));
                    assert_eq!(sync_opts.concurrency, Some(2));
                }
                _ => panic!("expected user action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[cfg(all(
        feature = "discovery",
        any(feature = "github", feature = "gitlab", feature = "gitea")
    ))]
    #[test]
    fn parses_discover_short_flags() {
        let cli = Cli::try_parse_from([
            "curator",
            "discover",
            "https://example.com",
            "-D",
            "5",
            "-p",
            "9",
            "-C",
            "4",
            "-d",
            "8",
            "-c",
            "2",
        ])
        .unwrap();
        match cli.command {
            Commands::Discover {
                discover_opts,
                sync_opts,
                ..
            } => {
                assert_eq!(discover_opts.max_depth, 5);
                assert_eq!(discover_opts.max_pages, 9);
                assert_eq!(discover_opts.crawl_concurrency, 4);
                assert_eq!(sync_opts.active_within_days, Some(8));
                assert_eq!(sync_opts.concurrency, Some(2));
            }
            _ => panic!("expected discover command"),
        }
    }

    #[test]
    fn parses_instance_update_clear_client_id_and_token_flow() {
        let cli = Cli::try_parse_from([
            "curator",
            "instance",
            "update",
            "custom",
            "--clear-oauth-client-id",
            "--oauth-flow",
            "token",
        ])
        .unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Update {
                    name,
                    oauth_client_id,
                    clear_oauth_client_id,
                    oauth_flow,
                } => {
                    assert_eq!(name, "custom");
                    assert!(oauth_client_id.is_none());
                    assert!(clear_oauth_client_id);
                    assert!(matches!(
                        oauth_flow,
                        Some(commands::instance::OauthFlowArg::Token)
                    ));
                }
                _ => panic!("expected update action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[test]
    fn parses_instance_add_minimal_well_known_name() {
        let cli = Cli::try_parse_from(["curator", "instance", "add", "github"]).unwrap();
        match cli.command {
            Commands::Instance { action } => match action {
                commands::instance::InstanceAction::Add {
                    name,
                    platform_type,
                    host,
                    oauth_client_id,
                    oauth_flow,
                } => {
                    assert_eq!(name, "github");
                    assert!(platform_type.is_none());
                    assert!(host.is_none());
                    assert!(oauth_client_id.is_none());
                    assert!(matches!(oauth_flow, commands::instance::OauthFlowArg::Auto));
                }
                _ => panic!("expected add action"),
            },
            _ => panic!("expected instance command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_migrate_json_output() {
        let cli = Cli::try_parse_from(["curator", "auth", "migrate", "--output", "json"]).unwrap();
        match cli.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::Migrate { output } => {
                    assert!(matches!(output, OutputFormat::Json));
                }
                _ => panic!("expected migrate action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_auth_status_table_without_instance() {
        let cli = Cli::try_parse_from(["curator", "auth", "status"]).unwrap();
        match cli.command {
            Commands::Auth { action } => match action {
                commands::auth::AuthAction::Status { instance, output } => {
                    assert!(instance.is_none());
                    assert!(matches!(output, OutputFormat::Table));
                }
                _ => panic!("expected status action"),
            },
            _ => panic!("expected auth command"),
        }
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn parses_sync_org_and_user_multiple_flags() {
        let org = Cli::try_parse_from([
            "curator",
            "sync",
            "org",
            "github",
            "rust-lang",
            "--days",
            "15",
            "--concurrency",
            "7",
            "--dry-run",
            "--incremental",
        ])
        .unwrap();
        match org.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::Org { sync_opts, .. } => {
                    assert_eq!(sync_opts.active_within_days, Some(15));
                    assert_eq!(sync_opts.concurrency, Some(7));
                    assert!(sync_opts.dry_run);
                    assert!(sync_opts.incremental);
                }
                _ => panic!("expected org action"),
            },
            _ => panic!("expected sync command"),
        }

        let user = Cli::try_parse_from([
            "curator",
            "sync",
            "user",
            "github",
            "octocat",
            "--days",
            "5",
            "--concurrency",
            "2",
            "--dry-run",
        ])
        .unwrap();
        match user.command {
            Commands::Sync { action } => match action {
                commands::sync::SyncAction::User { sync_opts, .. } => {
                    assert_eq!(sync_opts.active_within_days, Some(5));
                    assert_eq!(sync_opts.concurrency, Some(2));
                    assert!(sync_opts.dry_run);
                }
                _ => panic!("expected user action"),
            },
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn sqlite_database_parent_handles_queryless_sqlite_url() {
        let (parent, warn_relative) = sqlite_database_parent("sqlite:///tmp/noquery.db").unwrap();
        assert_eq!(parent, PathBuf::from("/tmp"));
        assert!(!warn_relative);
    }

    #[test]
    fn ensure_sqlite_database_dir_is_noop_for_non_sqlite_url() {
        ensure_sqlite_database_dir("postgres://localhost/db").expect("non-sqlite should be noop");
    }
}
