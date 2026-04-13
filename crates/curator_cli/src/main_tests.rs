use super::*;
use uuid::Uuid;

#[test]
fn parses_migrate_status_subcommand() {
    let cli =
        Cli::try_parse_from(["curator", "migrate", "status"]).expect("migrate status should parse");

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
    let cli =
        Cli::try_parse_from(["curator", "completions", "bash"]).expect("completions should parse");

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

    let (parent, warn_relative) =
        sqlite_database_parent("sqlite://curator.db").expect("bare sqlite filename should parse");
    assert_eq!(parent, PathBuf::new());
    assert!(!warn_relative);
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
fn parses_auth_login_subcommand() {
    let cli = Cli::try_parse_from(["curator", "auth", "login", "github"])
        .expect("auth login should parse");

    match cli.command {
        Commands::Auth { action } => match action {
            commands::auth::AuthAction::Login { instance } => assert_eq!(instance, "github"),
            _ => panic!("expected auth login action"),
        },
        _ => panic!("expected auth command"),
    }
}

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[test]
fn parses_legacy_login_subcommand() {
    let cli =
        Cli::try_parse_from(["curator", "login", "github"]).expect("legacy login should parse");

    match cli.command {
        Commands::Login { instance } => assert_eq!(instance, "github"),
        _ => panic!("expected legacy login command"),
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
    let cli = Cli::try_parse_from(["curator", "instance", "show", "github", "--output", "json"])
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

#[cfg(all(
    feature = "discovery",
    any(feature = "github", feature = "gitlab", feature = "gitea")
))]
#[test]
fn rejects_zero_discover_crawl_concurrency() {
    assert!(
        Cli::try_parse_from([
            "curator",
            "discover",
            "https://example.com",
            "--crawl-concurrency",
            "0",
        ])
        .is_err()
    );
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
    let org = Cli::try_parse_from(["curator", "sync", "org", "github", "rust-lang", "--no-star"])
        .unwrap();
    match org.command {
        Commands::Sync { action } => match action {
            commands::sync::SyncAction::Org { sync_opts, .. } => assert!(sync_opts.no_star),
            _ => panic!("expected org action"),
        },
        _ => panic!("expected sync command"),
    }

    let user =
        Cli::try_parse_from(["curator", "sync", "user", "github", "octocat", "--no-star"]).unwrap();
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
        Cli::try_parse_from(["curator", "sync", "org", "github", "rust-lang", "tokio-rs"]).unwrap();
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

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
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
fn parses_auth_login_minimal() {
    let cli = Cli::try_parse_from(["curator", "auth", "login", "github"]).unwrap();
    match cli.command {
        Commands::Auth { action } => match action {
            commands::auth::AuthAction::Login { instance } => assert_eq!(instance, "github"),
            _ => panic!("expected auth login action"),
        },
        _ => panic!("expected auth command"),
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

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
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
    let cli = Cli::try_parse_from(["curator", "instance", "show", "github", "-o", "json"]).unwrap();
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
    let cli = Cli::try_parse_from(["curator", "auth", "status", "github", "-o", "json"]).unwrap();
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

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[test]
fn rejects_zero_sync_concurrency() {
    assert!(
        Cli::try_parse_from(["curator", "sync", "org", "github", "rust-lang", "-c", "0"]).is_err()
    );
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
