//! Unified sync command for all platform instances.
//!
//! Syncs repositories from organizations, groups, users, or starred lists
//! across all platform types using a single command interface.

use std::sync::Arc;

use clap::Subcommand;
use console::Term;
#[cfg(feature = "github")]
use console::style;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};

use curator::{
    Instance, InstanceColumn, InstanceModel, PlatformType, db,
    sync::{PlatformOptions, SyncOptions, SyncStrategy},
};

use crate::CommonSyncOptions;
use crate::StarredSyncOptions;
use crate::commands::shared::{
    SyncKind, SyncRunner, build_rate_limiter, display_final_rate_limit, get_token_for_instance,
};
use crate::config::Config;

async fn run_namespace_sync_for_client<C: curator::PlatformClient + Clone + 'static>(
    runner: &SyncRunner,
    client: &C,
    names: &[String],
    is_tty: bool,
    no_rate_limit: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if names.len() == 1 {
        let result = runner.run_namespace(client, &names[0]).await?;
        runner.print_single_result(&names[0], &result, SyncKind::Namespace);
    } else {
        let result = runner.run_namespaces(client, names).await;
        runner.print_multi_result(names.len(), &result, SyncKind::Namespace);
    }

    display_final_rate_limit(client, is_tty, no_rate_limit).await;
    Ok(())
}

async fn run_user_sync_for_client<C: curator::PlatformClient + Clone + 'static>(
    runner: &SyncRunner,
    client: &C,
    names: &[String],
    is_tty: bool,
    no_rate_limit: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if names.len() == 1 {
        let result = runner.run_user(client, &names[0]).await?;
        runner.print_single_result(&names[0], &result, SyncKind::User);
    } else {
        let result = runner.run_users(client, names).await;
        runner.print_multi_result(names.len(), &result, SyncKind::User);
    }

    display_final_rate_limit(client, is_tty, no_rate_limit).await;
    Ok(())
}

async fn run_starred_sync_for_client<C: curator::PlatformClient + Clone + 'static>(
    runner: &SyncRunner,
    client: &C,
    prune: bool,
    is_tty: bool,
    no_rate_limit: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = runner.run_starred(client).await?;
    runner.print_starred_result(&result, prune);
    display_final_rate_limit(client, is_tty, no_rate_limit).await;
    Ok(())
}

/// Sync subcommands.
#[derive(Subcommand)]
pub enum SyncAction {
    /// Sync repositories from an organization or group
    ///
    /// For GitHub: syncs from an organization
    /// For GitLab: syncs from a group (supports nested paths like "my-company/team")
    /// For Gitea: syncs from an organization
    Org {
        /// Instance name (e.g., "github", "gitlab", "codeberg")
        instance: String,

        /// Organization/group name(s) - can specify multiple
        #[arg(required = true)]
        names: Vec<String>,

        /// Don't include projects from subgroups (GitLab only)
        #[arg(short = 's', long)]
        no_subgroups: bool,

        #[command(flatten)]
        sync_opts: CommonSyncOptions,
    },
    /// Sync repositories from a user
    User {
        /// Instance name (e.g., "github", "gitlab", "codeberg")
        instance: String,

        /// Username(s) - can specify multiple
        #[arg(required = true)]
        names: Vec<String>,

        #[command(flatten)]
        sync_opts: CommonSyncOptions,
    },
    /// Sync your starred repositories (and optionally prune inactive ones)
    Stars {
        /// Instance name (e.g., "github", "gitlab", "codeberg")
        instance: String,

        #[command(flatten)]
        sync_opts: StarredSyncOptions,
    },
}

/// Handle sync commands.
pub async fn handle_sync(
    action: SyncAction,
    config: &Config,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        SyncAction::Org {
            instance,
            names,
            no_subgroups,
            sync_opts,
        } => {
            sync_org(
                &instance,
                &names,
                no_subgroups,
                sync_opts,
                config,
                database_url,
            )
            .await?;
        }
        SyncAction::User {
            instance,
            names,
            sync_opts,
        } => {
            sync_user(&instance, &names, sync_opts, config, database_url).await?;
        }
        SyncAction::Stars {
            instance,
            sync_opts,
        } => {
            sync_stars(&instance, sync_opts, config, database_url).await?;
        }
    }
    Ok(())
}

/// Look up an instance by name.
async fn get_instance(
    db: &DatabaseConnection,
    name: &str,
) -> Result<InstanceModel, Box<dyn std::error::Error>> {
    Instance::find()
        .filter(InstanceColumn::Name.eq(name))
        .one(db)
        .await?
        .ok_or_else(|| {
            format!(
                "Instance '{}' not found. Add it first with: curator instance add {}",
                name, name
            )
            .into()
        })
}

fn merge_common_sync_options(
    sync_opts: &CommonSyncOptions,
    config: &Config,
) -> (u64, usize, bool, bool, SyncStrategy) {
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
    let strategy = if sync_opts.incremental {
        SyncStrategy::Incremental
    } else {
        SyncStrategy::Full
    };

    (
        active_within_days,
        concurrency,
        star,
        no_rate_limit,
        strategy,
    )
}

fn merge_starred_sync_options(
    sync_opts: &StarredSyncOptions,
    config: &Config,
) -> (u64, usize, bool, bool) {
    let active_within_days = sync_opts
        .active_within_days
        .unwrap_or(config.sync.active_within_days);
    let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
    let prune = !sync_opts.no_prune;
    let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

    (active_within_days, concurrency, prune, no_rate_limit)
}

/// Sync organizations/groups.
async fn sync_org(
    instance_name: &str,
    names: &[String],
    no_subgroups: bool,
    sync_opts: CommonSyncOptions,
    config: &Config,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_conn = db::connect_and_migrate(database_url).await?;
    let instance = get_instance(&db_conn, instance_name).await?;
    let token = get_token_for_instance(&instance, config).await?;

    let (active_within_days, concurrency, star, no_rate_limit, strategy) =
        merge_common_sync_options(&sync_opts, config);

    let db = Arc::new(db_conn);

    let options = SyncOptions {
        active_within: chrono::Duration::days(active_within_days as i64),
        star,
        dry_run: sync_opts.dry_run,
        concurrency,
        platform_options: PlatformOptions {
            include_subgroups: !no_subgroups,
        },
        prune: false,
        strategy,
    };

    let runner = SyncRunner::new(
        Arc::clone(&db),
        options.clone(),
        no_rate_limit,
        active_within_days,
    );

    // Display rate limit status (platform-dependent)
    let is_tty = Term::stdout().is_term();

    let rate_limiter = build_rate_limiter(instance.platform_type, no_rate_limit);

    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => {
            use curator::github::GitHubClient;

            let client = GitHubClient::new(&token, instance.id, rate_limiter)?;
            display_rate_limit(&client, is_tty).await;
            run_namespace_sync_for_client(&runner, &client, names, is_tty, no_rate_limit).await?;
        }
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => {
            use curator::gitlab::GitLabClient;

            let client =
                GitLabClient::new(&instance.host, &token, instance.id, rate_limiter).await?;
            run_namespace_sync_for_client(&runner, &client, names, is_tty, no_rate_limit).await?;
        }
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            use curator::gitea::GiteaClient;

            let client = GiteaClient::new(&instance.base_url(), &token, instance.id, rate_limiter)?;
            run_namespace_sync_for_client(&runner, &client, names, is_tty, no_rate_limit).await?;
        }
        #[allow(unreachable_patterns)]
        _ => {
            return Err(format!(
                "Platform type '{}' not supported for sync.",
                instance.platform_type
            )
            .into());
        }
    }

    Ok(())
}

/// Sync users.
async fn sync_user(
    instance_name: &str,
    names: &[String],
    sync_opts: CommonSyncOptions,
    config: &Config,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_conn = db::connect_and_migrate(database_url).await?;
    let instance = get_instance(&db_conn, instance_name).await?;
    let token = get_token_for_instance(&instance, config).await?;

    let (active_within_days, concurrency, star, no_rate_limit, strategy) =
        merge_common_sync_options(&sync_opts, config);

    let db = Arc::new(db_conn);

    let options = SyncOptions {
        active_within: chrono::Duration::days(active_within_days as i64),
        star,
        dry_run: sync_opts.dry_run,
        concurrency,
        platform_options: PlatformOptions::default(),
        prune: false,
        strategy,
    };

    let runner = SyncRunner::new(
        Arc::clone(&db),
        options.clone(),
        no_rate_limit,
        active_within_days,
    );

    let is_tty = Term::stdout().is_term();

    let rate_limiter = build_rate_limiter(instance.platform_type, no_rate_limit);

    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => {
            use curator::github::GitHubClient;

            let client = GitHubClient::new(&token, instance.id, rate_limiter)?;
            display_rate_limit(&client, is_tty).await;
            run_user_sync_for_client(&runner, &client, names, is_tty, no_rate_limit).await?;
        }
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => {
            use curator::gitlab::GitLabClient;

            let client =
                GitLabClient::new(&instance.host, &token, instance.id, rate_limiter).await?;
            run_user_sync_for_client(&runner, &client, names, is_tty, no_rate_limit).await?;
        }
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            use curator::gitea::GiteaClient;

            let client = GiteaClient::new(&instance.base_url(), &token, instance.id, rate_limiter)?;
            run_user_sync_for_client(&runner, &client, names, is_tty, no_rate_limit).await?;
        }
        #[allow(unreachable_patterns)]
        _ => {
            return Err(format!(
                "Platform type '{}' not supported for sync.",
                instance.platform_type
            )
            .into());
        }
    }

    Ok(())
}

/// Sync starred repositories.
async fn sync_stars(
    instance_name: &str,
    sync_opts: StarredSyncOptions,
    config: &Config,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_conn = db::connect_and_migrate(database_url).await?;
    let instance = get_instance(&db_conn, instance_name).await?;
    let token = get_token_for_instance(&instance, config).await?;

    let (active_within_days, concurrency, prune, no_rate_limit) =
        merge_starred_sync_options(&sync_opts, config);

    let db = Arc::new(db_conn);

    let options = SyncOptions {
        active_within: chrono::Duration::days(active_within_days as i64),
        star: false, // Stars sync doesn't star, it just fetches what's starred
        dry_run: sync_opts.dry_run,
        concurrency,
        platform_options: PlatformOptions::default(),
        prune,
        strategy: SyncStrategy::Full, // Starred sync always does full fetch
    };

    let runner = SyncRunner::new(
        Arc::clone(&db),
        options.clone(),
        no_rate_limit,
        active_within_days,
    );

    let is_tty = Term::stdout().is_term();

    let rate_limiter = build_rate_limiter(instance.platform_type, no_rate_limit);

    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => {
            use curator::github::GitHubClient;

            let client = GitHubClient::new(&token, instance.id, rate_limiter)?;
            display_rate_limit(&client, is_tty).await;
            run_starred_sync_for_client(&runner, &client, prune, is_tty, no_rate_limit).await?;
        }
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => {
            use curator::gitlab::GitLabClient;

            let client =
                GitLabClient::new(&instance.host, &token, instance.id, rate_limiter).await?;
            run_starred_sync_for_client(&runner, &client, prune, is_tty, no_rate_limit).await?;
        }
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            use curator::gitea::GiteaClient;

            let client = GiteaClient::new(&instance.base_url(), &token, instance.id, rate_limiter)?;
            run_starred_sync_for_client(&runner, &client, prune, is_tty, no_rate_limit).await?;
        }
        #[allow(unreachable_patterns)]
        _ => {
            return Err(format!(
                "Platform type '{}' not supported for sync.",
                instance.platform_type
            )
            .into());
        }
    }

    Ok(())
}

/// Display initial rate limit status (GitHub only currently).
#[cfg(feature = "github")]
async fn display_rate_limit<C: curator::PlatformClient>(client: &C, is_tty: bool) {
    match client.get_rate_limit().await {
        Ok(rate_limit) => {
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
        }
        Err(e) => {
            if is_tty {
                println!(
                    "{} Could not fetch rate limit: {}\n",
                    style("⚠").yellow(),
                    e
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use sea_orm::{
        ActiveModelTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, Set, Statement,
    };
    use uuid::Uuid;

    fn sample_common_sync_options() -> CommonSyncOptions {
        CommonSyncOptions {
            active_within_days: None,
            no_star: false,
            dry_run: false,
            concurrency: None,
            no_rate_limit: false,
            incremental: false,
        }
    }

    fn sample_starred_sync_options() -> StarredSyncOptions {
        StarredSyncOptions {
            active_within_days: None,
            no_prune: false,
            dry_run: false,
            concurrency: None,
            no_rate_limit: false,
        }
    }

    fn sqlite_test_url(label: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "curator-cli-sync-tests-{}-{}.db",
            label,
            Uuid::new_v4()
        ));
        format!("sqlite://{}?mode=rwc", path.display())
    }

    async fn setup_db(label: &str) -> DatabaseConnection {
        curator::db::connect_and_migrate(&sqlite_test_url(label))
            .await
            .expect("test database should initialize")
    }

    fn sample_instance(name: &str, platform_type: PlatformType, host: &str) -> InstanceModel {
        InstanceModel {
            id: Uuid::new_v4(),
            name: name.to_string(),
            platform_type,
            host: host.to_string(),
            oauth_client_id: None,
            oauth_flow: "auto".to_string(),
            created_at: Utc::now().fixed_offset(),
        }
    }

    #[test]
    fn merge_common_sync_options_uses_config_defaults() {
        let config = Config::default();
        let opts = sample_common_sync_options();

        let (days, concurrency, star, no_rate_limit, strategy) =
            merge_common_sync_options(&opts, &config);

        assert_eq!(days, config.sync.active_within_days);
        assert_eq!(concurrency, config.sync.concurrency);
        assert_eq!(star, config.sync.star);
        assert_eq!(no_rate_limit, config.sync.no_rate_limit);
        assert_eq!(strategy, SyncStrategy::Full);
    }

    #[test]
    fn merge_common_sync_options_cli_flags_override_config() {
        let config = Config::default();
        let opts = CommonSyncOptions {
            active_within_days: Some(14),
            no_star: true,
            dry_run: true,
            concurrency: Some(7),
            no_rate_limit: true,
            incremental: true,
        };

        let (days, concurrency, star, no_rate_limit, strategy) =
            merge_common_sync_options(&opts, &config);

        assert_eq!(days, 14);
        assert_eq!(concurrency, 7);
        assert!(!star);
        assert!(no_rate_limit);
        assert_eq!(strategy, SyncStrategy::Incremental);
    }

    #[test]
    fn merge_common_sync_options_no_star_has_highest_precedence() {
        let mut config = Config::default();
        config.sync.star = true;

        let opts = CommonSyncOptions {
            active_within_days: None,
            no_star: true,
            dry_run: false,
            concurrency: None,
            no_rate_limit: false,
            incremental: false,
        };

        let (_, _, star, _, _) = merge_common_sync_options(&opts, &config);
        assert!(!star);
    }

    #[test]
    fn merge_common_sync_options_respects_config_when_cli_flag_unset() {
        let mut config = Config::default();
        config.sync.star = false;
        config.sync.no_rate_limit = true;

        let opts = CommonSyncOptions {
            active_within_days: Some(21),
            no_star: false,
            dry_run: false,
            concurrency: Some(4),
            no_rate_limit: false,
            incremental: false,
        };

        let (days, concurrency, star, no_rate_limit, strategy) =
            merge_common_sync_options(&opts, &config);

        assert_eq!(days, 21);
        assert_eq!(concurrency, 4);
        assert!(!star);
        assert!(no_rate_limit);
        assert_eq!(strategy, SyncStrategy::Full);
    }

    #[test]
    fn merge_common_sync_options_partial_cli_overrides_keep_other_config_values() {
        let mut config = Config::default();
        config.sync.active_within_days = 90;
        config.sync.concurrency = 13;
        config.sync.star = false;
        config.sync.no_rate_limit = true;

        let opts = CommonSyncOptions {
            active_within_days: Some(7),
            no_star: false,
            dry_run: true,
            concurrency: None,
            no_rate_limit: false,
            incremental: false,
        };

        let (days, concurrency, star, no_rate_limit, strategy) =
            merge_common_sync_options(&opts, &config);

        assert_eq!(days, 7);
        assert_eq!(concurrency, 13);
        assert!(!star);
        assert!(no_rate_limit);
        assert_eq!(strategy, SyncStrategy::Full);
    }

    #[test]
    fn merge_starred_sync_options_combines_values() {
        let mut config = Config::default();
        config.sync.active_within_days = 45;
        config.sync.concurrency = 9;
        config.sync.no_rate_limit = true;

        let opts = StarredSyncOptions {
            active_within_days: Some(5),
            no_prune: true,
            dry_run: true,
            concurrency: Some(3),
            no_rate_limit: false,
        };

        let (days, concurrency, prune, no_rate_limit) = merge_starred_sync_options(&opts, &config);

        assert_eq!(days, 5);
        assert_eq!(concurrency, 3);
        assert!(!prune);
        assert!(no_rate_limit);
    }

    #[test]
    fn merge_starred_sync_options_uses_defaults_when_cli_unset() {
        let mut config = Config::default();
        config.sync.active_within_days = 60;
        config.sync.concurrency = 11;
        config.sync.no_rate_limit = false;

        let opts = sample_starred_sync_options();

        let (days, concurrency, prune, no_rate_limit) = merge_starred_sync_options(&opts, &config);

        assert_eq!(days, 60);
        assert_eq!(concurrency, 11);
        assert!(prune);
        assert!(!no_rate_limit);
    }

    #[test]
    fn merge_starred_sync_options_cli_no_rate_limit_overrides_config() {
        let mut config = Config::default();
        config.sync.no_rate_limit = false;

        let opts = StarredSyncOptions {
            active_within_days: None,
            no_prune: false,
            dry_run: false,
            concurrency: None,
            no_rate_limit: true,
        };

        let (_, _, prune, no_rate_limit) = merge_starred_sync_options(&opts, &config);
        assert!(prune);
        assert!(no_rate_limit);
    }

    #[tokio::test]
    async fn get_instance_returns_not_found_error_for_unknown_name() {
        let db = setup_db("missing-instance").await;

        let err = get_instance(&db, "does-not-exist")
            .await
            .expect_err("missing instance should error");

        assert!(
            err.to_string()
                .contains("Instance 'does-not-exist' not found"),
            "unexpected error: {}",
            err
        );
        assert!(
            err.to_string()
                .contains("curator instance add does-not-exist"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn get_instance_returns_inserted_record() {
        let db = setup_db("existing-instance").await;
        let expected = sample_instance("test-github", PlatformType::GitHub, "github.test");

        curator::entity::instance::ActiveModel {
            id: Set(expected.id),
            name: Set(expected.name.clone()),
            platform_type: Set(expected.platform_type),
            host: Set(expected.host.clone()),
            oauth_client_id: Set(expected.oauth_client_id.clone()),
            oauth_flow: Set(expected.oauth_flow.clone()),
            created_at: Set(expected.created_at),
        }
        .insert(&db)
        .await
        .expect("insert should succeed");

        let found = get_instance(&db, &expected.name)
            .await
            .expect("instance should be found");

        assert_eq!(found.id, expected.id);
        assert_eq!(found.name, expected.name);
        assert_eq!(found.platform_type, PlatformType::GitHub);
        assert_eq!(found.host, "github.test");
    }

    #[tokio::test]
    async fn get_instance_requires_exact_name_match() {
        let db = setup_db("exact-name-match").await;
        let stored_name = "custom-github-enterprise";
        let lookup_name = "custom-github";

        curator::entity::instance::ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set(stored_name.to_string()),
            platform_type: Set(PlatformType::GitHub),
            host: Set("github.enterprise.local".to_string()),
            oauth_client_id: Set(None),
            oauth_flow: Set("auto".to_string()),
            created_at: Set(Utc::now().fixed_offset()),
        }
        .insert(&db)
        .await
        .expect("insert should succeed");

        let err = get_instance(&db, lookup_name)
            .await
            .expect_err("partial name should not match");

        assert_eq!(
            err.to_string(),
            format!(
                "Instance '{}' not found. Add it first with: curator instance add {}",
                lookup_name, lookup_name
            )
        );
    }

    #[tokio::test]
    async fn get_instance_lookup_is_case_sensitive() {
        let db = setup_db("case-sensitive-name").await;
        let stored_name = "GitHub-Prod";
        let lookup_name = "github-prod";

        curator::entity::instance::ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set(stored_name.to_string()),
            platform_type: Set(PlatformType::GitHub),
            host: Set("github.prod.local".to_string()),
            oauth_client_id: Set(None),
            oauth_flow: Set("auto".to_string()),
            created_at: Set(Utc::now().fixed_offset()),
        }
        .insert(&db)
        .await
        .expect("insert should succeed");

        let err = get_instance(&db, lookup_name)
            .await
            .expect_err("differently cased name should not match");

        assert_eq!(
            err.to_string(),
            "Instance 'github-prod' not found. Add it first with: curator instance add github-prod"
        );
    }

    #[tokio::test]
    async fn get_instance_propagates_database_errors() {
        let db = setup_db("instance-query-failure").await;
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "DROP TABLE instances".to_string(),
        ))
        .await
        .expect("instances table should be dropped for error-path test");

        let err = get_instance(&db, "github")
            .await
            .expect_err("query failure should propagate");
        let message = err.to_string().to_ascii_lowercase();
        assert!(
            message.contains("no such table")
                || message.contains("query")
                || message.contains("connection"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn merge_starred_sync_options_config_no_rate_limit_is_preserved() {
        let mut config = Config::default();
        config.sync.no_rate_limit = true;

        let opts = StarredSyncOptions {
            active_within_days: Some(10),
            no_prune: false,
            dry_run: false,
            concurrency: Some(2),
            no_rate_limit: false,
        };

        let (days, concurrency, prune, no_rate_limit) = merge_starred_sync_options(&opts, &config);

        assert_eq!(days, 10);
        assert_eq!(concurrency, 2);
        assert!(prune);
        assert!(no_rate_limit);
    }

    #[test]
    fn sync_action_variants_constructable() {
        let common = sample_common_sync_options();
        let starred = sample_starred_sync_options();

        let org = SyncAction::Org {
            instance: "github".to_string(),
            names: vec!["rust-lang".to_string()],
            no_subgroups: false,
            sync_opts: common.clone(),
        };
        let user = SyncAction::User {
            instance: "github".to_string(),
            names: vec!["octocat".to_string()],
            sync_opts: common,
        };
        let stars = SyncAction::Stars {
            instance: "github".to_string(),
            sync_opts: starred,
        };

        assert!(matches!(org, SyncAction::Org { .. }));
        assert!(matches!(user, SyncAction::User { .. }));
        assert!(matches!(stars, SyncAction::Stars { .. }));
    }

    #[tokio::test]
    async fn handle_sync_org_returns_instance_not_found_before_auth() {
        let database_url = sqlite_test_url("handle-sync-org-missing-instance");
        let _db = curator::db::connect_and_migrate(&database_url)
            .await
            .expect("test database should initialize");

        let err = handle_sync(
            SyncAction::Org {
                instance: "missing".to_string(),
                names: vec!["org".to_string()],
                no_subgroups: false,
                sync_opts: sample_common_sync_options(),
            },
            &Config::default(),
            &database_url,
        )
        .await
        .expect_err("missing instance should fail sync setup");

        assert!(
            err.to_string().contains("Instance 'missing' not found"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn handle_sync_user_returns_instance_not_found_before_auth() {
        let database_url = sqlite_test_url("handle-sync-user-missing-instance");
        let _db = curator::db::connect_and_migrate(&database_url)
            .await
            .expect("test database should initialize");

        let err = handle_sync(
            SyncAction::User {
                instance: "missing".to_string(),
                names: vec!["octocat".to_string()],
                sync_opts: sample_common_sync_options(),
            },
            &Config::default(),
            &database_url,
        )
        .await
        .expect_err("missing instance should fail sync setup");

        assert!(
            err.to_string().contains("Instance 'missing' not found"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn handle_sync_stars_returns_instance_not_found_before_auth() {
        let database_url = sqlite_test_url("handle-sync-stars-missing-instance");
        let _db = curator::db::connect_and_migrate(&database_url)
            .await
            .expect("test database should initialize");

        let err = handle_sync(
            SyncAction::Stars {
                instance: "missing".to_string(),
                sync_opts: sample_starred_sync_options(),
            },
            &Config::default(),
            &database_url,
        )
        .await
        .expect_err("missing instance should fail sync setup");

        assert!(
            err.to_string().contains("Instance 'missing' not found"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn handle_sync_propagates_database_connection_errors() {
        let err = handle_sync(
            SyncAction::Org {
                instance: "github".to_string(),
                names: vec!["org".to_string()],
                no_subgroups: false,
                sync_opts: sample_common_sync_options(),
            },
            &Config::default(),
            "sqlite://",
        )
        .await
        .expect_err("invalid database URL should fail before instance lookup");

        assert!(
            !err.to_string().is_empty(),
            "expected a concrete database error message"
        );
    }
}
