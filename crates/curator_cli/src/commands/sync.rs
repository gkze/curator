//! Unified sync command for all platform instances.
//!
//! Syncs repositories from organizations, groups, users, or starred lists
//! across all platform types using a single command interface.

use std::sync::Arc;

use clap::Subcommand;
use console::{Term, style};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};

use curator::{
    Instance, InstanceColumn, InstanceModel, PlatformType, db, rate_limits,
    sync::{PlatformOptions, SyncOptions, SyncStrategy},
};

use crate::CommonSyncOptions;
use crate::StarredSyncOptions;
use crate::commands::shared::{
    SyncKind, SyncRunner, display_final_rate_limit, get_token_for_instance,
};
use crate::config::Config;

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

    // Create rate limiter for client construction
    let rate_limiter = if no_rate_limit {
        None
    } else {
        Some(curator::AdaptiveRateLimiter::new(
            rate_limits::default_rps_for_platform(instance.platform_type),
        ))
    };

    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => {
            use curator::github::GitHubClient;

            let client = GitHubClient::new(&token, instance.id, rate_limiter)?;
            display_rate_limit(&client, is_tty).await;

            if names.len() == 1 {
                let result = runner.run_namespace(&client, &names[0]).await?;
                runner.print_single_result(&names[0], &result, SyncKind::Namespace);
            } else {
                let result = runner.run_namespaces(&client, names).await;
                runner.print_multi_result(names.len(), &result, SyncKind::Namespace);
            }

            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
        }
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => {
            use curator::gitlab::GitLabClient;

            let client =
                GitLabClient::new(&instance.host, &token, instance.id, rate_limiter).await?;

            if names.len() == 1 {
                let result = runner.run_namespace(&client, &names[0]).await?;
                runner.print_single_result(&names[0], &result, SyncKind::Namespace);
            } else {
                let result = runner.run_namespaces(&client, names).await;
                runner.print_multi_result(names.len(), &result, SyncKind::Namespace);
            }

            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
        }
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            use curator::gitea::GiteaClient;

            let client = GiteaClient::new(&instance.base_url(), &token, instance.id, rate_limiter)?;

            if names.len() == 1 {
                let result = runner.run_namespace(&client, &names[0]).await?;
                runner.print_single_result(&names[0], &result, SyncKind::Namespace);
            } else {
                let result = runner.run_namespaces(&client, names).await;
                runner.print_multi_result(names.len(), &result, SyncKind::Namespace);
            }

            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
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

    // Create rate limiter for client construction
    let rate_limiter = if no_rate_limit {
        None
    } else {
        Some(curator::AdaptiveRateLimiter::new(
            rate_limits::default_rps_for_platform(instance.platform_type),
        ))
    };

    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => {
            use curator::github::GitHubClient;

            let client = GitHubClient::new(&token, instance.id, rate_limiter)?;
            display_rate_limit(&client, is_tty).await;

            if names.len() == 1 {
                let result = runner.run_user(&client, &names[0]).await?;
                runner.print_single_result(&names[0], &result, SyncKind::User);
            } else {
                let result = runner.run_users(&client, names).await;
                runner.print_multi_result(names.len(), &result, SyncKind::User);
            }

            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
        }
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => {
            use curator::gitlab::GitLabClient;

            let client =
                GitLabClient::new(&instance.host, &token, instance.id, rate_limiter).await?;

            if names.len() == 1 {
                let result = runner.run_user(&client, &names[0]).await?;
                runner.print_single_result(&names[0], &result, SyncKind::User);
            } else {
                let result = runner.run_users(&client, names).await;
                runner.print_multi_result(names.len(), &result, SyncKind::User);
            }

            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
        }
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            use curator::gitea::GiteaClient;

            let client = GiteaClient::new(&instance.base_url(), &token, instance.id, rate_limiter)?;

            if names.len() == 1 {
                let result = runner.run_user(&client, &names[0]).await?;
                runner.print_single_result(&names[0], &result, SyncKind::User);
            } else {
                let result = runner.run_users(&client, names).await;
                runner.print_multi_result(names.len(), &result, SyncKind::User);
            }

            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
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

    // Create rate limiter for client construction
    let rate_limiter = if no_rate_limit {
        None
    } else {
        Some(curator::AdaptiveRateLimiter::new(
            rate_limits::default_rps_for_platform(instance.platform_type),
        ))
    };

    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => {
            use curator::github::GitHubClient;

            let client = GitHubClient::new(&token, instance.id, rate_limiter)?;
            display_rate_limit(&client, is_tty).await;

            let result = runner.run_starred(&client).await?;
            runner.print_starred_result(&result, prune);

            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
        }
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => {
            use curator::gitlab::GitLabClient;

            let client =
                GitLabClient::new(&instance.host, &token, instance.id, rate_limiter).await?;

            let result = runner.run_starred(&client).await?;
            runner.print_starred_result(&result, prune);

            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
        }
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            use curator::gitea::GiteaClient;

            let client = GiteaClient::new(&instance.base_url(), &token, instance.id, rate_limiter)?;

            let result = runner.run_starred(&client).await?;
            runner.print_starred_result(&result, prune);

            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
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
