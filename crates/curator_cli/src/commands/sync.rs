//! Unified sync command for all platform instances.
//!
//! Syncs repositories from organizations, groups, users, or starred lists
//! across all platform types using a single command interface.

use std::sync::Arc;

use clap::Subcommand;
use console::Term;
#[cfg(feature = "github")]
use console::style;
use sea_orm::{DatabaseConnection, EntityTrait, QueryOrder};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use curator::{
    Instance, InstanceColumn, InstanceModel, PlatformType, db,
    sync::{PlatformOptions, SyncOptions, SyncStrategy},
};

use crate::CommonSyncOptions;
use crate::StarredSyncOptions;
use crate::commands::shared::{
    SyncKind, SyncRunner, active_within_duration, build_rate_limiter, display_final_rate_limit,
    find_instance_by_name, get_token_for_instance_with_db, resolve_common_sync_options,
    resolve_starred_sync_options,
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
        #[arg(required_unless_present = "all", conflicts_with = "all")]
        instance: Option<String>,

        /// Sync starred repositories for all configured instances
        #[arg(short = 'a', long, conflicts_with = "instance")]
        all: bool,

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
            all,
            sync_opts,
        } => {
            sync_stars(instance.as_deref(), all, sync_opts, config, database_url).await?;
        }
    }
    Ok(())
}

/// Look up an instance by name.
async fn get_instance(
    db: &DatabaseConnection,
    name: &str,
) -> Result<InstanceModel, Box<dyn std::error::Error>> {
    find_instance_by_name(db, name).await
}

/// List all configured instances, sorted by name.
async fn get_instances(
    db: &DatabaseConnection,
) -> Result<Vec<InstanceModel>, Box<dyn std::error::Error>> {
    Ok(Instance::find()
        .order_by_asc(InstanceColumn::Name)
        .all(db)
        .await?)
}

fn merge_common_sync_options(
    sync_opts: &CommonSyncOptions,
    config: &Config,
) -> (u64, usize, bool, bool, SyncStrategy) {
    let resolved = resolve_common_sync_options(sync_opts, config);

    (
        resolved.active_within_days,
        resolved.concurrency,
        resolved.star,
        resolved.no_rate_limit,
        resolved.strategy,
    )
}

fn merge_starred_sync_options(
    sync_opts: &StarredSyncOptions,
    config: &Config,
) -> (u64, usize, bool, bool) {
    let resolved = resolve_starred_sync_options(sync_opts, config);

    (
        resolved.active_within_days,
        resolved.concurrency,
        resolved.prune,
        resolved.no_rate_limit,
    )
}

#[derive(Clone, Copy)]
struct ResolvedStarredSyncSettings {
    active_within_days: u64,
    concurrency: usize,
    prune: bool,
    no_rate_limit: bool,
    dry_run: bool,
}

fn resolve_starred_sync_settings(
    sync_opts: &StarredSyncOptions,
    config: &Config,
) -> ResolvedStarredSyncSettings {
    let (active_within_days, concurrency, prune, no_rate_limit) =
        merge_starred_sync_options(sync_opts, config);

    ResolvedStarredSyncSettings {
        active_within_days,
        concurrency,
        prune,
        no_rate_limit,
        dry_run: sync_opts.dry_run,
    }
}

fn build_namespace_sync_options(
    active_within_days: u64,
    concurrency: usize,
    star: bool,
    dry_run: bool,
    no_subgroups: bool,
    strategy: SyncStrategy,
) -> Result<SyncOptions, Box<dyn std::error::Error>> {
    Ok(SyncOptions {
        active_within: active_within_duration(active_within_days)?,
        star,
        dry_run,
        concurrency,
        platform_options: PlatformOptions {
            include_subgroups: !no_subgroups,
        },
        prune: false,
        strategy,
    })
}

fn build_user_sync_options(
    active_within_days: u64,
    concurrency: usize,
    star: bool,
    dry_run: bool,
    strategy: SyncStrategy,
) -> Result<SyncOptions, Box<dyn std::error::Error>> {
    Ok(SyncOptions {
        active_within: active_within_duration(active_within_days)?,
        star,
        dry_run,
        concurrency,
        platform_options: PlatformOptions::default(),
        prune: false,
        strategy,
    })
}

fn build_starred_sync_options(
    settings: ResolvedStarredSyncSettings,
) -> Result<SyncOptions, Box<dyn std::error::Error>> {
    Ok(SyncOptions {
        active_within: active_within_duration(settings.active_within_days)?,
        star: false,
        dry_run: settings.dry_run,
        concurrency: settings.concurrency,
        platform_options: PlatformOptions::default(),
        prune: settings.prune,
        strategy: SyncStrategy::Full,
    })
}

fn stars_all_parallelism(requested_concurrency: usize, instance_count: usize) -> usize {
    if instance_count == 0 {
        return 0;
    }

    requested_concurrency.max(1).min(instance_count)
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
    let token = get_token_for_instance_with_db(&instance, config, Some(&db_conn)).await?;

    let (active_within_days, concurrency, star, no_rate_limit, strategy) =
        merge_common_sync_options(&sync_opts, config);

    let db = Arc::new(db_conn);

    let options = build_namespace_sync_options(
        active_within_days,
        concurrency,
        star,
        sync_opts.dry_run,
        no_subgroups,
        strategy,
    )?;

    let runner = build_runner(
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
        #[cfg(not(feature = "github"))]
        PlatformType::GitHub => {
            return Err(crate::commands::shared::unsupported_platform_error(
                instance.platform_type,
                "sync",
            ));
        }
        #[cfg(not(feature = "gitlab"))]
        PlatformType::GitLab => {
            return Err(crate::commands::shared::unsupported_platform_error(
                instance.platform_type,
                "sync",
            ));
        }
        #[cfg(not(feature = "gitea"))]
        PlatformType::Gitea => {
            return Err(crate::commands::shared::unsupported_platform_error(
                instance.platform_type,
                "sync",
            ));
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
    let token = get_token_for_instance_with_db(&instance, config, Some(&db_conn)).await?;

    let (active_within_days, concurrency, star, no_rate_limit, strategy) =
        merge_common_sync_options(&sync_opts, config);

    let db = Arc::new(db_conn);

    let options = build_user_sync_options(
        active_within_days,
        concurrency,
        star,
        sync_opts.dry_run,
        strategy,
    )?;

    let runner = build_runner(
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
        #[cfg(not(feature = "github"))]
        PlatformType::GitHub => {
            return Err(crate::commands::shared::unsupported_platform_error(
                instance.platform_type,
                "sync",
            ));
        }
        #[cfg(not(feature = "gitlab"))]
        PlatformType::GitLab => {
            return Err(crate::commands::shared::unsupported_platform_error(
                instance.platform_type,
                "sync",
            ));
        }
        #[cfg(not(feature = "gitea"))]
        PlatformType::Gitea => {
            return Err(crate::commands::shared::unsupported_platform_error(
                instance.platform_type,
                "sync",
            ));
        }
    }

    Ok(())
}

/// Sync starred repositories.
async fn sync_stars(
    instance_name: Option<&str>,
    all_instances: bool,
    sync_opts: StarredSyncOptions,
    config: &Config,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(db::connect_and_migrate(database_url).await?);

    if all_instances {
        sync_stars_all(&db, &sync_opts, config).await
    } else {
        let instance_name = instance_name
            .ok_or_else(|| "Instance name is required unless --all is used.".to_string())?;
        let instance = get_instance(db.as_ref(), instance_name).await?;
        sync_stars_for_instance(&db, &instance, &sync_opts, config).await
    }
}

async fn sync_stars_all(
    db: &Arc<DatabaseConnection>,
    sync_opts: &StarredSyncOptions,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let instances = get_instances(db.as_ref()).await?;

    if instances.is_empty() {
        return Err(
            "No instances configured. Add one first with: curator instance add github".into(),
        );
    }

    let is_tty = Term::stdout().is_term();
    let settings = resolve_starred_sync_settings(sync_opts, config);
    let mut failures = Vec::new();
    let mut jobs: Vec<(InstanceModel, String)> = Vec::new();

    for instance in instances {
        if is_tty {
            println!(
                "\n=== Syncing starred repositories for '{}' ({}) ===",
                instance.name, instance.host
            );
        } else {
            tracing::info!(
                instance = %instance.name,
                host = %instance.host,
                "Syncing starred repositories for instance"
            );
        }

        match get_token_for_instance_with_db(&instance, config, Some(db.as_ref())).await {
            Ok(token) => jobs.push((instance, token)),
            Err(err) => {
                if is_tty {
                    eprintln!("Failed to sync '{}': {}", instance.name, err);
                } else {
                    tracing::error!(
                        instance = %instance.name,
                        error = %err,
                        "Failed to sync starred repositories for instance"
                    );
                }
                failures.push(format_instance_failure(
                    &instance.name,
                    &instance.host,
                    &err,
                ));
            }
        }
    }

    let instance_parallelism = stars_all_parallelism(settings.concurrency, jobs.len());

    if instance_parallelism > 0 {
        let semaphore = Arc::new(Semaphore::new(instance_parallelism));
        let mut join_set = JoinSet::new();

        for (instance, token) in jobs {
            let db = Arc::clone(db);
            let semaphore = Arc::clone(&semaphore);

            join_set.spawn(async move {
                let _permit = semaphore.acquire_owned().await.ok();

                let name = instance.name.clone();
                let host = instance.host.clone();
                let result = sync_stars_for_instance_with_token(&db, &instance, &token, settings)
                    .await
                    .map_err(|e| e.to_string());

                (name, host, result)
            });
        }

        while let Some(join_result) = join_set.join_next().await {
            match join_result {
                Ok((name, _host, Ok(()))) => {
                    tracing::debug!(instance = %name, "starred sync complete");
                }
                Ok((name, host, Err(err))) => {
                    if is_tty {
                        eprintln!("Failed to sync '{}': {}", name, err);
                    } else {
                        tracing::error!(
                            instance = %name,
                            error = %err,
                            "Failed to sync starred repositories for instance"
                        );
                    }
                    failures.push(format_instance_failure(&name, &host, &err));
                }
                Err(err) => {
                    let message = format!("Internal task error: {}", err);
                    if is_tty {
                        eprintln!("{}", message);
                    } else {
                        tracing::error!(error = %err, "Starred sync task failed");
                    }
                    failures.push(message);
                }
            }
        }
    }

    if failures.is_empty() {
        return Ok(());
    }

    Err(format!(
        "Failed syncing stars for {} instance(s): {}",
        failures.len(),
        failures.join("; ")
    )
    .into())
}

fn format_instance_failure(name: &str, host: &str, err: &dyn std::fmt::Display) -> String {
    format!("{} ({}) - {}", name, host, err)
}

fn build_runner(
    db: Arc<DatabaseConnection>,
    options: SyncOptions,
    no_rate_limit: bool,
    active_within_days: u64,
) -> SyncRunner {
    SyncRunner::new(db, options, no_rate_limit, active_within_days)
}

async fn sync_stars_for_instance(
    db: &Arc<DatabaseConnection>,
    instance: &InstanceModel,
    sync_opts: &StarredSyncOptions,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let token = get_token_for_instance_with_db(instance, config, Some(db.as_ref())).await?;
    let settings = resolve_starred_sync_settings(sync_opts, config);

    sync_stars_for_instance_with_token(db, instance, &token, settings).await
}

async fn sync_stars_for_instance_with_token(
    db: &Arc<DatabaseConnection>,
    instance: &InstanceModel,
    token: &str,
    settings: ResolvedStarredSyncSettings,
) -> Result<(), Box<dyn std::error::Error>> {
    let options = build_starred_sync_options(settings)?;

    let runner = build_runner(
        Arc::clone(db),
        options,
        settings.no_rate_limit,
        settings.active_within_days,
    );

    let is_tty = Term::stdout().is_term();
    let rate_limiter = build_rate_limiter(instance.platform_type, settings.no_rate_limit);

    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => {
            use curator::github::GitHubClient;

            let client = GitHubClient::new(token, instance.id, rate_limiter)?;
            display_rate_limit(&client, is_tty).await;
            run_starred_sync_for_client(
                &runner,
                &client,
                settings.prune,
                is_tty,
                settings.no_rate_limit,
            )
            .await?;
        }
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => {
            use curator::gitlab::GitLabClient;

            let client =
                GitLabClient::new(&instance.host, token, instance.id, rate_limiter).await?;
            run_starred_sync_for_client(
                &runner,
                &client,
                settings.prune,
                is_tty,
                settings.no_rate_limit,
            )
            .await?;
        }
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            use curator::gitea::GiteaClient;

            let client = GiteaClient::new(&instance.base_url(), token, instance.id, rate_limiter)?;
            run_starred_sync_for_client(
                &runner,
                &client,
                settings.prune,
                is_tty,
                settings.no_rate_limit,
            )
            .await?;
        }
        #[cfg(not(feature = "github"))]
        PlatformType::GitHub => {
            return Err(crate::commands::shared::unsupported_platform_error(
                instance.platform_type,
                "sync",
            ));
        }
        #[cfg(not(feature = "gitlab"))]
        PlatformType::GitLab => {
            return Err(crate::commands::shared::unsupported_platform_error(
                instance.platform_type,
                "sync",
            ));
        }
        #[cfg(not(feature = "gitea"))]
        PlatformType::Gitea => {
            return Err(crate::commands::shared::unsupported_platform_error(
                instance.platform_type,
                "sync",
            ));
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
    use async_trait::async_trait;
    use chrono::Utc;
    use curator::platform::{OrgInfo, PlatformError, PlatformRepo, RateLimitInfo, UserInfo};
    use sea_orm::{
        ActiveModelTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, Set, Statement,
    };
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use crate::test_support::env_lock;

    #[derive(Clone)]
    struct TestClient {
        rate_limit_error: Option<String>,
    }

    #[async_trait]
    impl curator::PlatformClient for TestClient {
        fn platform_type(&self) -> PlatformType {
            PlatformType::GitHub
        }

        fn instance_id(&self) -> Uuid {
            Uuid::nil()
        }

        async fn get_rate_limit(&self) -> Result<RateLimitInfo, PlatformError> {
            if let Some(message) = &self.rate_limit_error {
                Err(PlatformError::internal(message.clone()))
            } else {
                Ok(RateLimitInfo {
                    limit: 5000,
                    remaining: 4999,
                    reset_at: Utc::now(),
                    retry_after: None,
                })
            }
        }

        async fn get_org_info(&self, org: &str) -> Result<OrgInfo, PlatformError> {
            Ok(OrgInfo {
                name: org.to_string(),
                public_repos: 0,
                description: None,
            })
        }

        async fn get_authenticated_user(&self) -> Result<UserInfo, PlatformError> {
            Ok(UserInfo {
                username: "tester".to_string(),
                name: Some("Tester".to_string()),
                email: None,
                bio: None,
                public_repos: 0,
                followers: 0,
            })
        }

        async fn get_repo(
            &self,
            owner: &str,
            name: &str,
            _db: Option<&DatabaseConnection>,
        ) -> Result<PlatformRepo, PlatformError> {
            Ok(sample_platform_repo(owner, name))
        }

        async fn list_org_repos(
            &self,
            org: &str,
            _db: Option<&DatabaseConnection>,
            _on_progress: Option<&curator::platform::ProgressCallback>,
        ) -> Result<Vec<PlatformRepo>, PlatformError> {
            Ok(vec![sample_platform_repo(org, "repo")])
        }

        async fn list_user_repos(
            &self,
            username: &str,
            _db: Option<&DatabaseConnection>,
            _on_progress: Option<&curator::platform::ProgressCallback>,
        ) -> Result<Vec<PlatformRepo>, PlatformError> {
            Ok(vec![sample_platform_repo(username, "repo")])
        }

        async fn is_repo_starred(&self, _owner: &str, _name: &str) -> Result<bool, PlatformError> {
            Ok(false)
        }

        async fn star_repo(&self, _owner: &str, _name: &str) -> Result<bool, PlatformError> {
            Ok(true)
        }

        async fn star_repo_with_retry(
            &self,
            _owner: &str,
            _name: &str,
            _on_progress: Option<&curator::platform::ProgressCallback>,
        ) -> Result<bool, PlatformError> {
            Ok(true)
        }

        async fn unstar_repo(&self, _owner: &str, _name: &str) -> Result<bool, PlatformError> {
            Ok(true)
        }

        async fn list_starred_repos(
            &self,
            _db: Option<&DatabaseConnection>,
            _concurrency: usize,
            _skip_rate_checks: bool,
            _on_progress: Option<&curator::platform::ProgressCallback>,
        ) -> Result<Vec<PlatformRepo>, PlatformError> {
            Ok(vec![sample_platform_repo("starred", "repo")])
        }

        async fn list_starred_repos_streaming(
            &self,
            repo_tx: mpsc::Sender<PlatformRepo>,
            _db: Option<&DatabaseConnection>,
            _concurrency: usize,
            _skip_rate_checks: bool,
            _on_progress: Option<&curator::platform::ProgressCallback>,
        ) -> Result<usize, PlatformError> {
            repo_tx
                .send(sample_platform_repo("starred", "repo"))
                .await
                .map_err(|_| PlatformError::internal("channel closed"))?;
            Ok(1)
        }
    }

    fn sample_platform_repo(owner: &str, name: &str) -> PlatformRepo {
        PlatformRepo {
            platform_id: 1,
            owner: owner.to_string(),
            name: name.to_string(),
            description: None,
            default_branch: "main".to_string(),
            visibility: curator::CodeVisibility::Public,
            is_fork: false,
            is_archived: false,
            stars: None,
            forks: None,
            language: None,
            topics: vec![],
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            pushed_at: Some(Utc::now()),
            license: None,
            homepage: None,
            size_kb: None,
            metadata: serde_json::Value::Null,
        }
    }

    async fn sample_runner(label: &str, dry_run: bool) -> SyncRunner {
        let db = Arc::new(setup_db(label).await);
        SyncRunner::new(
            db,
            SyncOptions {
                active_within: active_within_duration(30).expect("duration should resolve"),
                star: true,
                dry_run,
                concurrency: 2,
                platform_options: PlatformOptions::default(),
                prune: true,
                strategy: SyncStrategy::Full,
            },
            false,
            30,
        )
    }

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

    #[test]
    fn stars_all_parallelism_clamps_to_instance_count_and_minimum_one() {
        assert_eq!(stars_all_parallelism(20, 3), 3);
        assert_eq!(stars_all_parallelism(2, 5), 2);
        assert_eq!(stars_all_parallelism(0, 4), 1);
        assert_eq!(stars_all_parallelism(10, 0), 0);
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
    async fn get_instances_returns_all_configured_instances_sorted_by_name() {
        let db = setup_db("list-instances-sorted").await;

        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "DELETE FROM instances".to_string(),
        ))
        .await
        .expect("instances table should be cleared for deterministic ordering");

        for name in ["zeta", "alpha", "beta"] {
            curator::entity::instance::ActiveModel {
                id: Set(Uuid::new_v4()),
                name: Set(name.to_string()),
                platform_type: Set(PlatformType::GitHub),
                host: Set(format!("{name}.example.com")),
                oauth_client_id: Set(None),
                oauth_flow: Set("auto".to_string()),
                created_at: Set(Utc::now().fixed_offset()),
            }
            .insert(&db)
            .await
            .expect("insert should succeed");
        }

        let names: Vec<String> = get_instances(&db)
            .await
            .expect("instances should load")
            .into_iter()
            .map(|instance| instance.name)
            .collect();

        assert_eq!(names, vec!["alpha", "beta", "zeta"]);
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
            instance: Some("github".to_string()),
            all: false,
            sync_opts: starred,
        };

        assert!(matches!(org, SyncAction::Org { .. }));
        assert!(matches!(user, SyncAction::User { .. }));
        assert!(matches!(stars, SyncAction::Stars { .. }));
    }

    #[test]
    fn resolve_starred_sync_settings_includes_dry_run() {
        let opts = StarredSyncOptions {
            active_within_days: Some(8),
            no_prune: true,
            dry_run: true,
            concurrency: Some(5),
            no_rate_limit: true,
        };

        let settings = resolve_starred_sync_settings(&opts, &Config::default());
        assert_eq!(settings.active_within_days, 8);
        assert_eq!(settings.concurrency, 5);
        assert!(!settings.prune);
        assert!(settings.no_rate_limit);
        assert!(settings.dry_run);
    }

    #[cfg(feature = "github")]
    #[tokio::test]
    async fn run_namespace_sync_for_client_handles_single_and_multiple_names() {
        let runner = sample_runner("namespace-runner", true).await;
        let client = TestClient {
            rate_limit_error: None,
        };

        run_namespace_sync_for_client(&runner, &client, &["rust-lang".to_string()], false, false)
            .await
            .expect("single namespace sync should succeed");
        run_namespace_sync_for_client(
            &runner,
            &client,
            &["rust-lang".to_string(), "tokio-rs".to_string()],
            false,
            false,
        )
        .await
        .expect("multi namespace sync should succeed");
    }

    #[cfg(feature = "github")]
    #[tokio::test]
    async fn run_user_and_starred_sync_helpers_succeed() {
        let runner = sample_runner("user-starred-runner", true).await;
        let client = TestClient {
            rate_limit_error: None,
        };

        run_user_sync_for_client(&runner, &client, &["octocat".to_string()], false, false)
            .await
            .expect("user sync should succeed");
        run_starred_sync_for_client(&runner, &client, true, false, false)
            .await
            .expect("starred sync should succeed");
    }

    #[cfg(feature = "github")]
    #[tokio::test]
    async fn display_rate_limit_handles_success_and_error() {
        let ok_client = TestClient {
            rate_limit_error: None,
        };
        display_rate_limit(&ok_client, false).await;

        let err_client = TestClient {
            rate_limit_error: Some("rate limit unavailable".to_string()),
        };
        display_rate_limit(&err_client, false).await;
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
                instance: Some("missing".to_string()),
                all: false,
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
    async fn handle_sync_stars_requires_instance_when_all_is_false() {
        let database_url = sqlite_test_url("handle-sync-stars-requires-instance");
        let _db = curator::db::connect_and_migrate(&database_url)
            .await
            .expect("test database should initialize");

        let err = handle_sync(
            SyncAction::Stars {
                instance: None,
                all: false,
                sync_opts: sample_starred_sync_options(),
            },
            &Config::default(),
            &database_url,
        )
        .await
        .expect_err("missing instance should fail when --all is not used");

        assert!(
            err.to_string()
                .contains("Instance name is required unless --all is used."),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn handle_sync_stars_all_requires_at_least_one_configured_instance() {
        let database_url = sqlite_test_url("handle-sync-stars-all-no-instances");
        let db = curator::db::connect_and_migrate(&database_url)
            .await
            .expect("test database should initialize");

        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "DELETE FROM instances".to_string(),
        ))
        .await
        .expect("instances table should be empty");

        let err = handle_sync(
            SyncAction::Stars {
                instance: None,
                all: true,
                sync_opts: sample_starred_sync_options(),
            },
            &Config::default(),
            &database_url,
        )
        .await
        .expect_err("syncing all should fail when no instances exist");

        assert!(
            err.to_string().contains("No instances configured"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn handle_sync_stars_all_reports_all_instance_failures() {
        let _guard = env_lock().lock().await;
        let original_home = std::env::var("HOME").ok();
        let temp_home = std::env::temp_dir().join(format!("curator-sync-home-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&temp_home).expect("temp home should be created");
        unsafe {
            std::env::set_var("HOME", &temp_home);
        }

        let database_url = sqlite_test_url("handle-sync-stars-all-multi-failures");
        let db = curator::db::connect_and_migrate(&database_url)
            .await
            .expect("test database should initialize");

        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "DELETE FROM instances".to_string(),
        ))
        .await
        .expect("instances table should be empty");

        for (name, platform_type, host) in [
            ("alpha-gh", PlatformType::GitHub, "alpha.example.com"),
            ("beta-gl", PlatformType::GitLab, "beta.example.com"),
        ] {
            curator::entity::instance::ActiveModel {
                id: Set(Uuid::new_v4()),
                name: Set(name.to_string()),
                platform_type: Set(platform_type),
                host: Set(host.to_string()),
                oauth_client_id: Set(None),
                oauth_flow: Set("auto".to_string()),
                created_at: Set(Utc::now().fixed_offset()),
            }
            .insert(&db)
            .await
            .expect("insert should succeed");
        }

        let result = handle_sync(
            SyncAction::Stars {
                instance: None,
                all: true,
                sync_opts: sample_starred_sync_options(),
            },
            &Config::default(),
            &database_url,
        )
        .await;

        match original_home {
            Some(home) => unsafe {
                std::env::set_var("HOME", home);
            },
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }

        let err = result.expect_err("syncing all should return aggregated instance failures");
        let message = err.to_string();

        assert!(
            message.contains("Failed syncing stars for 2 instance(s)"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("alpha-gh (alpha.example.com)"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("beta-gl (beta.example.com)"),
            "unexpected error: {message}"
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

    #[test]
    fn sample_sync_option_builders_cover_defaults() {
        let common = sample_common_sync_options();
        assert!(!common.no_star);
        assert!(!common.dry_run);
        assert!(!common.no_rate_limit);
        assert!(!common.incremental);

        let starred = sample_starred_sync_options();
        assert!(!starred.no_prune);
        assert!(!starred.dry_run);
        assert!(!starred.no_rate_limit);
    }

    #[test]
    fn parallelism_helper_handles_zero_and_large_values() {
        assert_eq!(stars_all_parallelism(0, 10), 1);
        assert_eq!(stars_all_parallelism(10, 3), 3);
        assert_eq!(stars_all_parallelism(2, 3), 2);
    }

    #[test]
    fn build_sync_options_helpers_map_fields_correctly() {
        let namespace =
            build_namespace_sync_options(30, 4, true, true, true, SyncStrategy::Incremental)
                .expect("namespace options should build");
        assert!(namespace.dry_run);
        assert_eq!(namespace.concurrency, 4);
        assert!(!namespace.platform_options.include_subgroups);
        assert_eq!(namespace.strategy, SyncStrategy::Incremental);

        let user = build_user_sync_options(14, 2, false, false, SyncStrategy::Full)
            .expect("user options should build");
        assert!(!user.star);
        assert_eq!(user.concurrency, 2);
        assert_eq!(
            user.platform_options.include_subgroups,
            PlatformOptions::default().include_subgroups
        );

        let settings = ResolvedStarredSyncSettings {
            active_within_days: 10,
            concurrency: 5,
            prune: true,
            no_rate_limit: false,
            dry_run: true,
        };
        let starred = build_starred_sync_options(settings).expect("starred options should build");
        assert!(!starred.star);
        assert!(starred.prune);
        assert!(starred.dry_run);
        assert_eq!(starred.strategy, SyncStrategy::Full);
    }

    #[test]
    fn build_sync_options_helpers_propagate_large_duration_errors() {
        let err = build_namespace_sync_options(
            (i64::MAX as u64) + 1,
            1,
            true,
            false,
            false,
            SyncStrategy::Full,
        )
        .expect_err("oversized duration should fail");
        assert!(!err.to_string().is_empty());

        let err =
            build_user_sync_options((i64::MAX as u64) + 1, 1, true, false, SyncStrategy::Full)
                .expect_err("oversized duration should fail");
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn format_instance_failure_includes_name_host_and_error() {
        let failure = format_instance_failure("github", "github.com", &"boom");
        assert_eq!(failure, "github (github.com) - boom");
    }

    #[tokio::test]
    async fn build_runner_constructs_sync_runner() {
        let db = Arc::new(setup_db("build-runner").await);
        let options = build_user_sync_options(7, 3, true, true, SyncStrategy::Full).unwrap();
        let _runner = build_runner(db, options, true, 7);
    }

    #[tokio::test]
    async fn get_instances_returns_empty_after_clearing_seeded_rows() {
        let db = setup_db("get-instances-empty").await;
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "DELETE FROM instances".to_string(),
        ))
        .await
        .unwrap();

        let instances = get_instances(&db).await.unwrap();
        assert!(instances.is_empty());
    }

    #[cfg(feature = "gitlab")]
    #[tokio::test]
    async fn sync_org_enters_gitlab_branch_before_failing_client_setup() {
        let _guard = env_lock().lock().await;
        let database_url = sqlite_test_url("sync-org-gitlab-branch");
        let db = curator::db::connect_and_migrate(&database_url)
            .await
            .unwrap();
        let instance = sample_instance("work-gitlab", PlatformType::GitLab, "bad host");
        curator::entity::instance::ActiveModel {
            id: Set(instance.id),
            name: Set(instance.name.clone()),
            platform_type: Set(instance.platform_type),
            host: Set(instance.host.clone()),
            oauth_client_id: Set(None),
            oauth_flow: Set(instance.oauth_flow.clone()),
            created_at: Set(instance.created_at),
        }
        .insert(&db)
        .await
        .unwrap();
        unsafe {
            std::env::set_var("CURATOR_INSTANCE_WORK_GITLAB_TOKEN", "token");
        }

        let err = sync_org(
            "work-gitlab",
            &["group".to_string()],
            false,
            sample_common_sync_options(),
            &Config::default(),
            &database_url,
        )
        .await
        .expect_err("gitlab sync should fail for invalid host");

        unsafe {
            std::env::remove_var("CURATOR_INSTANCE_WORK_GITLAB_TOKEN");
        }
        assert!(!err.to_string().is_empty());
    }

    #[cfg(feature = "gitea")]
    #[tokio::test]
    async fn sync_user_enters_gitea_branch_before_failing_client_setup() {
        let _guard = env_lock().lock().await;
        let database_url = sqlite_test_url("sync-user-gitea-branch");
        let db = curator::db::connect_and_migrate(&database_url)
            .await
            .unwrap();
        let instance = sample_instance("forgejo", PlatformType::Gitea, "bad host");
        curator::entity::instance::ActiveModel {
            id: Set(instance.id),
            name: Set(instance.name.clone()),
            platform_type: Set(instance.platform_type),
            host: Set(instance.host.clone()),
            oauth_client_id: Set(None),
            oauth_flow: Set(instance.oauth_flow.clone()),
            created_at: Set(instance.created_at),
        }
        .insert(&db)
        .await
        .unwrap();
        unsafe {
            std::env::set_var("CURATOR_INSTANCE_FORGEJO_TOKEN", "token");
        }

        let err = sync_user(
            "forgejo",
            &["octocat".to_string()],
            sample_common_sync_options(),
            &Config::default(),
            &database_url,
        )
        .await
        .expect_err("gitea sync should fail for invalid host");

        unsafe {
            std::env::remove_var("CURATOR_INSTANCE_FORGEJO_TOKEN");
        }
        assert!(!err.to_string().is_empty());
    }

    #[cfg(feature = "gitlab")]
    #[tokio::test]
    async fn sync_stars_for_instance_with_token_enters_gitlab_branch() {
        let db = Arc::new(setup_db("stars-gitlab-branch").await);
        let instance = sample_instance("gitlab-stars", PlatformType::GitLab, "bad host");
        let settings = ResolvedStarredSyncSettings {
            active_within_days: 10,
            concurrency: 2,
            prune: false,
            no_rate_limit: true,
            dry_run: true,
        };

        let err = sync_stars_for_instance_with_token(&db, &instance, "token", settings)
            .await
            .expect_err("gitlab stars sync should fail for invalid host");
        assert!(!err.to_string().is_empty());
    }
}
