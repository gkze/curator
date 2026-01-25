use std::sync::Arc;

use curator::db;
use curator::entity::code_platform::CodePlatform;
use curator::gitlab::GitLabClient;
use curator::rate_limits;
use curator::sync::{PlatformOptions, SyncOptions};

use crate::GitlabAction;
use crate::commands::limits::RateLimitInfoMessage;
use crate::commands::shared::{SyncKind, SyncRunner};
use crate::config;

pub(crate) async fn handle_gitlab(
    action: GitlabAction,
    config: &config::Config,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        GitlabAction::Group {
            groups,
            host,
            no_subgroups,
            sync_opts,
        } => {
            // Resolve host: CLI arg > env > config > default
            let gitlab_host = host.unwrap_or_else(|| config.gitlab_host());
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
            let include_subgroups = if no_subgroups {
                false
            } else {
                config.gitlab.include_subgroups
            };
            let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

            let client = GitLabClient::new(&gitlab_host, &gitlab_token).await?;
            let db = Arc::new(db::connect(database_url).await?);

            let options = SyncOptions {
                active_within: chrono::Duration::days(active_within_days as i64),
                star,
                dry_run: sync_opts.dry_run,
                concurrency,
                platform_options: PlatformOptions { include_subgroups },
                prune: false,
            };

            let runner = SyncRunner::new(
                db,
                options,
                no_rate_limit,
                rate_limits::GITLAB_DEFAULT_RPS,
                active_within_days,
            );

            if groups.len() == 1 {
                let result = runner.run_namespace(&client, &groups[0]).await?;
                runner.print_single_result(&groups[0], &result, SyncKind::Namespace);
            } else {
                // GitLabClient now implements Clone, so we can use concurrent sync
                let result = runner.run_namespaces(&client, &groups).await;
                runner.print_multi_result(groups.len(), &result, SyncKind::Namespace);
            }
        }
        GitlabAction::User {
            users,
            host,
            sync_opts,
        } => {
            // Resolve host: CLI arg > env > config > default
            let gitlab_host = host.unwrap_or_else(|| config.gitlab_host());
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
            let db = Arc::new(db::connect(database_url).await?);

            let options = SyncOptions {
                active_within: chrono::Duration::days(active_within_days as i64),
                star,
                dry_run: sync_opts.dry_run,
                concurrency,
                platform_options: PlatformOptions::default(),
                prune: false,
            };

            let runner = SyncRunner::new(
                db,
                options,
                no_rate_limit,
                rate_limits::GITLAB_DEFAULT_RPS,
                active_within_days,
            );

            if users.len() == 1 {
                if runner.is_tty() {
                    println!("Syncing projects for user '{}'...\n", users[0]);
                }
                let result = runner.run_user(&client, &users[0]).await?;
                runner.print_single_result(&users[0], &result, SyncKind::User);
            } else {
                if runner.is_tty() {
                    println!("Syncing projects for {} users...\n", users.len());
                }
                let result = runner.run_users(&client, &users).await;
                runner.print_multi_result(users.len(), &result, SyncKind::User);
            }
        }
        GitlabAction::Stars { host, sync_opts } => {
            // Resolve host: CLI arg > env > config > default
            let gitlab_host = host.unwrap_or_else(|| config.gitlab_host());
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
            let db = Arc::new(db::connect(database_url).await?);

            let options = SyncOptions {
                active_within: chrono::Duration::days(active_within_days as i64),
                star: false, // Not used for starred sync
                dry_run: sync_opts.dry_run,
                concurrency,
                platform_options: PlatformOptions::default(),
                prune,
            };

            let runner = SyncRunner::new(
                db,
                options,
                no_rate_limit,
                rate_limits::GITLAB_DEFAULT_RPS,
                active_within_days,
            );

            if runner.is_tty() {
                println!("Syncing starred projects...\n");
            }

            let result = runner.run_starred(&client, CodePlatform::GitLab).await?;
            runner.print_starred_result(&result, prune);
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

    Ok(())
}
