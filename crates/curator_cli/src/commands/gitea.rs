use std::sync::Arc;

use curator::db;
use curator::entity::code_platform::CodePlatform;
use curator::gitea::{self, GiteaClient};
use curator::rate_limits;
use curator::sync::{PlatformOptions, SyncOptions};

use crate::commands::limits::RateLimitInfoMessage;
use crate::commands::shared::{SyncKind, SyncRunner};
use crate::config;
use crate::{CodebergAction, GiteaAction};

pub(crate) async fn handle_codeberg(
    action: CodebergAction,
    config: &config::Config,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        CodebergAction::Org { orgs, sync_opts } => {
            let codeberg_host = gitea::CODEBERG_HOST;
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

            let client = GiteaClient::new(codeberg_host, &codeberg_token, CodePlatform::Codeberg)?;
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
                rate_limits::GITEA_DEFAULT_RPS,
                active_within_days,
            );

            if orgs.len() == 1 {
                let result = runner.run_namespace(&client, &orgs[0]).await?;
                runner.print_single_result(&orgs[0], &result, SyncKind::Namespace);
            } else {
                let result = runner.run_namespaces(&client, &orgs).await;
                runner.print_multi_result(orgs.len(), &result, SyncKind::Namespace);
            }
        }
        CodebergAction::User { users, sync_opts } => {
            let codeberg_host = gitea::CODEBERG_HOST;
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

            let client = GiteaClient::new(codeberg_host, &codeberg_token, CodePlatform::Codeberg)?;
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
                rate_limits::GITEA_DEFAULT_RPS,
                active_within_days,
            );

            if users.len() == 1 {
                if runner.is_tty() {
                    println!("Syncing repositories for user '{}'...\n", users[0]);
                }
                let result = runner.run_user(&client, &users[0]).await?;
                runner.print_single_result(&users[0], &result, SyncKind::User);
            } else {
                if runner.is_tty() {
                    println!("Syncing repositories for {} users...\n", users.len());
                }
                let result = runner.run_users(&client, &users).await;
                runner.print_multi_result(users.len(), &result, SyncKind::User);
            }
        }
        CodebergAction::Stars { sync_opts } => {
            let codeberg_host = gitea::CODEBERG_HOST;
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

            let client = GiteaClient::new(codeberg_host, &codeberg_token, CodePlatform::Codeberg)?;
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
                rate_limits::GITEA_DEFAULT_RPS,
                active_within_days,
            );

            if runner.is_tty() {
                println!("Syncing starred repositories from Codeberg...\n");
            }

            let result = runner.run_starred(&client, CodePlatform::Codeberg).await?;
            runner.print_starred_result(&result, prune);
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

    Ok(())
}

pub(crate) async fn handle_gitea(
    action: GiteaAction,
    config: &config::Config,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        GiteaAction::Org {
            orgs,
            host,
            sync_opts,
        } => {
            let gitea_host = host.or_else(|| config.gitea_host()).expect(
                "Gitea host must be specified via --host, CURATOR_GITEA_HOST env var, or config file",
            );
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
                rate_limits::GITEA_DEFAULT_RPS,
                active_within_days,
            );

            if orgs.len() == 1 {
                let result = runner.run_namespace(&client, &orgs[0]).await?;
                runner.print_single_result(&orgs[0], &result, SyncKind::Namespace);
            } else {
                let result = runner.run_namespaces(&client, &orgs).await;
                runner.print_multi_result(orgs.len(), &result, SyncKind::Namespace);
            }
        }
        GiteaAction::User {
            users,
            host,
            sync_opts,
        } => {
            let gitea_host = host.or_else(|| config.gitea_host()).expect(
                "Gitea host must be specified via --host, CURATOR_GITEA_HOST env var, or config file",
            );
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
                rate_limits::GITEA_DEFAULT_RPS,
                active_within_days,
            );

            if users.len() == 1 {
                if runner.is_tty() {
                    println!(
                        "Syncing repositories for user '{}' on {}...\n",
                        users[0], gitea_host
                    );
                }
                let result = runner.run_user(&client, &users[0]).await?;
                runner.print_single_result(&users[0], &result, SyncKind::User);
            } else {
                if runner.is_tty() {
                    println!(
                        "Syncing repositories for {} users on {}...\n",
                        users.len(),
                        gitea_host
                    );
                }
                let result = runner.run_users(&client, &users).await;
                runner.print_multi_result(users.len(), &result, SyncKind::User);
            }
        }
        GiteaAction::Stars { host, sync_opts } => {
            let gitea_host = host.or_else(|| config.gitea_host()).expect(
                "Gitea host must be specified via --host, CURATOR_GITEA_HOST env var, or config file",
            );
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
                rate_limits::GITEA_DEFAULT_RPS,
                active_within_days,
            );

            if runner.is_tty() {
                println!("Syncing starred repositories from {}...\n", gitea_host);
            }

            let result = runner.run_starred(&client, CodePlatform::Gitea).await?;
            runner.print_starred_result(&result, prune);
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

    Ok(())
}
