use std::sync::Arc;

use console::Term;
use curator::PlatformClient;
use curator::db;
use curator::entity::code_platform::CodePlatform;
use curator::github::GitHubClient;
use curator::rate_limits;
use curator::sync::{PlatformOptions, SyncOptions};

use crate::GithubAction;
use crate::commands::limits::{RateLimitDisplay, github_rate_limits_to_display};
use crate::commands::shared::{SyncKind, SyncRunner, display_final_rate_limit};
use crate::config;

pub(crate) async fn handle_github(
    action: GithubAction,
    config: &config::Config,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        GithubAction::Login => {
            use curator::github::oauth::{poll_for_token, request_device_code_default};

            let is_tty = Term::stdout().is_term();

            if is_tty {
                println!("Authenticating with GitHub...\n");
            }

            // Request device code
            let device_code = request_device_code_default().await.map_err(|e| {
                Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error>
            })?;

            // Try to copy code to clipboard
            let clipboard_success = match arboard::Clipboard::new() {
                Ok(mut clipboard) => clipboard.set_text(&device_code.user_code).is_ok(),
                Err(_) => false,
            };

            // Display instructions
            if is_tty {
                println!("Please visit: {}", device_code.verification_uri);
                println!();
                if clipboard_success {
                    println!("Your code: {} (copied to clipboard)", device_code.user_code);
                } else {
                    println!("Your code: {}", device_code.user_code);
                }
                println!();
                println!(
                    "Waiting for authorization (expires in {} seconds)...",
                    device_code.expires_in
                );
            } else {
                tracing::info!(
                    verification_uri = %device_code.verification_uri,
                    user_code = %device_code.user_code,
                    "Please authorize the application"
                );
            }

            // Try to open browser
            let _ = open::that(&device_code.verification_uri);

            // Poll for token
            let token_response = poll_for_token(&device_code).await.map_err(|e| {
                Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error>
            })?;

            // Save token to config
            let config_path = config::Config::save_github_token(&token_response.access_token)?;

            if is_tty {
                println!();
                println!("Success! GitHub token saved to: {}", config_path.display());
                println!();
                println!("You can now use curator commands like:");
                println!("  curator github stars");
                println!("  curator github org <org-name>");
            } else {
                tracing::info!(
                    config_path = %config_path.display(),
                    "GitHub authentication successful"
                );
            }
        }
        GithubAction::Org { orgs, sync_opts } => {
            let github_token = config.github_token().expect(
                "No GitHub token configured. Run 'curator github login' to authenticate, or set CURATOR_GITHUB_TOKEN.",
            );
            let client = GitHubClient::new(&github_token)?;

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

            let db = Arc::new(db::connect(database_url).await?);
            let options = SyncOptions {
                active_within: chrono::Duration::days(active_within_days as i64),
                star,
                dry_run: sync_opts.dry_run,
                concurrency,
                platform_options: PlatformOptions::default(),
                prune: false,
            };

            // Display rate limit status
            let is_tty = Term::stdout().is_term();
            let rate_limit = client.get_rate_limit().await?;
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

            let runner = SyncRunner::new(
                db,
                options,
                no_rate_limit,
                rate_limits::GITHUB_DEFAULT_RPS,
                active_within_days,
            );

            if orgs.len() == 1 {
                let result = runner.run_namespace(&client, &orgs[0]).await?;
                runner.print_single_result(&orgs[0], &result, SyncKind::Namespace);
            } else {
                let result = runner.run_namespaces(&client, &orgs).await;
                runner.print_multi_result(orgs.len(), &result, SyncKind::Namespace);
            }

            display_final_rate_limit(&client, runner.is_tty(), runner.no_rate_limit()).await;
        }
        GithubAction::User { users, sync_opts } => {
            let github_token = config.github_token().expect(
                "No GitHub token configured. Run 'curator github login' to authenticate, or set CURATOR_GITHUB_TOKEN.",
            );
            let client = GitHubClient::new(&github_token)?;

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

            let db = Arc::new(db::connect(database_url).await?);
            let options = SyncOptions {
                active_within: chrono::Duration::days(active_within_days as i64),
                star,
                dry_run: sync_opts.dry_run,
                concurrency,
                platform_options: PlatformOptions::default(),
                prune: false,
            };

            // Display rate limit status
            let is_tty = Term::stdout().is_term();
            let rate_limit = client.get_rate_limit().await?;
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

            let runner = SyncRunner::new(
                db,
                options,
                no_rate_limit,
                rate_limits::GITHUB_DEFAULT_RPS,
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

            display_final_rate_limit(&client, runner.is_tty(), runner.no_rate_limit()).await;
        }
        GithubAction::Stars { sync_opts } => {
            let github_token = config.github_token().expect(
                "No GitHub token configured. Run 'curator github login' to authenticate, or set CURATOR_GITHUB_TOKEN.",
            );
            let client = GitHubClient::new(&github_token)?;

            // Merge CLI args with config defaults
            let active_within_days = sync_opts
                .active_within_days
                .unwrap_or(config.sync.active_within_days);
            let concurrency = sync_opts.concurrency.unwrap_or(config.sync.concurrency);
            let prune = !sync_opts.no_prune;
            let no_rate_limit = sync_opts.no_rate_limit || config.sync.no_rate_limit;

            let db = Arc::new(db::connect(database_url).await?);
            let options = SyncOptions {
                active_within: chrono::Duration::days(active_within_days as i64),
                star: false, // Not used for starred sync
                dry_run: sync_opts.dry_run,
                concurrency,
                platform_options: PlatformOptions::default(),
                prune,
            };

            // Display rate limit status
            let is_tty = Term::stdout().is_term();
            let rate_limit = client.get_rate_limit().await?;
            if is_tty {
                println!(
                    "Rate limit: {}/{} remaining (resets at {})\n",
                    rate_limit.remaining, rate_limit.limit, rate_limit.reset_at
                );
                println!("Syncing starred repositories...\n");
            }

            let runner = SyncRunner::new(
                db,
                options.clone(),
                no_rate_limit,
                rate_limits::GITHUB_DEFAULT_RPS,
                active_within_days,
            );

            let result = runner.run_starred(&client, CodePlatform::GitHub).await?;
            runner.print_starred_result(&result, prune);

            display_final_rate_limit(&client, runner.is_tty(), runner.no_rate_limit()).await;
        }
        GithubAction::Limits { output } => {
            let github_token = config.github_token().expect(
                "No GitHub token configured. Run 'curator github login' to authenticate, or set CURATOR_GITHUB_TOKEN.",
            );
            let client = GitHubClient::new(&github_token)?;

            let rate_limits = curator::github::get_rate_limits(client.inner()).await?;
            let items = github_rate_limits_to_display(&rate_limits.resources);
            RateLimitDisplay::print_many(items, output);
        }
    }

    Ok(())
}
