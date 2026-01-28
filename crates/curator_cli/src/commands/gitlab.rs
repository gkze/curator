use std::sync::Arc;

use console::Term;
use curator::db;
use curator::entity::code_platform::CodePlatform;
use curator::gitlab::GitLabClient;
use curator::gitlab::oauth;
use curator::rate_limits;
use curator::sync::{PlatformOptions, SyncOptions};

use crate::GitlabAction;
use crate::commands::limits::RateLimitInfoMessage;
use crate::commands::shared::{SyncKind, SyncRunner};
use crate::config;

/// Ensure we have a valid GitLab token, refreshing if expired.
///
/// Checks whether the stored OAuth token is expired (with a 5-minute buffer).
/// If expired and a refresh token is available, obtains new tokens and saves
/// them to the config file. Falls back to the stored token for PATs (no expiry).
///
/// Returns `None` if no token is configured at all.
async fn ensure_gitlab_token(
    config: &config::Config,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let token = match config.gitlab_token() {
        Some(t) => t,
        None => return Ok(None),
    };

    let expires_at = config.gitlab_token_expires_at();
    let refresh_token = config.gitlab_refresh_token();

    // If we have expiry info and the token is expired (or within 5 min), refresh
    if oauth::token_is_expired(expires_at, 300)
        && let Some(rt) = refresh_token
    {
        let host = config.gitlab_host();
        tracing::debug!("GitLab OAuth token expired, refreshing...");

        let new_tokens = oauth::refresh_access_token(&host, &rt).await.map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "Token refresh failed: {}. Run 'curator gitlab login' to re-authenticate.",
                e
            ))) as Box<dyn std::error::Error>
        })?;

        // Save refreshed tokens
        let exp = oauth::token_expires_at(&new_tokens);
        config::Config::save_gitlab_oauth_tokens(
            &new_tokens.access_token,
            new_tokens.refresh_token.as_deref(),
            exp,
        )?;

        tracing::debug!("GitLab OAuth token refreshed successfully");
        return Ok(Some(new_tokens.access_token));
    }
    // If expired but no refresh token — might be a PAT or stale OAuth token.
    // Return the token anyway and let the API call fail with a clear error.

    Ok(Some(token))
}

/// Get a valid GitLab token or exit with a helpful error message.
async fn require_gitlab_token(
    config: &config::Config,
) -> Result<String, Box<dyn std::error::Error>> {
    ensure_gitlab_token(config).await?.ok_or_else(|| {
        Box::new(std::io::Error::other(
            "No GitLab token configured. Run 'curator gitlab login' to authenticate, \
             or set CURATOR_GITLAB_TOKEN in environment, .env file, or config file.",
        )) as Box<dyn std::error::Error>
    })
}

pub(crate) async fn handle_gitlab(
    action: GitlabAction,
    config: &config::Config,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        GitlabAction::Login { host } => {
            let gitlab_host = host.unwrap_or_else(|| config.gitlab_host());
            let is_tty = Term::stdout().is_term();

            if is_tty {
                println!("Authenticating with GitLab ({})...\n", gitlab_host);
            }

            // Request device code
            let device_code = oauth::request_device_code_default(&gitlab_host)
                .await
                .map_err(|e| {
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

            // Try to open browser — prefer the complete URI with code pre-filled
            let open_url = device_code
                .verification_uri_complete
                .as_deref()
                .unwrap_or(&device_code.verification_uri);
            let _ = open::that(open_url);

            // Poll for token
            let token_response = oauth::poll_for_token(&gitlab_host, &device_code)
                .await
                .map_err(|e| {
                    Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error>
                })?;

            // Save tokens to config
            let expires_at = oauth::token_expires_at(&token_response);
            let config_path = config::Config::save_gitlab_oauth_tokens(
                &token_response.access_token,
                token_response.refresh_token.as_deref(),
                expires_at,
            )?;

            if is_tty {
                println!();
                println!("Success! GitLab token saved to: {}", config_path.display());
                if token_response.refresh_token.is_some() {
                    println!("Token will be automatically refreshed when it expires.");
                }
                println!();
                println!("You can now use curator commands like:");
                println!("  curator gitlab stars");
                println!("  curator gitlab group <group-path>");
            } else {
                tracing::info!(
                    config_path = %config_path.display(),
                    "GitLab authentication successful"
                );
            }
        }
        GitlabAction::Group {
            groups,
            host,
            no_subgroups,
            sync_opts,
        } => {
            // Resolve host: CLI arg > env > config > default
            let gitlab_host = host.unwrap_or_else(|| config.gitlab_host());
            let gitlab_token = require_gitlab_token(config).await?;

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
            let gitlab_token = require_gitlab_token(config).await?;

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
            let gitlab_token = require_gitlab_token(config).await?;

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
