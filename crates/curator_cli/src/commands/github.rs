use std::sync::Arc;

use console::Term;
use curator::PlatformClient;
use curator::db;
use curator::entity::code_platform::CodePlatform;
use curator::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use curator::github::GitHubClient;
use curator::rate_limits;
use curator::repository;
use curator::sync::{
    PlatformOptions, SyncOptions, sync_namespace_streaming, sync_namespaces_streaming,
    sync_starred_streaming, sync_user_streaming, sync_users_streaming,
};
use tokio::sync::mpsc;

use crate::GithubAction;
use crate::commands::limits::{RateLimitDisplay, github_rate_limits_to_display};
use crate::commands::shared::{
    MODEL_CHANNEL_BUFFER_SIZE, PersistTaskResult, await_persist_task, display_final_rate_limit,
    display_persist_errors, maybe_rate_limiter, spawn_persist_task, warn_no_rate_limit,
};
use crate::config;
use crate::progress::ProgressReporter;
use crate::shutdown::is_shutdown_requested;

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
            // CLI --no-star overrides config; if not set, use config value
            let star = if sync_opts.no_star {
                false
            } else {
                config.sync.star
            };
            // CLI --no-rate-limit overrides config
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

            let is_tty = Term::stdout().is_term();

            if sync_opts.dry_run && is_tty {
                println!("DRY RUN - no changes will be made\n");
            }

            // Check rate limit first
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

            // Create progress reporter (auto-detects TTY)
            let reporter = Arc::new(ProgressReporter::new());
            let progress = reporter.as_callback();

            // Create rate limiter for proactive rate limiting (if enabled)
            let rate_limiter = maybe_rate_limiter(no_rate_limit, rate_limits::GITHUB_DEFAULT_RPS);
            if no_rate_limit {
                warn_no_rate_limit(is_tty);
            }

            // Single org or multiple?
            if orgs.len() == 1 {
                let org = &orgs[0];

                // Set up streaming persistence (unless dry-run)
                let (tx, rx) =
                    mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);
                let persist_handle = if !options.dry_run {
                    let (handle, _counter) =
                        spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
                    Some(handle)
                } else {
                    drop(rx); // Don't need receiver in dry-run
                    None
                };

                let result = sync_namespace_streaming(
                    &client,
                    org,
                    &options,
                    rate_limiter.as_ref(),
                    Some(&*db),
                    tx,
                    Some(&*progress),
                )
                .await?;

                // Wait for persistence to complete
                let persist_result = if let Some(handle) = persist_handle {
                    await_persist_task(handle).await
                } else {
                    PersistTaskResult::default()
                };
                let saved = persist_result.saved_count;

                // Finish progress bars before printing summary
                reporter.finish();

                // Check if shutdown was requested
                let was_interrupted = is_shutdown_requested();

                if is_tty {
                    if was_interrupted {
                        println!("\n(Interrupted by user - partial results below)");
                    }
                    println!("\nSync results for '{}':", org);
                    println!("  Total repositories:    {}", result.processed);
                    println!(
                        "  Active (last {} days): {}",
                        active_within_days, result.matched
                    );

                    if options.star {
                        if options.dry_run {
                            println!("  Would star:            {}", result.starred);
                            println!("  Already starred:       {}", result.skipped);
                        } else {
                            println!("  Starred:               {}", result.starred);
                            println!("  Already starred:       {}", result.skipped);
                        }
                    }

                    if !options.dry_run {
                        println!("  Saved to database:     {}", saved);
                        if persist_result.has_errors() {
                            println!("  Failed to save:        {}", persist_result.failed_count());
                        }
                    } else {
                        println!("  Would save:            {}", result.matched);
                    }

                    // Report sync errors
                    if !result.errors.is_empty() {
                        println!("\nSync errors:");
                        for err in &result.errors {
                            println!("  - {}", err);
                        }
                    }

                    // Report persist errors
                    display_persist_errors(&persist_result, is_tty);
                } else {
                    tracing::info!(
                        org = %org,
                        processed = result.processed,
                        matched = result.matched,
                        starred = result.starred,
                        skipped = result.skipped,
                        saved = saved,
                        persist_errors = persist_result.failed_count(),
                        errors = result.errors.len(),
                        "Sync complete"
                    );
                    display_persist_errors(&persist_result, is_tty);
                }
            } else {
                // Multiple orgs - sync concurrently with streaming persistence

                // Set up streaming persistence (unless dry-run)
                let (tx, rx) =
                    mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);
                let persist_handle = if !options.dry_run {
                    let (handle, _counter) =
                        spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
                    Some(handle)
                } else {
                    drop(rx);
                    None
                };

                let org_results = sync_namespaces_streaming(
                    &client,
                    &orgs,
                    &options,
                    rate_limiter.as_ref(),
                    None, // db not passed to concurrent syncs
                    tx,
                    Some(&*progress),
                )
                .await;

                // Wait for persistence to complete
                let persist_result = if let Some(handle) = persist_handle {
                    await_persist_task(handle).await
                } else {
                    PersistTaskResult::default()
                };
                let total_saved = persist_result.saved_count;

                // Finish progress bars before printing summary
                reporter.finish();

                let mut total_processed = 0;
                let mut total_matched = 0;
                let mut total_starred = 0;
                let mut total_skipped = 0;
                let mut all_errors = Vec::new();

                for org_result in &org_results {
                    if let Some(err) = &org_result.error {
                        all_errors.push(format!("{}: {}", org_result.namespace, err));
                        continue;
                    }

                    let result = &org_result.result;
                    total_processed += result.processed;
                    total_matched += result.matched;
                    total_starred += result.starred;
                    total_skipped += result.skipped;

                    // Collect errors from this org
                    for err in &result.errors {
                        all_errors.push(format!("{}: {}", org_result.namespace, err));
                    }
                }

                // Check if shutdown was requested
                let was_interrupted = is_shutdown_requested();

                if is_tty {
                    // Summary
                    if was_interrupted {
                        println!("\n(Interrupted by user - partial results below)");
                    }
                    println!("\n=== SUMMARY ===");
                    println!("Total repositories processed: {}", total_processed);
                    println!(
                        "Total active (last {} days):  {}",
                        active_within_days, total_matched
                    );
                    if options.star {
                        if options.dry_run {
                            println!("Total would star:             {}", total_starred);
                            println!("Total already starred:        {}", total_skipped);
                        } else {
                            println!("Total starred:                {}", total_starred);
                            println!("Total already starred:        {}", total_skipped);
                        }
                    }
                    if !options.dry_run {
                        println!("Total saved to database:      {}", total_saved);
                        if persist_result.has_errors() {
                            println!(
                                "Total failed to save:         {}",
                                persist_result.failed_count()
                            );
                        }
                    } else {
                        println!("Total would save:             {}", total_matched);
                    }

                    // Report sync errors
                    if !all_errors.is_empty() {
                        println!("\nSync errors ({}):", all_errors.len());
                        for err in &all_errors {
                            println!("  - {}", err);
                        }
                    }

                    // Report persist errors
                    display_persist_errors(&persist_result, is_tty);
                } else {
                    tracing::info!(
                        orgs = orgs.len(),
                        processed = total_processed,
                        matched = total_matched,
                        starred = total_starred,
                        skipped = total_skipped,
                        saved = total_saved,
                        persist_errors = persist_result.failed_count(),
                        errors = all_errors.len(),
                        "Sync complete"
                    );
                    display_persist_errors(&persist_result, is_tty);
                }
            }

            // Final rate limit
            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
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

            let is_tty = Term::stdout().is_term();

            if options.dry_run && is_tty {
                println!("DRY RUN - no changes will be made\n");
            }

            // Check rate limit first
            let rate_limit = client.get_rate_limit().await?;
            if is_tty {
                println!(
                    "Rate limit: {}/{} remaining (resets at {})\n",
                    rate_limit.remaining, rate_limit.limit, rate_limit.reset_at
                );
            }

            // Create progress reporter
            let reporter = Arc::new(ProgressReporter::new());
            let progress = reporter.as_callback();

            // Create rate limiter (if enabled)
            let rate_limiter = maybe_rate_limiter(no_rate_limit, rate_limits::GITHUB_DEFAULT_RPS);
            if no_rate_limit {
                warn_no_rate_limit(is_tty);
            }

            if users.len() == 1 {
                // Single user - use simple path
                let user = &users[0];

                if is_tty {
                    println!("Syncing repositories for user '{}'...\n", user);
                }

                // Set up streaming persistence (unless dry-run)
                let (tx, rx) =
                    mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);
                let persist_handle = if !options.dry_run {
                    let (handle, _counter) =
                        spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
                    Some(handle)
                } else {
                    drop(rx);
                    None
                };

                let result = sync_user_streaming(
                    &client,
                    user,
                    &options,
                    rate_limiter.as_ref(),
                    Some(&*db),
                    tx,
                    Some(&*progress),
                )
                .await?;

                // Wait for persistence to complete
                let persist_result = if let Some(handle) = persist_handle {
                    await_persist_task(handle).await
                } else {
                    PersistTaskResult::default()
                };
                let saved = persist_result.saved_count;

                // Finish progress bars before printing summary
                reporter.finish();

                if is_tty {
                    println!("\nSync results for user '{}':", user);
                    println!("  Total repositories:    {}", result.processed);
                    println!(
                        "  Active (last {} days): {}",
                        active_within_days, result.matched
                    );

                    if options.star {
                        if options.dry_run {
                            println!("  Would star:            {}", result.starred);
                            println!("  Already starred:       {}", result.skipped);
                        } else {
                            println!("  Starred:               {}", result.starred);
                            println!("  Already starred:       {}", result.skipped);
                        }
                    }

                    if !options.dry_run {
                        println!("  Saved to database:     {}", saved);
                        if persist_result.has_errors() {
                            println!("  Failed to save:        {}", persist_result.failed_count());
                        }
                    } else {
                        println!("  Would save:            {}", result.matched);
                    }

                    if !result.errors.is_empty() {
                        println!("\nSync errors:");
                        for err in &result.errors {
                            println!("  - {}", err);
                        }
                    }
                    display_persist_errors(&persist_result, is_tty);
                } else {
                    tracing::info!(
                        user = %user,
                        processed = result.processed,
                        matched = result.matched,
                        starred = result.starred,
                        skipped = result.skipped,
                        saved = saved,
                        persist_errors = persist_result.failed_count(),
                        errors = result.errors.len(),
                        "User sync complete"
                    );
                    display_persist_errors(&persist_result, is_tty);
                }
            } else {
                // Multiple users - sync concurrently with streaming persistence
                if is_tty {
                    println!("Syncing repositories for {} users...\n", users.len());
                }

                // Set up streaming persistence (unless dry-run)
                let (tx, rx) =
                    mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);
                let persist_handle = if !options.dry_run {
                    let (handle, _counter) =
                        spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
                    Some(handle)
                } else {
                    drop(rx);
                    None
                };

                let user_results = sync_users_streaming(
                    &client,
                    &users,
                    &options,
                    rate_limiter.as_ref(),
                    None, // db not passed to concurrent syncs
                    tx,
                    Some(&*progress),
                )
                .await;

                // Wait for persistence to complete
                let persist_result = if let Some(handle) = persist_handle {
                    await_persist_task(handle).await
                } else {
                    PersistTaskResult::default()
                };
                let total_saved = persist_result.saved_count;

                // Finish progress bars before printing summary
                reporter.finish();

                let mut total_processed = 0;
                let mut total_matched = 0;
                let mut total_starred = 0;
                let mut total_skipped = 0;
                let mut all_errors = Vec::new();

                for user_result in &user_results {
                    if let Some(err) = &user_result.error {
                        all_errors.push(format!("{}: {}", user_result.namespace, err));
                        continue;
                    }

                    let result = &user_result.result;
                    total_processed += result.processed;
                    total_matched += result.matched;
                    total_starred += result.starred;
                    total_skipped += result.skipped;

                    for err in &result.errors {
                        all_errors.push(format!("{}: {}", user_result.namespace, err));
                    }
                }

                // Check if shutdown was requested
                let was_interrupted = is_shutdown_requested();

                if is_tty {
                    if was_interrupted {
                        println!("\n(Interrupted by user - partial results below)");
                    }
                    println!("\n=== SUMMARY ===");
                    println!("Total repositories processed: {}", total_processed);
                    println!(
                        "Total active (last {} days):  {}",
                        active_within_days, total_matched
                    );
                    if options.star {
                        if options.dry_run {
                            println!("Total would star:             {}", total_starred);
                            println!("Total already starred:        {}", total_skipped);
                        } else {
                            println!("Total starred:                {}", total_starred);
                            println!("Total already starred:        {}", total_skipped);
                        }
                    }
                    if !options.dry_run {
                        println!("Total saved to database:      {}", total_saved);
                        if persist_result.has_errors() {
                            println!(
                                "Total failed to save:         {}",
                                persist_result.failed_count()
                            );
                        }
                    } else {
                        println!("Total would save:             {}", total_matched);
                    }

                    if !all_errors.is_empty() {
                        println!("\nSync errors ({}):", all_errors.len());
                        for err in &all_errors {
                            println!("  - {}", err);
                        }
                    }
                    display_persist_errors(&persist_result, is_tty);
                } else {
                    tracing::info!(
                        users = users.len(),
                        processed = total_processed,
                        matched = total_matched,
                        starred = total_starred,
                        skipped = total_skipped,
                        saved = total_saved,
                        persist_errors = persist_result.failed_count(),
                        errors = all_errors.len(),
                        "User sync complete"
                    );
                    display_persist_errors(&persist_result, is_tty);
                }
            }

            // Final rate limit
            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
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

            let is_tty = Term::stdout().is_term();

            if options.dry_run && is_tty {
                println!("DRY RUN - no changes will be made\n");
            }

            // Check rate limit first
            let rate_limit = client.get_rate_limit().await?;
            if is_tty {
                println!(
                    "Rate limit: {}/{} remaining (resets at {})\n",
                    rate_limit.remaining, rate_limit.limit, rate_limit.reset_at
                );
                println!("Syncing starred repositories...\n");
            }

            // Create progress reporter
            let reporter = Arc::new(ProgressReporter::new());
            let progress = reporter.as_callback();

            // Create rate limiter (if enabled)
            let rate_limiter = maybe_rate_limiter(no_rate_limit, rate_limits::GITHUB_DEFAULT_RPS);
            if no_rate_limit {
                warn_no_rate_limit(is_tty);
            }

            // Set up streaming persistence (unless dry-run)
            let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);
            let persist_handle = if !options.dry_run {
                let (handle, _counter) =
                    spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
                Some(handle)
            } else {
                drop(rx);
                None
            };

            let result = sync_starred_streaming(
                &client,
                &options,
                rate_limiter.as_ref(),
                Some(&*db),
                options.concurrency,
                no_rate_limit,
                tx,
                Some(&*progress),
            )
            .await?;

            // Wait for persistence to complete
            let persist_result = if let Some(handle) = persist_handle {
                await_persist_task(handle).await
            } else {
                PersistTaskResult::default()
            };
            let saved = persist_result.saved_count;

            // Delete pruned repos from database (unless dry-run)
            let deleted = if !options.dry_run && !result.pruned_repos.is_empty() {
                repository::delete_by_owner_name(&db, CodePlatform::GitHub, &result.pruned_repos)
                    .await
                    .unwrap_or(0)
            } else {
                0
            };

            // Finish progress bars before printing summary
            reporter.finish();

            if is_tty {
                println!("\nSync results for starred repositories:");
                println!("  Total starred:         {}", result.processed);
                println!(
                    "  Active (last {} days): {}",
                    active_within_days, result.matched
                );

                if options.dry_run {
                    println!("  Would save:            {}", result.matched);
                    if prune {
                        println!("  Would prune:           {}", result.pruned);
                    }
                } else {
                    println!("  Saved to database:     {}", saved);
                    if persist_result.has_errors() {
                        println!("  Failed to save:        {}", persist_result.failed_count());
                    }
                    if prune {
                        println!("  Pruned (unstarred):    {}", result.pruned);
                        if deleted > 0 {
                            println!("  Deleted from database: {}", deleted);
                        }
                    }
                }

                if !result.errors.is_empty() {
                    println!("\nSync errors:");
                    for err in &result.errors {
                        println!("  - {}", err);
                    }
                }
                display_persist_errors(&persist_result, is_tty);
            } else {
                tracing::info!(
                    processed = result.processed,
                    matched = result.matched,
                    saved = saved,
                    persist_errors = persist_result.failed_count(),
                    pruned = result.pruned,
                    deleted = deleted,
                    errors = result.errors.len(),
                    "Starred sync complete"
                );
                display_persist_errors(&persist_result, is_tty);
            }

            // Final rate limit
            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
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
