use std::sync::Arc;

use console::Term;
use curator::db;
use curator::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use curator::gitlab::GitLabClient;
use curator::rate_limits;
use curator::sync::{
    PlatformOptions, SyncOptions, sync_namespace_streaming, sync_starred_streaming,
    sync_user_streaming, sync_users_streaming,
};
use tokio::sync::mpsc;

use crate::GitlabAction;
use crate::commands::limits::RateLimitInfoMessage;
use crate::commands::shared::{
    PersistTaskResult, await_persist_task, display_persist_errors, maybe_rate_limiter,
    spawn_persist_task, warn_no_rate_limit,
};
use crate::config;
use crate::progress::ProgressReporter;
use crate::shutdown::is_shutdown_requested;

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

            // Resolve token: env > config
            let gitlab_token = config.gitlab_token().expect(
                "CURATOR_GITLAB_TOKEN must be set in environment, .env file, or config file",
            );

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
            // CLI --no-subgroups overrides config; if not set, use config value
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

            let is_tty = Term::stdout().is_term();

            if options.dry_run && is_tty {
                println!("DRY RUN - no changes will be made\n");
            }

            // Create progress reporter (auto-detects TTY)
            let reporter = Arc::new(ProgressReporter::new());
            let progress = reporter.as_callback();

            // Create rate limiter (if enabled)
            let rate_limiter = maybe_rate_limiter(no_rate_limit, rate_limits::GITLAB_DEFAULT_RPS);
            if no_rate_limit {
                warn_no_rate_limit(is_tty);
            }

            // Single group or multiple?
            if groups.len() == 1 {
                let group = &groups[0];

                // Set up streaming persistence (unless dry-run)
                let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
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
                    group,
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
                    println!("\nSync results for '{}':", group);
                    println!("  Total projects:        {}", result.processed);
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
                        group = %group,
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
                // Multiple groups - sync sequentially with streaming persistence
                // (GitLab client doesn't implement Clone, so we can't easily parallelize)

                // Set up streaming persistence (unless dry-run)
                let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
                let persist_handle = if !options.dry_run {
                    let (handle, _counter) =
                        spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
                    Some(handle)
                } else {
                    drop(rx);
                    None
                };

                let mut total_processed = 0;
                let mut total_matched = 0;
                let mut total_starred = 0;
                let mut total_skipped = 0;
                let mut all_errors: Vec<String> = Vec::new();

                for group in &groups {
                    let (group_tx, mut group_rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);

                    // Forward models to the main persistence channel
                    let tx_clone = tx.clone();
                    let forward_handle = tokio::spawn(async move {
                        while let Some(model) = group_rx.recv().await {
                            if tx_clone.send(model).await.is_err() {
                                break;
                            }
                        }
                    });

                    match sync_namespace_streaming(
                        &client,
                        group,
                        &options,
                        rate_limiter.as_ref(),
                        Some(&*db),
                        group_tx,
                        Some(&*progress),
                    )
                    .await
                    {
                        Ok(result) => {
                            total_processed += result.processed;
                            total_matched += result.matched;
                            total_starred += result.starred;
                            total_skipped += result.skipped;
                            for err in result.errors {
                                all_errors.push(format!("{}: {}", group, err));
                            }
                        }
                        Err(e) => {
                            all_errors.push(format!("{}: {}", group, e));
                        }
                    }

                    // Wait for forwarding to complete
                    let _ = forward_handle.await;

                    // Check for shutdown
                    if is_shutdown_requested() {
                        break;
                    }
                }

                // Close the main channel to signal persistence task
                drop(tx);

                // Wait for persistence to complete
                let persist_result = if let Some(handle) = persist_handle {
                    await_persist_task(handle).await
                } else {
                    PersistTaskResult::default()
                };
                let total_saved = persist_result.saved_count;

                // Finish progress bars before printing summary
                reporter.finish();

                // Check if shutdown was requested
                let was_interrupted = is_shutdown_requested();

                if is_tty {
                    // Summary
                    if was_interrupted {
                        println!("\n(Interrupted by user - partial results below)");
                    }
                    println!("\n=== SUMMARY ===");
                    println!("Total projects processed: {}", total_processed);
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
                        groups = groups.len(),
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
        }
        GitlabAction::User {
            users,
            host,
            sync_opts,
        } => {
            // Resolve host: CLI arg > env > config > default
            let gitlab_host = host.unwrap_or_else(|| config.gitlab_host());

            // Resolve token: env > config
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

            let is_tty = Term::stdout().is_term();

            if options.dry_run && is_tty {
                println!("DRY RUN - no changes will be made\n");
            }

            // Create progress reporter
            let reporter = Arc::new(ProgressReporter::new());
            let progress = reporter.as_callback();

            // Create rate limiter (if enabled)
            let rate_limiter = maybe_rate_limiter(no_rate_limit, rate_limits::GITLAB_DEFAULT_RPS);
            if no_rate_limit {
                warn_no_rate_limit(is_tty);
            }

            if users.len() == 1 {
                let user = &users[0];

                if is_tty {
                    println!("Syncing projects for user '{}'...\n", user);
                }

                // Set up streaming persistence (unless dry-run)
                let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
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
                    println!("  Total projects:        {}", result.processed);
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
                // Multiple users
                if is_tty {
                    println!("Syncing projects for {} users...\n", users.len());
                }

                let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
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

                let persist_result = if let Some(handle) = persist_handle {
                    await_persist_task(handle).await
                } else {
                    PersistTaskResult::default()
                };
                let total_saved = persist_result.saved_count;

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

                if is_tty {
                    println!("\n=== SUMMARY ===");
                    println!("Total projects processed: {}", total_processed);
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
        }
        GitlabAction::Stars { host, sync_opts } => {
            // Resolve host: CLI arg > env > config > default
            let gitlab_host = host.unwrap_or_else(|| config.gitlab_host());

            // Resolve token: env > config
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

            let is_tty = Term::stdout().is_term();

            if options.dry_run && is_tty {
                println!("DRY RUN - no changes will be made\n");
            }

            if is_tty {
                println!("Syncing starred projects...\n");
            }

            // Create progress reporter
            let reporter = Arc::new(ProgressReporter::new());
            let progress = reporter.as_callback();

            // Create rate limiter (if enabled)
            let rate_limiter = maybe_rate_limiter(no_rate_limit, rate_limits::GITLAB_DEFAULT_RPS);
            if no_rate_limit {
                warn_no_rate_limit(is_tty);
            }

            // Set up streaming persistence (unless dry-run)
            let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(100);
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

            // Finish progress bars before printing summary
            reporter.finish();

            if is_tty {
                println!("\nSync results for starred projects:");
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
                    }
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
                    processed = result.processed,
                    matched = result.matched,
                    saved = saved,
                    persist_errors = persist_result.failed_count(),
                    pruned = result.pruned,
                    errors = result.errors.len(),
                    "Starred sync complete"
                );
                display_persist_errors(&persist_result, is_tty);
            }
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
