use std::sync::Arc;

use console::Term;
use curator::db;
use curator::entity::code_platform::CodePlatform;
use curator::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use curator::gitea::{self, GiteaClient};
use curator::repository;
use curator::sync::{
    PlatformOptions, SyncOptions, sync_namespace_streaming, sync_namespaces_streaming,
    sync_starred_streaming, sync_user_streaming, sync_users_streaming,
};
use curator::{ApiRateLimiter, PlatformClient, rate_limits};
use sea_orm::DatabaseConnection;
use tokio::sync::mpsc;

use crate::commands::limits::RateLimitInfoMessage;
use crate::commands::shared::{
    MODEL_CHANNEL_BUFFER_SIZE, PersistTaskResult, await_persist_task, display_persist_errors,
    maybe_rate_limiter, spawn_persist_task, warn_no_rate_limit,
};
use crate::config;
use crate::progress::ProgressReporter;
use crate::shutdown::is_shutdown_requested;
use crate::{CodebergAction, GiteaAction};

/// Run a Gitea-based sync (shared by Codeberg and Gitea subcommands).
async fn run_gitea_sync(
    client: &GiteaClient,
    orgs: &[String],
    options: &SyncOptions,
    db: Arc<DatabaseConnection>,
    active_within_days: u64,
    platform_name: &str,
    rate_limiter: Option<&ApiRateLimiter>,
) -> Result<(), Box<dyn std::error::Error>> {
    let is_tty = Term::stdout().is_term();

    if options.dry_run && is_tty {
        println!("DRY RUN - no changes will be made\n");
    }

    // Create progress reporter (auto-detects TTY)
    let reporter = Arc::new(ProgressReporter::new());
    let progress = reporter.as_callback();

    // Single org or multiple?
    if orgs.len() == 1 {
        let org = &orgs[0];

        // Set up streaming persistence (unless dry-run)
        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);
        let persist_handle = if !options.dry_run {
            let (handle, _counter) =
                spawn_persist_task(Arc::clone(&db), rx, Some(Arc::clone(&progress)));
            Some(handle)
        } else {
            drop(rx); // Don't need receiver in dry-run
            None
        };

        let result = sync_namespace_streaming(
            client,
            org,
            options,
            rate_limiter,
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
            println!("\nSync results for '{}' ({}):", org, platform_name);
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
                platform = %platform_name,
                processed = result.processed,
                matched = result.matched,
                starred = result.starred,
                skipped = result.skipped,
                saved = saved,
                errors = result.errors.len(),
                "Sync complete"
            );
        }
    } else {
        // Multiple orgs - sync concurrently with streaming persistence

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

        let org_results = sync_namespaces_streaming(
            client,
            orgs,
            options,
            rate_limiter,
            None,
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
            println!("\n=== SUMMARY ({}) ===", platform_name);
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
                platform = %platform_name,
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

    Ok(())
}

pub(crate) async fn handle_codeberg(
    action: CodebergAction,
    config: &config::Config,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        CodebergAction::Org { orgs, sync_opts } => {
            // Codeberg always uses codeberg.org
            let codeberg_host = gitea::CODEBERG_HOST;

            // Resolve token: env > config
            let codeberg_token = config.codeberg_token().expect(
                "CURATOR_CODEBERG_TOKEN must be set in environment, .env file, or config file",
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

            // Create rate limiter (if enabled)
            let rate_limiter = maybe_rate_limiter(no_rate_limit, rate_limits::GITEA_DEFAULT_RPS);
            let is_tty = Term::stdout().is_term();
            if no_rate_limit {
                warn_no_rate_limit(is_tty);
            }

            run_gitea_sync(
                &client,
                &orgs,
                &options,
                db,
                active_within_days,
                "Codeberg",
                rate_limiter.as_ref(),
            )
            .await?;
        }
        CodebergAction::User { users, sync_opts } => {
            // Codeberg always uses codeberg.org
            let codeberg_host = gitea::CODEBERG_HOST;

            // Resolve token: env > config
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

            let is_tty = Term::stdout().is_term();

            if options.dry_run && is_tty {
                println!("DRY RUN - no changes will be made\n");
            }

            // Create progress reporter
            let reporter = Arc::new(ProgressReporter::new());
            let progress = reporter.as_callback();

            // Create rate limiter (if enabled)
            let rate_limiter = maybe_rate_limiter(no_rate_limit, rate_limits::GITEA_DEFAULT_RPS);
            if no_rate_limit {
                warn_no_rate_limit(is_tty);
            }

            if users.len() == 1 {
                let user = &users[0];

                if is_tty {
                    println!("Syncing repositories for user '{}'...\n", user);
                }

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

                let persist_result = if let Some(handle) = persist_handle {
                    await_persist_task(handle).await
                } else {
                    PersistTaskResult::default()
                };
                let saved = persist_result.saved_count;

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
                    println!("Syncing repositories for {} users...\n", users.len());
                }

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
        CodebergAction::Stars { sync_opts } => {
            // Codeberg always uses codeberg.org
            let codeberg_host = gitea::CODEBERG_HOST;

            // Resolve token: env > config
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

            let is_tty = Term::stdout().is_term();

            if options.dry_run && is_tty {
                println!("DRY RUN - no changes will be made\n");
            }

            if is_tty {
                println!("Syncing starred repositories from Codeberg...\n");
            }

            // Create progress reporter
            let reporter = Arc::new(ProgressReporter::new());
            let progress = reporter.as_callback();

            // Create rate limiter (if enabled)
            let rate_limiter = maybe_rate_limiter(no_rate_limit, rate_limits::GITEA_DEFAULT_RPS);
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
                repository::delete_by_owner_name(&db, CodePlatform::Codeberg, &result.pruned_repos)
                    .await
                    .unwrap_or(0)
            } else {
                0
            };

            // Finish progress bars before printing summary
            reporter.finish();

            if is_tty {
                println!("\nSync results for starred repositories (Codeberg):");
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
                    deleted = deleted,
                    errors = result.errors.len(),
                    "Starred sync complete"
                );
                display_persist_errors(&persist_result, is_tty);
            }
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
            // Resolve host: CLI arg > env > config (required)
            let gitea_host = host.or_else(|| config.gitea_host()).expect(
                "Gitea host must be specified via --host, CURATOR_GITEA_HOST env var, or config file",
            );

            // Resolve token: env > config
            let gitea_token = config.gitea_token().expect(
                "CURATOR_GITEA_TOKEN must be set in environment, .env file, or config file",
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

            // Create rate limiter (if enabled)
            let rate_limiter = maybe_rate_limiter(no_rate_limit, rate_limits::GITEA_DEFAULT_RPS);
            let is_tty = Term::stdout().is_term();
            if no_rate_limit {
                warn_no_rate_limit(is_tty);
            }

            run_gitea_sync(
                &client,
                &orgs,
                &options,
                db,
                active_within_days,
                "Gitea",
                rate_limiter.as_ref(),
            )
            .await?;
        }
        GiteaAction::User {
            users,
            host,
            sync_opts,
        } => {
            // Resolve host: CLI arg > env > config (required)
            let gitea_host = host.or_else(|| config.gitea_host()).expect(
                "Gitea host must be specified via --host, CURATOR_GITEA_HOST env var, or config file",
            );

            // Resolve token: env > config
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

            let is_tty = Term::stdout().is_term();

            if options.dry_run && is_tty {
                println!("DRY RUN - no changes will be made\n");
            }

            // Create progress reporter
            let reporter = Arc::new(ProgressReporter::new());
            let progress = reporter.as_callback();

            // Create rate limiter (if enabled)
            let rate_limiter = maybe_rate_limiter(no_rate_limit, rate_limits::GITEA_DEFAULT_RPS);
            if no_rate_limit {
                warn_no_rate_limit(is_tty);
            }

            if users.len() == 1 {
                let user = &users[0];

                if is_tty {
                    println!(
                        "Syncing repositories for user '{}' on {}...\n",
                        user, gitea_host
                    );
                }

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

                let persist_result = if let Some(handle) = persist_handle {
                    await_persist_task(handle).await
                } else {
                    PersistTaskResult::default()
                };
                let saved = persist_result.saved_count;

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
                    }

                    // Report sync errors
                    if !result.errors.is_empty() {
                        println!("\nSync errors ({}):", result.errors.len());
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
                    println!(
                        "Syncing repositories for {} users on {}...\n",
                        users.len(),
                        gitea_host
                    );
                }

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
        GiteaAction::Stars { host, sync_opts } => {
            // Resolve host: CLI arg > env > config (required)
            let gitea_host = host.or_else(|| config.gitea_host()).expect(
                "Gitea host must be specified via --host, CURATOR_GITEA_HOST env var, or config file",
            );

            // Resolve token: env > config
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

            let is_tty = Term::stdout().is_term();

            if options.dry_run && is_tty {
                println!("DRY RUN - no changes will be made\n");
            }

            if is_tty {
                println!("Syncing starred repositories from {}...\n", gitea_host);
            }

            // Create progress reporter
            let reporter = Arc::new(ProgressReporter::new());
            let progress = reporter.as_callback();

            // Create rate limiter (if enabled)
            let rate_limiter = maybe_rate_limiter(no_rate_limit, rate_limits::GITEA_DEFAULT_RPS);
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
            // Note: Gitea platform type depends on the host
            let platform = client.platform();
            let deleted = if !options.dry_run && !result.pruned_repos.is_empty() {
                repository::delete_by_owner_name(&db, platform, &result.pruned_repos)
                    .await
                    .unwrap_or(0)
            } else {
                0
            };

            // Finish progress bars before printing summary
            reporter.finish();

            if is_tty {
                println!("\nSync results for starred repositories (Gitea):");
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
                    deleted = deleted,
                    errors = result.errors.len(),
                    "Starred sync complete"
                );
                display_persist_errors(&persist_result, is_tty);
            }
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
