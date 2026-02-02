//! Discovery command for crawling a URL and syncing discovered repositories.

use std::collections::HashMap;
use std::sync::Arc;

use console::Term;
use sea_orm::{DatabaseConnection, EntityTrait};
use url::Url;

use curator::discovery::{CrawlOptions, DiscoveryProgress, discover_repo_links};
use curator::{
    Instance, InstanceModel, PlatformType, db, rate_limits,
    sync::{PlatformOptions, SyncOptions, SyncStrategy},
};

use crate::CommonSyncOptions;
use crate::DiscoverOptions;
use crate::commands::shared::{
    SyncKind, SyncRunner, display_final_rate_limit, get_token_for_instance,
};
use crate::config::Config;
use crate::progress::ProgressReporter;

pub async fn handle_discover(
    url: String,
    discover_opts: DiscoverOptions,
    sync_opts: CommonSyncOptions,
    config: &Config,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let start_url = Url::parse(&url).map_err(|e| format!("Invalid URL '{}': {}", url, e))?;

    let crawl_options = CrawlOptions {
        max_depth: discover_opts.max_depth,
        max_pages: discover_opts.max_pages,
        concurrency: discover_opts.crawl_concurrency,
        same_host: !discover_opts.allow_external,
        include_subdomains: discover_opts.include_subdomains,
        use_sitemaps: !discover_opts.no_sitemaps,
        ..CrawlOptions::default()
    };

    let reporter = Arc::new(ProgressReporter::new());
    let progress_cb = {
        let reporter = Arc::clone(&reporter);
        let expected_pages = crawl_options.max_pages.min(u32::MAX as usize) as u32;
        move |event: DiscoveryProgress| match event {
            DiscoveryProgress::Started { max_pages, .. } => {
                reporter.handle(curator::sync::SyncProgress::FetchingRepos {
                    namespace: "discovery".to_string(),
                    total_repos: Some(max_pages),
                    expected_pages: Some(expected_pages),
                });
            }
            DiscoveryProgress::PageFetched { fetched } => {
                reporter.handle(curator::sync::SyncProgress::FetchedPage {
                    namespace: "discovery".to_string(),
                    page: fetched as u32,
                    count: 1,
                    total_so_far: fetched,
                    expected_pages: Some(expected_pages),
                });
            }
            DiscoveryProgress::Error { message } => {
                reporter.handle(curator::sync::SyncProgress::Warning { message });
            }
            DiscoveryProgress::Finished { pages_fetched, .. } => {
                reporter.handle(curator::sync::SyncProgress::FetchComplete {
                    namespace: "discovery".to_string(),
                    total: pages_fetched,
                });
            }
        }
    };

    let discovery = discover_repo_links(&start_url, &crawl_options, Some(&progress_cb)).await?;

    reporter.finish();

    let is_tty = Term::stdout().is_term();

    if is_tty {
        println!(
            "Discovery complete: visited {} pages (fetched {}), found {} repo links",
            discovery.pages_visited,
            discovery.pages_fetched,
            discovery.repo_links.len()
        );
    } else {
        tracing::info!(
            pages_visited = discovery.pages_visited,
            pages_fetched = discovery.pages_fetched,
            repo_links = discovery.repo_links.len(),
            "Discovery complete"
        );
    }

    if !discovery.errors.is_empty() {
        if is_tty {
            println!(
                "Discovery warnings: {} fetch errors",
                discovery.errors.len()
            );
        } else {
            tracing::warn!(errors = discovery.errors.len(), "Discovery warnings");
        }
    }

    if discovery.repo_links.is_empty() {
        if is_tty {
            println!("No repository links found.");
        } else {
            tracing::info!("No repository links found");
        }
        return Ok(());
    }

    let db_conn = Arc::new(db::connect_and_migrate(database_url).await?);
    let (instances_by_host, instances_by_id) = load_instances(db_conn.as_ref()).await?;

    if instances_by_host.is_empty() {
        println!("No instances configured. Add one with: curator instance add <name>");
        return Ok(());
    }

    let mut repos_by_instance: HashMap<uuid::Uuid, Vec<(String, String)>> = HashMap::new();
    let mut skipped_hosts: HashMap<String, usize> = HashMap::new();

    for repo in discovery.repo_links {
        let host = normalize_host(&repo.host);
        if let Some(instance_id) = instances_by_host.get(&host) {
            repos_by_instance
                .entry(*instance_id)
                .or_default()
                .push((repo.owner.clone(), repo.name.clone()));
        } else {
            *skipped_hosts.entry(host).or_insert(0) += 1;
        }
    }

    if !skipped_hosts.is_empty() {
        if is_tty {
            println!("Skipped hosts (no configured instance):");
        } else {
            tracing::warn!("Skipped hosts (no configured instance)");
        }
        let mut skipped: Vec<_> = skipped_hosts.into_iter().collect();
        skipped.sort_by(|a, b| a.0.cmp(&b.0));
        for (host, count) in skipped {
            if is_tty {
                println!("  - {} ({} repos)", host, count);
            } else {
                tracing::warn!(host = %host, repos = count, "Host skipped");
            }
        }
    }

    if repos_by_instance.is_empty() {
        if is_tty {
            println!("No repositories matched configured instances.");
        } else {
            tracing::info!("No repositories matched configured instances");
        }
        return Ok(());
    }

    for (instance_id, mut repos) in repos_by_instance {
        let Some(instance) = instances_by_id.get(&instance_id) else {
            continue;
        };

        repos.sort();
        repos.dedup();

        if is_tty {
            println!(
                "\nSyncing {} repositories for instance '{}' ({})",
                repos.len(),
                instance.name,
                instance.host
            );
        } else {
            tracing::info!(
                instance = %instance.name,
                host = %instance.host,
                repo_count = repos.len(),
                "Syncing repositories"
            );
        }

        sync_instance_repos(
            instance,
            &repos,
            sync_opts.clone(),
            config,
            Arc::clone(&db_conn),
        )
        .await?;
    }

    Ok(())
}

async fn load_instances(
    db: &DatabaseConnection,
) -> Result<
    (
        HashMap<String, uuid::Uuid>,
        HashMap<uuid::Uuid, InstanceModel>,
    ),
    Box<dyn std::error::Error>,
> {
    let instances = Instance::find().all(db).await?;

    let mut by_host = HashMap::new();
    let mut by_id = HashMap::new();

    for instance in instances {
        let host = normalize_host(&instance.host);
        by_host.insert(host, instance.id);
        by_id.insert(instance.id, instance);
    }

    Ok((by_host, by_id))
}

async fn sync_instance_repos(
    instance: &InstanceModel,
    repos: &[(String, String)],
    sync_opts: CommonSyncOptions,
    config: &Config,
    db_conn: Arc<DatabaseConnection>,
) -> Result<(), Box<dyn std::error::Error>> {
    let token = get_token_for_instance(instance, config).await?;

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
        Arc::clone(&db_conn),
        options.clone(),
        no_rate_limit,
        default_rps(instance),
        active_within_days,
    );

    let is_tty = Term::stdout().is_term();

    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => {
            use curator::github::GitHubClient;
            let client = GitHubClient::new(&token, instance.id)?;
            let result = runner.run_repo_list(&client, &instance.name, repos).await?;
            runner.print_single_result(&instance.name, &result, SyncKind::Namespace);
            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
        }
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => {
            use curator::gitlab::GitLabClient;
            let client = GitLabClient::new(&instance.host, &token, instance.id).await?;
            let result = runner.run_repo_list(&client, &instance.name, repos).await?;
            runner.print_single_result(&instance.name, &result, SyncKind::Namespace);
            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
        }
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            use curator::gitea::GiteaClient;
            let client = GiteaClient::new(&instance.base_url(), &token, instance.id)?;
            let result = runner.run_repo_list(&client, &instance.name, repos).await?;
            runner.print_single_result(&instance.name, &result, SyncKind::Namespace);
            display_final_rate_limit(&client, is_tty, no_rate_limit).await;
        }
        #[allow(unreachable_patterns)]
        _ => {
            return Err(format!(
                "Platform type '{}' not supported for discovery sync.",
                instance.platform_type
            )
            .into());
        }
    }

    Ok(())
}

fn default_rps(instance: &InstanceModel) -> u32 {
    match instance.platform_type {
        PlatformType::GitHub => rate_limits::GITHUB_DEFAULT_RPS,
        PlatformType::GitLab => rate_limits::GITLAB_DEFAULT_RPS,
        PlatformType::Gitea => rate_limits::GITEA_DEFAULT_RPS,
    }
}

fn normalize_host(host: &str) -> String {
    let host = host.trim_end_matches('.').to_lowercase();
    host.strip_prefix("www.").unwrap_or(&host).to_string()
}
