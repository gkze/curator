//! Discovery command for crawling a URL and syncing discovered repositories.

use std::collections::HashMap;
use std::sync::Arc;

use console::Term;
use sea_orm::{DatabaseConnection, EntityTrait};
use url::Url;

use curator::discovery::{CrawlOptions, DiscoveryProgress, RepoLink, discover_repo_links};
use curator::{
    Instance, InstanceModel, PlatformType, db,
    sync::{PlatformOptions, SyncOptions, SyncStrategy},
};

use crate::CommonSyncOptions;
use crate::DiscoverOptions;
use crate::commands::shared::{
    SyncKind, SyncRunner, build_rate_limiter, display_final_rate_limit, get_token_for_instance,
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

    let (repos_by_instance, skipped_hosts) =
        map_repos_to_instances(&discovery.repo_links, &instances_by_host);

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

    for (instance_id, repos) in repos_by_instance {
        let Some(instance) = instances_by_id.get(&instance_id) else {
            continue;
        };

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
        active_within_days,
    );

    let is_tty = Term::stdout().is_term();

    let rate_limiter = build_rate_limiter(instance.platform_type, no_rate_limit);

    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => {
            use curator::github::GitHubClient;
            let client = GitHubClient::new(&token, instance.id, rate_limiter)?;
            run_discovery_sync_for_client(&runner, &client, instance, repos, is_tty, no_rate_limit)
                .await?;
        }
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => {
            use curator::gitlab::GitLabClient;
            let client =
                GitLabClient::new(&instance.host, &token, instance.id, rate_limiter).await?;
            run_discovery_sync_for_client(&runner, &client, instance, repos, is_tty, no_rate_limit)
                .await?;
        }
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            use curator::gitea::GiteaClient;
            let client = GiteaClient::new(&instance.base_url(), &token, instance.id, rate_limiter)?;
            run_discovery_sync_for_client(&runner, &client, instance, repos, is_tty, no_rate_limit)
                .await?;
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

async fn run_discovery_sync_for_client<C: curator::PlatformClient + Clone + 'static>(
    runner: &SyncRunner,
    client: &C,
    instance: &InstanceModel,
    repos: &[(String, String)],
    is_tty: bool,
    no_rate_limit: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = runner.run_repo_list(client, &instance.name, repos).await?;
    runner.print_single_result(&instance.name, &result, SyncKind::Namespace);
    display_final_rate_limit(client, is_tty, no_rate_limit).await;
    Ok(())
}

fn normalize_host(host: &str) -> String {
    let host = host.trim().trim_end_matches('.').to_lowercase();
    host.strip_prefix("www.").unwrap_or(&host).to_string()
}

/// Repos grouped by instance ID, each as `(owner, repo_name)` pairs.
type ReposByInstance = HashMap<uuid::Uuid, Vec<(String, String)>>;

/// Hosts that couldn't be matched to an instance, with occurrence counts.
type SkippedHosts = HashMap<String, usize>;

fn map_repos_to_instances(
    repo_links: &[RepoLink],
    instances_by_host: &HashMap<String, uuid::Uuid>,
) -> (ReposByInstance, SkippedHosts) {
    let mut repos_by_instance: HashMap<uuid::Uuid, Vec<(String, String)>> = HashMap::new();
    let mut skipped_hosts: HashMap<String, usize> = HashMap::new();

    for repo in repo_links {
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

    for repos in repos_by_instance.values_mut() {
        repos.sort();
        repos.dedup();
    }

    (repos_by_instance, skipped_hosts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use sea_orm::{ActiveModelTrait, ConnectionTrait, DatabaseBackend, Set, Statement};

    fn sqlite_test_url(label: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "curator-cli-discover-tests-{}-{}.db",
            label,
            uuid::Uuid::new_v4()
        ));
        format!("sqlite://{}?mode=rwc", path.display())
    }

    async fn setup_db(label: &str) -> DatabaseConnection {
        curator::db::connect_and_migrate(&sqlite_test_url(label))
            .await
            .expect("test database should initialize")
    }

    fn repo_link(host: &str, owner: &str, name: &str) -> RepoLink {
        RepoLink {
            host: host.to_string(),
            owner: owner.to_string(),
            name: name.to_string(),
            original_url: format!("https://{host}/{owner}/{name}"),
            canonical_url: format!("https://{host}/{owner}/{name}"),
        }
    }

    #[test]
    fn map_repos_to_instances_maps_hosts_and_counts_skipped() {
        let github_id = uuid::Uuid::new_v4();
        let gitlab_id = uuid::Uuid::new_v4();

        let instances_by_host = HashMap::from([
            ("github.com".to_string(), github_id),
            ("gitlab.com".to_string(), gitlab_id),
        ]);

        let repos = vec![
            repo_link("www.GitHub.com", "rust-lang", "rust"),
            repo_link("GITLAB.COM.", "group", "project"),
            repo_link("unknown.example", "x", "y"),
            repo_link("UNKNOWN.EXAMPLE.", "x", "z"),
        ];

        let (mapped, skipped) = map_repos_to_instances(&repos, &instances_by_host);

        assert_eq!(
            mapped.get(&github_id),
            Some(&vec![("rust-lang".to_string(), "rust".to_string())])
        );
        assert_eq!(
            mapped.get(&gitlab_id),
            Some(&vec![("group".to_string(), "project".to_string())])
        );
        assert_eq!(skipped.get("unknown.example"), Some(&2));
    }

    #[test]
    fn map_repos_to_instances_dedupes_per_instance() {
        let github_id = uuid::Uuid::new_v4();
        let instances_by_host = HashMap::from([("github.com".to_string(), github_id)]);

        let repos = vec![
            repo_link("github.com", "owner", "repo"),
            repo_link("www.github.com", "owner", "repo"),
            repo_link("github.com", "owner", "another"),
            repo_link("github.com", "owner", "another"),
        ];

        let (mapped, skipped) = map_repos_to_instances(&repos, &instances_by_host);

        assert!(skipped.is_empty());
        assert_eq!(
            mapped.get(&github_id),
            Some(&vec![
                ("owner".to_string(), "another".to_string()),
                ("owner".to_string(), "repo".to_string()),
            ])
        );
    }

    #[test]
    fn map_repos_to_instances_handles_empty_input() {
        let instances_by_host = HashMap::from([("github.com".to_string(), uuid::Uuid::new_v4())]);

        let (mapped, skipped) = map_repos_to_instances(&[], &instances_by_host);

        assert!(mapped.is_empty());
        assert!(skipped.is_empty());
    }

    #[test]
    fn normalize_host_trims_whitespace_and_trailing_dots() {
        assert_eq!(normalize_host("  WWW.GitHub.com...  "), "github.com");
        assert_eq!(
            normalize_host(" www.gitlab.example.com "),
            "gitlab.example.com"
        );
    }

    #[test]
    fn map_repos_to_instances_uses_trimmed_host_for_matching() {
        let github_id = uuid::Uuid::new_v4();
        let instances_by_host = HashMap::from([("github.com".to_string(), github_id)]);

        let repos = vec![repo_link("  www.GitHub.com.  ", "owner", "repo")];

        let (mapped, skipped) = map_repos_to_instances(&repos, &instances_by_host);

        assert!(skipped.is_empty());
        assert_eq!(
            mapped.get(&github_id),
            Some(&vec![("owner".to_string(), "repo".to_string())])
        );
    }

    #[test]
    fn map_repos_to_instances_aggregates_unknown_hosts_by_normalized_key() {
        let repos = vec![
            repo_link("  WWW.Unknown.Example.  ", "o1", "r1"),
            repo_link("unknown.example", "o2", "r2"),
            repo_link("UNKNOWN.EXAMPLE...", "o3", "r3"),
        ];

        let (mapped, skipped) = map_repos_to_instances(&repos, &HashMap::new());

        assert!(mapped.is_empty());
        assert_eq!(skipped.get("unknown.example"), Some(&3));
    }

    #[test]
    fn map_repos_to_instances_dedupes_per_instance_not_globally() {
        let github_id = uuid::Uuid::new_v4();
        let gitlab_id = uuid::Uuid::new_v4();
        let instances_by_host = HashMap::from([
            ("github.com".to_string(), github_id),
            ("gitlab.com".to_string(), gitlab_id),
        ]);

        let repos = vec![
            repo_link("github.com", "owner", "repo"),
            repo_link("github.com", "owner", "repo"),
            repo_link("gitlab.com", "owner", "repo"),
            repo_link("gitlab.com", "owner", "repo"),
        ];

        let (mapped, skipped) = map_repos_to_instances(&repos, &instances_by_host);

        assert!(skipped.is_empty());
        assert_eq!(mapped.get(&github_id).map(Vec::len), Some(1));
        assert_eq!(mapped.get(&gitlab_id).map(Vec::len), Some(1));
        assert_eq!(
            mapped.get(&github_id),
            Some(&vec![("owner".to_string(), "repo".to_string())])
        );
        assert_eq!(
            mapped.get(&gitlab_id),
            Some(&vec![("owner".to_string(), "repo".to_string())])
        );
    }

    #[test]
    fn normalize_host_only_strips_single_www_prefix() {
        assert_eq!(normalize_host("www.www.github.com"), "www.github.com");
    }

    #[test]
    fn normalize_host_leaves_non_www_prefix_unchanged() {
        assert_eq!(normalize_host("api.github.com"), "api.github.com");
    }

    #[tokio::test]
    async fn handle_discover_rejects_invalid_url_before_crawl() {
        let err = handle_discover(
            "not a url".to_string(),
            DiscoverOptions {
                max_depth: 1,
                max_pages: 1,
                crawl_concurrency: 1,
                allow_external: false,
                include_subdomains: false,
                no_sitemaps: true,
            },
            CommonSyncOptions {
                active_within_days: None,
                no_star: false,
                dry_run: true,
                concurrency: Some(1),
                no_rate_limit: true,
                incremental: false,
            },
            &Config::default(),
            "sqlite://unused?mode=memory",
        )
        .await
        .expect_err("invalid URL should fail fast");

        assert!(
            err.to_string().contains("Invalid URL 'not a url'"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn load_instances_normalizes_hosts_for_lookup() {
        let db = setup_db("load-instances").await;

        let first_id = uuid::Uuid::new_v4();
        let second_id = uuid::Uuid::new_v4();

        curator::entity::instance::ActiveModel {
            id: Set(first_id),
            name: Set("gh".to_string()),
            platform_type: Set(PlatformType::GitHub),
            host: Set("WWW.GitHub.com.".to_string()),
            oauth_client_id: Set(None),
            oauth_flow: Set("auto".to_string()),
            created_at: Set(Utc::now().fixed_offset()),
        }
        .insert(&db)
        .await
        .expect("insert should succeed");

        curator::entity::instance::ActiveModel {
            id: Set(second_id),
            name: Set("gl".to_string()),
            platform_type: Set(PlatformType::GitLab),
            host: Set("gitlab.example.com".to_string()),
            oauth_client_id: Set(None),
            oauth_flow: Set("auto".to_string()),
            created_at: Set(Utc::now().fixed_offset()),
        }
        .insert(&db)
        .await
        .expect("insert should succeed");

        let (instances_by_host, instances_by_id) = load_instances(&db)
            .await
            .expect("load instances should succeed");

        assert_eq!(instances_by_host.get("github.com"), Some(&first_id));
        assert_eq!(
            instances_by_host.get("gitlab.example.com"),
            Some(&second_id)
        );
        assert!(instances_by_id.len() >= 2);
        assert_eq!(
            instances_by_id.get(&first_id).map(|i| i.name.as_str()),
            Some("gh")
        );
        assert_eq!(
            instances_by_id.get(&second_id).map(|i| i.name.as_str()),
            Some("gl")
        );
    }

    #[tokio::test]
    async fn load_instances_rejects_duplicate_host_per_platform_on_insert() {
        let db = setup_db("load-host-collision").await;

        let first_id = uuid::Uuid::new_v4();
        let second_id = uuid::Uuid::new_v4();

        curator::entity::instance::ActiveModel {
            id: Set(first_id),
            name: Set("first-collision".to_string()),
            platform_type: Set(PlatformType::GitLab),
            host: Set("collision.example.com".to_string()),
            oauth_client_id: Set(None),
            oauth_flow: Set("auto".to_string()),
            created_at: Set(Utc::now().fixed_offset()),
        }
        .insert(&db)
        .await
        .expect("first insert should succeed");

        let err = curator::entity::instance::ActiveModel {
            id: Set(second_id),
            name: Set("second-collision".to_string()),
            platform_type: Set(PlatformType::GitLab),
            host: Set("collision.example.com".to_string()),
            oauth_client_id: Set(None),
            oauth_flow: Set("auto".to_string()),
            created_at: Set(Utc::now().fixed_offset()),
        }
        .insert(&db)
        .await
        .expect_err("second insert should fail due to duplicate host/platform");

        assert!(
            err.to_string()
                .to_ascii_lowercase()
                .contains("unique constraint"),
            "unexpected error: {err}"
        );

        let (instances_by_host, instances_by_id) = load_instances(&db)
            .await
            .expect("load instances should succeed");

        assert_eq!(
            instances_by_host.get("collision.example.com"),
            Some(&first_id)
        );
        assert_eq!(
            instances_by_id.get(&first_id).map(|i| i.name.as_str()),
            Some("first-collision")
        );
        assert!(!instances_by_id.contains_key(&second_id));
    }

    #[tokio::test]
    async fn load_instances_propagates_database_errors() {
        let db = setup_db("load-instances-query-failure").await;
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "DROP TABLE instances".to_string(),
        ))
        .await
        .expect("instances table should be dropped for error-path test");

        let err = load_instances(&db)
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
}
