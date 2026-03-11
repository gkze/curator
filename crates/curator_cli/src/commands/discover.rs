//! Discovery command for crawling a URL and syncing discovered repositories.

use std::collections::HashMap;
use std::sync::Arc;

use console::Term;
use sea_orm::{DatabaseConnection, EntityTrait};
use url::Url;

use curator::discovery::DiscoveryResult;
use curator::discovery::{CrawlOptions, DiscoveryProgress, RepoLink, discover_repo_links};
use curator::{
    Instance, InstanceModel, PlatformType, db,
    sync::{PlatformOptions, SyncOptions},
};

use crate::CommonSyncOptions;
use crate::DiscoverOptions;
use crate::commands::shared::{
    ResolvedCommonSyncOptions, SyncKind, SyncRunner, active_within_duration, build_rate_limiter,
    display_final_rate_limit, get_token_for_instance_with_db, resolve_common_sync_options,
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

    let crawl_options = build_crawl_options(&discover_opts);

    let reporter = Arc::new(ProgressReporter::new());
    let progress_cb = {
        let reporter = Arc::clone(&reporter);
        let expected_pages = crawl_options.max_pages.min(u32::MAX as usize) as u32;
        move |event: DiscoveryProgress| {
            if let Some(progress) = discovery_progress_to_sync_progress(event, expected_pages) {
                reporter.handle(progress);
            }
        }
    };

    let discovery = discover_repo_links(&start_url, &crawl_options, Some(&progress_cb)).await?;

    reporter.finish();

    let is_tty = Term::stdout().is_term();

    if is_tty {
        println!("{}", discovery_complete_line(&discovery));
        if let Some(line) = discovery_warning_line(&discovery) {
            println!("{line}");
        }
    } else {
        tracing::info!(
            pages_visited = discovery.pages_visited,
            pages_fetched = discovery.pages_fetched,
            repo_links = discovery.repo_links.len(),
            "Discovery complete"
        );
        if !discovery.errors.is_empty() {
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
        for (host, count) in skipped_host_lines(&skipped_hosts) {
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

fn discovery_complete_line(discovery: &DiscoveryResult) -> String {
    format!(
        "Discovery complete: visited {} pages (fetched {}), found {} repo links",
        discovery.pages_visited,
        discovery.pages_fetched,
        discovery.repo_links.len()
    )
}

fn discovery_warning_line(discovery: &DiscoveryResult) -> Option<String> {
    (!discovery.errors.is_empty()).then(|| {
        format!(
            "Discovery warnings: {} fetch errors",
            discovery.errors.len()
        )
    })
}

fn skipped_host_lines(skipped_hosts: &SkippedHosts) -> Vec<(String, usize)> {
    let mut skipped: Vec<_> = skipped_hosts.iter().map(|(h, c)| (h.clone(), *c)).collect();
    skipped.sort_by(|a, b| a.0.cmp(&b.0));
    skipped
}

fn build_discovery_runner(
    db_conn: Arc<DatabaseConnection>,
    options: SyncOptions,
    no_rate_limit: bool,
    active_within_days: u64,
) -> SyncRunner {
    SyncRunner::new(db_conn, options, no_rate_limit, active_within_days)
}

fn build_discovery_sync_options_from_common(
    sync_opts: &CommonSyncOptions,
    config: &Config,
) -> Result<(SyncOptions, ResolvedCommonSyncOptions), Box<dyn std::error::Error>> {
    let resolved = resolve_common_sync_options(sync_opts, config);
    let options = build_discovery_sync_options(
        resolved.active_within_days,
        resolved.star,
        sync_opts.dry_run,
        resolved.concurrency,
        resolved.strategy,
    )?;
    Ok((options, resolved))
}

fn build_crawl_options(discover_opts: &DiscoverOptions) -> CrawlOptions {
    CrawlOptions {
        max_depth: discover_opts.max_depth,
        max_pages: discover_opts.max_pages,
        concurrency: discover_opts.crawl_concurrency,
        same_host: !discover_opts.allow_external,
        include_subdomains: discover_opts.include_subdomains,
        use_sitemaps: !discover_opts.no_sitemaps,
        ..CrawlOptions::default()
    }
}

fn discovery_progress_to_sync_progress(
    event: DiscoveryProgress,
    expected_pages: u32,
) -> Option<curator::sync::SyncProgress> {
    match event {
        DiscoveryProgress::Started { max_pages, .. } => {
            Some(curator::sync::SyncProgress::FetchingRepos {
                namespace: "discovery".to_string(),
                total_repos: Some(max_pages),
                expected_pages: Some(expected_pages),
            })
        }
        DiscoveryProgress::PageFetched { fetched } => {
            Some(curator::sync::SyncProgress::FetchedPage {
                namespace: "discovery".to_string(),
                page: fetched as u32,
                count: 1,
                total_so_far: fetched,
                expected_pages: Some(expected_pages),
            })
        }
        DiscoveryProgress::Error { message } => {
            Some(curator::sync::SyncProgress::Warning { message })
        }
        DiscoveryProgress::Finished { pages_fetched, .. } => {
            Some(curator::sync::SyncProgress::FetchComplete {
                namespace: "discovery".to_string(),
                total: pages_fetched,
            })
        }
    }
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
    let token = get_token_for_instance_with_db(instance, config, Some(db_conn.as_ref())).await?;
    let (options, resolved) = build_discovery_sync_options_from_common(&sync_opts, config)?;

    let runner = build_discovery_runner(
        Arc::clone(&db_conn),
        options.clone(),
        resolved.no_rate_limit,
        resolved.active_within_days,
    );

    let is_tty = Term::stdout().is_term();

    let rate_limiter = build_rate_limiter(instance.platform_type, resolved.no_rate_limit);

    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => {
            use curator::github::GitHubClient;
            let client = GitHubClient::new(&token, instance.id, rate_limiter)?;
            run_discovery_sync_for_client(
                &runner,
                &client,
                instance,
                repos,
                is_tty,
                resolved.no_rate_limit,
            )
            .await?;
        }
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => {
            use curator::gitlab::GitLabClient;
            let client =
                GitLabClient::new(&instance.host, &token, instance.id, rate_limiter).await?;
            run_discovery_sync_for_client(
                &runner,
                &client,
                instance,
                repos,
                is_tty,
                resolved.no_rate_limit,
            )
            .await?;
        }
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            use curator::gitea::GiteaClient;
            let client = GiteaClient::new(&instance.base_url(), &token, instance.id, rate_limiter)?;
            run_discovery_sync_for_client(
                &runner,
                &client,
                instance,
                repos,
                is_tty,
                resolved.no_rate_limit,
            )
            .await?;
        }
        #[cfg(not(feature = "github"))]
        PlatformType::GitHub => {
            return Err(crate::commands::shared::unsupported_platform_error(
                instance.platform_type,
                "discovery sync",
            ));
        }
        #[cfg(not(feature = "gitlab"))]
        PlatformType::GitLab => {
            return Err(crate::commands::shared::unsupported_platform_error(
                instance.platform_type,
                "discovery sync",
            ));
        }
        #[cfg(not(feature = "gitea"))]
        PlatformType::Gitea => {
            return Err(crate::commands::shared::unsupported_platform_error(
                instance.platform_type,
                "discovery sync",
            ));
        }
    }

    Ok(())
}

fn build_discovery_sync_options(
    active_within_days: u64,
    star: bool,
    dry_run: bool,
    concurrency: usize,
    strategy: curator::sync::SyncStrategy,
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
    use async_trait::async_trait;
    use chrono::Utc;
    use curator::platform::{OrgInfo, PlatformError, PlatformRepo, RateLimitInfo, UserInfo};
    use sea_orm::{ActiveModelTrait, ConnectionTrait, DatabaseBackend, Set, Statement};
    use std::sync::Arc;
    use tokio::sync::mpsc;

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

    #[derive(Clone)]
    struct TestClient;

    #[async_trait]
    impl curator::PlatformClient for TestClient {
        fn platform_type(&self) -> PlatformType {
            PlatformType::GitHub
        }

        fn instance_id(&self) -> uuid::Uuid {
            uuid::Uuid::nil()
        }

        async fn get_rate_limit(&self) -> Result<RateLimitInfo, PlatformError> {
            Ok(RateLimitInfo {
                limit: 5000,
                remaining: 4999,
                reset_at: Utc::now(),
                retry_after: None,
            })
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

    fn sample_instance(name: &str, platform_type: PlatformType, host: &str) -> InstanceModel {
        InstanceModel {
            id: uuid::Uuid::new_v4(),
            name: name.to_string(),
            platform_type,
            host: host.to_string(),
            oauth_client_id: None,
            oauth_flow: "auto".to_string(),
            created_at: Utc::now().fixed_offset(),
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

    #[tokio::test]
    async fn load_instances_returns_empty_maps_when_instances_table_is_empty() {
        let db = setup_db("load-instances-empty").await;
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "DELETE FROM instances".to_string(),
        ))
        .await
        .unwrap();

        let (instances_by_host, instances_by_id) = load_instances(&db).await.unwrap();
        assert!(instances_by_host.is_empty());
        assert!(instances_by_id.is_empty());
    }

    #[tokio::test]
    async fn run_discovery_sync_for_client_succeeds_for_repo_list() {
        let db = Arc::new(setup_db("run-discovery-sync").await);
        let runner = SyncRunner::new(
            Arc::clone(&db),
            SyncOptions {
                active_within: active_within_duration(30).expect("duration should resolve"),
                star: true,
                dry_run: true,
                concurrency: 2,
                platform_options: PlatformOptions::default(),
                prune: false,
                strategy: curator::sync::SyncStrategy::Full,
            },
            false,
            30,
        );
        let instance = InstanceModel {
            id: uuid::Uuid::new_v4(),
            name: "github".to_string(),
            platform_type: PlatformType::GitHub,
            host: "github.com".to_string(),
            oauth_client_id: None,
            oauth_flow: "auto".to_string(),
            created_at: Utc::now().fixed_offset(),
        };

        run_discovery_sync_for_client(
            &runner,
            &TestClient,
            &instance,
            &[("owner".to_string(), "repo".to_string())],
            false,
            false,
        )
        .await
        .expect("discovery sync helper should succeed");
    }

    #[test]
    fn normalize_host_handles_www_and_case() {
        assert_eq!(normalize_host(" WWW.GitHub.com. "), "github.com");
        assert_eq!(normalize_host("forge.example.com"), "forge.example.com");
    }

    #[test]
    fn build_crawl_options_maps_flags_to_crawler_options() {
        let opts = DiscoverOptions {
            max_depth: 3,
            max_pages: 25,
            crawl_concurrency: 7,
            allow_external: true,
            include_subdomains: true,
            no_sitemaps: true,
        };

        let crawl = build_crawl_options(&opts);
        assert_eq!(crawl.max_depth, 3);
        assert_eq!(crawl.max_pages, 25);
        assert_eq!(crawl.concurrency, 7);
        assert!(!crawl.same_host);
        assert!(crawl.include_subdomains);
        assert!(!crawl.use_sitemaps);
    }

    #[test]
    fn discovery_progress_to_sync_progress_maps_variants() {
        assert!(matches!(
            discovery_progress_to_sync_progress(
                DiscoveryProgress::Started {
                    seed_urls: 1,
                    max_pages: 12,
                    max_depth: 2,
                },
                12,
            ),
            Some(curator::sync::SyncProgress::FetchingRepos {
                total_repos: Some(12),
                ..
            })
        ));
        assert!(matches!(
            discovery_progress_to_sync_progress(DiscoveryProgress::PageFetched { fetched: 2 }, 12),
            Some(curator::sync::SyncProgress::FetchedPage { page: 2, .. })
        ));
        assert!(matches!(
            discovery_progress_to_sync_progress(
                DiscoveryProgress::Error {
                    message: "oops".into()
                },
                12,
            ),
            Some(curator::sync::SyncProgress::Warning { .. })
        ));
        assert!(matches!(
            discovery_progress_to_sync_progress(
                DiscoveryProgress::Finished {
                    pages_visited: 3,
                    pages_fetched: 2,
                    repo_links: 1,
                    errors: 0,
                },
                12,
            ),
            Some(curator::sync::SyncProgress::FetchComplete { total: 2, .. })
        ));
    }

    #[test]
    fn build_crawl_options_defaults_same_host_when_external_disabled() {
        let opts = DiscoverOptions {
            max_depth: 1,
            max_pages: 2,
            crawl_concurrency: 3,
            allow_external: false,
            include_subdomains: false,
            no_sitemaps: false,
        };

        let crawl = build_crawl_options(&opts);
        assert!(crawl.same_host);
        assert!(!crawl.include_subdomains);
        assert!(crawl.use_sitemaps);
    }

    #[test]
    fn discovery_output_helpers_format_expected_messages() {
        let discovery = curator::discovery::DiscoveryResult {
            start_url: Url::parse("https://example.com").unwrap(),
            seed_urls: vec![],
            pages_visited: 5,
            pages_fetched: 4,
            repo_links: vec![repo_link("github.com", "owner", "repo")],
            errors: vec!["oops".to_string(), "retry".to_string()],
        };

        assert!(discovery_complete_line(&discovery).contains("visited 5 pages"));
        assert_eq!(
            discovery_warning_line(&discovery).as_deref(),
            Some("Discovery warnings: 2 fetch errors")
        );
        let none = curator::discovery::DiscoveryResult {
            errors: vec![],
            ..discovery
        };
        assert!(discovery_warning_line(&none).is_none());
    }

    #[test]
    fn skipped_host_lines_sort_hosts() {
        let skipped = HashMap::from([
            ("z.example".to_string(), 2_usize),
            ("a.example".to_string(), 1_usize),
        ]);
        assert_eq!(
            skipped_host_lines(&skipped),
            vec![("a.example".to_string(), 1), ("z.example".to_string(), 2)]
        );
    }

    #[test]
    fn build_discovery_sync_options_maps_fields_and_errors_on_large_duration() {
        let options = build_discovery_sync_options(
            30,
            true,
            true,
            4,
            curator::sync::SyncStrategy::Incremental,
        )
        .unwrap();
        assert!(options.star);
        assert!(options.dry_run);
        assert_eq!(options.concurrency, 4);
        assert_eq!(options.strategy, curator::sync::SyncStrategy::Incremental);

        let err = build_discovery_sync_options(
            (i64::MAX as u64) + 1,
            true,
            false,
            1,
            curator::sync::SyncStrategy::Full,
        )
        .expect_err("oversized duration should fail");
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn build_discovery_sync_options_from_common_uses_config_resolution() {
        let mut config = crate::config::Config::default();
        config.sync.active_within_days = 21;
        config.sync.concurrency = 9;
        config.sync.star = false;
        config.sync.no_rate_limit = true;

        let sync_opts = crate::CommonSyncOptions {
            active_within_days: None,
            no_star: false,
            dry_run: true,
            concurrency: None,
            no_rate_limit: false,
            incremental: true,
        };

        let (options, resolved) =
            build_discovery_sync_options_from_common(&sync_opts, &config).unwrap();
        assert_eq!(resolved.active_within_days, 21);
        assert_eq!(resolved.concurrency, 9);
        assert!(!resolved.star);
        assert!(resolved.no_rate_limit);
        assert_eq!(resolved.strategy, curator::sync::SyncStrategy::Incremental);
        assert_eq!(options.concurrency, 9);
        assert!(options.dry_run);
    }

    #[cfg(feature = "gitlab")]
    #[tokio::test]
    async fn sync_instance_repos_enters_gitlab_branch_before_client_setup_fails() {
        let _guard = crate::test_support::env_lock().lock().await;
        let db = Arc::new(setup_db("discover-gitlab-branch").await);
        let instance = sample_instance("work-gitlab", PlatformType::GitLab, "bad host");
        unsafe {
            std::env::set_var("CURATOR_INSTANCE_WORK_GITLAB_TOKEN", "token");
        }

        let err = sync_instance_repos(
            &instance,
            &[("group".to_string(), "repo".to_string())],
            crate::CommonSyncOptions {
                active_within_days: Some(30),
                no_star: false,
                dry_run: true,
                concurrency: Some(2),
                no_rate_limit: true,
                incremental: false,
            },
            &crate::config::Config::default(),
            db,
        )
        .await
        .expect_err("gitlab branch should fail with invalid host");

        unsafe {
            std::env::remove_var("CURATOR_INSTANCE_WORK_GITLAB_TOKEN");
        }
        assert!(!err.to_string().is_empty());
    }

    #[tokio::test]
    async fn build_discovery_runner_constructs_runner() {
        let db = Arc::new(setup_db("build-discovery-runner").await);
        let options =
            build_discovery_sync_options(14, true, true, 3, curator::sync::SyncStrategy::Full)
                .unwrap();
        let _runner = build_discovery_runner(db, options, true, 14);
    }

    #[test]
    fn map_repos_to_instances_dedupes_within_instance_but_not_across_instances() {
        let github_id = uuid::Uuid::new_v4();
        let ghe_id = uuid::Uuid::new_v4();
        let instances_by_host = HashMap::from([
            ("github.com".to_string(), github_id),
            ("ghe.example.com".to_string(), ghe_id),
        ]);

        let repos = vec![
            repo_link("github.com", "owner", "repo"),
            repo_link("github.com", "owner", "repo"),
            repo_link("ghe.example.com", "owner", "repo"),
        ];

        let (mapped, skipped) = map_repos_to_instances(&repos, &instances_by_host);

        assert!(skipped.is_empty());
        assert_eq!(mapped.get(&github_id).unwrap().len(), 1);
        assert_eq!(mapped.get(&ghe_id).unwrap().len(), 1);
    }
}
