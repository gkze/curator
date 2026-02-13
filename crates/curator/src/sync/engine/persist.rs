use chrono::Utc;
use tokio::sync::mpsc;

use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::platform::{PlatformClient, PlatformRepo};

use super::super::progress::{ProgressCallback, SyncProgress, emit};
use super::super::types::{SyncOptions, SyncResult};
use super::filter::is_active_repo;

pub(super) struct StreamingProcessResult {
    pub(super) result: SyncResult,
    pub(super) repos_to_star: Vec<(String, String)>,
}

pub(super) fn build_models<C: PlatformClient>(
    client: &C,
    repos: &[&PlatformRepo],
) -> Vec<CodeRepositoryActiveModel> {
    repos
        .iter()
        .map(|repo| client.to_active_model(repo))
        .collect()
}

pub(super) async fn process_streaming_repos<C: PlatformClient>(
    client: &C,
    namespace: &str,
    repos: &[PlatformRepo],
    options: &SyncOptions,
    model_tx: &mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> StreamingProcessResult {
    let cutoff = Utc::now() - options.active_within;

    emit(
        on_progress,
        SyncProgress::FilteringByActivity {
            namespace: namespace.to_string(),
            days: options.active_within.num_days(),
        },
    );

    let mut result = SyncResult {
        processed: repos.len(),
        ..SyncResult::default()
    };

    let mut channel_closed = false;
    let mut repos_to_star: Vec<(String, String)> = Vec::new();

    for repo in repos {
        if is_active_repo(repo, cutoff) {
            result.matched += 1;

            if options.star {
                repos_to_star.push((repo.owner.clone(), repo.name.clone()));
            }

            if !options.dry_run && !channel_closed {
                let model = client.to_active_model(repo);
                if model_tx.send(model).await.is_err() {
                    channel_closed = true;
                    emit(
                        on_progress,
                        SyncProgress::Warning {
                            message: "persistence channel closed".to_string(),
                        },
                    );
                } else {
                    result.saved += 1;
                }
            }
        }
    }

    emit(
        on_progress,
        SyncProgress::FilterComplete {
            namespace: namespace.to_string(),
            matched: result.matched,
            total: result.processed,
        },
    );

    StreamingProcessResult {
        result,
        repos_to_star,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use chrono::{Duration, Utc};
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use crate::entity::code_visibility::CodeVisibility;
    use crate::entity::platform_type::PlatformType;
    use crate::platform::{OrgInfo, RateLimitInfo, Result as PlatformResult, UserInfo};
    use crate::sync::SyncStrategy;

    use super::*;

    #[derive(Clone, Default)]
    struct TestClient;

    #[async_trait]
    impl PlatformClient for TestClient {
        fn platform_type(&self) -> PlatformType {
            PlatformType::GitHub
        }

        fn instance_id(&self) -> Uuid {
            Uuid::nil()
        }

        async fn get_rate_limit(&self) -> PlatformResult<RateLimitInfo> {
            panic!("unused in tests")
        }

        async fn get_org_info(&self, _org: &str) -> PlatformResult<OrgInfo> {
            panic!("unused in tests")
        }

        async fn get_authenticated_user(&self) -> PlatformResult<UserInfo> {
            panic!("unused in tests")
        }

        async fn get_repo(
            &self,
            _owner: &str,
            _name: &str,
            _db: Option<&sea_orm::DatabaseConnection>,
        ) -> PlatformResult<PlatformRepo> {
            panic!("unused in tests")
        }

        async fn list_org_repos(
            &self,
            _org: &str,
            _db: Option<&sea_orm::DatabaseConnection>,
            _on_progress: Option<&ProgressCallback>,
        ) -> PlatformResult<Vec<PlatformRepo>> {
            panic!("unused in tests")
        }

        async fn list_user_repos(
            &self,
            _username: &str,
            _db: Option<&sea_orm::DatabaseConnection>,
            _on_progress: Option<&ProgressCallback>,
        ) -> PlatformResult<Vec<PlatformRepo>> {
            panic!("unused in tests")
        }

        async fn is_repo_starred(&self, _owner: &str, _name: &str) -> PlatformResult<bool> {
            panic!("unused in tests")
        }

        async fn star_repo(&self, _owner: &str, _name: &str) -> PlatformResult<bool> {
            panic!("unused in tests")
        }

        async fn star_repo_with_retry(
            &self,
            _owner: &str,
            _name: &str,
            _on_progress: Option<&ProgressCallback>,
        ) -> PlatformResult<bool> {
            panic!("unused in tests")
        }

        async fn unstar_repo(&self, _owner: &str, _name: &str) -> PlatformResult<bool> {
            panic!("unused in tests")
        }

        async fn list_starred_repos(
            &self,
            _db: Option<&sea_orm::DatabaseConnection>,
            _concurrency: usize,
            _skip_rate_checks: bool,
            _on_progress: Option<&ProgressCallback>,
        ) -> PlatformResult<Vec<PlatformRepo>> {
            panic!("unused in tests")
        }

        async fn list_starred_repos_streaming(
            &self,
            _repo_tx: mpsc::Sender<PlatformRepo>,
            _db: Option<&sea_orm::DatabaseConnection>,
            _concurrency: usize,
            _skip_rate_checks: bool,
            _on_progress: Option<&ProgressCallback>,
        ) -> PlatformResult<usize> {
            panic!("unused in tests")
        }

        fn to_active_model(&self, _repo: &PlatformRepo) -> CodeRepositoryActiveModel {
            CodeRepositoryActiveModel::default()
        }
    }

    fn repo(owner: &str, name: &str, days_ago: i64) -> PlatformRepo {
        PlatformRepo {
            platform_id: 42,
            owner: owner.to_string(),
            name: name.to_string(),
            description: None,
            default_branch: "main".to_string(),
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_archived: false,
            stars: None,
            forks: None,
            language: None,
            topics: vec![],
            created_at: None,
            updated_at: Some(Utc::now() - Duration::days(days_ago)),
            pushed_at: Some(Utc::now() - Duration::days(days_ago)),
            license: None,
            homepage: None,
            size_kb: None,
            metadata: serde_json::json!({}),
        }
    }

    fn options(star: bool, dry_run: bool) -> SyncOptions {
        SyncOptions {
            active_within: Duration::days(30),
            star,
            dry_run,
            concurrency: 4,
            platform_options: crate::sync::PlatformOptions::default(),
            prune: false,
            strategy: SyncStrategy::Full,
        }
    }

    #[tokio::test]
    async fn test_process_streaming_repos_collects_active_and_star_targets() {
        let client = TestClient;
        let repos = vec![repo("org", "active", 3), repo("org", "inactive", 90)];
        let (tx, mut rx) = mpsc::channel(10);

        let result =
            process_streaming_repos(&client, "org", &repos, &options(true, false), &tx, None).await;

        assert_eq!(result.result.processed, 2);
        assert_eq!(result.result.matched, 1);
        assert_eq!(result.result.saved, 1);
        assert_eq!(
            result.repos_to_star,
            vec![("org".to_string(), "active".to_string())]
        );

        let saved_model = rx.recv().await;
        assert!(saved_model.is_some());
    }

    #[tokio::test]
    async fn test_process_streaming_repos_warns_once_when_channel_closed() {
        let client = TestClient;
        let repos = vec![repo("org", "one", 1), repo("org", "two", 2)];

        let (tx, rx) = mpsc::channel(1);
        drop(rx);

        let events: Arc<Mutex<Vec<SyncProgress>>> = Arc::new(Mutex::new(Vec::new()));
        let events_capture = Arc::clone(&events);
        let callback: ProgressCallback = Box::new(move |event| {
            events_capture
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(event);
        });

        let result = process_streaming_repos(
            &client,
            "org",
            &repos,
            &options(false, false),
            &tx,
            Some(&callback),
        )
        .await;

        assert_eq!(result.result.processed, 2);
        assert_eq!(result.result.matched, 2);
        assert_eq!(result.result.saved, 0);

        let warnings = events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    SyncProgress::Warning { message }
                    if message == "persistence channel closed"
                )
            })
            .count();
        assert_eq!(warnings, 1);
    }
}
