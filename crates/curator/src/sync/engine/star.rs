use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::platform::{PlatformClient, PlatformError};

use super::super::progress::{ProgressCallback, SyncProgress, emit};
use super::super::types::StarringStats;

/// Star repositories concurrently using the platform client.
///
/// Uses a semaphore to limit concurrency. Rate limiting is handled
/// transparently by the platform client's adaptive rate limiter.
pub(super) async fn star_repos_concurrent<C: PlatformClient + Clone + 'static>(
    client: &C,
    repos: Vec<(String, String)>,
    concurrency: usize,
    dry_run: bool,
    on_progress: Option<&ProgressCallback>,
) -> StarringStats {
    let mut stats = StarringStats::default();

    if repos.is_empty() {
        return stats;
    }

    let concurrency = std::cmp::max(1, std::cmp::min(concurrency, repos.len()));
    let semaphore = Arc::new(Semaphore::new(concurrency));

    emit(
        on_progress,
        SyncProgress::StarringRepos {
            count: repos.len(),
            concurrency,
            dry_run,
        },
    );

    let mut handles = Vec::with_capacity(repos.len());

    for (owner, name) in repos {
        let client = client.clone();
        let semaphore = Arc::clone(&semaphore);

        let handle = tokio::spawn(async move {
            let _permit = match semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    return (
                        owner,
                        name,
                        Err(PlatformError::internal("Semaphore closed unexpectedly")),
                    );
                }
            };

            let result = if dry_run {
                match client.is_repo_starred(&owner, &name).await {
                    Ok(true) => Ok(false),
                    Ok(false) => Ok(true),
                    Err(e) => Err(e),
                }
            } else {
                client.star_repo_with_retry(&owner, &name, None).await
            };

            (owner, name, result)
        });

        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok((owner, name, Ok(true))) => {
                stats.starred += 1;
                emit(
                    on_progress,
                    SyncProgress::StarredRepo {
                        owner,
                        name,
                        already_starred: false,
                    },
                );
            }
            Ok((owner, name, Ok(false))) => {
                stats.skipped += 1;
                emit(
                    on_progress,
                    SyncProgress::StarredRepo {
                        owner,
                        name,
                        already_starred: true,
                    },
                );
            }
            Ok((owner, name, Err(e))) => {
                let err_msg = e.to_string();
                stats
                    .errors
                    .push(format!("{}/{}: {}", owner, name, err_msg));
                emit(
                    on_progress,
                    SyncProgress::StarError {
                        owner,
                        name,
                        error: err_msg,
                    },
                );
            }
            Err(e) => {
                stats.errors.push(format!("Task panic: {}", e));
            }
        }
    }

    emit(
        on_progress,
        SyncProgress::StarringComplete {
            starred: stats.starred,
            already_starred: stats.skipped,
            errors: stats.errors.len(),
        },
    );

    stats
}

/// Result of pruning operation.
pub(super) struct PruneResult {
    /// Number of repos pruned.
    pub(super) pruned: usize,
    /// List of successfully pruned repos (owner, name) for database cleanup.
    pub(super) pruned_repos: Vec<(String, String)>,
    /// Errors encountered.
    pub(super) errors: Vec<String>,
}

/// Prune (unstar) inactive repositories.
///
/// Unstars repositories that are outside the activity window.
/// Returns the list of successfully pruned repos so they can be deleted from the database.
pub(super) async fn prune_repos<C: PlatformClient + Clone + 'static>(
    client: &C,
    repos: Vec<(String, String)>,
    dry_run: bool,
    on_progress: Option<&ProgressCallback>,
) -> PruneResult {
    let mut result = PruneResult {
        pruned: 0,
        pruned_repos: Vec::new(),
        errors: Vec::new(),
    };

    if repos.is_empty() {
        return result;
    }

    emit(
        on_progress,
        SyncProgress::PruningRepos {
            count: repos.len(),
            dry_run,
        },
    );

    for (owner, name) in repos {
        if dry_run {
            result.pruned += 1;
            result.pruned_repos.push((owner.clone(), name.clone()));
            emit(
                on_progress,
                SyncProgress::PrunedRepo {
                    owner: owner.clone(),
                    name: name.clone(),
                },
            );
        } else {
            match client.unstar_repo(&owner, &name).await {
                Ok(true) => {
                    result.pruned += 1;
                    result.pruned_repos.push((owner.clone(), name.clone()));
                    emit(
                        on_progress,
                        SyncProgress::PrunedRepo {
                            owner: owner.clone(),
                            name: name.clone(),
                        },
                    );
                }
                Ok(false) => {}
                Err(e) if e.is_not_found() => {
                    // Project no longer exists — treat as pruned so the DB
                    // entry gets cleaned up instead of retrying forever.
                    result.pruned += 1;
                    result.pruned_repos.push((owner.clone(), name.clone()));
                    emit(
                        on_progress,
                        SyncProgress::PrunedRepo {
                            owner: owner.clone(),
                            name: name.clone(),
                        },
                    );
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    result
                        .errors
                        .push(format!("{}/{}: {}", owner, name, err_msg));
                    emit(
                        on_progress,
                        SyncProgress::PruneError {
                            owner: owner.clone(),
                            name: name.clone(),
                            error: err_msg,
                        },
                    );
                }
            }
        }
    }

    emit(
        on_progress,
        SyncProgress::PruningComplete {
            pruned: result.pruned,
            errors: result.errors.len(),
        },
    );

    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
    use crate::entity::platform_type::PlatformType;
    use crate::platform::{
        OrgInfo, PlatformRepo, RateLimitInfo, Result as PlatformResult, UserInfo,
    };

    use super::*;

    #[derive(Clone, Default)]
    struct TestClient {
        is_starred: Arc<Mutex<HashMap<String, PlatformResult<bool>>>>,
        star_results: Arc<Mutex<HashMap<String, PlatformResult<bool>>>>,
        unstar_results: Arc<Mutex<HashMap<String, PlatformResult<bool>>>>,
        is_starred_calls: Arc<Mutex<Vec<String>>>,
        star_calls: Arc<Mutex<Vec<String>>>,
        unstar_calls: Arc<Mutex<Vec<String>>>,
    }

    impl TestClient {
        fn key(owner: &str, name: &str) -> String {
            format!("{owner}/{name}")
        }

        fn set_is_starred(&self, owner: &str, name: &str, value: PlatformResult<bool>) {
            let key = Self::key(owner, name);
            self.is_starred
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key, value);
        }

        fn set_star_result(&self, owner: &str, name: &str, value: PlatformResult<bool>) {
            let key = Self::key(owner, name);
            self.star_results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key, value);
        }

        fn set_unstar_result(&self, owner: &str, name: &str, value: PlatformResult<bool>) {
            let key = Self::key(owner, name);
            self.unstar_results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key, value);
        }
    }

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

        async fn is_repo_starred(&self, owner: &str, name: &str) -> PlatformResult<bool> {
            let key = Self::key(owner, name);
            self.is_starred_calls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(key.clone());
            self.is_starred
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key)
                .unwrap_or(Ok(false))
        }

        async fn star_repo(&self, _owner: &str, _name: &str) -> PlatformResult<bool> {
            panic!("unused in tests")
        }

        async fn star_repo_with_retry(
            &self,
            owner: &str,
            name: &str,
            _on_progress: Option<&ProgressCallback>,
        ) -> PlatformResult<bool> {
            let key = Self::key(owner, name);
            self.star_calls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(key.clone());
            self.star_results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key)
                .unwrap_or(Ok(false))
        }

        async fn unstar_repo(&self, owner: &str, name: &str) -> PlatformResult<bool> {
            let key = Self::key(owner, name);
            self.unstar_calls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(key.clone());
            self.unstar_results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key)
                .unwrap_or(Ok(false))
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
            panic!("unused in tests")
        }
    }

    #[tokio::test]
    async fn test_star_repos_concurrent_dry_run_maps_statuses() {
        let client = TestClient::default();
        client.set_is_starred("org", "already", Ok(true));
        client.set_is_starred("org", "new", Ok(false));
        client.set_is_starred("org", "oops", Err(PlatformError::api("check failed")));

        let stats = star_repos_concurrent(
            &client,
            vec![
                ("org".to_string(), "already".to_string()),
                ("org".to_string(), "new".to_string()),
                ("org".to_string(), "oops".to_string()),
            ],
            3,
            true,
            None,
        )
        .await;

        assert_eq!(stats.starred, 1);
        assert_eq!(stats.skipped, 1);
        assert_eq!(stats.errors.len(), 1);
        assert!(stats.errors[0].contains("org/oops"));

        let is_starred_calls = client
            .is_starred_calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        let star_calls = client
            .star_calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        assert_eq!(is_starred_calls, 3);
        assert_eq!(star_calls, 0);
    }

    #[tokio::test]
    async fn test_star_repos_concurrent_with_zero_concurrency_completes() {
        let client = TestClient::default();
        client.set_star_result("org", "repo", Ok(true));

        let stats = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            star_repos_concurrent(
                &client,
                vec![("org".to_string(), "repo".to_string())],
                0,
                false,
                None,
            ),
        )
        .await
        .expect("star_repos_concurrent should not hang with zero concurrency");

        assert_eq!(stats.starred, 1);
        assert_eq!(stats.skipped, 0);
        assert!(stats.errors.is_empty());
    }

    #[tokio::test]
    async fn test_prune_repos_treats_not_found_as_pruned() {
        let client = TestClient::default();
        client.set_unstar_result("org", "gone", Err(PlatformError::not_found("org/gone")));

        let result = prune_repos(
            &client,
            vec![("org".to_string(), "gone".to_string())],
            false,
            None,
        )
        .await;

        assert_eq!(result.pruned, 1);
        assert_eq!(
            result.pruned_repos,
            vec![("org".to_string(), "gone".to_string())]
        );
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_prune_repos_mixed_results() {
        let client = TestClient::default();
        client.set_unstar_result("org", "yes", Ok(true));
        client.set_unstar_result("org", "no", Ok(false));
        client.set_unstar_result("org", "err", Err(PlatformError::api("boom")));

        let result = prune_repos(
            &client,
            vec![
                ("org".to_string(), "yes".to_string()),
                ("org".to_string(), "no".to_string()),
                ("org".to_string(), "err".to_string()),
            ],
            false,
            None,
        )
        .await;

        assert_eq!(result.pruned, 1);
        assert_eq!(
            result.pruned_repos,
            vec![("org".to_string(), "yes".to_string())]
        );
        assert_eq!(result.errors.len(), 1);
        assert!(result.errors[0].contains("org/err"));
    }
}
