use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::platform::{PlatformClient, PlatformError, PlatformRepo};

use super::super::progress::{ProgressCallback, SyncProgress, emit};
use super::super::types::StarringStats;

/// Star repositories sequentially using the platform client.
///
/// This is the unified starring implementation for use with the generic sync engine.
/// Currently unused as platform-specific sync functions use their own starring logic
/// that operates on platform-specific types before conversion.
#[allow(dead_code)]
pub(super) async fn star_repos_sequential<C: PlatformClient>(
    client: &C,
    repos: &[&PlatformRepo],
    dry_run: bool,
    on_progress: Option<&ProgressCallback>,
) -> StarringStats {
    let mut stats = StarringStats::default();

    if repos.is_empty() {
        return stats;
    }

    emit(
        on_progress,
        SyncProgress::StarringRepos {
            count: repos.len(),
            concurrency: 1,
            dry_run,
        },
    );

    for repo in repos {
        let result = if dry_run {
            Ok(true)
        } else {
            client
                .star_repo_with_retry(&repo.owner, &repo.name, on_progress)
                .await
        };

        match result {
            Ok(true) => {
                stats.starred += 1;
                emit(
                    on_progress,
                    SyncProgress::StarredRepo {
                        owner: repo.owner.clone(),
                        name: repo.name.clone(),
                        already_starred: false,
                    },
                );
            }
            Ok(false) => {
                stats.skipped += 1;
                emit(
                    on_progress,
                    SyncProgress::StarredRepo {
                        owner: repo.owner.clone(),
                        name: repo.name.clone(),
                        already_starred: true,
                    },
                );
            }
            Err(e) => {
                let err_msg = e.to_string();
                stats
                    .errors
                    .push(format!("{}/{}: {}", repo.owner, repo.name, err_msg));
                emit(
                    on_progress,
                    SyncProgress::StarError {
                        owner: repo.owner.clone(),
                        name: repo.name.clone(),
                        error: err_msg,
                    },
                );
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

/// Star repositories concurrently using the platform client.
///
/// Uses a semaphore to limit concurrency. When a rate limiter is provided,
/// waits for rate limit clearance before each starring operation.
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

    let concurrency = std::cmp::min(concurrency, repos.len());
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
