//! Shared cache-hit fallback pattern for paginated repository listings.

use sea_orm::DatabaseConnection;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::entity::code_repository::Model;
use crate::platform::errors::PlatformError;
use crate::platform::{PlatformRepo, ProgressCallback};
use crate::repository::{find_all_by_instance, find_all_by_instance_and_owner};
use crate::sync::{SyncProgress, emit};

/// Handle the cache-hit fallback pattern for paginated listings.
///
/// Check if all pages were cache hits and result is empty. If so, load
/// from database, emit progress events, and return cached models.
///
/// Returns models if cache had valid data, otherwise None.
pub async fn handle_cache_hit_fallback<'a, F, Fut>(
    db: Option<&'a DatabaseConnection>,
    cache_hits: u32,
    repository_loader: F,
    namespace: &'a str,
    on_progress: Option<&'a ProgressCallback>,
) -> Result<Option<Vec<Model>>, PlatformError>
where
    F: FnOnce(&'a DatabaseConnection) -> Fut,
    Fut: Future<Output = Result<Vec<Model>, PlatformError>>,
{
    if cache_hits > 0
        && let Some(db) = db
    {
        let models = repository_loader(db).await?;

        if !models.is_empty() {
            emit(
                on_progress,
                SyncProgress::CacheHit {
                    namespace: namespace.to_string(),
                    cached_count: models.len(),
                },
            );
            emit(
                on_progress,
                SyncProgress::FetchComplete {
                    namespace: namespace.to_string(),
                    total: models.len(),
                },
            );
            return Ok(Some(models));
        }
    }
    Ok(None)
}

/// Handle the cache-hit fallback pattern for streaming paginated listings.
///
/// When all pages returned 304 Not Modified and no items have been sent yet,
/// load cached repositories from the database and stream them through the
/// provided channel. Emits `CacheHit` progress if repos are found.
///
/// Returns the number of repos streamed (0 if no fallback was needed).
pub async fn handle_streaming_cache_hit_fallback<'a, F, Fut>(
    db: Option<&'a DatabaseConnection>,
    cache_hits: u32,
    repository_loader: F,
    namespace: &'a str,
    repo_tx: &mpsc::Sender<PlatformRepo>,
    total_sent: &Arc<AtomicUsize>,
    on_progress: Option<&'a ProgressCallback>,
) -> Result<usize, PlatformError>
where
    F: FnOnce(&'a DatabaseConnection) -> Fut,
    Fut: Future<Output = Result<Vec<Model>, PlatformError>>,
{
    if cache_hits > 0
        && let Some(db) = db
    {
        let cached_repos = repository_loader(db).await?;

        if !cached_repos.is_empty() {
            let cached_count = cached_repos.len();

            emit(
                on_progress,
                SyncProgress::CacheHit {
                    namespace: namespace.to_string(),
                    cached_count,
                },
            );

            for model in &cached_repos {
                let repo = PlatformRepo::from_model(model);
                if repo_tx.send(repo).await.is_ok() {
                    total_sent.fetch_add(1, Ordering::Relaxed);
                }
            }

            return Ok(cached_count);
        }
    }
    Ok(0)
}

/// Load repositories by instance ID (for starred listings).
pub async fn load_repos_by_instance(
    db: &DatabaseConnection,
    instance_id: Uuid,
) -> Result<Vec<Model>, PlatformError> {
    find_all_by_instance(db, instance_id)
        .await
        .map_err(|e| PlatformError::internal(e.to_string()))
}

/// Load repositories by instance ID and owner (for org/user listings).
pub async fn load_repos_by_instance_and_owner(
    db: &DatabaseConnection,
    instance_id: Uuid,
    owner: &str,
) -> Result<Vec<Model>, PlatformError> {
    find_all_by_instance_and_owner(db, instance_id, owner)
        .await
        .map_err(|e| PlatformError::internal(e.to_string()))
}
