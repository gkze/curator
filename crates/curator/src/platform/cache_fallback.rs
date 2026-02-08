//! Shared cache-hit fallback pattern for paginated repository listings.

use sea_orm::DatabaseConnection;
use std::future::Future;
use uuid::Uuid;

use crate::entity::code_repository::Model;
use crate::platform::ProgressCallback;
use crate::platform::errors::PlatformError;
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
