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

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use sea_orm::Database;

    use crate::entity::code_visibility::CodeVisibility;

    use super::*;

    fn model(owner: &str, name: &str) -> Model {
        Model {
            id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            platform_id: 1,
            owner: owner.to_string(),
            name: name.to_string(),
            description: None,
            default_branch: "main".to_string(),
            topics: serde_json::json!([]),
            primary_language: None,
            license_spdx: None,
            homepage: None,
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_mirror: false,
            is_archived: false,
            is_template: false,
            is_empty: false,
            stars: None,
            forks: None,
            open_issues: None,
            watchers: None,
            size_kb: None,
            has_issues: true,
            has_wiki: true,
            has_pull_requests: true,
            created_at: None,
            updated_at: None,
            pushed_at: None,
            platform_metadata: serde_json::json!({}),
            synced_at: Utc::now().fixed_offset(),
            etag: None,
        }
    }

    #[tokio::test]
    async fn test_handle_cache_hit_fallback_returns_none_without_db() {
        let result = handle_cache_hit_fallback(
            None,
            2,
            |_db| async { Ok(vec![model("org", "repo")]) },
            "org",
            None,
        )
        .await
        .expect("fallback should not fail");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_handle_cache_hit_fallback_emits_cache_events_for_non_empty_models() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite should connect");
        let events: Arc<Mutex<Vec<SyncProgress>>> = Arc::new(Mutex::new(Vec::new()));
        let events_capture = Arc::clone(&events);
        let callback: ProgressCallback = Box::new(move |event| {
            events_capture
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(event);
        });

        let result = handle_cache_hit_fallback(
            Some(&db),
            1,
            |_db| async { Ok(vec![model("org", "repo1"), model("org", "repo2")]) },
            "org",
            Some(&callback),
        )
        .await
        .expect("fallback should not fail")
        .expect("expected cached models");

        assert_eq!(result.len(), 2);
        let events = events.lock().unwrap_or_else(|e| e.into_inner());
        assert!(matches!(
            events.first(),
            Some(SyncProgress::CacheHit {
                cached_count: 2,
                ..
            })
        ));
        assert!(matches!(
            events.get(1),
            Some(SyncProgress::FetchComplete { total: 2, .. })
        ));
    }

    #[tokio::test]
    async fn test_handle_streaming_cache_hit_fallback_counts_only_successful_sends() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite should connect");
        let (tx, rx) = mpsc::channel::<PlatformRepo>(4);
        drop(rx);
        let total_sent = Arc::new(AtomicUsize::new(0));

        let streamed = handle_streaming_cache_hit_fallback(
            Some(&db),
            1,
            |_db| async { Ok(vec![model("org", "repo1"), model("org", "repo2")]) },
            "org",
            &tx,
            &total_sent,
            None,
        )
        .await
        .expect("streaming fallback should not fail");

        assert_eq!(streamed, 2);
        assert_eq!(total_sent.load(Ordering::Relaxed), 0);
    }
}
