use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, QueryFilter,
    sea_query::{Alias, Expr, OnConflict},
};
use uuid::Uuid;

use crate::entity::code_repository::{ActiveModel, Column, Entity as CodeRepository};

use super::errors::{RepositoryError, Result};
use super::single::upsert;

// ─── Bulk Operations ─────────────────────────────────────────────────────────

/// Insert multiple repositories in a single transaction.
///
/// # Errors
/// Returns `RepositoryError::Database` if any insert fails. The entire operation is atomic.
pub async fn insert_many(db: &DatabaseConnection, models: Vec<ActiveModel>) -> Result<u64> {
    if models.is_empty() {
        return Ok(0);
    }

    let count = models.len() as u64;
    CodeRepository::insert_many(models).exec(db).await?;
    Ok(count)
}

/// Upsert multiple repositories by their natural keys.
///
/// For each repository, if one with the same platform and platform_id exists,
/// it will be updated. Otherwise, a new one will be inserted.
///
/// Note: This performs individual upserts in sequence. For very large batches,
/// use `bulk_upsert` instead for better performance.
pub async fn upsert_many(db: &DatabaseConnection, models: Vec<ActiveModel>) -> Result<u64> {
    let mut count = 0u64;
    for model in models {
        upsert(db, model).await?;
        count += 1;
    }
    Ok(count)
}

/// Default number of retry attempts for bulk upsert operations.
pub const DEFAULT_BULK_UPSERT_RETRIES: u32 = 3;

/// Default initial backoff delay in milliseconds for bulk upsert retries.
pub const DEFAULT_BULK_UPSERT_BACKOFF_MS: u64 = 100;

/// Bulk upsert multiple repositories using SQL ON CONFLICT.
///
/// This is significantly faster than `upsert_many` for large batches because it:
/// - Uses a single INSERT ... ON CONFLICT DO UPDATE statement
/// - Reduces database round-trips from 2n to 1
/// - Only updates rows where `updated_at` has changed (content-based deduplication)
///
/// The natural key for conflict detection is (instance_id, platform_id).
/// The conditional update ensures we only modify rows when the platform's
/// `updated_at` timestamp has changed, avoiding unnecessary writes.
///
/// # Returns
/// Returns the number of rows actually inserted or updated.
pub async fn bulk_upsert(db: &DatabaseConnection, models: Vec<ActiveModel>) -> Result<u64> {
    bulk_upsert_inner(db, models).await
}

/// Bulk upsert with configurable retry logic.
///
/// Retries transient database errors (e.g., database locked, connection issues)
/// with exponential backoff.
///
/// # Arguments
/// * `db` - Database connection
/// * `models` - Models to upsert
/// * `max_retries` - Maximum number of retry attempts (0 = no retries)
/// * `initial_backoff_ms` - Initial backoff delay in milliseconds (doubles each retry)
///
/// # Returns
/// Returns the number of rows actually inserted or updated, or the last error if all retries fail.
pub async fn bulk_upsert_with_retry(
    db: &DatabaseConnection,
    models: Vec<ActiveModel>,
    max_retries: u32,
    initial_backoff_ms: u64,
) -> Result<u64> {
    if models.is_empty() {
        return Ok(0);
    }

    tracing::debug!(count = models.len(), "Starting bulk upsert");
    let mut last_error: Option<RepositoryError> = None;
    let mut backoff_ms = initial_backoff_ms;

    for attempt in 0..=max_retries {
        match bulk_upsert_inner(db, models.clone()).await {
            Ok(count) => return Ok(count),
            Err(e) => {
                // Check if the error is retryable
                if is_retryable_error(&e) && attempt < max_retries {
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_retries = max_retries,
                        backoff_ms = backoff_ms,
                        error = %e,
                        "Bulk upsert failed, retrying..."
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms *= 2; // Exponential backoff
                    last_error = Some(e);
                } else {
                    return Err(e);
                }
            }
        }
    }

    // Should not reach here, but return last error if we do
    Err(last_error.unwrap_or_else(|| RepositoryError::InvalidInput {
        message: "Unexpected retry loop exit".to_string(),
    }))
}

/// Delete multiple repositories by their UUIDs.
///
/// Returns the total number of rows deleted.
pub async fn delete_many(db: &DatabaseConnection, ids: Vec<Uuid>) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }

    let result = CodeRepository::delete_many()
        .filter(Column::Id.is_in(ids))
        .exec(db)
        .await?;
    Ok(result.rows_affected)
}

/// Delete all repositories for a given instance.
///
/// Returns the number of rows deleted.
pub async fn delete_by_instance(db: &DatabaseConnection, instance_id: Uuid) -> Result<u64> {
    let result = CodeRepository::delete_many()
        .filter(Column::InstanceId.eq(instance_id))
        .exec(db)
        .await?;
    Ok(result.rows_affected)
}

/// Delete repositories by owner/name pairs for a specific instance.
///
/// This is used when pruning starred repositories - when a repo is unstarred,
/// it should also be removed from the database.
///
/// Returns the number of rows deleted.
pub async fn delete_by_owner_name(
    db: &DatabaseConnection,
    instance_id: Uuid,
    repos: &[(String, String)], // (owner, name) pairs
) -> Result<u64> {
    if repos.is_empty() {
        return Ok(0);
    }

    let mut total_deleted = 0u64;

    // Delete in batches to avoid overly large queries
    for chunk in repos.chunks(100) {
        // Build OR conditions for each (owner, name) pair
        let mut condition = Condition::any();
        for (owner, name) in chunk {
            condition = condition.add(
                Condition::all()
                    .add(Column::Owner.eq(owner.clone()))
                    .add(Column::Name.eq(name.clone())),
            );
        }

        let result = CodeRepository::delete_many()
            .filter(Column::InstanceId.eq(instance_id))
            .filter(condition)
            .exec(db)
            .await?;

        total_deleted += result.rows_affected;
    }

    Ok(total_deleted)
}

/// Check if a repository error is retryable (transient).
fn is_retryable_error(err: &RepositoryError) -> bool {
    match err {
        RepositoryError::Database(db_err) => is_retryable_db_error(db_err),
        _ => false,
    }
}

fn is_retryable_db_error(err: &DbErr) -> bool {
    match err {
        DbErr::ConnectionAcquire(_) | DbErr::Conn(_) => true,
        DbErr::Exec(_) | DbErr::Query(_) => {
            let err_str = err.to_string().to_lowercase();
            // SQLite: database is locked, busy
            // PostgreSQL: connection refused, too many connections
            // General: timeout, connection reset
            err_str.contains("locked")
                || err_str.contains("busy")
                || err_str.contains("timeout")
                || err_str.contains("connection")
                || err_str.contains("temporarily unavailable")
        }
        _ => false,
    }
}

/// Build the ON CONFLICT clause used by bulk upsert.
///
/// Conflict detection uses (instance_id, platform_id) as the natural key.
/// Only updates rows where `updated_at` has changed (content-based deduplication),
/// preventing unnecessary writes when the API returns the same data.
pub(crate) fn build_upsert_on_conflict() -> OnConflict {
    OnConflict::columns([Column::InstanceId, Column::PlatformId])
        .update_columns([
            Column::Owner,
            Column::Name,
            Column::Description,
            Column::DefaultBranch,
            Column::Topics,
            Column::PrimaryLanguage,
            Column::LicenseSpdx,
            Column::Homepage,
            Column::Visibility,
            Column::IsFork,
            Column::IsMirror,
            Column::IsArchived,
            Column::IsTemplate,
            Column::IsEmpty,
            Column::Stars,
            Column::Forks,
            Column::OpenIssues,
            Column::Watchers,
            Column::SizeKb,
            Column::HasIssues,
            Column::HasWiki,
            Column::HasPullRequests,
            Column::CreatedAt,
            Column::UpdatedAt,
            Column::PushedAt,
            Column::PlatformMetadata,
            Column::SyncedAt,
        ])
        // Only update if updated_at has changed (content-based deduplication).
        // The condition is: existing.updated_at IS NULL OR existing.updated_at != new.updated_at
        .action_and_where(
            Condition::any()
                .add(Expr::col((CodeRepository, Column::UpdatedAt)).is_null())
                .add(
                    Expr::col((CodeRepository, Column::UpdatedAt))
                        .ne(Expr::col((Alias::new("excluded"), Column::UpdatedAt))),
                )
                .into(),
        )
        .to_owned()
}

/// Internal bulk upsert implementation.
async fn bulk_upsert_inner(db: &DatabaseConnection, models: Vec<ActiveModel>) -> Result<u64> {
    if models.is_empty() {
        return Ok(0);
    }

    CodeRepository::insert_many(models)
        .on_conflict(build_upsert_on_conflict())
        .exec_without_returning(db)
        .await
        .map_err(RepositoryError::from)
}
