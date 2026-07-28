use std::collections::HashMap;

use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, DbErr,
    EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set, TransactionTrait,
    sea_query::{Alias, Expr, LockType, OnConflict},
};
use uuid::Uuid;

use crate::entity::code_repository::{ActiveModel, Column, Entity as CodeRepository};

use super::errors::{RepositoryError, Result};
use super::single::{required_active_value, upsert};

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

const INCOMPLETE_RECONCILIATION_ERROR_PREFIX: &str = "incomplete repository rename:";
// Each natural-key predicate uses four bind parameters. Keep scans below
// conservative SQLite parameter limits as well as PostgreSQL's larger limit.
const NATURAL_KEY_CONFLICT_SCAN_SIZE: usize = 100;
// Match the normal persistence batch size while bounding unusually large
// connected rename components that must remain in one transaction.
const BULK_UPSERT_STATEMENT_SIZE: usize = 500;

/// Bulk upsert multiple repositories using SQL ON CONFLICT.
///
/// This is significantly faster than `upsert_many` for large batches because it:
/// - Uses bounded INSERT ... ON CONFLICT DO UPDATE statements in one transaction
/// - Reconciles conflicting owner/name rows once per batch
/// - Only updates rows where content or repository identity has changed
///
/// The natural key for conflict detection is (instance_id, platform_id).
/// The conditional update avoids unnecessary writes while still persisting
/// repository transfers and renames.
///
/// # Returns
/// Returns the number of rows actually inserted or updated.
pub async fn bulk_upsert(db: &DatabaseConnection, models: Vec<ActiveModel>) -> Result<u64> {
    bulk_upsert_inner(db, models).await
}

/// Bulk upsert with configurable retry logic.
///
/// Retries transient database errors (e.g., database locked, connection issues)
/// and reconciliation races with exponential backoff.
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
    bulk_upsert_with_retry_inner(db, models, max_retries, initial_backoff_ms, true).await
}

/// Bulk upsert while retrying only transient database failures.
///
/// Unlike [`bulk_upsert_with_retry`], this does not retry an incomplete
/// repository reconciliation. Use it when the caller has already classified a
/// dependency component as incomplete; repeating the same deterministic input
/// only adds backoff latency. Database locks, deadlocks, serialization failures,
/// and other transient connection errors still use the configured retry policy.
pub(crate) async fn bulk_upsert_with_transient_retry(
    db: &DatabaseConnection,
    models: Vec<ActiveModel>,
    max_retries: u32,
    initial_backoff_ms: u64,
) -> Result<u64> {
    bulk_upsert_with_retry_inner(db, models, max_retries, initial_backoff_ms, false).await
}

async fn bulk_upsert_with_retry_inner(
    db: &DatabaseConnection,
    models: Vec<ActiveModel>,
    max_retries: u32,
    initial_backoff_ms: u64,
    retry_incomplete_reconciliation: bool,
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
                if is_retryable_error(&e, retry_incomplete_reconciliation) && attempt < max_retries
                {
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
fn is_retryable_error(err: &RepositoryError, retry_incomplete_reconciliation: bool) -> bool {
    match err {
        RepositoryError::Database(db_err) => is_retryable_db_error(db_err),
        RepositoryError::InvalidInput { message } => {
            retry_incomplete_reconciliation
                && message.starts_with(INCOMPLETE_RECONCILIATION_ERROR_PREFIX)
        }
        _ => false,
    }
}

fn is_retryable_db_error(err: &DbErr) -> bool {
    match err {
        DbErr::ConnectionAcquire(_) | DbErr::Conn(_) => true,
        DbErr::Exec(_) | DbErr::Query(_) => {
            let err_str = err.to_string().to_lowercase();
            // SQLite: database is locked, busy
            // PostgreSQL: connection refused, too many connections, concurrent
            // transaction deadlocks and serialization failures
            // General: timeout, connection reset
            err_str.contains("locked")
                || err_str.contains("busy")
                || err_str.contains("timeout")
                || err_str.contains("connection")
                || err_str.contains("temporarily unavailable")
                || err_str.contains("deadlock detected")
                || err_str.contains("40p01")
                || err_str.contains("could not serialize")
                || err_str.contains("serialization failure")
                || err_str.contains("40001")
        }
        _ => false,
    }
}

/// Build the ON CONFLICT clause used by bulk upsert.
///
/// Conflict detection uses (instance_id, platform_id) as the natural key.
/// Only updates rows where `updated_at`, owner, or name has changed, preventing
/// unnecessary writes while still persisting repository transfers and renames.
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
        // Update if content changed or the platform reports a repository rename.
        .action_and_where(
            Condition::any()
                .add(Expr::col((CodeRepository, Column::UpdatedAt)).is_null())
                .add(
                    Expr::col((CodeRepository, Column::UpdatedAt))
                        .ne(Expr::col((Alias::new("excluded"), Column::UpdatedAt))),
                )
                .add(
                    Expr::col((CodeRepository, Column::Owner))
                        .ne(Expr::col((Alias::new("excluded"), Column::Owner))),
                )
                .add(
                    Expr::col((CodeRepository, Column::Name))
                        .ne(Expr::col((Alias::new("excluded"), Column::Name))),
                )
                .into(),
        )
        .to_owned()
}

/// Collapse duplicate platform identities before generating a single INSERT.
///
/// PostgreSQL rejects a statement that would affect the same conflict target
/// more than once. Redirected repository aliases can resolve to the same
/// platform ID, so keep the last occurrence to match sequential upsert
/// semantics while preserving deterministic input order.
fn deduplicate_by_platform_id(models: Vec<ActiveModel>) -> Result<Vec<ActiveModel>> {
    let mut deduplicated = Vec::with_capacity(models.len());
    let mut positions = HashMap::with_capacity(models.len());

    for model in models {
        let instance_id = required_active_value("instance_id", &model.instance_id)?;
        let platform_id = required_active_value("platform_id", &model.platform_id)?;
        let identity = (instance_id, platform_id);

        if let Some(index) = positions.get(&identity).copied() {
            deduplicated[index] = model;
        } else {
            positions.insert(identity, deduplicated.len());
            deduplicated.push(model);
        }
    }

    Ok(deduplicated)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PlatformIdentity {
    instance_id: Uuid,
    platform_id: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct NaturalKey {
    instance_id: Uuid,
    owner: String,
    name: String,
}

#[derive(Clone, Debug)]
struct UpsertKey {
    identity: PlatformIdentity,
    natural_key: NaturalKey,
}

fn upsert_keys(models: &[ActiveModel]) -> Result<Vec<UpsertKey>> {
    models
        .iter()
        .map(|model| {
            Ok(UpsertKey {
                identity: PlatformIdentity {
                    instance_id: required_active_value("instance_id", &model.instance_id)?,
                    platform_id: required_active_value("platform_id", &model.platform_id)?,
                },
                natural_key: NaturalKey {
                    instance_id: required_active_value("instance_id", &model.instance_id)?,
                    owner: required_active_value("owner", &model.owner)?,
                    name: required_active_value("name", &model.name)?,
                },
            })
        })
        .collect()
}

async fn find_natural_key_conflicts<C>(
    db: &C,
    upsert_keys: &[UpsertKey],
) -> Result<Vec<crate::entity::code_repository::Model>>
where
    C: ConnectionTrait,
{
    if upsert_keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut sorted_keys: Vec<_> = upsert_keys.iter().collect();
    sorted_keys.sort_unstable_by(|left, right| {
        (
            left.natural_key.instance_id,
            &left.natural_key.owner,
            &left.natural_key.name,
            left.identity.platform_id,
        )
            .cmp(&(
                right.natural_key.instance_id,
                &right.natural_key.owner,
                &right.natural_key.name,
                right.identity.platform_id,
            ))
    });

    let mut conflicting_rows = HashMap::new();
    for keys in sorted_keys.chunks(NATURAL_KEY_CONFLICT_SCAN_SIZE) {
        let mut conflicts = Condition::any();
        for key in keys {
            conflicts = conflicts.add(
                Condition::all()
                    .add(Column::InstanceId.eq(key.natural_key.instance_id))
                    .add(Column::Owner.eq(key.natural_key.owner.clone()))
                    .add(Column::Name.eq(key.natural_key.name.clone()))
                    .add(Column::PlatformId.ne(key.identity.platform_id)),
            );
        }

        for row in CodeRepository::find()
            .filter(conflicts)
            .order_by_asc(Column::Id)
            .lock(LockType::Update)
            .all(db)
            .await?
        {
            conflicting_rows.entry(row.id).or_insert(row);
        }
    }

    let mut conflicting_rows: Vec<_> = conflicting_rows.into_values().collect();
    conflicting_rows.sort_unstable_by_key(|row| row.id);
    Ok(conflicting_rows)
}

/// Split a streaming persistence batch into rows that are safe to write now
/// and rows that must wait for a later batch.
///
/// A row is deferred when its desired owner/name is occupied by a platform
/// identity that is not present in this batch. Deferral propagates backwards
/// through rename chains: if A needs B's current name and B is deferred, A
/// must be deferred too. Cycles whose members are all present remain ready and
/// are reconciled atomically by [`bulk_upsert`].
pub(crate) async fn partition_deferred_upserts(
    db: &DatabaseConnection,
    models: Vec<ActiveModel>,
) -> Result<(Vec<ActiveModel>, Vec<ActiveModel>)> {
    let models = deduplicate_by_platform_id(models)?;
    if models.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let keys = upsert_keys(&models)?;
    let positions_by_identity: HashMap<PlatformIdentity, usize> = keys
        .iter()
        .enumerate()
        .map(|(index, key)| (key.identity.clone(), index))
        .collect();
    let occupants_by_natural_key: HashMap<NaturalKey, PlatformIdentity> =
        find_natural_key_conflicts(db, &keys)
            .await?
            .into_iter()
            .map(|row| {
                (
                    NaturalKey {
                        instance_id: row.instance_id,
                        owner: row.owner,
                        name: row.name,
                    },
                    PlatformIdentity {
                        instance_id: row.instance_id,
                        platform_id: row.platform_id,
                    },
                )
            })
            .collect();

    let mut deferred = vec![false; models.len()];
    let mut dependencies = vec![None; models.len()];

    for (index, key) in keys.iter().enumerate() {
        let Some(occupant) = occupants_by_natural_key.get(&key.natural_key) else {
            continue;
        };

        if let Some(occupant_index) = positions_by_identity.get(occupant).copied() {
            dependencies[index] = Some(occupant_index);
        } else {
            deferred[index] = true;
        }
    }

    loop {
        let mut changed = false;
        for index in 0..deferred.len() {
            if !deferred[index]
                && dependencies[index].is_some_and(|dependency| deferred[dependency])
            {
                deferred[index] = true;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    let mut ready_models = Vec::with_capacity(models.len());
    let mut deferred_models = Vec::new();
    for (index, model) in models.into_iter().enumerate() {
        if deferred[index] {
            deferred_models.push(model);
        } else {
            ready_models.push(model);
        }
    }

    Ok((ready_models, deferred_models))
}

const RECONCILIATION_OWNER_PREFIX: &str = "__curator_reconcile__/";

fn reconciliation_owner(marker_id: Uuid) -> String {
    format!("{RECONCILIATION_OWNER_PREFIX}{marker_id}")
}

/// Move stale rows away from an incoming repository's current name.
///
/// A repository's platform ID is its durable identity, but owner/name is also
/// unique in the database. Rename chains can therefore require one repository
/// to take a name that is still attached to another repository's stale row.
///
/// Conflicting rows are assigned a random, reserved marker instead of being
/// deleted. The marker only exists inside the caller's transaction: displaced
/// rows that arrive in this batch retain their UUID. If a displaced row does
/// not arrive, the transaction is rejected rather than guessing that the
/// repository was deleted.
async fn reconcile_natural_key_conflicts<C>(
    db: &C,
    models: &[ActiveModel],
    reconciliation_marker_id: Uuid,
) -> Result<u64>
where
    C: ConnectionTrait,
{
    let upsert_keys = upsert_keys(models)?;
    let conflicting_rows = find_natural_key_conflicts(db, &upsert_keys).await?;

    let mut rows_affected = 0;
    for row in conflicting_rows {
        let id = row.id;
        let mut parked: ActiveModel = row.into();
        // This conventional reserved prefix plus a random UUID makes collision
        // with a real platform namespace negligible. The value is never
        // committed: the complete batch replaces it, or the transaction rolls
        // back.
        parked.owner = Set(reconciliation_owner(reconciliation_marker_id));
        parked.name = Set(id.to_string());
        parked.update(db).await?;
        rows_affected += 1;
    }

    Ok(rows_affected)
}

/// Reject a rename batch that did not include every displaced repository.
///
/// Absence from a persistence batch is not proof that a repository was deleted:
/// activity filtering and partial repository lists can both omit existing
/// repositories. Leaving this check inside the transaction guarantees that an
/// incomplete rename rolls back to the original owner/name values.
async fn ensure_reconciliation_complete<C>(db: &C, reconciliation_marker_id: Uuid) -> Result<()>
where
    C: ConnectionTrait,
{
    let unresolved = CodeRepository::find()
        .filter(Column::Owner.eq(reconciliation_owner(reconciliation_marker_id)))
        .one(db)
        .await?;

    if let Some(row) = unresolved {
        return Err(RepositoryError::InvalidInput {
            message: format!(
                "{INCOMPLETE_RECONCILIATION_ERROR_PREFIX} platform repository {} was displaced but its current identity was not included in the batch",
                row.platform_id
            ),
        });
    }

    Ok(())
}

/// Internal bulk upsert implementation.
async fn bulk_upsert_inner(db: &DatabaseConnection, mut models: Vec<ActiveModel>) -> Result<u64> {
    if models.is_empty() {
        return Ok(0);
    }

    models = deduplicate_by_platform_id(models)?;
    let reconciliation_marker_id = Uuid::new_v4();
    let transaction = db.begin().await?;

    let reconciled_rows = match reconcile_natural_key_conflicts(
        &transaction,
        &models,
        reconciliation_marker_id,
    )
    .await
    {
        Ok(rows_affected) => rows_affected,
        Err(error) => {
            if let Err(rollback_error) = transaction.rollback().await {
                tracing::warn!(error = %rollback_error, "Failed to roll back repository reconciliation");
            }
            return Err(error);
        }
    };

    let mut rows_affected = 0;
    for models in models.chunks(BULK_UPSERT_STATEMENT_SIZE) {
        match CodeRepository::insert_many(models.to_vec())
            .on_conflict(build_upsert_on_conflict())
            .exec_without_returning(&transaction)
            .await
        {
            Ok(chunk_rows_affected) => rows_affected += chunk_rows_affected,
            Err(error) => {
                if let Err(rollback_error) = transaction.rollback().await {
                    tracing::warn!(error = %rollback_error, "Failed to roll back repository upsert");
                }
                return Err(RepositoryError::from(error));
            }
        }
    }

    if reconciled_rows > 0
        && let Err(error) =
            ensure_reconciliation_complete(&transaction, reconciliation_marker_id).await
    {
        if let Err(rollback_error) = transaction.rollback().await {
            tracing::warn!(error = %rollback_error, "Failed to roll back incomplete repository reconciliation");
        }
        return Err(error);
    }

    transaction.commit().await?;
    Ok(rows_affected)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use sea_orm::{DatabaseBackend, DbErr, MockDatabase, MockExecResult, Set, TryIntoModel};

    use crate::entity::code_visibility::CodeVisibility;

    use super::*;

    fn active_model(instance_id: Uuid, owner: &str, name: &str, platform_id: i64) -> ActiveModel {
        let now = Utc::now().fixed_offset();
        ActiveModel {
            id: Set(Uuid::new_v4()),
            instance_id: Set(instance_id),
            platform_id: Set(platform_id),
            owner: Set(owner.to_string()),
            name: Set(name.to_string()),
            description: Set(None),
            default_branch: Set("main".to_string()),
            topics: Set(serde_json::json!([])),
            primary_language: Set(None),
            license_spdx: Set(None),
            homepage: Set(None),
            visibility: Set(CodeVisibility::Public),
            is_fork: Set(false),
            is_mirror: Set(false),
            is_archived: Set(false),
            is_template: Set(false),
            is_empty: Set(false),
            stars: Set(None),
            forks: Set(None),
            open_issues: Set(None),
            watchers: Set(None),
            size_kb: Set(None),
            has_issues: Set(true),
            has_wiki: Set(true),
            has_pull_requests: Set(true),
            created_at: Set(Some(now)),
            updated_at: Set(Some(now)),
            pushed_at: Set(Some(now)),
            platform_metadata: Set(serde_json::json!({})),
            synced_at: Set(now),
            etag: Set(None),
        }
    }

    fn stored_model(
        instance_id: Uuid,
        owner: &str,
        name: &str,
        platform_id: i64,
    ) -> crate::entity::code_repository::Model {
        active_model(instance_id, owner, name, platform_id)
            .try_into_model()
            .expect("fully populated active model")
    }

    #[tokio::test]
    async fn insert_many_returns_zero_for_empty_input() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite).into_connection();
        let count = insert_many(&db, Vec::new()).await.expect("should succeed");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn insert_many_returns_model_count_for_non_empty_input() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_exec_results([MockExecResult {
                rows_affected: 2,
                last_insert_id: 0,
            }])
            .into_connection();

        let instance_id = Uuid::new_v4();
        let models = vec![
            active_model(instance_id, "org", "a", 1),
            active_model(instance_id, "org", "b", 2),
        ];
        let count = insert_many(&db, models)
            .await
            .expect("insert_many should succeed");
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn bulk_upsert_returns_zero_for_empty_input() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite).into_connection();
        let count = bulk_upsert(&db, Vec::new()).await.expect("should succeed");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn bulk_upsert_returns_rows_affected() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([Vec::<crate::entity::code_repository::Model>::new()])
            .append_exec_results([MockExecResult {
                rows_affected: 2,
                last_insert_id: 0,
            }])
            .into_connection();

        let instance_id = Uuid::new_v4();
        let models = vec![
            active_model(instance_id, "org", "a", 1),
            active_model(instance_id, "org", "b", 2),
        ];
        let count = bulk_upsert(&db, models)
            .await
            .expect("bulk_upsert should succeed");
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn bulk_upsert_with_retry_retries_transient_errors() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_errors([DbErr::Conn(sea_orm::RuntimeErr::Internal(
                "temporarily unavailable".to_string(),
            ))])
            .append_query_results([Vec::<crate::entity::code_repository::Model>::new()])
            .append_exec_results([MockExecResult {
                rows_affected: 1,
                last_insert_id: 0,
            }])
            .into_connection();

        let instance_id = Uuid::new_v4();
        let models = vec![active_model(instance_id, "org", "a", 1)];

        let count = bulk_upsert_with_retry(&db, models, 1, 0)
            .await
            .expect("should succeed after retry");
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn bulk_upsert_with_transient_retry_preserves_database_retries() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_errors([DbErr::Conn(sea_orm::RuntimeErr::Internal(
                "temporarily unavailable".to_string(),
            ))])
            .append_query_results([Vec::<crate::entity::code_repository::Model>::new()])
            .append_exec_results([MockExecResult {
                rows_affected: 1,
                last_insert_id: 0,
            }])
            .into_connection();

        let instance_id = Uuid::new_v4();
        let models = vec![active_model(instance_id, "org", "a", 1)];

        let count = bulk_upsert_with_transient_retry(&db, models, 1, 0)
            .await
            .expect("transient database error should still retry");
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn bulk_upsert_with_retry_retries_incomplete_concurrent_reconciliation() {
        let instance_id = Uuid::new_v4();
        let occupant = stored_model(instance_id, "org", "target", 2);
        let unresolved = occupant.clone();
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            // The first transaction observes and parks a conflicting row, but
            // another overlapping rename makes that snapshot incomplete.
            .append_query_results([
                vec![occupant.clone()],
                vec![occupant],
                vec![unresolved],
                Vec::<crate::entity::code_repository::Model>::new(),
            ])
            .append_exec_results([
                MockExecResult {
                    rows_affected: 1,
                    last_insert_id: 0,
                },
                MockExecResult {
                    rows_affected: 1,
                    last_insert_id: 0,
                },
            ])
            .into_connection();

        let models = vec![active_model(instance_id, "org", "target", 1)];

        let count = bulk_upsert_with_retry(&db, models, 1, 0)
            .await
            .expect("a fresh transaction should retry the concurrent reconciliation");

        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn bulk_upsert_chunks_large_rename_cycle_inside_one_transaction() {
        const REPOSITORY_COUNT: usize = 501;
        const EXPECTED_CONFLICT_SCAN_LIMIT: usize = 100;
        const EXPECTED_INSERT_LIMIT: usize = 500;

        let instance_id = Uuid::new_v4();
        let stored: Vec<_> = (0..REPOSITORY_COUNT)
            .map(|index| {
                stored_model(
                    instance_id,
                    "org",
                    &format!("repo-{index:04}"),
                    index as i64,
                )
            })
            .collect();
        let renamed: Vec<_> = (0..REPOSITORY_COUNT)
            .map(|index| {
                active_model(
                    instance_id,
                    "org",
                    &format!("repo-{:04}", (index + 1) % REPOSITORY_COUNT),
                    index as i64,
                )
            })
            .collect();

        let mut query_results: Vec<Vec<crate::entity::code_repository::Model>> = stored
            .chunks(EXPECTED_CONFLICT_SCAN_LIMIT)
            .map(<[_]>::to_vec)
            .collect();
        query_results.extend(stored.iter().cloned().map(|row| vec![row]));
        query_results.push(Vec::new());

        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results(query_results)
            .append_exec_results([
                MockExecResult {
                    rows_affected: EXPECTED_INSERT_LIMIT as u64,
                    last_insert_id: 0,
                },
                MockExecResult {
                    rows_affected: (REPOSITORY_COUNT - EXPECTED_INSERT_LIMIT) as u64,
                    last_insert_id: 0,
                },
            ])
            .into_connection();

        let count = bulk_upsert(&db, renamed)
            .await
            .expect("a large complete rename cycle should be committed atomically");
        assert_eq!(count, REPOSITORY_COUNT as u64);

        let transaction_log = db.into_transaction_log();
        assert_eq!(
            transaction_log.len(),
            1,
            "the complete rename cycle must use one transaction"
        );
        let statements = transaction_log[0].statements();
        let conflict_scans: Vec<_> = statements
            .iter()
            .filter(|statement| statement.sql.contains("FOR UPDATE"))
            .collect();
        assert_eq!(
            conflict_scans.len(),
            REPOSITORY_COUNT.div_ceil(EXPECTED_CONFLICT_SCAN_LIMIT)
        );
        assert!(conflict_scans.iter().all(|statement| {
            statement
                .values
                .as_ref()
                .is_some_and(|values| values.0.len() <= EXPECTED_CONFLICT_SCAN_LIMIT * 4)
        }));

        let inserts: Vec<_> = statements
            .iter()
            .filter(|statement| statement.sql.starts_with("INSERT INTO"))
            .collect();
        assert_eq!(
            inserts.len(),
            REPOSITORY_COUNT.div_ceil(EXPECTED_INSERT_LIMIT)
        );
        let inserted_rows: Vec<_> = inserts
            .iter()
            .map(|statement| {
                let values_clause = statement
                    .sql
                    .split_once(" ON CONFLICT")
                    .expect("bulk insert should have an ON CONFLICT clause")
                    .0;
                values_clause.matches("), (").count() + 1
            })
            .collect();
        assert_eq!(inserted_rows.iter().sum::<usize>(), REPOSITORY_COUNT);
        assert!(
            inserted_rows
                .iter()
                .all(|row_count| *row_count <= EXPECTED_INSERT_LIMIT)
        );

        let first_insert = statements
            .iter()
            .position(|statement| statement.sql.starts_with("INSERT INTO"))
            .expect("bulk upsert should issue inserts");
        let last_conflict_scan = statements
            .iter()
            .rposition(|statement| statement.sql.contains("FOR UPDATE"))
            .expect("rename reconciliation should scan conflicts");
        assert!(
            last_conflict_scan < first_insert,
            "every conflict must be locked before any rename is inserted"
        );
        let reconciliation_updates: Vec<_> = statements
            .iter()
            .enumerate()
            .filter(|(_, statement)| statement.sql.starts_with("UPDATE \"code_repositories\""))
            .collect();
        assert_eq!(reconciliation_updates.len(), REPOSITORY_COUNT);
        assert!(
            reconciliation_updates
                .last()
                .is_some_and(|(index, _)| *index < first_insert),
            "every conflict must be parked before any rename is inserted"
        );
    }

    #[test]
    fn postgres_deadlock_and_serialization_errors_are_retryable() {
        for message in [
            "deadlock detected (SQLSTATE 40P01)",
            "could not serialize access due to concurrent update (SQLSTATE 40001)",
        ] {
            let error = DbErr::Query(sea_orm::RuntimeErr::Internal(message.to_string()));
            assert!(is_retryable_db_error(&error), "{message}");
        }
    }

    #[tokio::test]
    async fn conflict_query_locks_rows_in_deterministic_order_on_postgres() {
        let instance_id = Uuid::new_v4();
        let models = vec![active_model(instance_id, "org", "target", 1)];
        let keys = upsert_keys(&models).expect("fully populated active model");
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([Vec::<crate::entity::code_repository::Model>::new()])
            .into_connection();

        find_natural_key_conflicts(&db, &keys)
            .await
            .expect("conflict query should succeed");

        let log = format!("{:?}", db.into_transaction_log());
        assert!(log.contains("ORDER BY"));
        assert!(log.contains("FOR UPDATE"));
    }

    #[tokio::test]
    async fn partition_deferred_upserts_propagates_blocked_rename_chains() {
        let instance_id = Uuid::new_v4();
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([vec![
                stored_model(instance_id, "org", "b", 2),
                stored_model(instance_id, "org", "c", 3),
            ]])
            .into_connection();

        let models = vec![
            active_model(instance_id, "org", "b", 1),
            active_model(instance_id, "org", "c", 2),
        ];
        let (ready, deferred) = partition_deferred_upserts(&db, models)
            .await
            .expect("partition should succeed");

        assert!(ready.is_empty());
        assert_eq!(deferred.len(), 2);
        assert_eq!(
            required_active_value("platform_id", &deferred[0].platform_id).unwrap(),
            1
        );
        assert_eq!(
            required_active_value("platform_id", &deferred[1].platform_id).unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn partition_deferred_upserts_keeps_complete_cycles_ready() {
        let instance_id = Uuid::new_v4();
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([vec![
                stored_model(instance_id, "org", "a", 1),
                stored_model(instance_id, "org", "b", 2),
            ]])
            .into_connection();

        let models = vec![
            active_model(instance_id, "org", "b", 1),
            active_model(instance_id, "org", "a", 2),
        ];
        let (ready, deferred) = partition_deferred_upserts(&db, models)
            .await
            .expect("partition should succeed");

        assert_eq!(ready.len(), 2);
        assert!(deferred.is_empty());
    }

    #[tokio::test]
    async fn partition_deferred_upserts_deduplicates_before_querying() {
        let instance_id = Uuid::new_v4();
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([Vec::<crate::entity::code_repository::Model>::new()])
            .into_connection();

        let models = vec![
            active_model(instance_id, "org", "old-name", 1),
            active_model(instance_id, "org", "canonical-name", 1),
            active_model(instance_id, "org", "other", 2),
        ];
        let (ready, deferred) = partition_deferred_upserts(&db, models)
            .await
            .expect("partition should succeed");

        assert_eq!(ready.len(), 2);
        assert!(deferred.is_empty());
        assert_eq!(
            required_active_value("name", &ready[0].name).unwrap(),
            "canonical-name"
        );
        assert_eq!(
            required_active_value("platform_id", &ready[1].platform_id).unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn delete_many_returns_zero_for_empty_input() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite).into_connection();
        let deleted = delete_many(&db, Vec::new()).await.expect("should succeed");
        assert_eq!(deleted, 0);
    }

    #[tokio::test]
    async fn delete_many_returns_rows_affected() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_exec_results([MockExecResult {
                rows_affected: 3,
                last_insert_id: 0,
            }])
            .into_connection();

        let deleted = delete_many(&db, vec![Uuid::new_v4(), Uuid::new_v4()])
            .await
            .expect("delete_many should succeed");
        assert_eq!(deleted, 3);
    }

    #[tokio::test]
    async fn delete_by_instance_returns_rows_affected() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_exec_results([MockExecResult {
                rows_affected: 5,
                last_insert_id: 0,
            }])
            .into_connection();
        let deleted = delete_by_instance(&db, Uuid::new_v4())
            .await
            .expect("delete_by_instance should succeed");
        assert_eq!(deleted, 5);
    }

    #[tokio::test]
    async fn delete_by_owner_name_batches_in_chunks() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_exec_results([
                MockExecResult {
                    rows_affected: 100,
                    last_insert_id: 0,
                },
                MockExecResult {
                    rows_affected: 1,
                    last_insert_id: 0,
                },
            ])
            .into_connection();

        let instance_id = Uuid::new_v4();
        let repos: Vec<(String, String)> = (0..101)
            .map(|i| ("org".to_string(), format!("repo-{i}")))
            .collect();

        let deleted = delete_by_owner_name(&db, instance_id, &repos)
            .await
            .expect("delete_by_owner_name should succeed");
        assert_eq!(deleted, 101);
    }
}
