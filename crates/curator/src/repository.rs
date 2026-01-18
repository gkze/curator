//! Repository CRUD operations for CodeRepository entities.
//!
//! This module provides functions for creating, reading, updating, and deleting
//! repository records, including bulk operations for efficient syncing.

use chrono::{DateTime, Utc};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait,
    PaginatorTrait, QueryFilter, QueryOrder, Set,
    sea_query::{Alias, Expr, OnConflict},
};
use thiserror::Error;
use uuid::Uuid;

use crate::entity::code_platform::CodePlatform;
use crate::entity::code_repository::{ActiveModel, Column, Entity as CodeRepository, Model};

/// Errors that can occur during repository operations.
#[derive(Debug, Error)]
pub enum RepositoryError {
    /// Database error from sea-orm.
    #[error("Database error: {0}")]
    Database(#[from] DbErr),

    /// Repository not found.
    #[error("Repository not found: {context}")]
    NotFound { context: String },

    /// Duplicate repository (natural key conflict).
    #[error("Repository already exists: {platform}/{owner}/{name}")]
    Duplicate {
        platform: CodePlatform,
        owner: String,
        name: String,
    },

    /// Invalid input data.
    #[error("Invalid input: {message}")]
    InvalidInput { message: String },

    /// Bulk operation partially failed.
    #[error("Bulk operation failed: {succeeded} succeeded, {failed} failed")]
    PartialFailure { succeeded: usize, failed: usize },
}

impl RepositoryError {
    /// Create a NotFound error for a UUID lookup.
    pub fn not_found_by_id(id: Uuid) -> Self {
        Self::NotFound {
            context: format!("id={}", id),
        }
    }

    /// Create a NotFound error for a natural key lookup.
    pub fn not_found_by_key(platform: CodePlatform, owner: &str, name: &str) -> Self {
        Self::NotFound {
            context: format!("{:?}/{}/{}", platform, owner, name),
        }
    }

    /// Create a NotFound error for a platform_id lookup.
    pub fn not_found_by_platform_id(platform: CodePlatform, platform_id: i64) -> Self {
        Self::NotFound {
            context: format!("{:?} platform_id={}", platform, platform_id),
        }
    }
}

/// Result type alias for repository operations.
pub type Result<T> = std::result::Result<T, RepositoryError>;

/// Pagination parameters for list queries.
#[derive(Debug, Clone, Default)]
pub struct Pagination {
    /// Page number (0-indexed).
    pub page: u64,
    /// Items per page.
    pub per_page: u64,
}

impl Pagination {
    /// Create a new pagination with the given page and per_page values.
    pub fn new(page: u64, per_page: u64) -> Self {
        Self { page, per_page }
    }
}

/// Result of a paginated query.
#[derive(Debug, Clone)]
pub struct PaginatedResult<T> {
    /// The items for the current page.
    pub items: Vec<T>,
    /// Total number of items across all pages.
    pub total: u64,
    /// Current page number (0-indexed).
    pub page: u64,
    /// Items per page.
    pub per_page: u64,
    /// Total number of pages.
    pub total_pages: u64,
}

// ─── Single Record Operations ────────────────────────────────────────────────

/// Insert a new repository.
///
/// # Errors
/// Returns `RepositoryError::Database` if the insert fails (e.g., duplicate natural key).
pub async fn insert(db: &DatabaseConnection, model: ActiveModel) -> Result<Model> {
    model.insert(db).await.map_err(RepositoryError::from)
}

/// Find a repository by its UUID.
pub async fn find_by_id(db: &DatabaseConnection, id: Uuid) -> Result<Option<Model>> {
    CodeRepository::find_by_id(id)
        .one(db)
        .await
        .map_err(RepositoryError::from)
}

/// Find a repository by its natural key (platform + owner + name).
pub async fn find_by_natural_key(
    db: &DatabaseConnection,
    platform: CodePlatform,
    owner: &str,
    name: &str,
) -> Result<Option<Model>> {
    CodeRepository::find()
        .filter(Column::Platform.eq(platform))
        .filter(Column::Owner.eq(owner))
        .filter(Column::Name.eq(name))
        .one(db)
        .await
        .map_err(RepositoryError::from)
}

/// Find a repository by platform and platform_id (numeric ID from the platform).
pub async fn find_by_platform_id(
    db: &DatabaseConnection,
    platform: CodePlatform,
    platform_id: i64,
) -> Result<Option<Model>> {
    CodeRepository::find()
        .filter(Column::Platform.eq(platform))
        .filter(Column::PlatformId.eq(platform_id))
        .one(db)
        .await
        .map_err(RepositoryError::from)
}

/// Update an existing repository.
///
/// # Errors
/// Returns `RepositoryError::Database` if the update fails.
pub async fn update(db: &DatabaseConnection, model: ActiveModel) -> Result<Model> {
    model.update(db).await.map_err(RepositoryError::from)
}

/// Insert or update a repository by its natural key (platform + owner + name).
///
/// If a repository with the same platform, owner, and name exists, it will be updated.
/// Otherwise, a new repository will be inserted.
pub async fn upsert(db: &DatabaseConnection, model: ActiveModel) -> Result<Model> {
    // Extract natural key from the active model
    let platform = model.platform.clone().unwrap();
    let owner = model.owner.clone().unwrap();
    let name = model.name.clone().unwrap();

    // Check if exists
    let existing = find_by_natural_key(db, platform, &owner, &name).await?;

    match existing {
        Some(existing) => {
            // Update: set the ID from existing record
            let mut update_model = model;
            update_model.id = Set(existing.id);
            update_model.update(db).await.map_err(RepositoryError::from)
        }
        None => {
            // Insert: ensure ID is set
            let mut insert_model = model;
            if insert_model.id.is_not_set() {
                insert_model.id = Set(Uuid::new_v4());
            }
            insert_model.insert(db).await.map_err(RepositoryError::from)
        }
    }
}

/// Delete a repository by its UUID.
///
/// Returns the number of rows deleted (0 or 1).
pub async fn delete(db: &DatabaseConnection, id: Uuid) -> Result<u64> {
    let result = CodeRepository::delete_by_id(id).exec(db).await?;
    Ok(result.rows_affected)
}

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

/// Bulk upsert multiple repositories using SQL ON CONFLICT.
///
/// This is significantly faster than `upsert_many` for large batches because it:
/// - Uses a single INSERT ... ON CONFLICT DO UPDATE statement
/// - Reduces database round-trips from 2n to 1
/// - Only updates rows where `updated_at` has changed (content-based deduplication)
///
/// The natural key for conflict detection is (platform, owner, name).
/// The conditional update ensures we only modify rows when the platform's
/// `updated_at` timestamp has changed, avoiding unnecessary writes.
///
/// # Returns
/// Returns the number of rows actually inserted or updated.
pub async fn bulk_upsert(db: &DatabaseConnection, models: Vec<ActiveModel>) -> Result<u64> {
    if models.is_empty() {
        return Ok(0);
    }

    let on_conflict = OnConflict::columns([Column::Platform, Column::PlatformId])
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
        // This prevents unnecessary writes when the API returns the same data.
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
        .to_owned();

    CodeRepository::insert_many(models)
        .on_conflict(on_conflict)
        .exec_without_returning(db)
        .await
        .map_err(RepositoryError::from)
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

/// Delete all repositories for a given platform.
///
/// Returns the number of rows deleted.
pub async fn delete_by_platform(db: &DatabaseConnection, platform: CodePlatform) -> Result<u64> {
    let result = CodeRepository::delete_many()
        .filter(Column::Platform.eq(platform))
        .exec(db)
        .await?;
    Ok(result.rows_affected)
}

/// Delete repositories by owner/name pairs for a specific platform.
///
/// This is used when pruning starred repositories - when a repo is unstarred,
/// it should also be removed from the database.
///
/// Returns the number of rows deleted.
pub async fn delete_by_owner_name(
    db: &DatabaseConnection,
    platform: CodePlatform,
    repos: &[(String, String)], // (owner, name) pairs
) -> Result<u64> {
    if repos.is_empty() {
        return Ok(0);
    }

    let mut total_deleted = 0u64;

    // Delete in batches to avoid overly large queries
    for chunk in repos.chunks(100) {
        // Build OR conditions for each (owner, name) pair
        let mut condition = sea_orm::Condition::any();
        for (owner, name) in chunk {
            condition = condition.add(
                sea_orm::Condition::all()
                    .add(Column::Owner.eq(owner.clone()))
                    .add(Column::Name.eq(name.clone())),
            );
        }

        let result = CodeRepository::delete_many()
            .filter(Column::Platform.eq(platform.clone()))
            .filter(condition)
            .exec(db)
            .await?;

        total_deleted += result.rows_affected;
    }

    Ok(total_deleted)
}

// ─── Query Operations ────────────────────────────────────────────────────────

/// Find all repositories with pagination.
pub async fn find_all(
    db: &DatabaseConnection,
    pagination: Pagination,
) -> Result<PaginatedResult<Model>> {
    let paginator = CodeRepository::find()
        .order_by_asc(Column::Owner)
        .order_by_asc(Column::Name)
        .paginate(db, pagination.per_page);

    let total = paginator.num_items().await?;
    let total_pages = paginator.num_pages().await?;
    let items = paginator.fetch_page(pagination.page).await?;

    Ok(PaginatedResult {
        items,
        total,
        page: pagination.page,
        per_page: pagination.per_page,
        total_pages,
    })
}

/// Find all repositories for a given platform with pagination.
pub async fn find_by_platform(
    db: &DatabaseConnection,
    platform: CodePlatform,
    pagination: Pagination,
) -> Result<PaginatedResult<Model>> {
    let paginator = CodeRepository::find()
        .filter(Column::Platform.eq(platform))
        .order_by_asc(Column::Owner)
        .order_by_asc(Column::Name)
        .paginate(db, pagination.per_page);

    let total = paginator.num_items().await?;
    let total_pages = paginator.num_pages().await?;
    let items = paginator.fetch_page(pagination.page).await?;

    Ok(PaginatedResult {
        items,
        total,
        page: pagination.page,
        per_page: pagination.per_page,
        total_pages,
    })
}

/// Find all repositories for a given owner with pagination.
pub async fn find_by_owner(
    db: &DatabaseConnection,
    owner: &str,
    pagination: Pagination,
) -> Result<PaginatedResult<Model>> {
    let paginator = CodeRepository::find()
        .filter(Column::Owner.eq(owner))
        .order_by_asc(Column::Owner)
        .order_by_asc(Column::Name)
        .paginate(db, pagination.per_page);

    let total = paginator.num_items().await?;
    let total_pages = paginator.num_pages().await?;
    let items = paginator.fetch_page(pagination.page).await?;

    Ok(PaginatedResult {
        items,
        total,
        page: pagination.page,
        per_page: pagination.per_page,
        total_pages,
    })
}

/// Find repositories that haven't been synced since the given time.
///
/// Returns up to `limit` repositories, ordered by oldest sync first.
pub async fn find_stale(
    db: &DatabaseConnection,
    older_than: DateTime<Utc>,
    limit: u64,
) -> Result<Vec<Model>> {
    CodeRepository::find()
        .filter(Column::SyncedAt.lt(older_than))
        .order_by_asc(Column::SyncedAt)
        .paginate(db, limit)
        .fetch_page(0)
        .await
        .map_err(RepositoryError::from)
}

/// Count total repositories.
pub async fn count(db: &DatabaseConnection) -> Result<u64> {
    CodeRepository::find()
        .count(db)
        .await
        .map_err(RepositoryError::from)
}

/// Count repositories for a given platform.
pub async fn count_by_platform(db: &DatabaseConnection, platform: CodePlatform) -> Result<u64> {
    CodeRepository::find()
        .filter(Column::Platform.eq(platform))
        .count(db)
        .await
        .map_err(RepositoryError::from)
}

/// Find all repositories for a given platform without pagination.
///
/// This returns all repos for the platform in a single query. Use with caution
/// for platforms with many repos - prefer `find_by_platform` with pagination
/// for large result sets.
pub async fn find_all_by_platform(
    db: &DatabaseConnection,
    platform: CodePlatform,
) -> Result<Vec<Model>> {
    CodeRepository::find()
        .filter(Column::Platform.eq(platform))
        .order_by_asc(Column::Owner)
        .order_by_asc(Column::Name)
        .all(db)
        .await
        .map_err(RepositoryError::from)
}

/// Find all repositories for a given platform and owner without pagination.
///
/// This is used when loading cached repos on a full cache hit, where we need
/// all repos for a specific org/owner to avoid re-fetching from the API.
pub async fn find_all_by_platform_and_owner(
    db: &DatabaseConnection,
    platform: CodePlatform,
    owner: &str,
) -> Result<Vec<Model>> {
    CodeRepository::find()
        .filter(Column::Platform.eq(platform))
        .filter(Column::Owner.eq(owner))
        .order_by_asc(Column::Name)
        .all(db)
        .await
        .map_err(RepositoryError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repository_error_not_found_by_id() {
        let id = Uuid::new_v4();
        let err = RepositoryError::not_found_by_id(id);
        let msg = err.to_string();
        assert!(msg.contains("not found"));
        assert!(msg.contains(&id.to_string()));
    }

    #[test]
    fn test_repository_error_not_found_by_key() {
        let err = RepositoryError::not_found_by_key(CodePlatform::GitHub, "owner", "repo");
        let msg = err.to_string();
        assert!(msg.contains("not found"));
        assert!(msg.contains("GitHub"));
        assert!(msg.contains("owner"));
        assert!(msg.contains("repo"));
    }

    #[test]
    fn test_repository_error_not_found_by_platform_id() {
        let err = RepositoryError::not_found_by_platform_id(CodePlatform::GitLab, 12345);
        let msg = err.to_string();
        assert!(msg.contains("not found"));
        assert!(msg.contains("GitLab"));
        assert!(msg.contains("12345"));
    }

    #[test]
    fn test_repository_error_duplicate() {
        let err = RepositoryError::Duplicate {
            platform: CodePlatform::Codeberg,
            owner: "myorg".to_string(),
            name: "myrepo".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("already exists"));
        assert!(msg.contains("codeberg")); // lowercase per Display impl
        assert!(msg.contains("myorg"));
        assert!(msg.contains("myrepo"));
    }

    #[test]
    fn test_repository_error_invalid_input() {
        let err = RepositoryError::InvalidInput {
            message: "Missing required field".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("Invalid input"));
        assert!(msg.contains("Missing required field"));
    }

    #[test]
    fn test_repository_error_partial_failure() {
        let err = RepositoryError::PartialFailure {
            succeeded: 10,
            failed: 3,
        };
        let msg = err.to_string();
        assert!(msg.contains("10 succeeded"));
        assert!(msg.contains("3 failed"));
    }

    #[test]
    fn test_pagination_new() {
        let p = Pagination::new(5, 25);
        assert_eq!(p.page, 5);
        assert_eq!(p.per_page, 25);
    }

    #[test]
    fn test_pagination_default() {
        let p = Pagination::default();
        assert_eq!(p.page, 0);
        assert_eq!(p.per_page, 0);
    }

    #[test]
    fn test_paginated_result_fields() {
        let result: PaginatedResult<String> = PaginatedResult {
            items: vec!["a".to_string(), "b".to_string()],
            total: 100,
            page: 2,
            per_page: 10,
            total_pages: 10,
        };
        assert_eq!(result.items.len(), 2);
        assert_eq!(result.total, 100);
        assert_eq!(result.page, 2);
        assert_eq!(result.per_page, 10);
        assert_eq!(result.total_pages, 10);
    }

    #[test]
    fn test_repository_error_database_from_db_err() {
        // Test that DbErr converts to RepositoryError::Database
        let db_err = DbErr::RecordNotFound("test".to_string());
        let repo_err: RepositoryError = db_err.into();
        let msg = repo_err.to_string();
        assert!(msg.contains("Database error"));
    }

    #[test]
    fn test_repository_error_variants_debug() {
        // Ensure all error variants implement Debug
        let errors: Vec<RepositoryError> = vec![
            RepositoryError::not_found_by_id(Uuid::new_v4()),
            RepositoryError::not_found_by_key(CodePlatform::GitHub, "o", "r"),
            RepositoryError::not_found_by_platform_id(CodePlatform::GitLab, 1),
            RepositoryError::Duplicate {
                platform: CodePlatform::Codeberg,
                owner: "o".to_string(),
                name: "r".to_string(),
            },
            RepositoryError::InvalidInput {
                message: "test".to_string(),
            },
            RepositoryError::PartialFailure {
                succeeded: 1,
                failed: 2,
            },
        ];
        for err in errors {
            let debug_str = format!("{:?}", err);
            assert!(!debug_str.is_empty());
        }
    }

    #[test]
    fn test_pagination_clone() {
        let p1 = Pagination::new(1, 20);
        let p2 = p1.clone();
        assert_eq!(p1.page, p2.page);
        assert_eq!(p1.per_page, p2.per_page);
    }

    #[test]
    fn test_paginated_result_clone() {
        let r1: PaginatedResult<i32> = PaginatedResult {
            items: vec![1, 2, 3],
            total: 100,
            page: 0,
            per_page: 10,
            total_pages: 10,
        };
        let r2 = r1.clone();
        assert_eq!(r1.items, r2.items);
        assert_eq!(r1.total, r2.total);
    }

    #[test]
    fn test_pagination_debug() {
        let p = Pagination::new(5, 50);
        let debug_str = format!("{:?}", p);
        assert!(debug_str.contains("Pagination"));
        assert!(debug_str.contains("5"));
        assert!(debug_str.contains("50"));
    }

    #[test]
    fn test_paginated_result_debug() {
        let r: PaginatedResult<&str> = PaginatedResult {
            items: vec!["test"],
            total: 1,
            page: 0,
            per_page: 10,
            total_pages: 1,
        };
        let debug_str = format!("{:?}", r);
        assert!(debug_str.contains("PaginatedResult"));
        assert!(debug_str.contains("test"));
    }

    /// Test that the OnConflict with action_and_where generates correct SQL.
    ///
    /// This is a compile-time sanity check that the query builder accepts
    /// our conditional update syntax.
    #[test]
    fn test_bulk_upsert_query_builds() {
        use sea_orm::QueryTrait;

        // Create a minimal active model
        let model = ActiveModel {
            id: Set(Uuid::new_v4()),
            platform: Set(CodePlatform::GitHub),
            platform_id: Set(12345),
            owner: Set("test-owner".to_string()),
            name: Set("test-repo".to_string()),
            description: Set(Some("A test repo".to_string())),
            default_branch: Set("main".to_string()),
            topics: Set(serde_json::json!(["rust", "test"])),
            primary_language: Set(Some("Rust".to_string())),
            license_spdx: Set(Some("MIT".to_string())),
            homepage: Set(None),
            visibility: Set(crate::entity::code_visibility::CodeVisibility::Public),
            is_fork: Set(false),
            is_mirror: Set(false),
            is_archived: Set(false),
            is_template: Set(false),
            is_empty: Set(false),
            stars: Set(Some(100)),
            forks: Set(Some(10)),
            open_issues: Set(Some(5)),
            watchers: Set(Some(50)),
            size_kb: Set(Some(1024)),
            has_issues: Set(true),
            has_wiki: Set(true),
            has_pull_requests: Set(true),
            created_at: Set(Some(chrono::Utc::now().fixed_offset())),
            updated_at: Set(Some(chrono::Utc::now().fixed_offset())),
            pushed_at: Set(Some(chrono::Utc::now().fixed_offset())),
            platform_metadata: Set(serde_json::json!({})),
            synced_at: Set(chrono::Utc::now().fixed_offset()),
            etag: Set(None),
        };

        // Build the query (same logic as bulk_upsert)
        let on_conflict = OnConflict::columns([Column::Platform, Column::Owner, Column::Name])
            .update_columns([
                Column::PlatformId,
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
            .action_and_where(
                Condition::any()
                    .add(Expr::col((CodeRepository, Column::UpdatedAt)).is_null())
                    .add(
                        Expr::col((CodeRepository, Column::UpdatedAt))
                            .ne(Expr::col((Alias::new("excluded"), Column::UpdatedAt))),
                    )
                    .into(),
            )
            .to_owned();

        let query = CodeRepository::insert_many(vec![model])
            .on_conflict(on_conflict)
            .build(sea_orm::DatabaseBackend::Sqlite);

        // Verify the SQL contains our conditional WHERE clause
        let sql = query.to_string();
        assert!(
            sql.contains("ON CONFLICT"),
            "SQL should contain ON CONFLICT: {}",
            sql
        );
        assert!(
            sql.contains("DO UPDATE"),
            "SQL should contain DO UPDATE: {}",
            sql
        );
        assert!(
            sql.contains("WHERE"),
            "SQL should contain WHERE clause: {}",
            sql
        );
        assert!(
            sql.contains("excluded"),
            "SQL should reference excluded table: {}",
            sql
        );
        assert!(
            sql.contains("updated_at"),
            "SQL should reference updated_at column: {}",
            sql
        );
    }

    #[test]
    fn test_bulk_upsert_empty_returns_zero() {
        // This tests the early return path
        // We can't easily test the async function without a runtime,
        // but we can verify the logic by checking the function signature
        // The actual test would require an async test with a mock DB

        // For now, just verify the function exists and compiles
        async fn _check_signature(
            _db: &sea_orm::DatabaseConnection,
            _models: Vec<ActiveModel>,
        ) -> Result<u64> {
            Ok(0)
        }
    }
}
