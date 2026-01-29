use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};
use uuid::Uuid;

use crate::entity::code_repository::{Column, Entity as CodeRepository, Model};

use super::errors::{RepositoryError, Result};

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

/// Find all repositories for a given instance with pagination.
pub async fn find_by_instance(
    db: &DatabaseConnection,
    instance_id: Uuid,
    pagination: Pagination,
) -> Result<PaginatedResult<Model>> {
    let paginator = CodeRepository::find()
        .filter(Column::InstanceId.eq(instance_id))
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

/// Count repositories for a given instance.
pub async fn count_by_instance(db: &DatabaseConnection, instance_id: Uuid) -> Result<u64> {
    CodeRepository::find()
        .filter(Column::InstanceId.eq(instance_id))
        .count(db)
        .await
        .map_err(RepositoryError::from)
}

/// Find all repositories for a given instance without pagination.
///
/// This returns all repos for the instance in a single query. Use with caution
/// for instances with many repos - prefer `find_by_instance` with pagination
/// for large result sets.
pub async fn find_all_by_instance(
    db: &DatabaseConnection,
    instance_id: Uuid,
) -> Result<Vec<Model>> {
    CodeRepository::find()
        .filter(Column::InstanceId.eq(instance_id))
        .order_by_asc(Column::Owner)
        .order_by_asc(Column::Name)
        .all(db)
        .await
        .map_err(RepositoryError::from)
}

/// Find all repositories for a given instance and owner without pagination.
///
/// This is used when loading cached repos on a full cache hit, where we need
/// all repos for a specific org/owner to avoid re-fetching from the API.
pub async fn find_all_by_instance_and_owner(
    db: &DatabaseConnection,
    instance_id: Uuid,
    owner: &str,
) -> Result<Vec<Model>> {
    CodeRepository::find()
        .filter(Column::InstanceId.eq(instance_id))
        .filter(Column::Owner.eq(owner))
        .order_by_asc(Column::Name)
        .all(db)
        .await
        .map_err(RepositoryError::from)
}
