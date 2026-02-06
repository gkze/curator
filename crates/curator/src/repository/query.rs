use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect,
};
use uuid::Uuid;

use crate::entity::code_repository::{Column, Entity as CodeRepository, Model};

use super::errors::{RepositoryError, Result};

/// Information about a repository's sync state used for incremental sync.
///
/// This struct contains the minimum data needed to determine if a repository
/// needs to be re-fetched during an incremental sync operation.
#[derive(Debug, Clone)]
pub struct RepoSyncInfo {
    /// The platform-specific numeric ID of the repository.
    pub platform_id: i64,
    /// Repository name.
    pub name: String,
    /// When the repository was last updated on the platform.
    pub updated_at: Option<DateTime<Utc>>,
    /// When code was last pushed (GitHub-specific, may be None for other platforms).
    pub pushed_at: Option<DateTime<Utc>>,
    /// When this record was last synced from the platform.
    pub synced_at: DateTime<Utc>,
}

/// Pagination parameters for list queries.
#[derive(Debug, Clone)]
pub struct Pagination {
    /// Page number (0-indexed).
    pub page: u64,
    /// Items per page.
    pub per_page: u64,
}

impl Pagination {
    /// Create a new pagination with the given page and per_page values.
    pub fn new(page: u64, per_page: u64) -> Self {
        Self {
            page,
            per_page: per_page.max(MIN_PER_PAGE),
        }
    }
}

const MIN_PER_PAGE: u64 = 1;

impl Default for Pagination {
    fn default() -> Self {
        Self {
            page: 0,
            per_page: MIN_PER_PAGE,
        }
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

/// Get sync information for all repositories in a given instance and owner.
///
/// This returns a lightweight projection of repository data needed for
/// incremental sync decisions. Each `RepoSyncInfo` contains the platform ID,
/// name, last update timestamps, and last sync time.
///
/// Used by the sync engine to determine which repositories need refreshing
/// based on comparing platform `updated_at`/`pushed_at` with stored `synced_at`.
pub async fn get_sync_info_by_instance_and_owner(
    db: &DatabaseConnection,
    instance_id: Uuid,
    owner: &str,
) -> Result<Vec<RepoSyncInfo>> {
    // Select only the columns we need for incremental sync decisions
    let results = CodeRepository::find()
        .filter(Column::InstanceId.eq(instance_id))
        .filter(Column::Owner.eq(owner))
        .select_only()
        .column(Column::PlatformId)
        .column(Column::Name)
        .column(Column::UpdatedAt)
        .column(Column::PushedAt)
        .column(Column::SyncedAt)
        .into_tuple::<(
            i64,
            String,
            Option<chrono::DateTime<chrono::FixedOffset>>,
            Option<chrono::DateTime<chrono::FixedOffset>>,
            chrono::DateTime<chrono::FixedOffset>,
        )>()
        .all(db)
        .await
        .map_err(RepositoryError::from)?;

    Ok(results
        .into_iter()
        .map(
            |(platform_id, name, updated_at, pushed_at, synced_at)| RepoSyncInfo {
                platform_id,
                name,
                updated_at: updated_at.map(|dt| dt.with_timezone(&Utc)),
                pushed_at: pushed_at.map(|dt| dt.with_timezone(&Utc)),
                synced_at: synced_at.with_timezone(&Utc),
            },
        )
        .collect())
}
