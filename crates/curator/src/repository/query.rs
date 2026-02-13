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

#[cfg(all(test, feature = "sqlite", feature = "migrate"))]
mod tests {
    use chrono::{Duration, Utc};
    use sea_orm::{EntityTrait, Set};

    use crate::connect_and_migrate;
    use crate::entity::code_repository::ActiveModel;
    use crate::entity::code_visibility::CodeVisibility;
    use crate::entity::instance::{ActiveModel as InstanceActiveModel, Entity as Instance};
    use crate::entity::platform_type::PlatformType;
    use crate::repository;

    use super::*;

    fn test_instance_id() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000222").expect("valid uuid")
    }

    async fn setup_db() -> DatabaseConnection {
        let db = connect_and_migrate("sqlite::memory:")
            .await
            .expect("test db should migrate");

        let now = Utc::now().fixed_offset();
        let instance = InstanceActiveModel {
            id: Set(test_instance_id()),
            name: Set("query-test".to_string()),
            platform_type: Set(PlatformType::GitHub),
            host: Set("query-test.example.com".to_string()),
            oauth_client_id: Set(None),
            oauth_flow: Set("auto".to_string()),
            created_at: Set(now),
        };
        Instance::insert(instance)
            .exec(&db)
            .await
            .expect("instance should insert");

        db
    }

    fn model(
        owner: &str,
        name: &str,
        platform_id: i64,
        synced_at: chrono::DateTime<Utc>,
    ) -> ActiveModel {
        let fixed = synced_at.fixed_offset();
        ActiveModel {
            id: Set(Uuid::new_v4()),
            instance_id: Set(test_instance_id()),
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
            created_at: Set(Some(fixed)),
            updated_at: Set(Some(fixed)),
            pushed_at: Set(Some(fixed)),
            platform_metadata: Set(serde_json::json!({})),
            synced_at: Set(fixed),
            etag: Set(None),
        }
    }

    #[tokio::test]
    async fn test_find_stale_orders_oldest_first_and_honors_limit() {
        let db = setup_db().await;
        let now = Utc::now();
        repository::bulk_upsert(
            &db,
            vec![
                model("org", "oldest", 1, now - Duration::days(20)),
                model("org", "middle", 2, now - Duration::days(10)),
                model("org", "new", 3, now - Duration::days(1)),
            ],
        )
        .await
        .expect("seed data should insert");

        let stale = find_stale(&db, now - Duration::days(2), 2)
            .await
            .expect("find_stale should succeed");

        assert_eq!(stale.len(), 2);
        assert_eq!(stale[0].name, "oldest");
        assert_eq!(stale[1].name, "middle");
    }

    #[tokio::test]
    async fn test_get_sync_info_by_instance_and_owner_maps_nullable_timestamps() {
        let db = setup_db().await;
        let mut active = model("org", "active", 11, Utc::now());
        active.updated_at = Set(None);
        active.pushed_at = Set(None);

        repository::bulk_upsert(&db, vec![active])
            .await
            .expect("seed data should insert");

        let info = get_sync_info_by_instance_and_owner(&db, test_instance_id(), "org")
            .await
            .expect("sync info query should succeed");

        assert_eq!(info.len(), 1);
        assert_eq!(info[0].platform_id, 11);
        assert_eq!(info[0].name, "active");
        assert!(info[0].updated_at.is_none());
        assert!(info[0].pushed_at.is_none());
    }
}
