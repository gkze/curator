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
    let total_pages = total.div_ceil(pagination.per_page);
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
    let total_pages = total.div_ceil(pagination.per_page);
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
    let total_pages = total.div_ceil(pagination.per_page);
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

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use sea_orm::sea_query::Value;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use std::collections::BTreeMap;

    use crate::entity::code_visibility::CodeVisibility;

    use super::*;

    fn count_row(count: u64) -> BTreeMap<String, Value> {
        let mut row = BTreeMap::new();
        // SeaORM decodes paginator/count results from the `num_items` column.
        // For SQLite (and most backends), SeaORM reads this as an i32.
        row.insert(
            "num_items".to_string(),
            Value::Int(Some(count.try_into().unwrap_or(i32::MAX))),
        );
        row
    }

    fn sync_info_row(
        platform_id: i64,
        name: &str,
        updated_at: Option<chrono::DateTime<chrono::FixedOffset>>,
        pushed_at: Option<chrono::DateTime<chrono::FixedOffset>>,
        synced_at: chrono::DateTime<chrono::FixedOffset>,
    ) -> BTreeMap<String, Value> {
        let mut row = BTreeMap::new();
        row.insert("0".to_string(), Value::BigInt(Some(platform_id)));
        row.insert(
            "1".to_string(),
            Value::String(Some(Box::new(name.to_string()))),
        );
        row.insert(
            "2".to_string(),
            Value::ChronoDateTimeWithTimeZone(updated_at.map(Box::new)),
        );
        row.insert(
            "3".to_string(),
            Value::ChronoDateTimeWithTimeZone(pushed_at.map(Box::new)),
        );
        row.insert(
            "4".to_string(),
            Value::ChronoDateTimeWithTimeZone(Some(Box::new(synced_at))),
        );
        row
    }

    fn model(instance_id: Uuid, owner: &str, name: &str, platform_id: i64) -> Model {
        Model {
            id: Uuid::new_v4(),
            instance_id,
            platform_id,
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
    async fn find_all_returns_items_and_total_pages() {
        let instance_id = Uuid::new_v4();
        let items = vec![
            model(instance_id, "a", "one", 1),
            model(instance_id, "a", "two", 2),
        ];

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            // num_items()
            .append_query_results([vec![count_row(3)]])
            // fetch_page()
            .append_query_results([items.clone()])
            .into_connection();

        let result = find_all(&db, Pagination::new(0, 2))
            .await
            .expect("find_all should succeed");

        assert_eq!(result.total, 3);
        assert_eq!(result.total_pages, 2);
        assert_eq!(result.page, 0);
        assert_eq!(result.per_page, 2);
        assert_eq!(result.items, items);
    }

    #[tokio::test]
    async fn find_by_instance_filters_and_paginates() {
        let instance_id = Uuid::new_v4();
        let items = vec![model(instance_id, "org", "repo", 1)];

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([vec![count_row(1)]])
            .append_query_results([items.clone()])
            .into_connection();

        let result = find_by_instance(&db, instance_id, Pagination::new(0, 50))
            .await
            .expect("query should succeed");
        assert_eq!(result.total, 1);
        assert_eq!(result.total_pages, 1);
        assert_eq!(result.items, items);
    }

    #[tokio::test]
    async fn find_by_owner_filters_and_paginates() {
        let instance_id = Uuid::new_v4();
        let items = vec![model(instance_id, "org", "repo", 1)];

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([vec![count_row(1)]])
            .append_query_results([items.clone()])
            .into_connection();

        let result = find_by_owner(&db, "org", Pagination::new(0, 50))
            .await
            .expect("query should succeed");
        assert_eq!(result.total, 1);
        assert_eq!(result.total_pages, 1);
        assert_eq!(result.items, items);
    }

    #[tokio::test]
    async fn find_stale_returns_results() {
        let instance_id = Uuid::new_v4();
        let items = vec![model(instance_id, "org", "old", 1)];

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([items.clone()])
            .into_connection();

        let stale = find_stale(&db, Utc::now(), 10)
            .await
            .expect("find_stale should succeed");
        assert_eq!(stale, items);
    }

    #[tokio::test]
    async fn count_and_count_by_instance_return_expected_values() {
        let instance_id = Uuid::new_v4();
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([vec![count_row(42)]])
            .append_query_results([vec![count_row(7)]])
            .into_connection();

        let total = count(&db).await.expect("count succeeds");
        assert_eq!(total, 42);

        let total = count_by_instance(&db, instance_id)
            .await
            .expect("count_by_instance succeeds");
        assert_eq!(total, 7);
    }

    #[tokio::test]
    async fn find_all_by_instance_and_owner_queries_return_models() {
        let instance_id = Uuid::new_v4();
        let all_items = vec![
            model(instance_id, "org", "a", 1),
            model(instance_id, "org", "b", 2),
        ];
        let owner_items = vec![model(instance_id, "org", "a", 1)];

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([all_items.clone()])
            .append_query_results([owner_items.clone()])
            .into_connection();

        let got = find_all_by_instance(&db, instance_id)
            .await
            .expect("find_all_by_instance succeeds");
        assert_eq!(got, all_items);

        let got = find_all_by_instance_and_owner(&db, instance_id, "org")
            .await
            .expect("find_all_by_instance_and_owner succeeds");
        assert_eq!(got, owner_items);
    }

    #[tokio::test]
    async fn get_sync_info_by_instance_and_owner_maps_fixed_offset_to_utc() {
        let instance_id = Uuid::new_v4();
        let synced_at = Utc::now().fixed_offset();
        let rows = vec![sync_info_row(99, "repo", None, None, synced_at)];

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([rows])
            .into_connection();

        let info = get_sync_info_by_instance_and_owner(&db, instance_id, "org")
            .await
            .expect("query should succeed");
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].platform_id, 99);
        assert_eq!(info[0].name, "repo");
        assert!(info[0].updated_at.is_none());
        assert!(info[0].pushed_at.is_none());
    }
}

#[cfg(all(test, feature = "sqlite", feature = "migrate"))]
mod integration_tests {
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
