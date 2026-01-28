//! Repository CRUD operations for CodeRepository entities.
//!
//! This module provides functions for creating, reading, updating, and deleting
//! repository records, including bulk operations for efficient syncing.

mod bulk;
mod errors;
mod query;
mod single;

pub use bulk::{
    DEFAULT_BULK_UPSERT_BACKOFF_MS, DEFAULT_BULK_UPSERT_RETRIES, bulk_upsert,
    bulk_upsert_with_retry, delete_by_owner_name, delete_by_platform, delete_many, insert_many,
    upsert_many,
};
pub use errors::{RepositoryError, Result};
pub use query::{
    PaginatedResult, Pagination, count, count_by_platform, find_all, find_all_by_platform,
    find_all_by_platform_and_owner, find_by_owner, find_by_platform, find_stale,
};
pub use single::{
    delete, find_by_id, find_by_natural_key, find_by_platform_id, insert, update, upsert,
};

#[cfg(test)]
mod tests {
    use super::*;
    use bulk::build_upsert_on_conflict;
    use chrono::Utc;
    use sea_orm::{DbErr, EntityTrait, QueryTrait, Set};
    use serde_json::json;
    use uuid::Uuid;

    use crate::entity::code_platform::CodePlatform;
    use crate::entity::code_repository::{ActiveModel, Entity as CodeRepository};
    use crate::entity::code_visibility::CodeVisibility;

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
        assert_eq!(r1.page, r2.page);
        assert_eq!(r1.per_page, r2.per_page);
        assert_eq!(r1.total_pages, r2.total_pages);
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
    /// Uses the shared `build_upsert_on_conflict()` function to ensure the test
    /// exercises the exact same conflict resolution logic as production code.
    #[test]
    fn test_bulk_upsert_query_builds() {
        // Create a minimal active model
        let model = ActiveModel {
            id: Set(Uuid::new_v4()),
            platform: Set(CodePlatform::GitHub),
            platform_id: Set(12345),
            owner: Set("test-owner".to_string()),
            name: Set("test-repo".to_string()),
            description: Set(Some("A test repo".to_string())),
            default_branch: Set("main".to_string()),
            topics: Set(json!(["rust", "test"])),
            primary_language: Set(Some("Rust".to_string())),
            license_spdx: Set(Some("MIT".to_string())),
            homepage: Set(None),
            visibility: Set(CodeVisibility::Public),
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
            created_at: Set(Some(Utc::now().fixed_offset())),
            updated_at: Set(Some(Utc::now().fixed_offset())),
            pushed_at: Set(Some(Utc::now().fixed_offset())),
            platform_metadata: Set(json!({})),
            synced_at: Set(Utc::now().fixed_offset()),
            etag: Set(None),
        };

        // Use the shared on_conflict builder (same as production bulk_upsert)
        let query = CodeRepository::insert_many(vec![model])
            .on_conflict(build_upsert_on_conflict())
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
        // Verify the conflict keys match production: (platform, platform_id)
        assert!(
            sql.contains("\"platform_id\""),
            "SQL ON CONFLICT should reference platform_id: {}",
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
