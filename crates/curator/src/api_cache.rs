//! API cache operations for storing ETags and cache metadata.
//!
//! This module provides functions for managing cached API responses,
//! enabling conditional requests that avoid refetching unchanged data.

use chrono::Utc;
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, Set, sea_query::OnConflict,
};
use thiserror::Error;
use uuid::Uuid;

use crate::entity::api_cache::{ActiveModel, Column, EndpointType, Entity as ApiCache, Model};
use crate::entity::code_platform::CodePlatform;

/// Errors that can occur during API cache operations.
#[derive(Debug, Error)]
pub enum CacheError {
    /// Database error from sea-orm.
    #[error("Database error: {0}")]
    Database(#[from] DbErr),

    /// Cache entry not found.
    #[error("Cache entry not found: {platform:?}/{endpoint_type:?}/{cache_key}")]
    NotFound {
        platform: CodePlatform,
        endpoint_type: EndpointType,
        cache_key: String,
    },
}

/// Result type alias for cache operations.
pub type Result<T> = std::result::Result<T, CacheError>;

/// Get a cached ETag for a specific endpoint.
///
/// Returns `None` if no cache entry exists.
pub async fn get_etag(
    db: &DatabaseConnection,
    platform: CodePlatform,
    endpoint_type: EndpointType,
    cache_key: &str,
) -> Result<Option<String>> {
    let entry = ApiCache::find()
        .filter(Column::Platform.eq(platform.clone()))
        .filter(Column::EndpointType.eq(endpoint_type))
        .filter(Column::CacheKey.eq(cache_key))
        .one(db)
        .await?;

    Ok(entry.and_then(|e| e.etag))
}

/// Get a cache entry by its lookup key.
pub async fn get(
    db: &DatabaseConnection,
    platform: CodePlatform,
    endpoint_type: EndpointType,
    cache_key: &str,
) -> Result<Option<Model>> {
    let entry = ApiCache::find()
        .filter(Column::Platform.eq(platform.clone()))
        .filter(Column::EndpointType.eq(endpoint_type))
        .filter(Column::CacheKey.eq(cache_key))
        .one(db)
        .await?;

    Ok(entry)
}

/// Store or update a cache entry with a new ETag.
///
/// This performs an upsert - inserting a new entry or updating an existing one.
pub async fn upsert(
    db: &DatabaseConnection,
    platform: CodePlatform,
    endpoint_type: EndpointType,
    cache_key: &str,
    etag: Option<String>,
) -> Result<()> {
    upsert_with_pagination(db, platform, endpoint_type, cache_key, etag, None).await
}

/// Store or update a cache entry with ETag and pagination info.
///
/// This performs an upsert - inserting a new entry or updating an existing one.
/// The `total_pages` field is typically only set for page 1 entries.
pub async fn upsert_with_pagination(
    db: &DatabaseConnection,
    platform: CodePlatform,
    endpoint_type: EndpointType,
    cache_key: &str,
    etag: Option<String>,
    total_pages: Option<i32>,
) -> Result<()> {
    let now = Utc::now().fixed_offset();

    let model = ActiveModel {
        id: Set(Uuid::new_v4()),
        platform: Set(platform),
        endpoint_type: Set(endpoint_type),
        cache_key: Set(cache_key.to_string()),
        etag: Set(etag),
        total_pages: Set(total_pages),
        cached_at: Set(now),
    };

    ApiCache::insert(model)
        .on_conflict(
            OnConflict::columns([Column::Platform, Column::EndpointType, Column::CacheKey])
                .update_columns([Column::Etag, Column::TotalPages, Column::CachedAt])
                .to_owned(),
        )
        .exec(db)
        .await?;

    Ok(())
}

/// Get ETags for multiple cache keys in a single query.
///
/// Returns a HashMap mapping cache_key -> Option<etag>.
/// Keys not found in the database will have None values.
pub async fn get_etags_batch(
    db: &DatabaseConnection,
    platform: CodePlatform,
    endpoint_type: EndpointType,
    cache_keys: &[String],
) -> Result<std::collections::HashMap<String, Option<String>>> {
    use std::collections::HashMap;

    if cache_keys.is_empty() {
        return Ok(HashMap::new());
    }

    let entries = ApiCache::find()
        .filter(Column::Platform.eq(platform.clone()))
        .filter(Column::EndpointType.eq(endpoint_type))
        .filter(Column::CacheKey.is_in(cache_keys.iter().map(|s| s.as_str())))
        .all(db)
        .await?;

    let mut result: HashMap<String, Option<String>> = HashMap::new();

    // Initialize all keys with None
    for key in cache_keys {
        result.insert(key.clone(), None);
    }

    // Fill in ETags for entries that exist
    for entry in entries {
        result.insert(entry.cache_key, entry.etag);
    }

    Ok(result)
}

/// Get the total pages for a namespace from page 1's cache entry.
///
/// Note: This uses the format `{namespace}/page/1` for the cache key.
/// For starred repos, use `get_starred_total_pages` instead.
pub async fn get_total_pages(
    db: &DatabaseConnection,
    platform: CodePlatform,
    endpoint_type: EndpointType,
    namespace: &str,
) -> Result<Option<i32>> {
    // Page 1 cache key stores the total pages
    let page1_key = format!("{}/page/1", namespace);
    let entry = get(db, platform, endpoint_type, &page1_key).await?;
    Ok(entry.and_then(|e| e.total_pages))
}

/// Get the total pages for starred repos from page 1's cache entry.
///
/// Uses the starred-specific cache key format `{username}/starred/page/1`.
pub async fn get_starred_total_pages(
    db: &DatabaseConnection,
    platform: CodePlatform,
    username: &str,
) -> Result<Option<i32>> {
    use crate::entity::api_cache::Model as ApiCacheModel;

    let page1_key = ApiCacheModel::starred_key(username, 1);
    let entry = get(db, platform, EndpointType::Starred, &page1_key).await?;
    Ok(entry.and_then(|e| e.total_pages))
}

/// Delete a specific cache entry.
pub async fn delete(
    db: &DatabaseConnection,
    platform: CodePlatform,
    endpoint_type: EndpointType,
    cache_key: &str,
) -> Result<bool> {
    let result = ApiCache::delete_many()
        .filter(Column::Platform.eq(platform))
        .filter(Column::EndpointType.eq(endpoint_type))
        .filter(Column::CacheKey.eq(cache_key))
        .exec(db)
        .await?;

    Ok(result.rows_affected > 0)
}

/// Delete all cache entries for a platform.
pub async fn delete_by_platform(db: &DatabaseConnection, platform: CodePlatform) -> Result<u64> {
    let result = ApiCache::delete_many()
        .filter(Column::Platform.eq(platform))
        .exec(db)
        .await?;

    Ok(result.rows_affected)
}

/// Delete all cache entries for a platform and endpoint type.
pub async fn delete_by_endpoint_type(
    db: &DatabaseConnection,
    platform: CodePlatform,
    endpoint_type: EndpointType,
) -> Result<u64> {
    let result = ApiCache::delete_many()
        .filter(Column::Platform.eq(platform))
        .filter(Column::EndpointType.eq(endpoint_type))
        .exec(db)
        .await?;

    Ok(result.rows_affected)
}

/// Delete cache entries older than the specified cutoff time.
pub async fn delete_stale(db: &DatabaseConnection, cutoff: chrono::DateTime<Utc>) -> Result<u64> {
    let result = ApiCache::delete_many()
        .filter(Column::CachedAt.lt(cutoff.fixed_offset()))
        .exec(db)
        .await?;

    Ok(result.rows_affected)
}

/// Result of a conditional fetch operation.
#[derive(Debug, Clone)]
pub enum ConditionalResult<T> {
    /// The resource was not modified (304 response).
    /// The cached ETag is still valid.
    NotModified,

    /// The resource was modified.
    /// Contains the new data and optional new ETag.
    Modified { data: T, etag: Option<String> },
}

impl<T> ConditionalResult<T> {
    /// Returns true if the result indicates the resource was not modified.
    pub fn is_not_modified(&self) -> bool {
        matches!(self, ConditionalResult::NotModified)
    }

    /// Returns true if the result indicates the resource was modified.
    pub fn is_modified(&self) -> bool {
        matches!(self, ConditionalResult::Modified { .. })
    }

    /// Extract the data if modified, or return a default.
    pub fn unwrap_or(self, default: T) -> T {
        match self {
            ConditionalResult::NotModified => default,
            ConditionalResult::Modified { data, .. } => data,
        }
    }

    /// Extract the data if modified, or compute a default.
    pub fn unwrap_or_else<F: FnOnce() -> T>(self, f: F) -> T {
        match self {
            ConditionalResult::NotModified => f(),
            ConditionalResult::Modified { data, .. } => data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conditional_result_not_modified() {
        let result: ConditionalResult<Vec<String>> = ConditionalResult::NotModified;
        assert!(result.is_not_modified());
        assert!(!result.is_modified());
    }

    #[test]
    fn test_conditional_result_modified() {
        let result: ConditionalResult<Vec<String>> = ConditionalResult::Modified {
            data: vec!["test".to_string()],
            etag: Some("W/\"abc123\"".to_string()),
        };
        assert!(!result.is_not_modified());
        assert!(result.is_modified());
    }

    #[test]
    fn test_conditional_result_unwrap_or() {
        let not_modified: ConditionalResult<i32> = ConditionalResult::NotModified;
        assert_eq!(not_modified.unwrap_or(42), 42);

        let modified: ConditionalResult<i32> = ConditionalResult::Modified {
            data: 100,
            etag: None,
        };
        assert_eq!(modified.unwrap_or(42), 100);
    }

    #[test]
    fn test_conditional_result_unwrap_or_else() {
        let not_modified: ConditionalResult<i32> = ConditionalResult::NotModified;
        assert_eq!(not_modified.unwrap_or_else(|| 42 * 2), 84);

        let modified: ConditionalResult<i32> = ConditionalResult::Modified {
            data: 100,
            etag: None,
        };
        assert_eq!(
            modified.unwrap_or_else(|| panic!("should not be called")),
            100
        );
    }

    #[test]
    fn test_conditional_result_modified_without_etag() {
        let result: ConditionalResult<String> = ConditionalResult::Modified {
            data: "test data".to_string(),
            etag: None,
        };
        assert!(result.is_modified());
        assert!(!result.is_not_modified());
    }

    #[test]
    fn test_cache_error_not_found_display() {
        let err = CacheError::NotFound {
            platform: CodePlatform::GitHub,
            endpoint_type: EndpointType::OrgRepos,
            cache_key: "rust-lang/page/1".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("Cache entry not found"));
        assert!(msg.contains("GitHub"));
        assert!(msg.contains("OrgRepos"));
        assert!(msg.contains("rust-lang/page/1"));
    }

    #[test]
    fn test_cache_error_from_db_error() {
        // Test that DbErr converts to CacheError::Database
        // We can't easily construct a DbErr, but we can verify the From impl exists
        fn assert_from_impl<T: From<sea_orm::DbErr>>() {}
        assert_from_impl::<CacheError>();
    }

    #[test]
    fn test_endpoint_type_variants() {
        // Verify all endpoint types exist and can be matched
        let types = [
            EndpointType::OrgRepos,
            EndpointType::UserRepos,
            EndpointType::Starred,
            EndpointType::SingleRepo,
        ];
        assert_eq!(types.len(), 4);
    }
}
