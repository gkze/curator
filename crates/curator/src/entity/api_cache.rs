//! ApiCache entity - stores ETags and cache metadata for API endpoints.
//!
//! This enables conditional requests that avoid refetching unchanged data,
//! particularly useful for paginated list endpoints where one ETag covers
//! multiple resources.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::entity::code_platform::CodePlatform;

/// Type of API endpoint being cached.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::None)")]
pub enum EndpointType {
    /// Organization/group repositories list endpoint.
    #[sea_orm(string_value = "org_repos")]
    OrgRepos,
    /// User repositories list endpoint.
    #[sea_orm(string_value = "user_repos")]
    UserRepos,
    /// Starred repositories list endpoint.
    #[sea_orm(string_value = "starred")]
    Starred,
    /// Single repository endpoint.
    #[sea_orm(string_value = "single_repo")]
    SingleRepo,
}

/// ApiCache model - tracks ETags for API endpoints to enable conditional fetching.
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "api_cache")]
pub struct Model {
    /// Internal UUID primary key.
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,

    /// Source platform (github, gitlab, codeberg, gitea).
    pub platform: CodePlatform,

    /// Type of endpoint being cached.
    pub endpoint_type: EndpointType,

    /// Cache key (e.g., "rust-lang/page/1", "gkze/starred/page/3").
    pub cache_key: String,

    /// The ETag value from the API response.
    #[sea_orm(column_type = "Text", nullable)]
    pub etag: Option<String>,

    /// Total number of pages (from Link header's rel="last").
    /// Only populated for page 1 entries.
    pub total_pages: Option<i32>,

    /// When this cache entry was last updated.
    pub cached_at: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

impl Model {
    /// Build a cache key for organization/group repositories.
    pub fn org_repos_key(owner: &str, page: u32) -> String {
        format!("{}/page/{}", owner, page)
    }

    /// Build a cache key for user repositories.
    pub fn user_repos_key(username: &str, page: u32) -> String {
        format!("{}/page/{}", username, page)
    }

    /// Build a cache key for starred repositories.
    pub fn starred_key(username: &str, page: u32) -> String {
        format!("{}/starred/page/{}", username, page)
    }

    /// Build a cache key for a single repository.
    pub fn single_repo_key(owner: &str, name: &str) -> String {
        format!("{}/{}", owner, name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_org_repos_key() {
        assert_eq!(Model::org_repos_key("rust-lang", 1), "rust-lang/page/1");
        assert_eq!(Model::org_repos_key("rust-lang", 5), "rust-lang/page/5");
    }

    #[test]
    fn test_user_repos_key() {
        assert_eq!(Model::user_repos_key("octocat", 1), "octocat/page/1");
    }

    #[test]
    fn test_starred_key() {
        assert_eq!(Model::starred_key("gkze", 1), "gkze/starred/page/1");
    }

    #[test]
    fn test_single_repo_key() {
        assert_eq!(
            Model::single_repo_key("rust-lang", "rust"),
            "rust-lang/rust"
        );
    }
}
