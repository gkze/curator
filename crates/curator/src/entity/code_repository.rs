//! CodeRepository entity - unified schema for repositories across platform instances.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::entity::code_visibility::CodeVisibility;

/// CodeRepository model - tracks repositories across multiple code forge instances.
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "code_repositories")]
pub struct Model {
    /// Internal UUID primary key.
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,

    // ─── Instance Identity ─────────────────────────────────────────────────────
    /// Reference to the platform instance this repository belongs to.
    pub instance_id: Uuid,
    /// Platform-specific numeric ID.
    pub platform_id: i64,

    // ─── Naming ──────────────────────────────────────────────────────────────
    /// Owner login (user or organization).
    pub owner: String,
    /// Repository name (URL-safe slug).
    pub name: String,

    // ─── Content ─────────────────────────────────────────────────────────────
    /// Repository description.
    pub description: Option<String>,
    /// Default branch name.
    #[sea_orm(default_value = "main")]
    pub default_branch: String,
    /// Repository topics/tags (stored as JSON array for cross-database compatibility).
    #[sea_orm(column_type = "Json")]
    pub topics: serde_json::Value,
    /// Primary programming language.
    pub primary_language: Option<String>,
    /// License SPDX identifier (best-effort extraction).
    pub license_spdx: Option<String>,
    /// Project homepage URL.
    #[sea_orm(column_type = "Text")]
    pub homepage: Option<String>,

    // ─── Visibility ──────────────────────────────────────────────────────────
    /// Visibility level (public, private, internal).
    pub visibility: CodeVisibility,
    /// Whether this is a fork of another repository.
    #[sea_orm(default_value = false)]
    pub is_fork: bool,
    /// Whether this is a mirror of another repository.
    #[sea_orm(default_value = false)]
    pub is_mirror: bool,
    /// Whether the repository is archived (read-only).
    #[sea_orm(default_value = false)]
    pub is_archived: bool,
    /// Whether this is a template repository.
    #[sea_orm(default_value = false)]
    pub is_template: bool,
    /// Whether the repository is empty (no commits).
    #[sea_orm(default_value = false)]
    pub is_empty: bool,

    // ─── Statistics ──────────────────────────────────────────────────────────
    /// Star/favorite count.
    pub stars: Option<i32>,
    /// Fork count.
    pub forks: Option<i32>,
    /// Open issue count.
    pub open_issues: Option<i32>,
    /// Watcher count (GitHub only).
    pub watchers: Option<i32>,
    /// Repository size in KB.
    pub size_kb: Option<i64>,

    // ─── Features ────────────────────────────────────────────────────────────
    /// Whether issues are enabled.
    #[sea_orm(default_value = true)]
    pub has_issues: bool,
    /// Whether wiki is enabled.
    #[sea_orm(default_value = true)]
    pub has_wiki: bool,
    /// Whether pull/merge requests are enabled.
    #[sea_orm(default_value = true)]
    pub has_pull_requests: bool,

    // ─── Timestamps ──────────────────────────────────────────────────────────
    /// When the repository was created on the platform.
    pub created_at: Option<DateTimeWithTimeZone>,
    /// When the repository was last updated.
    pub updated_at: Option<DateTimeWithTimeZone>,
    /// When code was last pushed (GitHub only).
    pub pushed_at: Option<DateTimeWithTimeZone>,

    // ─── Platform-Specific ───────────────────────────────────────────────────
    /// Platform-specific metadata stored as JSON.
    ///
    /// This allows storing platform-specific fields that don't fit the common schema,
    /// such as GitHub's `node_id`, GitLab's `commit_count`, etc.
    #[sea_orm(column_type = "Json")]
    pub platform_metadata: serde_json::Value,

    // ─── Tracking ────────────────────────────────────────────────────────────
    /// When this record was last synced from the platform.
    pub synced_at: DateTimeWithTimeZone,

    // ─── Caching ─────────────────────────────────────────────────────────────
    /// ETag from the API response for conditional fetching.
    /// Used with If-None-Match header to avoid refetching unchanged data.
    #[sea_orm(column_type = "Text", nullable)]
    pub etag: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    /// A repository belongs to an instance.
    #[sea_orm(
        belongs_to = "super::instance::Entity",
        from = "Column::InstanceId",
        to = "super::instance::Column::Id"
    )]
    Instance,
}

impl Related<super::instance::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Instance.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl Model {
    /// Compute the full name (owner/name).
    pub fn full_name(&self) -> String {
        format!("{}/{}", self.owner, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_test_model(owner: &str, name: &str) -> Model {
        Model {
            id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            platform_id: 12345,
            owner: owner.to_string(),
            name: name.to_string(),
            description: Some("A test repository".to_string()),
            default_branch: "main".to_string(),
            topics: serde_json::json!(["rust", "cli"]),
            primary_language: Some("Rust".to_string()),
            license_spdx: Some("MIT".to_string()),
            homepage: None,
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_mirror: false,
            is_archived: false,
            is_template: false,
            is_empty: false,
            stars: Some(100),
            forks: Some(10),
            open_issues: Some(5),
            watchers: Some(50),
            size_kb: Some(1024),
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

    #[test]
    fn test_full_name() {
        let model = make_test_model("octocat", "hello-world");
        assert_eq!(model.full_name(), "octocat/hello-world");
    }
}
