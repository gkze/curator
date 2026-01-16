//! GitLab API data types.

use chrono::{DateTime, Utc};
use serde::Deserialize;

// Re-export shared types from sync module
pub use crate::sync::{ProgressCallback, SyncProgress, emit};

/// GitLab project - fields we need from the API response.
///
/// This struct is used to deserialize GitLab API responses. We define only
/// the fields we need, which makes the code resilient to API changes.
#[derive(Debug, Clone, Deserialize)]
pub struct GitLabProject {
    /// Project ID.
    pub id: u64,
    /// Project name.
    pub name: String,
    /// Project path (slug).
    pub path: String,
    /// Full path including namespace (e.g., "group/subgroup/project").
    pub path_with_namespace: String,
    /// Project description.
    pub description: Option<String>,
    /// Default branch name.
    pub default_branch: Option<String>,
    /// Visibility level: "public", "private", or "internal".
    pub visibility: String,
    /// Whether the project is archived.
    pub archived: bool,
    /// Project topics/tags.
    #[serde(default)]
    pub topics: Vec<String>,
    /// Number of stars.
    #[serde(default)]
    pub star_count: u32,
    /// Number of forks (may not be included in all API responses).
    #[serde(default)]
    pub forks_count: u32,
    /// Number of open issues (may be null if issues are disabled).
    pub open_issues_count: Option<u32>,
    /// When the project was created.
    pub created_at: DateTime<Utc>,
    /// When the project was last active.
    pub last_activity_at: DateTime<Utc>,
    /// Namespace information.
    pub namespace: GitLabNamespace,
    /// If this is a fork, info about the source project.
    pub forked_from_project: Option<Box<ForkedFrom>>,
    /// Whether this is a mirror.
    pub mirror: Option<bool>,
    /// Whether issues are enabled.
    pub issues_enabled: Option<bool>,
    /// Whether wiki is enabled.
    pub wiki_enabled: Option<bool>,
    /// Whether merge requests are enabled.
    pub merge_requests_enabled: Option<bool>,
    /// Web URL to the project.
    pub web_url: String,
    /// SSH clone URL.
    pub ssh_url_to_repo: Option<String>,
    /// HTTP clone URL.
    pub http_url_to_repo: Option<String>,
}

/// GitLab namespace (group or user).
#[derive(Debug, Clone, Deserialize)]
pub struct GitLabNamespace {
    /// Namespace ID.
    pub id: u64,
    /// Namespace name.
    pub name: String,
    /// Namespace path (slug).
    pub path: String,
    /// Full path (e.g., "group/subgroup").
    pub full_path: String,
    /// Kind: "group" or "user".
    pub kind: String,
}

/// Minimal fork source information.
#[derive(Debug, Clone, Deserialize)]
pub struct ForkedFrom {
    /// Source project ID.
    pub id: u64,
}

/// GitLab group information.
#[derive(Debug, Clone, Deserialize)]
pub struct GitLabGroup {
    /// Group ID.
    pub id: u64,
    /// Group name.
    pub name: String,
    /// Full path (e.g., "parent/child").
    pub full_path: String,
    /// Group description.
    pub description: Option<String>,
    /// Visibility level.
    pub visibility: String,
}

/// GitLab user information.
#[derive(Debug, Clone, Deserialize)]
pub struct GitLabUser {
    /// User ID.
    pub id: u64,
    /// Username.
    pub username: String,
    /// Display name.
    pub name: Option<String>,
    /// Email (may be empty based on privacy settings).
    pub email: Option<String>,
    /// User bio.
    pub bio: Option<String>,
    /// Number of public repos (only available on /user endpoint).
    #[serde(default)]
    pub public_email: Option<String>,
}
