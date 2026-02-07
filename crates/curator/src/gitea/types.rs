//! Gitea API data types.

use chrono::{DateTime, Utc};
use serde::Deserialize;

// Re-export shared types from sync module
pub use crate::sync::{ProgressCallback, SyncProgress};

/// Gitea repository - fields we need from the API response.
///
/// This struct is used to deserialize Gitea API responses. We define only
/// the fields we need, which makes the code resilient to API changes.
///
/// API docs: https://docs.gitea.com/api/1.20/#tag/repository/operation/repoSearch
#[derive(Debug, Clone, Deserialize)]
pub struct GiteaRepo {
    /// Repository ID.
    pub id: i64,
    /// Repository name.
    pub name: String,
    /// Full name including owner (e.g., "owner/repo").
    pub full_name: String,
    /// Repository description.
    pub description: Option<String>,
    /// Default branch name.
    pub default_branch: Option<String>,
    /// Whether the repository is private.
    pub private: bool,
    /// Whether the repository is a fork.
    pub fork: bool,
    /// Whether the repository is archived.
    pub archived: bool,
    /// Whether the repository is a mirror.
    pub mirror: bool,
    /// Whether the repository is empty.
    pub empty: bool,
    /// Whether the repository is a template.
    pub template: bool,
    /// Number of stars.
    pub stars_count: u32,
    /// Number of forks.
    pub forks_count: u32,
    /// Number of open issues.
    pub open_issues_count: u32,
    /// Number of watchers.
    pub watchers_count: u32,
    /// Size in KB.
    pub size: u64,
    /// Primary programming language.
    pub language: Option<String>,
    /// Repository topics/tags.
    #[serde(default)]
    pub topics: Vec<String>,
    /// When the repo was created.
    pub created_at: DateTime<Utc>,
    /// When the repo was last updated.
    pub updated_at: DateTime<Utc>,
    /// When code was last pushed (may be null).
    pub pushed_at: Option<DateTime<Utc>>,
    /// Owner information.
    pub owner: GiteaUser,
    /// HTML URL to the repository.
    pub html_url: String,
    /// SSH clone URL.
    pub ssh_url: Option<String>,
    /// HTTP clone URL.
    pub clone_url: Option<String>,
    /// Website URL.
    pub website: Option<String>,
    /// Whether issues are enabled.
    #[serde(default)]
    pub has_issues: bool,
    /// Whether wiki is enabled.
    #[serde(default)]
    pub has_wiki: bool,
    /// Whether pull requests are enabled.
    #[serde(default)]
    pub has_pull_requests: bool,
}

/// Gitea user/organization.
#[derive(Debug, Clone, Deserialize)]
pub struct GiteaUser {
    /// User ID.
    pub id: i64,
    /// Username/login.
    pub login: String,
    /// Full name.
    pub full_name: Option<String>,
    /// Avatar URL.
    pub avatar_url: Option<String>,
}

/// Gitea organization.
#[derive(Debug, Clone, Deserialize)]
pub struct GiteaOrg {
    /// Organization ID.
    pub id: i64,
    /// Organization username.
    pub username: String,
    /// Organization name.
    pub full_name: Option<String>,
    /// Organization description.
    pub description: Option<String>,
    /// Avatar URL.
    pub avatar_url: Option<String>,
    /// Website URL.
    pub website: Option<String>,
    /// Location.
    pub location: Option<String>,
    /// Visibility: "public" or "limited" or "private".
    pub visibility: Option<String>,
    /// Number of repositories in the organization.
    #[serde(default)]
    pub repo_count: usize,
}

/// Gitea user (authenticated user endpoint response).
#[derive(Debug, Clone, Deserialize)]
pub struct GiteaAuthUser {
    /// User ID.
    pub id: i64,
    /// Username/login.
    pub login: String,
    /// Full name.
    pub full_name: Option<String>,
    /// Email address.
    pub email: Option<String>,
    /// User bio/description.
    pub description: Option<String>,
    /// Number of followers.
    #[serde(default)]
    pub followers_count: usize,
}
