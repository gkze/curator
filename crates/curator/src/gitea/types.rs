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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gitea_repo_deserializes_defaults_and_optional_fields() {
        let json = r#"{
            "id": 1,
            "name": "repo",
            "full_name": "owner/repo",
            "private": false,
            "fork": false,
            "archived": false,
            "mirror": false,
            "empty": false,
            "template": false,
            "stars_count": 1,
            "forks_count": 2,
            "open_issues_count": 3,
            "watchers_count": 4,
            "size": 5,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "pushed_at": null,
            "owner": {"id": 2, "login": "owner"},
            "html_url": "https://example.com/owner/repo"
        }"#;

        let repo: GiteaRepo = serde_json::from_str(json).unwrap();
        assert_eq!(repo.full_name, "owner/repo");
        assert!(repo.topics.is_empty());
        assert!(!repo.has_issues);
        assert!(!repo.has_wiki);
        assert!(!repo.has_pull_requests);
        assert!(repo.pushed_at.is_none());
    }

    #[test]
    fn gitea_repo_deserializes_topics_and_feature_flags() {
        let json = r#"{
            "id": 1,
            "name": "repo",
            "full_name": "owner/repo",
            "private": true,
            "fork": true,
            "archived": true,
            "mirror": true,
            "empty": true,
            "template": true,
            "stars_count": 1,
            "forks_count": 2,
            "open_issues_count": 3,
            "watchers_count": 4,
            "size": 5,
            "language": "Rust",
            "topics": ["rust", "cli"],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "pushed_at": "2024-01-03T00:00:00Z",
            "owner": {"id": 2, "login": "owner", "full_name": "Owner"},
            "html_url": "https://example.com/owner/repo",
            "ssh_url": "git@example.com:owner/repo.git",
            "clone_url": "https://example.com/owner/repo.git",
            "website": "https://repo.example.com",
            "has_issues": true,
            "has_wiki": true,
            "has_pull_requests": true
        }"#;

        let repo: GiteaRepo = serde_json::from_str(json).unwrap();
        assert_eq!(repo.language.as_deref(), Some("Rust"));
        assert_eq!(repo.topics, vec!["rust", "cli"]);
        assert!(repo.has_issues && repo.has_wiki && repo.has_pull_requests);
        assert_eq!(repo.owner.full_name.as_deref(), Some("Owner"));
    }

    #[test]
    fn gitea_org_and_auth_user_deserialize_defaults() {
        let org: GiteaOrg =
            serde_json::from_str(r#"{"id":1,"username":"org","repo_count":7}"#).unwrap();
        assert_eq!(org.username, "org");
        assert_eq!(org.repo_count, 7);
        assert!(org.visibility.is_none());

        let user: GiteaAuthUser = serde_json::from_str(r#"{"id":2,"login":"user"}"#).unwrap();
        assert_eq!(user.login, "user");
        assert_eq!(user.followers_count, 0);
        assert!(user.email.is_none());
    }
}
