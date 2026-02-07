//! GitLab API data types.

use chrono::{DateTime, Utc};
use serde::Deserialize;

// Re-export shared types from sync module
pub use crate::sync::{ProgressCallback, SyncProgress, emit};

use super::api::types as generated;

/// GitLab project - fields we need from the API response.
///
/// This struct provides strongly-typed access to the fields we use.
/// The generated Progenitor types have every field as `Option` (per the
/// OpenAPI spec), so these `From` impls extract what we need with sensible
/// defaults.
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
    /// Public email address.
    #[serde(default)]
    pub public_email: Option<String>,
    /// Number of followers.
    #[serde(default)]
    pub followers: usize,
}

// ---------------------------------------------------------------------------
// From impls: Progenitor generated types → our types
// ---------------------------------------------------------------------------

impl From<generated::ApiEntitiesProject> for GitLabProject {
    fn from(p: generated::ApiEntitiesProject) -> Self {
        let ns = p
            .namespace
            .map(GitLabNamespace::from)
            .unwrap_or_else(|| GitLabNamespace {
                id: 0,
                name: String::new(),
                path: String::new(),
                full_path: String::new(),
                kind: String::new(),
            });

        Self {
            id: p.id.unwrap_or(0) as u64,
            name: p.name.unwrap_or_default(),
            path: p.path.unwrap_or_default(),
            path_with_namespace: p.path_with_namespace.unwrap_or_default(),
            description: p.description,
            default_branch: p.default_branch,
            visibility: p.visibility.unwrap_or_else(|| "private".to_string()),
            archived: p.archived.unwrap_or(false),
            topics: p.topics,
            star_count: p.star_count.unwrap_or(0) as u32,
            forks_count: p.forks_count.unwrap_or(0) as u32,
            open_issues_count: p.open_issues_count.map(|v| v as u32),
            created_at: p.created_at.unwrap_or_else(Utc::now),
            last_activity_at: p.last_activity_at.unwrap_or_else(Utc::now),
            namespace: ns,
            forked_from_project: p.forked_from_project.map(|fp| {
                Box::new(ForkedFrom {
                    id: fp.id.unwrap_or(0) as u64,
                })
            }),
            mirror: p.mirror,
            issues_enabled: p.issues_enabled,
            wiki_enabled: p.wiki_enabled,
            merge_requests_enabled: p.merge_requests_enabled,
            web_url: p.web_url.unwrap_or_default(),
            ssh_url_to_repo: p.ssh_url_to_repo,
            http_url_to_repo: p.http_url_to_repo,
        }
    }
}

impl From<generated::ApiEntitiesProjectWithAccess> for GitLabProject {
    fn from(p: generated::ApiEntitiesProjectWithAccess) -> Self {
        let ns = p
            .namespace
            .map(GitLabNamespace::from)
            .unwrap_or_else(|| GitLabNamespace {
                id: 0,
                name: String::new(),
                path: String::new(),
                full_path: String::new(),
                kind: String::new(),
            });

        Self {
            id: p.id.unwrap_or(0) as u64,
            name: p.name.unwrap_or_default(),
            path: p.path.unwrap_or_default(),
            path_with_namespace: p.path_with_namespace.unwrap_or_default(),
            description: p.description,
            default_branch: p.default_branch,
            visibility: p.visibility.unwrap_or_else(|| "private".to_string()),
            archived: p.archived.unwrap_or(false),
            topics: p.topics,
            star_count: p.star_count.unwrap_or(0) as u32,
            forks_count: p.forks_count.unwrap_or(0) as u32,
            open_issues_count: p.open_issues_count.map(|v| v as u32),
            created_at: p.created_at.unwrap_or_else(Utc::now),
            last_activity_at: p.last_activity_at.unwrap_or_else(Utc::now),
            namespace: ns,
            forked_from_project: p.forked_from_project.map(|fp| {
                Box::new(ForkedFrom {
                    id: fp.id.unwrap_or(0) as u64,
                })
            }),
            mirror: p.mirror,
            issues_enabled: p.issues_enabled,
            wiki_enabled: p.wiki_enabled,
            merge_requests_enabled: p.merge_requests_enabled,
            web_url: p.web_url.unwrap_or_default(),
            ssh_url_to_repo: p.ssh_url_to_repo,
            http_url_to_repo: p.http_url_to_repo,
        }
    }
}

impl From<generated::ApiEntitiesNamespaceBasic> for GitLabNamespace {
    fn from(ns: generated::ApiEntitiesNamespaceBasic) -> Self {
        Self {
            id: ns.id.unwrap_or(0) as u64,
            name: ns.name.unwrap_or_default(),
            path: ns.path.unwrap_or_default(),
            full_path: ns.full_path.unwrap_or_default(),
            kind: ns.kind.unwrap_or_default(),
        }
    }
}

impl From<generated::ApiEntitiesCurrentUser> for GitLabUser {
    fn from(u: generated::ApiEntitiesCurrentUser) -> Self {
        Self {
            id: u.id as u64,
            username: u.username,
            name: u.name,
            email: u.email,
            bio: u.bio,
            public_email: u.public_email,
            // Generated type doesn't include followers; fetched separately
            // via get_authenticated_user() raw request
            followers: 0,
        }
    }
}

impl From<generated::ApiEntitiesGroupDetail> for GitLabGroup {
    fn from(g: generated::ApiEntitiesGroupDetail) -> Self {
        Self {
            id: g.id.and_then(|s| s.parse::<u64>().ok()).unwrap_or(0),
            name: g.name.unwrap_or_default(),
            full_path: g.full_path.unwrap_or_default(),
            description: g.description,
            visibility: g.visibility.unwrap_or_else(|| "private".to_string()),
        }
    }
}

impl From<generated::ApiEntitiesBasicProjectDetails> for GitLabProject {
    fn from(p: generated::ApiEntitiesBasicProjectDetails) -> Self {
        let ns = p
            .namespace
            .map(GitLabNamespace::from)
            .unwrap_or_else(|| GitLabNamespace {
                id: 0,
                name: String::new(),
                path: String::new(),
                full_path: String::new(),
                kind: String::new(),
            });

        Self {
            id: p.id.unwrap_or(0) as u64,
            name: p.name.unwrap_or_default(),
            path: p.path.unwrap_or_default(),
            path_with_namespace: p.path_with_namespace.unwrap_or_default(),
            description: p.description,
            default_branch: p.default_branch,
            visibility: p.visibility.unwrap_or_else(|| "private".to_string()),
            archived: false, // not in BasicProjectDetails
            topics: p.topics,
            star_count: p.star_count.unwrap_or(0) as u32,
            forks_count: p.forks_count.unwrap_or(0) as u32,
            open_issues_count: None, // not in BasicProjectDetails
            created_at: p.created_at.unwrap_or_else(Utc::now),
            last_activity_at: p.last_activity_at.unwrap_or_else(Utc::now),
            namespace: ns,
            forked_from_project: None,    // not in BasicProjectDetails
            mirror: None,                 // not in BasicProjectDetails
            issues_enabled: None,         // not in BasicProjectDetails
            wiki_enabled: None,           // not in BasicProjectDetails
            merge_requests_enabled: None, // not in BasicProjectDetails
            web_url: p.web_url.unwrap_or_default(),
            ssh_url_to_repo: p.ssh_url_to_repo,
            http_url_to_repo: p.http_url_to_repo,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gitlab_project_deserialize_minimal() {
        let json = r#"{
            "id": 12345,
            "name": "test-project",
            "path": "test-project",
            "path_with_namespace": "group/test-project",
            "visibility": "public",
            "archived": false,
            "created_at": "2024-01-01T00:00:00Z",
            "last_activity_at": "2024-06-01T00:00:00Z",
            "namespace": {
                "id": 1,
                "name": "group",
                "path": "group",
                "full_path": "group",
                "kind": "group"
            },
            "web_url": "https://gitlab.com/group/test-project"
        }"#;

        let project: GitLabProject = serde_json::from_str(json).unwrap();
        assert_eq!(project.id, 12345);
        assert_eq!(project.name, "test-project");
        assert_eq!(project.visibility, "public");
        assert!(!project.archived);
        assert_eq!(project.star_count, 0); // defaults
        assert_eq!(project.forks_count, 0); // defaults
        assert!(project.topics.is_empty()); // defaults
    }

    #[test]
    fn test_gitlab_project_deserialize_full() {
        let json = r#"{
            "id": 12345,
            "name": "test-project",
            "path": "test-project",
            "path_with_namespace": "group/subgroup/test-project",
            "description": "A test project",
            "default_branch": "develop",
            "visibility": "private",
            "archived": true,
            "topics": ["rust", "api"],
            "star_count": 42,
            "forks_count": 5,
            "open_issues_count": 10,
            "created_at": "2024-01-01T00:00:00Z",
            "last_activity_at": "2024-06-01T00:00:00Z",
            "namespace": {
                "id": 1,
                "name": "subgroup",
                "path": "subgroup",
                "full_path": "group/subgroup",
                "kind": "group"
            },
            "forked_from_project": {"id": 999},
            "mirror": true,
            "issues_enabled": true,
            "wiki_enabled": false,
            "merge_requests_enabled": true,
            "web_url": "https://gitlab.com/group/subgroup/test-project",
            "ssh_url_to_repo": "git@gitlab.com:group/subgroup/test-project.git",
            "http_url_to_repo": "https://gitlab.com/group/subgroup/test-project.git"
        }"#;

        let project: GitLabProject = serde_json::from_str(json).unwrap();
        assert_eq!(project.id, 12345);
        assert_eq!(project.description, Some("A test project".to_string()));
        assert_eq!(project.default_branch, Some("develop".to_string()));
        assert!(project.archived);
        assert_eq!(project.topics, vec!["rust", "api"]);
        assert_eq!(project.star_count, 42);
        assert_eq!(project.forks_count, 5);
        assert_eq!(project.open_issues_count, Some(10));
        assert!(project.forked_from_project.is_some());
        assert_eq!(project.forked_from_project.as_ref().unwrap().id, 999);
        assert_eq!(project.mirror, Some(true));
        assert_eq!(project.issues_enabled, Some(true));
        assert_eq!(project.wiki_enabled, Some(false));
    }

    #[test]
    fn test_gitlab_namespace_deserialize() {
        let json = r#"{
            "id": 123,
            "name": "My Group",
            "path": "my-group",
            "full_path": "parent/my-group",
            "kind": "group"
        }"#;

        let ns: GitLabNamespace = serde_json::from_str(json).unwrap();
        assert_eq!(ns.id, 123);
        assert_eq!(ns.name, "My Group");
        assert_eq!(ns.path, "my-group");
        assert_eq!(ns.full_path, "parent/my-group");
        assert_eq!(ns.kind, "group");
    }

    #[test]
    fn test_gitlab_namespace_user_kind() {
        let json = r#"{
            "id": 456,
            "name": "johndoe",
            "path": "johndoe",
            "full_path": "johndoe",
            "kind": "user"
        }"#;

        let ns: GitLabNamespace = serde_json::from_str(json).unwrap();
        assert_eq!(ns.kind, "user");
        assert_eq!(ns.full_path, "johndoe");
    }

    #[test]
    fn test_gitlab_user_deserialize_minimal() {
        let json = r#"{
            "id": 123,
            "username": "johndoe"
        }"#;

        let user: GitLabUser = serde_json::from_str(json).unwrap();
        assert_eq!(user.id, 123);
        assert_eq!(user.username, "johndoe");
        assert!(user.name.is_none());
        assert!(user.email.is_none());
        assert!(user.bio.is_none());
    }

    #[test]
    fn test_gitlab_user_deserialize_full() {
        let json = r#"{
            "id": 123,
            "username": "johndoe",
            "name": "John Doe",
            "email": "john@example.com",
            "bio": "Software developer",
            "public_email": "john.public@example.com"
        }"#;

        let user: GitLabUser = serde_json::from_str(json).unwrap();
        assert_eq!(user.id, 123);
        assert_eq!(user.username, "johndoe");
        assert_eq!(user.name, Some("John Doe".to_string()));
        assert_eq!(user.email, Some("john@example.com".to_string()));
        assert_eq!(user.bio, Some("Software developer".to_string()));
        assert_eq!(
            user.public_email,
            Some("john.public@example.com".to_string())
        );
    }

    #[test]
    fn test_gitlab_group_deserialize() {
        let json = r#"{
            "id": 789,
            "name": "My Group",
            "full_path": "parent/my-group",
            "description": "A test group",
            "visibility": "internal"
        }"#;

        let group: GitLabGroup = serde_json::from_str(json).unwrap();
        assert_eq!(group.id, 789);
        assert_eq!(group.name, "My Group");
        assert_eq!(group.full_path, "parent/my-group");
        assert_eq!(group.description, Some("A test group".to_string()));
        assert_eq!(group.visibility, "internal");
    }

    #[test]
    fn test_gitlab_group_deserialize_minimal() {
        let json = r#"{
            "id": 789,
            "name": "My Group",
            "full_path": "my-group",
            "visibility": "public"
        }"#;

        let group: GitLabGroup = serde_json::from_str(json).unwrap();
        assert_eq!(group.id, 789);
        assert!(group.description.is_none());
    }

    #[test]
    fn test_forked_from_deserialize() {
        let json = r#"{"id": 999}"#;

        let forked: ForkedFrom = serde_json::from_str(json).unwrap();
        assert_eq!(forked.id, 999);
    }

    #[test]
    fn test_gitlab_project_clone() {
        let project = GitLabProject {
            id: 1,
            name: "test".to_string(),
            path: "test".to_string(),
            path_with_namespace: "group/test".to_string(),
            description: None,
            default_branch: None,
            visibility: "public".to_string(),
            archived: false,
            topics: vec![],
            star_count: 0,
            forks_count: 0,
            open_issues_count: None,
            created_at: Utc::now(),
            last_activity_at: Utc::now(),
            namespace: GitLabNamespace {
                id: 1,
                name: "group".to_string(),
                path: "group".to_string(),
                full_path: "group".to_string(),
                kind: "group".to_string(),
            },
            forked_from_project: None,
            mirror: None,
            issues_enabled: None,
            wiki_enabled: None,
            merge_requests_enabled: None,
            web_url: "https://gitlab.com/group/test".to_string(),
            ssh_url_to_repo: None,
            http_url_to_repo: None,
        };

        let cloned = project.clone();
        assert_eq!(cloned.id, project.id);
        assert_eq!(cloned.name, project.name);
    }

    #[test]
    fn test_gitlab_project_debug() {
        let project = GitLabProject {
            id: 1,
            name: "test".to_string(),
            path: "test".to_string(),
            path_with_namespace: "group/test".to_string(),
            description: None,
            default_branch: None,
            visibility: "public".to_string(),
            archived: false,
            topics: vec![],
            star_count: 0,
            forks_count: 0,
            open_issues_count: None,
            created_at: Utc::now(),
            last_activity_at: Utc::now(),
            namespace: GitLabNamespace {
                id: 1,
                name: "group".to_string(),
                path: "group".to_string(),
                full_path: "group".to_string(),
                kind: "group".to_string(),
            },
            forked_from_project: None,
            mirror: None,
            issues_enabled: None,
            wiki_enabled: None,
            merge_requests_enabled: None,
            web_url: "https://gitlab.com/group/test".to_string(),
            ssh_url_to_repo: None,
            http_url_to_repo: None,
        };

        let debug_str = format!("{:?}", project);
        assert!(debug_str.contains("GitLabProject"));
        assert!(debug_str.contains("test"));
    }
}
