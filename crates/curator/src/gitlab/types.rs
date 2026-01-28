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
    /// Number of public repos (only available on /user endpoint).
    #[serde(default)]
    pub public_email: Option<String>,
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

impl From<generated::GetApiV4UserResponse> for GitLabUser {
    fn from(u: generated::GetApiV4UserResponse) -> Self {
        Self {
            id: u.id as u64,
            username: u.username,
            name: u.name,
            email: u.email,
            bio: u.bio,
            public_email: u.public_email,
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
