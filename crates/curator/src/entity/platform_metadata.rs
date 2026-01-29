//! Typed platform-specific metadata structs for JSONB storage.
//!
//! These structs provide type-safe access to the `platform_metadata` JSONB field
//! in the `code_repositories` table. Each platform has unique fields that don't
//! fit the common repository schema.
//!
//! # Usage
//!
//! ```ignore
//! use curator::entity::platform_metadata::{PlatformMetadata, GitHubMetadata};
//!
//! // Parse from JSON stored in database
//! let metadata: GitHubMetadata = serde_json::from_value(repo.platform_metadata.clone())?;
//!
//! // Or use the unified enum
//! let metadata = PlatformMetadata::from_json(PlatformType::GitHub, &repo.platform_metadata)?;
//! ```

use serde::{Deserialize, Serialize};

use super::platform_type::PlatformType;

/// GitHub-specific repository metadata.
///
/// Contains fields unique to GitHub's API that aren't part of the common repository schema.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GitHubMetadata {
    /// GitHub's GraphQL node ID for the repository.
    pub node_id: Option<String>,

    /// Whether the repository is private (redundant with visibility, but preserved from API).
    pub private: Option<bool>,

    /// Whether squash merging is allowed for pull requests.
    pub allow_squash_merge: Option<bool>,

    /// Whether merge commits are allowed for pull requests.
    pub allow_merge_commit: Option<bool>,

    /// Whether rebase merging is allowed for pull requests.
    pub allow_rebase_merge: Option<bool>,

    /// Whether auto-merge is enabled for pull requests.
    pub allow_auto_merge: Option<bool>,

    /// Whether branches are automatically deleted after merge.
    pub delete_branch_on_merge: Option<bool>,

    /// Whether this repository is a mirror.
    pub mirror: Option<bool>,

    /// Whether this is a template repository.
    pub template: Option<bool>,

    /// Whether the repository is empty (no commits).
    pub empty: Option<bool>,

    /// Number of open issues.
    pub open_issues_count: Option<u32>,

    /// Number of watchers.
    pub watchers_count: Option<u32>,

    /// Whether issues are enabled.
    pub has_issues: Option<bool>,

    /// Whether wiki is enabled.
    pub has_wiki: Option<bool>,

    /// Whether pull requests are enabled (always true for GitHub).
    pub has_pull_requests: Option<bool>,
}

/// GitLab-specific repository (project) metadata.
///
/// Contains fields unique to GitLab's API, particularly namespace/group information.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GitLabMetadata {
    /// Web URL for the project.
    pub web_url: Option<String>,

    /// SSH clone URL.
    pub ssh_url_to_repo: Option<String>,

    /// HTTPS clone URL.
    pub http_url_to_repo: Option<String>,

    /// Namespace (group/user) numeric ID.
    pub namespace_id: Option<i64>,

    /// Namespace kind ("group" or "user").
    pub namespace_kind: Option<String>,

    /// Namespace path (URL slug).
    pub namespace_path: Option<String>,

    /// Whether this repository is a mirror.
    pub mirror: Option<bool>,

    /// Project path (URL slug without namespace).
    pub path: Option<String>,

    /// Number of open issues.
    pub open_issues_count: Option<u32>,

    /// Whether issues are enabled.
    pub has_issues: Option<bool>,

    /// Whether wiki is enabled.
    pub has_wiki: Option<bool>,

    /// Whether merge requests are enabled.
    pub has_pull_requests: Option<bool>,
}

/// Gitea-specific repository metadata.
///
/// Gitea, Forgejo, and Codeberg all share the same API structure.
/// This struct is used for all Gitea-compatible instances.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GiteaMetadata {
    /// Whether this repository is a mirror.
    pub mirror: Option<bool>,

    /// Whether the repository is empty (no commits).
    pub empty: Option<bool>,

    /// Whether this is a template repository.
    pub template: Option<bool>,

    /// Web URL for the repository.
    pub html_url: Option<String>,

    /// SSH clone URL.
    pub ssh_url: Option<String>,

    /// HTTPS clone URL.
    pub clone_url: Option<String>,

    /// Number of open issues.
    pub open_issues_count: Option<u32>,

    /// Number of watchers.
    pub watchers_count: Option<u32>,

    /// Whether issues are enabled.
    pub has_issues: Option<bool>,

    /// Whether wiki is enabled.
    pub has_wiki: Option<bool>,

    /// Whether pull requests are enabled.
    pub has_pull_requests: Option<bool>,
}

/// Unified enum for platform-specific metadata.
///
/// This enum wraps the platform-specific metadata structs, allowing type-safe
/// handling of metadata regardless of the source platform.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "platform", content = "data")]
pub enum PlatformMetadata {
    /// GitHub repository metadata.
    GitHub(GitHubMetadata),
    /// GitLab project metadata.
    GitLab(GitLabMetadata),
    /// Gitea/Forgejo/Codeberg repository metadata.
    Gitea(GiteaMetadata),
}

impl PlatformMetadata {
    /// Parse platform metadata from JSON based on the platform type.
    ///
    /// # Arguments
    ///
    /// * `platform_type` - The platform type this metadata came from
    /// * `json` - The JSON value containing the metadata
    ///
    /// # Returns
    ///
    /// Returns the parsed metadata wrapped in the appropriate enum variant.
    ///
    /// # Errors
    ///
    /// Returns an error if the JSON cannot be deserialized into the expected struct.
    pub fn from_json(
        platform_type: PlatformType,
        json: &serde_json::Value,
    ) -> Result<Self, serde_json::Error> {
        match platform_type {
            PlatformType::GitHub => {
                let metadata: GitHubMetadata = serde_json::from_value(json.clone())?;
                Ok(PlatformMetadata::GitHub(metadata))
            }
            PlatformType::GitLab => {
                let metadata: GitLabMetadata = serde_json::from_value(json.clone())?;
                Ok(PlatformMetadata::GitLab(metadata))
            }
            PlatformType::Gitea => {
                let metadata: GiteaMetadata = serde_json::from_value(json.clone())?;
                Ok(PlatformMetadata::Gitea(metadata))
            }
        }
    }

    /// Convert to a JSON value suitable for storage.
    ///
    /// This returns the inner metadata as JSON without the enum wrapper,
    /// which is the format used in the database.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            PlatformMetadata::GitHub(m) => {
                serde_json::to_value(m).unwrap_or(serde_json::Value::Null)
            }
            PlatformMetadata::GitLab(m) => {
                serde_json::to_value(m).unwrap_or(serde_json::Value::Null)
            }
            PlatformMetadata::Gitea(m) => {
                serde_json::to_value(m).unwrap_or(serde_json::Value::Null)
            }
        }
    }

    /// Get the platform type for this metadata.
    pub fn platform_type(&self) -> PlatformType {
        match self {
            PlatformMetadata::GitHub(_) => PlatformType::GitHub,
            PlatformMetadata::GitLab(_) => PlatformType::GitLab,
            PlatformMetadata::Gitea(_) => PlatformType::Gitea,
        }
    }

    /// Get the web URL from metadata if available.
    pub fn web_url(&self) -> Option<&str> {
        match self {
            PlatformMetadata::GitHub(_) => None, // GitHub uses computed URLs
            PlatformMetadata::GitLab(m) => m.web_url.as_deref(),
            PlatformMetadata::Gitea(m) => m.html_url.as_deref(),
        }
    }

    /// Get the SSH clone URL from metadata if available.
    pub fn ssh_url(&self) -> Option<&str> {
        match self {
            PlatformMetadata::GitHub(_) => None, // GitHub uses computed URLs
            PlatformMetadata::GitLab(m) => m.ssh_url_to_repo.as_deref(),
            PlatformMetadata::Gitea(m) => m.ssh_url.as_deref(),
        }
    }

    /// Check if issues are enabled.
    pub fn has_issues(&self) -> Option<bool> {
        match self {
            PlatformMetadata::GitHub(m) => m.has_issues,
            PlatformMetadata::GitLab(m) => m.has_issues,
            PlatformMetadata::Gitea(m) => m.has_issues,
        }
    }

    /// Check if wiki is enabled.
    pub fn has_wiki(&self) -> Option<bool> {
        match self {
            PlatformMetadata::GitHub(m) => m.has_wiki,
            PlatformMetadata::GitLab(m) => m.has_wiki,
            PlatformMetadata::Gitea(m) => m.has_wiki,
        }
    }

    /// Check if pull requests / merge requests are enabled.
    pub fn has_pull_requests(&self) -> Option<bool> {
        match self {
            PlatformMetadata::GitHub(m) => m.has_pull_requests,
            PlatformMetadata::GitLab(m) => m.has_pull_requests,
            PlatformMetadata::Gitea(m) => m.has_pull_requests,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_github_metadata_deserialize() {
        let json = json!({
            "node_id": "MDEwOlJlcG9zaXRvcnkxMjM0NTY=",
            "private": false,
            "allow_squash_merge": true,
            "allow_merge_commit": true,
            "allow_rebase_merge": false,
            "watchers_count": 42,
            "has_issues": true
        });

        let metadata: GitHubMetadata = serde_json::from_value(json).unwrap();
        assert_eq!(
            metadata.node_id,
            Some("MDEwOlJlcG9zaXRvcnkxMjM0NTY=".to_string())
        );
        assert_eq!(metadata.private, Some(false));
        assert_eq!(metadata.allow_squash_merge, Some(true));
        assert_eq!(metadata.allow_rebase_merge, Some(false));
        assert_eq!(metadata.watchers_count, Some(42));
        assert_eq!(metadata.has_issues, Some(true));
        // Unset fields should be None
        assert_eq!(metadata.delete_branch_on_merge, None);
    }

    #[test]
    fn test_github_metadata_serialize() {
        let metadata = GitHubMetadata {
            node_id: Some("MDEwOlJlcG9zaXRvcnkxMjM0NTY=".to_string()),
            allow_squash_merge: Some(true),
            ..Default::default()
        };

        let json = serde_json::to_value(&metadata).unwrap();
        assert_eq!(json["node_id"], "MDEwOlJlcG9zaXRvcnkxMjM0NTY=");
        assert_eq!(json["allow_squash_merge"], true);
    }

    #[test]
    fn test_gitlab_metadata_deserialize() {
        let json = json!({
            "web_url": "https://gitlab.com/group/project",
            "ssh_url_to_repo": "git@gitlab.com:group/project.git",
            "namespace_id": 12345,
            "namespace_kind": "group",
            "namespace_path": "group"
        });

        let metadata: GitLabMetadata = serde_json::from_value(json).unwrap();
        assert_eq!(
            metadata.web_url,
            Some("https://gitlab.com/group/project".to_string())
        );
        assert_eq!(
            metadata.ssh_url_to_repo,
            Some("git@gitlab.com:group/project.git".to_string())
        );
        assert_eq!(metadata.namespace_id, Some(12345));
        assert_eq!(metadata.namespace_kind, Some("group".to_string()));
    }

    #[test]
    fn test_gitea_metadata_deserialize() {
        let json = json!({
            "mirror": false,
            "empty": false,
            "template": true,
            "html_url": "https://codeberg.org/org/repo",
            "ssh_url": "git@codeberg.org:org/repo.git",
            "open_issues_count": 5
        });

        let metadata: GiteaMetadata = serde_json::from_value(json).unwrap();
        assert_eq!(metadata.mirror, Some(false));
        assert_eq!(metadata.template, Some(true));
        assert_eq!(
            metadata.html_url,
            Some("https://codeberg.org/org/repo".to_string())
        );
        assert_eq!(metadata.open_issues_count, Some(5));
    }

    #[test]
    fn test_platform_metadata_from_json_github() {
        let json = json!({
            "node_id": "test123",
            "has_issues": true
        });

        let metadata = PlatformMetadata::from_json(PlatformType::GitHub, &json).unwrap();

        match metadata {
            PlatformMetadata::GitHub(m) => {
                assert_eq!(m.node_id, Some("test123".to_string()));
                assert_eq!(m.has_issues, Some(true));
            }
            _ => panic!("Expected GitHub metadata"),
        }
    }

    #[test]
    fn test_platform_metadata_from_json_gitlab() {
        let json = json!({
            "web_url": "https://gitlab.com/test",
            "namespace_kind": "group"
        });

        let metadata = PlatformMetadata::from_json(PlatformType::GitLab, &json).unwrap();

        match metadata {
            PlatformMetadata::GitLab(m) => {
                assert_eq!(m.web_url, Some("https://gitlab.com/test".to_string()));
                assert_eq!(m.namespace_kind, Some("group".to_string()));
            }
            _ => panic!("Expected GitLab metadata"),
        }
    }

    #[test]
    fn test_platform_metadata_from_json_gitea() {
        let json = json!({
            "html_url": "https://codeberg.org/test",
            "template": false
        });

        let metadata = PlatformMetadata::from_json(PlatformType::Gitea, &json).unwrap();

        match metadata {
            PlatformMetadata::Gitea(m) => {
                assert_eq!(m.html_url, Some("https://codeberg.org/test".to_string()));
                assert_eq!(m.template, Some(false));
            }
            _ => panic!("Expected Gitea metadata"),
        }
    }

    #[test]
    fn test_platform_metadata_to_json() {
        let metadata = PlatformMetadata::GitHub(GitHubMetadata {
            node_id: Some("test123".to_string()),
            allow_squash_merge: Some(true),
            ..Default::default()
        });

        let json = metadata.to_json();
        assert_eq!(json["node_id"], "test123");
        assert_eq!(json["allow_squash_merge"], true);
    }

    #[test]
    fn test_platform_metadata_platform_type() {
        assert_eq!(
            PlatformMetadata::GitHub(Default::default()).platform_type(),
            PlatformType::GitHub
        );
        assert_eq!(
            PlatformMetadata::GitLab(Default::default()).platform_type(),
            PlatformType::GitLab
        );
        assert_eq!(
            PlatformMetadata::Gitea(Default::default()).platform_type(),
            PlatformType::Gitea
        );
    }

    #[test]
    fn test_platform_metadata_web_url() {
        let github = PlatformMetadata::GitHub(Default::default());
        assert_eq!(github.web_url(), None);

        let gitlab = PlatformMetadata::GitLab(GitLabMetadata {
            web_url: Some("https://gitlab.com/test".to_string()),
            ..Default::default()
        });
        assert_eq!(gitlab.web_url(), Some("https://gitlab.com/test"));

        let gitea = PlatformMetadata::Gitea(GiteaMetadata {
            html_url: Some("https://codeberg.org/test".to_string()),
            ..Default::default()
        });
        assert_eq!(gitea.web_url(), Some("https://codeberg.org/test"));
    }

    #[test]
    fn test_platform_metadata_ssh_url() {
        let github = PlatformMetadata::GitHub(Default::default());
        assert_eq!(github.ssh_url(), None);

        let gitlab = PlatformMetadata::GitLab(GitLabMetadata {
            ssh_url_to_repo: Some("git@gitlab.com:group/project.git".to_string()),
            ..Default::default()
        });
        assert_eq!(gitlab.ssh_url(), Some("git@gitlab.com:group/project.git"));

        let gitea = PlatformMetadata::Gitea(GiteaMetadata {
            ssh_url: Some("git@gitea.example.com:org/repo.git".to_string()),
            ..Default::default()
        });
        assert_eq!(gitea.ssh_url(), Some("git@gitea.example.com:org/repo.git"));
    }

    #[test]
    fn test_empty_json_deserializes_to_defaults() {
        let json = json!({});

        let github: GitHubMetadata = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(github, GitHubMetadata::default());

        let gitlab: GitLabMetadata = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(gitlab, GitLabMetadata::default());

        let gitea: GiteaMetadata = serde_json::from_value(json).unwrap();
        assert_eq!(gitea, GiteaMetadata::default());
    }

    #[test]
    fn test_unknown_fields_ignored() {
        let json = json!({
            "node_id": "test123",
            "unknown_field": "should be ignored",
            "another_unknown": 42
        });

        let metadata: GitHubMetadata = serde_json::from_value(json).unwrap();
        assert_eq!(metadata.node_id, Some("test123".to_string()));
        // Unknown fields don't cause errors, they're just ignored
    }
}
