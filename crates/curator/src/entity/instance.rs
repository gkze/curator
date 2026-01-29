//! Instance entity - represents a specific deployment of a platform type.
//!
//! An instance is a specific deployment of a platform type, such as:
//! - `github.com` (GitHub's SaaS offering)
//! - `gitlab.com` (GitLab's SaaS offering)
//! - `gitlab.mycompany.com` (self-hosted GitLab)
//! - `codeberg.org` (public Gitea/Forgejo instance)
//! - `gitea.mycompany.com` (self-hosted Gitea)

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::entity::platform_type::PlatformType;

/// Instance model - tracks specific deployments of platform types.
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "instances")]
pub struct Model {
    /// Internal UUID primary key.
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,

    /// User-friendly name for this instance (e.g., "github", "work-gitlab", "codeberg").
    /// Must be unique across all instances.
    #[sea_orm(unique)]
    pub name: String,

    /// The type of platform software (github, gitlab, gitea).
    pub platform_type: PlatformType,

    /// The host URL for this instance (e.g., "github.com", "gitlab.mycompany.com").
    /// Does not include the protocol (https://).
    pub host: String,

    /// When this instance was first configured.
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    /// An instance has many repositories.
    #[sea_orm(has_many = "super::code_repository::Entity")]
    CodeRepositories,
    /// An instance has many API cache entries.
    #[sea_orm(has_many = "super::api_cache::Entity")]
    ApiCache,
}

impl Related<super::code_repository::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::CodeRepositories.def()
    }
}

impl Related<super::api_cache::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ApiCache.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl Model {
    /// Compute the base URL for this instance (with protocol).
    pub fn base_url(&self) -> String {
        format!("https://{}", self.host)
    }

    /// Compute the API URL for this instance.
    pub fn api_url(&self) -> String {
        match self.platform_type {
            PlatformType::GitHub => {
                if self.host == "github.com" {
                    "https://api.github.com".to_string()
                } else {
                    // GitHub Enterprise uses /api/v3
                    format!("https://{}/api/v3", self.host)
                }
            }
            PlatformType::GitLab => {
                format!("https://{}/api/v4", self.host)
            }
            PlatformType::Gitea => {
                format!("https://{}/api/v1", self.host)
            }
        }
    }

    /// Compute the web URL for a repository on this instance.
    pub fn repo_url(&self, owner: &str, name: &str) -> String {
        format!("https://{}/{}/{}", self.host, owner, name)
    }

    /// Compute the HTTPS clone URL for a repository on this instance.
    pub fn clone_url_https(&self, owner: &str, name: &str) -> String {
        format!("https://{}/{}/{}.git", self.host, owner, name)
    }

    /// Compute the SSH clone URL for a repository on this instance.
    pub fn clone_url_ssh(&self, owner: &str, name: &str) -> String {
        format!("git@{}:{}/{}.git", self.host, owner, name)
    }

    /// Check if this is the canonical GitHub instance.
    pub fn is_github_com(&self) -> bool {
        self.platform_type == PlatformType::GitHub && self.host == "github.com"
    }

    /// Check if this is the canonical GitLab instance.
    pub fn is_gitlab_com(&self) -> bool {
        self.platform_type == PlatformType::GitLab && self.host == "gitlab.com"
    }

    /// Check if this is the Codeberg instance.
    pub fn is_codeberg(&self) -> bool {
        self.platform_type == PlatformType::Gitea && self.host == "codeberg.org"
    }
}

/// Well-known instance configurations.
pub mod well_known {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    /// Create a Model for github.com
    pub fn github() -> Model {
        Model {
            id: Uuid::nil(), // Will be replaced when inserted
            name: "github".to_string(),
            platform_type: PlatformType::GitHub,
            host: "github.com".to_string(),
            created_at: Utc::now().fixed_offset(),
        }
    }

    /// Create a Model for gitlab.com
    pub fn gitlab() -> Model {
        Model {
            id: Uuid::nil(),
            name: "gitlab".to_string(),
            platform_type: PlatformType::GitLab,
            host: "gitlab.com".to_string(),
            created_at: Utc::now().fixed_offset(),
        }
    }

    /// Create a Model for codeberg.org
    pub fn codeberg() -> Model {
        Model {
            id: Uuid::nil(),
            name: "codeberg".to_string(),
            platform_type: PlatformType::Gitea,
            host: "codeberg.org".to_string(),
            created_at: Utc::now().fixed_offset(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_instance(platform_type: PlatformType, host: &str) -> Model {
        Model {
            id: Uuid::new_v4(),
            name: "test".to_string(),
            platform_type,
            host: host.to_string(),
            created_at: Utc::now().fixed_offset(),
        }
    }

    #[test]
    fn test_api_url_github() {
        let instance = make_instance(PlatformType::GitHub, "github.com");
        assert_eq!(instance.api_url(), "https://api.github.com");

        let ghe = make_instance(PlatformType::GitHub, "github.mycompany.com");
        assert_eq!(ghe.api_url(), "https://github.mycompany.com/api/v3");
    }

    #[test]
    fn test_api_url_gitlab() {
        let instance = make_instance(PlatformType::GitLab, "gitlab.com");
        assert_eq!(instance.api_url(), "https://gitlab.com/api/v4");

        let self_hosted = make_instance(PlatformType::GitLab, "gitlab.mycompany.com");
        assert_eq!(self_hosted.api_url(), "https://gitlab.mycompany.com/api/v4");
    }

    #[test]
    fn test_api_url_gitea() {
        let codeberg = make_instance(PlatformType::Gitea, "codeberg.org");
        assert_eq!(codeberg.api_url(), "https://codeberg.org/api/v1");

        let self_hosted = make_instance(PlatformType::Gitea, "gitea.mycompany.com");
        assert_eq!(self_hosted.api_url(), "https://gitea.mycompany.com/api/v1");
    }

    #[test]
    fn test_repo_urls() {
        let instance = make_instance(PlatformType::GitHub, "github.com");
        assert_eq!(
            instance.repo_url("octocat", "hello"),
            "https://github.com/octocat/hello"
        );
        assert_eq!(
            instance.clone_url_https("octocat", "hello"),
            "https://github.com/octocat/hello.git"
        );
        assert_eq!(
            instance.clone_url_ssh("octocat", "hello"),
            "git@github.com:octocat/hello.git"
        );
    }

    #[test]
    fn test_well_known_instances() {
        let github = well_known::github();
        assert!(github.is_github_com());

        let gitlab = well_known::gitlab();
        assert!(gitlab.is_gitlab_com());

        let codeberg = well_known::codeberg();
        assert!(codeberg.is_codeberg());
    }
}
