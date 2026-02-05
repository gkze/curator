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

/// Well-known instance definitions - the single source of truth.
///
/// This module centralizes all well-known instance configurations to avoid
/// duplication across the codebase.
pub mod well_known {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    /// Definition of a well-known instance (compile-time data).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct WellKnownInstance {
        /// Canonical name (e.g., "github", "gitlab", "codeberg")
        pub name: &'static str,
        /// Platform type
        pub platform_type: PlatformType,
        /// Host URL without protocol
        pub host: &'static str,
    }

    impl WellKnownInstance {
        /// Convert to a Model instance (with nil UUID, to be replaced on insert).
        pub fn to_model(&self) -> Model {
            Model {
                id: Uuid::nil(),
                name: self.name.to_string(),
                platform_type: self.platform_type,
                host: self.host.to_string(),
                created_at: Utc::now().fixed_offset(),
            }
        }
    }

    /// All well-known instances - the canonical source of truth.
    pub const INSTANCES: &[WellKnownInstance] = &[
        WellKnownInstance {
            name: "github",
            platform_type: PlatformType::GitHub,
            host: "github.com",
        },
        WellKnownInstance {
            name: "gitlab",
            platform_type: PlatformType::GitLab,
            host: "gitlab.com",
        },
        WellKnownInstance {
            name: "gnome-gitlab",
            platform_type: PlatformType::GitLab,
            host: "gitlab.gnome.org",
        },
        WellKnownInstance {
            name: "freedesktop-gitlab",
            platform_type: PlatformType::GitLab,
            host: "gitlab.freedesktop.org",
        },
        WellKnownInstance {
            name: "kde-gitlab",
            platform_type: PlatformType::GitLab,
            host: "invent.kde.org",
        },
        WellKnownInstance {
            name: "kitware-gitlab",
            platform_type: PlatformType::GitLab,
            host: "gitlab.kitware.com",
        },
        WellKnownInstance {
            name: "haskell-gitlab",
            platform_type: PlatformType::GitLab,
            host: "gitlab.haskell.org",
        },
        WellKnownInstance {
            name: "archlinux-gitlab",
            platform_type: PlatformType::GitLab,
            host: "gitlab.archlinux.org",
        },
        WellKnownInstance {
            name: "codeberg",
            platform_type: PlatformType::Gitea,
            host: "codeberg.org",
        },
    ];

    /// Look up a well-known instance by name (case-insensitive).
    pub fn by_name(name: &str) -> Option<&'static WellKnownInstance> {
        let lower = name.to_lowercase();
        INSTANCES.iter().find(|i| i.name == lower)
    }

    /// Look up a well-known instance by platform type and host.
    pub fn by_platform_and_host(
        platform_type: PlatformType,
        host: &str,
    ) -> Option<&'static WellKnownInstance> {
        INSTANCES
            .iter()
            .find(|i| i.platform_type == platform_type && i.host == host)
    }

    /// Check if a name refers to a well-known instance.
    pub fn is_well_known_name(name: &str) -> bool {
        by_name(name).is_some()
    }

    /// Check if a platform/host combo is a well-known instance.
    pub fn is_well_known(platform_type: PlatformType, host: &str) -> bool {
        by_platform_and_host(platform_type, host).is_some()
    }

    // Convenience functions for backward compatibility

    /// Create a Model for github.com
    pub fn github() -> Model {
        by_name("github").unwrap().to_model()
    }

    /// Create a Model for gitlab.com
    pub fn gitlab() -> Model {
        by_name("gitlab").unwrap().to_model()
    }

    /// Create a Model for codeberg.org
    pub fn codeberg() -> Model {
        by_name("codeberg").unwrap().to_model()
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

    #[test]
    fn test_well_known_by_name() {
        // Case-insensitive lookup
        assert!(well_known::by_name("github").is_some());
        assert!(well_known::by_name("GitHub").is_some());
        assert!(well_known::by_name("GITHUB").is_some());
        assert!(well_known::by_name("gitlab").is_some());
        assert!(well_known::by_name("codeberg").is_some());

        // Unknown names return None
        assert!(well_known::by_name("unknown").is_none());
        assert!(well_known::by_name("gitea").is_none()); // gitea != codeberg
    }

    #[test]
    fn test_well_known_by_platform_and_host() {
        // Exact matches
        assert!(well_known::by_platform_and_host(PlatformType::GitHub, "github.com").is_some());
        assert!(well_known::by_platform_and_host(PlatformType::GitLab, "gitlab.com").is_some());
        assert!(well_known::by_platform_and_host(PlatformType::Gitea, "codeberg.org").is_some());

        // Wrong combos return None
        assert!(well_known::by_platform_and_host(PlatformType::GitHub, "gitlab.com").is_none());
        assert!(well_known::by_platform_and_host(PlatformType::GitLab, "github.com").is_none());
        assert!(well_known::by_platform_and_host(PlatformType::Gitea, "gitea.io").is_none());
    }

    #[test]
    fn test_well_known_is_well_known() {
        assert!(well_known::is_well_known_name("github"));
        assert!(well_known::is_well_known_name("gitlab"));
        assert!(well_known::is_well_known_name("codeberg"));
        assert!(!well_known::is_well_known_name("unknown"));

        assert!(well_known::is_well_known(
            PlatformType::GitHub,
            "github.com"
        ));
        assert!(well_known::is_well_known(
            PlatformType::GitLab,
            "gitlab.com"
        ));
        assert!(well_known::is_well_known(
            PlatformType::Gitea,
            "codeberg.org"
        ));
        assert!(!well_known::is_well_known(
            PlatformType::GitHub,
            "ghe.company.com"
        ));
    }

    #[test]
    fn test_well_known_instance_to_model() {
        let wk = well_known::by_name("github").unwrap();
        let model = wk.to_model();

        assert_eq!(model.name, "github");
        assert_eq!(model.platform_type, PlatformType::GitHub);
        assert_eq!(model.host, "github.com");
        assert_eq!(model.id, Uuid::nil()); // Nil until inserted
    }

    #[test]
    fn test_well_known_instances_constant() {
        // Verify the INSTANCES constant has exactly 9 entries
        assert_eq!(well_known::INSTANCES.len(), 9);

        // Verify each has unique name and host
        let names: Vec<_> = well_known::INSTANCES.iter().map(|i| i.name).collect();
        let hosts: Vec<_> = well_known::INSTANCES.iter().map(|i| i.host).collect();

        assert!(names.contains(&"github"));
        assert!(names.contains(&"gitlab"));
        assert!(names.contains(&"gnome-gitlab"));
        assert!(names.contains(&"freedesktop-gitlab"));
        assert!(names.contains(&"kde-gitlab"));
        assert!(names.contains(&"kitware-gitlab"));
        assert!(names.contains(&"haskell-gitlab"));
        assert!(names.contains(&"archlinux-gitlab"));
        assert!(names.contains(&"codeberg"));

        assert!(hosts.contains(&"github.com"));
        assert!(hosts.contains(&"gitlab.com"));
        assert!(hosts.contains(&"gitlab.gnome.org"));
        assert!(hosts.contains(&"gitlab.freedesktop.org"));
        assert!(hosts.contains(&"invent.kde.org"));
        assert!(hosts.contains(&"gitlab.kitware.com"));
        assert!(hosts.contains(&"gitlab.haskell.org"));
        assert!(hosts.contains(&"gitlab.archlinux.org"));
        assert!(hosts.contains(&"codeberg.org"));
    }
}
