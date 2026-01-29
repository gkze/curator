//! Platform type enum for type-safe forge platform handling.
//!
//! This represents the *type* of platform software, not a specific instance.
//! For instance-specific data, see the `instance` entity.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

/// Supported code forge platform types.
///
/// This enum represents the type of platform software, not a specific deployment.
/// For example, both `github.com` and a GitHub Enterprise instance would have
/// the same `PlatformType::GitHub`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::None)")]
pub enum PlatformType {
    /// GitHub (github.com or GitHub Enterprise)
    #[sea_orm(string_value = "github")]
    GitHub,
    /// GitLab (gitlab.com or self-hosted GitLab)
    #[sea_orm(string_value = "gitlab")]
    GitLab,
    /// Gitea or Forgejo (includes Codeberg and other instances)
    #[sea_orm(string_value = "gitea")]
    Gitea,
}

impl std::fmt::Display for PlatformType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlatformType::GitHub => write!(f, "github"),
            PlatformType::GitLab => write!(f, "gitlab"),
            PlatformType::Gitea => write!(f, "gitea"),
        }
    }
}

impl std::str::FromStr for PlatformType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "github" => Ok(PlatformType::GitHub),
            "gitlab" => Ok(PlatformType::GitLab),
            "gitea" | "forgejo" | "codeberg" => Ok(PlatformType::Gitea),
            _ => Err(format!("Unknown platform type: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(PlatformType::GitHub.to_string(), "github");
        assert_eq!(PlatformType::GitLab.to_string(), "gitlab");
        assert_eq!(PlatformType::Gitea.to_string(), "gitea");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            "github".parse::<PlatformType>().unwrap(),
            PlatformType::GitHub
        );
        assert_eq!(
            "gitlab".parse::<PlatformType>().unwrap(),
            PlatformType::GitLab
        );
        assert_eq!(
            "gitea".parse::<PlatformType>().unwrap(),
            PlatformType::Gitea
        );
        assert_eq!(
            "forgejo".parse::<PlatformType>().unwrap(),
            PlatformType::Gitea
        );
        assert_eq!(
            "codeberg".parse::<PlatformType>().unwrap(),
            PlatformType::Gitea
        );
    }
}
