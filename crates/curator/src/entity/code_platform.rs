//! Platform enum for type-safe forge platform handling.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

/// Supported code forge platforms.
#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::None)")]
pub enum CodePlatform {
    #[sea_orm(string_value = "github")]
    GitHub,
    #[sea_orm(string_value = "gitlab")]
    GitLab,
    #[sea_orm(string_value = "codeberg")]
    Codeberg,
    #[sea_orm(string_value = "gitea")]
    Gitea,
}

impl std::fmt::Display for CodePlatform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodePlatform::GitHub => write!(f, "github"),
            CodePlatform::GitLab => write!(f, "gitlab"),
            CodePlatform::Codeberg => write!(f, "codeberg"),
            CodePlatform::Gitea => write!(f, "gitea"),
        }
    }
}
