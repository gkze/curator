//! Visibility enum for repository access levels.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

/// Repository visibility levels (normalized across platforms).
#[derive(
    Clone, Debug, Default, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::None)")]
pub enum CodeVisibility {
    #[sea_orm(string_value = "public")]
    #[default]
    Public,
    #[sea_orm(string_value = "private")]
    Private,
    /// GitLab-specific: visible to logged-in users within the instance.
    #[sea_orm(string_value = "internal")]
    Internal,
}

impl std::fmt::Display for CodeVisibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodeVisibility::Public => write!(f, "public"),
            CodeVisibility::Private => write!(f, "private"),
            CodeVisibility::Internal => write!(f, "internal"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_public() {
        assert_eq!(CodeVisibility::default(), CodeVisibility::Public);
    }

    #[test]
    fn display_outputs_expected_strings() {
        assert_eq!(CodeVisibility::Public.to_string(), "public");
        assert_eq!(CodeVisibility::Private.to_string(), "private");
        assert_eq!(CodeVisibility::Internal.to_string(), "internal");
    }
}
