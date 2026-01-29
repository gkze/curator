//! Common re-exports for convenient entity usage.

pub use super::api_cache::{
    ActiveModel as ApiCacheActiveModel, Column as ApiCacheColumn, EndpointType, Entity as ApiCache,
    Model as ApiCacheModel,
};
pub use super::code_repository::{
    ActiveModel as CodeRepositoryActiveModel, Column as CodeRepositoryColumn,
    Entity as CodeRepository, Model as CodeRepositoryModel,
};
pub use super::code_visibility::CodeVisibility;
pub use super::instance::{
    ActiveModel as InstanceActiveModel, Column as InstanceColumn, Entity as Instance,
    Model as InstanceModel, well_known,
};
pub use super::platform_metadata::{
    GitHubMetadata, GitLabMetadata, GiteaMetadata, PlatformMetadata,
};
pub use super::platform_type::PlatformType;
