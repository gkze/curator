//! Initial migration to create the curator database schema.
//!
//! This schema uses an instance-based model where platforms (GitHub, GitLab, Gitea)
//! can have multiple instances (e.g., github.com, gitlab.com, gitlab.mycompany.com).

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create tables in dependency order
        self.create_instances(manager).await?;
        // Box::pin to avoid large future on the stack (30KB+)
        Box::pin(self.create_code_repositories(manager)).await?;
        self.create_api_cache(manager).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(ApiCache::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(CodeRepositories::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(Instances::Table).to_owned())
            .await?;
        Ok(())
    }
}

impl Migration {
    /// Create the instances table for tracking platform instances.
    ///
    /// An instance represents a specific deployment of a platform type, e.g.:
    /// - name: "github", platform_type: "github", host: "github.com"
    /// - name: "work-gitlab", platform_type: "gitlab", host: "gitlab.mycompany.com"
    /// - name: "codeberg", platform_type: "gitea", host: "codeberg.org"
    async fn create_instances(&self, manager: &SchemaManager<'_>) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Instances::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Instances::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(Instances::Name)
                            .string()
                            .not_null()
                            .unique_key(),
                    )
                    .col(ColumnDef::new(Instances::PlatformType).string().not_null())
                    .col(ColumnDef::new(Instances::Host).string().not_null())
                    .col(
                        ColumnDef::new(Instances::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        // Index on platform_type for filtering by type
        manager
            .create_index(
                Index::create()
                    .name("idx_instances_platform_type")
                    .table(Instances::Table)
                    .col(Instances::PlatformType)
                    .to_owned(),
            )
            .await?;

        // Unique constraint on (platform_type, host) to prevent duplicate instances
        manager
            .create_index(
                Index::create()
                    .name("idx_instances_platform_type_host")
                    .table(Instances::Table)
                    .col(Instances::PlatformType)
                    .col(Instances::Host)
                    .unique()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn create_code_repositories(&self, manager: &SchemaManager<'_>) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(CodeRepositories::Table)
                    .if_not_exists()
                    // Internal
                    .col(
                        ColumnDef::new(CodeRepositories::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    // Instance reference (replaces platform column)
                    .col(
                        ColumnDef::new(CodeRepositories::InstanceId)
                            .uuid()
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_code_repos_instance")
                            .from(CodeRepositories::Table, CodeRepositories::InstanceId)
                            .to(Instances::Table, Instances::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    // Platform-specific ID (unique within the instance)
                    .col(
                        ColumnDef::new(CodeRepositories::PlatformId)
                            .big_integer()
                            .not_null(),
                    )
                    // Naming
                    .col(ColumnDef::new(CodeRepositories::Owner).string().not_null())
                    .col(ColumnDef::new(CodeRepositories::Name).string().not_null())
                    // Content
                    .col(ColumnDef::new(CodeRepositories::Description).text().null())
                    .col(
                        ColumnDef::new(CodeRepositories::DefaultBranch)
                            .string()
                            .not_null()
                            .default("main"),
                    )
                    .col(
                        ColumnDef::new(CodeRepositories::Topics)
                            .json()
                            .not_null()
                            .default(Expr::cust("'[]'")),
                    )
                    .col(
                        ColumnDef::new(CodeRepositories::PrimaryLanguage)
                            .string()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(CodeRepositories::LicenseSpdx)
                            .string()
                            .null(),
                    )
                    .col(ColumnDef::new(CodeRepositories::Homepage).text().null())
                    // Visibility
                    .col(
                        ColumnDef::new(CodeRepositories::Visibility)
                            .string()
                            .not_null()
                            .default("public"),
                    )
                    .col(
                        ColumnDef::new(CodeRepositories::IsFork)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(CodeRepositories::IsMirror)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(CodeRepositories::IsArchived)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(CodeRepositories::IsTemplate)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(CodeRepositories::IsEmpty)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    // Statistics
                    .col(ColumnDef::new(CodeRepositories::Stars).integer().null())
                    .col(ColumnDef::new(CodeRepositories::Forks).integer().null())
                    .col(
                        ColumnDef::new(CodeRepositories::OpenIssues)
                            .integer()
                            .null(),
                    )
                    .col(ColumnDef::new(CodeRepositories::Watchers).integer().null())
                    .col(
                        ColumnDef::new(CodeRepositories::SizeKb)
                            .big_integer()
                            .null(),
                    )
                    // Features
                    .col(
                        ColumnDef::new(CodeRepositories::HasIssues)
                            .boolean()
                            .not_null()
                            .default(true),
                    )
                    .col(
                        ColumnDef::new(CodeRepositories::HasWiki)
                            .boolean()
                            .not_null()
                            .default(true),
                    )
                    .col(
                        ColumnDef::new(CodeRepositories::HasPullRequests)
                            .boolean()
                            .not_null()
                            .default(true),
                    )
                    // Timestamps
                    .col(
                        ColumnDef::new(CodeRepositories::CreatedAt)
                            .timestamp_with_time_zone()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(CodeRepositories::UpdatedAt)
                            .timestamp_with_time_zone()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(CodeRepositories::PushedAt)
                            .timestamp_with_time_zone()
                            .null(),
                    )
                    // Platform-specific JSON
                    .col(
                        ColumnDef::new(CodeRepositories::PlatformMetadata)
                            .json()
                            .not_null()
                            .default(Expr::cust("'{}'")),
                    )
                    // Tracking
                    .col(
                        ColumnDef::new(CodeRepositories::SyncedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    // Caching
                    .col(ColumnDef::new(CodeRepositories::Etag).text().null())
                    .to_owned(),
            )
            .await?;

        // Unique constraint on (instance_id, owner, name)
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_instance_owner_name")
                    .table(CodeRepositories::Table)
                    .col(CodeRepositories::InstanceId)
                    .col(CodeRepositories::Owner)
                    .col(CodeRepositories::Name)
                    .unique()
                    .to_owned(),
            )
            .await?;

        // Index on instance_id
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_instance")
                    .table(CodeRepositories::Table)
                    .col(CodeRepositories::InstanceId)
                    .to_owned(),
            )
            .await?;

        // Index on platform_id
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_platform_id")
                    .table(CodeRepositories::Table)
                    .col(CodeRepositories::PlatformId)
                    .to_owned(),
            )
            .await?;

        // Index on stars (descending)
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_stars")
                    .table(CodeRepositories::Table)
                    .col((CodeRepositories::Stars, IndexOrder::Desc))
                    .to_owned(),
            )
            .await?;

        // Index on primary_language
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_language")
                    .table(CodeRepositories::Table)
                    .col(CodeRepositories::PrimaryLanguage)
                    .to_owned(),
            )
            .await?;

        // Index on visibility
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_visibility")
                    .table(CodeRepositories::Table)
                    .col(CodeRepositories::Visibility)
                    .to_owned(),
            )
            .await?;

        // Index on synced_at
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_synced")
                    .table(CodeRepositories::Table)
                    .col(CodeRepositories::SyncedAt)
                    .to_owned(),
            )
            .await?;

        // Composite index on (instance_id, platform_id) - unique within instance
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_instance_platform_id")
                    .table(CodeRepositories::Table)
                    .col(CodeRepositories::InstanceId)
                    .col(CodeRepositories::PlatformId)
                    .unique()
                    .to_owned(),
            )
            .await?;

        // Composite index on (instance_id, owner)
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_instance_owner")
                    .table(CodeRepositories::Table)
                    .col(CodeRepositories::InstanceId)
                    .col(CodeRepositories::Owner)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn create_api_cache(&self, manager: &SchemaManager<'_>) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(ApiCache::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(ApiCache::Id).uuid().not_null().primary_key())
                    .col(ColumnDef::new(ApiCache::InstanceId).uuid().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_api_cache_instance")
                            .from(ApiCache::Table, ApiCache::InstanceId)
                            .to(Instances::Table, Instances::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .col(ColumnDef::new(ApiCache::EndpointType).string().not_null())
                    .col(ColumnDef::new(ApiCache::CacheKey).string().not_null())
                    .col(ColumnDef::new(ApiCache::Etag).text().null())
                    .col(ColumnDef::new(ApiCache::TotalPages).integer().null())
                    .col(
                        ColumnDef::new(ApiCache::CachedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        // Unique constraint on (instance_id, endpoint_type, cache_key)
        manager
            .create_index(
                Index::create()
                    .name("idx_api_cache_lookup")
                    .table(ApiCache::Table)
                    .col(ApiCache::InstanceId)
                    .col(ApiCache::EndpointType)
                    .col(ApiCache::CacheKey)
                    .unique()
                    .to_owned(),
            )
            .await?;

        // Index on cached_at
        manager
            .create_index(
                Index::create()
                    .name("idx_api_cache_cached_at")
                    .table(ApiCache::Table)
                    .col(ApiCache::CachedAt)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
#[sea_orm(iden = "instances")]
enum Instances {
    Table,
    Id,
    Name,
    PlatformType,
    Host,
    CreatedAt,
}

#[derive(DeriveIden)]
#[sea_orm(iden = "code_repositories")]
enum CodeRepositories {
    Table,
    Id,
    InstanceId,
    PlatformId,
    Owner,
    Name,
    Description,
    DefaultBranch,
    Topics,
    PrimaryLanguage,
    LicenseSpdx,
    Homepage,
    Visibility,
    IsFork,
    IsMirror,
    IsArchived,
    IsTemplate,
    IsEmpty,
    Stars,
    Forks,
    OpenIssues,
    Watchers,
    SizeKb,
    HasIssues,
    HasWiki,
    HasPullRequests,
    CreatedAt,
    UpdatedAt,
    PushedAt,
    PlatformMetadata,
    SyncedAt,
    Etag,
}

#[derive(DeriveIden)]
#[sea_orm(iden = "api_cache")]
enum ApiCache {
    Table,
    Id,
    InstanceId,
    EndpointType,
    CacheKey,
    Etag,
    TotalPages,
    CachedAt,
}
