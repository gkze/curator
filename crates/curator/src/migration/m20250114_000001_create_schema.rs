//! Initial migration to create the curator database schema.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        self.create_code_repositories(manager).await?;
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
        Ok(())
    }
}

impl Migration {
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
                    // Platform identity
                    .col(
                        ColumnDef::new(CodeRepositories::Platform)
                            .string()
                            .not_null(),
                    )
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

        // Unique constraint on (platform, owner, name)
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_platform_owner_name")
                    .table(CodeRepositories::Table)
                    .col(CodeRepositories::Platform)
                    .col(CodeRepositories::Owner)
                    .col(CodeRepositories::Name)
                    .unique()
                    .to_owned(),
            )
            .await?;

        // Index on platform
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_platform")
                    .table(CodeRepositories::Table)
                    .col(CodeRepositories::Platform)
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

        // Composite index on (platform, platform_id)
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_platform_platform_id")
                    .table(CodeRepositories::Table)
                    .col(CodeRepositories::Platform)
                    .col(CodeRepositories::PlatformId)
                    .unique()
                    .to_owned(),
            )
            .await?;

        // Composite index on (platform, owner)
        manager
            .create_index(
                Index::create()
                    .name("idx_code_repos_platform_owner")
                    .table(CodeRepositories::Table)
                    .col(CodeRepositories::Platform)
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
                    .col(ColumnDef::new(ApiCache::Platform).string().not_null())
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

        // Unique constraint on (platform, endpoint_type, cache_key)
        manager
            .create_index(
                Index::create()
                    .name("idx_api_cache_lookup")
                    .table(ApiCache::Table)
                    .col(ApiCache::Platform)
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
#[sea_orm(iden = "code_repositories")]
enum CodeRepositories {
    Table,
    Id,
    Platform,
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
    Platform,
    EndpointType,
    CacheKey,
    Etag,
    TotalPages,
    CachedAt,
}
