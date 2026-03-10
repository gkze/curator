//! Add per-instance credential storage.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(InstanceCredentials::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(InstanceCredentials::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(InstanceCredentials::InstanceId)
                            .uuid()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(InstanceCredentials::Backend)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(InstanceCredentials::AuthKind)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(InstanceCredentials::AccessToken)
                            .text()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(InstanceCredentials::RefreshToken)
                            .text()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(InstanceCredentials::TokenExpiresAt)
                            .big_integer()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(InstanceCredentials::TokenType)
                            .string()
                            .null(),
                    )
                    .col(ColumnDef::new(InstanceCredentials::Scopes).json().null())
                    .col(
                        ColumnDef::new(InstanceCredentials::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(InstanceCredentials::UpdatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_instance_credentials_instance")
                            .from(InstanceCredentials::Table, InstanceCredentials::InstanceId)
                            .to(Instances::Table, Instances::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_instance_credentials_instance_id")
                    .table(InstanceCredentials::Table)
                    .col(InstanceCredentials::InstanceId)
                    .unique()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(InstanceCredentials::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum Instances {
    Table,
    Id,
}

#[derive(DeriveIden)]
enum InstanceCredentials {
    Table,
    Id,
    InstanceId,
    Backend,
    AuthKind,
    AccessToken,
    RefreshToken,
    TokenExpiresAt,
    TokenType,
    Scopes,
    CreatedAt,
    UpdatedAt,
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm_migration::MigratorTrait;

    struct TestMigrator;

    #[async_trait::async_trait]
    impl MigratorTrait for TestMigrator {
        fn migrations() -> Vec<Box<dyn MigrationTrait>> {
            vec![Box::new(Migration)]
        }
    }

    #[tokio::test]
    async fn migration_up_and_down_succeed() {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("in-memory db should connect");
        let schema = SchemaManager::new(&db);
        schema
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
                    .to_owned(),
            )
            .await
            .expect("instances table should exist");

        TestMigrator::up(&db, None)
            .await
            .expect("migration up should succeed");
        TestMigrator::down(&db, Some(1))
            .await
            .expect("migration down should succeed");
    }
}
