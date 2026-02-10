//! Add auth configuration columns to instances.
//!
//! This allows OAuth configuration to be stored per instance, enabling custom
//! and self-hosted instances to use OAuth without hardcoded source changes.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Instances::Table)
                    .add_column(ColumnDef::new(Instances::OauthClientId).string().null())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Instances::Table)
                    .add_column(
                        ColumnDef::new(Instances::OauthFlow)
                            .string()
                            .not_null()
                            .default("auto"),
                    )
                    .to_owned(),
            )
            .await?;

        // Backfill well-known OAuth Client IDs into existing rows.
        let db = manager.get_connection();
        for (name, client_id) in [
            ("github", "Ov23liN0721EfoUpRrLl"),
            (
                "gitlab",
                "eba8ea9cbb5e8ddd455a3b3db35871963d8aa6b0a344a4b8c8e34ae8d71f336f",
            ),
            (
                "kitware-gitlab",
                "2860b6473e16b639ccb37ce9ffdc6643cd5d09f6e55168621a72f6d687f3c637",
            ),
            ("codeberg", "dfe120ce-2440-4f13-8bb0-9ba5620542a7"),
        ] {
            let sql = format!(
                "UPDATE instances SET oauth_client_id = '{}' WHERE name = '{}'",
                client_id, name
            );
            db.execute_unprepared(&sql).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Instances::Table)
                    .drop_column(Instances::OauthFlow)
                    .drop_column(Instances::OauthClientId)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
#[sea_orm(iden = "instances")]
enum Instances {
    Table,
    OauthClientId,
    OauthFlow,
}
