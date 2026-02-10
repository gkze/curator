//! Database migrations for the curator schema.
//!
//! This module is only available when the `migrate` feature is enabled.

pub use sea_orm_migration::prelude::*;

mod m20250114_000001_create_schema;
mod m20250114_000002_seed_well_known_instances;
mod m20250205_000001_seed_gitlab_instances;
mod m20260210_000001_add_instance_oauth_client_id;

/// The migrator that runs all migrations.
pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20250114_000001_create_schema::Migration),
            Box::new(m20250114_000002_seed_well_known_instances::Migration),
            Box::new(m20250205_000001_seed_gitlab_instances::Migration),
            Box::new(m20260210_000001_add_instance_oauth_client_id::Migration),
        ]
    }

    fn migration_table_name() -> SeaRc<dyn Iden> {
        SeaRc::new(Alias::new("curator_migrations"))
    }
}
