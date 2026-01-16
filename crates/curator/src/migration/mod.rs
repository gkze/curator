//! Database migrations for the curator schema.
//!
//! This module is only available when the `migrate` feature is enabled.

pub use sea_orm_migration::prelude::*;

mod m20250114_000001_create_schema;

/// The migrator that runs all migrations.
pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(m20250114_000001_create_schema::Migration)]
    }

    fn migration_table_name() -> SeaRc<dyn Iden> {
        SeaRc::new(Alias::new("curator_migrations"))
    }
}
