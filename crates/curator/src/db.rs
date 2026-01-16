//! Database connection utilities.

use sea_orm::{Database, DatabaseConnection, DbErr};

/// Establish a connection to the database.
///
/// # Arguments
/// * `database_url` - PostgreSQL connection string (e.g., `postgres:///curator_dev`)
///
/// # Errors
/// Returns `DbErr` if the connection cannot be established.
pub async fn connect(database_url: &str) -> Result<DatabaseConnection, DbErr> {
    Database::connect(database_url).await
}

/// Establish a connection to the database and run all pending migrations.
///
/// This is the recommended way to initialize the database for applications
/// using curator. It ensures the schema is always up-to-date.
///
/// # Arguments
/// * `database_url` - PostgreSQL connection string (e.g., `postgres:///curator_dev`)
///
/// # Errors
/// Returns `DbErr` if the connection cannot be established or migrations fail.
///
/// # Example
/// ```ignore
/// let db = curator::connect_and_migrate("postgres:///my_app").await?;
/// ```
#[cfg(feature = "migrate")]
pub async fn connect_and_migrate(database_url: &str) -> Result<DatabaseConnection, DbErr> {
    use sea_orm_migration::MigratorTrait;

    let db = Database::connect(database_url).await?;
    crate::migration::Migrator::up(&db, None).await?;
    Ok(db)
}
