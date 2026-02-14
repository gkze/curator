//! Database connection utilities.

use sea_orm::{Database, DatabaseConnection, DbErr};

/// Configure SQLite-specific pragmas for better performance and concurrency.
///
/// This sets:
/// - `journal_mode=WAL` - Write-ahead logging for better concurrent access
/// - `busy_timeout=5000` - Wait up to 5 seconds for locks instead of failing immediately
/// - `synchronous=NORMAL` - Good balance of safety and performance with WAL
async fn configure_sqlite(db: &DatabaseConnection) -> Result<(), DbErr> {
    use sea_orm::{ConnectionTrait, Statement};

    // Enable WAL mode for better concurrency (readers don't block writers)
    db.execute(Statement::from_string(
        db.get_database_backend(),
        "PRAGMA journal_mode=WAL".to_string(),
    ))
    .await?;

    // Wait up to 5 seconds for locks instead of failing immediately
    db.execute(Statement::from_string(
        db.get_database_backend(),
        "PRAGMA busy_timeout=5000".to_string(),
    ))
    .await?;

    // NORMAL synchronous is safe with WAL and faster than FULL
    db.execute(Statement::from_string(
        db.get_database_backend(),
        "PRAGMA synchronous=NORMAL".to_string(),
    ))
    .await?;

    Ok(())
}

/// Establish a connection to the database.
///
/// For SQLite databases, this automatically configures:
/// - WAL journal mode for better concurrency
/// - 5 second busy timeout to handle lock contention
/// - NORMAL synchronous mode for better performance
///
/// # Arguments
/// * `database_url` - Database connection string (e.g., `sqlite:///path/to/db` or `postgres:///curator_dev`)
///
/// # Errors
/// Returns `DbErr` if the connection cannot be established.
pub async fn connect(database_url: &str) -> Result<DatabaseConnection, DbErr> {
    let db = Database::connect(database_url).await?;

    // Configure SQLite-specific settings
    if database_url.starts_with("sqlite://") {
        configure_sqlite(&db).await?;
    }

    Ok(db)
}

/// Establish a connection to the database and run all pending migrations.
///
/// This is the recommended way to initialize the database for applications
/// using curator. It ensures the schema is always up-to-date.
///
/// For SQLite databases, this automatically configures:
/// - WAL journal mode for better concurrency
/// - 5 second busy timeout to handle lock contention
/// - NORMAL synchronous mode for better performance
///
/// # Arguments
/// * `database_url` - Database connection string (e.g., `sqlite:///path/to/db` or `postgres:///curator_dev`)
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

    // Configure SQLite-specific settings
    if database_url.starts_with("sqlite://") {
        configure_sqlite(&db).await?;
    }

    crate::migration::Migrator::up(&db, None).await?;
    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{DatabaseBackend, MockDatabase, MockExecResult};

    #[tokio::test]
    async fn configure_sqlite_runs_all_pragmas() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_exec_results([
                MockExecResult {
                    rows_affected: 0,
                    last_insert_id: 0,
                },
                MockExecResult {
                    rows_affected: 0,
                    last_insert_id: 0,
                },
                MockExecResult {
                    rows_affected: 0,
                    last_insert_id: 0,
                },
            ])
            .into_connection();

        configure_sqlite(&db)
            .await
            .expect("mock sqlite pragma execs should succeed");
    }

    #[tokio::test]
    async fn connect_returns_error_for_invalid_database_url() {
        let err = connect("this-is-not-a-db-url")
            .await
            .expect_err("invalid URL should error");
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("error") || msg.contains("invalid"),
            "unexpected error message: {err}"
        );
    }
}
