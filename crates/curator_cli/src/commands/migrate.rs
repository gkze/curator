use curator::db;
use curator::migration::{Migrator, MigratorTrait};

use crate::MigrateAction;

pub(crate) async fn handle_migrate(
    action: MigrateAction,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = db::connect(database_url).await?;

    match action {
        MigrateAction::Up => {
            println!("Applying migrations...");
            Migrator::up(&db, None).await?;
            println!("Migrations applied successfully.");
        }
        MigrateAction::Down => {
            println!("Rolling back last migration...");
            Migrator::down(&db, Some(1)).await?;
            println!("Rollback complete.");
        }
        MigrateAction::Status => {
            println!("Migration status:");
            Migrator::status(&db).await?;
        }
        MigrateAction::Fresh => {
            println!("Dropping all tables and reapplying migrations...");
            Migrator::fresh(&db).await?;
            println!("Fresh migration complete.");
        }
    }

    Ok(())
}
