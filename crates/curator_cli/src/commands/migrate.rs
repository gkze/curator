use curator::db;
use curator::migration::{Migrator, MigratorTrait};

use crate::MigrateAction;

fn action_messages(action: &MigrateAction) -> (&'static str, Option<&'static str>) {
    match action {
        MigrateAction::Up => (
            "Applying migrations...",
            Some("Migrations applied successfully."),
        ),
        MigrateAction::Down => ("Rolling back last migration...", Some("Rollback complete.")),
        MigrateAction::Status => ("Migration status:", None),
        MigrateAction::Fresh => (
            "Dropping all tables and reapplying migrations...",
            Some("Fresh migration complete."),
        ),
    }
}

pub(crate) async fn handle_migrate(
    action: MigrateAction,
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = db::connect(database_url).await?;

    let (before_message, after_message) = action_messages(&action);
    println!("{before_message}");

    match action {
        MigrateAction::Up => {
            Migrator::up(&db, None).await?;
        }
        MigrateAction::Down => {
            Migrator::down(&db, Some(1)).await?;
        }
        MigrateAction::Status => {
            Migrator::status(&db).await?;
        }
        MigrateAction::Fresh => {
            Migrator::fresh(&db).await?;
        }
    }

    if let Some(after_message) = after_message {
        println!("{after_message}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_messages_cover_all_migration_actions() {
        assert_eq!(
            action_messages(&MigrateAction::Up),
            (
                "Applying migrations...",
                Some("Migrations applied successfully.")
            )
        );
        assert_eq!(
            action_messages(&MigrateAction::Down),
            ("Rolling back last migration...", Some("Rollback complete."))
        );
        assert_eq!(
            action_messages(&MigrateAction::Status),
            ("Migration status:", None)
        );
        assert_eq!(
            action_messages(&MigrateAction::Fresh),
            (
                "Dropping all tables and reapplying migrations...",
                Some("Fresh migration complete.")
            )
        );
    }
}
