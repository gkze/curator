//! Seed migration to insert well-known public GitLab instances.
//!
//! This migration adds popular public GitLab instances beyond gitlab.com:
//! GNOME, freedesktop.org, KDE, Kitware, GHC/Haskell, and Arch Linux.
//!
//! UUIDs are generated at migration time. The migration is idempotent via
//! ON CONFLICT DO NOTHING on the unique name constraint.

use sea_orm_migration::prelude::*;
use uuid::Uuid;

#[derive(DeriveMigrationName)]
pub struct Migration;

/// Well-known instance definitions.
struct WellKnownInstance {
    name: &'static str,
    platform_type: &'static str,
    host: &'static str,
}

const GITLAB_INSTANCES: &[WellKnownInstance] = &[
    WellKnownInstance {
        name: "gnome-gitlab",
        platform_type: "gitlab",
        host: "gitlab.gnome.org",
    },
    WellKnownInstance {
        name: "freedesktop-gitlab",
        platform_type: "gitlab",
        host: "gitlab.freedesktop.org",
    },
    WellKnownInstance {
        name: "kde-gitlab",
        platform_type: "gitlab",
        host: "invent.kde.org",
    },
    WellKnownInstance {
        name: "kitware-gitlab",
        platform_type: "gitlab",
        host: "gitlab.kitware.com",
    },
    WellKnownInstance {
        name: "haskell-gitlab",
        platform_type: "gitlab",
        host: "gitlab.haskell.org",
    },
    WellKnownInstance {
        name: "archlinux-gitlab",
        platform_type: "gitlab",
        host: "gitlab.archlinux.org",
    },
];

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        for instance in GITLAB_INSTANCES {
            // Generate UUID and convert to hex for SQLite blob literal
            let id = Uuid::new_v4();
            let id_hex = id
                .as_bytes()
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>();

            // Use SQLite X'...' hex literal for UUID blob, ON CONFLICT for idempotency
            let sql = format!(
                r#"INSERT INTO instances (id, name, platform_type, host, created_at)
                   VALUES (X'{}', '{}', '{}', '{}', CURRENT_TIMESTAMP)
                   ON CONFLICT (name) DO NOTHING"#,
                id_hex, instance.name, instance.platform_type, instance.host
            );

            db.execute_unprepared(&sql).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        // Remove the GitLab instances by name
        for instance in GITLAB_INSTANCES {
            let sql = format!("DELETE FROM instances WHERE name = '{}'", instance.name);
            db.execute_unprepared(&sql).await?;
        }

        Ok(())
    }
}
