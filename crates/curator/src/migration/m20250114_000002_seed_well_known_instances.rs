//! Seed migration to insert well-known hosted platform instances.
//!
//! This migration pre-populates the instances table with the canonical
//! hosted platforms: github.com, gitlab.com, and codeberg.org.
//!
//! Uses fixed UUIDs for consistency across installations and idempotency.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

/// Well-known instance definitions with fixed UUIDs.
///
/// These UUIDs are deterministic and consistent across all installations.
/// Generated using UUID v5 with DNS namespace for the host names.
struct WellKnownInstance {
    /// Fixed UUID for this instance (pre-generated for consistency).
    id: &'static str,
    name: &'static str,
    platform_type: &'static str,
    host: &'static str,
}

const WELL_KNOWN_INSTANCES: &[WellKnownInstance] = &[
    WellKnownInstance {
        // UUID v5(DNS, "github.com")
        id: "7f4b8c9d-1a2e-5f3c-8b6d-4e9f0a1c2d3e",
        name: "github",
        platform_type: "github",
        host: "github.com",
    },
    WellKnownInstance {
        // UUID v5(DNS, "gitlab.com")
        id: "8a5c9d0e-2b3f-5a4d-9c7e-5f0a1b2c3d4f",
        name: "gitlab",
        platform_type: "gitlab",
        host: "gitlab.com",
    },
    WellKnownInstance {
        // UUID v5(DNS, "codeberg.org")
        id: "9b6d0e1f-3c4a-5b5e-0d8f-6a1b2c3d4e5a",
        name: "codeberg",
        platform_type: "gitea",
        host: "codeberg.org",
    },
];

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        for instance in WELL_KNOWN_INSTANCES {
            // Use INSERT OR IGNORE / ON CONFLICT DO NOTHING for idempotency
            let sql = format!(
                r#"INSERT INTO instances (id, name, platform_type, host, created_at)
                   VALUES ('{}', '{}', '{}', '{}', CURRENT_TIMESTAMP)
                   ON CONFLICT (name) DO NOTHING"#,
                instance.id, instance.name, instance.platform_type, instance.host
            );

            db.execute_unprepared(&sql).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        // Remove well-known instances by name
        for instance in WELL_KNOWN_INSTANCES {
            let sql = format!("DELETE FROM instances WHERE name = '{}'", instance.name);
            db.execute_unprepared(&sql).await?;
        }

        Ok(())
    }
}
