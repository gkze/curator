use clap::Subcommand;
use console::style;
#[cfg(test)]
use sea_orm::{ColumnTrait, QueryFilter};
use sea_orm::{DatabaseConnection, EntityTrait, QueryOrder};
use tabled::{Table, Tabled, settings::Style};

use curator::{Instance, InstanceColumn, InstanceModel, PlatformType};

use crate::commands::shared::find_instance_by_name;
use crate::config::Config;
use crate::credentials::{
    CredentialSource, CredentialStatus, credential_status, delete_credential,
    legacy_credential_for_instance, save_credential,
};

use super::limits::OutputFormat;

#[derive(Subcommand)]
pub enum AuthAction {
    /// Show auth status for one or all instances
    Status {
        /// Optional instance name
        instance: Option<String>,

        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
    /// Remove stored per-instance credentials for an instance
    Logout {
        /// Instance name
        instance: String,
    },
    /// Copy legacy platform-global config tokens into per-instance storage
    Migrate {
        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
    /// Remove legacy platform-global credentials from config once per-instance auth is in place
    CleanupLegacy {
        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
}

#[derive(Debug, serde::Serialize, Tabled)]
struct AuthStatusRow {
    #[tabled(rename = "Instance")]
    instance: String,
    #[tabled(rename = "Platform")]
    platform: String,
    #[tabled(rename = "Configured Store")]
    configured_store: String,
    #[tabled(rename = "Active Credential")]
    active_credential: String,
    #[tabled(rename = "Auth Kind")]
    auth_kind: String,
    #[tabled(rename = "Expires")]
    expires: String,
    #[tabled(rename = "Legacy Fallback")]
    legacy_fallback: String,
}

#[derive(Debug, serde::Serialize, Tabled)]
struct MigrationRow {
    #[tabled(rename = "Platform")]
    platform: String,
    #[tabled(rename = "Instance")]
    instance: String,
    #[tabled(rename = "Result")]
    result: String,
}

#[derive(Debug, serde::Serialize, Tabled)]
struct CleanupRow {
    #[tabled(rename = "Platform")]
    platform: String,
    #[tabled(rename = "Result")]
    result: String,
}

pub async fn handle_auth(
    action: AuthAction,
    db: &DatabaseConnection,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        AuthAction::Status { instance, output } => {
            handle_status(instance.as_deref(), output, db, config).await
        }
        AuthAction::Logout { instance } => handle_logout(&instance, db, config).await,
        AuthAction::Migrate { output } => handle_migrate(output, db, config).await,
        AuthAction::CleanupLegacy { output } => handle_cleanup_legacy(output, db, config).await,
    }
}

async fn handle_status(
    instance: Option<&str>,
    output: OutputFormat,
    db: &DatabaseConnection,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let instances = if let Some(name) = instance {
        vec![find_instance_by_name(db, name).await?]
    } else {
        Instance::find()
            .order_by_asc(InstanceColumn::Name)
            .all(db)
            .await?
    };

    let mut rows = Vec::with_capacity(instances.len());
    for instance in &instances {
        let status = credential_status(instance, config, Some(db)).await?;
        rows.push(auth_status_row(instance, status));
    }

    render_rows(output, rows)?;
    Ok(())
}

async fn handle_logout(
    instance_name: &str,
    db: &DatabaseConnection,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let instance = find_instance_by_name(db, instance_name).await?;
    let status = credential_status(&instance, config, Some(db)).await?;

    if let Some(source) = status.active_source.clone() {
        delete_credential(&instance, &source, Some(db)).await?;
        println!(
            "{} Removed {} credential for '{}'",
            style("✓").green().bold(),
            source.describe(),
            style(instance_name).cyan()
        );
    } else {
        println!(
            "{} No per-instance credential stored for '{}'",
            style("i").cyan().bold(),
            style(instance_name).cyan()
        );
    }

    if status.has_legacy_fallback {
        println!(
            "{} Legacy platform-global credentials still exist for this instance and may continue to authenticate it.",
            style("!").yellow().bold()
        );
        println!(
            "  Run {} to copy legacy credentials into per-instance storage, then remove the old config manually if desired.",
            style("curator auth migrate").cyan()
        );
    }

    Ok(())
}

fn host_matched_instances(
    instances: &[InstanceModel],
    configured_host: Option<&str>,
) -> Vec<InstanceModel> {
    let Some(configured_host) = configured_host else {
        return instances.to_vec();
    };

    let normalized_host = curator::oauth::normalize_host(configured_host);
    let exact: Vec<_> = instances
        .iter()
        .filter(|instance| curator::oauth::normalize_host(&instance.host) == normalized_host)
        .cloned()
        .collect();

    if exact.len() == 1 {
        exact
    } else {
        instances.to_vec()
    }
}

async fn handle_migrate(
    output: OutputFormat,
    db: &DatabaseConnection,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let instances = Instance::find().all(db).await?;
    let mut rows = Vec::new();

    for platform in [
        PlatformType::GitHub,
        PlatformType::GitLab,
        PlatformType::Gitea,
    ] {
        let matching: Vec<InstanceModel> = instances
            .iter()
            .filter(|instance| instance.platform_type == platform)
            .cloned()
            .collect();

        if matching.is_empty() {
            continue;
        }

        if platform == PlatformType::Gitea {
            let codeberg: Vec<InstanceModel> = matching
                .iter()
                .filter(|instance| instance.is_codeberg())
                .cloned()
                .collect();
            if let Some(template_instance) = codeberg.first()
                && let Some((credential, source)) =
                    legacy_credential_for_instance(template_instance, config)
            {
                let instance = template_instance;
                if credential_status(instance, config, Some(db))
                    .await?
                    .active_source
                    .is_some()
                {
                    rows.push(MigrationRow {
                        platform: instance.platform_type.to_string(),
                        instance: instance.name.clone(),
                        result: "skipped (already has per-instance credential)".to_string(),
                    });
                } else {
                    let saved = save_credential(instance, &credential, config, db).await?;
                    rows.push(MigrationRow {
                        platform: instance.platform_type.to_string(),
                        instance: instance.name.clone(),
                        result: format!("migrated from {source} to {}", saved.describe()),
                    });
                }
            }

            let self_hosted: Vec<InstanceModel> = matching
                .iter()
                .filter(|instance| !instance.is_codeberg())
                .cloned()
                .collect();
            let Some(template_instance) = self_hosted.first() else {
                continue;
            };
            let Some((credential, source)) =
                legacy_credential_for_instance(template_instance, config)
            else {
                continue;
            };
            let targeted = host_matched_instances(&self_hosted, config.gitea.host.as_deref());

            if targeted.len() == 1 {
                let instance = &targeted[0];
                if credential_status(instance, config, Some(db))
                    .await?
                    .active_source
                    .is_some()
                {
                    rows.push(MigrationRow {
                        platform: instance.platform_type.to_string(),
                        instance: instance.name.clone(),
                        result: "skipped (already has per-instance credential)".to_string(),
                    });
                } else {
                    let saved = save_credential(instance, &credential, config, db).await?;
                    rows.push(MigrationRow {
                        platform: instance.platform_type.to_string(),
                        instance: instance.name.clone(),
                        result: format!("migrated from {source} to {}", saved.describe()),
                    });
                }
            } else {
                for instance in targeted {
                    rows.push(MigrationRow {
                        platform: instance.platform_type.to_string(),
                        instance: instance.name,
                        result: "skipped (legacy credential is ambiguous across gitea instances)"
                            .to_string(),
                    });
                }
            }
            continue;
        }

        let Some(template_instance) = matching.first() else {
            continue;
        };
        let Some((credential, source)) = legacy_credential_for_instance(template_instance, config)
        else {
            continue;
        };

        let targeted: Vec<InstanceModel> = match platform {
            PlatformType::GitLab => host_matched_instances(
                &matching,
                config.gitlab.host.as_deref().or(Some("gitlab.com")),
            ),
            _ => matching.clone(),
        };

        if targeted.len() == 1 {
            let instance = &targeted[0];
            if credential_status(instance, config, Some(db))
                .await?
                .active_source
                .is_some()
            {
                rows.push(MigrationRow {
                    platform: instance.platform_type.to_string(),
                    instance: instance.name.clone(),
                    result: "skipped (already has per-instance credential)".to_string(),
                });
            } else {
                let saved = save_credential(instance, &credential, config, db).await?;
                rows.push(MigrationRow {
                    platform: instance.platform_type.to_string(),
                    instance: instance.name.clone(),
                    result: format!("migrated from {source} to {}", saved.describe()),
                });
            }
        } else {
            for instance in targeted {
                rows.push(MigrationRow {
                    platform: instance.platform_type.to_string(),
                    instance: instance.name,
                    result: format!(
                        "skipped (legacy credential is ambiguous across {} instances)",
                        platform
                    ),
                });
            }
        }
    }

    if rows.is_empty() {
        println!("No legacy credentials found to migrate.");
        return Ok(());
    }

    render_rows(output, rows)?;
    println!();
    println!(
        "{} Legacy config entries are left in place for compatibility. Remove them manually after verifying per-instance auth.",
        style("i").cyan().bold()
    );
    Ok(())
}

async fn handle_cleanup_legacy(
    output: OutputFormat,
    db: &DatabaseConnection,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let instances = Instance::find().all(db).await?;
    let mut rows = Vec::new();

    for (label, section, platform_instances) in [
        (
            "github".to_string(),
            "github",
            instances
                .iter()
                .filter(|instance| instance.platform_type == PlatformType::GitHub)
                .collect::<Vec<_>>(),
        ),
        (
            "gitlab".to_string(),
            "gitlab",
            instances
                .iter()
                .filter(|instance| instance.platform_type == PlatformType::GitLab)
                .collect::<Vec<_>>(),
        ),
        (
            "codeberg".to_string(),
            "codeberg",
            instances
                .iter()
                .filter(|instance| instance.is_codeberg())
                .collect::<Vec<_>>(),
        ),
        (
            "gitea".to_string(),
            "gitea",
            instances
                .iter()
                .filter(|instance| {
                    instance.platform_type == PlatformType::Gitea && !instance.is_codeberg()
                })
                .collect::<Vec<_>>(),
        ),
    ] {
        let has_legacy = platform_instances
            .iter()
            .any(|instance| legacy_credential_for_instance(instance, config).is_some());

        if !has_legacy {
            continue;
        }

        let mut ready = true;
        for instance in &platform_instances {
            if credential_status(instance, config, Some(db))
                .await?
                .active_source
                .is_none()
            {
                ready = false;
                break;
            }
        }

        if !ready {
            rows.push(CleanupRow {
                platform: label.clone(),
                result: "skipped (some instances still lack per-instance credentials)".to_string(),
            });
            continue;
        }

        match Config::clear_legacy_tokens(section)? {
            Some(path) => rows.push(CleanupRow {
                platform: label.clone(),
                result: format!("removed legacy config keys from {}", path.display()),
            }),
            None => rows.push(CleanupRow {
                platform: label,
                result: "nothing to remove".to_string(),
            }),
        }
    }

    if rows.is_empty() {
        println!("No legacy credentials found to clean up.");
        return Ok(());
    }

    render_rows(output, rows)
}

fn render_rows<T: serde::Serialize + Tabled>(
    output: OutputFormat,
    rows: Vec<T>,
) -> Result<(), Box<dyn std::error::Error>> {
    match output {
        OutputFormat::Table => {
            let mut table = Table::new(rows);
            table.with(Style::rounded());
            println!("{}", table);
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&rows)?),
    }
    Ok(())
}

fn auth_status_row(instance: &InstanceModel, status: CredentialStatus) -> AuthStatusRow {
    AuthStatusRow {
        instance: instance.name.clone(),
        platform: instance.platform_type.to_string(),
        configured_store: format!("{:?}", status.configured_store).to_lowercase(),
        active_credential: credential_source_label(status.active_source.as_ref()),
        auth_kind: status.auth_kind.unwrap_or_else(|| "-".to_string()),
        expires: status
            .token_expires_at
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        legacy_fallback: status
            .legacy_source
            .map(ToString::to_string)
            .unwrap_or_else(|| "none".to_string()),
    }
}

fn credential_source_label(source: Option<&CredentialSource>) -> String {
    match source {
        Some(CredentialSource::Keychain) => "keychain".to_string(),
        Some(CredentialSource::File(_)) => "file".to_string(),
        Some(CredentialSource::Db) => "db".to_string(),
        None => "missing".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use sea_orm::{ActiveModelTrait, Set};
    use std::ffi::OsString;
    use std::fs;
    use uuid::Uuid;

    use crate::credentials::{StoredCredential, load_credential};

    fn sample_instance(name: &str, platform: PlatformType, host: &str) -> InstanceModel {
        InstanceModel {
            id: Uuid::new_v4(),
            name: name.to_string(),
            platform_type: platform,
            host: host.to_string(),
            oauth_client_id: None,
            oauth_flow: "auto".to_string(),
            created_at: Utc::now().fixed_offset(),
        }
    }

    fn sample_credential(token: &str) -> StoredCredential {
        StoredCredential {
            access_token: token.to_string(),
            refresh_token: None,
            token_expires_at: None,
            auth_kind: "pat".to_string(),
            token_type: None,
        }
    }

    struct TempConfigEnv {
        temp_dir: std::path::PathBuf,
        previous_home: Option<OsString>,
        previous_xdg_config_home: Option<OsString>,
    }

    impl TempConfigEnv {
        fn new(label: &str) -> Self {
            let temp_dir =
                std::env::temp_dir().join(format!("curator-auth-tests-{label}-{}", Uuid::new_v4()));
            fs::create_dir_all(&temp_dir).unwrap();
            let previous_home = std::env::var_os("HOME");
            let previous_xdg_config_home = std::env::var_os("XDG_CONFIG_HOME");
            unsafe {
                std::env::set_var("HOME", &temp_dir);
                std::env::set_var("XDG_CONFIG_HOME", &temp_dir);
            }
            Self {
                temp_dir,
                previous_home,
                previous_xdg_config_home,
            }
        }

        fn auth_path(&self) -> String {
            self.temp_dir
                .join("curator")
                .join("auth.toml")
                .display()
                .to_string()
        }
    }

    impl Drop for TempConfigEnv {
        fn drop(&mut self) {
            unsafe {
                match &self.previous_home {
                    Some(value) => std::env::set_var("HOME", value),
                    None => std::env::remove_var("HOME"),
                }
                match &self.previous_xdg_config_home {
                    Some(value) => std::env::set_var("XDG_CONFIG_HOME", value),
                    None => std::env::remove_var("XDG_CONFIG_HOME"),
                }
            }
            let _ = fs::remove_dir_all(&self.temp_dir);
        }
    }

    async fn setup_db(label: &str) -> DatabaseConnection {
        curator::db::connect_and_migrate(&format!(
            "sqlite://{}?mode=rwc",
            std::env::temp_dir()
                .join(format!("curator-auth-tests-{label}-{}.db", Uuid::new_v4()))
                .display()
        ))
        .await
        .expect("test database should initialize")
    }

    async fn insert_instance(db: &DatabaseConnection, instance: &InstanceModel) {
        curator::entity::instance::ActiveModel {
            id: Set(instance.id),
            name: Set(instance.name.clone()),
            platform_type: Set(instance.platform_type),
            host: Set(instance.host.clone()),
            oauth_client_id: Set(instance.oauth_client_id.clone()),
            oauth_flow: Set(instance.oauth_flow.clone()),
            created_at: Set(instance.created_at),
        }
        .insert(db)
        .await
        .expect("instance should insert");
    }

    #[tokio::test]
    async fn handle_logout_removes_file_backed_credentials() {
        let env = TempConfigEnv::new("logout");
        let db = setup_db("logout").await;
        let instance = sample_instance("work-github", PlatformType::GitHub, "github.work.test");
        insert_instance(&db, &instance).await;

        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(env.auth_path()),
            },
            ..Config::default()
        };

        save_credential(&instance, &sample_credential("secret"), &config, &db)
            .await
            .unwrap();
        handle_logout(&instance.name, &db, &config).await.unwrap();

        assert!(
            load_credential(&instance, &config, Some(&db))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn handle_migrate_moves_unambiguous_legacy_credentials() {
        let env = TempConfigEnv::new("migrate");
        let db = setup_db("migrate").await;
        let instance = sample_instance("gitlab-work", PlatformType::GitLab, "gitlab.work.test");
        insert_instance(&db, &instance).await;

        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(env.auth_path()),
            },
            gitlab: crate::config::GitLabConfig {
                host: Some("gitlab.work.test".to_string()),
                token: Some("legacy-token".to_string()),
                refresh_token: None,
                token_expires_at: None,
                include_subgroups: true,
            },
            ..Config::default()
        };

        handle_migrate(OutputFormat::Json, &db, &config)
            .await
            .unwrap();

        let loaded = load_credential(&instance, &config, Some(&db))
            .await
            .unwrap();
        assert_eq!(loaded.unwrap().credential.access_token, "legacy-token");
    }

    #[tokio::test]
    async fn handle_migrate_skips_ambiguous_legacy_credentials() {
        let env = TempConfigEnv::new("migrate-ambiguous");
        let db = setup_db("migrate-ambiguous").await;
        let first = sample_instance("gitlab-a", PlatformType::GitLab, "gitlab.a.test");
        let second = sample_instance("gitlab-b", PlatformType::GitLab, "gitlab.b.test");
        insert_instance(&db, &first).await;
        insert_instance(&db, &second).await;

        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(env.auth_path()),
            },
            gitlab: crate::config::GitLabConfig {
                host: Some("gitlab.com".to_string()),
                token: Some("legacy-token".to_string()),
                refresh_token: None,
                token_expires_at: None,
                include_subgroups: true,
            },
            ..Config::default()
        };

        handle_migrate(OutputFormat::Table, &db, &config)
            .await
            .unwrap();

        assert!(
            load_credential(&first, &config, Some(&db))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            load_credential(&second, &config, Some(&db))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn handle_cleanup_legacy_removes_global_config_after_per_instance_migration() {
        let _guard = crate::test_support::env_lock().lock().await;
        let env = TempConfigEnv::new("cleanup");
        let db = setup_db("cleanup").await;
        let instance = sample_instance("github-clean", PlatformType::GitHub, "github.clean.test");
        insert_instance(&db, &instance).await;

        crate::config::Config::save_github_token("legacy-gh").unwrap();
        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(env.auth_path()),
            },
            github: crate::config::GitHubConfig {
                token: Some("legacy-gh".to_string()),
            },
            ..Config::default()
        };

        for github_instance in Instance::find()
            .filter(InstanceColumn::PlatformType.eq(PlatformType::GitHub))
            .all(&db)
            .await
            .unwrap()
        {
            save_credential(
                &github_instance,
                &sample_credential("instance-gh"),
                &config,
                &db,
            )
            .await
            .unwrap();
        }
        handle_cleanup_legacy(OutputFormat::Json, &db, &config)
            .await
            .unwrap();

        assert!(
            credential_status(&instance, &config, Some(&db))
                .await
                .unwrap()
                .active_source
                .is_some()
        );
    }

    #[tokio::test]
    async fn handle_migrate_gitea_branch_skips_existing_and_migrates_missing() {
        let env = TempConfigEnv::new("migrate-gitea");
        let db = setup_db("migrate-gitea").await;
        let codeberg = Instance::find()
            .filter(InstanceColumn::Name.eq("codeberg"))
            .one(&db)
            .await
            .unwrap()
            .expect("seeded codeberg instance should exist");
        let gitea = sample_instance("forgejo-work", PlatformType::Gitea, "forgejo.work.test");
        insert_instance(&db, &gitea).await;

        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(env.auth_path()),
            },
            codeberg: crate::config::CodebergConfig {
                token: Some("legacy-codeberg".to_string()),
                refresh_token: None,
                token_expires_at: None,
            },
            gitea: crate::config::GiteaConfig {
                host: Some("forgejo.work.test".to_string()),
                token: Some("legacy-gitea".to_string()),
            },
            ..Config::default()
        };

        save_credential(
            &codeberg,
            &sample_credential("already-present"),
            &config,
            &db,
        )
        .await
        .unwrap();

        handle_migrate(OutputFormat::Json, &db, &config)
            .await
            .unwrap();

        let codeberg_loaded = load_credential(&codeberg, &config, Some(&db))
            .await
            .unwrap()
            .unwrap();
        let gitea_loaded = load_credential(&gitea, &config, Some(&db))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(codeberg_loaded.credential.access_token, "already-present");
        assert_eq!(gitea_loaded.credential.access_token, "legacy-gitea");
    }

    #[tokio::test]
    async fn handle_migrate_gitea_branch_only_targets_matching_host() {
        let env = TempConfigEnv::new("migrate-gitea-host-match");
        let db = setup_db("migrate-gitea-host-match").await;
        let first = sample_instance("forgejo-work", PlatformType::Gitea, "forgejo.work.test");
        let second = sample_instance(
            "forgejo-personal",
            PlatformType::Gitea,
            "forgejo.personal.test",
        );
        insert_instance(&db, &first).await;
        insert_instance(&db, &second).await;

        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(env.auth_path()),
            },
            gitea: crate::config::GiteaConfig {
                host: Some("https://forgejo.work.test/".to_string()),
                token: Some("legacy-gitea".to_string()),
            },
            ..Config::default()
        };

        handle_migrate(OutputFormat::Json, &db, &config)
            .await
            .unwrap();

        let first_loaded = load_credential(&first, &config, Some(&db))
            .await
            .unwrap()
            .unwrap();
        let second_loaded = load_credential(&second, &config, Some(&db)).await.unwrap();

        assert_eq!(first_loaded.credential.access_token, "legacy-gitea");
        assert!(second_loaded.is_none());
    }

    #[tokio::test]
    async fn handle_cleanup_legacy_skips_when_instance_credentials_are_missing() {
        let _guard = crate::test_support::env_lock().lock().await;
        let env = TempConfigEnv::new("cleanup-skip");
        let db = setup_db("cleanup-skip").await;
        let instance = sample_instance("github-skip", PlatformType::GitHub, "github.skip.test");
        insert_instance(&db, &instance).await;

        crate::config::Config::save_github_token("legacy-gh-skip").unwrap();
        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(env.auth_path()),
            },
            github: crate::config::GitHubConfig {
                token: Some("legacy-gh-skip".to_string()),
            },
            ..Config::default()
        };

        handle_cleanup_legacy(OutputFormat::Table, &db, &config)
            .await
            .unwrap();

        assert!(legacy_credential_for_instance(&instance, &config).is_some());
        assert!(
            load_credential(&instance, &config, Some(&db))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn credential_source_label_formats_all_variants() {
        assert_eq!(
            credential_source_label(Some(&CredentialSource::Keychain)),
            "keychain"
        );
        assert_eq!(
            credential_source_label(Some(&CredentialSource::File(std::path::PathBuf::from(
                "/tmp/auth.toml"
            )))),
            "file"
        );
        assert_eq!(credential_source_label(Some(&CredentialSource::Db)), "db");
        assert_eq!(credential_source_label(None), "missing");
    }

    #[test]
    fn auth_status_row_formats_missing_and_legacy_state() {
        let instance = sample_instance("gitlab", PlatformType::GitLab, "gitlab.example.com");
        let row = auth_status_row(
            &instance,
            crate::credentials::CredentialStatus {
                configured_store: crate::config::CredentialStore::Auto,
                active_source: None,
                auth_kind: None,
                token_expires_at: None,
                has_legacy_fallback: true,
                legacy_source: Some("legacy gitlab config"),
            },
        );

        assert_eq!(row.instance, "gitlab");
        assert_eq!(row.platform, "gitlab");
        assert_eq!(row.configured_store, "auto");
        assert_eq!(row.active_credential, "missing");
        assert_eq!(row.auth_kind, "-");
        assert_eq!(row.expires, "-");
        assert_eq!(row.legacy_fallback, "legacy gitlab config");
    }

    #[test]
    fn auth_status_row_formats_active_db_credential() {
        let instance = sample_instance("gitea", PlatformType::Gitea, "forge.test");
        let row = auth_status_row(
            &instance,
            crate::credentials::CredentialStatus {
                configured_store: crate::config::CredentialStore::Db,
                active_source: Some(CredentialSource::Db),
                auth_kind: Some("oauth".to_string()),
                token_expires_at: Some(123),
                has_legacy_fallback: false,
                legacy_source: None,
            },
        );

        assert_eq!(row.configured_store, "db");
        assert_eq!(row.active_credential, "db");
        assert_eq!(row.auth_kind, "oauth");
        assert_eq!(row.expires, "123");
        assert_eq!(row.legacy_fallback, "none");
    }

    #[tokio::test]
    async fn handle_cleanup_legacy_noop_when_no_legacy_credentials_exist() {
        let env = TempConfigEnv::new("cleanup-noop");
        let db = setup_db("cleanup-noop").await;
        let instance = sample_instance("github-clean", PlatformType::GitHub, "github.clean.test");
        insert_instance(&db, &instance).await;

        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(env.auth_path()),
            },
            ..Config::default()
        };

        handle_cleanup_legacy(OutputFormat::Json, &db, &config)
            .await
            .unwrap();
        assert!(
            load_credential(&instance, &config, Some(&db))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn handle_logout_is_noop_without_per_instance_credential() {
        let env = TempConfigEnv::new("logout-noop");
        let db = setup_db("logout-noop").await;
        let instance = sample_instance("github-no-cred", PlatformType::GitHub, "github.no.cred");
        insert_instance(&db, &instance).await;
        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(env.auth_path()),
            },
            ..Config::default()
        };

        handle_logout(&instance.name, &db, &config).await.unwrap();
    }

    #[tokio::test]
    async fn handle_status_supports_all_instances_and_single_instance_json() {
        let env = TempConfigEnv::new("status");
        let db = setup_db("status").await;
        let github = sample_instance("github-status", PlatformType::GitHub, "github.status.test");
        let gitlab = sample_instance("gitlab-status", PlatformType::GitLab, "gitlab.status.test");
        insert_instance(&db, &github).await;
        insert_instance(&db, &gitlab).await;

        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(env.auth_path()),
            },
            ..Config::default()
        };

        handle_status(None, OutputFormat::Table, &db, &config)
            .await
            .unwrap();
        handle_status(Some("github-status"), OutputFormat::Json, &db, &config)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn handle_auth_dispatches_status_and_cleanup_actions() {
        let env = TempConfigEnv::new("dispatch");
        let db = setup_db("dispatch").await;
        let instance = sample_instance(
            "github-dispatch",
            PlatformType::GitHub,
            "github.dispatch.test",
        );
        insert_instance(&db, &instance).await;
        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(env.auth_path()),
            },
            ..Config::default()
        };

        handle_auth(
            AuthAction::Status {
                instance: Some("github-dispatch".to_string()),
                output: OutputFormat::Json,
            },
            &db,
            &config,
        )
        .await
        .unwrap();

        handle_auth(
            AuthAction::CleanupLegacy {
                output: OutputFormat::Table,
            },
            &db,
            &config,
        )
        .await
        .unwrap();
    }
}
