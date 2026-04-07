use clap::Subcommand;
use console::style;
use sea_orm::{DatabaseConnection, EntityTrait, QueryOrder};
use tabled::{Table, Tabled, settings::Style};

use curator::platform::{PlatformClient, short_error_message};
use curator::{Instance, InstanceColumn, InstanceModel, PlatformType};

use crate::commands::shared::{find_instance_by_name, peek_token_for_instance_with_db};
use crate::config::Config;
use crate::credentials::{
    CredentialSource, CredentialStatus, credential_status, delete_credential,
};

use super::OutputFormat;

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
    #[tabled(rename = "Live Valid")]
    live_valid: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveAuthValidity {
    Valid,
    Invalid,
    Error,
    Missing,
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
        let live_validity = live_auth_validity(instance, &status, config, db).await;
        rows.push(auth_status_row(instance, status, live_validity));
    }

    render_rows(output, rows)?;
    Ok(())
}

async fn live_auth_validity(
    instance: &InstanceModel,
    status: &CredentialStatus,
    config: &Config,
    db: &DatabaseConnection,
) -> LiveAuthValidity {
    let token = match peek_token_for_instance_with_db(instance, config, Some(db)).await {
        Ok(token) => token,
        Err(_) => {
            return if status.active_source.is_some() {
                LiveAuthValidity::Error
            } else {
                LiveAuthValidity::Missing
            };
        }
    };

    let result = match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => {
            let client = match curator::github::GitHubClient::new(&token, instance.id, None) {
                Ok(client) => client,
                Err(_) => return LiveAuthValidity::Error,
            };
            client.get_authenticated_user().await
        }
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => {
            let client =
                match curator::gitlab::GitLabClient::new(&instance.host, &token, instance.id, None)
                    .await
                {
                    Ok(client) => client,
                    Err(_) => return LiveAuthValidity::Error,
                };
            client.get_authenticated_user().await
        }
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            let client = match curator::gitea::GiteaClient::new(
                &instance.base_url(),
                &token,
                instance.id,
                None,
            ) {
                Ok(client) => client,
                Err(_) => return LiveAuthValidity::Error,
            };
            client.get_authenticated_user().await
        }
        #[cfg(not(feature = "github"))]
        PlatformType::GitHub => return LiveAuthValidity::Error,
        #[cfg(not(feature = "gitlab"))]
        PlatformType::GitLab => return LiveAuthValidity::Error,
        #[cfg(not(feature = "gitea"))]
        PlatformType::Gitea => return LiveAuthValidity::Error,
    };

    match result {
        Ok(_) => LiveAuthValidity::Valid,
        Err(curator::platform::PlatformError::AuthRequired) => LiveAuthValidity::Invalid,
        Err(err) => {
            tracing::debug!(
                instance = %instance.name,
                host = %instance.host,
                error = %short_error_message(&err),
                "live auth validation failed"
            );
            LiveAuthValidity::Error
        }
    }
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

    Ok(())
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

fn auth_status_row(
    instance: &InstanceModel,
    status: CredentialStatus,
    live_validity: LiveAuthValidity,
) -> AuthStatusRow {
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
        live_valid: live_auth_validity_label(live_validity),
    }
}

fn live_auth_validity_label(validity: LiveAuthValidity) -> String {
    match validity {
        LiveAuthValidity::Valid => "yes".to_string(),
        LiveAuthValidity::Invalid => "no".to_string(),
        LiveAuthValidity::Error => "error".to_string(),
        LiveAuthValidity::Missing => "missing".to_string(),
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

    use crate::credentials::{StoredCredential, load_credential, save_credential};

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
    fn auth_status_row_formats_missing_state() {
        let instance = sample_instance("gitlab", PlatformType::GitLab, "gitlab.example.com");
        let row = auth_status_row(
            &instance,
            crate::credentials::CredentialStatus {
                configured_store: crate::config::CredentialStore::Auto,
                active_source: None,
                auth_kind: None,
                token_expires_at: None,
            },
            LiveAuthValidity::Missing,
        );

        assert_eq!(row.instance, "gitlab");
        assert_eq!(row.platform, "gitlab");
        assert_eq!(row.configured_store, "auto");
        assert_eq!(row.active_credential, "missing");
        assert_eq!(row.auth_kind, "-");
        assert_eq!(row.expires, "-");
        assert_eq!(row.live_valid, "missing");
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
            },
            LiveAuthValidity::Valid,
        );

        assert_eq!(row.configured_store, "db");
        assert_eq!(row.active_credential, "db");
        assert_eq!(row.auth_kind, "oauth");
        assert_eq!(row.expires, "123");
        assert_eq!(row.live_valid, "yes");
    }

    #[test]
    fn live_auth_validity_label_formats_all_variants() {
        assert_eq!(live_auth_validity_label(LiveAuthValidity::Valid), "yes");
        assert_eq!(live_auth_validity_label(LiveAuthValidity::Invalid), "no");
        assert_eq!(live_auth_validity_label(LiveAuthValidity::Error), "error");
        assert_eq!(
            live_auth_validity_label(LiveAuthValidity::Missing),
            "missing"
        );
    }

    #[tokio::test]
    async fn live_auth_validity_reports_missing_without_any_configured_token() {
        let env = TempConfigEnv::new("status-missing-live-valid");
        let db = setup_db("status-missing-live-valid").await;
        let instance = sample_instance(
            "github-missing-live-valid",
            PlatformType::GitHub,
            "github.missing.live.valid",
        );
        insert_instance(&db, &instance).await;

        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(env.auth_path()),
            },
            ..Config::default()
        };

        assert_eq!(
            live_auth_validity(
                &instance,
                &crate::credentials::CredentialStatus {
                    configured_store: crate::config::CredentialStore::File,
                    active_source: None,
                    auth_kind: None,
                    token_expires_at: None,
                },
                &config,
                &db,
            )
            .await,
            LiveAuthValidity::Missing
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
    async fn handle_auth_dispatches_status_and_logout_actions() {
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
            AuthAction::Logout {
                instance: "github-dispatch".to_string(),
            },
            &db,
            &config,
        )
        .await
        .unwrap();
    }
}
