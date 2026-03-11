use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use chrono::Utc;
use curator::{
    InstanceCredential, InstanceCredentialColumn, InstanceCredentialModel, InstanceModel,
};
use keyring::Entry;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    Set,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::{Config, CredentialStore};

const KEYCHAIN_SERVICE: &str = "dev.gkze.curator.instance-auth";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredCredential {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub token_expires_at: Option<u64>,
    pub auth_kind: String,
    pub token_type: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct LocatedCredential {
    pub credential: StoredCredential,
    pub source: CredentialSource,
}

#[derive(Debug, Clone)]
pub(crate) enum CredentialSource {
    Keychain,
    File(PathBuf),
    Db,
}

#[derive(Debug, Clone)]
pub(crate) struct CredentialStatus {
    pub configured_store: CredentialStore,
    pub active_source: Option<CredentialSource>,
    pub auth_kind: Option<String>,
    pub token_expires_at: Option<u64>,
    pub has_legacy_fallback: bool,
    pub legacy_source: Option<&'static str>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AuthFile {
    instances: BTreeMap<String, StoredCredential>,
}

impl CredentialSource {
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Keychain => "system keychain".to_string(),
            Self::File(path) => format!("file {}", path.display()),
            Self::Db => "database".to_string(),
        }
    }
}

pub(crate) async fn save_credential(
    instance: &InstanceModel,
    credential: &StoredCredential,
    config: &Config,
    db: &DatabaseConnection,
) -> Result<CredentialSource, Box<dyn std::error::Error>> {
    match config.credential_store() {
        CredentialStore::Auto => match save_to_keychain(instance, credential) {
            Ok(()) => Ok(CredentialSource::Keychain),
            Err(_) => {
                let path = save_to_file(instance, credential, config)?;
                Ok(CredentialSource::File(path))
            }
        },
        CredentialStore::Keychain => {
            save_to_keychain(instance, credential)?;
            Ok(CredentialSource::Keychain)
        }
        CredentialStore::File => {
            let path = save_to_file(instance, credential, config)?;
            Ok(CredentialSource::File(path))
        }
        CredentialStore::Db => {
            save_to_db(instance, credential, db).await?;
            Ok(CredentialSource::Db)
        }
    }
}

pub(crate) async fn load_credential(
    instance: &InstanceModel,
    config: &Config,
    db: Option<&DatabaseConnection>,
) -> Result<Option<LocatedCredential>, Box<dyn std::error::Error>> {
    match config.credential_store() {
        CredentialStore::Auto => {
            if let Ok(Some(credential)) = load_from_keychain(instance) {
                return Ok(Some(LocatedCredential {
                    credential,
                    source: CredentialSource::Keychain,
                }));
            }

            if let Some((path, credential)) = load_from_file(instance, config)? {
                return Ok(Some(LocatedCredential {
                    credential,
                    source: CredentialSource::File(path),
                }));
            }

            Ok(None)
        }
        CredentialStore::Keychain => {
            Ok(
                load_from_keychain(instance)?.map(|credential| LocatedCredential {
                    credential,
                    source: CredentialSource::Keychain,
                }),
            )
        }
        CredentialStore::File => {
            Ok(
                load_from_file(instance, config)?.map(|(path, credential)| LocatedCredential {
                    credential,
                    source: CredentialSource::File(path),
                }),
            )
        }
        CredentialStore::Db => {
            let db = db.ok_or_else(|| {
                io::Error::other("database backend requires a database connection")
            })?;
            Ok(load_from_db(instance, db)
                .await?
                .map(|credential| LocatedCredential {
                    credential,
                    source: CredentialSource::Db,
                }))
        }
    }
}

pub(crate) async fn update_credential(
    instance: &InstanceModel,
    source: &CredentialSource,
    credential: &StoredCredential,
    db: Option<&DatabaseConnection>,
) -> Result<(), Box<dyn std::error::Error>> {
    match source {
        CredentialSource::Keychain => save_to_keychain(instance, credential)?,
        CredentialSource::File(path) => save_to_file_path(instance, credential, path)?,
        CredentialSource::Db => {
            let db = db.ok_or_else(|| {
                io::Error::other("database backend requires a database connection")
            })?;
            save_to_db(instance, credential, db).await?;
        }
    }

    Ok(())
}

pub(crate) async fn delete_credential(
    instance: &InstanceModel,
    source: &CredentialSource,
    db: Option<&DatabaseConnection>,
) -> Result<(), Box<dyn std::error::Error>> {
    match source {
        CredentialSource::Keychain => delete_from_keychain(instance)?,
        CredentialSource::File(path) => delete_from_file_path(instance, path)?,
        CredentialSource::Db => {
            let db = db.ok_or_else(|| {
                io::Error::other("database backend requires a database connection")
            })?;
            delete_from_db(instance, db).await?;
        }
    }

    Ok(())
}

pub(crate) async fn credential_status(
    instance: &InstanceModel,
    config: &Config,
    db: Option<&DatabaseConnection>,
) -> Result<CredentialStatus, Box<dyn std::error::Error>> {
    let active = load_credential(instance, config, db).await?;
    let legacy = legacy_credential_for_instance(instance, config);

    Ok(CredentialStatus {
        configured_store: config.credential_store(),
        active_source: active.as_ref().map(|located| located.source.clone()),
        auth_kind: active
            .as_ref()
            .map(|located| located.credential.auth_kind.clone()),
        token_expires_at: active.and_then(|located| located.credential.token_expires_at),
        has_legacy_fallback: legacy.is_some(),
        legacy_source: legacy.map(|(_, source)| source),
    })
}

pub(crate) fn legacy_credential_for_instance(
    instance: &InstanceModel,
    config: &Config,
) -> Option<(StoredCredential, &'static str)> {
    match instance.platform_type {
        #[cfg(feature = "github")]
        curator::PlatformType::GitHub => config.github_token().map(|token| {
            (
                StoredCredential {
                    access_token: token,
                    refresh_token: None,
                    token_expires_at: None,
                    auth_kind: "pat".to_string(),
                    token_type: None,
                },
                "legacy github config",
            )
        }),
        #[cfg(feature = "gitlab")]
        curator::PlatformType::GitLab => config.gitlab_token().map(|token| {
            (
                StoredCredential {
                    access_token: token,
                    refresh_token: config.gitlab_refresh_token(),
                    token_expires_at: config.gitlab_token_expires_at(),
                    auth_kind: if config.gitlab_refresh_token().is_some() {
                        "oauth"
                    } else {
                        "pat"
                    }
                    .to_string(),
                    token_type: None,
                },
                "legacy gitlab config",
            )
        }),
        #[cfg(feature = "gitea")]
        curator::PlatformType::Gitea if instance.is_codeberg() => {
            config.codeberg_token().map(|token| {
                (
                    StoredCredential {
                        access_token: token,
                        refresh_token: config.codeberg_refresh_token(),
                        token_expires_at: config.codeberg_token_expires_at(),
                        auth_kind: if config.codeberg_refresh_token().is_some() {
                            "oauth"
                        } else {
                            "pat"
                        }
                        .to_string(),
                        token_type: None,
                    },
                    "legacy codeberg config",
                )
            })
        }
        #[cfg(feature = "gitea")]
        curator::PlatformType::Gitea => config.gitea_token().map(|token| {
            (
                StoredCredential {
                    access_token: token,
                    refresh_token: None,
                    token_expires_at: None,
                    auth_kind: "pat".to_string(),
                    token_type: None,
                },
                "legacy gitea config",
            )
        }),
        // When a platform feature is disabled, no legacy credential is available
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

fn save_to_keychain(
    instance: &InstanceModel,
    credential: &StoredCredential,
) -> Result<(), Box<dyn std::error::Error>> {
    let entry = keychain_entry(instance)?;
    let payload = serde_json::to_string(credential)?;
    entry.set_password(&payload)?;
    Ok(())
}

fn load_from_keychain(
    instance: &InstanceModel,
) -> Result<Option<StoredCredential>, Box<dyn std::error::Error>> {
    let entry = keychain_entry(instance)?;
    match entry.get_password() {
        Ok(payload) => Ok(Some(serde_json::from_str(&payload)?)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(err) => Err(Box::new(err)),
    }
}

fn delete_from_keychain(instance: &InstanceModel) -> Result<(), Box<dyn std::error::Error>> {
    let entry = keychain_entry(instance)?;
    match entry.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(err) => Err(Box::new(err)),
    }
}

fn keychain_entry(instance: &InstanceModel) -> Result<Entry, keyring::Error> {
    Entry::new(KEYCHAIN_SERVICE, &instance.id.to_string())
}

fn save_to_file(
    instance: &InstanceModel,
    credential: &StoredCredential,
    config: &Config,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = config
        .auth_file_path()
        .ok_or_else(|| io::Error::other("could not determine auth file path"))?;
    save_to_file_path(instance, credential, &path)?;
    Ok(path)
}

fn save_to_file_path(
    instance: &InstanceModel,
    credential: &StoredCredential,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut auth_file = load_auth_file(path)?;
    auth_file
        .instances
        .insert(instance.name.clone(), credential.clone());
    fs::write(path, toml::to_string_pretty(&auth_file)?)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
}

fn load_from_file(
    instance: &InstanceModel,
    config: &Config,
) -> Result<Option<(PathBuf, StoredCredential)>, Box<dyn std::error::Error>> {
    let Some(path) = config.auth_file_path() else {
        return Ok(None);
    };

    if !path.exists() {
        return Ok(None);
    }

    let auth_file = load_auth_file(&path)?;
    Ok(auth_file
        .instances
        .get(&instance.name)
        .cloned()
        .map(|credential| (path, credential)))
}

fn delete_from_file_path(
    instance: &InstanceModel,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(());
    }

    let mut auth_file = load_auth_file(path)?;
    auth_file.instances.remove(&instance.name);

    if auth_file.instances.is_empty() {
        fs::remove_file(path)?;
    } else {
        fs::write(path, toml::to_string_pretty(&auth_file)?)?;
    }

    Ok(())
}

fn load_auth_file(path: &Path) -> Result<AuthFile, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(AuthFile::default());
    }

    let contents = fs::read_to_string(path)?;
    if contents.trim().is_empty() {
        return Ok(AuthFile::default());
    }

    Ok(toml::from_str(&contents)?)
}

async fn save_to_db(
    instance: &InstanceModel,
    credential: &StoredCredential,
    db: &DatabaseConnection,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(existing) = InstanceCredential::find()
        .filter(InstanceCredentialColumn::InstanceId.eq(instance.id))
        .one(db)
        .await?
    {
        let mut model = existing.into_active_model();
        model.backend = Set("db".to_string());
        model.auth_kind = Set(credential.auth_kind.clone());
        model.access_token = Set(Some(credential.access_token.clone()));
        model.refresh_token = Set(credential.refresh_token.clone());
        model.token_expires_at = Set(credential.token_expires_at.map(|value| value as i64));
        model.token_type = Set(credential.token_type.clone());
        model.updated_at = Set(Utc::now().fixed_offset());
        model.update(db).await?;
    } else {
        let model = curator::entity::instance_credential::ActiveModel {
            id: Set(Uuid::new_v4()),
            instance_id: Set(instance.id),
            backend: Set("db".to_string()),
            auth_kind: Set(credential.auth_kind.clone()),
            access_token: Set(Some(credential.access_token.clone())),
            refresh_token: Set(credential.refresh_token.clone()),
            token_expires_at: Set(credential.token_expires_at.map(|value| value as i64)),
            token_type: Set(credential.token_type.clone()),
            scopes: Set(None),
            created_at: Set(Utc::now().fixed_offset()),
            updated_at: Set(Utc::now().fixed_offset()),
        };
        model.insert(db).await?;
    }

    Ok(())
}

async fn load_from_db(
    instance: &InstanceModel,
    db: &DatabaseConnection,
) -> Result<Option<StoredCredential>, Box<dyn std::error::Error>> {
    let record: Option<InstanceCredentialModel> = InstanceCredential::find()
        .filter(InstanceCredentialColumn::InstanceId.eq(instance.id))
        .one(db)
        .await?;

    Ok(record.and_then(|record| {
        record.access_token.map(|access_token| StoredCredential {
            access_token,
            refresh_token: record.refresh_token,
            token_expires_at: record.token_expires_at.map(|value| value as u64),
            auth_kind: record.auth_kind,
            token_type: record.token_type,
        })
    }))
}

async fn delete_from_db(
    instance: &InstanceModel,
    db: &DatabaseConnection,
) -> Result<(), Box<dyn std::error::Error>> {
    InstanceCredential::delete_many()
        .filter(InstanceCredentialColumn::InstanceId.eq(instance.id))
        .exec(db)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use curator::PlatformType;
    use std::ffi::OsString;

    fn sample_instance(name: &str) -> InstanceModel {
        InstanceModel {
            id: Uuid::new_v4(),
            name: name.to_string(),
            platform_type: PlatformType::GitHub,
            host: format!("{}.example.com", name),
            oauth_client_id: None,
            oauth_flow: "auto".to_string(),
            created_at: Utc::now().fixed_offset(),
        }
    }

    fn sample_credential(token: &str) -> StoredCredential {
        StoredCredential {
            access_token: token.to_string(),
            refresh_token: Some("refresh-token".to_string()),
            token_expires_at: Some(1_700_000_000),
            auth_kind: "oauth".to_string(),
            token_type: Some("Bearer".to_string()),
        }
    }

    struct TempHome {
        root: PathBuf,
        previous_home: Option<OsString>,
        previous_xdg: Option<OsString>,
    }

    impl TempHome {
        fn new(label: &str) -> Self {
            let root = std::env::temp_dir()
                .join(format!("curator-credentials-{label}-{}", Uuid::new_v4()));
            fs::create_dir_all(&root).expect("temp root should be created");
            let previous_home = std::env::var_os("HOME");
            let previous_xdg = std::env::var_os("XDG_CONFIG_HOME");
            unsafe {
                std::env::set_var("HOME", &root);
                std::env::set_var("XDG_CONFIG_HOME", &root);
            }
            Self {
                root,
                previous_home,
                previous_xdg,
            }
        }
    }

    impl Drop for TempHome {
        fn drop(&mut self) {
            unsafe {
                match &self.previous_home {
                    Some(value) => std::env::set_var("HOME", value),
                    None => std::env::remove_var("HOME"),
                }
                match &self.previous_xdg {
                    Some(value) => std::env::set_var("XDG_CONFIG_HOME", value),
                    None => std::env::remove_var("XDG_CONFIG_HOME"),
                }
            }
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    async fn setup_db(label: &str) -> DatabaseConnection {
        curator::db::connect_and_migrate(&format!(
            "sqlite://{}?mode=rwc",
            std::env::temp_dir()
                .join(format!("curator-credentials-{label}-{}.db", Uuid::new_v4()))
                .display()
        ))
        .await
        .expect("test database should initialize")
    }

    async fn insert_instance(db: &DatabaseConnection, instance: &InstanceModel) {
        let model = curator::entity::instance::ActiveModel {
            id: Set(instance.id),
            name: Set(instance.name.clone()),
            platform_type: Set(instance.platform_type),
            host: Set(instance.host.clone()),
            oauth_client_id: Set(instance.oauth_client_id.clone()),
            oauth_flow: Set(instance.oauth_flow.clone()),
            created_at: Set(instance.created_at),
        };
        model.insert(db).await.expect("instance should insert");
    }

    #[test]
    fn credential_source_describe_reports_backend() {
        assert_eq!(CredentialSource::Keychain.describe(), "system keychain");
        assert_eq!(CredentialSource::Db.describe(), "database");
        assert!(
            CredentialSource::File(PathBuf::from("/tmp/auth.toml"))
                .describe()
                .contains("auth.toml")
        );
    }

    #[test]
    fn load_auth_file_returns_default_for_missing_and_empty_file() {
        let dir = std::env::temp_dir().join(format!("curator-auth-file-{}", Uuid::new_v4()));
        let path = dir.join("auth.toml");
        assert!(load_auth_file(&path).unwrap().instances.is_empty());

        fs::create_dir_all(&dir).unwrap();
        fs::write(&path, "   \n").unwrap();
        assert!(load_auth_file(&path).unwrap().instances.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_and_load_file_round_trip() {
        let _home = TempHome::new("file-round-trip");
        let instance = sample_instance("work-gitlab");
        let credential = sample_credential("token-a");
        let auth_path =
            std::env::temp_dir().join(format!("curator-auth-round-trip-{}.toml", Uuid::new_v4()));
        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: CredentialStore::File,
                file_path: Some(auth_path.display().to_string()),
            },
            ..Config::default()
        };

        let path = save_to_file(&instance, &credential, &config).unwrap();
        let loaded = load_from_file(&instance, &config).unwrap().unwrap();

        assert_eq!(loaded.0, path);
        assert_eq!(loaded.1, credential);
    }

    #[test]
    fn save_to_file_overwrites_existing_instance_only() {
        let _home = TempHome::new("file-overwrite");
        let auth_path =
            std::env::temp_dir().join(format!("curator-auth-overwrite-{}.toml", Uuid::new_v4()));
        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: CredentialStore::File,
                file_path: Some(auth_path.display().to_string()),
            },
            ..Config::default()
        };
        let first = sample_instance("first");
        let second = sample_instance("second");

        save_to_file(&first, &sample_credential("one"), &config).unwrap();
        save_to_file(&second, &sample_credential("two"), &config).unwrap();
        save_to_file(&first, &sample_credential("three"), &config).unwrap();

        let first_loaded = load_from_file(&first, &config).unwrap().unwrap().1;
        let second_loaded = load_from_file(&second, &config).unwrap().unwrap().1;

        assert_eq!(first_loaded.access_token, "three");
        assert_eq!(second_loaded.access_token, "two");
    }

    #[tokio::test]
    async fn save_and_load_db_round_trip() {
        let db = setup_db("db-round-trip").await;
        let instance = sample_instance("db-instance");
        insert_instance(&db, &instance).await;
        let credential = sample_credential("db-token");

        save_to_db(&instance, &credential, &db).await.unwrap();
        let loaded = load_from_db(&instance, &db).await.unwrap().unwrap();

        assert_eq!(loaded, credential);
    }

    #[tokio::test]
    async fn save_to_db_updates_existing_row() {
        let db = setup_db("db-update").await;
        let instance = sample_instance("db-update-instance");
        insert_instance(&db, &instance).await;

        save_to_db(&instance, &sample_credential("old-token"), &db)
            .await
            .unwrap();
        save_to_db(&instance, &sample_credential("new-token"), &db)
            .await
            .unwrap();

        let loaded = load_from_db(&instance, &db).await.unwrap().unwrap();
        assert_eq!(loaded.access_token, "new-token");
    }

    #[tokio::test]
    async fn load_credential_requires_db_connection_for_db_backend() {
        let instance = sample_instance("missing-db");
        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: CredentialStore::Db,
                file_path: None,
            },
            ..Config::default()
        };

        let err = load_credential(&instance, &config, None)
            .await
            .expect_err("db backend should require a db connection");

        assert!(err.to_string().contains("requires a database connection"));
    }

    #[tokio::test]
    async fn update_credential_updates_file_backend() {
        let _home = TempHome::new("update-file");
        let auth_path =
            std::env::temp_dir().join(format!("curator-auth-update-{}.toml", Uuid::new_v4()));
        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: CredentialStore::File,
                file_path: Some(auth_path.display().to_string()),
            },
            ..Config::default()
        };
        let instance = sample_instance("file-instance");
        let path = save_to_file(&instance, &sample_credential("before"), &config).unwrap();

        update_credential(
            &instance,
            &CredentialSource::File(path.clone()),
            &sample_credential("after"),
            None,
        )
        .await
        .unwrap();

        let loaded = load_from_file(&instance, &config).unwrap().unwrap().1;
        assert_eq!(loaded.access_token, "after");
    }

    #[tokio::test]
    async fn update_credential_updates_db_backend() {
        let db = setup_db("update-db").await;
        let instance = sample_instance("db-instance-update");
        insert_instance(&db, &instance).await;
        save_to_db(&instance, &sample_credential("before"), &db)
            .await
            .unwrap();

        update_credential(
            &instance,
            &CredentialSource::Db,
            &sample_credential("after"),
            Some(&db),
        )
        .await
        .unwrap();

        let loaded = load_from_db(&instance, &db).await.unwrap().unwrap();
        assert_eq!(loaded.access_token, "after");
    }

    #[cfg(feature = "github")]
    #[test]
    fn legacy_credential_for_instance_maps_github_source() {
        let github = sample_instance("github");
        let mut github_config = Config::default();
        github_config.github.token = Some("gh-token".to_string());
        let github_legacy = legacy_credential_for_instance(&github, &github_config).unwrap();
        assert_eq!(github_legacy.0.access_token, "gh-token");
        assert_eq!(github_legacy.1, "legacy github config");
    }

    #[cfg(feature = "gitlab")]
    #[test]
    fn legacy_credential_for_instance_maps_gitlab_source() {
        let mut gitlab = sample_instance("gitlab");
        gitlab.platform_type = curator::PlatformType::GitLab;
        let mut gitlab_config = Config::default();
        gitlab_config.gitlab.token = Some("gl-token".to_string());
        gitlab_config.gitlab.refresh_token = Some("refresh".to_string());
        let gitlab_legacy = legacy_credential_for_instance(&gitlab, &gitlab_config).unwrap();
        assert_eq!(gitlab_legacy.0.auth_kind, "oauth");
        assert_eq!(gitlab_legacy.1, "legacy gitlab config");
    }

    #[cfg(feature = "gitea")]
    #[test]
    fn legacy_credential_for_instance_maps_codeberg_and_gitea_sources() {
        let mut codeberg = sample_instance("codeberg");
        codeberg.platform_type = curator::PlatformType::Gitea;
        codeberg.host = "codeberg.org".to_string();
        let mut codeberg_config = Config::default();
        codeberg_config.codeberg.token = Some("cb-token".to_string());
        let codeberg_legacy = legacy_credential_for_instance(&codeberg, &codeberg_config).unwrap();
        assert_eq!(codeberg_legacy.1, "legacy codeberg config");

        let mut gitea = sample_instance("gitea");
        gitea.platform_type = curator::PlatformType::Gitea;
        let mut gitea_config = Config::default();
        gitea_config.gitea.token = Some("gt-token".to_string());
        let gitea_legacy = legacy_credential_for_instance(&gitea, &gitea_config).unwrap();
        assert_eq!(gitea_legacy.1, "legacy gitea config");
    }
}
