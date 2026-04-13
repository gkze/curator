//! Configuration file support for curator.
//!
//! Configuration is loaded with the following precedence (highest to lowest):
//! 1. CLI flags
//! 2. Environment variables (prefixed with `CURATOR_`, e.g., `CURATOR_DATABASE_URL`)
//!    including values from `.env` (loaded by `main` before `Config::try_load()`)
//! 3. Config file (~/.config/curator/config.toml or ./curator.toml)
//! 4. Built-in defaults
//!
//! The database URL defaults to `sqlite://~/.local/state/curator/curator.db` on Linux
//! (using the XDG state directory) if not explicitly configured.
//!
//! Example config file:
//! ```toml
//! [database]
//! url = "sqlite://~/.local/state/curator/curator.db"  # optional, this is the default
//!
//! [auth]
//! credential_store = "auto" # auto, keychain, file, or db
//! file_path = "~/.config/curator/auth.toml" # optional file backend path
//! # db stores secrets in the curator database in plaintext-at-rest
//!
//! [gitlab]
//! include_subgroups = true
//!
//! [sync]
//! active_within_days = 60
//! concurrency = 20
//! star = true
//! no_rate_limit = false
//! ```

use std::path::{Path, PathBuf};

use config::{Config as ConfigStore, ConfigBuilder, Environment, File, FileFormat};
use directories::{BaseDirs, ProjectDirs};
use serde::Deserialize;
use url::Url;

/// Top-level configuration.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Database configuration.
    pub database: DatabaseConfig,
    /// Authentication backend configuration.
    pub auth: AuthConfig,
    /// GitLab-specific configuration.
    pub gitlab: GitLabConfig,
    /// Default sync options.
    pub sync: SyncConfig,
}

/// Authentication configuration.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// How per-instance credentials are persisted.
    pub credential_store: CredentialStore,
    /// Optional path for the file credential backend.
    pub file_path: Option<String>,
}

/// Supported credential storage backends.
#[derive(Debug, Default, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialStore {
    #[default]
    Auto,
    Keychain,
    File,
    Db,
}

/// Database configuration.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct DatabaseConfig {
    /// Database connection URL.
    /// Supports sqlite:// and postgres:// schemes.
    /// Defaults to `sqlite://~/.local/state/curator/curator.db` if not specified.
    pub url: Option<String>,
}

/// GitLab configuration.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct GitLabConfig {
    /// Include projects from subgroups when syncing.
    pub include_subgroups: bool,
}

impl Default for GitLabConfig {
    fn default() -> Self {
        Self {
            include_subgroups: true,
        }
    }
}

/// Default sync options.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct SyncConfig {
    /// Only include repos active within this many days.
    pub active_within_days: u64,
    /// Maximum concurrent API requests.
    pub concurrency: usize,
    /// Whether to star repositories by default.
    pub star: bool,
    /// Whether to disable proactive rate limiting.
    pub no_rate_limit: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            active_within_days: 60,
            concurrency: 20,
            star: true,
            no_rate_limit: false,
        }
    }
}

impl Config {
    fn build_config() -> ConfigBuilder<config::builder::DefaultState> {
        let mut builder = ConfigStore::builder();

        // Add XDG config file if it exists
        if let Some(proj_dirs) = ProjectDirs::from("", "", "curator") {
            let xdg_config = proj_dirs.config_dir().join("config.toml");
            if xdg_config.exists() {
                tracing::debug!("Loading config from {:?}", xdg_config);
                builder = builder.add_source(
                    File::from(xdg_config)
                        .format(FileFormat::Toml)
                        .required(false),
                );
            }
        }

        // Add local config file (higher priority than XDG)
        let local_config = PathBuf::from("curator.toml");
        if local_config.exists() {
            tracing::debug!("Loading config from ./curator.toml");
            builder = builder.add_source(
                File::from(local_config)
                    .format(FileFormat::Toml)
                    .required(false),
            );
        }

        // Add CURATOR_ prefixed environment variables
        // e.g., CURATOR_DATABASE_URL -> database.url
        builder.add_source(
            Environment::with_prefix("CURATOR")
                .separator("_")
                .try_parsing(true),
        )
    }

    /// Try to load and deserialize configuration.
    ///
    /// Returns an error when config files are invalid or deserialization fails.
    pub fn try_load() -> std::result::Result<Self, config::ConfigError> {
        let settings = Self::build_config().build()?;
        settings.try_deserialize::<Config>()
    }

    /// Get the database URL, falling back to the default state directory path.
    ///
    /// If no database URL is configured, defaults to `sqlite://~/.local/state/curator/curator.db?mode=rwc`
    /// on Linux (using XDG state directory) or the platform-appropriate equivalent.
    /// The `mode=rwc` parameter enables read-write access and creates the file if it doesn't exist.
    ///
    /// Note: SQLite-specific pragmas (WAL mode, busy timeout) are configured automatically
    /// when connecting via `curator::db::connect()` or `curator::db::connect_and_migrate()`.
    pub fn database_url(&self) -> Option<String> {
        self.database
            .url
            .as_deref()
            .map(normalize_configured_database_url)
            .or_else(|| {
                Self::default_state_dir().map(|state_dir| {
                    let db_path = state_dir.join("curator.db");
                    sqlite_database_url(&db_path)
                })
            })
    }

    /// Get the configured credential store mode.
    pub fn credential_store(&self) -> CredentialStore {
        self.auth.credential_store
    }

    /// Get the auth file path for the file credential backend.
    pub fn auth_file_path(&self) -> Option<PathBuf> {
        if let Some(path) = &self.auth.file_path {
            return Some(expand_user_path(path));
        }

        Self::default_config_path().and_then(|path| path.parent().map(|p| p.join("auth.toml")))
    }

    /// Get the default config file path.
    ///
    /// Prefers `$XDG_CONFIG_HOME/curator/config.toml` when the environment
    /// variable is set (on any platform), falling back to the
    /// platform-specific config directory from the `directories` crate.
    #[allow(dead_code)]
    pub fn default_config_path() -> Option<PathBuf> {
        if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
            let p = PathBuf::from(xdg);
            if p.is_absolute() {
                return Some(p.join("curator").join("config.toml"));
            }
        }
        ProjectDirs::from("", "", "curator").map(|dirs| dirs.config_dir().join("config.toml"))
    }

    /// Get the default data directory path.
    #[allow(dead_code)]
    pub fn default_data_dir() -> Option<PathBuf> {
        ProjectDirs::from("", "", "curator").map(|dirs| dirs.data_dir().to_path_buf())
    }

    /// Get the default state directory path.
    ///
    /// On Linux, this is `$XDG_STATE_HOME/curator` or `~/.local/state/curator`.
    /// On macOS/Windows, falls back to the data directory.
    pub fn default_state_dir() -> Option<PathBuf> {
        ProjectDirs::from("", "", "curator").map(|dirs| {
            // state_dir() returns None on macOS/Windows, fall back to data_dir
            dirs.state_dir()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| dirs.data_dir().to_path_buf())
        })
    }
}

fn sqlite_database_url(path: &Path) -> String {
    sqlite_database_url_with_suffix(path, "?mode=rwc")
}

fn sqlite_database_url_with_suffix(path: &Path, suffix: &str) -> String {
    // Build through file:// first so path components are URL-encoded.
    match Url::from_file_path(path) {
        Ok(file_url) => {
            let encoded_path = file_url.to_string();
            format!(
                "sqlite://{}{}",
                encoded_path.trim_start_matches("file://"),
                suffix
            )
        }
        Err(_) => format!("sqlite://{}{}", path.display(), suffix),
    }
}

pub(crate) fn sqlite_database_parent(database_url: &str) -> Option<(PathBuf, bool)> {
    let db_path = sqlite_database_path(database_url)?;
    let parent = db_path.parent()?.to_path_buf();
    let warn_relative = db_path.is_relative() && !parent.as_os_str().is_empty();
    Some((parent, warn_relative))
}

fn normalize_configured_database_url(database_url: &str) -> String {
    let Some((path, suffix)) = split_sqlite_database_url(database_url) else {
        return database_url.to_string();
    };

    if !path_uses_home_shorthand(path) {
        return database_url.to_string();
    }

    sqlite_database_url_with_suffix(&expand_user_path(path), suffix)
}

fn sqlite_database_path(database_url: &str) -> Option<PathBuf> {
    let (path, _) = split_sqlite_database_url(database_url)?;
    if path.is_empty() {
        return None;
    }

    if path_uses_home_shorthand(path) {
        return Some(expand_user_path(path));
    }

    if path.starts_with('/') {
        return Some(decode_absolute_sqlite_path(path).unwrap_or_else(|| PathBuf::from(path)));
    }

    Some(PathBuf::from(path))
}

fn split_sqlite_database_url(database_url: &str) -> Option<(&str, &str)> {
    let path_and_suffix = database_url.strip_prefix("sqlite://")?;
    let split_at = path_and_suffix
        .find(['?', '#'])
        .unwrap_or(path_and_suffix.len());
    Some((&path_and_suffix[..split_at], &path_and_suffix[split_at..]))
}

fn decode_absolute_sqlite_path(path: &str) -> Option<PathBuf> {
    Url::parse(&format!("file://{path}"))
        .ok()?
        .to_file_path()
        .ok()
}

fn path_uses_home_shorthand(path: &str) -> bool {
    path == "~" || path.starts_with("~/")
}

pub(crate) fn expand_user_path(path: &str) -> PathBuf {
    if !path_uses_home_shorthand(path) {
        return PathBuf::from(path);
    }

    let Some(home_dir) = BaseDirs::new().map(|dirs| dirs.home_dir().to_path_buf()) else {
        return PathBuf::from(path);
    };

    match path.strip_prefix("~/") {
        Some(suffix) => home_dir.join(suffix),
        None => home_dir,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::env_lock;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.auth.credential_store, CredentialStore::Auto);
        assert_eq!(config.sync.active_within_days, 60);
        assert_eq!(config.sync.concurrency, 20);
        assert!(config.sync.star);
        assert!(!config.sync.no_rate_limit);
        assert!(config.database.url.is_none());
        assert!(config.gitlab.include_subgroups);
    }

    #[test]
    fn test_config_builder_with_toml_string() {
        let toml_content = r#"
            [database]
            url = "sqlite:///tmp/test.db"

            [auth]
            credential_store = "db"

            [gitlab]
            include_subgroups = false

            [sync]
            active_within_days = 90
            concurrency = 10
            star = false
        "#;

        let settings = ConfigStore::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        assert_eq!(
            config.database.url,
            Some("sqlite:///tmp/test.db".to_string())
        );
        assert_eq!(config.auth.credential_store, CredentialStore::Db);
        assert!(!config.gitlab.include_subgroups);
        assert_eq!(config.sync.active_within_days, 90);
        assert_eq!(config.sync.concurrency, 10);
        assert!(!config.sync.star);
    }

    #[test]
    fn test_config_builder_with_defaults() {
        let settings = ConfigStore::builder().build().unwrap();
        let config: Config = settings.try_deserialize().unwrap_or_default();

        assert_eq!(config.sync.active_within_days, 60);
        assert_eq!(config.sync.concurrency, 20);
        assert!(config.sync.star);
    }

    #[test]
    fn test_config_builder_partial_override() {
        let toml_content = r#"
            [sync]
            active_within_days = 30
        "#;

        let settings = ConfigStore::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        assert_eq!(config.sync.active_within_days, 30);
        assert_eq!(config.sync.concurrency, 20);
        assert!(config.sync.star);
    }

    #[test]
    fn test_full_config_parsing() {
        let toml_content = r#"
            [database]
            url = "sqlite:///tmp/test.db"

            [auth]
            credential_store = "file"
            file_path = "/tmp/curator-auth.toml"

            [gitlab]
            include_subgroups = false

            [sync]
            active_within_days = 90
            concurrency = 10
            star = false
        "#;

        let settings = ConfigStore::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        assert_eq!(
            config.database.url,
            Some("sqlite:///tmp/test.db".to_string())
        );
        assert_eq!(config.auth.credential_store, CredentialStore::File);
        assert_eq!(
            config.auth.file_path,
            Some("/tmp/curator-auth.toml".to_string())
        );
        assert!(!config.gitlab.include_subgroups);
        assert_eq!(config.sync.active_within_days, 90);
        assert_eq!(config.sync.concurrency, 10);
        assert!(!config.sync.star);
    }

    #[test]
    fn test_sync_config_no_rate_limit() {
        let toml_content = r#"
            [sync]
            no_rate_limit = true
        "#;

        let settings = ConfigStore::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        assert!(config.sync.no_rate_limit);
    }

    #[test]
    fn test_sync_config_default_no_rate_limit() {
        let config = SyncConfig::default();
        assert!(!config.no_rate_limit);
    }

    #[test]
    fn test_gitlab_config_include_subgroups() {
        let toml_content = r#"
            [gitlab]
            include_subgroups = false
        "#;

        let settings = ConfigStore::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        assert!(!config.gitlab.include_subgroups);
    }

    #[test]
    fn test_gitlab_config_default_include_subgroups() {
        let config = GitLabConfig::default();
        assert!(config.include_subgroups);
    }

    #[test]
    fn test_database_config_default() {
        let config = DatabaseConfig::default();
        assert!(config.url.is_none());
    }

    #[test]
    fn test_database_url_defaults_to_state_dir() {
        let config = Config::default();
        let db_url = config.database_url();

        assert!(db_url.is_some());
        let url = db_url.unwrap();
        assert!(url.starts_with("sqlite://"));
        assert!(url.contains("curator.db"));
        assert!(url.ends_with("?mode=rwc"));
    }

    #[test]
    fn test_sqlite_database_url_encodes_special_characters() {
        let db_path = PathBuf::from("/tmp/curator db/with#chars?.db");
        let url = sqlite_database_url(&db_path);

        assert!(url.starts_with("sqlite:///tmp/curator%20db/with%23chars%3F.db"));
        assert!(url.ends_with("?mode=rwc"));
    }

    #[test]
    fn test_database_url_respects_configured_value() {
        let toml_content = r#"
            [database]
            url = "postgres://localhost/curator"
        "#;

        let settings = ConfigStore::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();
        let db_url = config.database_url();

        assert_eq!(db_url, Some("postgres://localhost/curator".to_string()));
    }

    #[tokio::test]
    async fn test_auth_file_path_expands_home_shorthand() {
        let _guard = env_lock().lock().await;
        let home_dir =
            std::env::temp_dir().join(format!("curator-config-home-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&home_dir).unwrap();
        let previous_home = std::env::var_os("HOME");

        unsafe {
            std::env::set_var("HOME", &home_dir);
        }

        let config = Config {
            auth: AuthConfig {
                credential_store: CredentialStore::File,
                file_path: Some("~/.config/curator/auth.toml".to_string()),
            },
            ..Config::default()
        };

        assert_eq!(
            config.auth_file_path(),
            Some(home_dir.join(".config").join("curator").join("auth.toml"))
        );

        match previous_home {
            Some(home) => unsafe { std::env::set_var("HOME", home) },
            None => unsafe { std::env::remove_var("HOME") },
        }
        let _ = std::fs::remove_dir_all(&home_dir);
    }

    #[tokio::test]
    async fn test_database_url_expands_home_shorthand_for_configured_sqlite_url() {
        let _guard = env_lock().lock().await;
        let home_dir =
            std::env::temp_dir().join(format!("curator-db-home-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&home_dir).unwrap();
        let previous_home = std::env::var_os("HOME");

        unsafe {
            std::env::set_var("HOME", &home_dir);
        }

        let config = Config {
            database: DatabaseConfig {
                url: Some("sqlite://~/.local/state/curator/curator.db?mode=ro".to_string()),
            },
            ..Config::default()
        };

        assert_eq!(
            config.database_url(),
            Some(format!(
                "sqlite://{}?mode=ro",
                home_dir
                    .join(".local")
                    .join("state")
                    .join("curator")
                    .join("curator.db")
                    .display()
            ))
        );

        match previous_home {
            Some(home) => unsafe { std::env::set_var("HOME", home) },
            None => unsafe { std::env::remove_var("HOME") },
        }
        let _ = std::fs::remove_dir_all(&home_dir);
    }

    #[test]
    fn test_sqlite_database_parent_decodes_encoded_paths() {
        let (parent, warn_relative) =
            sqlite_database_parent("sqlite:///tmp/curator%20db/with%23chars%3F.db?mode=rwc")
                .unwrap();

        assert_eq!(parent, PathBuf::from("/tmp/curator db"));
        assert!(!warn_relative);
    }

    #[test]
    fn test_default_state_dir() {
        let state_dir = Config::default_state_dir();
        assert!(state_dir.is_some());
        let path = state_dir.unwrap();
        assert!(path.to_string_lossy().contains("curator"));
    }

    #[test]
    fn test_sync_config_all_fields() {
        let config = SyncConfig {
            active_within_days: 30,
            concurrency: 5,
            star: false,
            no_rate_limit: true,
        };

        assert_eq!(config.active_within_days, 30);
        assert_eq!(config.concurrency, 5);
        assert!(!config.star);
        assert!(config.no_rate_limit);
    }

    #[test]
    fn test_environment_prefix() {
        let env_source = Environment::with_prefix("CURATOR")
            .separator("_")
            .prefix_separator("_");

        let _builder = ConfigStore::builder().add_source(env_source);
    }

    #[test]
    fn test_config_merging_order() {
        let base_toml = r#"
            [sync]
            active_within_days = 60
            concurrency = 20
        "#;

        let override_toml = r#"
            [sync]
            active_within_days = 30
        "#;

        let settings = ConfigStore::builder()
            .add_source(config::File::from_str(base_toml, FileFormat::Toml))
            .add_source(config::File::from_str(override_toml, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        assert_eq!(config.sync.active_within_days, 30);
        assert_eq!(config.sync.concurrency, 20);
    }

    #[test]
    fn test_config_invalid_toml() {
        let invalid_toml = r#"
            [sync
            active_within_days = 60
        "#;

        let result = ConfigStore::builder()
            .add_source(config::File::from_str(invalid_toml, FileFormat::Toml))
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_config_unknown_fields_ignored() {
        let toml_content = r#"
            [github]
            token = "ignored"

            [gitea]
            host = "https://gitea.example.com"
            token = "ignored"

            [sync]
            active_within_days = 60
            unknown_field = "should be ignored"
        "#;

        let settings = ConfigStore::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();
        assert_eq!(config.sync.active_within_days, 60);
    }
}
