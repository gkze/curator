//! Configuration file support for curator.
//!
//! Configuration is loaded with the following precedence (highest to lowest):
//! 1. CLI flags
//! 2. Environment variables (prefixed with `CURATOR_`, e.g., `CURATOR_DATABASE_URL`)
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
//! [github]
//! token = "ghp_..."  # or use CURATOR_GITHUB_TOKEN env var
//!
//! [gitlab]
//! host = "gitlab.com"  # or self-hosted instance
//! token = "glpat-..."  # or use CURATOR_GITLAB_TOKEN env var
//!
//! [codeberg]
//! token = "..."  # or use CURATOR_CODEBERG_TOKEN env var
//!
//! [gitea]
//! host = "https://gitea.example.com"  # self-hosted Gitea/Forgejo instance
//! token = "..."  # or use CURATOR_GITEA_TOKEN env var
//!
//! [sync]
//! active_within_days = 60
//! concurrency = 20
//! star = true
//! no_rate_limit = false
//! ```

use std::path::PathBuf;
#[cfg(feature = "github")]
use std::{fs, io};

use config::{Config as ConfigBuilder, Environment, File, FileFormat};
use directories::ProjectDirs;
use serde::Deserialize;

/// Top-level configuration.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Database configuration.
    pub database: DatabaseConfig,
    /// GitHub configuration.
    pub github: GitHubConfig,
    /// GitLab configuration.
    pub gitlab: GitLabConfig,
    /// Codeberg configuration (codeberg.org).
    pub codeberg: CodebergConfig,
    /// Gitea configuration (self-hosted Gitea/Forgejo).
    pub gitea: GiteaConfig,
    /// Default sync options.
    pub sync: SyncConfig,
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

/// GitHub configuration.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct GitHubConfig {
    /// GitHub API token.
    /// Can also be set via CURATOR_GITHUB_TOKEN environment variable.
    pub token: Option<String>,
}

/// GitLab configuration.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct GitLabConfig {
    /// GitLab host (e.g., "gitlab.com" or "https://gitlab.example.com").
    /// Can also be set via CURATOR_GITLAB_HOST environment variable.
    pub host: Option<String>,
    /// GitLab API token (personal access token).
    /// Can also be set via CURATOR_GITLAB_TOKEN environment variable.
    pub token: Option<String>,
    /// Include projects from subgroups when syncing.
    pub include_subgroups: bool,
}

impl Default for GitLabConfig {
    fn default() -> Self {
        Self {
            host: Some("gitlab.com".to_string()),
            token: None,
            include_subgroups: true,
        }
    }
}

/// Codeberg configuration (for codeberg.org).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct CodebergConfig {
    /// Codeberg API token (personal access token).
    /// Can also be set via CURATOR_CODEBERG_TOKEN environment variable.
    pub token: Option<String>,
}

/// Gitea configuration (for self-hosted Gitea/Forgejo instances).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct GiteaConfig {
    /// Gitea host URL (e.g., "https://gitea.example.com").
    /// Can also be set via CURATOR_GITEA_HOST environment variable.
    pub host: Option<String>,
    /// Gitea API token (personal access token).
    /// Can also be set via CURATOR_GITEA_TOKEN environment variable.
    pub token: Option<String>,
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
    /// Load configuration using the config crate's layered approach.
    ///
    /// Sources are loaded in order (later sources override earlier):
    /// 1. Built-in defaults
    /// 2. XDG config file (~/.config/curator/config.toml)
    /// 3. Local config file (./curator.toml)
    /// 4. Environment variables with CURATOR_ prefix
    /// 5. Legacy environment variables (for backwards compatibility)
    pub fn load() -> Self {
        let mut builder = ConfigBuilder::builder();

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
        builder = builder.add_source(
            Environment::with_prefix("CURATOR")
                .separator("_")
                .try_parsing(true),
        );

        // Build the config and deserialize
        match builder.build() {
            Ok(settings) => match settings.try_deserialize::<Config>() {
                Ok(config) => config,
                Err(e) => {
                    tracing::warn!("Failed to deserialize config: {}", e);
                    Config::default()
                }
            },
            Err(e) => {
                tracing::warn!("Failed to build config: {}", e);
                Config::default()
            }
        }
    }

    /// Get the database URL, falling back to the default state directory path.
    ///
    /// If no database URL is configured, defaults to `sqlite://~/.local/state/curator/curator.db?mode=rwc`
    /// on Linux (using XDG state directory) or the platform-appropriate equivalent.
    /// The `mode=rwc` parameter enables read-write access and creates the file if it doesn't exist.
    pub fn database_url(&self) -> Option<String> {
        self.database.url.clone().or_else(|| {
            Self::default_state_dir().map(|state_dir| {
                let db_path = state_dir.join("curator.db");
                format!("sqlite://{}?mode=rwc", db_path.display())
            })
        })
    }

    /// Get the GitHub token.
    #[cfg(feature = "github")]
    pub fn github_token(&self) -> Option<String> {
        self.github.token.clone()
    }

    /// Get the GitLab host.
    #[cfg(feature = "gitlab")]
    pub fn gitlab_host(&self) -> String {
        self.gitlab
            .host
            .clone()
            .unwrap_or_else(|| "gitlab.com".to_string())
    }

    /// Get the GitLab token.
    #[cfg(feature = "gitlab")]
    pub fn gitlab_token(&self) -> Option<String> {
        self.gitlab.token.clone()
    }

    /// Get the Codeberg token.
    #[cfg(feature = "gitea")]
    pub fn codeberg_token(&self) -> Option<String> {
        self.codeberg.token.clone()
    }

    /// Get the Gitea host.
    #[cfg(feature = "gitea")]
    pub fn gitea_host(&self) -> Option<String> {
        self.gitea.host.clone()
    }

    /// Get the Gitea token.
    #[cfg(feature = "gitea")]
    pub fn gitea_token(&self) -> Option<String> {
        self.gitea.token.clone()
    }

    /// Get the default config file path.
    #[allow(dead_code)]
    pub fn default_config_path() -> Option<PathBuf> {
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

    /// Save a GitHub token to the config file.
    ///
    /// Creates the config file and parent directories if they don't exist.
    /// If a config file already exists, it updates only the `[github]` section,
    /// preserving formatting, comments, and other settings.
    #[cfg(feature = "github")]
    pub fn save_github_token(token: &str) -> io::Result<PathBuf> {
        use toml_edit::{DocumentMut, value};

        let config_path = Self::default_config_path().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "Could not determine config directory",
            )
        })?;

        // Ensure parent directory exists
        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Read existing config or start fresh
        let content = if config_path.exists() {
            fs::read_to_string(&config_path)?
        } else {
            String::new()
        };

        // Parse as TOML document (preserves formatting and comments)
        let mut doc: DocumentMut = content.parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Invalid TOML: {}", e))
        })?;

        // Ensure [github] table exists and set the token
        if !doc.contains_key("github") {
            doc["github"] = toml_edit::table();
        }
        doc["github"]["token"] = value(token);

        // Write back to file
        fs::write(&config_path, doc.to_string())?;
        Ok(config_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.sync.active_within_days, 60);
        assert_eq!(config.sync.concurrency, 20);
        assert!(config.sync.star);
        assert!(!config.sync.no_rate_limit);
        assert!(config.database.url.is_none());
        assert!(config.github.token.is_none());
        assert_eq!(config.gitlab.host, Some("gitlab.com".to_string()));
        assert!(config.gitlab.token.is_none());
        assert!(config.gitlab.include_subgroups);
        assert!(config.codeberg.token.is_none());
        assert!(config.gitea.host.is_none());
        assert!(config.gitea.token.is_none());
    }

    #[test]
    fn test_gitlab_host_default() {
        let config = Config::default();
        assert_eq!(config.gitlab_host(), "gitlab.com");
    }

    #[test]
    fn test_config_builder_with_toml_string() {
        // Test that we can still parse TOML content correctly
        let toml_content = r#"
            [database]
            url = "sqlite:///tmp/test.db"

            [github]
            token = "ghp_test123"

            [sync]
            active_within_days = 90
            concurrency = 10
            star = false
        "#;

        let settings = ConfigBuilder::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        assert_eq!(
            config.database.url,
            Some("sqlite:///tmp/test.db".to_string())
        );
        assert_eq!(config.github.token, Some("ghp_test123".to_string()));
        assert_eq!(config.sync.active_within_days, 90);
        assert_eq!(config.sync.concurrency, 10);
        assert!(!config.sync.star);
    }

    #[test]
    fn test_config_builder_with_defaults() {
        // Test that defaults are applied when no config is provided
        let settings = ConfigBuilder::builder().build().unwrap();

        let config: Config = settings.try_deserialize().unwrap_or_default();

        assert_eq!(config.sync.active_within_days, 60);
        assert_eq!(config.sync.concurrency, 20);
        assert!(config.sync.star);
    }

    #[test]
    fn test_config_builder_partial_override() {
        // Test that partial config overrides only specified values
        let toml_content = r#"
            [sync]
            active_within_days = 30
        "#;

        let settings = ConfigBuilder::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        assert_eq!(config.sync.active_within_days, 30);
        // Other values should be defaults
        assert_eq!(config.sync.concurrency, 20);
        assert!(config.sync.star);
    }

    #[test]
    fn test_full_config_parsing() {
        let toml_content = r#"
            [database]
            url = "sqlite:///tmp/test.db"

            [github]
            token = "ghp_test123"

            [codeberg]
            token = "codeberg_token"

            [gitea]
            host = "https://gitea.example.com"
            token = "gitea_token"

            [sync]
            active_within_days = 90
            concurrency = 10
            star = false
        "#;

        let settings = ConfigBuilder::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        assert_eq!(
            config.database.url,
            Some("sqlite:///tmp/test.db".to_string())
        );
        assert_eq!(config.github.token, Some("ghp_test123".to_string()));
        assert_eq!(config.codeberg.token, Some("codeberg_token".to_string()));
        assert_eq!(
            config.gitea.host,
            Some("https://gitea.example.com".to_string())
        );
        assert_eq!(config.gitea.token, Some("gitea_token".to_string()));
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

        let settings = ConfigBuilder::builder()
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
            host = "gitlab.example.com"
            token = "test_token"
            include_subgroups = false
        "#;

        let settings = ConfigBuilder::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        assert!(!config.gitlab.include_subgroups);
        assert_eq!(config.gitlab.host, Some("gitlab.example.com".to_string()));
    }

    #[test]
    fn test_gitlab_config_default_include_subgroups() {
        let config = GitLabConfig::default();
        assert!(config.include_subgroups);
        assert_eq!(config.host, Some("gitlab.com".to_string()));
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

        // Should return Some with a default SQLite path
        assert!(db_url.is_some());
        let url = db_url.unwrap();
        assert!(url.starts_with("sqlite://"));
        assert!(url.contains("curator.db"));
        assert!(url.ends_with("?mode=rwc"));
    }

    #[test]
    fn test_database_url_respects_configured_value() {
        let toml_content = r#"
            [database]
            url = "postgres://localhost/curator"
        "#;

        let settings = ConfigBuilder::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();
        let db_url = config.database_url();

        assert_eq!(db_url, Some("postgres://localhost/curator".to_string()));
    }

    #[test]
    fn test_default_state_dir() {
        let state_dir = Config::default_state_dir();
        assert!(state_dir.is_some());
        let path = state_dir.unwrap();
        // Should contain "curator" in the path
        assert!(path.to_string_lossy().contains("curator"));
    }

    #[test]
    fn test_github_config_default() {
        let config = GitHubConfig::default();
        assert!(config.token.is_none());
    }

    #[test]
    fn test_codeberg_config_default() {
        let config = CodebergConfig::default();
        assert!(config.token.is_none());
    }

    #[test]
    fn test_gitea_config_default() {
        let config = GiteaConfig::default();
        assert!(config.host.is_none());
        assert!(config.token.is_none());
    }

    #[test]
    fn test_config_gitlab_host_with_custom() {
        let toml_content = r#"
            [gitlab]
            host = "https://gitlab.mycompany.com"
        "#;

        let settings = ConfigBuilder::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        assert_eq!(config.gitlab_host(), "https://gitlab.mycompany.com");
    }

    #[test]
    fn test_config_all_token_methods_none_by_default() {
        let config = Config::default();

        // Without environment variables set, these should all return None
        // Note: These tests may be affected by actual environment variables
        // In practice, we can't easily test env var resolution without
        // actually setting them, but we can test the config file paths
        assert!(config.github.token.is_none());
        assert!(config.gitlab.token.is_none());
        assert!(config.codeberg.token.is_none());
        assert!(config.gitea.token.is_none());
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
        // Test that environment variables with CURATOR_ prefix would be recognized
        // We can verify the Environment source is correctly configured
        let env_source = Environment::with_prefix("CURATOR")
            .separator("_")
            .prefix_separator("_");

        // Just verify it can be created without error
        let _builder = ConfigBuilder::builder().add_source(env_source);
    }

    #[test]
    fn test_config_merging_order() {
        // When multiple sources are added, later sources should override earlier ones
        let base_toml = r#"
            [sync]
            active_within_days = 60
            concurrency = 20
        "#;

        let override_toml = r#"
            [sync]
            active_within_days = 30
        "#;

        let settings = ConfigBuilder::builder()
            .add_source(config::File::from_str(base_toml, FileFormat::Toml))
            .add_source(config::File::from_str(override_toml, FileFormat::Toml))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        // active_within_days should be overridden to 30
        assert_eq!(config.sync.active_within_days, 30);
        // concurrency should remain 20 from base (not overridden)
        assert_eq!(config.sync.concurrency, 20);
    }

    #[test]
    fn test_config_invalid_toml() {
        let invalid_toml = r#"
            [sync
            active_within_days = 60
        "#;

        let result = ConfigBuilder::builder()
            .add_source(config::File::from_str(invalid_toml, FileFormat::Toml))
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_config_unknown_fields_ignored() {
        // Unknown fields should be silently ignored (serde default behavior)
        let toml_content = r#"
            [sync]
            active_within_days = 60
            unknown_field = "should be ignored"
        "#;

        let settings = ConfigBuilder::builder()
            .add_source(config::File::from_str(toml_content, FileFormat::Toml))
            .build()
            .unwrap();

        // This should succeed despite unknown_field
        let config: Config = settings.try_deserialize().unwrap();
        assert_eq!(config.sync.active_within_days, 60);
    }
}
