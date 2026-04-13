//! Instance management commands.
//!
//! Commands for managing platform instances (add, list, remove, show).

use chrono::Utc;
use clap::Subcommand;
use console::style;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    PaginatorTrait, QueryFilter, Set,
};
use tabled::{Table, Tabled, settings::Style};
use url::Url;
use uuid::Uuid;

use curator::{
    CodeRepository, CodeRepositoryColumn, Instance, InstanceColumn, InstanceModel, PlatformType,
    entity::instance::well_known,
};

use super::OutputFormat;
use crate::config::Config;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use crate::credentials::{
    CredentialSource, CredentialStatus, credential_status, delete_credential,
};

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum OauthFlowArg {
    Auto,
    Device,
    Pkce,
    Token,
}

impl OauthFlowArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Device => "device",
            Self::Pkce => "pkce",
            Self::Token => "token",
        }
    }
}

/// Instance management subcommands.
#[derive(Subcommand)]
pub enum InstanceAction {
    /// Add a new instance
    ///
    /// Add a well-known instance (github, gitlab, codeberg) or a custom instance
    /// with a specified platform type and host.
    Add {
        /// Instance name (e.g., "github", "work-gitlab", "my-gitea")
        name: String,

        /// Platform type (github, gitlab, gitea)
        /// Required for custom instances, inferred for well-known names
        #[arg(short = 't', long)]
        platform_type: Option<String>,

        /// Host URL without protocol (e.g., "gitlab.mycompany.com")
        /// Required for custom instances, inferred for well-known names
        #[arg(short = 'H', long)]
        host: Option<String>,

        /// OAuth Client ID for this instance.
        ///
        /// Required for OAuth login on custom/self-hosted instances.
        #[arg(short = 'c', long)]
        oauth_client_id: Option<String>,

        /// Preferred auth flow for this instance.
        ///
        /// `auto` tries device flow, then PKCE, then token login.
        #[arg(short = 'f', long, value_enum, default_value_t = OauthFlowArg::Auto)]
        oauth_flow: OauthFlowArg,
    },
    /// Update auth settings for an existing instance
    Update {
        /// Instance name
        name: String,

        /// OAuth Client ID for this instance.
        #[arg(short = 'c', long)]
        oauth_client_id: Option<String>,

        /// Clear the stored OAuth Client ID.
        #[arg(long)]
        clear_oauth_client_id: bool,

        /// Preferred auth flow for this instance.
        #[arg(short = 'f', long, value_enum)]
        oauth_flow: Option<OauthFlowArg>,
    },
    /// List all configured instances
    List {
        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
    /// Remove an instance
    ///
    /// Warning: This will also remove all repositories associated with the instance.
    Remove {
        /// Instance name to remove
        name: String,

        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Show details of an instance
    Show {
        /// Instance name
        name: String,

        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
}

/// Display struct for instance listing.
#[derive(Debug, Clone, serde::Serialize, Tabled)]
struct InstanceDisplay {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Platform")]
    platform_type: String,
    #[tabled(rename = "Host")]
    host: String,
    #[tabled(rename = "API URL")]
    api_url: String,
    #[tabled(rename = "Created")]
    created_at: String,
}

impl From<&InstanceModel> for InstanceDisplay {
    fn from(instance: &InstanceModel) -> Self {
        Self {
            name: instance.name.clone(),
            platform_type: instance.platform_type.to_string(),
            host: instance.host.clone(),
            api_url: instance.api_url(),
            created_at: instance.created_at.format("%Y-%m-%d").to_string(),
        }
    }
}

/// Handle instance management commands.
pub async fn handle_instance(
    action: InstanceAction,
    db: &DatabaseConnection,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        InstanceAction::Add {
            name,
            platform_type,
            host,
            oauth_client_id,
            oauth_flow,
        } => {
            add_instance(
                db,
                &name,
                platform_type.as_deref(),
                host.as_deref(),
                oauth_client_id,
                oauth_flow,
            )
            .await?;
        }
        InstanceAction::Update {
            name,
            oauth_client_id,
            clear_oauth_client_id,
            oauth_flow,
        } => {
            update_instance(
                db,
                &name,
                oauth_client_id,
                clear_oauth_client_id,
                oauth_flow,
            )
            .await?;
        }
        InstanceAction::List { output } => {
            list_instances(db, output).await?;
        }
        InstanceAction::Remove { name, yes } => {
            remove_instance(db, &name, yes, config).await?;
        }
        InstanceAction::Show { name, output } => {
            show_instance(db, &name, output, config).await?;
        }
    }
    Ok(())
}

/// Update auth settings for an existing instance.
async fn update_instance(
    db: &DatabaseConnection,
    name: &str,
    oauth_client_id: Option<String>,
    clear_oauth_client_id: bool,
    oauth_flow: Option<OauthFlowArg>,
) -> Result<(), Box<dyn std::error::Error>> {
    if oauth_client_id.is_some() && clear_oauth_client_id {
        return Err("Use either --oauth-client-id or --clear-oauth-client-id, not both".into());
    }

    let mut instance = Instance::find()
        .filter(InstanceColumn::Name.eq(name))
        .one(db)
        .await?
        .ok_or_else(|| format!("Instance '{}' not found", name))?
        .into_active_model();

    let mut changed = false;

    if let Some(client_id) = oauth_client_id {
        instance.oauth_client_id = Set(Some(client_id));
        changed = true;
    } else if clear_oauth_client_id {
        instance.oauth_client_id = Set(None);
        changed = true;
    }

    if let Some(flow) = oauth_flow {
        instance.oauth_flow = Set(flow.as_str().to_string());
        changed = true;
    }

    if !changed {
        return Err(
            "No updates requested. Set --oauth-client-id, --clear-oauth-client-id, or --oauth-flow"
                .into(),
        );
    }

    let updated = instance.update(db).await?;

    println!(
        "{} Updated instance '{}' auth settings",
        style("✓").green().bold(),
        style(&updated.name).cyan()
    );
    println!(
        "  OAuth Client ID: {}",
        style(
            updated
                .oauth_client_id
                .as_deref()
                .unwrap_or("(not set)")
                .to_string()
        )
        .dim()
        .cyan()
    );
    println!("  OAuth flow: {}", style(updated.oauth_flow).dim().cyan());

    Ok(())
}

/// Add a new instance to the database.
async fn add_instance(
    db: &DatabaseConnection,
    name: &str,
    platform_type: Option<&str>,
    host: Option<&str>,
    oauth_client_id: Option<String>,
    oauth_flow: OauthFlowArg,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if instance already exists
    let existing = Instance::find()
        .filter(InstanceColumn::Name.eq(name))
        .one(db)
        .await?;

    if existing.is_some() {
        return Err(format!("Instance '{}' already exists", name).into());
    }

    // Determine the instance configuration - check well-known first
    let (pt, h, oauth_client_id) = if let Some(wk) = well_known::by_name(name) {
        (
            wk.platform_type,
            wk.host.to_string(),
            oauth_client_id.or_else(|| wk.oauth_client_id.map(ToString::to_string)),
        )
    } else {
        // Custom instance - require platform_type and host
        let pt = platform_type
            .ok_or_else(|| {
                format!(
                    "Platform type is required for custom instance '{}'. Use -t/--platform-type (github, gitlab, gitea)",
                    name
                )
            })?
            .parse::<PlatformType>()
            .map_err(|e| format!("Invalid platform type: {}", e))?;

        let h = host
            .ok_or_else(|| {
                format!(
                    "Host is required for custom instance '{}'. Use -H/--host (e.g., gitlab.mycompany.com)",
                    name
                )
            })
            .and_then(|raw| normalize_host(raw).map_err(|e| e.to_string()))?;

        (pt, h, oauth_client_id)
    };

    // Create the instance
    let instance = curator::entity::instance::ActiveModel {
        id: Set(Uuid::new_v4()),
        name: Set(name.to_string()),
        platform_type: Set(pt),
        host: Set(h.clone()),
        oauth_client_id: Set(oauth_client_id.clone()),
        oauth_flow: Set(oauth_flow.as_str().to_string()),
        created_at: Set(Utc::now().fixed_offset()),
    };

    let result = instance.insert(db).await?;

    println!(
        "{} Added instance '{}' ({} @ {})",
        style("✓").green().bold(),
        style(&result.name).cyan(),
        result.platform_type,
        result.host
    );

    if let Some(client_id) = oauth_client_id {
        println!(
            "  OAuth Client ID configured: {}",
            style(client_id).dim().cyan()
        );
    }
    println!("  OAuth flow: {}", style(oauth_flow.as_str()).dim().cyan());

    Ok(())
}

fn normalize_host(input: &str) -> Result<String, &'static str> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("Host cannot be empty");
    }

    let with_scheme = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("https://{trimmed}")
    };

    let parsed = Url::parse(&with_scheme).map_err(|_| "Host must be a valid hostname")?;

    if !matches!(parsed.scheme(), "http" | "https") {
        return Err("Host must use http or https when a scheme is provided");
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("Host must not include credentials");
    }

    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err("Host must not include query parameters or fragments");
    }

    if parsed.path() != "/" && !parsed.path().is_empty() {
        return Err("Host must not include a path");
    }

    let hostname = parsed
        .host_str()
        .ok_or("Host must include a valid domain or IP address")?
        .trim_end_matches('.')
        .to_ascii_lowercase();

    if hostname.is_empty() {
        return Err("Host must include a valid domain or IP address");
    }

    Ok(match parsed.port() {
        Some(port) => format!("{hostname}:{port}"),
        None => hostname,
    })
}

/// List all configured instances.
async fn list_instances(
    db: &DatabaseConnection,
    output: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let instances = Instance::find().all(db).await?;

    if instances.is_empty() {
        println!("No instances configured.");
        println!();
        println!("Add an instance with:");
        println!("  curator instance add github      # Well-known instance");
        println!("  curator instance add my-gitlab -t gitlab -H gitlab.mycompany.com  # Custom");
        return Ok(());
    }

    let displays: Vec<InstanceDisplay> = instances.iter().map(InstanceDisplay::from).collect();

    match output {
        OutputFormat::Table => {
            let mut table = Table::new(displays);
            table.with(Style::rounded());
            println!("{}", table);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&displays)?);
        }
    }

    Ok(())
}

/// Remove an instance from the database.
async fn remove_instance(
    db: &DatabaseConnection,
    name: &str,
    skip_confirm: bool,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    // Find the instance
    let instance = Instance::find()
        .filter(InstanceColumn::Name.eq(name))
        .one(db)
        .await?
        .ok_or_else(|| format!("Instance '{}' not found", name))?;

    // Count associated repositories
    let repo_count = CodeRepository::find()
        .filter(CodeRepositoryColumn::InstanceId.eq(instance.id))
        .count(db)
        .await?;

    // Confirm deletion
    if !skip_confirm {
        println!(
            "{} About to remove instance '{}' ({} @ {})",
            style("⚠").yellow().bold(),
            style(&instance.name).cyan(),
            instance.platform_type,
            instance.host
        );

        if repo_count > 0 {
            println!(
                "  {} {} associated repositor{} will also be removed!",
                style("Warning:").yellow().bold(),
                repo_count,
                if repo_count == 1 { "y" } else { "ies" }
            );
        }

        print!("Continue? [y/N] ");
        use std::io::{self, Write};
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    remove_instance_credentials(&instance, config, db).await?;

    // Delete associated repositories first (cascade)
    if repo_count > 0 {
        CodeRepository::delete_many()
            .filter(CodeRepositoryColumn::InstanceId.eq(instance.id))
            .exec(db)
            .await?;
    }

    // Delete the instance
    Instance::delete_by_id(instance.id).exec(db).await?;

    println!(
        "{} Removed instance '{}' (and {} repositor{})",
        style("✓").green().bold(),
        style(name).cyan(),
        repo_count,
        if repo_count == 1 { "y" } else { "ies" }
    );

    Ok(())
}

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
async fn remove_instance_credentials(
    instance: &InstanceModel,
    config: &Config,
    db: &DatabaseConnection,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(source) = credential_status(instance, config, Some(db))
        .await?
        .active_source
    {
        if should_delete_credentials_on_instance_remove(&source) {
            delete_credential(instance, &source, Some(db)).await?;
        } else {
            tracing::info!(
                instance = %instance.name,
                host = %instance.host,
                "retaining shared keychain credential while removing instance"
            );
        }
    }

    Ok(())
}

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
fn should_delete_credentials_on_instance_remove(source: &CredentialSource) -> bool {
    !matches!(source, CredentialSource::Keychain)
}

#[cfg(not(any(feature = "github", feature = "gitlab", feature = "gitea")))]
async fn remove_instance_credentials(
    _instance: &InstanceModel,
    _config: &Config,
    _db: &DatabaseConnection,
) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

/// Show details of an instance.
async fn show_instance(
    db: &DatabaseConnection,
    name: &str,
    output: OutputFormat,
    #[allow(unused_variables)] config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    // Find the instance
    let instance = Instance::find()
        .filter(InstanceColumn::Name.eq(name))
        .one(db)
        .await?
        .ok_or_else(|| format!("Instance '{}' not found", name))?;

    // Count associated repositories
    let repo_count = CodeRepository::find()
        .filter(CodeRepositoryColumn::InstanceId.eq(instance.id))
        .count(db)
        .await?;

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    {
        let auth_status = credential_status(&instance, config, Some(db)).await?;
        render_instance_output(
            output,
            build_instance_details_with_auth(&instance, repo_count, &auth_status),
            build_instance_json_with_auth(instance, repo_count, auth_status),
        )?;
    }

    #[cfg(not(any(feature = "github", feature = "gitlab", feature = "gitea")))]
    {
        render_instance_output(
            output,
            build_instance_details_basic(&instance, repo_count),
            build_instance_json(instance, repo_count),
        )?;
    }

    Ok(())
}

#[allow(dead_code)]
#[derive(Debug, serde::Serialize, Tabled)]
struct InstanceDetail {
    #[tabled(rename = "Property")]
    property: String,
    #[tabled(rename = "Value")]
    value: String,
}

#[derive(serde::Serialize)]
struct InstanceJson {
    #[serde(flatten)]
    instance: InstanceModel,
    base_url: String,
    api_url: String,
    repository_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    credential_store: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    credential_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_expires_at: Option<u64>,
}

fn render_instance_output<T: serde::Serialize>(
    output: OutputFormat,
    details: Vec<InstanceDetail>,
    json: T,
) -> Result<(), Box<dyn std::error::Error>> {
    match output {
        OutputFormat::Table => {
            let mut table = Table::new(details);
            table.with(Style::rounded());
            println!("{}", table);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
    }

    Ok(())
}

fn build_instance_details_base(instance: &InstanceModel, repo_count: u64) -> Vec<InstanceDetail> {
    vec![
        InstanceDetail {
            property: "Name".to_string(),
            value: instance.name.clone(),
        },
        InstanceDetail {
            property: "Platform Type".to_string(),
            value: instance.platform_type.to_string(),
        },
        InstanceDetail {
            property: "Host".to_string(),
            value: instance.host.clone(),
        },
        InstanceDetail {
            property: "Base URL".to_string(),
            value: instance.base_url(),
        },
        InstanceDetail {
            property: "API URL".to_string(),
            value: instance.api_url(),
        },
        InstanceDetail {
            property: "Repositories".to_string(),
            value: repo_count.to_string(),
        },
    ]
}

fn finalize_instance_details(
    mut details: Vec<InstanceDetail>,
    instance: &InstanceModel,
) -> Vec<InstanceDetail> {
    details.extend([
        InstanceDetail {
            property: "Created".to_string(),
            value: instance
                .created_at
                .format("%Y-%m-%d %H:%M:%S %Z")
                .to_string(),
        },
        InstanceDetail {
            property: "ID".to_string(),
            value: instance.id.to_string(),
        },
    ]);
    details
}

#[cfg(not(any(feature = "github", feature = "gitlab", feature = "gitea")))]
fn build_instance_details_basic(instance: &InstanceModel, repo_count: u64) -> Vec<InstanceDetail> {
    finalize_instance_details(build_instance_details_base(instance, repo_count), instance)
}

fn build_instance_json(instance: InstanceModel, repo_count: u64) -> InstanceJson {
    InstanceJson {
        base_url: instance.base_url(),
        api_url: instance.api_url(),
        repository_count: repo_count,
        credential_store: None,
        credential_status: None,
        auth_kind: None,
        token_expires_at: None,
        instance,
    }
}

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
fn build_instance_details_with_auth(
    instance: &InstanceModel,
    repo_count: u64,
    auth_status: &CredentialStatus,
) -> Vec<InstanceDetail> {
    let mut details = build_instance_details_base(instance, repo_count);
    details.splice(
        3..3,
        [
            InstanceDetail {
                property: "OAuth Client ID".to_string(),
                value: instance
                    .oauth_client_id
                    .clone()
                    .unwrap_or_else(|| "(not set)".to_string()),
            },
            InstanceDetail {
                property: "OAuth Flow".to_string(),
                value: instance.oauth_flow.clone(),
            },
        ],
    );
    details.extend([
        InstanceDetail {
            property: "Credential Store".to_string(),
            value: format!("{:?}", auth_status.configured_store).to_lowercase(),
        },
        InstanceDetail {
            property: "Credential Status".to_string(),
            value: format_instance_credential_status(auth_status.active_source.as_ref()),
        },
        InstanceDetail {
            property: "Auth Kind".to_string(),
            value: auth_status
                .auth_kind
                .clone()
                .unwrap_or_else(|| "(unknown)".to_string()),
        },
        InstanceDetail {
            property: "Token Expires At".to_string(),
            value: auth_status
                .token_expires_at
                .map(|value| value.to_string())
                .unwrap_or_else(|| "(not set)".to_string()),
        },
    ]);

    finalize_instance_details(details, instance)
}

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
fn build_instance_json_with_auth(
    instance: InstanceModel,
    repo_count: u64,
    auth_status: CredentialStatus,
) -> InstanceJson {
    let mut json = build_instance_json(instance, repo_count);
    json.credential_store = Some(format!("{:?}", auth_status.configured_store).to_lowercase());
    json.credential_status = Some(format_instance_credential_status(
        auth_status.active_source.as_ref(),
    ));
    json.auth_kind = auth_status.auth_kind;
    json.token_expires_at = auth_status.token_expires_at;
    json
}

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
fn format_instance_credential_status(source: Option<&CredentialSource>) -> String {
    match source {
        Some(CredentialSource::Keychain) => "configured (keychain)".to_string(),
        Some(CredentialSource::File(_)) => "configured (file)".to_string(),
        Some(CredentialSource::Db) => "configured (db)".to_string(),
        None => "missing".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthConfig, CredentialStore};
    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    use crate::credentials::{StoredCredential, load_credential, save_credential};
    use sea_orm::DatabaseConnection;

    fn sqlite_test_url(label: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "curator-cli-instance-tests-{}-{}.db",
            label,
            Uuid::new_v4()
        ));
        format!("sqlite://{}?mode=rwc", path.display())
    }

    async fn setup_db(label: &str) -> DatabaseConnection {
        curator::db::connect_and_migrate(&sqlite_test_url(label))
            .await
            .expect("test database should initialize")
    }

    fn isolated_auth_config() -> Config {
        Config {
            auth: AuthConfig {
                credential_store: CredentialStore::Db,
                file_path: None,
            },
            ..Config::default()
        }
    }

    #[test]
    fn normalize_host_accepts_plain_hostname() {
        assert_eq!(
            normalize_host("GitLab.MyCompany.com").unwrap(),
            "gitlab.mycompany.com"
        );
    }

    #[test]
    fn normalize_host_strips_scheme_and_trailing_dot() {
        assert_eq!(
            normalize_host("https://example.com.").unwrap(),
            "example.com"
        );
    }

    #[test]
    fn normalize_host_keeps_port() {
        assert_eq!(
            normalize_host("http://example.com:8443").unwrap(),
            "example.com:8443"
        );
    }

    #[test]
    fn normalize_host_accepts_explicit_root_path() {
        assert_eq!(
            normalize_host("https://Example.COM/").unwrap(),
            "example.com"
        );
    }

    #[test]
    fn normalize_host_rejects_path_or_query() {
        assert!(normalize_host("example.com/path").is_err());
        assert!(normalize_host("example.com?x=1").is_err());
        assert_eq!(
            normalize_host("example.com#frag").unwrap_err(),
            "Host must not include query parameters or fragments"
        );
    }

    #[test]
    fn normalize_host_rejects_credentials_and_bad_scheme() {
        assert_eq!(
            normalize_host("https://user:pass@example.com").unwrap_err(),
            "Host must not include credentials"
        );
        assert_eq!(
            normalize_host("ftp://example.com").unwrap_err(),
            "Host must use http or https when a scheme is provided"
        );
    }

    #[test]
    fn normalize_host_rejects_non_root_path_with_scheme() {
        assert_eq!(
            normalize_host("https://example.com/api/v4").unwrap_err(),
            "Host must not include a path"
        );
    }

    #[test]
    fn normalize_host_accepts_ipv4_with_port() {
        assert_eq!(
            normalize_host("192.168.1.10:8443").unwrap(),
            "192.168.1.10:8443"
        );
    }

    #[test]
    fn normalize_host_rejects_empty_and_missing_hostname() {
        assert_eq!(normalize_host("   ").unwrap_err(), "Host cannot be empty");
        assert_eq!(
            normalize_host("https://").unwrap_err(),
            "Host must be a valid hostname"
        );
    }

    #[test]
    fn oauth_flow_arg_as_str_covers_all_variants() {
        assert_eq!(OauthFlowArg::Auto.as_str(), "auto");
        assert_eq!(OauthFlowArg::Device.as_str(), "device");
        assert_eq!(OauthFlowArg::Pkce.as_str(), "pkce");
        assert_eq!(OauthFlowArg::Token.as_str(), "token");
    }

    #[test]
    fn instance_display_from_model_uses_expected_fields() {
        let model = InstanceModel {
            id: Uuid::new_v4(),
            name: "my-instance".to_string(),
            platform_type: PlatformType::GitLab,
            host: "gitlab.example.com".to_string(),
            oauth_client_id: Some("cid".to_string()),
            oauth_flow: "pkce".to_string(),
            created_at: Utc::now().fixed_offset(),
        };

        let display = InstanceDisplay::from(&model);

        assert_eq!(display.name, "my-instance");
        assert_eq!(display.platform_type, "gitlab");
        assert_eq!(display.host, "gitlab.example.com");
        assert!(display.api_url.contains("gitlab.example.com"));
        assert_eq!(display.created_at.len(), 10);
    }

    #[tokio::test]
    async fn add_instance_rejects_invalid_platform_type_with_clear_error() {
        let db = setup_db("invalid-platform").await;

        let err = add_instance(
            &db,
            "my-custom",
            Some("not-a-platform"),
            Some("git.example.com"),
            None,
            OauthFlowArg::Token,
        )
        .await
        .expect_err("invalid platform type should fail");

        assert!(
            err.to_string().contains("Invalid platform type:"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn add_instance_custom_requires_platform_and_host() {
        let db = setup_db("custom-required-fields").await;

        let missing_platform = add_instance(
            &db,
            "my-custom",
            None,
            Some("git.example.com"),
            None,
            OauthFlowArg::Auto,
        )
        .await
        .expect_err("custom instance without platform should fail");
        assert!(
            missing_platform
                .to_string()
                .contains("Platform type is required for custom instance 'my-custom'"),
            "unexpected error: {missing_platform}"
        );

        let missing_host = add_instance(
            &db,
            "my-custom",
            Some("gitlab"),
            None,
            None,
            OauthFlowArg::Auto,
        )
        .await
        .expect_err("custom instance without host should fail");
        assert!(
            missing_host
                .to_string()
                .contains("Host is required for custom instance 'my-custom'"),
            "unexpected error: {missing_host}"
        );
    }

    #[tokio::test]
    async fn add_instance_well_known_prefers_explicit_oauth_client_id() {
        let db = setup_db("well-known-client-id-precedence").await;

        Instance::delete_many()
            .filter(InstanceColumn::Name.eq("github"))
            .exec(&db)
            .await
            .expect("existing github seed should be removable for test");

        add_instance(
            &db,
            "github",
            None,
            None,
            Some("cli-client-id".to_string()),
            OauthFlowArg::Auto,
        )
        .await
        .expect("well-known instance should be created");

        let github = Instance::find()
            .filter(InstanceColumn::Name.eq("github"))
            .one(&db)
            .await
            .expect("query should succeed")
            .expect("instance should exist");

        assert_eq!(github.oauth_client_id.as_deref(), Some("cli-client-id"));
    }

    #[tokio::test]
    async fn update_instance_rejects_conflicts_and_empty_update() {
        let db = setup_db("update-errors").await;

        add_instance(
            &db,
            "my-gitlab",
            Some("gitlab"),
            Some("gitlab.example.com"),
            Some("cid".to_string()),
            OauthFlowArg::Auto,
        )
        .await
        .expect("seed instance should be created");

        let conflict_err =
            update_instance(&db, "my-gitlab", Some("new-cid".to_string()), true, None)
                .await
                .expect_err("conflicting update flags should fail");
        assert!(
            conflict_err
                .to_string()
                .contains("Use either --oauth-client-id or --clear-oauth-client-id"),
            "unexpected error: {conflict_err}"
        );

        let noop_err = update_instance(&db, "my-gitlab", None, false, None)
            .await
            .expect_err("empty update should fail");
        assert!(
            noop_err
                .to_string()
                .contains("No updates requested. Set --oauth-client-id"),
            "unexpected error: {noop_err}"
        );
    }

    #[tokio::test]
    async fn update_instance_reports_missing_instance_name() {
        let db = setup_db("update-missing").await;

        let err = update_instance(&db, "missing", None, false, Some(OauthFlowArg::Token))
            .await
            .expect_err("missing instance should fail");

        assert!(
            err.to_string().contains("Instance 'missing' not found"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn add_instance_rejects_duplicate_name_with_clear_error() {
        let db = setup_db("duplicate-instance").await;

        add_instance(
            &db,
            "my-gitlab",
            Some("gitlab"),
            Some("gitlab.example.com"),
            None,
            OauthFlowArg::Auto,
        )
        .await
        .expect("first insert should succeed");

        let err = add_instance(
            &db,
            "my-gitlab",
            Some("gitlab"),
            Some("gitlab.example.com"),
            None,
            OauthFlowArg::Auto,
        )
        .await
        .expect_err("duplicate insert should fail");

        assert_eq!(err.to_string(), "Instance 'my-gitlab' already exists");
    }

    #[tokio::test]
    async fn update_instance_can_clear_client_id_and_switch_flow() {
        let db = setup_db("update-clear-and-flow").await;

        add_instance(
            &db,
            "my-gitea",
            Some("gitea"),
            Some("gitea.example.com"),
            Some("initial-client-id".to_string()),
            OauthFlowArg::Auto,
        )
        .await
        .expect("seed instance should be created");

        update_instance(&db, "my-gitea", None, true, Some(OauthFlowArg::Token))
            .await
            .expect("update should succeed");

        let updated = Instance::find()
            .filter(InstanceColumn::Name.eq("my-gitea"))
            .one(&db)
            .await
            .expect("query should succeed")
            .expect("instance should exist");

        assert_eq!(updated.oauth_client_id, None);
        assert_eq!(updated.oauth_flow, "token");
    }

    #[tokio::test]
    async fn update_instance_only_changes_flow_when_requested() {
        let db = setup_db("update-flow-only").await;

        add_instance(
            &db,
            "my-gitlab",
            Some("gitlab"),
            Some("gitlab.example.com"),
            Some("client-id".to_string()),
            OauthFlowArg::Auto,
        )
        .await
        .expect("seed instance should be created");

        update_instance(&db, "my-gitlab", None, false, Some(OauthFlowArg::Pkce))
            .await
            .expect("flow-only update should succeed");

        let updated = Instance::find()
            .filter(InstanceColumn::Name.eq("my-gitlab"))
            .one(&db)
            .await
            .expect("query should succeed")
            .expect("instance should exist");

        assert_eq!(updated.oauth_client_id.as_deref(), Some("client-id"));
        assert_eq!(updated.oauth_flow, "pkce");
    }

    #[tokio::test]
    async fn handle_instance_dispatches_show_and_remove_errors_for_missing_instance() {
        let db = setup_db("handle-instance-dispatch").await;

        let show_err = handle_instance(
            InstanceAction::Show {
                name: "missing".to_string(),
                output: OutputFormat::Json,
            },
            &db,
            &Config::default(),
        )
        .await
        .expect_err("show should fail for missing instance");
        assert!(
            show_err
                .to_string()
                .contains("Instance 'missing' not found"),
            "unexpected error: {show_err}"
        );

        let remove_err = handle_instance(
            InstanceAction::Remove {
                name: "missing".to_string(),
                yes: true,
            },
            &db,
            &Config::default(),
        )
        .await
        .expect_err("remove should fail for missing instance");
        assert!(
            remove_err
                .to_string()
                .contains("Instance 'missing' not found"),
            "unexpected error: {remove_err}"
        );
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn format_instance_credential_status_covers_all_sources() {
        assert_eq!(
            format_instance_credential_status(Some(&CredentialSource::Keychain)),
            "configured (keychain)"
        );
        assert_eq!(
            format_instance_credential_status(Some(&CredentialSource::File(
                std::path::PathBuf::from("/tmp/auth.toml")
            ))),
            "configured (file)"
        );
        assert_eq!(
            format_instance_credential_status(Some(&CredentialSource::Db)),
            "configured (db)"
        );
        assert_eq!(format_instance_credential_status(None), "missing");
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn remove_instance_only_deletes_instance_scoped_credentials() {
        assert!(!should_delete_credentials_on_instance_remove(
            &CredentialSource::Keychain
        ));
        assert!(should_delete_credentials_on_instance_remove(
            &CredentialSource::File(std::path::PathBuf::from("/tmp/auth.toml"))
        ));
        assert!(should_delete_credentials_on_instance_remove(
            &CredentialSource::Db
        ));
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn build_instance_helpers_include_auth_fields() {
        let instance = InstanceModel {
            id: Uuid::new_v4(),
            name: "gitlab-work".to_string(),
            platform_type: PlatformType::GitLab,
            host: "gitlab.work.test".to_string(),
            oauth_client_id: Some("cid".to_string()),
            oauth_flow: "pkce".to_string(),
            created_at: Utc::now().fixed_offset(),
        };
        let auth_status = crate::credentials::CredentialStatus {
            configured_store: crate::config::CredentialStore::Db,
            active_source: Some(CredentialSource::Db),
            auth_kind: Some("oauth".to_string()),
            token_expires_at: Some(1234),
        };

        let details = build_instance_details_with_auth(&instance, 7, &auth_status);
        assert!(
            details
                .iter()
                .any(|item| item.property == "Credential Store" && item.value == "db")
        );
        assert!(details.iter().any(|item| item.property == "Credential Status" && item.value == "configured (db)"));

        let json = build_instance_json_with_auth(instance, 7, auth_status);
        assert_eq!(json.repository_count, 7);
        assert_eq!(json.credential_store.as_deref(), Some("db"));
        assert_eq!(json.credential_status.as_deref(), Some("configured (db)"));
        assert_eq!(json.auth_kind.as_deref(), Some("oauth"));
        assert_eq!(json.token_expires_at, Some(1234));
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[test]
    fn build_instance_helpers_cover_missing_auth_state() {
        let instance = InstanceModel {
            id: Uuid::new_v4(),
            name: "github".to_string(),
            platform_type: PlatformType::GitHub,
            host: "github.com".to_string(),
            oauth_client_id: None,
            oauth_flow: "auto".to_string(),
            created_at: Utc::now().fixed_offset(),
        };
        let auth_status = crate::credentials::CredentialStatus {
            configured_store: crate::config::CredentialStore::Auto,
            active_source: None,
            auth_kind: None,
            token_expires_at: None,
        };

        let details = build_instance_details_with_auth(&instance, 0, &auth_status);
        assert!(
            details
                .iter()
                .any(|item| item.property == "Credential Status" && item.value == "missing")
        );
        assert!(
            details
                .iter()
                .any(|item| item.property == "Auth Kind" && item.value == "(unknown)")
        );
        assert!(
            details
                .iter()
                .any(|item| item.property == "Token Expires At" && item.value == "(not set)")
        );

        let json = build_instance_json_with_auth(instance, 0, auth_status);
        assert_eq!(json.credential_store.as_deref(), Some("auto"));
        assert_eq!(json.credential_status.as_deref(), Some("missing"));
        assert_eq!(json.auth_kind, None);
        assert_eq!(json.token_expires_at, None);
    }

    #[test]
    fn instance_display_from_model_formats_gitea_instance() {
        let model = InstanceModel {
            id: Uuid::new_v4(),
            name: "forgejo".to_string(),
            platform_type: PlatformType::Gitea,
            host: "forgejo.example.com".to_string(),
            oauth_client_id: None,
            oauth_flow: "token".to_string(),
            created_at: Utc::now().fixed_offset(),
        };

        let display = InstanceDisplay::from(&model);
        assert_eq!(display.platform_type, "gitea");
        assert!(display.api_url.ends_with("/api/v1"));
        assert_eq!(display.host, "forgejo.example.com");
    }

    #[tokio::test]
    async fn add_instance_creates_custom_instance_with_expected_fields() {
        let db = setup_db("add-custom-success").await;

        add_instance(
            &db,
            "work-gitlab",
            Some("gitlab"),
            Some("GitLab.Work.Test"),
            Some("cid".to_string()),
            OauthFlowArg::Pkce,
        )
        .await
        .unwrap();

        let stored = Instance::find()
            .filter(InstanceColumn::Name.eq("work-gitlab"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.platform_type, PlatformType::GitLab);
        assert_eq!(stored.host, "gitlab.work.test");
        assert_eq!(stored.oauth_client_id.as_deref(), Some("cid"));
        assert_eq!(stored.oauth_flow, "pkce");
    }

    #[tokio::test]
    async fn remove_instance_deletes_instance_and_associated_repositories() {
        let db = setup_db("remove-instance-success").await;
        add_instance(
            &db,
            "work-gitlab",
            Some("gitlab"),
            Some("gitlab.work.test"),
            None,
            OauthFlowArg::Auto,
        )
        .await
        .unwrap();
        let instance = Instance::find()
            .filter(InstanceColumn::Name.eq("work-gitlab"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();

        curator::entity::code_repository::ActiveModel {
            id: Set(Uuid::new_v4()),
            instance_id: Set(instance.id),
            platform_id: Set(123),
            owner: Set("owner".to_string()),
            name: Set("repo".to_string()),
            description: Set(None),
            default_branch: Set("main".to_string()),
            topics: Set(serde_json::json!([])),
            primary_language: Set(None),
            license_spdx: Set(None),
            homepage: Set(None),
            visibility: Set(curator::CodeVisibility::Public),
            is_fork: Set(false),
            is_mirror: Set(false),
            is_archived: Set(false),
            is_template: Set(false),
            is_empty: Set(false),
            stars: Set(None),
            forks: Set(None),
            open_issues: Set(None),
            watchers: Set(None),
            size_kb: Set(None),
            has_issues: Set(true),
            has_wiki: Set(true),
            has_pull_requests: Set(true),
            created_at: Set(Some(Utc::now().fixed_offset())),
            updated_at: Set(Some(Utc::now().fixed_offset())),
            pushed_at: Set(None),
            platform_metadata: Set(serde_json::json!({})),
            synced_at: Set(Utc::now().fixed_offset()),
            etag: Set(None),
        }
        .insert(&db)
        .await
        .unwrap();

        remove_instance(&db, "work-gitlab", true, &isolated_auth_config())
            .await
            .unwrap();

        assert!(
            Instance::find()
                .filter(InstanceColumn::Name.eq("work-gitlab"))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            CodeRepository::find()
                .filter(CodeRepositoryColumn::InstanceId.eq(instance.id))
                .count(&db)
                .await
                .unwrap(),
            0
        );
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[tokio::test]
    async fn remove_instance_deletes_file_backed_credentials() {
        let db = setup_db("remove-instance-credentials").await;
        add_instance(
            &db,
            "work-gitlab",
            Some("gitlab"),
            Some("gitlab.work.test"),
            None,
            OauthFlowArg::Auto,
        )
        .await
        .unwrap();

        let instance = Instance::find()
            .filter(InstanceColumn::Name.eq("work-gitlab"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();

        let auth_path =
            std::env::temp_dir().join(format!("curator-instance-auth-{}.toml", Uuid::new_v4()));
        let config = Config {
            auth: crate::config::AuthConfig {
                credential_store: crate::config::CredentialStore::File,
                file_path: Some(auth_path.display().to_string()),
            },
            ..Config::default()
        };

        save_credential(
            &instance,
            &StoredCredential {
                access_token: "secret".to_string(),
                refresh_token: None,
                token_expires_at: None,
                auth_kind: "pat".to_string(),
                token_type: None,
            },
            &config,
            &db,
        )
        .await
        .unwrap();

        remove_instance(&db, "work-gitlab", true, &config)
            .await
            .unwrap();

        assert!(
            load_credential(&instance, &config, Some(&db))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
    #[tokio::test]
    async fn list_instances_and_show_instance_succeed_for_json_output() {
        let db = setup_db("list-show-success").await;
        add_instance(
            &db,
            "work-gitlab",
            Some("gitlab"),
            Some("gitlab.work.test"),
            None,
            OauthFlowArg::Auto,
        )
        .await
        .unwrap();

        list_instances(&db, OutputFormat::Json).await.unwrap();
        show_instance(
            &db,
            "work-gitlab",
            OutputFormat::Json,
            &isolated_auth_config(),
        )
        .await
        .unwrap();
    }
}
