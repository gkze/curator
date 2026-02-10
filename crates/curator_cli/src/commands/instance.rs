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
use uuid::Uuid;

use curator::{
    CodeRepository, CodeRepositoryColumn, Instance, InstanceColumn, InstanceModel, PlatformType,
    entity::instance::well_known,
};

use super::limits::OutputFormat;

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
            remove_instance(db, &name, yes).await?;
        }
        InstanceAction::Show { name, output } => {
            show_instance(db, &name, output).await?;
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
            })?
            .to_string();

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

/// Show details of an instance.
async fn show_instance(
    db: &DatabaseConnection,
    name: &str,
    output: OutputFormat,
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

    // Build detailed display
    #[derive(Debug, serde::Serialize, Tabled)]
    struct InstanceDetail {
        #[tabled(rename = "Property")]
        property: String,
        #[tabled(rename = "Value")]
        value: String,
    }

    let details = vec![
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
    ];

    match output {
        OutputFormat::Table => {
            let mut table = Table::new(details);
            table.with(Style::rounded());
            println!("{}", table);
        }
        OutputFormat::Json => {
            // For JSON, output the full model plus repo count
            #[derive(serde::Serialize)]
            struct InstanceJson {
                #[serde(flatten)]
                instance: InstanceModel,
                base_url: String,
                api_url: String,
                repository_count: u64,
            }

            let json = InstanceJson {
                base_url: instance.base_url(),
                api_url: instance.api_url(),
                repository_count: repo_count,
                instance,
            };
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
    }

    Ok(())
}
