//! Instance management commands.
//!
//! Commands for managing platform instances (add, list, remove, show).

use chrono::Utc;
use clap::Subcommand;
use console::style;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    Set,
};
use tabled::{Table, Tabled, settings::Style};
use uuid::Uuid;

use curator::{
    CodeRepository, CodeRepositoryColumn, Instance, InstanceColumn, InstanceModel, PlatformType,
    entity::instance::well_known,
};

use super::limits::OutputFormat;

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
        } => {
            add_instance(db, &name, platform_type.as_deref(), host.as_deref()).await?;
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

/// Add a new instance to the database.
async fn add_instance(
    db: &DatabaseConnection,
    name: &str,
    platform_type: Option<&str>,
    host: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if instance already exists
    let existing = Instance::find()
        .filter(InstanceColumn::Name.eq(name))
        .one(db)
        .await?;

    if existing.is_some() {
        return Err(format!("Instance '{}' already exists", name).into());
    }

    // Determine the instance configuration
    let (pt, h) = match name.to_lowercase().as_str() {
        "github" => {
            let wk = well_known::github();
            (wk.platform_type, wk.host)
        }
        "gitlab" => {
            let wk = well_known::gitlab();
            (wk.platform_type, wk.host)
        }
        "codeberg" => {
            let wk = well_known::codeberg();
            (wk.platform_type, wk.host)
        }
        _ => {
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

            (pt, h)
        }
    };

    // Create the instance
    let instance = curator::entity::instance::ActiveModel {
        id: Set(Uuid::new_v4()),
        name: Set(name.to_string()),
        platform_type: Set(pt),
        host: Set(h.clone()),
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
