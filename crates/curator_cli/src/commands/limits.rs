use clap::ValueEnum;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use uuid::Uuid;

use curator::{Instance, InstanceColumn, PlatformType};

use crate::commands::shared::get_token_for_instance;
use crate::config::Config;

/// Output format for rate limit display.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub(crate) enum OutputFormat {
    /// Display as a formatted table (default)
    #[default]
    Table,
    /// Display as JSON
    Json,
}

/// Handle the unified limits command.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub(crate) async fn handle_limits(
    instance_name: &str,
    output: OutputFormat,
    config: &Config,
    db: &DatabaseConnection,
) -> Result<(), Box<dyn std::error::Error>> {
    // Look up the instance
    let instance = Instance::find()
        .filter(InstanceColumn::Name.eq(instance_name))
        .one(db)
        .await?
        .ok_or_else(|| {
            format!(
                "Instance '{}' not found. Add it first with: curator instance add {}",
                instance_name, instance_name
            )
        })?;

    // Get token for instance (with automatic refresh for OAuth tokens)
    let token = get_token_for_instance(&instance, config).await?;

    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => {
            use curator::github::GitHubClient;

            let client = GitHubClient::new(&token, Uuid::nil())?;
            let rate_limits = curator::github::get_rate_limits(client.inner()).await?;
            let items = github_rate_limits_to_display(&rate_limits.resources);
            RateLimitDisplay::print_many(items, output);
        }
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => {
            let info = RateLimitInfoMessage {
                platform: format!("GitLab ({})", instance.host),
                message: "Header-based rate limiting".to_string(),
                default_limit: "Varies by endpoint (typically 100/min authenticated)".to_string(),
                note: "Check RateLimit-* headers in API responses".to_string(),
                docs_url: "https://docs.gitlab.com/ee/administration/settings/rate_limits.html"
                    .to_string(),
            };
            info.print(output);
        }
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => {
            let platform_name = if instance.is_codeberg() {
                "Codeberg"
            } else {
                "Gitea"
            };
            let info = RateLimitInfoMessage {
                platform: format!("{} ({})", platform_name, instance.host),
                message: "Header-based rate limiting".to_string(),
                default_limit: "Varies by endpoint".to_string(),
                note: "Check X-RateLimit-* headers in API responses".to_string(),
                docs_url: "https://docs.gitea.com/usage/api-usage".to_string(),
            };
            info.print(output);
        }
        #[allow(unreachable_patterns)]
        _ => {
            return Err(format!(
                "Platform type '{}' not supported for limits display.",
                instance.platform_type
            )
            .into());
        }
    }

    Ok(())
}

/// Rate limit information for display.
#[cfg(feature = "github")]
#[derive(Debug, Clone, serde::Serialize, tabled::Tabled)]
pub(crate) struct RateLimitDisplay {
    #[tabled(rename = "Resource")]
    #[serde(rename = "resource")]
    pub resource: String,
    #[tabled(rename = "Limit")]
    pub limit: String,
    #[tabled(rename = "Used")]
    pub used: String,
    #[tabled(rename = "Remaining")]
    pub remaining: String,
    #[tabled(rename = "Usage %")]
    pub usage_percent: String,
    #[tabled(rename = "Resets At")]
    pub reset_at: String,
    #[tabled(rename = "Resets In")]
    pub reset_in: String,
}

#[cfg(feature = "github")]
impl RateLimitDisplay {
    pub(crate) fn from_github_resource(
        name: &str,
        resource: &curator::github::RateLimitResource,
    ) -> Self {
        let usage_percent = if resource.limit > 0 {
            (resource.used as f64 / resource.limit as f64) * 100.0
        } else {
            0.0
        };
        let now = chrono::Utc::now();
        let reset_at = resource.reset_at();
        let reset_duration = reset_at.signed_duration_since(now);
        let reset_in = if reset_duration.num_seconds() > 0 {
            format_duration(reset_duration)
        } else {
            "now".to_string()
        };

        Self {
            resource: name.to_string(),
            limit: resource.limit.to_string(),
            used: resource.used.to_string(),
            remaining: resource.remaining.to_string(),
            usage_percent: format!("{:.1}%", usage_percent),
            reset_at: reset_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
            reset_in,
        }
    }

    pub(crate) fn print_many(mut items: Vec<Self>, format: OutputFormat) {
        // Sort by resource name for consistent output
        items.sort_by(|a, b| a.resource.cmp(&b.resource));

        match format {
            OutputFormat::Table => {
                let mut table = tabled::Table::new(items);
                table.with(tabled::settings::Style::rounded());
                println!("{}", table);
            }
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&items).unwrap());
            }
        }
    }
}

/// Build a vector of all GitHub rate limit resources for display.
#[cfg(feature = "github")]
pub(crate) fn github_rate_limits_to_display(
    limits: &curator::github::GitHubRateLimits,
) -> Vec<RateLimitDisplay> {
    let mut items = Vec::new();

    // Required resources
    items.push(RateLimitDisplay::from_github_resource("core", &limits.core));
    items.push(RateLimitDisplay::from_github_resource(
        "search",
        &limits.search,
    ));

    // Optional resources - add if present
    if let Some(ref r) = limits.graphql {
        items.push(RateLimitDisplay::from_github_resource("graphql", r));
    }
    if let Some(ref r) = limits.code_search {
        items.push(RateLimitDisplay::from_github_resource("code_search", r));
    }
    if let Some(ref r) = limits.integration_manifest {
        items.push(RateLimitDisplay::from_github_resource(
            "integration_manifest",
            r,
        ));
    }
    if let Some(ref r) = limits.source_import {
        items.push(RateLimitDisplay::from_github_resource("source_import", r));
    }
    if let Some(ref r) = limits.code_scanning_upload {
        items.push(RateLimitDisplay::from_github_resource(
            "code_scanning_upload",
            r,
        ));
    }
    if let Some(ref r) = limits.actions_runner_registration {
        items.push(RateLimitDisplay::from_github_resource(
            "actions_runner_registration",
            r,
        ));
    }
    if let Some(ref r) = limits.scim {
        items.push(RateLimitDisplay::from_github_resource("scim", r));
    }
    if let Some(ref r) = limits.dependency_snapshots {
        items.push(RateLimitDisplay::from_github_resource(
            "dependency_snapshots",
            r,
        ));
    }
    if let Some(ref r) = limits.audit_log {
        items.push(RateLimitDisplay::from_github_resource("audit_log", r));
    }
    if let Some(ref r) = limits.code_scanning_autofix {
        items.push(RateLimitDisplay::from_github_resource(
            "code_scanning_autofix",
            r,
        ));
    }

    items
}

/// Rate limit informational message for platforms without dedicated endpoints.
#[cfg(any(feature = "gitlab", feature = "gitea"))]
#[derive(Debug, Clone, serde::Serialize, tabled::Tabled)]
pub(crate) struct RateLimitInfoMessage {
    #[tabled(rename = "Platform")]
    pub platform: String,
    #[tabled(rename = "Rate Limiting")]
    pub message: String,
    #[tabled(rename = "Default Limit")]
    pub default_limit: String,
    #[tabled(rename = "Note")]
    pub note: String,
    #[tabled(rename = "Documentation")]
    pub docs_url: String,
}

#[cfg(any(feature = "gitlab", feature = "gitea"))]
impl RateLimitInfoMessage {
    pub(crate) fn print(self, format: OutputFormat) {
        match format {
            OutputFormat::Table => {
                let mut table = tabled::Table::new(vec![self]);
                table.with(tabled::settings::Style::rounded());
                println!("{}", table);
            }
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&self).unwrap());
            }
        }
    }
}

/// Format a duration in a human-readable way.
#[cfg(feature = "github")]
fn format_duration(duration: chrono::Duration) -> String {
    let total_secs = duration.num_seconds();
    if total_secs < 60 {
        format!("{}s", total_secs)
    } else if total_secs < 3600 {
        let mins = total_secs / 60;
        let secs = total_secs % 60;
        if secs > 0 {
            format!("{}m {}s", mins, secs)
        } else {
            format!("{}m", mins)
        }
    } else {
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        if mins > 0 {
            format!("{}h {}m", hours, mins)
        } else {
            format!("{}h", hours)
        }
    }
}
