//! Unified login command for all platform instances.
//!
//! Authenticates with a platform instance using the appropriate OAuth flow.

use std::time::Duration;

use chrono::Utc;
use console::{Term, style};
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use uuid::Uuid;

use curator::{
    Instance, InstanceColumn, InstanceModel, PlatformType, entity::instance::well_known,
};

use crate::config::Config;

/// Handle the login command for a given instance.
pub async fn handle_login(
    instance_name: &str,
    db: &DatabaseConnection,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let is_tty = Term::stdout().is_term();

    // Get or create the instance
    let instance = get_or_create_instance(db, instance_name).await?;

    if is_tty {
        println!(
            "Authenticating with {} ({} @ {})...\n",
            style(&instance.name).cyan(),
            instance.platform_type,
            instance.host
        );
    }

    // Dispatch to the appropriate OAuth flow based on platform type
    match instance.platform_type {
        #[cfg(feature = "github")]
        PlatformType::GitHub => login_github(&instance, config, is_tty).await?,
        #[cfg(feature = "gitlab")]
        PlatformType::GitLab => login_gitlab(&instance, config, is_tty).await?,
        #[cfg(feature = "gitea")]
        PlatformType::Gitea => login_gitea(&instance, config, is_tty).await?,
        #[allow(unreachable_patterns)]
        _ => {
            return Err(format!(
                "Login not supported for platform type '{}'. Enable the appropriate feature.",
                instance.platform_type
            )
            .into());
        }
    }

    Ok(())
}

/// Get an existing instance or create a well-known one automatically.
async fn get_or_create_instance(
    db: &DatabaseConnection,
    name: &str,
) -> Result<InstanceModel, Box<dyn std::error::Error>> {
    // Try to find existing instance
    if let Some(instance) = Instance::find()
        .filter(InstanceColumn::Name.eq(name))
        .one(db)
        .await?
    {
        return Ok(instance);
    }

    // Check if this is a well-known instance name we can auto-create
    let wk = well_known::by_name(name).ok_or_else(|| {
        format!(
            "Instance '{}' not found. Add it first with: curator instance add {} -t <platform> -H <host>",
            name, name
        )
    })?;
    let (pt, host) = (wk.platform_type, wk.host.to_string());

    // Auto-create the well-known instance
    let instance = curator::entity::instance::ActiveModel {
        id: Set(Uuid::new_v4()),
        name: Set(name.to_string()),
        platform_type: Set(pt),
        host: Set(host),
        created_at: Set(Utc::now().fixed_offset()),
    };

    let result = instance.insert(db).await?;

    println!(
        "{} Auto-created instance '{}' ({} @ {})",
        style("✓").green().bold(),
        style(&result.name).cyan(),
        result.platform_type,
        result.host
    );

    Ok(result)
}

/// GitHub OAuth Device Flow login.
#[cfg(feature = "github")]
async fn login_github(
    instance: &InstanceModel,
    _config: &Config,
    is_tty: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use curator::github::oauth::{poll_for_token, request_device_code_default};

    // Only github.com is supported for now (GitHub Enterprise would need custom client ID)
    if !instance.is_github_com() {
        return Err(format!(
            "GitHub Enterprise login not yet supported for host '{}'. \
             Configure CURATOR_GITHUB_TOKEN manually.",
            instance.host
        )
        .into());
    }

    // Request device code
    let device_code = request_device_code_default().await.map_err(|e| {
        Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error>
    })?;

    // Try to copy code to clipboard
    let clipboard_success = copy_to_clipboard(&device_code.user_code);

    // Display instructions
    if is_tty {
        println!("Please visit: {}", device_code.verification_uri);
        println!();
        if clipboard_success {
            println!("Your code: {} (copied to clipboard)", device_code.user_code);
        } else {
            println!("Your code: {}", device_code.user_code);
        }
        println!();
        println!(
            "Waiting for authorization (expires in {} seconds)...",
            device_code.expires_in
        );
    } else {
        tracing::info!(
            verification_uri = %device_code.verification_uri,
            user_code = %device_code.user_code,
            "Please authorize the application"
        );
    }

    // Try to open browser
    let _ = open::that(&device_code.verification_uri);

    // Poll for token
    let token_response = poll_for_token(&device_code).await.map_err(|e| {
        Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error>
    })?;

    // Save token to config
    let config_path = Config::save_github_token(&token_response.access_token)?;

    if is_tty {
        println!();
        println!(
            "{} GitHub token saved to: {}",
            style("✓").green().bold(),
            config_path.display()
        );
        println!();
        println!("You can now use curator commands like:");
        println!("  curator sync stars github");
        println!("  curator sync org github <org-name>");
    } else {
        tracing::info!(
            config_path = %config_path.display(),
            "GitHub authentication successful"
        );
    }

    Ok(())
}

/// GitLab OAuth Device Flow login.
#[cfg(feature = "gitlab")]
async fn login_gitlab(
    instance: &InstanceModel,
    _config: &Config,
    is_tty: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use curator::gitlab::oauth;

    // Resolve the OAuth Client ID for this instance
    let client_id = well_known::by_name(&instance.name)
        .and_then(|wk| wk.oauth_client_id)
        .ok_or_else(|| {
            format!(
                "No OAuth Client ID registered for GitLab instance '{}' ({}).\n\
                 Register an OAuth application on https://{} and add its Client ID to \
                 the well-known instances list.",
                instance.name, instance.host, instance.host,
            )
        })?;

    // Request device code
    let device_code = oauth::request_device_code_default(&instance.host, client_id)
        .await
        .map_err(|e| {
            Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error>
        })?;

    // Try to copy code to clipboard
    let clipboard_success = copy_to_clipboard(&device_code.user_code);

    // Display instructions
    if is_tty {
        println!("Please visit: {}", device_code.verification_uri);
        println!();
        if clipboard_success {
            println!("Your code: {} (copied to clipboard)", device_code.user_code);
        } else {
            println!("Your code: {}", device_code.user_code);
        }
        println!();
        println!(
            "Waiting for authorization (expires in {} seconds)...",
            device_code.expires_in
        );
    } else {
        tracing::info!(
            verification_uri = %device_code.verification_uri,
            user_code = %device_code.user_code,
            "Please authorize the application"
        );
    }

    // Try to open browser — prefer the complete URI with code pre-filled
    let open_url = device_code
        .verification_uri_complete
        .as_deref()
        .unwrap_or(&device_code.verification_uri);
    let _ = open::that(open_url);

    // Poll for token
    let token_response = oauth::poll_for_token(&instance.host, client_id, &device_code)
        .await
        .map_err(|e| {
            Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error>
        })?;

    // Save tokens to config
    let expires_at = oauth::token_expires_at(&token_response);
    let config_path = Config::save_gitlab_oauth_tokens(
        &token_response.access_token,
        token_response.refresh_token.as_deref(),
        expires_at,
    )?;

    if is_tty {
        println!();
        println!(
            "{} GitLab token saved to: {}",
            style("✓").green().bold(),
            config_path.display()
        );
        if token_response.refresh_token.is_some() {
            println!("Token will be automatically refreshed when it expires.");
        }
        println!();
        println!("You can now use curator commands like:");
        println!("  curator sync stars {}", instance.name);
        println!("  curator sync group {} <group-path>", instance.name);
    } else {
        tracing::info!(
            config_path = %config_path.display(),
            "GitLab authentication successful"
        );
    }

    Ok(())
}

/// Gitea/Codeberg PKCE OAuth login.
#[cfg(feature = "gitea")]
async fn login_gitea(
    instance: &InstanceModel,
    _config: &Config,
    is_tty: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use curator::gitea::oauth;

    // Codeberg has a registered OAuth app, other instances need manual setup
    if !instance.is_codeberg() {
        return Err(format!(
            "OAuth login not yet supported for Gitea instance '{}'. \
             Configure CURATOR_GITEA_TOKEN manually.",
            instance.host
        )
        .into());
    }

    // Use the PKCE OAuth flow
    let auth = oauth::CodebergAuth::new();
    let timeout = Duration::from_secs(300); // 5 minute timeout

    let token_response = oauth::authorize(&auth, timeout).await.map_err(|e| {
        Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error>
    })?;

    // Calculate expiry timestamp
    let expires_at = oauth::token_expires_at(&token_response);

    // Save tokens to config
    let config_path = Config::save_codeberg_oauth_tokens(
        &token_response.access_token,
        token_response.refresh_token.as_deref(),
        expires_at,
    )?;

    if is_tty {
        println!();
        println!(
            "{} Codeberg token saved to: {}",
            style("✓").green().bold(),
            config_path.display()
        );
        if token_response.refresh_token.is_some() {
            println!("Token will be automatically refreshed when it expires.");
        }
        println!();
        println!("You can now use curator commands like:");
        println!("  curator sync stars codeberg");
        println!("  curator sync org codeberg <org-name>");
    } else {
        tracing::info!(
            config_path = %config_path.display(),
            "Codeberg authentication successful"
        );
    }

    Ok(())
}

/// Try to copy text to clipboard, returning true if successful.
fn copy_to_clipboard(text: &str) -> bool {
    match arboard::Clipboard::new() {
        Ok(mut clipboard) => clipboard.set_text(text).is_ok(),
        Err(_) => false,
    }
}
