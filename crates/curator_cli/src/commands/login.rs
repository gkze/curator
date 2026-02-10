//! Unified login command for all platform instances.
//!
//! Uses OAuth where available, with PAT fallback for self-hosted Gitea/Forgejo.

#[cfg(feature = "gitea")]
use std::time::Duration;

use chrono::Utc;
use console::{Term, style};
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use uuid::Uuid;

use curator::{
    Instance, InstanceColumn, InstanceModel, PlatformType, entity::instance::well_known,
};

use crate::config::Config;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoginMethod {
    Device,
    Pkce,
    Token,
}

fn login_method_order(instance: &InstanceModel) -> Vec<LoginMethod> {
    match instance.oauth_flow.trim().to_ascii_lowercase().as_str() {
        "token" => vec![LoginMethod::Token],
        "pkce" => vec![LoginMethod::Pkce, LoginMethod::Token],
        "device" | "auto" => vec![LoginMethod::Device, LoginMethod::Pkce, LoginMethod::Token],
        _ => vec![LoginMethod::Device, LoginMethod::Pkce, LoginMethod::Token],
    }
}

fn resolve_client_id(instance: &InstanceModel) -> Option<&str> {
    instance
        .oauth_client_id
        .as_deref()
        .or_else(|| well_known::by_name(&instance.name).and_then(|wk| wk.oauth_client_id))
}

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
        oauth_client_id: Set(wk.oauth_client_id.map(ToString::to_string)),
        oauth_flow: Set("auto".to_string()),
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

/// GitHub login with flow fallback.
#[cfg(feature = "github")]
async fn login_github(
    instance: &InstanceModel,
    _config: &Config,
    is_tty: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use curator::github::oauth::{poll_for_token, request_device_code_default};

    let mut attempted_oauth = false;
    for method in login_method_order(instance) {
        match method {
            LoginMethod::Device => {
                if !instance.is_github_com() {
                    continue;
                }
                attempted_oauth = true;

                let device_code = match request_device_code_default().await {
                    Ok(dc) => dc,
                    Err(e) => {
                        if is_tty {
                            eprintln!("OAuth device flow unavailable, falling back: {}", e);
                        }
                        continue;
                    }
                };

                let clipboard_success = copy_to_clipboard(&device_code.user_code);

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

                let _ = open::that(&device_code.verification_uri);

                let token_response = match poll_for_token(&device_code).await {
                    Ok(token) => token,
                    Err(e) => {
                        if is_tty {
                            eprintln!("OAuth authorization failed, falling back: {}", e);
                        }
                        continue;
                    }
                };

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

                return Ok(());
            }
            LoginMethod::Pkce => continue,
            LoginMethod::Token => {
                let token = read_token_with_prompt(
                    "CURATOR_GITHUB_TOKEN",
                    "GitHub",
                    Some("https://github.com/settings/tokens"),
                    "GitHub token",
                    is_tty,
                )?;

                let config_path = Config::save_github_token(&token)?;
                if is_tty {
                    println!();
                    println!(
                        "{} GitHub token saved to: {}",
                        style("✓").green().bold(),
                        config_path.display()
                    );
                }
                return Ok(());
            }
        }
    }

    if attempted_oauth {
        Err("Unable to authenticate with GitHub using the configured methods.".into())
    } else {
        Err("No supported login method available for this GitHub instance.".into())
    }
}

/// GitLab login with flow fallback.
#[cfg(feature = "gitlab")]
async fn login_gitlab(
    instance: &InstanceModel,
    _config: &Config,
    is_tty: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use curator::gitlab::oauth;

    for method in login_method_order(instance) {
        match method {
            LoginMethod::Device => {
                let Some(client_id) = resolve_client_id(instance) else {
                    continue;
                };

                let device_code =
                    match oauth::request_device_code_default(&instance.host, client_id).await {
                        Ok(dc) => dc,
                        Err(e) => {
                            if is_tty {
                                eprintln!("GitLab device flow unavailable, falling back: {}", e);
                            }
                            continue;
                        }
                    };

                let clipboard_success = copy_to_clipboard(&device_code.user_code);

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

                let open_url = device_code
                    .verification_uri_complete
                    .as_deref()
                    .unwrap_or(&device_code.verification_uri);
                let _ = open::that(open_url);

                let token_response =
                    match oauth::poll_for_token(&instance.host, client_id, &device_code).await {
                        Ok(token) => token,
                        Err(e) => {
                            if is_tty {
                                eprintln!("GitLab OAuth authorization failed, falling back: {}", e);
                            }
                            continue;
                        }
                    };

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

                return Ok(());
            }
            LoginMethod::Pkce => continue,
            LoginMethod::Token => {
                let token = read_token_with_prompt(
                    "CURATOR_GITLAB_TOKEN",
                    "GitLab",
                    Some(&format!(
                        "https://{}/-/user_settings/personal_access_tokens",
                        instance.host
                    )),
                    "GitLab token",
                    is_tty,
                )?;

                let config_path = Config::save_gitlab_oauth_tokens(&token, None, None)?;
                if is_tty {
                    println!();
                    println!(
                        "{} GitLab token saved to: {}",
                        style("✓").green().bold(),
                        config_path.display()
                    );
                }
                return Ok(());
            }
        }
    }

    Err(format!(
        "No usable login method found for GitLab instance '{}' ({}).\n\
         Add an OAuth client id with:\n\
         curator instance update {} -c <client-id>\n\
         or set CURATOR_GITLAB_TOKEN.",
        instance.name, instance.host, instance.name,
    )
    .into())
}

/// Gitea/Codeberg login with flow fallback.
#[cfg(feature = "gitea")]
async fn login_gitea(
    instance: &InstanceModel,
    _config: &Config,
    is_tty: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use curator::gitea::oauth;

    for method in login_method_order(instance) {
        match method {
            LoginMethod::Device => continue,
            LoginMethod::Pkce => {
                let Some(client_id) = resolve_client_id(instance) else {
                    continue;
                };

                let timeout = Duration::from_secs(300);
                let token_response = if instance.is_codeberg() {
                    let auth = oauth::CodebergAuth::new();
                    oauth::authorize(&auth, timeout).await
                } else {
                    let auth = oauth::GiteaAuth::new(&instance.host, client_id.to_string());
                    oauth::authorize(&auth, timeout).await
                }
                .map_err(|e| std::io::Error::other(e.to_string()));

                let token_response = match token_response {
                    Ok(token) => token,
                    Err(e) => {
                        if is_tty {
                            eprintln!("Gitea PKCE flow failed, falling back: {}", e);
                        }
                        continue;
                    }
                };

                let expires_at = oauth::token_expires_at(&token_response);

                let config_path = if instance.is_codeberg() {
                    Config::save_codeberg_oauth_tokens(
                        &token_response.access_token,
                        token_response.refresh_token.as_deref(),
                        expires_at,
                    )?
                } else {
                    Config::save_gitea_token(&instance.host, &token_response.access_token)?
                };

                if is_tty {
                    println!();
                    println!(
                        "{} {} token saved to: {}",
                        style("✓").green().bold(),
                        if instance.is_codeberg() {
                            "Codeberg"
                        } else {
                            "Gitea/Forgejo"
                        },
                        config_path.display()
                    );
                    if instance.is_codeberg() && token_response.refresh_token.is_some() {
                        println!("Token will be automatically refreshed when it expires.");
                    }
                    println!();
                    println!("You can now use curator commands like:");
                    println!("  curator sync stars {}", instance.name);
                    println!("  curator sync org {} <org-name>", instance.name);
                } else {
                    tracing::info!(
                        config_path = %config_path.display(),
                        host = %instance.host,
                        "Gitea authentication successful"
                    );
                }

                return Ok(());
            }
            LoginMethod::Token => {
                let (env_var, provider, settings_url, prompt) = if instance.is_codeberg() {
                    (
                        "CURATOR_CODEBERG_TOKEN",
                        "Codeberg",
                        Some("https://codeberg.org/user/settings/applications".to_string()),
                        "Codeberg token",
                    )
                } else {
                    (
                        "CURATOR_GITEA_TOKEN",
                        "Gitea/Forgejo",
                        Some(format!(
                            "https://{}/user/settings/applications",
                            instance.host
                        )),
                        "Gitea/Forgejo token",
                    )
                };

                let token = read_token_with_prompt(
                    env_var,
                    provider,
                    settings_url.as_deref(),
                    prompt,
                    is_tty,
                )?;

                let config_path = if instance.is_codeberg() {
                    Config::save_codeberg_oauth_tokens(&token, None, None)?
                } else {
                    Config::save_gitea_token(&instance.host, &token)?
                };

                if is_tty {
                    println!();
                    println!(
                        "{} {} token saved to: {}",
                        style("✓").green().bold(),
                        provider,
                        config_path.display()
                    );
                }
                return Ok(());
            }
        }
    }

    Err("No usable login method found for this Gitea/Forgejo instance.".into())
}

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
fn read_token_with_prompt(
    env_var: &str,
    provider_name: &str,
    token_url: Option<&str>,
    prompt_label: &str,
    is_tty: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    let token = if let Ok(token) = std::env::var(env_var) {
        token.trim().to_string()
    } else if is_tty {
        if let Some(url) = token_url {
            println!(
                "OAuth is not available for this {} instance.\n\
                 Please create a Personal Access Token on:\n\
                 {}\n",
                provider_name, url
            );
        }

        let token = rpassword::prompt_password(format!("Enter {}: ", prompt_label))?;
        token.trim().to_string()
    } else {
        return Err(format!("No token provided for {}. Set {}.", provider_name, env_var).into());
    };

    if token.is_empty() {
        return Err("Empty token provided".into());
    }

    Ok(token)
}

/// Try to copy text to clipboard, returning true if successful.
#[cfg(any(feature = "github", feature = "gitlab"))]
fn copy_to_clipboard(text: &str) -> bool {
    match arboard::Clipboard::new() {
        Ok(mut clipboard) => clipboard.set_text(text).is_ok(),
        Err(_) => false,
    }
}
