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
                    println!("  curator sync org {} <group-path>", instance.name);
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use sea_orm::{
        ConnectionTrait, DatabaseBackend, EntityTrait, Set, Statement, sea_query::OnConflict,
    };
    use std::ffi::OsString;
    use std::fs;
    use std::sync::OnceLock;
    use tokio::sync::Mutex;
    use toml_edit::DocumentMut;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn assert_config_value(path: &std::path::Path, section: &str, key: &str, expected: &str) {
        let config_contents =
            fs::read_to_string(path).expect("login flow should write config file");
        let document = config_contents
            .parse::<DocumentMut>()
            .expect("config file should remain valid toml");

        let section = document
            .get(section)
            .and_then(|value| value.as_table())
            .expect("config file should include expected section");

        let actual = section
            .get(key)
            .and_then(|value| value.as_str())
            .expect("config value should be a string");

        assert_eq!(actual, expected);
    }

    struct TempConfigEnv {
        temp_dir: std::path::PathBuf,
        previous_home: Option<OsString>,
        previous_xdg_config_home: Option<OsString>,
    }

    impl TempConfigEnv {
        fn new(label: &str) -> Self {
            let temp_dir = std::env::temp_dir().join(format!(
                "curator-cli-login-tests-{}-{}",
                label,
                Uuid::new_v4()
            ));
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

        fn config_path(&self) -> std::path::PathBuf {
            Config::default_config_path().expect("config path should be available")
        }
    }

    impl Drop for TempConfigEnv {
        fn drop(&mut self) {
            unsafe {
                match &self.previous_home {
                    Some(home) => std::env::set_var("HOME", home),
                    None => std::env::remove_var("HOME"),
                }

                match &self.previous_xdg_config_home {
                    Some(home) => std::env::set_var("XDG_CONFIG_HOME", home),
                    None => std::env::remove_var("XDG_CONFIG_HOME"),
                }
            }

            let _ = fs::remove_dir_all(&self.temp_dir);
        }
    }

    async fn insert_instance(db: &DatabaseConnection, instance: &InstanceModel) {
        use curator::entity::instance::{Column, Entity};

        let active = curator::entity::instance::ActiveModel {
            id: Set(instance.id),
            name: Set(instance.name.clone()),
            platform_type: Set(instance.platform_type),
            host: Set(instance.host.clone()),
            oauth_client_id: Set(instance.oauth_client_id.clone()),
            oauth_flow: Set(instance.oauth_flow.clone()),
            created_at: Set(instance.created_at),
        };

        // Migrations seed well-known instances (github.com, gitlab.com, codeberg.org, ...).
        // These tests need to be able to "insert" those instances without failing on the
        // platform_type+host uniqueness constraint and still force the desired oauth_flow.
        Entity::insert(active)
            .on_conflict(
                OnConflict::columns([Column::PlatformType, Column::Host])
                    .update_columns([
                        Column::Name,
                        Column::OauthClientId,
                        Column::OauthFlow,
                        Column::CreatedAt,
                    ])
                    .to_owned(),
            )
            .exec(db)
            .await
            .expect("insert test instance");
    }

    fn sample_instance(
        name: &str,
        flow: &str,
        platform_type: PlatformType,
        host: &str,
    ) -> InstanceModel {
        InstanceModel {
            id: Uuid::new_v4(),
            name: name.to_string(),
            platform_type,
            host: host.to_string(),
            oauth_client_id: None,
            oauth_flow: flow.to_string(),
            created_at: Utc::now().fixed_offset(),
        }
    }

    fn sqlite_test_url(label: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "curator-cli-login-tests-{}-{}.db",
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

    #[test]
    fn login_method_order_prefers_configured_flow() {
        let token = sample_instance("custom", "token", PlatformType::GitHub, "github.com");
        assert_eq!(login_method_order(&token), vec![LoginMethod::Token]);

        let spaced_token =
            sample_instance("custom", "  ToKeN  ", PlatformType::GitHub, "github.com");
        assert_eq!(login_method_order(&spaced_token), vec![LoginMethod::Token]);

        let pkce = sample_instance("custom", "pkce", PlatformType::Gitea, "codeberg.org");
        assert_eq!(
            login_method_order(&pkce),
            vec![LoginMethod::Pkce, LoginMethod::Token]
        );

        let auto = sample_instance("custom", "auto", PlatformType::GitLab, "gitlab.com");
        assert_eq!(
            login_method_order(&auto),
            vec![LoginMethod::Device, LoginMethod::Pkce, LoginMethod::Token]
        );

        let unknown = sample_instance("custom", "mystery", PlatformType::GitLab, "gitlab.com");
        assert_eq!(
            login_method_order(&unknown),
            vec![LoginMethod::Device, LoginMethod::Pkce, LoginMethod::Token]
        );

        let spaced_device =
            sample_instance("custom", "  DeViCe  ", PlatformType::GitHub, "github.com");
        assert_eq!(
            login_method_order(&spaced_device),
            vec![LoginMethod::Device, LoginMethod::Pkce, LoginMethod::Token]
        );

        let spaced_pkce = sample_instance("custom", "  PKcE  ", PlatformType::GitLab, "gitlab.com");
        assert_eq!(
            login_method_order(&spaced_pkce),
            vec![LoginMethod::Pkce, LoginMethod::Token]
        );

        let blank = sample_instance("custom", "   ", PlatformType::GitHub, "github.com");
        assert_eq!(
            login_method_order(&blank),
            vec![LoginMethod::Device, LoginMethod::Pkce, LoginMethod::Token]
        );
    }

    #[test]
    fn resolve_client_id_prefers_instance_then_well_known() {
        let mut instance = sample_instance("github", "auto", PlatformType::GitHub, "github.com");
        instance.oauth_client_id = Some("custom-client-id".to_string());
        assert_eq!(resolve_client_id(&instance), Some("custom-client-id"));

        let wk = sample_instance("github", "auto", PlatformType::GitHub, "github.com");
        assert_eq!(resolve_client_id(&wk), Some("Ov23liN0721EfoUpRrLl"));

        let unknown = sample_instance("my-ghe", "auto", PlatformType::GitHub, "ghe.local");
        assert_eq!(resolve_client_id(&unknown), None);

        let mut empty_but_explicit =
            sample_instance("github", "auto", PlatformType::GitHub, "github.com");
        empty_but_explicit.oauth_client_id = Some(String::new());
        assert_eq!(resolve_client_id(&empty_but_explicit), Some(""));

        let wk_without_client_id = sample_instance(
            "gnome-gitlab",
            "auto",
            PlatformType::GitLab,
            "gitlab.gnome.org",
        );
        assert_eq!(resolve_client_id(&wk_without_client_id), None);
    }

    #[tokio::test]
    async fn read_token_with_prompt_uses_env_and_rejects_empty_tokens() {
        let _guard = env_lock().lock().await;
        let key = "CURATOR_TEST_TOKEN_LOGIN";
        unsafe {
            std::env::set_var(key, "  abc123  ");
        }
        let token = read_token_with_prompt(key, "Provider", None, "Token", false)
            .expect("env token should be accepted");
        assert_eq!(token, "abc123");

        unsafe {
            std::env::set_var(key, "   ");
        }
        let err = read_token_with_prompt(key, "Provider", None, "Token", false)
            .expect_err("empty token should be rejected");
        assert!(err.to_string().contains("Empty token"));

        unsafe {
            std::env::remove_var(key);
        }
    }

    #[tokio::test]
    async fn read_token_with_prompt_errors_when_non_tty_and_missing_env() {
        let _guard = env_lock().lock().await;
        let key = "CURATOR_TEST_TOKEN_MISSING";
        unsafe {
            std::env::remove_var(key);
        }

        let err = read_token_with_prompt(key, "Provider", None, "Token", false)
            .expect_err("missing env in non-tty mode should fail");
        assert!(err.to_string().contains("No token provided"));
        assert!(err.to_string().contains(key));
        assert!(err.to_string().contains("Provider"));
    }

    #[tokio::test]
    async fn read_token_with_prompt_uses_env_even_when_tty_and_token_url_present() {
        let _guard = env_lock().lock().await;
        let key = "CURATOR_TEST_TOKEN_TTY_PRECEDENCE";
        unsafe {
            std::env::set_var(key, " from-env ");
        }

        let token = read_token_with_prompt(
            key,
            "Provider",
            Some("https://example.com/tokens"),
            "Token",
            true,
        )
        .expect("env token should bypass interactive prompt");

        unsafe {
            std::env::remove_var(key);
        }

        assert_eq!(token, "from-env");
    }

    #[tokio::test]
    async fn get_or_create_instance_returns_existing_instance() {
        let db = setup_db("existing-instance").await;

        let model = sample_instance(
            "my-gitlab",
            "auto",
            PlatformType::GitLab,
            "gitlab.example.com",
        );
        curator::entity::instance::ActiveModel {
            id: Set(model.id),
            name: Set(model.name.clone()),
            platform_type: Set(model.platform_type),
            host: Set(model.host.clone()),
            oauth_client_id: Set(model.oauth_client_id.clone()),
            oauth_flow: Set(model.oauth_flow.clone()),
            created_at: Set(model.created_at),
        }
        .insert(&db)
        .await
        .expect("insert should succeed");

        let found = get_or_create_instance(&db, &model.name)
            .await
            .expect("instance lookup should succeed");
        assert_eq!(found.id, model.id);
        assert_eq!(found.host, "gitlab.example.com");
    }

    #[tokio::test]
    async fn get_or_create_instance_auto_creates_well_known_instances() {
        let db = setup_db("autocreate").await;

        let created = get_or_create_instance(&db, "github")
            .await
            .expect("well-known instance should auto-create");
        assert_eq!(created.name, "github");
        assert_eq!(created.platform_type, PlatformType::GitHub);
        assert_eq!(created.host, "github.com");
    }

    #[tokio::test]
    async fn get_or_create_instance_prefers_existing_even_for_well_known_name() {
        let db = setup_db("existing-well-known").await;

        let mut existing = Instance::find()
            .filter(InstanceColumn::Name.eq("github"))
            .one(&db)
            .await
            .expect("query should succeed")
            .expect("github instance should exist after migration");

        existing.host = "github.enterprise".to_string();
        existing.oauth_client_id = Some("existing-client".to_string());
        existing.oauth_flow = "token".to_string();

        curator::entity::instance::ActiveModel {
            id: Set(existing.id),
            name: Set(existing.name.clone()),
            platform_type: Set(existing.platform_type),
            host: Set(existing.host.clone()),
            oauth_client_id: Set(existing.oauth_client_id.clone()),
            oauth_flow: Set(existing.oauth_flow.clone()),
            created_at: Set(existing.created_at),
        }
        .update(&db)
        .await
        .expect("existing instance update should succeed");

        let resolved = get_or_create_instance(&db, "github")
            .await
            .expect("existing well-known instance should be reused");

        assert_eq!(resolved.id, existing.id);
        assert_eq!(resolved.host, "github.enterprise");
        assert_eq!(resolved.oauth_flow, "token");
        assert_eq!(resolved.oauth_client_id.as_deref(), Some("existing-client"));
    }

    #[tokio::test]
    async fn get_or_create_instance_auto_creates_well_known_without_oauth_client_id() {
        let db = setup_db("autocreate-no-client-id").await;

        let created = get_or_create_instance(&db, "gnome-gitlab")
            .await
            .expect("well-known instance without client id should auto-create");

        assert_eq!(created.name, "gnome-gitlab");
        assert_eq!(created.platform_type, PlatformType::GitLab);
        assert_eq!(created.host, "gitlab.gnome.org");
        assert_eq!(created.oauth_client_id, None);
        assert_eq!(created.oauth_flow, "auto");
    }

    #[tokio::test]
    async fn get_or_create_instance_auto_creates_well_known_with_oauth_client_id() {
        let db = setup_db("autocreate-with-client-id").await;

        let created = get_or_create_instance(&db, "github")
            .await
            .expect("well-known github should auto-create with client id");

        assert_eq!(created.name, "github");
        assert_eq!(created.platform_type, PlatformType::GitHub);
        assert_eq!(created.host, "github.com");
        assert_eq!(
            created.oauth_client_id.as_deref(),
            Some("Ov23liN0721EfoUpRrLl")
        );
        assert_eq!(created.oauth_flow, "auto");
    }

    #[tokio::test]
    async fn get_or_create_instance_errors_for_unknown_instance_name() {
        let db = setup_db("unknown").await;

        let err = get_or_create_instance(&db, "nope")
            .await
            .expect_err("unknown instance should fail");
        assert!(err.to_string().contains("Instance 'nope' not found"));
        assert!(
            err.to_string().contains("curator instance add nope"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn get_or_create_instance_case_mismatched_well_known_name_hits_insert_conflict() {
        let db = setup_db("well-known-case-sensitive").await;

        let err = get_or_create_instance(&db, "GitHub")
            .await
            .expect_err("case-mismatched lookup should not reuse existing github row");

        assert!(
            err.to_string().contains("UNIQUE constraint failed"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn get_or_create_instance_propagates_database_errors() {
        let db = setup_db("query-failure").await;
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "DROP TABLE instances".to_string(),
        ))
        .await
        .expect("instances table should be dropped for error-path test");

        let err = get_or_create_instance(&db, "github")
            .await
            .expect_err("query failure should propagate");
        let message = err.to_string().to_ascii_lowercase();
        assert!(
            message.contains("no such table")
                || message.contains("query")
                || message.contains("connection"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn handle_login_returns_database_error_before_authentication() {
        let db = setup_db("handle-login-query-failure").await;
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "DROP TABLE instances".to_string(),
        ))
        .await
        .expect("instances table should be dropped for error-path test");

        let err = handle_login("github", &db, &Config::default())
            .await
            .expect_err("database query failure should fail before auth flow starts");
        let message = err.to_string().to_ascii_lowercase();
        assert!(
            message.contains("no such table")
                || message.contains("query")
                || message.contains("connection"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn handle_login_returns_unknown_instance_error_before_authentication() {
        let db = setup_db("handle-login-unknown-instance").await;

        let err = handle_login("definitely-missing", &db, &Config::default())
            .await
            .expect_err("unknown instance should fail before auth flow");

        assert!(
            err.to_string()
                .contains("Instance 'definitely-missing' not found"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    #[cfg(feature = "github")]
    async fn handle_login_dispatches_github_token_flow_with_env_token() {
        let _guard = env_lock().lock().await;
        let config_env = TempConfigEnv::new("github-token");

        let db = setup_db("handle-login-github-token").await;
        let instance = sample_instance("github", "token", PlatformType::GitHub, "github.com");
        insert_instance(&db, &instance).await;

        unsafe {
            std::env::set_var("CURATOR_GITHUB_TOKEN", "gh_test_token");
        }

        let result = handle_login(&instance.name, &db, &Config::default()).await;
        assert!(
            result.is_ok(),
            "github token flow should succeed: {result:?}"
        );

        let config_path = config_env.config_path();
        assert_config_value(&config_path, "github", "token", "gh_test_token");

        unsafe {
            std::env::remove_var("CURATOR_GITHUB_TOKEN");
        }
    }

    #[tokio::test]
    #[cfg(feature = "gitlab")]
    async fn handle_login_dispatches_gitlab_token_flow_with_env_token() {
        let _guard = env_lock().lock().await;
        let config_env = TempConfigEnv::new("gitlab-token");

        let db = setup_db("handle-login-gitlab-token").await;
        let instance = sample_instance("gitlab", "token", PlatformType::GitLab, "gitlab.com");
        insert_instance(&db, &instance).await;

        unsafe {
            std::env::set_var("CURATOR_GITLAB_TOKEN", "gl_test_token");
        }

        let result = handle_login(&instance.name, &db, &Config::default()).await;
        assert!(
            result.is_ok(),
            "gitlab token flow should succeed: {result:?}"
        );

        let config_path = config_env.config_path();
        assert_config_value(&config_path, "gitlab", "token", "gl_test_token");

        unsafe {
            std::env::remove_var("CURATOR_GITLAB_TOKEN");
        }
    }

    #[tokio::test]
    #[cfg(feature = "gitea")]
    async fn handle_login_dispatches_gitea_token_flow_with_env_token() {
        let _guard = env_lock().lock().await;
        let config_env = TempConfigEnv::new("gitea-token");

        let db = setup_db("handle-login-gitea-token").await;
        let instance =
            sample_instance("login-gitea", "token", PlatformType::Gitea, "gitea.example");
        insert_instance(&db, &instance).await;

        unsafe {
            std::env::set_var("CURATOR_GITEA_TOKEN", "gitea_test_token");
        }

        let result = handle_login(&instance.name, &db, &Config::default()).await;
        assert!(
            result.is_ok(),
            "gitea token flow should succeed: {result:?}"
        );

        let config_path = config_env.config_path();
        assert_config_value(&config_path, "gitea", "token", "gitea_test_token");
        assert_config_value(&config_path, "gitea", "host", "gitea.example");

        unsafe {
            std::env::remove_var("CURATOR_GITEA_TOKEN");
        }
    }

    #[tokio::test]
    #[cfg(feature = "gitea")]
    async fn handle_login_dispatches_codeberg_token_flow_with_env_token() {
        let _guard = env_lock().lock().await;
        let config_env = TempConfigEnv::new("codeberg-token");

        let db = setup_db("handle-login-codeberg-token").await;
        let instance = sample_instance("codeberg", "token", PlatformType::Gitea, "codeberg.org");
        insert_instance(&db, &instance).await;

        unsafe {
            std::env::set_var("CURATOR_CODEBERG_TOKEN", "codeberg_test_token");
        }

        let result = handle_login(&instance.name, &db, &Config::default()).await;
        assert!(
            result.is_ok(),
            "codeberg token flow should succeed: {result:?}"
        );

        let config_path = config_env.config_path();
        assert_config_value(&config_path, "codeberg", "token", "codeberg_test_token");

        unsafe {
            std::env::remove_var("CURATOR_CODEBERG_TOKEN");
        }
    }
}
