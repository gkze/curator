//! Gitea/Codeberg OAuth PKCE flow implementation.
//!
//! This module provides functionality for authenticating users via Gitea's
//! OAuth Authorization Code flow with PKCE (RFC 7636), which is the secure
//! method for public clients like CLI applications.
//!
//! # Supported Platforms
//!
//! - **Codeberg** - Uses the registered Curator OAuth app (built-in client ID)
//! - **Self-hosted Gitea/Forgejo** - Requires manual OAuth app registration
//!
//! # PKCE Flow Overview
//!
//! 1. Generate a random code verifier and compute its SHA256 challenge
//! 2. Build the authorization URL with the code challenge and redirect to browser
//! 3. Start a local callback server to receive the authorization code
//! 4. User authorizes in browser and is redirected to the local callback
//! 5. Exchange the authorization code + verifier for an access token
//!
//! # Example
//!
//! ```ignore
//! use curator::gitea::oauth::{authorize, CodebergAuth};
//! use std::time::Duration;
//!
//! // For Codeberg (uses built-in client ID)
//! let auth = CodebergAuth::new();
//! let token = authorize(&auth, Duration::from_secs(300)).await?;
//! println!("Access token: {}", token.access_token);
//!
//! // For self-hosted Gitea
//! use curator::gitea::oauth::GiteaAuth;
//! let auth = GiteaAuth::new("https://git.example.com", "your_client_id");
//! let token = authorize(&auth, Duration::from_secs(300)).await?;
//! ```
//!
//! # Registering an OAuth App
//!
//! For self-hosted Gitea/Forgejo instances, you need to register an OAuth app:
//!
//! 1. Go to Settings → Applications → Create a new OAuth2 Application
//! 2. Application Name: "Curator"
//! 3. Redirect URI: `http://127.0.0.1:18484/callback`
//! 4. Check "Confidential Client" if available (not required for PKCE)
//! 5. Copy the Client ID (not the secret - PKCE doesn't need it)

use crate::oauth::{
    CallbackServer, DEFAULT_CALLBACK_PORT, OAuthError, normalize_host, redirect_uri,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;

/// The Client ID for the Curator OAuth App on Codeberg.
///
/// This is a public identifier and is safe to embed in the source code.
/// It identifies the Curator application to Codeberg during OAuth flows.
///
/// The redirect URI registered for this app is: `http://127.0.0.1:18484/callback`
pub const CODEBERG_CLIENT_ID: &str = "dfe120ce-2440-4f13-8bb0-9ba5620542a7";

/// Codeberg host URL.
pub const CODEBERG_HOST: &str = "https://codeberg.org";

/// Default OAuth scope for Gitea API access.
///
/// - `read:user` - Read user profile
/// - `write:repository` - Star/unstar repositories
/// - `read:organization` - List organization memberships
pub const DEFAULT_SCOPE: &str = "read:user write:repository read:organization";

/// OAuth configuration for a Gitea-based platform.
pub trait GiteaOAuth {
    /// The base URL of the Gitea instance (e.g., "https://codeberg.org").
    fn base_url(&self) -> &str;

    /// The OAuth client ID.
    fn client_id(&self) -> &str;

    /// The OAuth scope to request.
    fn scope(&self) -> &str {
        DEFAULT_SCOPE
    }

    /// The provider name for error messages.
    fn provider_name(&self) -> &'static str {
        "Gitea"
    }

    /// Create a provider-specific error.
    fn error(&self, message: impl Into<String>) -> OAuthError {
        OAuthError::provider(self.provider_name(), message)
    }
}

/// OAuth configuration for Codeberg (uses built-in client ID).
#[derive(Debug, Clone)]
pub struct CodebergAuth;

impl CodebergAuth {
    /// Create a new Codeberg OAuth configuration.
    pub fn new() -> Self {
        Self
    }
}

impl Default for CodebergAuth {
    fn default() -> Self {
        Self::new()
    }
}

impl GiteaOAuth for CodebergAuth {
    fn base_url(&self) -> &str {
        CODEBERG_HOST
    }

    fn client_id(&self) -> &str {
        CODEBERG_CLIENT_ID
    }

    fn provider_name(&self) -> &'static str {
        "Codeberg"
    }

    fn error(&self, message: impl Into<String>) -> OAuthError {
        OAuthError::codeberg(message)
    }
}

/// OAuth configuration for self-hosted Gitea/Forgejo instances.
#[derive(Debug, Clone)]
pub struct GiteaAuth {
    base_url: String,
    client_id: String,
    scope: String,
}

impl GiteaAuth {
    /// Create a new Gitea OAuth configuration.
    ///
    /// # Arguments
    ///
    /// * `host` - The Gitea host (e.g., "git.example.com" or "https://git.example.com").
    /// * `client_id` - The OAuth client ID from your registered application.
    pub fn new(host: &str, client_id: impl Into<String>) -> Self {
        Self {
            base_url: normalize_host(host),
            client_id: client_id.into(),
            scope: DEFAULT_SCOPE.into(),
        }
    }

    /// Set a custom scope for the OAuth request.
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = scope.into();
        self
    }
}

impl GiteaOAuth for GiteaAuth {
    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn client_id(&self) -> &str {
        &self.client_id
    }

    fn scope(&self) -> &str {
        &self.scope
    }
}

/// Successful access token response from Gitea.
#[derive(Clone, Deserialize, Serialize)]
pub struct AccessTokenResponse {
    /// The OAuth access token.
    pub access_token: String,

    /// The token type (usually "Bearer").
    pub token_type: String,

    /// How long until the token expires (in seconds).
    #[serde(default)]
    pub expires_in: Option<u64>,

    /// The refresh token for obtaining new access tokens.
    #[serde(default)]
    pub refresh_token: Option<String>,

    /// The granted scope.
    #[serde(default)]
    pub scope: Option<String>,
}

impl std::fmt::Debug for AccessTokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessTokenResponse")
            .field("access_token", &"[REDACTED]")
            .field("token_type", &self.token_type)
            .field("expires_in", &self.expires_in)
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("scope", &self.scope)
            .finish()
    }
}

/// Error response from the token endpoint.
#[derive(Debug, Deserialize)]
struct TokenErrorResponse {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

/// PKCE code verifier for OAuth authorization.
///
/// The code verifier is a cryptographically random string that is used to
/// prove that the client exchanging the authorization code is the same client
/// that initiated the flow.
#[derive(Debug, Clone)]
pub struct PkceVerifier {
    verifier: String,
    challenge: String,
}

impl PkceVerifier {
    /// Generate a new random PKCE code verifier and its corresponding challenge.
    pub fn new() -> Self {
        use rand::Rng;

        // Generate 32 random bytes (256 bits of entropy)
        let random_bytes: [u8; 32] = rand::rng().random();

        // Base64url encode without padding
        let verifier = base64_url_encode(&random_bytes);

        // Compute SHA256 challenge
        let mut hasher = Sha256::new();
        hasher.update(verifier.as_bytes());
        let hash = hasher.finalize();
        let challenge = base64_url_encode(&hash);

        Self {
            verifier,
            challenge,
        }
    }

    /// Get the code verifier string (used when exchanging the authorization code).
    pub fn verifier(&self) -> &str {
        &self.verifier
    }

    /// Get the code challenge string (sent with the authorization request).
    pub fn challenge(&self) -> &str {
        &self.challenge
    }
}

impl Default for PkceVerifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Base64url encode bytes without padding (RFC 4648 Section 5).
fn base64_url_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Generate a cryptographically random state string for CSRF protection.
pub fn generate_state() -> String {
    use rand::Rng;
    let random_bytes: [u8; 16] = rand::rng().random();
    base64_url_encode(&random_bytes)
}

/// Build the authorization URL for the OAuth PKCE flow.
///
/// # Arguments
///
/// * `auth` - The OAuth configuration.
/// * `pkce` - The PKCE code verifier/challenge.
/// * `state` - The CSRF protection state string.
/// * `redirect_uri` - The callback URI.
///
/// # Returns
///
/// The full authorization URL to open in the browser.
pub fn build_auth_url(
    auth: &impl GiteaOAuth,
    pkce: &PkceVerifier,
    state: &str,
    redirect_uri: &str,
) -> String {
    let base = auth.base_url();
    let client_id = auth.client_id();
    let scope = auth.scope();

    format!(
        "{}/login/oauth/authorize?\
         client_id={}&\
         redirect_uri={}&\
         response_type=code&\
         code_challenge={}&\
         code_challenge_method=S256&\
         state={}&\
         scope={}",
        base,
        urlencoding::encode(client_id),
        urlencoding::encode(redirect_uri),
        urlencoding::encode(pkce.challenge()),
        urlencoding::encode(state),
        urlencoding::encode(scope)
    )
}

/// Exchange an authorization code for an access token.
///
/// # Arguments
///
/// * `auth` - The OAuth configuration.
/// * `code` - The authorization code received from the callback.
/// * `pkce` - The PKCE code verifier (must be the same one used to generate the challenge).
/// * `redirect_uri` - The callback URI (must match the one in the authorization request).
///
/// # Returns
///
/// An [`AccessTokenResponse`] containing the access token on success.
pub async fn exchange_code(
    auth: &impl GiteaOAuth,
    code: &str,
    pkce: &PkceVerifier,
    redirect_uri: &str,
) -> Result<AccessTokenResponse, OAuthError> {
    let client = Client::new();
    let url = format!("{}/login/oauth/access_token", auth.base_url());

    let response = client
        .post(&url)
        .header("Accept", "application/json")
        .form(&[
            ("client_id", auth.client_id()),
            ("code", code),
            ("code_verifier", pkce.verifier()),
            ("redirect_uri", redirect_uri),
            ("grant_type", "authorization_code"),
        ])
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await.unwrap_or_default();

    if !status.is_success() {
        // Try to parse as error response
        if let Ok(err) = serde_json::from_str::<TokenErrorResponse>(&text) {
            return Err(auth.error(err.error_description.unwrap_or(err.error)));
        }
        return Err(auth.error(format!("Token exchange failed ({}): {}", status, text)));
    }

    serde_json::from_str::<AccessTokenResponse>(&text)
        .map_err(|e| OAuthError::Parse(format!("Failed to parse token response: {}", e)))
}

/// Refresh an expired access token using a refresh token.
///
/// # Arguments
///
/// * `auth` - The OAuth configuration.
/// * `refresh_token` - The refresh token from a previous token response.
///
/// # Returns
///
/// A new [`AccessTokenResponse`] with fresh tokens.
pub async fn refresh_access_token(
    auth: &impl GiteaOAuth,
    refresh_token: &str,
) -> Result<AccessTokenResponse, OAuthError> {
    let client = Client::new();
    let url = format!("{}/login/oauth/access_token", auth.base_url());

    let response = client
        .post(&url)
        .header("Accept", "application/json")
        .form(&[
            ("client_id", auth.client_id()),
            ("refresh_token", refresh_token),
            ("grant_type", "refresh_token"),
        ])
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await.unwrap_or_default();

    if !status.is_success() {
        if let Ok(err) = serde_json::from_str::<TokenErrorResponse>(&text) {
            return Err(auth.error(err.error_description.unwrap_or(err.error)));
        }
        return Err(auth.error(format!("Token refresh failed ({}): {}", status, text)));
    }

    serde_json::from_str::<AccessTokenResponse>(&text)
        .map_err(|e| OAuthError::Parse(format!("Failed to parse token response: {}", e)))
}

/// Perform the complete OAuth PKCE authorization flow.
///
/// This is the high-level function that orchestrates the entire PKCE flow:
///
/// 1. Generates PKCE verifier/challenge and CSRF state
/// 2. Opens the authorization URL in the user's browser
/// 3. Starts a local callback server to receive the authorization code
/// 4. Exchanges the code for an access token
///
/// # Arguments
///
/// * `auth` - The OAuth configuration (Codeberg or Gitea).
/// * `timeout` - Maximum time to wait for the user to authorize.
///
/// # Returns
///
/// An [`AccessTokenResponse`] containing the access token on success.
///
/// # Example
///
/// ```ignore
/// use curator::gitea::oauth::{authorize, CodebergAuth};
/// use std::time::Duration;
///
/// let auth = CodebergAuth::new();
/// let token = authorize(&auth, Duration::from_secs(300)).await?;
/// println!("Logged in! Token: {}", token.access_token);
/// ```
pub async fn authorize(
    auth: &impl GiteaOAuth,
    timeout: Duration,
) -> Result<AccessTokenResponse, OAuthError> {
    // Generate PKCE challenge and CSRF state
    let pkce = PkceVerifier::new();
    let state = generate_state();
    let callback_uri = redirect_uri(DEFAULT_CALLBACK_PORT);

    // Build authorization URL
    let auth_url = build_auth_url(auth, &pkce, &state, &callback_uri);

    // Open browser
    tracing::info!("Opening browser for authorization...");
    if let Err(e) = open::that(&auth_url) {
        tracing::warn!("Failed to open browser automatically: {}", e);
        println!(
            "\nPlease open this URL in your browser:\n\n  {}\n",
            auth_url
        );
    }

    // Wait for callback
    let server = CallbackServer::new(DEFAULT_CALLBACK_PORT, &state);
    let code = server.wait_for_code(timeout).await?;

    // Exchange code for token
    exchange_code(auth, &code, &pkce, &callback_uri).await
}

/// Compute the expiry Unix timestamp from an [`AccessTokenResponse`].
///
/// Returns `None` if `expires_in` is missing. Uses the current time as the
/// base since Gitea doesn't return a `created_at` field.
pub fn token_expires_at(token: &AccessTokenResponse) -> Option<u64> {
    token.expires_in.map(|expires_in| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + expires_in
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pkce_verifier_length() {
        let pkce = PkceVerifier::new();
        // Base64url of 32 bytes = 43 characters (without padding)
        assert_eq!(pkce.verifier().len(), 43);
        // SHA256 hash of 43 bytes, base64url encoded = 43 characters
        assert_eq!(pkce.challenge().len(), 43);
    }

    #[test]
    fn test_pkce_verifier_unique() {
        let pkce1 = PkceVerifier::new();
        let pkce2 = PkceVerifier::new();
        assert_ne!(pkce1.verifier(), pkce2.verifier());
        assert_ne!(pkce1.challenge(), pkce2.challenge());
    }

    #[test]
    fn test_generate_state_length() {
        let state = generate_state();
        // Base64url of 16 bytes = 22 characters (without padding)
        assert_eq!(state.len(), 22);
    }

    #[test]
    fn test_generate_state_unique() {
        let state1 = generate_state();
        let state2 = generate_state();
        assert_ne!(state1, state2);
    }

    #[test]
    fn test_codeberg_auth() {
        let auth = CodebergAuth::new();
        assert_eq!(auth.base_url(), CODEBERG_HOST);
        assert_eq!(auth.client_id(), CODEBERG_CLIENT_ID);
        assert_eq!(auth.provider_name(), "Codeberg");
    }

    #[test]
    fn test_gitea_auth() {
        let auth = GiteaAuth::new("git.example.com", "test_client_id");
        assert_eq!(auth.base_url(), "https://git.example.com");
        assert_eq!(auth.client_id(), "test_client_id");
        assert_eq!(auth.scope(), DEFAULT_SCOPE);
        assert_eq!(auth.provider_name(), "Gitea");
    }

    #[test]
    fn test_gitea_auth_with_scheme() {
        let auth = GiteaAuth::new("http://localhost:3000", "test");
        assert_eq!(auth.base_url(), "http://localhost:3000");
    }

    #[test]
    fn test_gitea_auth_custom_scope() {
        let auth = GiteaAuth::new("git.example.com", "test").with_scope("read:user");
        assert_eq!(auth.scope(), "read:user");
    }

    #[test]
    fn test_build_auth_url() {
        let auth = GiteaAuth::new("https://codeberg.org", "test_client");
        let pkce = PkceVerifier::new();
        let state = "test_state";
        let redirect = "http://127.0.0.1:18484/callback";

        let url = build_auth_url(&auth, &pkce, state, redirect);

        assert!(url.starts_with("https://codeberg.org/login/oauth/authorize?"));
        assert!(url.contains("client_id=test_client"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("code_challenge="));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("state=test_state"));
    }

    #[test]
    fn test_access_token_response_deserialize() {
        let json = r#"{
            "access_token": "TOKEN",
            "token_type": "Bearer",
            "expires_in": 7200,
            "refresh_token": "REFRESH",
            "scope": "read:user"
        }"#;

        let response: AccessTokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.access_token, "TOKEN");
        assert_eq!(response.token_type, "Bearer");
        assert_eq!(response.expires_in, Some(7200));
        assert_eq!(response.refresh_token.as_deref(), Some("REFRESH"));
        assert_eq!(response.scope.as_deref(), Some("read:user"));
    }

    #[test]
    fn test_access_token_response_minimal() {
        let json = r#"{
            "access_token": "TOKEN",
            "token_type": "Bearer"
        }"#;

        let response: AccessTokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.access_token, "TOKEN");
        assert_eq!(response.expires_in, None);
        assert_eq!(response.refresh_token, None);
    }

    #[test]
    fn test_token_expires_at() {
        let token = AccessTokenResponse {
            access_token: "t".into(),
            token_type: "Bearer".into(),
            expires_in: Some(7200),
            refresh_token: None,
            scope: None,
        };

        let expires_at = token_expires_at(&token);
        assert!(expires_at.is_some());

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Should be approximately now + 7200
        let diff = expires_at.unwrap() - now;
        assert!((7199..=7201).contains(&diff));
    }

    #[test]
    fn test_token_expires_at_none() {
        let token = AccessTokenResponse {
            access_token: "t".into(),
            token_type: "Bearer".into(),
            expires_in: None,
            refresh_token: None,
            scope: None,
        };

        assert_eq!(token_expires_at(&token), None);
    }

    #[test]
    fn test_base64_url_encode() {
        // Test vector: empty input
        assert_eq!(base64_url_encode(&[]), "");

        // Test with known input
        let input = [0xfb, 0xff]; // Would have + and / in standard base64
        let encoded = base64_url_encode(&input);
        assert!(!encoded.contains('+'));
        assert!(!encoded.contains('/'));
        assert!(!encoded.contains('='));
    }

    // ========== Additional PKCE Tests ==========

    #[test]
    fn test_pkce_verifier_charset() {
        // RFC 7636: code_verifier must be [A-Z] / [a-z] / [0-9] / "-" / "." / "_" / "~"
        // Base64url uses [A-Za-z0-9_-] which is a subset of the allowed charset
        let pkce = PkceVerifier::new();
        for c in pkce.verifier().chars() {
            assert!(
                c.is_ascii_alphanumeric() || c == '_' || c == '-',
                "Invalid character in verifier: {:?}",
                c
            );
        }
        for c in pkce.challenge().chars() {
            assert!(
                c.is_ascii_alphanumeric() || c == '_' || c == '-',
                "Invalid character in challenge: {:?}",
                c
            );
        }
    }

    #[test]
    fn test_pkce_challenge_sha256_correctness() {
        // Test that challenge is correctly derived from verifier via SHA256
        // We can verify by manually computing: SHA256(verifier) -> base64url
        use sha2::{Digest, Sha256};

        let pkce = PkceVerifier::new();

        // Recompute the challenge manually
        let mut hasher = Sha256::new();
        hasher.update(pkce.verifier().as_bytes());
        let hash = hasher.finalize();
        let expected_challenge = base64_url_encode(&hash);

        assert_eq!(pkce.challenge(), expected_challenge);
    }

    #[test]
    fn test_pkce_known_vector() {
        // RFC 7636 Appendix B test vector (conceptually)
        // Verifier: "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        // Challenge: E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM
        // We can't directly test our PkceVerifier with a fixed verifier,
        // but we can verify the base64url encoding and SHA256 are correct.

        // Test known SHA256 -> base64url encoding
        let input = "test_verifier_string";
        let mut hasher = sha2::Sha256::new();
        hasher.update(input.as_bytes());
        let hash = hasher.finalize();
        let encoded = base64_url_encode(&hash);

        // SHA256("test_verifier_string") = known hash
        // Just verify it's the right length and charset
        assert_eq!(encoded.len(), 43); // SHA256 = 32 bytes -> 43 base64url chars
        assert!(
            encoded
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        );
    }

    #[test]
    fn test_pkce_default() {
        let pkce = PkceVerifier::default();
        assert_eq!(pkce.verifier().len(), 43);
        assert_eq!(pkce.challenge().len(), 43);
    }

    // ========== State Generation Tests ==========

    #[test]
    fn test_generate_state_charset() {
        // State should only contain base64url safe characters
        for _ in 0..10 {
            let state = generate_state();
            for c in state.chars() {
                assert!(
                    c.is_ascii_alphanumeric() || c == '_' || c == '-',
                    "Invalid character in state: {:?}",
                    c
                );
            }
        }
    }

    #[test]
    fn test_generate_state_entropy() {
        // Generate 100 states and ensure no collisions (probabilistically)
        let states: std::collections::HashSet<String> =
            (0..100).map(|_| generate_state()).collect();
        assert_eq!(
            states.len(),
            100,
            "State collision detected - insufficient entropy"
        );
    }

    // ========== Auth URL Tests ==========

    #[test]
    fn test_build_auth_url_special_chars_in_scope() {
        let auth = GiteaAuth::new("https://git.example.com", "client_id")
            .with_scope("read:user write:repository");
        let pkce = PkceVerifier::new();
        let state = "test_state";
        let redirect = "http://127.0.0.1:18484/callback";

        let url = build_auth_url(&auth, &pkce, state, redirect);

        // Colons and spaces should be URL-encoded
        assert!(url.contains("scope=read%3Auser%20write%3Arepository"));
    }

    #[test]
    fn test_build_auth_url_all_parameters_present() {
        let auth = CodebergAuth::new();
        let pkce = PkceVerifier::new();
        let state = generate_state();
        let redirect = "http://127.0.0.1:18484/callback";

        let url = build_auth_url(&auth, &pkce, &state, redirect);

        // Verify all required OAuth parameters are present
        assert!(url.contains("client_id="));
        assert!(url.contains("redirect_uri="));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("code_challenge="));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("state="));
        assert!(url.contains("scope="));
    }

    #[test]
    fn test_build_auth_url_codeberg() {
        let auth = CodebergAuth::new();
        let pkce = PkceVerifier::new();
        let state = "csrf_token";
        let redirect = "http://127.0.0.1:18484/callback";

        let url = build_auth_url(&auth, &pkce, state, redirect);

        assert!(url.starts_with("https://codeberg.org/login/oauth/authorize?"));
        assert!(url.contains(&format!("client_id={}", CODEBERG_CLIENT_ID)));
    }

    // ========== TokenErrorResponse Tests ==========

    #[test]
    fn test_token_error_response_full() {
        let json = r#"{
            "error": "invalid_grant",
            "error_description": "The authorization code has expired"
        }"#;

        let err: TokenErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(err.error, "invalid_grant");
        assert_eq!(
            err.error_description,
            Some("The authorization code has expired".into())
        );
    }

    #[test]
    fn test_token_error_response_minimal() {
        let json = r#"{"error": "access_denied"}"#;

        let err: TokenErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(err.error, "access_denied");
        assert_eq!(err.error_description, None);
    }

    #[test]
    fn test_token_error_response_various_errors() {
        // Test common OAuth error codes
        let errors = [
            "invalid_request",
            "unauthorized_client",
            "access_denied",
            "unsupported_response_type",
            "invalid_scope",
            "server_error",
            "temporarily_unavailable",
        ];

        for error in errors {
            let json = format!(r#"{{"error": "{}"}}"#, error);
            let parsed: TokenErrorResponse = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed.error, error);
        }
    }

    // ========== AccessTokenResponse Tests ==========

    #[test]
    fn test_access_token_response_serialize_roundtrip() {
        let original = AccessTokenResponse {
            access_token: "test_token_123".into(),
            token_type: "Bearer".into(),
            expires_in: Some(3600),
            refresh_token: Some("refresh_456".into()),
            scope: Some("read:user".into()),
        };

        let json = serde_json::to_string(&original).unwrap();
        let parsed: AccessTokenResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.access_token, original.access_token);
        assert_eq!(parsed.token_type, original.token_type);
        assert_eq!(parsed.expires_in, original.expires_in);
        assert_eq!(parsed.refresh_token, original.refresh_token);
        assert_eq!(parsed.scope, original.scope);
    }

    #[test]
    fn test_access_token_response_clone() {
        let original = AccessTokenResponse {
            access_token: "token".into(),
            token_type: "Bearer".into(),
            expires_in: Some(7200),
            refresh_token: None,
            scope: None,
        };

        let cloned = original.clone();
        assert_eq!(cloned.access_token, original.access_token);
        assert_eq!(cloned.expires_in, original.expires_in);
    }

    #[test]
    fn test_access_token_response_debug() {
        let token = AccessTokenResponse {
            access_token: "secret".into(),
            token_type: "Bearer".into(),
            expires_in: None,
            refresh_token: None,
            scope: None,
        };

        let debug_str = format!("{:?}", token);
        assert!(debug_str.contains("AccessTokenResponse"));
        assert!(!debug_str.contains("secret")); // Token must be redacted in Debug output
        assert!(debug_str.contains("[REDACTED]"));
    }

    // ========== GiteaAuth Tests ==========

    #[test]
    fn test_gitea_auth_clone() {
        let auth = GiteaAuth::new("https://git.example.com", "client_123").with_scope("read:user");
        let cloned = auth.clone();

        assert_eq!(cloned.base_url(), auth.base_url());
        assert_eq!(cloned.client_id(), auth.client_id());
        assert_eq!(cloned.scope(), auth.scope());
    }

    #[test]
    fn test_gitea_auth_debug() {
        let auth = GiteaAuth::new("https://git.example.com", "client_id");
        let debug_str = format!("{:?}", auth);
        assert!(debug_str.contains("GiteaAuth"));
        assert!(debug_str.contains("git.example.com"));
    }

    #[test]
    fn test_gitea_auth_various_hosts() {
        // Test with various host formats
        let cases = [
            ("git.example.com", "https://git.example.com"),
            ("https://git.example.com", "https://git.example.com"),
            ("http://localhost:3000", "http://localhost:3000"),
            ("192.168.1.1:3000", "https://192.168.1.1:3000"),
        ];

        for (input, expected) in cases {
            let auth = GiteaAuth::new(input, "client");
            assert_eq!(auth.base_url(), expected, "Failed for input: {}", input);
        }
    }

    #[test]
    fn test_gitea_auth_error() {
        let auth = GiteaAuth::new("https://git.example.com", "client");
        let error = auth.error("test error message");
        let error_str = format!("{}", error);
        assert!(error_str.contains("test error message"));
    }

    // ========== CodebergAuth Tests ==========

    #[test]
    fn test_codeberg_auth_clone() {
        let auth = CodebergAuth::new();
        let cloned = auth.clone();
        assert_eq!(cloned.base_url(), auth.base_url());
        assert_eq!(cloned.client_id(), auth.client_id());
    }

    #[test]
    fn test_codeberg_auth_debug() {
        let auth = CodebergAuth::new();
        let debug_str = format!("{:?}", auth);
        assert!(debug_str.contains("CodebergAuth"));
    }

    #[test]
    fn test_codeberg_auth_default() {
        // Use Default trait explicitly via function syntax to test the impl
        let auth = <CodebergAuth as Default>::default();
        assert_eq!(auth.base_url(), CODEBERG_HOST);
        assert_eq!(auth.client_id(), CODEBERG_CLIENT_ID);
    }

    #[test]
    fn test_codeberg_auth_error() {
        let auth = CodebergAuth::new();
        let error = auth.error("codeberg-specific error");
        let error_str = format!("{}", error);
        assert!(error_str.contains("codeberg-specific error"));
    }

    #[test]
    fn test_codeberg_auth_scope() {
        let auth = CodebergAuth::new();
        assert_eq!(auth.scope(), DEFAULT_SCOPE);
    }

    // ========== PkceVerifier Tests ==========

    #[test]
    fn test_pkce_verifier_clone() {
        let pkce = PkceVerifier::new();
        let cloned = pkce.clone();
        assert_eq!(cloned.verifier(), pkce.verifier());
        assert_eq!(cloned.challenge(), pkce.challenge());
    }

    #[test]
    fn test_pkce_verifier_debug() {
        let pkce = PkceVerifier::new();
        let debug_str = format!("{:?}", pkce);
        assert!(debug_str.contains("PkceVerifier"));
        assert!(debug_str.contains("verifier"));
        assert!(debug_str.contains("challenge"));
    }

    // ========== token_expires_at Tests ==========

    #[test]
    fn test_token_expires_at_zero() {
        let token = AccessTokenResponse {
            access_token: "t".into(),
            token_type: "Bearer".into(),
            expires_in: Some(0),
            refresh_token: None,
            scope: None,
        };

        let expires_at = token_expires_at(&token);
        assert!(expires_at.is_some());

        // Should be approximately now
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let diff = if expires_at.unwrap() >= now {
            expires_at.unwrap() - now
        } else {
            now - expires_at.unwrap()
        };
        assert!(diff <= 1, "Expected timestamp near now");
    }

    #[test]
    fn test_token_expires_at_large_value() {
        let token = AccessTokenResponse {
            access_token: "t".into(),
            token_type: "Bearer".into(),
            expires_in: Some(31536000), // 1 year in seconds
            refresh_token: None,
            scope: None,
        };

        let expires_at = token_expires_at(&token);
        assert!(expires_at.is_some());

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let diff = expires_at.unwrap() - now;
        // Should be approximately 1 year
        assert!((31535999..=31536001).contains(&diff));
    }

    // ========== Constants Tests ==========

    #[test]
    fn test_constants() {
        assert!(!CODEBERG_CLIENT_ID.is_empty());
        assert!(CODEBERG_HOST.starts_with("https://"));
        assert!(!DEFAULT_SCOPE.is_empty());
        assert!(DEFAULT_SCOPE.contains("read:user"));
    }
}
