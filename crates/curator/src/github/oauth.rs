//! GitHub OAuth Device Flow implementation.
//!
//! This module provides functionality for authenticating users via GitHub's
//! OAuth Device Flow, which is ideal for CLI applications.
//!
//! # Device Flow Overview
//!
//! 1. Request a device code from GitHub
//! 2. Display the user code and verification URL to the user
//! 3. User visits the URL and enters the code
//! 4. Poll GitHub until the user completes authorization
//! 5. Receive an access token
//!
//! # Example
//!
//! ```ignore
//! use curator::github::oauth::{request_device_code, poll_for_token};
//!
//! // Step 1: Request device code
//! let device_code = request_device_code("public_repo").await?;
//!
//! // Step 2: Show user the code
//! println!("Go to: {}", device_code.verification_uri);
//! println!("Enter code: {}", device_code.user_code);
//!
//! // Step 3: Poll for token
//! let token = poll_for_token(&device_code).await?;
//! println!("Access token: {}", token.access_token);
//! ```

use crate::oauth::OAuthError;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The Client ID for the Curator OAuth App.
///
/// This is a public identifier and is safe to embed in the source code.
/// It identifies the Curator application to GitHub during OAuth flows.
pub const CLIENT_ID: &str = "Ov23liN0721EfoUpRrLl";

/// GitHub's device authorization endpoint.
const DEVICE_CODE_URL: &str = "https://github.com/login/device/code";

/// GitHub's OAuth token endpoint.
const TOKEN_URL: &str = "https://github.com/login/oauth/access_token";

fn device_code_url() -> String {
    std::env::var("CURATOR_GITHUB_DEVICE_CODE_URL").unwrap_or_else(|_| DEVICE_CODE_URL.to_string())
}

fn token_url() -> String {
    std::env::var("CURATOR_GITHUB_TOKEN_URL").unwrap_or_else(|_| TOKEN_URL.to_string())
}

/// Response from GitHub's device code endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct DeviceCodeResponse {
    /// The device verification code (sent to GitHub during polling).
    pub device_code: String,

    /// The code the user enters at the verification URL.
    pub user_code: String,

    /// The URL where the user enters the code.
    pub verification_uri: String,

    /// How long until the device code expires (in seconds).
    pub expires_in: u64,

    /// Minimum interval between polling requests (in seconds).
    pub interval: u64,
}

/// Successful access token response from GitHub.
#[derive(Clone, Deserialize, Serialize)]
pub struct AccessTokenResponse {
    /// The OAuth access token.
    pub access_token: String,

    /// The token type (usually "bearer").
    pub token_type: String,

    /// The granted scopes (space-separated).
    pub scope: String,
}

impl std::fmt::Debug for AccessTokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessTokenResponse")
            .field("access_token", &"[REDACTED]")
            .field("token_type", &self.token_type)
            .field("scope", &self.scope)
            .finish()
    }
}

/// Error response during token polling.
#[derive(Debug, Deserialize)]
struct TokenErrorResponse {
    error: String,
    error_description: Option<String>,
    #[serde(default)]
    interval: Option<u64>,
}

/// Combined response type for token endpoint (can be success or error).
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TokenResponse {
    Success(AccessTokenResponse),
    Error(TokenErrorResponse),
}

/// Request a device code from GitHub to start the OAuth Device Flow.
///
/// # Arguments
///
/// * `scope` - The OAuth scope(s) to request (e.g., "public_repo" for starring public repos).
///
/// # Returns
///
/// A [`DeviceCodeResponse`] containing the user code and verification URL.
///
/// # Example
///
/// ```ignore
/// let device_code = request_device_code("public_repo").await?;
/// println!("Visit {} and enter code: {}", device_code.verification_uri, device_code.user_code);
/// ```
pub async fn request_device_code(scope: &str) -> Result<DeviceCodeResponse, OAuthError> {
    let client = Client::new();

    let response = client
        .post(device_code_url())
        .header("Accept", "application/json")
        .form(&[("client_id", CLIENT_ID), ("scope", scope)])
        .send()
        .await?;

    if !response.status().is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(OAuthError::github(format!(
            "Failed to get device code: {}",
            text
        )));
    }

    response
        .json::<DeviceCodeResponse>()
        .await
        .map_err(|e| OAuthError::Parse(e.to_string()))
}

/// Poll GitHub for an access token after the user has been shown the device code.
///
/// This function will poll GitHub at the specified interval until:
/// - The user authorizes the application (returns the access token)
/// - The device code expires (returns [`OAuthError::Expired`])
/// - The user denies authorization (returns [`OAuthError::AccessDenied`])
///
/// # Arguments
///
/// * `device_code` - The device code response from [`request_device_code`].
///
/// # Returns
///
/// An [`AccessTokenResponse`] containing the access token on success.
///
/// # Example
///
/// ```ignore
/// let token = poll_for_token(&device_code).await?;
/// // Save token.access_token for future API calls
/// ```
pub async fn poll_for_token(
    device_code: &DeviceCodeResponse,
) -> Result<AccessTokenResponse, OAuthError> {
    let client = Client::new();
    let mut interval = Duration::from_secs(device_code.interval);
    let deadline = std::time::Instant::now() + Duration::from_secs(device_code.expires_in);

    loop {
        // Check if we've exceeded the expiration time
        if std::time::Instant::now() >= deadline {
            return Err(OAuthError::Expired);
        }

        // Wait before polling
        tokio::time::sleep(interval).await;

        let response = client
            .post(token_url())
            .header("Accept", "application/json")
            .form(&[
                ("client_id", CLIENT_ID),
                ("device_code", device_code.device_code.as_str()),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ])
            .send()
            .await?;

        if !response.status().is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(OAuthError::github(format!(
                "Token request failed: {}",
                text
            )));
        }

        let token_response: TokenResponse = response
            .json()
            .await
            .map_err(|e| OAuthError::Parse(e.to_string()))?;

        match token_response {
            TokenResponse::Success(token) => return Ok(token),
            TokenResponse::Error(err) => match err.error.as_str() {
                "authorization_pending" => {
                    // User hasn't authorized yet, keep polling
                    continue;
                }
                "slow_down" => {
                    // Increase interval as requested
                    if let Some(new_interval) = err.interval {
                        interval = Duration::from_secs(new_interval);
                    } else {
                        interval += Duration::from_secs(5);
                    }
                    continue;
                }
                "expired_token" => return Err(OAuthError::Expired),
                "access_denied" => return Err(OAuthError::AccessDenied),
                "incorrect_device_code" => return Err(OAuthError::InvalidDeviceCode),
                _ => {
                    return Err(OAuthError::github(
                        err.error_description.unwrap_or(err.error),
                    ));
                }
            },
        }
    }
}

/// Request a device code with the default scope for Curator (public_repo).
///
/// This is a convenience function that requests the minimal scope needed
/// for Curator's functionality (starring public repositories).
pub async fn request_device_code_default() -> Result<DeviceCodeResponse, OAuthError> {
    request_device_code("public_repo").await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::{ffi::OsString, thread};
    use tokio::sync::Mutex as TokioMutex;
    use uuid::Uuid;

    fn env_lock() -> &'static TokioMutex<()> {
        use std::sync::OnceLock;

        static LOCK: OnceLock<TokioMutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| TokioMutex::new(()))
    }

    struct TempGithubOAuthEnv {
        previous_device_code_url: Option<OsString>,
        previous_token_url: Option<OsString>,
    }

    impl TempGithubOAuthEnv {
        fn new(device_code_url: &str, token_url: &str) -> Self {
            let previous_device_code_url = std::env::var_os("CURATOR_GITHUB_DEVICE_CODE_URL");
            let previous_token_url = std::env::var_os("CURATOR_GITHUB_TOKEN_URL");

            unsafe {
                std::env::set_var("CURATOR_GITHUB_DEVICE_CODE_URL", device_code_url);
                std::env::set_var("CURATOR_GITHUB_TOKEN_URL", token_url);
            }

            Self {
                previous_device_code_url,
                previous_token_url,
            }
        }
    }

    impl Drop for TempGithubOAuthEnv {
        fn drop(&mut self) {
            unsafe {
                match &self.previous_device_code_url {
                    Some(value) => std::env::set_var("CURATOR_GITHUB_DEVICE_CODE_URL", value),
                    None => std::env::remove_var("CURATOR_GITHUB_DEVICE_CODE_URL"),
                }
                match &self.previous_token_url {
                    Some(value) => std::env::set_var("CURATOR_GITHUB_TOKEN_URL", value),
                    None => std::env::remove_var("CURATOR_GITHUB_TOKEN_URL"),
                }
            }
        }
    }

    #[derive(Debug, Clone)]
    struct PlannedResponse {
        status: u16,
        headers: Vec<(String, String)>,
        body: String,
    }

    #[derive(Debug, Clone)]
    struct RecordedRequest {
        method: String,
        path: String,
        body: String,
    }

    #[derive(Clone)]
    struct TestServer {
        base_url: String,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    impl TestServer {
        fn spawn(route_responses: HashMap<String, VecDeque<PlannedResponse>>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
            let addr = listener.local_addr().expect("server addr");

            let total: usize = route_responses.values().map(|v| v.len()).sum();
            let state = Arc::new(Mutex::new(route_responses));
            let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::new(Mutex::new(Vec::new()));

            let requests_for_thread = Arc::clone(&requests);
            thread::spawn(move || {
                let mut served = 0usize;
                for mut stream in listener.incoming().flatten() {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    loop {
                        match stream.read(&mut tmp) {
                            Ok(0) => break,
                            Ok(n) => {
                                buf.extend_from_slice(&tmp[..n]);
                                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }

                    let request_text = String::from_utf8_lossy(&buf);
                    let mut lines = request_text.lines();
                    let request_line = lines.next().unwrap_or_default();
                    let mut parts = request_line.split_whitespace();
                    let method = parts.next().unwrap_or("").to_string();
                    let raw_path = parts.next().unwrap_or("/");
                    let path = raw_path.split('?').next().unwrap_or(raw_path).to_string();

                    let (headers_str, body_str) = request_text
                        .split_once("\r\n\r\n")
                        .unwrap_or((&request_text, ""));
                    let content_length = headers_str
                        .lines()
                        .find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            if k.eq_ignore_ascii_case("content-length") {
                                v.trim().parse::<usize>().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0);
                    let mut body = body_str.as_bytes().to_vec();
                    while body.len() < content_length {
                        let mut more = vec![0u8; content_length - body.len()];
                        match stream.read(&mut more) {
                            Ok(0) => break,
                            Ok(n) => body.extend_from_slice(&more[..n]),
                            Err(_) => break,
                        }
                    }

                    requests_for_thread
                        .lock()
                        .expect("requests lock")
                        .push(RecordedRequest {
                            method,
                            path: path.clone(),
                            body: String::from_utf8_lossy(&body).to_string(),
                        });

                    let planned = {
                        let mut state = state.lock().expect("state lock");
                        state.get_mut(&path).and_then(|q| q.pop_front()).unwrap_or(
                            PlannedResponse {
                                status: 500,
                                headers: vec![(
                                    "content-type".to_string(),
                                    "text/plain".to_string(),
                                )],
                                body: "no planned response".to_string(),
                            },
                        )
                    };

                    let body_bytes = planned.body.as_bytes();
                    let mut response = format!(
                        "HTTP/1.1 {} OK\r\nConnection: close\r\nContent-Length: {}\r\n",
                        planned.status,
                        body_bytes.len()
                    );
                    for (k, v) in planned.headers {
                        response.push_str(&format!("{}: {}\r\n", k, v));
                    }
                    response.push_str("\r\n");

                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.write_all(body_bytes);
                    let _ = stream.flush();

                    served += 1;
                    if served >= total {
                        break;
                    }
                }
            });

            Self {
                base_url: format!("http://{}", addr),
                requests,
            }
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().expect("requests lock").clone()
        }
    }

    #[test]
    fn test_client_id_is_set() {
        assert!(!CLIENT_ID.is_empty());
        assert_eq!(CLIENT_ID, "Ov23liN0721EfoUpRrLl");
    }

    #[test]
    fn test_device_code_response_deserialize() {
        let json = r#"{
            "device_code": "abc123",
            "user_code": "ABCD-1234",
            "verification_uri": "https://github.com/login/device",
            "expires_in": 900,
            "interval": 5
        }"#;

        let response: DeviceCodeResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.device_code, "abc123");
        assert_eq!(response.user_code, "ABCD-1234");
        assert_eq!(response.verification_uri, "https://github.com/login/device");
        assert_eq!(response.expires_in, 900);
        assert_eq!(response.interval, 5);
    }

    #[test]
    fn test_access_token_response_deserialize() {
        let json = r#"{
            "access_token": "gho_xxxxxxxxxxxx",
            "token_type": "bearer",
            "scope": "public_repo"
        }"#;

        let response: AccessTokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.access_token, "gho_xxxxxxxxxxxx");
        assert_eq!(response.token_type, "bearer");
        assert_eq!(response.scope, "public_repo");
    }

    #[tokio::test]
    async fn request_device_code_default_uses_public_repo_scope() {
        let _guard = env_lock().lock().await;

        let server = TestServer::spawn(HashMap::from([(
            "/login/device/code".to_string(),
            VecDeque::from([PlannedResponse {
                status: 200,
                headers: vec![("content-type".to_string(), "application/json".to_string())],
                body: r#"{"device_code":"abc","user_code":"ABCD-1234","verification_uri":"https://example.test","expires_in":900,"interval":0}"#
                    .to_string(),
            }]),
        )]));

        let _env = TempGithubOAuthEnv::new(
            &format!("{}/login/device/code", server.base_url),
            &format!("{}/login/oauth/access_token", server.base_url),
        );

        let _ = request_device_code_default()
            .await
            .expect("request_device_code_default should succeed");

        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].path, "/login/device/code");
        assert!(requests[0].body.contains("client_id="));
        assert!(requests[0].body.contains("scope=public_repo"));
    }

    #[tokio::test]
    async fn request_device_code_returns_github_error_on_non_success() {
        let _guard = env_lock().lock().await;

        let server = TestServer::spawn(HashMap::from([(
            "/login/device/code".to_string(),
            VecDeque::from([PlannedResponse {
                status: 400,
                headers: vec![("content-type".to_string(), "text/plain".to_string())],
                body: "bad request".to_string(),
            }]),
        )]));

        let _env = TempGithubOAuthEnv::new(
            &format!("{}/login/device/code", server.base_url),
            &format!("{}/login/oauth/access_token", server.base_url),
        );

        let err = request_device_code("public_repo")
            .await
            .expect_err("should fail");
        assert!(err.to_string().contains("GitHub error:"));
        assert!(err.to_string().contains("Failed to get device code"));
        assert!(err.to_string().contains("bad request"));
    }

    #[tokio::test]
    async fn poll_for_token_returns_token_after_pending_and_slow_down() {
        let _guard = env_lock().lock().await;

        let server = TestServer::spawn(HashMap::from([(
            "/login/oauth/access_token".to_string(),
            VecDeque::from([
                PlannedResponse {
                    status: 200,
                    headers: vec![("content-type".to_string(), "application/json".to_string())],
                    body: r#"{"error":"authorization_pending"}"#.to_string(),
                },
                PlannedResponse {
                    status: 200,
                    headers: vec![("content-type".to_string(), "application/json".to_string())],
                    body: r#"{"error":"slow_down","interval":0}"#.to_string(),
                },
                PlannedResponse {
                    status: 200,
                    headers: vec![("content-type".to_string(), "application/json".to_string())],
                    body:
                        r#"{"access_token":"gho_test","token_type":"bearer","scope":"public_repo"}"#
                            .to_string(),
                },
            ]),
        )]));

        let _env = TempGithubOAuthEnv::new(
            &format!("{}/login/device/code", server.base_url),
            &format!("{}/login/oauth/access_token", server.base_url),
        );

        let device_code = DeviceCodeResponse {
            device_code: "device".to_string(),
            user_code: "ABCD-1234".to_string(),
            verification_uri: "https://example.test".to_string(),
            expires_in: 60,
            interval: 0,
        };

        let token = poll_for_token(&device_code)
            .await
            .expect("poll_for_token should succeed");
        assert_eq!(token.access_token, "gho_test");
        assert_eq!(token.token_type, "bearer");

        let requests = server.requests();
        assert_eq!(requests.len(), 3);
        assert!(
            requests
                .iter()
                .all(|r| r.path == "/login/oauth/access_token")
        );
    }

    #[tokio::test]
    async fn poll_for_token_maps_known_error_variants() {
        let _guard = env_lock().lock().await;

        for (error_json, expected) in [
            (r#"{"error":"expired_token"}"#, "Expired"),
            (r#"{"error":"access_denied"}"#, "AccessDenied"),
            (r#"{"error":"incorrect_device_code"}"#, "InvalidDeviceCode"),
        ] {
            let server = TestServer::spawn(HashMap::from([(
                "/login/oauth/access_token".to_string(),
                VecDeque::from([PlannedResponse {
                    status: 200,
                    headers: vec![("content-type".to_string(), "application/json".to_string())],
                    body: error_json.to_string(),
                }]),
            )]));

            let _env = TempGithubOAuthEnv::new(
                &format!("{}/login/device/code", server.base_url),
                &format!("{}/login/oauth/access_token", server.base_url),
            );

            let device_code = DeviceCodeResponse {
                device_code: Uuid::new_v4().to_string(),
                user_code: "ABCD-1234".to_string(),
                verification_uri: "https://example.test".to_string(),
                expires_in: 60,
                interval: 0,
            };

            let err = poll_for_token(&device_code)
                .await
                .expect_err("should map to an OAuth error");

            let actual = match err {
                OAuthError::Expired => "Expired",
                OAuthError::AccessDenied => "AccessDenied",
                OAuthError::InvalidDeviceCode => "InvalidDeviceCode",
                other => panic!("unexpected error variant: {other:?}"),
            };
            assert_eq!(actual, expected);
        }
    }
}
