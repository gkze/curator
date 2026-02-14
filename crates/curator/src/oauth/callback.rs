//! Local callback server for OAuth PKCE flows.
//!
//! This module provides a simple HTTP server that listens for OAuth callbacks
//! on a local port. It's used for the Authorization Code flow with PKCE,
//! which requires a redirect URI to receive the authorization code.
//!
//! # Example
//!
//! ```ignore
//! use curator::oauth::callback::CallbackServer;
//! use std::time::Duration;
//!
//! let server = CallbackServer::new(18484, "random_state_string");
//! let code = server.wait_for_code(Duration::from_secs(300)).await?;
//! println!("Received authorization code: {}", code);
//! ```

use super::error::OAuthError;
use axum::{
    Router,
    extract::{Query, State},
    response::{Html, IntoResponse},
    routing::get,
};
use serde::Deserialize;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::oneshot;

/// Default port for the OAuth callback server.
pub const DEFAULT_CALLBACK_PORT: u16 = 18484;

/// Build the redirect URI for the callback server.
pub fn redirect_uri(port: u16) -> String {
    format!("http://127.0.0.1:{}/callback", port)
}

/// Query parameters received in the OAuth callback.
#[derive(Debug, Deserialize)]
pub struct CallbackParams {
    /// The authorization code (on success).
    pub code: Option<String>,
    /// The state parameter (for CSRF validation).
    pub state: Option<String>,
    /// Error code (on failure).
    pub error: Option<String>,
    /// Error description (on failure).
    pub error_description: Option<String>,
}

/// Shared state for the callback handler.
struct CallbackState {
    /// The expected state parameter for CSRF validation.
    expected_state: String,
    /// Channel to send the result back.
    tx: Option<oneshot::Sender<Result<String, OAuthError>>>,
}

/// A local HTTP server that listens for OAuth callbacks.
///
/// The server listens on `http://127.0.0.1:{port}/callback` and waits for
/// the OAuth provider to redirect the user back with an authorization code.
pub struct CallbackServer {
    port: u16,
    expected_state: String,
}

impl CallbackServer {
    /// Create a new callback server.
    ///
    /// # Arguments
    ///
    /// * `port` - The port to listen on (usually [`DEFAULT_CALLBACK_PORT`]).
    /// * `expected_state` - The state parameter that was sent in the authorization request.
    pub fn new(port: u16, expected_state: impl Into<String>) -> Self {
        Self {
            port,
            expected_state: expected_state.into(),
        }
    }

    /// Wait for the OAuth callback and return the authorization code.
    ///
    /// This starts a local HTTP server and waits for the OAuth provider to
    /// redirect the user back. The server validates the state parameter and
    /// returns the authorization code on success.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for the callback.
    ///
    /// # Returns
    ///
    /// The authorization code on success, or an error if:
    /// - The timeout is reached
    /// - The state parameter doesn't match (CSRF protection)
    /// - The user denied authorization
    /// - The server failed to start
    pub async fn wait_for_code(self, timeout: Duration) -> Result<String, OAuthError> {
        let (tx, rx) = oneshot::channel();

        let state = Arc::new(tokio::sync::Mutex::new(CallbackState {
            expected_state: self.expected_state,
            tx: Some(tx),
        }));

        let app = Router::new()
            .route("/callback", get(handle_callback))
            .with_state(state);

        let addr = SocketAddr::from(([127, 0, 0, 1], self.port));
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            OAuthError::Server(format!("Failed to bind to port {}: {}", self.port, e))
        })?;

        tracing::debug!(
            "OAuth callback server listening on http://{}/callback",
            addr
        );

        // Run the server with a timeout
        let server = axum::serve(listener, app);

        tokio::select! {
            result = rx => {
                match result {
                    Ok(code_result) => code_result,
                    Err(_) => Err(OAuthError::Server("Callback channel closed unexpectedly".into())),
                }
            }
            _ = tokio::time::sleep(timeout) => {
                Err(OAuthError::Expired)
            }
            result = server => {
                match result {
                    Ok(()) => Err(OAuthError::Server("Server shut down unexpectedly".into())),
                    Err(e) => Err(OAuthError::Server(format!("Server error: {}", e))),
                }
            }
        }
    }
}

/// Handle the OAuth callback request.
async fn handle_callback(
    State(state): State<Arc<tokio::sync::Mutex<CallbackState>>>,
    Query(params): Query<CallbackParams>,
) -> impl IntoResponse {
    let mut state = state.lock().await;

    let result = process_callback(&state.expected_state, params);
    let is_success = result.is_ok();

    // Send the result through the channel
    if let Some(tx) = state.tx.take() {
        let _ = tx.send(result);
    }

    // Return a user-friendly HTML response
    if is_success {
        Html(SUCCESS_HTML)
    } else {
        Html(ERROR_HTML)
    }
}

/// Process the callback parameters and validate the state.
fn process_callback(expected_state: &str, params: CallbackParams) -> Result<String, OAuthError> {
    // Check for errors first
    if let Some(error) = params.error {
        if error == "access_denied" {
            return Err(OAuthError::AccessDenied);
        }
        let message = params.error_description.unwrap_or_else(|| error.clone());
        return Err(OAuthError::Provider {
            provider: "OAuth",
            message,
        });
    }

    // Validate state (CSRF protection)
    match params.state {
        Some(ref state) if state == expected_state => {}
        Some(_) => return Err(OAuthError::InvalidState),
        None => return Err(OAuthError::InvalidState),
    }

    // Extract the authorization code
    params
        .code
        .ok_or_else(|| OAuthError::Parse("Missing authorization code in callback".into()))
}

/// HTML response shown to the user on successful authorization.
const SUCCESS_HTML: &str = include_str!("callback_success.html");

/// HTML response shown to the user on failed authorization.
const ERROR_HTML: &str = include_str!("callback_error.html");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redirect_uri() {
        assert_eq!(redirect_uri(18484), "http://127.0.0.1:18484/callback");
    }

    #[test]
    fn test_process_callback_success() {
        let params = CallbackParams {
            code: Some("auth_code_123".into()),
            state: Some("expected_state".into()),
            error: None,
            error_description: None,
        };
        let result = process_callback("expected_state", params);
        assert_eq!(result.unwrap(), "auth_code_123");
    }

    #[test]
    fn test_process_callback_invalid_state() {
        let params = CallbackParams {
            code: Some("auth_code_123".into()),
            state: Some("wrong_state".into()),
            error: None,
            error_description: None,
        };
        let result = process_callback("expected_state", params);
        assert!(matches!(result, Err(OAuthError::InvalidState)));
    }

    #[test]
    fn test_process_callback_missing_state() {
        let params = CallbackParams {
            code: Some("auth_code_123".into()),
            state: None,
            error: None,
            error_description: None,
        };
        let result = process_callback("expected_state", params);
        assert!(matches!(result, Err(OAuthError::InvalidState)));
    }

    #[test]
    fn test_process_callback_access_denied() {
        let params = CallbackParams {
            code: None,
            state: Some("expected_state".into()),
            error: Some("access_denied".into()),
            error_description: Some("User denied access".into()),
        };
        let result = process_callback("expected_state", params);
        assert!(matches!(result, Err(OAuthError::AccessDenied)));
    }

    #[test]
    fn test_process_callback_missing_code() {
        let params = CallbackParams {
            code: None,
            state: Some("expected_state".into()),
            error: None,
            error_description: None,
        };
        let result = process_callback("expected_state", params);
        assert!(matches!(result, Err(OAuthError::Parse(_))));
    }
}
