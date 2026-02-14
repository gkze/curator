use async_trait::async_trait;
use thiserror::Error;

/// Minimal HTTP method enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

impl HttpMethod {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Delete => "DELETE",
        }
    }
}

/// HTTP headers represented as key/value pairs.
///
/// Header names are treated case-insensitively by helper functions.
pub type HttpHeaders = Vec<(String, String)>;

/// A minimal HTTP request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: HttpHeaders,
    pub body: Vec<u8>,
}

/// A minimal HTTP response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HttpHeaders,
    pub body: Vec<u8>,
}

impl HttpResponse {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        header_get(&self.headers, name)
    }
}

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("http transport error: {0}")]
    Transport(String),

    #[error("no mock response registered for {method} {url}")]
    NoMockResponse { method: String, url: String },
}

/// Transport boundary for all HTTP I/O.
#[async_trait]
pub trait HttpTransport: Send + Sync {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpError>;
}

/// Get the first header value matching `name` (case-insensitive).
#[must_use]
pub fn header_get<'a>(headers: &'a HttpHeaders, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

#[cfg(any(
    feature = "github",
    feature = "gitlab",
    feature = "gitea",
    feature = "discovery"
))]
pub mod reqwest_transport {
    use super::*;

    use std::time::Duration as StdDuration;

    /// A real HTTP transport backed by reqwest.
    #[derive(Clone)]
    pub struct ReqwestTransport {
        client: reqwest::Client,
    }

    impl ReqwestTransport {
        pub fn new(client: reqwest::Client) -> Self {
            Self { client }
        }

        pub fn with_timeout(timeout: StdDuration) -> Result<Self, HttpError> {
            let client = reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .map_err(|e| HttpError::Transport(e.to_string()))?;
            Ok(Self { client })
        }
    }

    #[async_trait]
    impl HttpTransport for ReqwestTransport {
        async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
            let method = match request.method {
                HttpMethod::Get => reqwest::Method::GET,
                HttpMethod::Post => reqwest::Method::POST,
                HttpMethod::Put => reqwest::Method::PUT,
                HttpMethod::Delete => reqwest::Method::DELETE,
            };

            let mut builder = self.client.request(method, &request.url);
            for (k, v) in request.headers {
                builder = builder.header(&k, &v);
            }

            if !request.body.is_empty() {
                builder = builder.body(request.body);
            }

            let resp = builder
                .send()
                .await
                .map_err(|e| HttpError::Transport(e.to_string()))?;

            let status = resp.status().as_u16();
            let mut headers: HttpHeaders = Vec::new();
            for (name, value) in resp.headers().iter() {
                headers.push((
                    name.as_str().to_string(),
                    value.to_str().unwrap_or_default().to_string(),
                ));
            }

            let body = resp
                .bytes()
                .await
                .map_err(|e| HttpError::Transport(e.to_string()))?
                .to_vec();

            Ok(HttpResponse {
                status,
                headers,
                body,
            })
        }
    }
}

// ---------- Test-only mock transport ----------

#[cfg(test)]
use std::collections::{HashMap, VecDeque};
#[cfg(test)]
use std::sync::{Arc, Mutex};

/// In-memory mock transport.
///
/// This is designed for unit tests: no sockets, no loopback HTTP servers.
#[cfg(test)]
#[derive(Clone, Default)]
pub struct MockTransport {
    inner: Arc<Mutex<MockTransportInner>>,
}

#[cfg(test)]
#[derive(Default)]
struct MockTransportInner {
    routes: HashMap<(HttpMethod, String), VecDeque<HttpResponse>>,
    requests: Vec<HttpRequest>,
}

#[cfg(test)]
impl MockTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a response for a method + URL.
    ///
    /// If multiple responses are registered for the same key, they are returned
    /// in FIFO order.
    pub fn push_response(
        &self,
        method: HttpMethod,
        url: impl Into<String>,
        response: HttpResponse,
    ) {
        let mut inner = self
            .inner
            .lock()
            .expect("mock transport lock should not be poisoned");
        inner
            .routes
            .entry((method, url.into()))
            .or_default()
            .push_back(response);
    }

    #[must_use]
    pub fn requests(&self) -> Vec<HttpRequest> {
        let inner = self
            .inner
            .lock()
            .expect("mock transport lock should not be poisoned");
        inner.requests.clone()
    }
}

#[cfg(test)]
#[async_trait]
impl HttpTransport for MockTransport {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let mut inner = self
            .inner
            .lock()
            .expect("mock transport lock should not be poisoned");

        let key = (request.method, request.url.clone());
        inner.requests.push(request);

        match inner.routes.get_mut(&key).and_then(|q| q.pop_front()) {
            Some(resp) => Ok(resp),
            None => Err(HttpError::NoMockResponse {
                method: key.0.as_str().to_string(),
                url: key.1,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn header_get_is_case_insensitive_and_returns_first_match() {
        let headers: HttpHeaders = vec![
            ("ETag".to_string(), "W/\"abc\"".to_string()),
            ("etag".to_string(), "W/\"def\"".to_string()),
        ];
        assert_eq!(header_get(&headers, "etag"), Some("W/\"abc\""));
        assert_eq!(header_get(&headers, "ETAG"), Some("W/\"abc\""));
        assert_eq!(header_get(&headers, "missing"), None);
    }

    #[test]
    fn http_method_as_str_matches_expected_values() {
        assert_eq!(HttpMethod::Get.as_str(), "GET");
        assert_eq!(HttpMethod::Post.as_str(), "POST");
        assert_eq!(HttpMethod::Put.as_str(), "PUT");
        assert_eq!(HttpMethod::Delete.as_str(), "DELETE");
    }

    #[test]
    fn http_response_header_delegates_to_helper() {
        let resp = HttpResponse {
            status: 200,
            headers: vec![("Content-Type".to_string(), "text/plain".to_string())],
            body: Vec::new(),
        };
        assert_eq!(resp.header("content-type"), Some("text/plain"));
        assert_eq!(resp.header("missing"), None);
    }

    #[tokio::test]
    async fn mock_transport_returns_registered_response_and_records_request() {
        let transport = MockTransport::new();
        let url = "https://example.com/api";

        transport.push_response(
            HttpMethod::Get,
            url,
            HttpResponse {
                status: 200,
                headers: vec![("X-Test".to_string(), "ok".to_string())],
                body: b"hello".to_vec(),
            },
        );

        let req = HttpRequest {
            method: HttpMethod::Get,
            url: url.to_string(),
            headers: vec![("Accept".to_string(), "application/json".to_string())],
            body: Vec::new(),
        };
        let resp = transport.send(req.clone()).await.expect("mock response");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.header("x-test"), Some("ok"));
        assert_eq!(resp.body, b"hello".to_vec());

        let requests = transport.requests();
        assert_eq!(requests, vec![req]);
    }

    #[tokio::test]
    async fn mock_transport_errors_when_no_response_is_registered() {
        let transport = MockTransport::new();
        let req = HttpRequest {
            method: HttpMethod::Get,
            url: "https://example.com/missing".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
        };

        let err = transport
            .send(req)
            .await
            .expect_err("missing mock should error");
        match err {
            HttpError::NoMockResponse { method, url } => {
                assert_eq!(method, "GET");
                assert_eq!(url, "https://example.com/missing");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    #[cfg(any(
        feature = "github",
        feature = "gitlab",
        feature = "gitea",
        feature = "discovery"
    ))]
    fn reqwest_transport_with_timeout_builds_client() {
        let transport = reqwest_transport::ReqwestTransport::with_timeout(Duration::from_millis(1))
            .expect("reqwest transport should build");
        let _ = transport;
    }

    #[tokio::test]
    #[cfg(any(
        feature = "github",
        feature = "gitlab",
        feature = "gitea",
        feature = "discovery"
    ))]
    async fn reqwest_transport_send_makes_request_and_reads_response() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");

        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set_read_timeout");

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
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut =>
                    {
                        break;
                    }
                    Err(e) => panic!("read request: {e}"),
                }
            }

            let req_text = String::from_utf8_lossy(&buf);
            assert!(
                req_text.starts_with("PUT /test "),
                "unexpected request line: {req_text:?}"
            );
            assert!(
                req_text.to_lowercase().contains("x-test: 1"),
                "expected x-test header"
            );

            // Respond with a small body.
            let body = b"ok";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("write headers");
            stream.write_all(body).expect("write body");
            stream.flush().ok();
        });

        let transport = reqwest_transport::ReqwestTransport::new(reqwest::Client::new());
        let url = format!("http://{addr}/test");
        let req = HttpRequest {
            method: HttpMethod::Put,
            url,
            headers: vec![("X-Test".to_string(), "1".to_string())],
            body: b"hello".to_vec(),
        };

        let resp = transport.send(req).await.expect("transport should succeed");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"ok".to_vec());

        handle.join().expect("server thread");
    }

    #[tokio::test]
    #[cfg(any(
        feature = "github",
        feature = "gitlab",
        feature = "gitea",
        feature = "discovery"
    ))]
    async fn reqwest_transport_send_returns_transport_error_for_invalid_url() {
        let transport = reqwest_transport::ReqwestTransport::new(reqwest::Client::new());
        let req = HttpRequest {
            method: HttpMethod::Get,
            url: "not a url".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
        };

        let err = transport.send(req).await.expect_err("expected error");
        assert!(matches!(err, HttpError::Transport(_)));
    }
}
