use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use regex::Regex;
use scraper::{Html, Selector};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use url::Url;

use super::repo_links::{RepoLink, extract_repo_link, normalize_host};
use super::sitemap::collect_sitemap_urls;

use crate::{HttpMethod, HttpRequest, HttpTransport, header_get};

pub type DiscoveryProgressCallback = dyn Fn(DiscoveryProgress) + Send + Sync;

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("invalid start URL: {0}")]
    InvalidUrl(String),
    #[error("failed to build HTTP client: {0}")]
    Client(String),
}

#[derive(Debug, Clone)]
pub struct CrawlOptions {
    pub max_depth: usize,
    pub max_pages: usize,
    pub concurrency: usize,
    pub same_host: bool,
    pub include_subdomains: bool,
    pub use_sitemaps: bool,
    pub request_timeout: StdDuration,
    pub max_body_bytes: usize,
    pub user_agent: Option<String>,
}

#[derive(Debug, Clone)]
pub enum DiscoveryProgress {
    Started {
        seed_urls: usize,
        max_pages: usize,
        max_depth: usize,
    },
    PageFetched {
        fetched: usize,
    },
    Error {
        message: String,
    },
    Finished {
        pages_visited: usize,
        pages_fetched: usize,
        repo_links: usize,
        errors: usize,
    },
}

impl Default for CrawlOptions {
    fn default() -> Self {
        Self {
            max_depth: 2,
            max_pages: 1000,
            concurrency: 10,
            same_host: true,
            include_subdomains: false,
            use_sitemaps: true,
            request_timeout: StdDuration::from_secs(15),
            max_body_bytes: 4 * 1024 * 1024,
            user_agent: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    pub start_url: Url,
    pub seed_urls: Vec<Url>,
    pub pages_visited: usize,
    pub pages_fetched: usize,
    pub repo_links: Vec<RepoLink>,
    pub errors: Vec<String>,
}

pub async fn discover_repo_links(
    start_url: &Url,
    options: &CrawlOptions,
    on_progress: Option<&DiscoveryProgressCallback>,
) -> Result<DiscoveryResult, DiscoveryError> {
    let transport = build_transport(options)?;

    discover_repo_links_with_transport(start_url, options, transport, on_progress).await
}

pub async fn discover_repo_links_with_transport(
    start_url: &Url,
    options: &CrawlOptions,
    transport: Arc<dyn HttpTransport>,
    on_progress: Option<&DiscoveryProgressCallback>,
) -> Result<DiscoveryResult, DiscoveryError> {
    let start_url = normalize_url(start_url)
        .ok_or_else(|| DiscoveryError::InvalidUrl(start_url.as_str().to_string()))?;

    let user_agent = options
        .user_agent
        .clone()
        .unwrap_or_else(|| "curator-discovery/0.1".to_string());

    let mut seed_urls = vec![start_url.clone()];
    if options.use_sitemaps {
        let sitemap_urls = collect_sitemap_urls(
            transport.as_ref(),
            &start_url,
            options.max_pages,
            &user_agent,
        )
        .await;
        seed_urls.extend(sitemap_urls);
    }

    emit_progress(
        on_progress,
        DiscoveryProgress::Started {
            seed_urls: seed_urls.len(),
            max_pages: options.max_pages,
            max_depth: options.max_depth,
        },
    );

    let mut crawl_output = crawl_site(
        Arc::clone(&transport),
        &start_url,
        &seed_urls,
        options,
        &user_agent,
        on_progress,
    )
    .await;

    if let Some(repo) = extract_repo_link(&start_url) {
        let mut repo_map: HashMap<String, RepoLink> = crawl_output
            .repo_links
            .into_iter()
            .map(|repo| (repo.key(), repo))
            .collect();
        repo_map.entry(repo.key()).or_insert(repo);
        let mut repo_links: Vec<RepoLink> = repo_map.into_values().collect();
        repo_links.sort_by_key(|repo| repo.key());
        crawl_output.repo_links = repo_links;
    }

    emit_progress(
        on_progress,
        DiscoveryProgress::Finished {
            pages_visited: crawl_output.pages_visited,
            pages_fetched: crawl_output.pages_fetched,
            repo_links: crawl_output.repo_links.len(),
            errors: crawl_output.errors.len(),
        },
    );

    Ok(DiscoveryResult {
        start_url,
        seed_urls: crawl_output.seed_urls,
        pages_visited: crawl_output.pages_visited,
        pages_fetched: crawl_output.pages_fetched,
        repo_links: crawl_output.repo_links,
        errors: crawl_output.errors,
    })
}

struct CrawlOutput {
    seed_urls: Vec<Url>,
    pages_visited: usize,
    pages_fetched: usize,
    repo_links: Vec<RepoLink>,
    errors: Vec<String>,
}

async fn crawl_site(
    transport: Arc<dyn HttpTransport>,
    start_url: &Url,
    seed_urls: &[Url],
    options: &CrawlOptions,
    user_agent: &str,
    on_progress: Option<&DiscoveryProgressCallback>,
) -> CrawlOutput {
    let root_host = normalize_host(start_url.host_str().unwrap_or_default());
    let restrict_same_host_to_repo_candidates = options.same_host
        && is_well_known_forge_host(&root_host)
        && extract_repo_link(start_url).is_some();
    let mut queue: VecDeque<(Url, usize)> = VecDeque::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut errors = Vec::new();
    let mut repo_map: HashMap<String, RepoLink> = HashMap::new();

    for url in seed_urls {
        if let Some(normalized) = normalize_url(url)
            && should_enqueue(
                &normalized,
                &root_host,
                options,
                restrict_same_host_to_repo_candidates,
            )
        {
            queue.push_back((normalized, 0));
        }
    }

    if queue.is_empty() {
        queue.push_back((start_url.clone(), 0));
    }

    let concurrency = std::cmp::max(1, options.concurrency);
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut join_set: JoinSet<Result<PageResult, String>> = JoinSet::new();
    let mut pages_visited = 0usize;
    let mut pages_fetched = 0usize;

    loop {
        while join_set.len() < concurrency && !queue.is_empty() && visited.len() < options.max_pages
        {
            let (url, depth) = queue.pop_front().expect("queue should be non-empty");
            let key = normalized_key(&url);
            if !visited.insert(key) {
                continue;
            }
            pages_visited += 1;

            let transport = Arc::clone(&transport);
            let semaphore = Arc::clone(&semaphore);
            let max_body_bytes = options.max_body_bytes;
            let user_agent = user_agent.to_string();

            join_set.spawn(fetch_page_with_permit(
                transport,
                semaphore,
                url,
                depth,
                max_body_bytes,
                user_agent,
            ));
        }

        if join_set.is_empty() && (queue.is_empty() || visited.len() >= options.max_pages) {
            break;
        }

        let Some(join_result) = join_set.join_next().await else {
            if queue.is_empty() {
                break;
            }
            continue;
        };

        match join_result {
            Ok(Ok(PageResult { depth, data })) => {
                pages_fetched += 1;
                emit_progress(
                    on_progress,
                    DiscoveryProgress::PageFetched {
                        fetched: pages_fetched,
                    },
                );

                for repo in data.repo_links {
                    repo_map.entry(repo.key()).or_insert(repo);
                }

                if depth < options.max_depth {
                    for link in data.links {
                        if visited.len() >= options.max_pages {
                            break;
                        }
                        if should_enqueue(
                            &link,
                            &root_host,
                            options,
                            restrict_same_host_to_repo_candidates,
                        ) {
                            queue.push_back((link, depth + 1));
                        }
                    }
                }
            }
            Ok(Err(err)) => {
                emit_progress(
                    on_progress,
                    DiscoveryProgress::Error {
                        message: err.clone(),
                    },
                );
                errors.push(err);
            }
            Err(err) => {
                let message = format!("crawl task failed: {}", err);
                emit_progress(
                    on_progress,
                    DiscoveryProgress::Error {
                        message: message.clone(),
                    },
                );
                errors.push(message);
            }
        }
    }

    let mut repo_links: Vec<RepoLink> = repo_map.into_values().collect();
    repo_links.sort_by_key(|repo| repo.key());

    CrawlOutput {
        seed_urls: seed_urls.to_vec(),
        pages_visited,
        pages_fetched,
        repo_links,
        errors,
    }
}

struct PageData {
    links: Vec<Url>,
    repo_links: Vec<RepoLink>,
}

struct PageResult {
    depth: usize,
    data: PageData,
}

async fn fetch_page_with_permit(
    transport: Arc<dyn HttpTransport>,
    semaphore: Arc<Semaphore>,
    url: Url,
    depth: usize,
    max_body_bytes: usize,
    user_agent: String,
) -> Result<PageResult, String> {
    let _permit = semaphore
        .acquire()
        .await
        .map_err(|_| "semaphore closed".to_string())?;
    let url_str = url.to_string();
    let data = fetch_page(transport.as_ref(), &url, max_body_bytes, &user_agent)
        .await
        .map_err(|err| format!("{}: {}", url_str, err))?;
    Ok(PageResult { depth, data })
}

async fn fetch_page(
    transport: &dyn HttpTransport,
    url: &Url,
    max_body_bytes: usize,
    user_agent: &str,
) -> Result<PageData, String> {
    let request = HttpRequest {
        method: HttpMethod::Get,
        url: url.to_string(),
        headers: vec![("User-Agent".to_string(), user_agent.to_string())],
        body: Vec::new(),
    };

    let response = transport.send(request).await.map_err(|e| e.to_string())?;

    if !(200..=299).contains(&response.status) {
        return Err(format!("HTTP {}", response.status));
    }

    let content_length = header_get(&response.headers, "Content-Length")
        .and_then(|value| value.parse::<usize>().ok());
    let content_type = header_get(&response.headers, "Content-Type").unwrap_or("");

    if let Some(content_length) = content_length
        && content_length > max_body_bytes
    {
        return Err(format!("body too large ({} bytes)", content_length));
    }

    if response.body.len() > max_body_bytes {
        return Err(format!("body too large ({} bytes)", response.body.len()));
    }

    let body = String::from_utf8_lossy(&response.body).to_string();

    if !content_type.contains("text/html") && !looks_like_html(&body) {
        return Ok(PageData {
            links: Vec::new(),
            repo_links: Vec::new(),
        });
    }

    let (links, repo_links) = extract_links(url, &body);
    Ok(PageData { links, repo_links })
}

fn extract_links(base: &Url, body: &str) -> (Vec<Url>, Vec<RepoLink>) {
    let mut links = Vec::new();
    let mut repo_links = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    let document = Html::parse_document(body);
    let selector = Selector::parse("a[href]").expect("selector should parse");

    for element in document.select(&selector) {
        let Some(href) = element.value().attr("href") else {
            continue;
        };

        let href = href.trim();
        if href.is_empty()
            || href.starts_with('#')
            || href.starts_with("mailto:")
            || href.starts_with("javascript:")
        {
            continue;
        }

        let link = match base.join(href) {
            Ok(url) => url,
            Err(_) => continue,
        };

        let Some(normalized) = normalize_url(&link) else {
            continue;
        };

        let key = normalized.to_string();
        if !seen.insert(key) {
            continue;
        }

        if let Some(repo) = extract_repo_link(&normalized) {
            repo_links.push(repo);
        }

        links.push(normalized);
    }

    // Also extract plain-text URLs from the body (e.g. inside <pre>/<code> blocks
    // that are not wrapped in <a> tags). This catches curated lists like
    // awesome-* repos that list URLs as plain text.
    extract_text_urls(base, body, &mut seen, &mut links, &mut repo_links);

    (links, repo_links)
}

/// Scan the raw HTML body for bare `https://` URLs that were not already found
/// as `<a href>` links. This is intentionally limited to `https://` to avoid
/// false positives from other URL-like strings.
fn extract_text_urls(
    _base: &Url,
    body: &str,
    seen: &mut HashSet<String>,
    links: &mut Vec<Url>,
    repo_links: &mut Vec<RepoLink>,
) {
    // Match https:// URLs using only characters that are safe in bare URL
    // contexts. We intentionally exclude `&`, `'`, `"`, `<`, `>`, and `{`/`}`
    // so the regex stops before HTML entities (`&amp;`, `&quot;`), attribute
    // boundaries, and embedded JSON.
    let url_re =
        Regex::new(r#"https://[A-Za-z0-9._~:/?#\[\]@!$()*+,;=%-]+"#).expect("regex is valid");

    for m in url_re.find_iter(body) {
        let raw = m.as_str().trim_end_matches(['.', ',', ')', ']', ';', ':']);

        let Ok(url) = Url::parse(raw) else {
            continue;
        };

        let Some(normalized) = normalize_url(&url) else {
            continue;
        };

        let key = normalized.to_string();
        if !seen.insert(key) {
            continue;
        }

        if let Some(repo) = extract_repo_link(&normalized) {
            repo_links.push(repo);
        }

        links.push(normalized);
    }
}

fn build_transport(options: &CrawlOptions) -> Result<Arc<dyn HttpTransport>, DiscoveryError> {
    use crate::http::reqwest_transport::ReqwestTransport;

    crate::http::ensure_rustls_crypto_provider();
    let client = reqwest::Client::builder()
        .timeout(options.request_timeout)
        .build()
        .map_err(|e| DiscoveryError::Client(e.to_string()))?;

    Ok(Arc::new(ReqwestTransport::new(client)))
}

fn normalize_url(url: &Url) -> Option<Url> {
    if url.scheme() != "http" && url.scheme() != "https" {
        return None;
    }

    let mut normalized = url.clone();
    normalized.set_fragment(None);
    normalized.set_query(None);
    Some(normalized)
}

fn normalized_key(url: &Url) -> String {
    let mut key = url.to_string();
    if let Some(pos) = key.find('#') {
        key.truncate(pos);
    }
    key
}

fn should_visit(url: &Url, root_host: &str, options: &CrawlOptions) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = normalize_host(host);

    if !options.same_host {
        return true;
    }

    if host == root_host {
        return true;
    }

    options.include_subdomains && host.ends_with(&format!(".{root_host}"))
}

fn should_enqueue(
    url: &Url,
    root_host: &str,
    options: &CrawlOptions,
    restrict_same_host_to_repo_candidates: bool,
) -> bool {
    if !should_visit(url, root_host, options) {
        return false;
    }

    if !restrict_same_host_to_repo_candidates {
        return true;
    }

    let Some(host) = url.host_str() else {
        return false;
    };

    if normalize_host(host) != root_host {
        return true;
    }

    extract_repo_link(url).is_some()
}

fn is_well_known_forge_host(host: &str) -> bool {
    matches!(
        host,
        "github.com" | "gitlab.com" | "codeberg.org" | "gitea.com" | "git.sr.ht"
    )
}

fn looks_like_html(body: &str) -> bool {
    let trimmed = body.trim_start();
    trimmed.starts_with("<!DOCTYPE html")
        || trimmed.starts_with("<html")
        || trimmed.contains("<head")
        || trimmed.contains("<body")
}

fn emit_progress(on_progress: Option<&DiscoveryProgressCallback>, event: DiscoveryProgress) {
    if let Some(callback) = on_progress {
        callback(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::{HttpHeaders, HttpResponse, HttpTransport, MockTransport};

    fn make_response(
        status: u16,
        content_type: Option<&str>,
        body: &str,
        content_length_override: Option<usize>,
        include_content_length: bool,
    ) -> HttpResponse {
        let mut headers: HttpHeaders = Vec::new();
        if let Some(content_type) = content_type {
            headers.push(("Content-Type".to_string(), content_type.to_string()));
        }
        if include_content_length {
            let len = content_length_override.unwrap_or(body.len());
            headers.push(("Content-Length".to_string(), len.to_string()));
        }

        HttpResponse {
            status,
            headers,
            body: body.as_bytes().to_vec(),
        }
    }

    fn count_requests(transport: &MockTransport, url: &Url) -> usize {
        transport
            .requests()
            .iter()
            .filter(|req| req.method == HttpMethod::Get && req.url == url.to_string())
            .count()
    }

    #[test]
    fn test_normalize_url_strips_fragment_and_query() {
        let url = Url::parse("https://example.com/path?utm=1#section").unwrap();
        let normalized = normalize_url(&url).expect("normalized");

        assert_eq!(normalized.as_str(), "https://example.com/path");
    }

    #[test]
    fn test_should_visit_same_host_only() {
        let root = "example.com";
        let options = CrawlOptions {
            same_host: true,
            include_subdomains: false,
            ..CrawlOptions::default()
        };

        let same = Url::parse("https://example.com/page").unwrap();
        let sub = Url::parse("https://docs.example.com/page").unwrap();
        let external = Url::parse("https://other.com/page").unwrap();

        assert!(should_visit(&same, root, &options));
        assert!(!should_visit(&sub, root, &options));
        assert!(!should_visit(&external, root, &options));
    }

    #[test]
    fn test_should_visit_subdomains() {
        let root = "example.com";
        let options = CrawlOptions {
            same_host: true,
            include_subdomains: true,
            ..CrawlOptions::default()
        };

        let sub = Url::parse("https://docs.example.com/page").unwrap();
        assert!(should_visit(&sub, root, &options));
    }

    #[test]
    fn test_should_visit_when_same_host_disabled() {
        let options = CrawlOptions {
            same_host: false,
            include_subdomains: false,
            ..CrawlOptions::default()
        };

        let external = Url::parse("https://other.example.org/page").unwrap();
        assert!(should_visit(&external, "example.com", &options));
    }

    #[test]
    fn test_should_enqueue_filters_non_repo_paths_when_restricted() {
        let options = CrawlOptions {
            same_host: true,
            include_subdomains: false,
            ..CrawlOptions::default()
        };

        let repo_link = Url::parse("https://github.com/rust-lang/rust").unwrap();
        let marketplace = Url::parse("https://github.com/marketplace").unwrap();
        let partners = Url::parse("https://github.com/partners/resources").unwrap();

        assert!(should_enqueue(&repo_link, "github.com", &options, true));
        assert!(!should_enqueue(&marketplace, "github.com", &options, true));
        assert!(!should_enqueue(&partners, "github.com", &options, true));
    }

    #[test]
    fn test_should_enqueue_does_not_filter_when_restriction_disabled() {
        let options = CrawlOptions {
            same_host: true,
            include_subdomains: false,
            ..CrawlOptions::default()
        };

        let docs_page = Url::parse("https://example.test/docs/install").unwrap();
        assert!(should_enqueue(&docs_page, "example.test", &options, false));
    }

    #[test]
    fn test_is_well_known_forge_host() {
        assert!(is_well_known_forge_host("github.com"));
        assert!(is_well_known_forge_host("gitlab.com"));
        assert!(!is_well_known_forge_host("example.com"));
    }

    #[tokio::test]
    async fn test_fetch_page_skips_non_html_content_type() {
        let transport = MockTransport::new();
        let url = Url::parse("https://example.test/json").unwrap();
        transport.push_response(
            HttpMethod::Get,
            url.to_string(),
            make_response(200, Some("application/json"), "{\"ok\":true}", None, true),
        );

        let result = fetch_page(&transport, &url, 1024, "test-agent")
            .await
            .expect("fetch_page should succeed");

        assert!(result.links.is_empty());
        assert!(result.repo_links.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_page_rejects_large_content_length() {
        let transport = MockTransport::new();
        let url = Url::parse("https://example.test/too-big-header").unwrap();
        transport.push_response(
            HttpMethod::Get,
            url.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                "<html><body>small body</body></html>",
                Some(10_000),
                true,
            ),
        );

        let err = fetch_page(&transport, &url, 32, "test-agent")
            .await
            .err()
            .expect("fetch_page should reject oversized body");

        assert!(err.contains("body too large"));
    }

    #[tokio::test]
    async fn test_fetch_page_rejects_large_body_without_content_length() {
        let transport = MockTransport::new();
        let url = Url::parse("https://example.test/too-big-body").unwrap();
        transport.push_response(
            HttpMethod::Get,
            url.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                "<html><body>12345678901234567890</body></html>",
                None,
                false,
            ),
        );

        let err = fetch_page(&transport, &url, 10, "test-agent")
            .await
            .err()
            .expect("fetch_page should reject oversized body");

        assert!(err.contains("body too large"));
    }

    #[tokio::test]
    async fn test_crawl_site_honors_max_depth() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());

        let start = Url::parse("https://example.test/").unwrap();
        let a = start.join("/a").unwrap();
        let b = start.join("/b").unwrap();
        let a_deep = start.join("/a/deep").unwrap();
        let b_deep = start.join("/b/deep").unwrap();

        transport.push_response(
            HttpMethod::Get,
            start.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                r#"<html><body><a href="/a">A</a><a href="/b">B</a></body></html>"#,
                None,
                true,
            ),
        );
        transport.push_response(
            HttpMethod::Get,
            a.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                r#"<html><body><a href="/a/deep">deep</a></body></html>"#,
                None,
                true,
            ),
        );
        transport.push_response(
            HttpMethod::Get,
            b.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                r#"<html><body><a href="/b/deep">deep</a></body></html>"#,
                None,
                true,
            ),
        );

        let options = CrawlOptions {
            max_depth: 1,
            max_pages: 100,
            concurrency: 2,
            same_host: true,
            include_subdomains: false,
            use_sitemaps: false,
            request_timeout: StdDuration::from_secs(5),
            max_body_bytes: 1024 * 1024,
            user_agent: None,
        };

        let output = crawl_site(
            transport_arc,
            &start,
            std::slice::from_ref(&start),
            &options,
            "test-agent",
            None,
        )
        .await;

        assert_eq!(output.pages_visited, 3);
        assert_eq!(output.pages_fetched, 3);
        assert_eq!(count_requests(&transport, &a_deep), 0);
        assert_eq!(count_requests(&transport, &b_deep), 0);
        assert!(output.errors.is_empty());
    }

    #[tokio::test]
    async fn test_crawl_site_honors_max_pages_limit() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());

        let start = Url::parse("https://example.test/").unwrap();
        let one = start.join("/one").unwrap();
        let two = start.join("/two").unwrap();
        let three = start.join("/three").unwrap();

        transport.push_response(
            HttpMethod::Get,
            start.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                r#"<html><body><a href="/one">1</a><a href="/two">2</a><a href="/three">3</a></body></html>"#,
                None,
                true,
            ),
        );
        transport.push_response(
            HttpMethod::Get,
            one.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                "<html></html>",
                None,
                true,
            ),
        );
        transport.push_response(
            HttpMethod::Get,
            two.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                "<html></html>",
                None,
                true,
            ),
        );
        transport.push_response(
            HttpMethod::Get,
            three.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                "<html></html>",
                None,
                true,
            ),
        );

        let options = CrawlOptions {
            max_depth: 3,
            max_pages: 2,
            concurrency: 2,
            same_host: true,
            include_subdomains: false,
            use_sitemaps: false,
            request_timeout: StdDuration::from_secs(5),
            max_body_bytes: 1024 * 1024,
            user_agent: None,
        };

        let output = crawl_site(
            transport_arc,
            &start,
            std::slice::from_ref(&start),
            &options,
            "test-agent",
            None,
        )
        .await;

        assert_eq!(output.pages_visited, 2);
        assert_eq!(output.pages_fetched, 2);
        assert_eq!(count_requests(&transport, &start), 1);
    }

    #[tokio::test]
    async fn test_crawl_site_filters_known_github_non_repo_links_when_start_is_repo() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());

        let start = Url::parse("https://github.com/sindresorhus/awesome").unwrap();
        let repo = Url::parse("https://github.com/rust-lang/rust").unwrap();
        let marketplace = Url::parse("https://github.com/marketplace").unwrap();
        let partners = Url::parse("https://github.com/partners/resources").unwrap();

        transport.push_response(
            HttpMethod::Get,
            start.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                r#"<html><body><a href="/rust-lang/rust">repo</a><a href="/marketplace">marketplace</a><a href="/partners/resources">partners</a></body></html>"#,
                None,
                true,
            ),
        );
        transport.push_response(
            HttpMethod::Get,
            repo.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                "<html></html>",
                None,
                true,
            ),
        );

        let options = CrawlOptions {
            max_depth: 1,
            max_pages: 10,
            concurrency: 2,
            same_host: true,
            include_subdomains: false,
            use_sitemaps: false,
            request_timeout: StdDuration::from_secs(5),
            max_body_bytes: 1024 * 1024,
            user_agent: None,
        };

        let output = crawl_site(
            transport_arc,
            &start,
            std::slice::from_ref(&start),
            &options,
            "test-agent",
            None,
        )
        .await;

        assert_eq!(output.pages_visited, 2);
        assert_eq!(output.pages_fetched, 2);
        assert_eq!(count_requests(&transport, &marketplace), 0);
        assert_eq!(count_requests(&transport, &partners), 0);
        assert!(output.errors.is_empty());
    }

    #[tokio::test]
    async fn test_crawl_site_does_not_filter_same_host_on_non_forge_hosts() {
        let transport = MockTransport::new();
        let transport_arc: Arc<dyn HttpTransport> = Arc::new(transport.clone());

        let start = Url::parse("https://example.test/docs").unwrap();
        let linked = Url::parse("https://example.test/docs/install").unwrap();

        transport.push_response(
            HttpMethod::Get,
            start.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                r#"<html><body><a href="/docs/install">install</a></body></html>"#,
                None,
                true,
            ),
        );
        transport.push_response(
            HttpMethod::Get,
            linked.to_string(),
            make_response(
                200,
                Some("text/html; charset=utf-8"),
                "<html></html>",
                None,
                true,
            ),
        );

        let options = CrawlOptions {
            max_depth: 1,
            max_pages: 10,
            concurrency: 2,
            same_host: true,
            include_subdomains: false,
            use_sitemaps: false,
            request_timeout: StdDuration::from_secs(5),
            max_body_bytes: 1024 * 1024,
            user_agent: None,
        };

        let output = crawl_site(
            transport_arc,
            &start,
            std::slice::from_ref(&start),
            &options,
            "test-agent",
            None,
        )
        .await;

        assert_eq!(output.pages_visited, 2);
        assert_eq!(output.pages_fetched, 2);
        assert_eq!(count_requests(&transport, &linked), 1);
    }

    #[test]
    fn test_extract_links_edge_cases() {
        let base = Url::parse("https://example.com/root/index.html").unwrap();
        let body = r##"
            <html>
                <body>
                    <a href="">empty</a>
                    <a href="   ">spaces</a>
                    <a href="#fragment-only">fragment</a>
                    <a href="mailto:hello@example.com">mail</a>
                    <a href="javascript:void(0)">js</a>
                    <a href="ftp://example.com/file">ftp</a>
                    <a href="http://[invalid">invalid</a>
                    <a href=" /docs/page?utm=1#section ">relative</a>
                    <a href="https://github.com/Owner/Repo/issues">repo</a>
                    <a href="//cdn.example.com/lib.js?x=1#frag">protocol-relative</a>
                </body>
            </html>
        "##;

        let (links, repo_links) = extract_links(&base, body);

        let link_strings: Vec<String> = links.into_iter().map(|url| url.to_string()).collect();
        assert_eq!(
            link_strings,
            vec![
                "https://example.com/docs/page".to_string(),
                "https://github.com/Owner/Repo/issues".to_string(),
                "https://cdn.example.com/lib.js".to_string(),
            ]
        );

        assert!(repo_links.iter().any(|repo| {
            repo.host == "github.com"
                && repo.owner == "Owner"
                && repo.name == "Repo"
                && repo.canonical_url == "https://github.com/Owner/Repo"
        }));
    }

    // --- Text URL extraction tests ---

    #[test]
    fn test_extract_text_urls_from_pre_block() {
        let base = Url::parse("https://example.com/page").unwrap();
        let body = r#"<html><body>
            <pre>
            https://github.com/user/plugin1
            https://github.com/user/plugin2
            </pre>
        </body></html>"#;

        let (_, repo_links) = extract_links(&base, body);

        let names: Vec<&str> = repo_links.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"plugin1"), "missing plugin1: {:?}", names);
        assert!(names.contains(&"plugin2"), "missing plugin2: {:?}", names);
    }

    #[test]
    fn test_extract_text_urls_deduplicates_with_href_links() {
        let base = Url::parse("https://example.com/page").unwrap();
        // The same URL appears as both an <a> tag and plain text
        let body = r#"<html><body>
            <a href="https://github.com/owner/repo">link</a>
            <pre>https://github.com/owner/repo</pre>
        </body></html>"#;

        let (links, repo_links) = extract_links(&base, body);

        // Should appear only once due to deduplication
        let github_links: Vec<_> = links
            .iter()
            .filter(|u| u.as_str() == "https://github.com/owner/repo")
            .collect();
        assert_eq!(github_links.len(), 1, "URL should be deduplicated");

        let matching: Vec<_> = repo_links
            .iter()
            .filter(|r| r.owner == "owner" && r.name == "repo")
            .collect();
        assert_eq!(matching.len(), 1, "repo link should be deduplicated");
    }

    #[test]
    fn test_extract_text_urls_stops_at_html_entities() {
        let base = Url::parse("https://example.com/page").unwrap();
        // Simulates HTML attribute with entities: ...neoai.nvim&quot;,&quot;user_id&quot;...
        let body = r#"<html><body>
            <div data-x="https://github.com/owner/neoai.nvim&quot;,&quot;user_id&quot;:null">
            </div>
        </body></html>"#;

        let (_, repo_links) = extract_links(&base, body);

        // Should extract owner/neoai.nvim (stopping before &quot;), NOT neoai.nvim&quot;...
        for repo in &repo_links {
            assert!(
                !repo.name.contains("&quot;"),
                "name should not contain HTML entities: {}",
                repo.name,
            );
            assert!(
                !repo.name.contains("&amp;"),
                "name should not contain &amp;: {}",
                repo.name,
            );
        }
    }

    #[test]
    fn test_extract_text_urls_trims_trailing_punctuation() {
        let base = Url::parse("https://example.com/page").unwrap();
        let body = r#"<html><body>
            Check out https://github.com/owner/repo. It's great!
            Also see https://github.com/other/project, which is nice.
            And (https://github.com/third/lib) is useful.
        </body></html>"#;

        let (_, repo_links) = extract_links(&base, body);

        let names: Vec<&str> = repo_links.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"repo"), "missing 'repo': {:?}", names);
        assert!(names.contains(&"project"), "missing 'project': {:?}", names);
        assert!(names.contains(&"lib"), "missing 'lib': {:?}", names);
    }

    #[test]
    fn test_extract_text_urls_ignores_non_https() {
        let base = Url::parse("https://example.com/page").unwrap();
        let body = r#"<html><body>
            <pre>
            http://github.com/owner/insecure-repo
            ftp://github.com/owner/ftp-repo
            </pre>
        </body></html>"#;

        let (_, repo_links) = extract_links(&base, body);

        // text URL extraction only picks up https:// URLs
        let text_repos: Vec<_> = repo_links
            .iter()
            .filter(|r| r.name == "insecure-repo" || r.name == "ftp-repo")
            .collect();
        assert!(
            text_repos.is_empty(),
            "should not extract non-https text URLs: {:?}",
            text_repos,
        );
    }

    #[test]
    fn test_extract_text_urls_filters_non_repo_hosts() {
        let base = Url::parse("https://example.com/page").unwrap();
        let body = r#"<html><body>
            <pre>
            https://img.shields.io/badge/license-MIT-green
            https://camo.githubusercontent.com/abc123/image.png
            https://github.com/real/repo
            </pre>
        </body></html>"#;

        let (_, repo_links) = extract_links(&base, body);

        // Only the real repo should appear
        assert_eq!(repo_links.len(), 1);
        assert_eq!(repo_links[0].owner, "real");
        assert_eq!(repo_links[0].name, "repo");
    }
}
