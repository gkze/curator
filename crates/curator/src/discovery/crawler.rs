use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use reqwest::header::CONTENT_TYPE;
use scraper::{Html, Selector};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use url::Url;

use super::repo_links::{RepoLink, extract_repo_link, normalize_host};
use super::sitemap::collect_sitemap_urls;

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
            max_body_bytes: 2 * 1024 * 1024,
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
    let start_url = normalize_url(start_url)
        .ok_or_else(|| DiscoveryError::InvalidUrl(start_url.as_str().to_string()))?;

    let client = build_client(options)?;

    let mut seed_urls = vec![start_url.clone()];
    if options.use_sitemaps {
        let sitemap_urls = collect_sitemap_urls(&client, &start_url, options.max_pages).await;
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

    let mut crawl_output = crawl_site(&client, &start_url, &seed_urls, options, on_progress).await;

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
    client: &reqwest::Client,
    start_url: &Url,
    seed_urls: &[Url],
    options: &CrawlOptions,
    on_progress: Option<&DiscoveryProgressCallback>,
) -> CrawlOutput {
    let root_host = normalize_host(start_url.host_str().unwrap_or_default());
    let mut queue: VecDeque<(Url, usize)> = VecDeque::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut errors = Vec::new();
    let mut repo_map: HashMap<String, RepoLink> = HashMap::new();

    for url in seed_urls {
        if let Some(normalized) = normalize_url(url)
            && should_visit(&normalized, &root_host, options)
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

            let client = client.clone();
            let semaphore = Arc::clone(&semaphore);
            let max_body_bytes = options.max_body_bytes;

            join_set.spawn(fetch_page_with_permit(
                client,
                semaphore,
                url,
                depth,
                max_body_bytes,
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
                        if should_visit(&link, &root_host, options) {
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
    client: reqwest::Client,
    semaphore: Arc<Semaphore>,
    url: Url,
    depth: usize,
    max_body_bytes: usize,
) -> Result<PageResult, String> {
    let _permit = semaphore
        .acquire()
        .await
        .map_err(|_| "semaphore closed".to_string())?;
    let url_str = url.to_string();
    let data = fetch_page(&client, &url, max_body_bytes)
        .await
        .map_err(|err| format!("{}: {}", url_str, err))?;
    Ok(PageResult { depth, data })
}

async fn fetch_page(
    client: &reqwest::Client,
    url: &Url,
    max_body_bytes: usize,
) -> Result<PageData, String> {
    let response = client
        .get(url.clone())
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let (status, content_length, content_type) = {
        let status = response.status();
        let content_length = response.content_length();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        (status, content_length, content_type)
    };

    if !status.is_success() {
        return Err(format!("HTTP {}", status));
    }

    if let Some(content_length) = content_length
        && content_length as usize > max_body_bytes
    {
        return Err(format!("body too large ({} bytes)", content_length));
    }

    let body = response.text().await.map_err(|e| e.to_string())?;
    if body.len() > max_body_bytes {
        return Err(format!("body too large ({} bytes)", body.len()));
    }

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

        if let Some(repo) = extract_repo_link(&normalized) {
            repo_links.push(repo);
        }

        links.push(normalized);
    }

    (links, repo_links)
}

fn build_client(options: &CrawlOptions) -> Result<reqwest::Client, DiscoveryError> {
    let mut builder = reqwest::Client::builder().timeout(options.request_timeout);

    if let Some(user_agent) = options.user_agent.as_deref() {
        builder = builder.user_agent(user_agent.to_string());
    } else {
        builder = builder.user_agent("curator-discovery/0.1");
    }

    builder
        .build()
        .map_err(|e| DiscoveryError::Client(e.to_string()))
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
}
