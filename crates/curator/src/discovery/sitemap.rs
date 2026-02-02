use std::collections::{HashSet, VecDeque};

use quick_xml::Reader;
use quick_xml::events::Event;
use url::Url;

pub async fn collect_sitemap_urls(
    client: &reqwest::Client,
    start_url: &Url,
    max_urls: usize,
) -> Vec<Url> {
    let mut sitemap_urls = robots_sitemaps(client, start_url).await;

    if sitemap_urls.is_empty()
        && let Ok(default_sitemap) = start_url.join("/sitemap.xml")
    {
        sitemap_urls.push(default_sitemap);
    }

    let mut queue: VecDeque<Url> = sitemap_urls.into_iter().collect();
    let mut seen_sitemaps: HashSet<String> = HashSet::new();
    let mut urls: Vec<Url> = Vec::new();

    while let Some(sitemap_url) = queue.pop_front() {
        if urls.len() >= max_urls {
            break;
        }

        let key = sitemap_url.to_string();
        if !seen_sitemaps.insert(key) {
            continue;
        }

        let Ok(response) = client.get(sitemap_url.clone()).send().await else {
            continue;
        };

        if !response.status().is_success() {
            continue;
        }

        let Ok(body) = response.text().await else {
            continue;
        };

        match parse_sitemap(&body) {
            SitemapParse::Index(locations) => {
                for loc in locations {
                    if let Ok(url) = Url::parse(&loc) {
                        queue.push_back(url);
                    }
                }
            }
            SitemapParse::UrlSet(locations) => {
                for loc in locations {
                    if urls.len() >= max_urls {
                        break;
                    }
                    if let Ok(url) = Url::parse(&loc) {
                        urls.push(url);
                    }
                }
            }
        }
    }

    urls
}

async fn robots_sitemaps(client: &reqwest::Client, start_url: &Url) -> Vec<Url> {
    let Ok(robots_url) = start_url.join("/robots.txt") else {
        return Vec::new();
    };

    let Ok(response) = client.get(robots_url).send().await else {
        return Vec::new();
    };

    if !response.status().is_success() {
        return Vec::new();
    }

    let Ok(body) = response.text().await else {
        return Vec::new();
    };

    body.lines()
        .filter_map(|line| line.split_once(':'))
        .filter_map(|(key, value)| {
            if key.trim().eq_ignore_ascii_case("sitemap") {
                let loc = value.trim();
                Url::parse(loc).ok()
            } else {
                None
            }
        })
        .collect()
}

enum SitemapParse {
    Index(Vec<String>),
    UrlSet(Vec<String>),
}

fn parse_sitemap(xml: &str) -> SitemapParse {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut root: Option<Vec<u8>> = None;
    let mut locs: Vec<String> = Vec::new();
    let mut in_loc = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) => {
                if root.is_none() {
                    root = Some(event.name().as_ref().to_vec());
                }
                if event.name().as_ref().eq_ignore_ascii_case(b"loc") {
                    in_loc = true;
                }
            }
            Ok(Event::End(event)) => {
                if event.name().as_ref().eq_ignore_ascii_case(b"loc") {
                    in_loc = false;
                }
            }
            Ok(Event::Text(text)) => {
                if in_loc && let Ok(value) = text.unescape() {
                    locs.push(value.into_owned());
                }
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
    }

    let is_index = root
        .as_ref()
        .map(|name| name.eq_ignore_ascii_case(b"sitemapindex"))
        .unwrap_or(false);

    if is_index {
        SitemapParse::Index(locs)
    } else {
        SitemapParse::UrlSet(locs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_urlset() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/</loc></url>
  <url><loc>https://example.com/docs</loc></url>
</urlset>
"#;

        match parse_sitemap(xml) {
            SitemapParse::UrlSet(urls) => {
                assert_eq!(urls.len(), 2);
                assert_eq!(urls[0], "https://example.com/");
                assert_eq!(urls[1], "https://example.com/docs");
            }
            SitemapParse::Index(_) => panic!("expected urlset"),
        }
    }

    #[test]
    fn test_parse_sitemap_index() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://example.com/sitemap-1.xml</loc></sitemap>
  <sitemap><loc>https://example.com/sitemap-2.xml</loc></sitemap>
</sitemapindex>
"#;

        match parse_sitemap(xml) {
            SitemapParse::Index(urls) => {
                assert_eq!(urls.len(), 2);
                assert_eq!(urls[0], "https://example.com/sitemap-1.xml");
                assert_eq!(urls[1], "https://example.com/sitemap-2.xml");
            }
            SitemapParse::UrlSet(_) => panic!("expected sitemap index"),
        }
    }
}
