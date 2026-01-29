//! Shared types for conditional HTTP fetching with ETag caching.
//!
//! This module provides types used across platform clients for implementing
//! efficient ETag-based conditional requests. When a client has a cached ETag,
//! it can send an `If-None-Match` header and receive a 304 Not Modified response
//! if the data hasn't changed, avoiding unnecessary data transfer.

/// Pagination information from an API response.
///
/// Different platforms provide pagination info in different ways:
/// - GitHub: Link header with `rel="last"` and `rel="next"` links
/// - GitLab: `x-total-pages` header
/// - Gitea: Link header similar to GitHub
///
/// This struct provides a unified representation.
#[derive(Debug, Clone, Default)]
pub struct PaginationInfo {
    /// Total number of pages (if known).
    pub total_pages: Option<u32>,
    /// Next page number (if there are more pages).
    pub next_page: Option<u32>,
}

impl PaginationInfo {
    /// Create pagination info with just total pages.
    #[inline]
    pub fn with_total(total_pages: u32) -> Self {
        Self {
            total_pages: Some(total_pages),
            next_page: None,
        }
    }

    /// Create pagination info from GitLab's total pages header.
    #[inline]
    pub fn from_total_pages(total_pages: Option<i32>) -> Self {
        Self {
            total_pages: total_pages.and_then(|p| u32::try_from(p).ok()),
            next_page: None,
        }
    }

    /// Returns true if there are more pages to fetch.
    #[inline]
    pub fn has_more(&self) -> bool {
        self.next_page.is_some()
    }
}

/// Result of a conditional GET request using ETag caching.
///
/// When making a conditional request with an `If-None-Match` header containing
/// a cached ETag, the server may return:
/// - 304 Not Modified: The cached data is still valid
/// - 200 OK: New data with an optional new ETag
#[derive(Debug, Clone)]
pub enum FetchResult<T> {
    /// Server returned 304 Not Modified — cached data is still valid.
    NotModified,
    /// Server returned new data with an optional ETag for future caching.
    Fetched {
        /// The fetched data.
        data: T,
        /// ETag for caching (if provided by server).
        etag: Option<String>,
        /// Pagination information (if applicable).
        pagination: PaginationInfo,
    },
}

impl<T> FetchResult<T> {
    /// Returns true if the result indicates not modified (cache hit).
    #[inline]
    pub fn is_not_modified(&self) -> bool {
        matches!(self, FetchResult::NotModified)
    }

    /// Extract data if fetched, returning None if not modified.
    pub fn into_data(self) -> Option<T> {
        match self {
            FetchResult::NotModified => None,
            FetchResult::Fetched { data, .. } => Some(data),
        }
    }

    /// Get ETag if fetched, None otherwise.
    pub fn etag(&self) -> Option<&str> {
        match self {
            FetchResult::NotModified => None,
            FetchResult::Fetched { etag, .. } => etag.as_deref(),
        }
    }

    /// Get pagination info if fetched, None otherwise.
    pub fn pagination(&self) -> Option<&PaginationInfo> {
        match self {
            FetchResult::NotModified => None,
            FetchResult::Fetched { pagination, .. } => Some(pagination),
        }
    }

    /// Get total pages if known.
    pub fn total_pages(&self) -> Option<u32> {
        self.pagination().and_then(|p| p.total_pages)
    }
}

/// Statistics about cache usage during a paginated fetch operation.
///
/// Tracks how many pages were cache hits (304 Not Modified) vs. freshly fetched.
/// This is useful for monitoring cache effectiveness and debugging.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of pages that returned 304 Not Modified (cache hits).
    pub cache_hits: u32,
    /// Number of pages that were freshly fetched.
    pub pages_fetched: u32,
}

impl CacheStats {
    /// Create new stats with the given values.
    #[inline]
    pub fn new(cache_hits: u32, pages_fetched: u32) -> Self {
        Self {
            cache_hits,
            pages_fetched,
        }
    }

    /// Returns true if all requests were cache hits (no fresh fetches needed).
    #[inline]
    pub fn all_cached(&self) -> bool {
        self.cache_hits > 0 && self.pages_fetched == 0
    }

    /// Returns the cache hit ratio (0.0 to 1.0).
    ///
    /// Returns 0.0 if no requests were made.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.pages_fetched;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    /// Record a cache hit (304 Not Modified).
    #[inline]
    pub fn record_hit(&mut self) {
        self.cache_hits += 1;
    }

    /// Record a fresh fetch (200 OK with data).
    #[inline]
    pub fn record_fetch(&mut self) {
        self.pages_fetched += 1;
    }

    /// Merge stats from another CacheStats.
    #[inline]
    pub fn merge(&mut self, other: &CacheStats) {
        self.cache_hits += other.cache_hits;
        self.pages_fetched += other.pages_fetched;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagination_info_default() {
        let info = PaginationInfo::default();
        assert_eq!(info.total_pages, None);
        assert_eq!(info.next_page, None);
        assert!(!info.has_more());
    }

    #[test]
    fn test_pagination_info_with_total() {
        let info = PaginationInfo::with_total(5);
        assert_eq!(info.total_pages, Some(5));
        assert_eq!(info.next_page, None);
    }

    #[test]
    fn test_pagination_info_from_total_pages() {
        assert_eq!(
            PaginationInfo::from_total_pages(Some(10)).total_pages,
            Some(10)
        );
        assert_eq!(PaginationInfo::from_total_pages(None).total_pages, None);
        assert_eq!(PaginationInfo::from_total_pages(Some(-1)).total_pages, None);
        // Negative not valid
    }

    #[test]
    fn test_fetch_result_not_modified() {
        let result: FetchResult<String> = FetchResult::NotModified;
        assert!(result.is_not_modified());
        assert!(result.etag().is_none());
        assert!(result.pagination().is_none());
        assert!(result.into_data().is_none());
    }

    #[test]
    fn test_fetch_result_fetched() {
        let result = FetchResult::Fetched {
            data: "hello".to_string(),
            etag: Some("abc123".to_string()),
            pagination: PaginationInfo::with_total(3),
        };
        assert!(!result.is_not_modified());
        assert_eq!(result.etag(), Some("abc123"));
        assert_eq!(result.total_pages(), Some(3));
        assert_eq!(result.into_data(), Some("hello".to_string()));
    }

    #[test]
    fn test_fetch_result_fetched_no_etag() {
        let result = FetchResult::Fetched {
            data: vec![1, 2, 3],
            etag: None,
            pagination: PaginationInfo::default(),
        };
        assert!(result.etag().is_none());
        assert_eq!(result.total_pages(), None);
    }

    #[test]
    fn test_cache_stats_default() {
        let stats = CacheStats::default();
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.pages_fetched, 0);
        assert!(!stats.all_cached());
        assert_eq!(stats.hit_ratio(), 0.0);
    }

    #[test]
    fn test_cache_stats_all_cached() {
        let stats = CacheStats::new(5, 0);
        assert!(stats.all_cached());
        assert_eq!(stats.hit_ratio(), 1.0);
    }

    #[test]
    fn test_cache_stats_no_cache_hits() {
        let stats = CacheStats::new(0, 5);
        assert!(!stats.all_cached());
        assert_eq!(stats.hit_ratio(), 0.0);
    }

    #[test]
    fn test_cache_stats_mixed() {
        let stats = CacheStats::new(3, 2);
        assert!(!stats.all_cached());
        assert!((stats.hit_ratio() - 0.6).abs() < 0.001);
    }

    #[test]
    fn test_cache_stats_record() {
        let mut stats = CacheStats::default();
        stats.record_hit();
        stats.record_hit();
        stats.record_fetch();
        assert_eq!(stats.cache_hits, 2);
        assert_eq!(stats.pages_fetched, 1);
    }

    #[test]
    fn test_cache_stats_merge() {
        let mut stats1 = CacheStats::new(2, 1);
        let stats2 = CacheStats::new(3, 2);
        stats1.merge(&stats2);
        assert_eq!(stats1.cache_hits, 5);
        assert_eq!(stats1.pages_fetched, 3);
    }
}
