//! Shared helpers for concurrent paginated fetches.

use std::future::Future;

use futures::stream::{self, StreamExt};

/// Collect page fetch results concurrently without preserving page order.
///
/// This is useful for providers that know their total page count and want to
/// fan out page fetches with bounded concurrency.
pub async fn collect_pages_unordered<T, I, F, Fut>(
    pages: I,
    concurrency: usize,
    fetch_page: F,
) -> Vec<T>
where
    I: IntoIterator<Item = u32>,
    F: Fn(u32) -> Fut,
    Fut: Future<Output = T>,
{
    let limit = concurrency.max(1);
    stream::iter(pages.into_iter().map(fetch_page))
        .buffer_unordered(limit)
        .collect()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn collect_pages_unordered_collects_all_pages() {
        let results = collect_pages_unordered(1..=5, 3, |page| async move { page }).await;

        assert_eq!(results.len(), 5);
        assert!(results.contains(&1));
        assert!(results.contains(&5));
    }

    #[tokio::test]
    async fn collect_pages_unordered_treats_zero_concurrency_as_one() {
        let results = collect_pages_unordered(1..=3, 0, |page| async move { page }).await;

        assert_eq!(results.len(), 3);
        assert!(results.contains(&1));
        assert!(results.contains(&2));
        assert!(results.contains(&3));
    }
}
