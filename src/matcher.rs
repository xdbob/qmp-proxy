// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::RwLock;

pub struct Matcher<T> {
    counter: AtomicUsize,
    map: RwLock<HashMap<usize, T>>,
}

impl<T> Matcher<T> {
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
            map: RwLock::new(HashMap::new()),
        }
    }

    pub async fn insert(&self, val: T) -> usize {
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        self.map.write().await.insert(id, val);
        id
    }

    pub async fn pop(&self, id: usize) -> Option<T> {
        self.map.write().await.remove(&id)
    }

    pub async fn remove_if<F>(&self, predicate: F) -> Vec<T>
    where
        F: Fn(usize, &T) -> bool,
    {
        self.map
            .write()
            .await
            .extract_if(|&id, val| predicate(id, val))
            .map(|(_id, val)| val)
            .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ids_are_unique() {
        let matcher = Matcher::new();
        let a = matcher.insert(1).await;
        let b = matcher.insert(2).await;
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn pop_returns_and_removes() {
        let matcher = Matcher::new();
        let id = matcher.insert("foo").await;
        assert_eq!(matcher.pop(id).await, Some("foo"));
        assert_eq!(matcher.pop(id).await, None);
    }

    #[tokio::test]
    async fn remove_if_returns_dropped_entries() {
        let matcher = Matcher::new();
        let keep = matcher.insert(1).await;
        let drop = matcher.insert(2).await;
        let removed = matcher.remove_if(|_, val| *val == 2).await;
        assert_eq!(removed, vec![2]);
        assert!(matcher.pop(keep).await.is_some());
        assert_eq!(matcher.pop(drop).await, None);
    }

    #[tokio::test]
    async fn remove_if_with_no_match_is_empty() {
        let matcher = Matcher::new();
        let id = matcher.insert(1).await;
        assert!(matcher.remove_if(|_, _| false).await.is_empty());
        assert!(matcher.pop(id).await.is_some());
    }
}
