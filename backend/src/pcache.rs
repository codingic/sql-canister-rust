//! Page cache module
//!
//! Manages in-memory cache of database pages.

use crate::types::Pgno;
use std::collections::HashMap;

/// Page cache entry
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// Page data
    pub data: Vec<u8>,
    /// Is dirty
    pub dirty: bool,
    /// Reference count
    pub ref_count: u32,
}

/// Page cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum cache size in pages
    pub max_pages: usize,
    /// Page size in bytes
    pub page_size: u32,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            max_pages: 2000,
            page_size: 4096,
        }
    }
}

/// Page cache
pub struct PageCache {
    /// Configuration
    config: CacheConfig,
    /// Cached pages
    cache: HashMap<Pgno, CacheEntry>,
    /// LRU order
    lru: Vec<Pgno>,
}

impl PageCache {
    /// Create a new page cache
    pub fn new(config: CacheConfig) -> Self {
        PageCache {
            config,
            cache: HashMap::new(),
            lru: Vec::new(),
        }
    }

    /// Get a page from cache
    pub fn get(&self, pgno: Pgno) -> Option<&CacheEntry> {
        self.cache.get(&pgno)
    }

    /// Get a mutable page from cache
    pub fn get_mut(&mut self, pgno: Pgno) -> Option<&mut CacheEntry> {
        if self.cache.contains_key(&pgno) {
            // Move to front of LRU
            self.lru.retain(|&p| p != pgno);
            self.lru.push(pgno);
        }
        self.cache.get_mut(&pgno)
    }

    /// Add a page to cache
    pub fn insert(&mut self, pgno: Pgno, data: Vec<u8>) {
        // Evict if necessary
        while self.cache.len() >= self.config.max_pages {
            if let Some(old_pgno) = self.lru.first().copied() {
                self.lru.remove(0);
                self.cache.remove(&old_pgno);
            } else {
                break;
            }
        }

        self.cache.insert(
            pgno,
            CacheEntry {
                data,
                dirty: false,
                ref_count: 0,
            },
        );
        self.lru.push(pgno);
    }

    /// Remove a page from cache
    pub fn remove(&mut self, pgno: Pgno) -> Option<CacheEntry> {
        self.lru.retain(|&p| p != pgno);
        self.cache.remove(&pgno)
    }

    /// Check if page is cached
    pub fn contains(&self, pgno: Pgno) -> bool {
        self.cache.contains_key(&pgno)
    }

    /// Get cache size
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.cache.clear();
        self.lru.clear();
    }

    /// Get dirty pages
    pub fn dirty_pages(&self) -> Vec<Pgno> {
        self.cache
            .iter()
            .filter(|(_, entry)| entry.dirty)
            .map(|(&pgno, _)| pgno)
            .collect()
    }

    /// Mark page as dirty
    pub fn mark_dirty(&mut self, pgno: Pgno) {
        if let Some(entry) = self.cache.get_mut(&pgno) {
            entry.dirty = true;
        }
    }

    /// Mark page as clean
    pub fn mark_clean(&mut self, pgno: Pgno) {
        if let Some(entry) = self.cache.get_mut(&pgno) {
            entry.dirty = false;
        }
    }

    /// Increment reference count
    pub fn increment_ref(&mut self, pgno: Pgno) {
        if let Some(entry) = self.cache.get_mut(&pgno) {
            entry.ref_count += 1;
        }
    }

    /// Decrement reference count
    pub fn decrement_ref(&mut self, pgno: Pgno) {
        if let Some(entry) = self.cache.get_mut(&pgno) {
            if entry.ref_count > 0 {
                entry.ref_count -= 1;
            }
        }
    }
}

impl Default for PageCache {
    fn default() -> Self {
        Self::new(CacheConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_new() {
        let cache = PageCache::default();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_insert() {
        let mut cache = PageCache::default();
        cache.insert(1, vec![0u8; 4096]);
        assert_eq!(cache.len(), 1);
        assert!(cache.contains(1));
    }

    #[test]
    fn test_cache_remove() {
        let mut cache = PageCache::default();
        cache.insert(1, vec![0u8; 4096]);
        let entry = cache.remove(1);
        assert!(entry.is_some());
        assert!(!cache.contains(1));
    }

    #[test]
    fn test_cache_dirty() {
        let mut cache = PageCache::default();
        cache.insert(1, vec![0u8; 4096]);
        cache.mark_dirty(1);

        let dirty = cache.dirty_pages();
        assert_eq!(dirty.len(), 1);
        assert!(dirty.contains(&1));

        cache.mark_clean(1);
        let dirty = cache.dirty_pages();
        assert!(dirty.is_empty());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let config = CacheConfig {
            max_pages: 2,
            page_size: 4096,
        };
        let mut cache = PageCache::new(config);

        cache.insert(1, vec![0u8; 4096]);
        cache.insert(2, vec![0u8; 4096]);
        cache.insert(3, vec![0u8; 4096]);

        // First page should be evicted
        assert!(!cache.contains(1));
        assert!(cache.contains(2));
        assert!(cache.contains(3));
    }
}
