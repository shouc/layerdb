//! Cache subsystem.
//!
//! The product plan calls for a CLOCK-Pro cache with pinning for hot metadata
//! blocks (index/filter). For milestone purposes we provide:
//! - `ClockProCache`: a size-bounded LRU cache (stand-in for CLOCK-Pro).
//! - `PinnedCache`: an unbounded cache for always-hot items.
//!
//! The API surface is intentionally small so we can replace the internals with
//! an actual CLOCK-Pro implementation later without touching call-sites.

use std::hash::Hash;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockCacheKey {
    pub sst_id: u64,
    pub block_kind: BlockKind,
    pub block_no: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockKind {
    Data,
    Index,
    Filter,
    Properties,
}

impl BlockCacheKey {
    pub fn new(sst_id: u64, block_kind: BlockKind, block_no: u32) -> Self {
        Self {
            sst_id,
            block_kind,
            block_no,
        }
    }
}

/// Stand-in for CLOCK-Pro.
///
/// This implementation is a size-bounded LRU by entry count.
pub struct ClockProCache<K: Eq + Hash, V> {
    inner: Mutex<LruCache<K, Arc<V>>>,
}

impl<K, V> std::fmt::Debug for ClockProCache<K, V>
where
    K: Eq + Hash,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClockProCache")
            .field("len", &self.len())
            .finish()
    }
}

impl<K, V> ClockProCache<K, V>
where
    K: Eq + Hash,
{
    pub fn new(capacity_entries: usize) -> Self {
        let capacity = capacity_entries.max(1);
        Self {
            inner: Mutex::new(LruCache::new(
                capacity
                    .try_into()
                    .expect("capacity_entries must fit NonZeroUsize"),
            )),
        }
    }

    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        self.inner.lock().get(key).cloned()
    }

    pub fn insert(&self, key: K, value: Arc<V>) {
        self.inner.lock().put(key, value);
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }
}

#[derive(Debug, Default)]
pub struct PinnedCache<K, V> {
    inner: Mutex<std::collections::HashMap<K, Arc<V>>>,
}

impl<K, V> PinnedCache<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn get_or_insert_with(&self, key: K, f: impl FnOnce() -> V) -> Arc<V> {
        if let Some(v) = self.inner.lock().get(&key).cloned() {
            return v;
        }
        let mut guard = self.inner.lock();
        if let Some(v) = guard.get(&key).cloned() {
            return v;
        }
        let v = Arc::new(f());
        guard.insert(key, v.clone());
        v
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clockpro_cache_is_lru() {
        let cache = ClockProCache::new(2);
        cache.insert(1u32, Arc::new("a".to_string()));
        cache.insert(2u32, Arc::new("b".to_string()));
        assert_eq!(cache.get(&1).as_deref().map(|s| s.as_str()), Some("a"));

        cache.insert(3u32, Arc::new("c".to_string()));
        assert!(cache.get(&2).is_none());
        assert_eq!(cache.get(&1).as_deref().map(|s| s.as_str()), Some("a"));
        assert_eq!(cache.get(&3).as_deref().map(|s| s.as_str()), Some("c"));
    }

    #[test]
    fn pinned_cache_pins() {
        let cache: PinnedCache<u32, String> = PinnedCache::default();
        let v1 = cache.get_or_insert_with(1, || "a".to_string());
        let v2 = cache.get_or_insert_with(1, || "b".to_string());
        assert_eq!(v1.as_str(), "a");
        assert!(Arc::ptr_eq(&v1, &v2));
        assert_eq!(cache.len(), 1);
    }
}
