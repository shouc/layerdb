use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

#[cfg(all(feature = "native-uring", target_os = "linux"))]
use super::native_uring;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufPoolStats {
    pub buckets: usize,
    pub cached_buffers: usize,
}

#[derive(Debug, Clone)]
pub struct BufPool {
    inner: Arc<BufPoolInner>,
}

#[derive(Debug)]
struct BufPoolInner {
    bucket_sizes: Vec<usize>,
    max_cached_per_bucket: usize,
    free: Mutex<HashMap<usize, Vec<Vec<u8>>>>,

    #[cfg(all(feature = "native-uring", target_os = "linux"))]
    native_uring: Option<Arc<native_uring::NativeUring>>,
}

impl Default for BufPool {
    fn default() -> Self {
        Self::new([4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024], 64)
    }
}

impl BufPool {
    pub fn new(
        bucket_sizes: impl IntoIterator<Item = usize>,
        max_cached_per_bucket: usize,
    ) -> Self {
        let mut sizes: Vec<usize> = bucket_sizes.into_iter().filter(|s| *s > 0).collect();
        sizes.sort_unstable();
        sizes.dedup();

        Self {
            inner: Arc::new(BufPoolInner {
                bucket_sizes: sizes,
                max_cached_per_bucket,
                free: Mutex::new(HashMap::new()),

                #[cfg(all(feature = "native-uring", target_os = "linux"))]
                native_uring: None,
            }),
        }
    }

    #[cfg(all(feature = "native-uring", target_os = "linux"))]
    pub(crate) fn with_native_uring(
        native: Arc<native_uring::NativeUring>,
        bucket_sizes: impl IntoIterator<Item = usize>,
        max_cached_per_bucket: usize,
    ) -> Self {
        let mut pool = Self::new(bucket_sizes, max_cached_per_bucket);
        let inner = Arc::get_mut(&mut pool.inner).expect("new BufPool has unique Arc");
        inner.native_uring = Some(native);
        pool
    }

    pub fn acquire(&self, min_capacity: usize) -> PooledBuf {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if let Some(native) = &self.inner.native_uring {
            if let Some(buf) = native.try_acquire_fixed_buf(min_capacity) {
                return PooledBuf {
                    inner: PooledBufInner::Fixed(buf),
                    bucket_capacity: None,
                    pool: Arc::downgrade(&self.inner),
                };
            }
        }

        let target_cap = self
            .inner
            .bucket_sizes
            .iter()
            .copied()
            .find(|size| *size >= min_capacity);

        if let Some(bucket) = target_cap {
            let mut free = self.inner.free.lock();
            if let Some(list) = free.get_mut(&bucket) {
                if let Some(mut data) = list.pop() {
                    data.clear();
                    return PooledBuf {
                        inner: PooledBufInner::Vec(data),
                        bucket_capacity: Some(bucket),
                        pool: Arc::downgrade(&self.inner),
                    };
                }
            }

            return PooledBuf {
                inner: PooledBufInner::Vec(Vec::with_capacity(bucket)),
                bucket_capacity: Some(bucket),
                pool: Arc::downgrade(&self.inner),
            };
        }

        PooledBuf {
            inner: PooledBufInner::Vec(Vec::with_capacity(min_capacity)),
            bucket_capacity: None,
            pool: Arc::downgrade(&self.inner),
        }
    }

    pub fn stats(&self) -> BufPoolStats {
        let free = self.inner.free.lock();
        let cached_buffers = free.values().map(|v| v.len()).sum();
        BufPoolStats {
            buckets: self.inner.bucket_sizes.len(),
            cached_buffers,
        }
    }
}

#[derive(Debug)]
pub struct PooledBuf {
    inner: PooledBufInner,
    bucket_capacity: Option<usize>,
    pool: Weak<BufPoolInner>,
}

#[derive(Debug)]
enum PooledBufInner {
    Vec(Vec<u8>),

    #[cfg(all(feature = "native-uring", target_os = "linux"))]
    Fixed(native_uring::FixedBuf),
}

impl PooledBufInner {
    fn len(&self) -> usize {
        match self {
            PooledBufInner::Vec(v) => v.len(),

            #[cfg(all(feature = "native-uring", target_os = "linux"))]
            PooledBufInner::Fixed(b) => b.len(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            PooledBufInner::Vec(v) => v.capacity(),

            #[cfg(all(feature = "native-uring", target_os = "linux"))]
            PooledBufInner::Fixed(b) => b.capacity(),
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            PooledBufInner::Vec(v) => v.as_slice(),

            #[cfg(all(feature = "native-uring", target_os = "linux"))]
            PooledBufInner::Fixed(b) => b.as_slice(),
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            PooledBufInner::Vec(v) => v.as_mut_slice(),

            #[cfg(all(feature = "native-uring", target_os = "linux"))]
            PooledBufInner::Fixed(b) => b.as_mut_slice(),
        }
    }

    fn clear(&mut self) {
        match self {
            PooledBufInner::Vec(v) => v.clear(),

            #[cfg(all(feature = "native-uring", target_os = "linux"))]
            PooledBufInner::Fixed(b) => b.clear(),
        }
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        match self {
            PooledBufInner::Vec(v) => v.extend_from_slice(bytes),

            #[cfg(all(feature = "native-uring", target_os = "linux"))]
            PooledBufInner::Fixed(b) => {
                let old_len = b.len();
                let new_len = old_len + bytes.len();
                assert!(new_len <= b.capacity(), "fixed buf overflow");
                b.resize(new_len, 0);
                b.as_mut_slice()[old_len..new_len].copy_from_slice(bytes);
            }
        }
    }
}

impl PooledBuf {
    pub fn as_slice(&self) -> &[u8] {
        self.inner.as_slice()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.inner.as_mut_slice()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.inner.extend_from_slice(bytes);
    }

    pub fn resize(&mut self, new_len: usize, value: u8) {
        match &mut self.inner {
            PooledBufInner::Vec(v) => v.resize(new_len, value),

            #[cfg(all(feature = "native-uring", target_os = "linux"))]
            PooledBufInner::Fixed(b) => b.resize(new_len, value),
        }
    }

    pub(crate) fn fixed_buf_index(&self) -> Option<u16> {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        {
            match &self.inner {
                PooledBufInner::Fixed(b) => Some(b.buf_index()),
                _ => None,
            }
        }

        #[cfg(not(all(feature = "native-uring", target_os = "linux")))]
        {
            None
        }
    }

    pub fn into_vec(mut self) -> Vec<u8> {
        self.bucket_capacity = None;
        match &mut self.inner {
            PooledBufInner::Vec(v) => std::mem::take(v),

            #[cfg(all(feature = "native-uring", target_os = "linux"))]
            PooledBufInner::Fixed(b) => b.as_slice().to_vec(),
        }
    }
}

impl std::ops::Deref for PooledBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner.as_slice()
    }
}

impl std::ops::DerefMut for PooledBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut_slice()
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        let Some(bucket) = self.bucket_capacity else {
            return;
        };

        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        let vec = match &mut self.inner {
            PooledBufInner::Vec(vec) => vec,
            PooledBufInner::Fixed(_) => return,
        };
        #[cfg(not(all(feature = "native-uring", target_os = "linux")))]
        let PooledBufInner::Vec(vec) = &mut self.inner;
        if vec.capacity() != bucket {
            return;
        }
        let Some(inner) = self.pool.upgrade() else {
            return;
        };

        vec.clear();
        let mut free = inner.free.lock();
        let list = free.entry(bucket).or_default();
        if list.len() < inner.max_cached_per_bucket {
            list.push(std::mem::take(vec));
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileRegistry {
    inner: Arc<FileRegistryInner>,
}

#[derive(Debug)]
struct FileRegistryInner {
    next_id: AtomicU64,
    by_path: RwLock<HashMap<PathBuf, u64>>,
    by_id: RwLock<HashMap<u64, PathBuf>>,
}

impl Default for FileRegistry {
    fn default() -> Self {
        Self {
            inner: Arc::new(FileRegistryInner {
                next_id: AtomicU64::new(1),
                by_path: RwLock::new(HashMap::new()),
                by_id: RwLock::new(HashMap::new()),
            }),
        }
    }
}

impl FileRegistry {
    pub fn register(&self, path: impl Into<PathBuf>) -> u64 {
        let path = path.into();

        if let Some(id) = self.inner.by_path.read().get(&path).copied() {
            return id;
        }

        let mut by_path = self.inner.by_path.write();
        if let Some(id) = by_path.get(&path).copied() {
            return id;
        }

        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        by_path.insert(path.clone(), id);
        self.inner.by_id.write().insert(id, path);
        id
    }

    pub fn id_for(&self, path: impl AsRef<Path>) -> Option<u64> {
        self.inner.by_path.read().get(path.as_ref()).copied()
    }

    pub fn path_for(&self, file_id: u64) -> Option<PathBuf> {
        self.inner.by_id.read().get(&file_id).cloned()
    }

    pub fn unregister_path(&self, path: impl AsRef<Path>) -> Option<u64> {
        let mut by_path = self.inner.by_path.write();
        let id = by_path.remove(path.as_ref())?;
        self.inner.by_id.write().remove(&id);
        Some(id)
    }

    pub fn len(&self) -> usize {
        self.inner.by_path.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
