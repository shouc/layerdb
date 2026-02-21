//! Async IO abstractions.
//!
//! Milestone 3 introduces a thin execution layer that can later swap to native
//! `io_uring` while keeping call-sites stable. For now it is backed by `tokio`
//! file APIs and bounded by a semaphore to model queue depth.

use std::io::{IoSlice, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[cfg(not(unix))]
use std::io::{Read, Seek};

#[cfg(all(feature = "native-uring", target_os = "linux"))]
pub(crate) mod native_uring;
mod pool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackend {
    Tokio,
    Blocking,
    /// macOS event-loop backend.
    ///
    /// On macOS this maps to the runtime's kqueue-backed async path.
    /// On non-macOS platforms it falls back to `Blocking`.
    Kqueue,
    Uring,
}

pub use pool::{BufPool, BufPoolStats, FileRegistry, PooledBuf};

impl Default for IoBackend {
    fn default() -> Self {
        #[cfg(target_os = "macos")]
        {
            Self::Kqueue
        }
        #[cfg(not(target_os = "macos"))]
        {
            Self::Uring
        }
    }
}

#[derive(Debug, Clone)]
pub struct UringExecutor {
    max_in_flight: usize,
    permits: Arc<Semaphore>,
    backend: IoBackend,

    #[cfg(all(feature = "native-uring", target_os = "linux"))]
    native: Option<Arc<native_uring::NativeUring>>,
}

impl Default for UringExecutor {
    fn default() -> Self {
        Self::new(256)
    }
}

impl UringExecutor {
    pub fn new(max_in_flight: usize) -> Self {
        // Prefer platform default backend (Linux: io_uring, macOS: kqueue runtime path).
        Self::with_backend(max_in_flight, IoBackend::default())
    }

    pub fn with_backend(max_in_flight: usize, backend: IoBackend) -> Self {
        assert!(max_in_flight > 0, "max_in_flight must be > 0");

        let backend = normalize_backend(backend);

        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        let native = match backend {
            IoBackend::Uring => native_uring::NativeUring::new(max_in_flight)
                .ok()
                .map(Arc::new),
            _ => None,
        };

        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        let backend = if backend == IoBackend::Uring && native.is_none() {
            IoBackend::Blocking
        } else {
            backend
        };

        Self {
            max_in_flight,
            permits: Arc::new(Semaphore::new(max_in_flight)),
            backend,

            #[cfg(all(feature = "native-uring", target_os = "linux"))]
            native,
        }
    }

    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    pub fn backend(&self) -> IoBackend {
        self.backend
    }

    #[cfg(all(feature = "native-uring", target_os = "linux"))]
    pub(crate) fn native_uring(&self) -> Option<Arc<native_uring::NativeUring>> {
        self.native.clone()
    }

    pub fn supports_native_uring() -> bool {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        {
            true
        }

        #[cfg(not(all(feature = "native-uring", target_os = "linux")))]
        {
            false
        }
    }

    pub fn supports_kqueue() -> bool {
        cfg!(target_os = "macos")
    }

    pub(crate) fn register_file_blocking(&self, file: &std::fs::File) -> Option<u32> {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if self.backend == IoBackend::Uring {
            if let Some(native) = &self.native {
                return native.register_file(file);
            }
        }

        let _ = file;
        None
    }

    pub(crate) fn unregister_file_blocking(&self, fixed_fd: u32) {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if self.backend == IoBackend::Uring {
            if let Some(native) = &self.native {
                native.unregister_file(fixed_fd);
            }
        }

        let _ = fixed_fd;
    }

    async fn acquire_permit(&self) -> anyhow::Result<OwnedSemaphorePermit> {
        self.permits
            .clone()
            .acquire_owned()
            .await
            .context("io executor is closed")
    }

    pub async fn append(&self, path: impl AsRef<Path>, data: &[u8]) -> anyhow::Result<u64> {
        let _permit = self.acquire_permit().await?;
        if matches!(self.backend, IoBackend::Blocking | IoBackend::Kqueue) {
            let path = path.as_ref().to_path_buf();
            let data = data.to_vec();
            let this = self.clone();
            return tokio::task::spawn_blocking(move || this.append_blocking(path, &data))
                .await
                .context("join append task")?;
        }
        let path = path.as_ref();
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .await
            .with_context(|| format!("open for append: {}", path.display()))?;
        let offset = file
            .metadata()
            .await
            .with_context(|| format!("metadata: {}", path.display()))?
            .len();
        file.write_all(data)
            .await
            .with_context(|| format!("append write: {}", path.display()))?;
        Ok(offset)
    }

    pub async fn append_many(
        &self,
        path: impl AsRef<Path>,
        chunks: Vec<Vec<u8>>,
    ) -> anyhow::Result<u64> {
        let _permit = self.acquire_permit().await?;
        let path = path.as_ref().to_path_buf();

        tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("open for append_many: {}", path.display()))?;
            let offset = file
                .metadata()
                .with_context(|| format!("metadata: {}", path.display()))?
                .len();

            let mut chunk_idx = 0usize;
            let mut chunk_off = 0usize;

            while chunk_idx < chunks.len() {
                let mut slices = Vec::with_capacity(chunks.len() - chunk_idx);
                slices.push(IoSlice::new(&chunks[chunk_idx][chunk_off..]));
                for chunk in chunks.iter().skip(chunk_idx + 1) {
                    slices.push(IoSlice::new(chunk));
                }

                let written = file
                    .write_vectored(&slices)
                    .with_context(|| format!("append_many write_vectored: {}", path.display()))?;
                if written == 0 {
                    anyhow::bail!("append_many wrote zero bytes: {}", path.display());
                }

                let mut remaining = written;
                while remaining > 0 && chunk_idx < chunks.len() {
                    let available = chunks[chunk_idx].len() - chunk_off;
                    if remaining < available {
                        chunk_off += remaining;
                        remaining = 0;
                    } else {
                        remaining -= available;
                        chunk_idx += 1;
                        chunk_off = 0;
                    }
                }
            }

            Ok(offset)
        })
        .await
        .context("join append_many task")?
    }

    pub async fn write_all_at(
        &self,
        path: impl AsRef<Path>,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        let _permit = self.acquire_permit().await?;
        if matches!(self.backend, IoBackend::Blocking | IoBackend::Kqueue) {
            let path = path.as_ref().to_path_buf();
            let data = data.to_vec();
            let this = self.clone();
            return tokio::task::spawn_blocking(move || {
                this.write_all_at_blocking(path, offset, &data)
            })
            .await
            .context("join write_all_at task")?;
        }
        let path = path.as_ref();
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .await
            .with_context(|| format!("open for write: {}", path.display()))?;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .with_context(|| format!("seek: {}", path.display()))?;
        file.write_all(data)
            .await
            .with_context(|| format!("write: {}", path.display()))?;
        Ok(())
    }

    pub async fn read_exact_at(
        &self,
        path: impl AsRef<Path>,
        offset: u64,
        len: usize,
    ) -> anyhow::Result<Bytes> {
        let _permit = self.acquire_permit().await?;
        if matches!(self.backend, IoBackend::Blocking | IoBackend::Kqueue) {
            let path = path.as_ref().to_path_buf();
            let this = self.clone();
            return tokio::task::spawn_blocking(move || -> anyhow::Result<Bytes> {
                let mut buf = vec![0u8; len];
                this.read_into_at_blocking(path, offset, &mut buf)?;
                Ok(Bytes::from(buf))
            })
            .await
            .context("join read_exact_at task")?;
        }
        let path = path.as_ref();
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .await
            .with_context(|| format!("open for read: {}", path.display()))?;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .with_context(|| format!("seek: {}", path.display()))?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)
            .await
            .with_context(|| format!("read_exact: {}", path.display()))?;
        Ok(Bytes::from(buf))
    }

    pub async fn read_into_at(
        &self,
        path: impl AsRef<Path>,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        let _permit = self.acquire_permit().await?;
        if matches!(self.backend, IoBackend::Blocking | IoBackend::Kqueue) {
            let path = path.as_ref().to_path_buf();
            let this = self.clone();
            let len = buf.len();
            let bytes = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
                let mut out = vec![0u8; len];
                this.read_into_at_blocking(path, offset, &mut out)?;
                Ok(out)
            })
            .await
            .context("join read_into_at task")??;
            buf.copy_from_slice(&bytes);
            return Ok(());
        }
        let path = path.as_ref();
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .await
            .with_context(|| format!("open for read_into_at: {}", path.display()))?;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .with_context(|| format!("seek: {}", path.display()))?;
        file.read_exact(buf)
            .await
            .with_context(|| format!("read_exact: {}", path.display()))?;
        Ok(())
    }

    pub async fn sync_file(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let _permit = self.acquire_permit().await?;
        if matches!(self.backend, IoBackend::Blocking | IoBackend::Kqueue) {
            let path = path.as_ref().to_path_buf();
            let this = self.clone();
            return tokio::task::spawn_blocking(move || this.sync_file_blocking(path))
                .await
                .context("join sync_file task")?;
        }
        let path = path.as_ref();
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .await
            .with_context(|| format!("open for sync: {}", path.display()))?;
        file.sync_data()
            .await
            .with_context(|| format!("sync_data: {}", path.display()))
    }

    pub async fn sync_parent_dir(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let _permit = self.acquire_permit().await?;
        let parent = path
            .as_ref()
            .parent()
            .context("path has no parent for dir sync")?
            .to_path_buf();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let dir = std::fs::File::open(&parent)
                .with_context(|| format!("open dir: {}", parent.display()))?;
            dir.sync_all()
                .with_context(|| format!("sync dir: {}", parent.display()))
        })
        .await
        .context("join sync_parent_dir task")?
    }

    pub fn append_blocking(&self, path: impl AsRef<Path>, data: &[u8]) -> anyhow::Result<u64> {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if self.backend == IoBackend::Uring {
            if let Some(native) = &self.native {
                return native.append(path.as_ref(), data);
            }
        }

        let path = path.as_ref();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open for append: {}", path.display()))?;
        let offset = file
            .metadata()
            .with_context(|| format!("metadata: {}", path.display()))?
            .len();
        file.write_all(data)
            .with_context(|| format!("append write: {}", path.display()))?;
        Ok(offset)
    }

    pub fn append_many_blocking(
        &self,
        path: impl AsRef<Path>,
        chunks: &[Vec<u8>],
    ) -> anyhow::Result<u64> {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if self.backend == IoBackend::Uring {
            if let Some(native) = &self.native {
                return native.append_many(path.as_ref(), chunks);
            }
        }

        let path = path.as_ref();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open for append_many: {}", path.display()))?;
        let offset = file
            .metadata()
            .with_context(|| format!("metadata: {}", path.display()))?
            .len();

        let mut chunk_idx = 0usize;
        let mut chunk_off = 0usize;

        while chunk_idx < chunks.len() {
            let mut slices = Vec::with_capacity(chunks.len() - chunk_idx);
            slices.push(IoSlice::new(&chunks[chunk_idx][chunk_off..]));
            for chunk in chunks.iter().skip(chunk_idx + 1) {
                slices.push(IoSlice::new(chunk));
            }

            let written = file
                .write_vectored(&slices)
                .with_context(|| format!("append_many write_vectored: {}", path.display()))?;
            if written == 0 {
                anyhow::bail!("append_many wrote zero bytes: {}", path.display());
            }

            let mut remaining = written;
            while remaining > 0 && chunk_idx < chunks.len() {
                let available = chunks[chunk_idx].len() - chunk_off;
                if remaining < available {
                    chunk_off += remaining;
                    remaining = 0;
                } else {
                    remaining -= available;
                    chunk_idx += 1;
                    chunk_off = 0;
                }
            }
        }

        Ok(offset)
    }

    pub fn write_all_at_blocking(
        &self,
        path: impl AsRef<Path>,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        let path = path.as_ref();
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("open for write: {}", path.display()))?;
        self.write_all_at_file_blocking(&file, offset, data)
            .with_context(|| format!("write_all_at: {}", path.display()))
    }

    pub fn write_all_at_file_blocking(
        &self,
        file: &std::fs::File,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if self.backend == IoBackend::Uring {
            if let Some(native) = &self.native {
                return native.write_all_at_file(file, offset, data);
            }
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;

            let mut written = 0usize;
            while written < data.len() {
                let n = file.write_at(&data[written..], offset + written as u64)?;
                if n == 0 {
                    anyhow::bail!("write_all_at wrote zero bytes");
                }
                written += n;
            }
            Ok(())
        }

        #[cfg(not(unix))]
        {
            let mut cloned = file.try_clone().context("try_clone file")?;
            cloned
                .seek(std::io::SeekFrom::Start(offset))
                .context("seek")?;
            cloned.write_all(data).context("write")?;
            Ok(())
        }
    }

    pub(crate) fn write_all_at_file_blocking_fixed(
        &self,
        file: &std::fs::File,
        fixed_fd: u32,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if self.backend == IoBackend::Uring {
            if let Some(native) = &self.native {
                return native.write_all_at_file_fixed(file, fixed_fd, offset, data);
            }
        }

        let _ = fixed_fd;
        self.write_all_at_file_blocking(file, offset, data)
    }

    pub(crate) fn write_all_at_file_blocking_batch(
        &self,
        file: &std::fs::File,
        fixed_fd: Option<u32>,
        writes: &[(u64, &[u8])],
    ) -> anyhow::Result<()> {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if self.backend == IoBackend::Uring {
            if let Some(native) = &self.native {
                return native.write_all_at_file_batch(file, fixed_fd, writes);
            }
        }

        if let Some(fixed) = fixed_fd {
            for (offset, data) in writes {
                self.write_all_at_file_blocking_fixed(file, fixed, *offset, data)?;
            }
        } else {
            for (offset, data) in writes {
                self.write_all_at_file_blocking(file, *offset, data)?;
            }
        }

        Ok(())
    }

    pub fn read_into_at_blocking(
        &self,
        path: impl AsRef<Path>,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        let path = path.as_ref();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .with_context(|| format!("open for read_into_at: {}", path.display()))?;
        self.read_into_at_file_blocking(&file, offset, buf)
            .with_context(|| format!("read_into_at: {}", path.display()))
    }

    pub fn read_into_at_file_blocking(
        &self,
        file: &std::fs::File,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if self.backend == IoBackend::Uring {
            if let Some(native) = &self.native {
                return native.read_into_at_file(file, offset, buf);
            }
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;

            let mut read = 0usize;
            while read < buf.len() {
                let n = file.read_at(&mut buf[read..], offset + read as u64)?;
                if n == 0 {
                    anyhow::bail!("read_into_at hit EOF");
                }
                read += n;
            }
            Ok(())
        }

        #[cfg(not(unix))]
        {
            let mut cloned = file.try_clone().context("try_clone file")?;
            cloned
                .seek(std::io::SeekFrom::Start(offset))
                .context("seek")?;
            cloned.read_exact(buf).context("read_exact")?;
            Ok(())
        }
    }

    pub(crate) fn read_into_at_file_blocking_fixed(
        &self,
        file: &std::fs::File,
        fixed_fd: u32,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if self.backend == IoBackend::Uring {
            if let Some(native) = &self.native {
                return native.read_into_at_file_fixed(file, fixed_fd, offset, buf);
            }
        }

        let _ = fixed_fd;
        self.read_into_at_file_blocking(file, offset, buf)
    }

    pub(crate) fn read_into_at_file_fixed_buf_blocking(
        &self,
        file: &std::fs::File,
        fixed_fd: Option<u32>,
        offset: u64,
        buf_index: u16,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if self.backend == IoBackend::Uring {
            if let Some(native) = &self.native {
                return native.read_into_at_file_fixed_buf(file, fixed_fd, offset, buf_index, buf);
            }
        }

        let _ = fixed_fd;
        let _ = buf_index;
        self.read_into_at_file_blocking(file, offset, buf)
    }

    pub fn sync_file_blocking(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = path.as_ref();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("open for sync: {}", path.display()))?;
        self.sync_file_file_blocking(&file)
            .with_context(|| format!("sync_file: {}", path.display()))
    }

    pub fn sync_file_file_blocking(&self, file: &std::fs::File) -> anyhow::Result<()> {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if self.backend == IoBackend::Uring {
            if let Some(native) = &self.native {
                return native.sync_file_file(file);
            }
        }

        file.sync_data().context("sync_data")
    }

    pub(crate) fn sync_file_file_blocking_fixed(
        &self,
        file: &std::fs::File,
        fixed_fd: u32,
    ) -> anyhow::Result<()> {
        #[cfg(all(feature = "native-uring", target_os = "linux"))]
        if self.backend == IoBackend::Uring {
            if let Some(native) = &self.native {
                return native.sync_file_file_fixed(file, fixed_fd);
            }
        }

        let _ = fixed_fd;
        self.sync_file_file_blocking(file)
    }

    pub fn sync_parent_dir_blocking(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let parent = path
            .as_ref()
            .parent()
            .context("path has no parent for dir sync")?;
        let dir = std::fs::File::open(parent)
            .with_context(|| format!("open dir: {}", parent.display()))?;
        dir.sync_all()
            .with_context(|| format!("sync dir: {}", parent.display()))
    }
}

fn normalize_backend(backend: IoBackend) -> IoBackend {
    match backend {
        IoBackend::Tokio => IoBackend::Tokio,
        IoBackend::Blocking => IoBackend::Blocking,
        IoBackend::Kqueue => {
            #[cfg(target_os = "macos")]
            {
                IoBackend::Kqueue
            }

            #[cfg(not(target_os = "macos"))]
            {
                IoBackend::Blocking
            }
        }
        IoBackend::Uring => {
            #[cfg(all(feature = "native-uring", target_os = "linux"))]
            {
                IoBackend::Uring
            }

            #[cfg(not(all(feature = "native-uring", target_os = "linux")))]
            {
                IoBackend::Blocking
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn io_backend_default_is_platform_aware() {
        #[cfg(target_os = "macos")]
        assert_eq!(IoBackend::default(), IoBackend::Kqueue);

        #[cfg(not(target_os = "macos"))]
        assert_eq!(IoBackend::default(), IoBackend::Uring);
    }

    #[test]
    fn kqueue_support_flag_matches_platform() {
        assert_eq!(UringExecutor::supports_kqueue(), cfg!(target_os = "macos"));
    }

    #[tokio::test]
    async fn executor_append_read_and_sync() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("data.bin");
        let io = UringExecutor::new(8);

        let offset0 = io.append(&path, b"hello").await.expect("append0");
        let offset1 = io.append(&path, b"world").await.expect("append1");
        assert_eq!(offset0, 0);
        assert_eq!(offset1, 5);

        io.write_all_at(&path, 5, b" ").await.expect("write_at");
        io.sync_file(&path).await.expect("sync_file");
        io.sync_parent_dir(&path).await.expect("sync_dir");

        let got = io.read_exact_at(&path, 0, 10).await.expect("read");
        assert_eq!(got.as_ref(), b"hello orld");
    }

    #[tokio::test]
    async fn executor_append_many_writes_contiguously() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("data_many.bin");
        let io = UringExecutor::new(8);

        let chunks = vec![b"ab".to_vec(), b"cd".to_vec()];
        let offset0 = io.append_many(&path, chunks).await.expect("append_many");
        let offset1 = io.append(&path, b"ef").await.expect("append");

        assert_eq!(offset0, 0);
        assert_eq!(offset1, 4);

        let got = io.read_exact_at(&path, 0, 6).await.expect("read");
        assert_eq!(got.as_ref(), b"abcdef");
    }

    #[tokio::test]
    async fn executor_append_many_handles_large_chunk_sets() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("data_many_large.bin");
        let io = UringExecutor::new(8);

        let mut expected = Vec::new();
        let mut chunks = Vec::new();
        for i in 0..128u16 {
            let len = 1024 + (i as usize % 31);
            let byte = (i % 251) as u8;
            let chunk = vec![byte; len];
            expected.extend_from_slice(&chunk);
            chunks.push(chunk);
        }

        let offset0 = io.append_many(&path, chunks).await.expect("append_many");
        assert_eq!(offset0, 0);

        let got = io
            .read_exact_at(&path, 0, expected.len())
            .await
            .expect("read");
        assert_eq!(got.as_ref(), expected.as_slice());
    }

    #[test]
    fn buf_pool_reuses_bucketed_buffers() {
        let pool = BufPool::new([16, 64], 8);

        {
            let mut buf = pool.acquire(10);
            assert_eq!(buf.capacity(), 16);
            buf.extend_from_slice(b"abc");
            assert_eq!(buf.len(), 3);
        }

        let before = pool.stats().cached_buffers;
        assert!(before >= 1);

        {
            let buf = pool.acquire(10);
            assert_eq!(buf.capacity(), 16);
        }

        let after = pool.stats().cached_buffers;
        assert!(after >= 1);
    }

    #[test]
    fn file_registry_round_trip() {
        let registry = FileRegistry::default();
        let p = PathBuf::from("/tmp/a.sst");

        let id1 = registry.register(&p);
        let id2 = registry.register(&p);
        assert_eq!(id1, id2);
        assert_eq!(registry.id_for(&p), Some(id1));
        assert_eq!(registry.path_for(id1), Some(p.clone()));
        assert_eq!(registry.len(), 1);

        let removed = registry.unregister_path(&p);
        assert_eq!(removed, Some(id1));
        assert!(registry.is_empty());
    }
}
