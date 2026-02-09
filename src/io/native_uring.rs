use std::io::{IoSlice, Read, Seek, Write};
use std::path::Path;

use anyhow::Context;

pub struct NativeUring {
    _max_in_flight: usize,
    #[cfg(feature = "native-uring")]
    _probe: io_uring::IoUring,
}

impl std::fmt::Debug for NativeUring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeUring")
            .field("max_in_flight", &self._max_in_flight)
            .finish_non_exhaustive()
    }
}

impl NativeUring {
    pub fn new(max_in_flight: usize) -> anyhow::Result<Self> {
        let entries = max_in_flight.max(1).next_power_of_two().min(1024) as u32;
        let probe = io_uring::IoUring::new(entries).context("create io_uring")?;
        Ok(Self {
            _max_in_flight: max_in_flight,
            _probe: probe,
        })
    }

    pub fn append(&self, path: &Path, data: &[u8]) -> anyhow::Result<u64> {
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

    pub fn append_many(&self, path: &Path, chunks: &[Vec<u8>]) -> anyhow::Result<u64> {
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

    pub fn write_all_at(&self, path: &Path, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("open for write: {}", path.display()))?;
        file.seek(std::io::SeekFrom::Start(offset))
            .with_context(|| format!("seek: {}", path.display()))?;
        file.write_all(data)
            .with_context(|| format!("write: {}", path.display()))?;
        Ok(())
    }

    pub fn read_into_at(&self, path: &Path, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .with_context(|| format!("open for read_into_at: {}", path.display()))?;
        file.seek(std::io::SeekFrom::Start(offset))
            .with_context(|| format!("seek: {}", path.display()))?;
        file.read_exact(buf)
            .with_context(|| format!("read_exact: {}", path.display()))?;
        Ok(())
    }

    pub fn sync_file(&self, path: &Path) -> anyhow::Result<()> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("open for sync: {}", path.display()))?;
        file.sync_data()
            .with_context(|| format!("sync_data: {}", path.display()))
    }
}
