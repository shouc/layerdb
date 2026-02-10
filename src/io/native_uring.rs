use std::io::{IoSlice, Write};
use std::path::Path;

use anyhow::Context;
use io_uring::{opcode, types, IoUring};
use parking_lot::Mutex;
use std::os::unix::io::AsRawFd;

pub struct NativeUring {
    _max_in_flight: usize,
    ring: Mutex<IoUring>,
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
        let ring = IoUring::new(entries).context("create io_uring")?;
        Ok(Self {
            _max_in_flight: max_in_flight,
            ring: Mutex::new(ring),
        })
    }

    fn submit_one(&self, entry: io_uring::squeue::Entry) -> anyhow::Result<i32> {
        let mut ring = self.ring.lock();

        unsafe {
            ring.submission()
                .push(&entry)
                .map_err(|_| anyhow::anyhow!("io_uring submission queue is full"))?;
        }

        ring.submit_and_wait(1)
            .context("io_uring submit_and_wait")?;

        let cqe = ring
            .completion()
            .next()
            .context("io_uring completion queue empty")?;
        Ok(cqe.result())
    }

    fn result_to_io(res: i32) -> std::io::Result<usize> {
        if res >= 0 {
            Ok(res as usize)
        } else {
            Err(std::io::Error::from_raw_os_error(-res))
        }
    }

    pub fn write_all_at_file(
        &self,
        file: &std::fs::File,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let fd = file.as_raw_fd();
        let mut written = 0usize;
        while written < data.len() {
            let remaining = data.len() - written;
            let len: u32 = remaining
                .min(u32::MAX as usize)
                .try_into()
                .expect("len fits u32");
            let ptr = unsafe { data.as_ptr().add(written) };
            let entry = opcode::Write::new(types::Fd(fd), ptr, len)
                .offset(offset + written as u64)
                .build();

            let res = self.submit_one(entry).context("io_uring write")?;
            let n = Self::result_to_io(res).context("io_uring write result")?;
            if n == 0 {
                anyhow::bail!("io_uring write returned zero bytes");
            }
            written += n;
        }

        Ok(())
    }

    pub fn read_into_at_file(
        &self,
        file: &std::fs::File,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        let fd = file.as_raw_fd();
        let mut read = 0usize;
        while read < buf.len() {
            let remaining = buf.len() - read;
            let len: u32 = remaining
                .min(u32::MAX as usize)
                .try_into()
                .expect("len fits u32");
            let ptr = unsafe { buf.as_mut_ptr().add(read) };
            let entry = opcode::Read::new(types::Fd(fd), ptr, len)
                .offset(offset + read as u64)
                .build();

            let res = self.submit_one(entry).context("io_uring read")?;
            let n = Self::result_to_io(res).context("io_uring read result")?;
            if n == 0 {
                anyhow::bail!("io_uring read hit EOF");
            }
            read += n;
        }

        Ok(())
    }

    pub fn sync_file_file(&self, file: &std::fs::File) -> anyhow::Result<()> {
        let fd = file.as_raw_fd();
        let entry = opcode::Fsync::new(types::Fd(fd))
            .flags(types::FsyncFlags::DATASYNC)
            .build();
        let res = self.submit_one(entry).context("io_uring fsync")?;
        let _ = Self::result_to_io(res).context("io_uring fsync result")?;
        Ok(())
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

    #[allow(dead_code)]
    pub fn write_all_at(&self, path: &Path, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("open for write: {}", path.display()))?;
        self.write_all_at_file(&file, offset, data)
            .with_context(|| format!("write_all_at: {}", path.display()))
    }

    #[allow(dead_code)]
    pub fn read_into_at(&self, path: &Path, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .with_context(|| format!("open for read_into_at: {}", path.display()))?;
        self.read_into_at_file(&file, offset, buf)
            .with_context(|| format!("read_into_at: {}", path.display()))
    }

    #[allow(dead_code)]
    pub fn sync_file(&self, path: &Path) -> anyhow::Result<()> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("open for sync: {}", path.display()))?;
        self.sync_file_file(&file)
            .with_context(|| format!("sync_data: {}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::NativeUring;

    #[test]
    fn native_uring_smoke_roundtrip() {
        let uring = match NativeUring::new(8) {
            Ok(uring) => uring,
            Err(_) => return,
        };

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("smoke.bin");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("open");

        let payload = b"hello";
        uring.write_all_at_file(&file, 0, payload).expect("write");
        uring.sync_file_file(&file).expect("sync");

        let mut out = vec![0u8; payload.len()];
        uring.read_into_at_file(&file, 0, &mut out).expect("read");
        assert_eq!(out.as_slice(), payload);
    }
}
