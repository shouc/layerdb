use std::io::{IoSlice, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use io_uring::{opcode, types, IoUring};
use parking_lot::Mutex;
use std::os::unix::io::AsRawFd;

const REGISTERED_FILE_SLOTS: u32 = 1024;
const FIXED_BUF_BYTES: usize = 256 * 1024;
const FIXED_BUF_MAX: usize = 64;

#[derive(Debug)]
struct RegisteredFiles {
    size: u32,
    free: Vec<u32>,
}

#[derive(Debug)]
struct FixedBufPool {
    capacity: usize,
    buffers: Vec<Box<[u8]>>,
    free: Mutex<Vec<u16>>,
}

#[derive(Debug)]
pub(crate) struct FixedBuf {
    native: Arc<NativeUring>,
    buf_index: u16,
    ptr: *mut u8,
    capacity: usize,
    len: usize,
}

impl FixedBuf {
    pub(crate) fn buf_index(&self) -> u16 {
        self.buf_index
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn resize(&mut self, new_len: usize, value: u8) {
        assert!(new_len <= self.capacity, "fixed buf len exceeds capacity");

        if new_len > self.len {
            unsafe {
                std::slice::from_raw_parts_mut(self.ptr.add(self.len), new_len - self.len)
                    .fill(value);
            }
        }
        self.len = new_len;
    }

    pub(crate) fn clear(&mut self) {
        self.len = 0;
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for FixedBuf {
    fn drop(&mut self) {
        self.native.release_fixed_buf(self.buf_index);
    }
}

pub struct NativeUring {
    _max_in_flight: usize,
    ring: Mutex<IoUring>,

    registered_files: Mutex<Option<RegisteredFiles>>,
    fixed_bufs: Option<FixedBufPool>,
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
        let registered_files = Mutex::new(Self::try_register_files(&ring));
        let fixed_bufs = Self::try_register_fixed_bufs(&ring, max_in_flight);

        Ok(Self {
            _max_in_flight: max_in_flight,
            ring: Mutex::new(ring),
            registered_files,
            fixed_bufs,
        })
    }

    fn try_register_files(ring: &IoUring) -> Option<RegisteredFiles> {
        let submitter = ring.submitter();
        let slots = REGISTERED_FILE_SLOTS;
        let sparse = submitter.register_files_sparse(slots);
        let registered = match sparse {
            Ok(()) => true,
            Err(e) => {
                let is_sparse_unsupported = e.raw_os_error() == Some(libc::EINVAL)
                    || e.raw_os_error() == Some(libc::EOPNOTSUPP);
                if !is_sparse_unsupported {
                    return None;
                }

                let fds = vec![-1; slots as usize];
                submitter.register_files(&fds).is_ok()
            }
        };

        if !registered {
            return None;
        }

        let mut free = Vec::with_capacity(slots as usize);
        // LIFO so we can pop() quickly.
        for idx in (0..slots).rev() {
            free.push(idx);
        }

        Some(RegisteredFiles { size: slots, free })
    }

    fn try_register_fixed_bufs(ring: &IoUring, max_in_flight: usize) -> Option<FixedBufPool> {
        let submitter = ring.submitter();

        let count = max_in_flight.clamp(1, FIXED_BUF_MAX);
        let mut buffers: Vec<Box<[u8]>> = Vec::with_capacity(count);
        let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(count);

        for _ in 0..count {
            let mut buf = vec![0u8; FIXED_BUF_BYTES].into_boxed_slice();
            let ptr = buf.as_mut_ptr() as *mut libc::c_void;
            iovecs.push(libc::iovec {
                iov_base: ptr,
                iov_len: FIXED_BUF_BYTES,
            });
            buffers.push(buf);
        }

        let registered = unsafe { submitter.register_buffers(&iovecs).is_ok() };
        if !registered {
            return None;
        }

        let free: Vec<u16> = (0..count)
            .rev()
            .map(|idx| idx.try_into().expect("fixed buf index fits u16"))
            .collect();

        Some(FixedBufPool {
            capacity: FIXED_BUF_BYTES,
            buffers,
            free: Mutex::new(free),
        })
    }

    pub fn register_file(&self, file: &std::fs::File) -> Option<u32> {
        let slot = {
            let mut guard = self.registered_files.lock();
            let Some(files) = guard.as_mut() else {
                return None;
            };
            files.free.pop()?
        };

        let fd = file.as_raw_fd();
        let updated = {
            let ring = self.ring.lock();
            ring.submitter().register_files_update(slot, &[fd]).ok()
        };

        match updated {
            Some(1) => Some(slot),
            _ => {
                let mut guard = self.registered_files.lock();
                if let Some(files) = guard.as_mut() {
                    files.free.push(slot);
                }
                None
            }
        }
    }

    pub fn unregister_file(&self, fixed_fd: u32) {
        let updated = {
            let ring = self.ring.lock();
            ring.submitter().register_files_update(fixed_fd, &[-1]).ok()
        };

        if updated != Some(1) {
            return;
        }

        let mut guard = self.registered_files.lock();
        let Some(files) = guard.as_mut() else {
            return;
        };
        if fixed_fd < files.size {
            files.free.push(fixed_fd);
        }
    }

    pub(crate) fn try_acquire_fixed_buf(self: &Arc<Self>, min_capacity: usize) -> Option<FixedBuf> {
        let pool = self.fixed_bufs.as_ref()?;
        if min_capacity > pool.capacity {
            return None;
        }

        let buf_index = {
            let mut free = pool.free.lock();
            free.pop()?
        };

        let ptr = pool
            .buffers
            .get(buf_index as usize)
            .map(|b| b.as_ptr() as *mut u8)?;

        Some(FixedBuf {
            native: self.clone(),
            buf_index,
            ptr,
            capacity: pool.capacity,
            len: 0,
        })
    }

    fn release_fixed_buf(&self, buf_index: u16) {
        let Some(pool) = self.fixed_bufs.as_ref() else {
            return;
        };
        let mut free = pool.free.lock();
        free.push(buf_index);
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

    fn submit_batch(&self, entries: Vec<io_uring::squeue::Entry>) -> anyhow::Result<Vec<i32>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let want = entries.len();
        let mut ring = self.ring.lock();

        unsafe {
            let mut sq = ring.submission();
            for entry in &entries {
                sq.push(entry)
                    .map_err(|_| anyhow::anyhow!("io_uring submission queue is full"))?;
            }
        }

        ring.submit_and_wait(want)
            .context("io_uring submit_and_wait batch")?;

        let mut results = vec![i32::MIN; want];
        let mut seen = 0usize;

        while seen < want {
            {
                let mut cq = ring.completion();
                while seen < want {
                    let Some(cqe) = cq.next() else {
                        break;
                    };
                    let idx: usize = cqe
                        .user_data()
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("io_uring completion user_data overflow"))?;
                    if idx >= want {
                        anyhow::bail!("io_uring completion user_data out of range: {idx}");
                    }
                    results[idx] = cqe.result();
                    seen += 1;
                }
            }

            if seen < want {
                ring.submit_and_wait(want - seen)
                    .context("io_uring submit_and_wait batch (drain)")?;
            }
        }

        Ok(results)
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
        self.write_all_at_file_impl(file, None, offset, data)
    }

    pub fn write_all_at_file_batch(
        &self,
        file: &std::fs::File,
        fixed_fd: Option<u32>,
        writes: &[(u64, &[u8])],
    ) -> anyhow::Result<()> {
        #[derive(Debug, Clone, Copy)]
        struct PendingSlice {
            offset: u64,
            ptr: *const u8,
            len: usize,
        }

        if writes.is_empty() {
            return Ok(());
        }

        let fd = file.as_raw_fd();
        let max_batch = self._max_in_flight.clamp(1, 128);

        let mut pending = Vec::new();
        for (offset, data) in writes {
            if data.is_empty() {
                continue;
            }
            if data.len() > u32::MAX as usize {
                self.write_all_at_file_impl(file, fixed_fd, *offset, data)?;
                continue;
            }
            pending.push(PendingSlice {
                offset: *offset,
                ptr: data.as_ptr(),
                len: data.len(),
            });
        }

        while !pending.is_empty() {
            let chunk_len = pending.len().min(max_batch);
            let mut chunk: Vec<PendingSlice> = pending.drain(..chunk_len).collect();
            let mut entries = Vec::with_capacity(chunk_len);

            for (idx, slice) in chunk.iter().enumerate() {
                let len: u32 = slice
                    .len
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("batch write len exceeds u32"))?;
                let entry = if let Some(fixed) = fixed_fd {
                    opcode::Write::new(types::Fixed(fixed), slice.ptr, len)
                        .offset(slice.offset)
                        .build()
                } else {
                    opcode::Write::new(types::Fd(fd), slice.ptr, len)
                        .offset(slice.offset)
                        .build()
                };
                entries.push(entry.user_data(idx as u64));
            }

            let results = self.submit_batch(entries).context("io_uring write batch")?;
            debug_assert_eq!(results.len(), chunk_len);

            for (idx, slice) in chunk.iter_mut().enumerate() {
                let n = Self::result_to_io(results[idx]).with_context(|| {
                    format!("io_uring write batch result offset {}", slice.offset)
                })?;
                if n == 0 {
                    anyhow::bail!("io_uring write batch returned zero bytes");
                }
                if n < slice.len {
                    slice.offset = slice
                        .offset
                        .checked_add(n as u64)
                        .context("io_uring write batch offset overflow")?;
                    slice.ptr = unsafe { slice.ptr.add(n) };
                    slice.len -= n;
                    pending.push(*slice);
                }
            }
        }

        Ok(())
    }

    pub fn write_all_at_file_fixed(
        &self,
        file: &std::fs::File,
        fixed_fd: u32,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        self.write_all_at_file_impl(file, Some(fixed_fd), offset, data)
    }

    fn write_all_at_file_impl(
        &self,
        file: &std::fs::File,
        fixed_fd: Option<u32>,
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
            let entry = if let Some(fixed) = fixed_fd {
                opcode::Write::new(types::Fixed(fixed), ptr, len)
                    .offset(offset + written as u64)
                    .build()
            } else {
                opcode::Write::new(types::Fd(fd), ptr, len)
                    .offset(offset + written as u64)
                    .build()
            };

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
        self.read_into_at_file_impl(file, None, offset, buf)
    }

    pub fn read_into_at_file_fixed(
        &self,
        file: &std::fs::File,
        fixed_fd: u32,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        self.read_into_at_file_impl(file, Some(fixed_fd), offset, buf)
    }

    fn read_into_at_file_impl(
        &self,
        file: &std::fs::File,
        fixed_fd: Option<u32>,
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
            let entry = if let Some(fixed) = fixed_fd {
                opcode::Read::new(types::Fixed(fixed), ptr, len)
                    .offset(offset + read as u64)
                    .build()
            } else {
                opcode::Read::new(types::Fd(fd), ptr, len)
                    .offset(offset + read as u64)
                    .build()
            };

            let res = self.submit_one(entry).context("io_uring read")?;
            let n = Self::result_to_io(res).context("io_uring read result")?;
            if n == 0 {
                anyhow::bail!("io_uring read hit EOF");
            }
            read += n;
        }

        Ok(())
    }

    pub fn read_into_at_file_fixed_buf(
        &self,
        file: &std::fs::File,
        fixed_fd: Option<u32>,
        offset: u64,
        buf_index: u16,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        let fd = file.as_raw_fd();
        let len: u32 = buf
            .len()
            .try_into()
            .map_err(|_| anyhow::anyhow!("fixed read len exceeds u32"))?;
        let ptr = buf.as_mut_ptr();

        let entry = if let Some(fixed) = fixed_fd {
            opcode::ReadFixed::new(types::Fixed(fixed), ptr, len, buf_index)
                .offset(offset)
                .build()
        } else {
            opcode::ReadFixed::new(types::Fd(fd), ptr, len, buf_index)
                .offset(offset)
                .build()
        };

        let res = self.submit_one(entry).context("io_uring read_fixed")?;
        let n = Self::result_to_io(res).context("io_uring read_fixed result")?;
        if n == 0 {
            anyhow::bail!("io_uring read_fixed hit EOF");
        }
        if n != buf.len() {
            anyhow::bail!(
                "io_uring read_fixed returned short read: {n} != {}",
                buf.len()
            );
        }
        Ok(())
    }

    pub fn sync_file_file(&self, file: &std::fs::File) -> anyhow::Result<()> {
        self.sync_file_file_impl(file, None)
    }

    pub fn sync_file_file_fixed(&self, file: &std::fs::File, fixed_fd: u32) -> anyhow::Result<()> {
        self.sync_file_file_impl(file, Some(fixed_fd))
    }

    fn sync_file_file_impl(
        &self,
        file: &std::fs::File,
        fixed_fd: Option<u32>,
    ) -> anyhow::Result<()> {
        let fd = file.as_raw_fd();
        let entry = if let Some(fixed) = fixed_fd {
            opcode::Fsync::new(types::Fixed(fixed))
                .flags(types::FsyncFlags::DATASYNC)
                .build()
        } else {
            opcode::Fsync::new(types::Fd(fd))
                .flags(types::FsyncFlags::DATASYNC)
                .build()
        };
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

    #[test]
    fn native_uring_fixed_files_and_buffers_roundtrip() {
        let uring = match NativeUring::new(8) {
            Ok(uring) => std::sync::Arc::new(uring),
            Err(_) => return,
        };

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fixed.bin");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("open");

        let fixed_fd = match uring.register_file(&file) {
            Some(fd) => fd,
            None => return,
        };
        let mut fixed_buf = match uring.try_acquire_fixed_buf(16) {
            Some(buf) => buf,
            None => return,
        };

        let payload = b"hello-fixed";
        uring
            .write_all_at_file_fixed(&file, fixed_fd, 0, payload)
            .expect("write");
        uring.sync_file_file_fixed(&file, fixed_fd).expect("sync");

        fixed_buf.resize(payload.len(), 0);
        uring
            .read_into_at_file_fixed_buf(
                &file,
                Some(fixed_fd),
                0,
                fixed_buf.buf_index(),
                fixed_buf.as_mut_slice(),
            )
            .expect("read");
        assert_eq!(fixed_buf.as_slice(), payload);

        drop(fixed_buf);
        uring.unregister_file(fixed_fd);
    }

    #[test]
    fn native_uring_batch_write_roundtrip() {
        let uring = match NativeUring::new(8) {
            Ok(uring) => uring,
            Err(_) => return,
        };

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("batch.bin");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("open");

        // Write 2 slices at different offsets in one batch.
        let part_a = b"hello";
        let part_b = b"world";
        uring
            .write_all_at_file_batch(
                &file,
                None,
                &[(0, part_a.as_slice()), (10, part_b.as_slice())],
            )
            .expect("write_batch");
        uring.sync_file_file(&file).expect("sync");

        let mut out = vec![0u8; 15];
        uring.read_into_at_file(&file, 0, &mut out).expect("read");
        assert_eq!(&out[0..5], part_a);
        assert_eq!(&out[10..15], part_b);
    }
}
