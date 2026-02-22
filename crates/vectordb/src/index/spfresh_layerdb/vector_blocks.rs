use std::fs::OpenOptions;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;
use memmap2::Mmap;
use rustc_hash::FxHashMap;

const VECTOR_BLOCKS_DIR: &str = "vector_blocks";
const VECTOR_BLOCK_FILE_EXT: &str = "vb";
const VECTOR_BLOCK_INDEX_FILE_EXT: &str = "vbi";
const VECTOR_BLOCK_HEADER_TAG: &[u8] = b"vb1";
const VECTOR_BLOCK_INDEX_HEADER_TAG: &[u8] = b"vbi1";
const VECTOR_BLOCK_TOMBSTONE: u8 = 1;
const VECTOR_BLOCK_LIVE: u8 = 0;
const VECTOR_BLOCK_HAS_POSTING: u8 = 1 << 1;
const REMAP_INTERVAL_RECORDS: usize = 1_024;
const INDEX_RECORD_SIZE: usize = 8 + 1 + 8;
const DISTANCE_SCAN_PREFETCH_DISTANCE: usize = 16;

fn record_size(dim: usize) -> usize {
    8 + 1 + 8 + dim.saturating_mul(4)
}

fn header_size() -> usize {
    VECTOR_BLOCK_HEADER_TAG.len() + 4
}

fn index_header_size() -> usize {
    VECTOR_BLOCK_INDEX_HEADER_TAG.len() + 4
}

fn file_path_for_epoch(root: &Path, epoch: u64) -> PathBuf {
    root.join(VECTOR_BLOCKS_DIR)
        .join(format!("epoch-{epoch:016x}.{VECTOR_BLOCK_FILE_EXT}"))
}

fn index_file_path_for_epoch(root: &Path, epoch: u64) -> PathBuf {
    root.join(VECTOR_BLOCKS_DIR)
        .join(format!("epoch-{epoch:016x}.{VECTOR_BLOCK_INDEX_FILE_EXT}"))
}

#[derive(Debug)]
pub(crate) struct VectorBlockStore {
    root: PathBuf,
    dim: usize,
    epoch: u64,
    file: std::fs::File,
    index_file: std::fs::File,
    mmap: Option<Mmap>,
    offsets: FxHashMap<u64, u64>,
    pending_remap_records: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct VectorBlockState {
    pub posting_id: Option<usize>,
    pub values: Vec<f32>,
}

impl VectorBlockStore {
    #[inline]
    fn prefetch_record(mmap: &Mmap, offset: u64, rec_size: usize) {
        let Some(start) = usize::try_from(offset).ok() else {
            return;
        };
        let Some(end) = start.checked_add(rec_size) else {
            return;
        };
        if end > mmap.len() {
            return;
        }
        let ptr = mmap.as_ptr();
        // SAFETY: start/end bounds are validated above.
        let addr = unsafe { ptr.add(start) };
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        unsafe {
            #[cfg(target_arch = "x86")]
            use std::arch::x86::{_mm_prefetch, _MM_HINT_T0};
            #[cfg(target_arch = "x86_64")]
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(addr.cast::<i8>(), _MM_HINT_T0);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            core::arch::asm!(
                "prfm pldl1keep, [{addr}]",
                addr = in(reg) addr,
                options(nostack, readonly, preserves_flags)
            );
        }
    }

    fn open_vector_file(root: &Path, epoch: u64, dim: usize) -> anyhow::Result<std::fs::File> {
        let path = file_path_for_epoch(root, epoch);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open vector blocks file {}", path.display()))?;

        let metadata = file
            .metadata()
            .with_context(|| format!("stat vector blocks file {}", path.display()))?;
        if metadata.len() == 0 {
            let mut header = Vec::with_capacity(header_size());
            header.extend_from_slice(VECTOR_BLOCK_HEADER_TAG);
            let dim_u32 = u32::try_from(dim).context("vector blocks dim does not fit u32")?;
            header.extend_from_slice(&dim_u32.to_le_bytes());
            file.write_all(&header)
                .with_context(|| format!("write vector blocks header {}", path.display()))?;
            file.flush()
                .with_context(|| format!("flush vector blocks header {}", path.display()))?;
        }
        Ok(file)
    }

    fn open_index_file(root: &Path, epoch: u64, dim: usize) -> anyhow::Result<std::fs::File> {
        let path = index_file_path_for_epoch(root, epoch);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open vector index file {}", path.display()))?;

        let metadata = file
            .metadata()
            .with_context(|| format!("stat vector index file {}", path.display()))?;
        if metadata.len() == 0 {
            let mut header = Vec::with_capacity(index_header_size());
            header.extend_from_slice(VECTOR_BLOCK_INDEX_HEADER_TAG);
            let dim_u32 = u32::try_from(dim).context("vector index dim does not fit u32")?;
            header.extend_from_slice(&dim_u32.to_le_bytes());
            file.write_all(&header)
                .with_context(|| format!("write vector index header {}", path.display()))?;
            file.flush()
                .with_context(|| format!("flush vector index header {}", path.display()))?;
        }
        Ok(file)
    }

    pub(crate) fn open(root: impl AsRef<Path>, dim: usize, epoch: u64) -> anyhow::Result<Self> {
        let root = root.as_ref().to_path_buf();
        let blocks_dir = root.join(VECTOR_BLOCKS_DIR);
        std::fs::create_dir_all(&blocks_dir)
            .with_context(|| format!("create vector blocks dir {}", blocks_dir.display()))?;
        let file = Self::open_vector_file(&root, epoch, dim)?;
        let index_file = Self::open_index_file(&root, epoch, dim)?;

        let mut store = Self {
            root,
            dim,
            epoch,
            file,
            index_file,
            mmap: None,
            offsets: FxHashMap::default(),
            pending_remap_records: 0,
        };
        store.validate_header()?;
        if let Err(err) = store.validate_index_header() {
            eprintln!("vector index header invalid; rebuilding sidecar: {err:#}");
            store.reset_index_file()?;
        }
        store.rebuild_offsets()?;
        store.remap()?;
        Ok(store)
    }

    pub(crate) fn rotate_epoch(&mut self, epoch: u64) -> anyhow::Result<()> {
        if self.epoch == epoch {
            return Ok(());
        }
        let file = Self::open_vector_file(&self.root, epoch, self.dim)?;
        let index_file = Self::open_index_file(&self.root, epoch, self.dim)?;

        self.file = file;
        self.index_file = index_file;
        self.epoch = epoch;
        self.offsets.clear();
        self.pending_remap_records = 0;
        self.validate_header()?;
        if let Err(err) = self.validate_index_header() {
            eprintln!("vector index header invalid after rotate; rebuilding sidecar: {err:#}");
            self.reset_index_file()?;
        }
        self.rebuild_offsets()?;
        self.remap()?;
        Ok(())
    }

    fn push_index_record(buf: &mut Vec<u8>, id: u64, flags: u8, offset: u64) {
        buf.extend_from_slice(&id.to_le_bytes());
        buf.push(flags);
        buf.extend_from_slice(&offset.to_le_bytes());
    }

    fn append_index_records(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.index_file
            .write_all(bytes)
            .context("append vector index records")
    }

    pub(crate) fn append_upsert_batch_with_posting<F>(
        &mut self,
        rows: &[(u64, Vec<f32>)],
        mut posting_for_id: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(u64) -> Option<usize>,
    {
        if rows.is_empty() {
            return Ok(());
        }

        let rec_size = record_size(self.dim);
        let rec_size_u64 = u64::try_from(rec_size).context("vector block record size overflow")?;
        let mut buf = Vec::with_capacity(rec_size.saturating_mul(rows.len()));
        let mut index_buf = Vec::with_capacity(INDEX_RECORD_SIZE.saturating_mul(rows.len()));
        let mut offset = self
            .file
            .seek(SeekFrom::End(0))
            .context("seek vector blocks end")?;

        for (id, values) in rows {
            if values.len() != self.dim {
                anyhow::bail!(
                    "vector block upsert dim mismatch: got {}, expected {}",
                    values.len(),
                    self.dim
                );
            }
            let mut flags = VECTOR_BLOCK_LIVE;
            let posting_u64 = posting_for_id(*id)
                .map(|pid| u64::try_from(pid).context("vector block posting id does not fit u64"))
                .transpose()?;
            if posting_u64.is_some() {
                flags |= VECTOR_BLOCK_HAS_POSTING;
            }

            let record_offset = offset;
            buf.extend_from_slice(&id.to_le_bytes());
            buf.push(flags);
            buf.extend_from_slice(&posting_u64.unwrap_or_default().to_le_bytes());
            for value in values {
                buf.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            Self::push_index_record(&mut index_buf, *id, flags, record_offset);
            self.offsets.insert(*id, offset);
            offset = offset
                .checked_add(rec_size_u64)
                .ok_or_else(|| anyhow::anyhow!("vector block offset overflow"))?;
        }

        self.file
            .write_all(&buf)
            .context("append vector block upsert batch")?;
        self.append_index_records(index_buf.as_slice())?;
        self.pending_remap_records = self.pending_remap_records.saturating_add(rows.len());
        self.maybe_remap()?;
        Ok(())
    }

    pub(crate) fn append_upsert_with_posting(
        &mut self,
        id: u64,
        posting_id: Option<usize>,
        values: &[f32],
    ) -> anyhow::Result<()> {
        if values.len() != self.dim {
            anyhow::bail!(
                "vector block upsert dim mismatch: got {}, expected {}",
                values.len(),
                self.dim
            );
        }
        let offset = self
            .file
            .seek(SeekFrom::End(0))
            .context("seek vector blocks end")?;
        let mut flags = VECTOR_BLOCK_LIVE;
        let posting_u64 = posting_id
            .map(|pid| u64::try_from(pid).context("vector block posting id does not fit u64"))
            .transpose()?;
        if posting_u64.is_some() {
            flags |= VECTOR_BLOCK_HAS_POSTING;
        }
        let mut buf = Vec::with_capacity(record_size(self.dim));
        buf.extend_from_slice(&id.to_le_bytes());
        buf.push(flags);
        buf.extend_from_slice(&posting_u64.unwrap_or_default().to_le_bytes());
        for value in values {
            buf.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        self.file
            .write_all(&buf)
            .context("append vector block upsert")?;
        let mut index_buf = Vec::with_capacity(INDEX_RECORD_SIZE);
        Self::push_index_record(&mut index_buf, id, flags, offset);
        self.append_index_records(index_buf.as_slice())?;
        self.offsets.insert(id, offset);
        self.pending_remap_records = self.pending_remap_records.saturating_add(1);
        self.maybe_remap()?;
        Ok(())
    }

    pub(crate) fn append_delete_batch(&mut self, ids: &[u64]) -> anyhow::Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let rec_size = record_size(self.dim);
        let rec_size_u64 = u64::try_from(rec_size).context("vector block record size overflow")?;
        let tombstone_tail = vec![0u8; rec_size.saturating_sub(17)];
        let mut buf = Vec::with_capacity(rec_size.saturating_mul(ids.len()));
        let mut index_buf = Vec::with_capacity(INDEX_RECORD_SIZE.saturating_mul(ids.len()));
        let mut offset = self
            .file
            .seek(SeekFrom::End(0))
            .context("seek vector blocks end")?;
        for id in ids {
            buf.extend_from_slice(&id.to_le_bytes());
            buf.push(VECTOR_BLOCK_TOMBSTONE);
            buf.extend_from_slice(&0u64.to_le_bytes());
            buf.extend_from_slice(tombstone_tail.as_slice());
            Self::push_index_record(&mut index_buf, *id, VECTOR_BLOCK_TOMBSTONE, offset);
            self.offsets.remove(id);
            offset = offset
                .checked_add(rec_size_u64)
                .ok_or_else(|| anyhow::anyhow!("vector block offset overflow"))?;
        }
        self.file
            .write_all(&buf)
            .context("append vector block tombstone batch")?;
        self.append_index_records(index_buf.as_slice())?;
        self.pending_remap_records = self.pending_remap_records.saturating_add(ids.len());
        self.maybe_remap()?;
        Ok(())
    }

    pub(crate) fn get(&self, id: u64) -> Option<Vec<f32>> {
        self.get_state(id).map(|state| state.values)
    }

    pub(crate) fn distances_for_ids(
        &self,
        ids: &[u64],
        query: &[f32],
    ) -> (Vec<(u64, f32)>, Vec<u64>) {
        if query.len() != self.dim {
            return (Vec::new(), ids.to_vec());
        }
        let Some(mmap) = self.mmap.as_ref() else {
            return (Vec::new(), ids.to_vec());
        };
        let rec_size = record_size(self.dim);
        let mut found = Vec::with_capacity(ids.len());
        let mut missing = Vec::new();
        let iterate =
            |id: u64, offset: u64, found: &mut Vec<(u64, f32)>, missing: &mut Vec<u64>| {
                let Some(start) = usize::try_from(offset).ok() else {
                    missing.push(id);
                    return;
                };
                let Some(end) = start.checked_add(rec_size) else {
                    missing.push(id);
                    return;
                };
                let Some(record) = mmap.get(start..end) else {
                    missing.push(id);
                    return;
                };
                let flags = record[8];
                if (flags & VECTOR_BLOCK_TOMBSTONE) != 0 {
                    missing.push(id);
                    return;
                }

                let mut distance = 0.0f32;
                let mut cursor = 17usize;
                for query_value in query {
                    // SAFETY: `record` length is exactly one full record (rec_size), and
                    // `query.len() == self.dim`, so each 4-byte load is in-bounds.
                    let bits = unsafe {
                        u32::from_le(record.as_ptr().add(cursor).cast::<u32>().read_unaligned())
                    };
                    let value = f32::from_bits(bits);
                    let delta = *query_value - value;
                    distance += delta * delta;
                    cursor += 4;
                }
                if distance.is_finite() {
                    found.push((id, distance));
                } else {
                    missing.push(id);
                }
            };

        if ids.len() >= 2_048 {
            let mut ordered_offsets = Vec::with_capacity(ids.len());
            for id in ids {
                let Some(offset) = self.offsets.get(id).copied() else {
                    missing.push(*id);
                    continue;
                };
                ordered_offsets.push((*id, offset));
            }
            // Group large scans by file locality to reduce page-fault churn on cold scans.
            ordered_offsets.sort_unstable_by_key(|(_, offset)| *offset);
            for idx in 0..ordered_offsets.len() {
                if let Some((_, prefetch_offset)) =
                    ordered_offsets.get(idx.saturating_add(DISTANCE_SCAN_PREFETCH_DISTANCE))
                {
                    Self::prefetch_record(mmap, *prefetch_offset, rec_size);
                }
                let (id, offset) = ordered_offsets[idx];
                iterate(id, offset, &mut found, &mut missing);
            }
        } else {
            for id in ids {
                let Some(offset) = self.offsets.get(id).copied() else {
                    missing.push(*id);
                    continue;
                };
                iterate(*id, offset, &mut found, &mut missing);
            }
        }
        (found, missing)
    }

    pub(crate) fn live_len(&self) -> usize {
        self.offsets.len()
    }

    pub(crate) fn live_ids(&self) -> Vec<u64> {
        self.offsets.keys().copied().collect()
    }

    pub(crate) fn get_state(&self, id: u64) -> Option<VectorBlockState> {
        let offset = *self.offsets.get(&id)?;
        let mmap = self.mmap.as_ref()?;
        let rec_size = record_size(self.dim);
        let start = usize::try_from(offset).ok()?;
        let end = start.checked_add(rec_size)?;
        let record = mmap.get(start..end)?;
        let flags = *record.get(8)?;
        let tombstone = (flags & VECTOR_BLOCK_TOMBSTONE) != 0;
        if tombstone {
            return None;
        }
        let posting_id = if (flags & VECTOR_BLOCK_HAS_POSTING) != 0 {
            let posting_bytes = record.get(9..17)?;
            let mut arr = [0u8; 8];
            arr.copy_from_slice(posting_bytes);
            let pid_u64 = u64::from_le_bytes(arr);
            Some(usize::try_from(pid_u64).ok()?)
        } else {
            None
        };
        let mut values = Vec::with_capacity(self.dim);
        let mut cursor = 17usize;
        for _ in 0..self.dim {
            // SAFETY: `record` length is a full record and we iterate exactly `self.dim`
            // values from the payload region starting at byte 17.
            let bits =
                unsafe { u32::from_le(record.as_ptr().add(cursor).cast::<u32>().read_unaligned()) };
            values.push(f32::from_bits(bits));
            cursor += 4;
        }
        Some(VectorBlockState { posting_id, values })
    }

    fn validate_header(&mut self) -> anyhow::Result<()> {
        self.file
            .seek(SeekFrom::Start(0))
            .context("seek vector blocks header")?;
        let mut header = vec![0u8; header_size()];
        self.file
            .read_exact(&mut header)
            .context("read vector blocks header")?;
        if &header[..VECTOR_BLOCK_HEADER_TAG.len()] != VECTOR_BLOCK_HEADER_TAG {
            anyhow::bail!("invalid vector blocks header tag");
        }
        let mut dim_arr = [0u8; 4];
        dim_arr.copy_from_slice(&header[VECTOR_BLOCK_HEADER_TAG.len()..header_size()]);
        let dim =
            usize::try_from(u32::from_le_bytes(dim_arr)).context("vector blocks dim overflow")?;
        if dim != self.dim {
            anyhow::bail!(
                "vector blocks dim mismatch: file={}, expected={}",
                dim,
                self.dim
            );
        }
        Ok(())
    }

    fn validate_index_header(&mut self) -> anyhow::Result<()> {
        self.index_file
            .seek(SeekFrom::Start(0))
            .context("seek vector index header")?;
        let mut header = vec![0u8; index_header_size()];
        self.index_file
            .read_exact(&mut header)
            .context("read vector index header")?;
        if &header[..VECTOR_BLOCK_INDEX_HEADER_TAG.len()] != VECTOR_BLOCK_INDEX_HEADER_TAG {
            anyhow::bail!("invalid vector index header tag");
        }
        let mut dim_arr = [0u8; 4];
        dim_arr.copy_from_slice(&header[VECTOR_BLOCK_INDEX_HEADER_TAG.len()..index_header_size()]);
        let dim =
            usize::try_from(u32::from_le_bytes(dim_arr)).context("vector index dim overflow")?;
        if dim != self.dim {
            anyhow::bail!(
                "vector index dim mismatch: file={}, expected={}",
                dim,
                self.dim
            );
        }
        Ok(())
    }

    fn reset_index_file(&mut self) -> anyhow::Result<()> {
        self.index_file
            .set_len(0)
            .context("truncate vector index")?;
        self.index_file
            .seek(SeekFrom::Start(0))
            .context("seek vector index reset start")?;
        let mut header = Vec::with_capacity(index_header_size());
        header.extend_from_slice(VECTOR_BLOCK_INDEX_HEADER_TAG);
        let dim_u32 = u32::try_from(self.dim).context("vector index dim does not fit u32")?;
        header.extend_from_slice(&dim_u32.to_le_bytes());
        self.index_file
            .write_all(header.as_slice())
            .context("write vector index reset header")?;
        self.index_file
            .flush()
            .context("flush vector index reset header")?;
        self.index_file
            .seek(SeekFrom::End(0))
            .context("seek vector index end after reset")?;
        Ok(())
    }

    fn replay_index_sidecar(&mut self, vector_file_len: u64) -> anyhow::Result<Option<u64>> {
        let sidecar_len = self
            .index_file
            .metadata()
            .context("stat vector index sidecar")?
            .len();
        let index_header_u64 =
            u64::try_from(index_header_size()).context("vector index header size overflow")?;
        if sidecar_len <= index_header_u64 {
            return Ok(None);
        }
        let rec_size_u64 =
            u64::try_from(record_size(self.dim)).context("vector block record size overflow")?;
        let vector_header_u64 =
            u64::try_from(header_size()).context("vector block header size overflow")?;
        let mut reader = BufReader::with_capacity(
            64 * 1024,
            self.index_file
                .try_clone()
                .context("clone vector index sidecar")?,
        );
        reader
            .seek(SeekFrom::Start(index_header_u64))
            .context("seek vector index records start")?;
        let mut rec = [0u8; INDEX_RECORD_SIZE];
        let mut loaded_any = false;
        let mut covered_end = vector_header_u64;
        loop {
            match reader.read_exact(&mut rec) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err).context("read vector index sidecar record"),
            }
            let mut id_arr = [0u8; 8];
            id_arr.copy_from_slice(&rec[..8]);
            let id = u64::from_le_bytes(id_arr);
            let flags = rec[8];
            let mut off_arr = [0u8; 8];
            off_arr.copy_from_slice(&rec[9..17]);
            let offset = u64::from_le_bytes(off_arr);
            let Some(end) = offset.checked_add(rec_size_u64) else {
                break;
            };
            if offset < vector_header_u64 || end > vector_file_len {
                break;
            }
            if (flags & VECTOR_BLOCK_TOMBSTONE) != 0 {
                self.offsets.remove(&id);
            } else {
                self.offsets.insert(id, offset);
            }
            loaded_any = true;
            if end > covered_end {
                covered_end = end;
            }
        }
        Ok(loaded_any.then_some(covered_end))
    }

    fn rebuild_offsets(&mut self) -> anyhow::Result<()> {
        self.offsets.clear();
        let file_len = self
            .file
            .metadata()
            .context("stat vector blocks before scan")?
            .len();
        if file_len < u64::try_from(header_size()).context("vector blocks header size overflow")? {
            self.file
                .seek(SeekFrom::End(0))
                .context("seek vector blocks end after empty scan")?;
            return Ok(());
        }
        let vector_header_u64 =
            u64::try_from(header_size()).context("vector block header size overflow")?;
        let mut scan_start = vector_header_u64;
        match self.replay_index_sidecar(file_len)? {
            Some(covered_end) => {
                scan_start = covered_end.min(file_len);
            }
            None => {
                self.reset_index_file()?;
            }
        }
        if scan_start >= file_len {
            self.file
                .seek(SeekFrom::End(0))
                .context("seek vector blocks end after sidecar replay")?;
            return Ok(());
        }
        let rec_size = record_size(self.dim);
        let chunk_size = rec_size.saturating_mul(1024).max(rec_size);
        let mut reader = BufReader::with_capacity(
            chunk_size.max(64 * 1024),
            self.file.try_clone().context("clone vector blocks file")?,
        );
        let mut offset = scan_start;
        reader
            .seek(SeekFrom::Start(offset))
            .context("seek vector blocks scan start")?;

        let mut chunk = vec![0u8; chunk_size];
        let mut carry = Vec::<u8>::new();
        loop {
            let read = reader
                .read(chunk.as_mut_slice())
                .context("read vector blocks scan chunk")?;
            if read == 0 {
                break;
            }
            if carry.is_empty() {
                let mut index_records =
                    Vec::with_capacity((read / rec_size).saturating_mul(INDEX_RECORD_SIZE));
                let consumed = Self::scan_records_into_offsets(
                    chunk.get(..read).expect("read length checked"),
                    rec_size,
                    offset,
                    &mut self.offsets,
                    &mut index_records,
                )?;
                self.append_index_records(index_records.as_slice())?;
                if consumed < read {
                    carry.extend_from_slice(&chunk[consumed..read]);
                }
                offset = offset
                    .checked_add(u64::try_from(consumed).context("vector block offset overflow")?)
                    .ok_or_else(|| anyhow::anyhow!("vector block offset overflow"))?;
            } else {
                let mut merged = Vec::with_capacity(carry.len().saturating_add(read));
                merged.extend_from_slice(carry.as_slice());
                merged.extend_from_slice(chunk.get(..read).expect("read length checked"));
                let mut index_records =
                    Vec::with_capacity((merged.len() / rec_size).saturating_mul(INDEX_RECORD_SIZE));
                let consumed = Self::scan_records_into_offsets(
                    merged.as_slice(),
                    rec_size,
                    offset,
                    &mut self.offsets,
                    &mut index_records,
                )?;
                self.append_index_records(index_records.as_slice())?;
                carry.clear();
                if consumed < merged.len() {
                    carry.extend_from_slice(&merged[consumed..]);
                }
                offset = offset
                    .checked_add(u64::try_from(consumed).context("vector block offset overflow")?)
                    .ok_or_else(|| anyhow::anyhow!("vector block offset overflow"))?;
            }
        }
        self.file
            .seek(SeekFrom::End(0))
            .context("seek vector blocks end after scan")?;
        Ok(())
    }

    fn scan_records_into_offsets(
        bytes: &[u8],
        rec_size: usize,
        base_offset: u64,
        offsets: &mut FxHashMap<u64, u64>,
        index_records: &mut Vec<u8>,
    ) -> anyhow::Result<usize> {
        let records_len = bytes.len() / rec_size * rec_size;
        let mut cursor = 0usize;
        while cursor < records_len {
            // SAFETY: `cursor < records_len <= bytes.len()`, and each record is `rec_size`
            // bytes where `rec_size >= 17`, so id/flags reads are in-bounds.
            let id =
                unsafe { u64::from_le(bytes.as_ptr().add(cursor).cast::<u64>().read_unaligned()) };
            let flags = unsafe { *bytes.as_ptr().add(cursor + 8) };
            let record_offset = base_offset
                .checked_add(u64::try_from(cursor).context("vector block offset overflow")?)
                .ok_or_else(|| anyhow::anyhow!("vector block offset overflow"))?;
            if (flags & VECTOR_BLOCK_TOMBSTONE) != 0 {
                offsets.remove(&id);
            } else {
                offsets.insert(id, record_offset);
            }
            Self::push_index_record(index_records, id, flags, record_offset);
            cursor += rec_size;
        }
        Ok(records_len)
    }

    fn maybe_remap(&mut self) -> anyhow::Result<()> {
        if self.pending_remap_records < REMAP_INTERVAL_RECORDS {
            return Ok(());
        }
        self.remap()
    }

    fn remap(&mut self) -> anyhow::Result<()> {
        self.file
            .flush()
            .context("flush vector blocks before remap")?;
        self.index_file
            .flush()
            .context("flush vector index sidecar before remap")?;
        // SAFETY: we only read through mmap snapshots and remap after flushing appends.
        let mmap = unsafe { Mmap::map(&self.file) }.context("mmap vector blocks file")?;
        self.mmap = Some(mmap);
        self.pending_remap_records = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use tempfile::TempDir;

    use super::{index_file_path_for_epoch, VectorBlockStore, INDEX_RECORD_SIZE};

    #[test]
    fn distances_for_ids_uses_live_records_and_reports_missing() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let mut store = VectorBlockStore::open(dir.path(), 4, 1)?;
        store.append_upsert_with_posting(1, None, &[0.0, 1.0, 2.0, 3.0])?;
        store.append_upsert_with_posting(2, None, &[4.0, 5.0, 6.0, 7.0])?;
        store.append_delete_batch(&[2])?;
        store.remap()?;

        let (found, missing) = store.distances_for_ids(&[1, 2, 3], &[0.0, 1.0, 2.0, 3.0]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].0, 1);
        assert!((found[0].1 - 0.0).abs() <= f32::EPSILON);
        assert_eq!(missing, vec![2, 3]);
        Ok(())
    }

    #[test]
    fn distances_for_ids_large_scan_path_matches_expected() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let mut store = VectorBlockStore::open(dir.path(), 2, 1)?;
        let total = 2_200u64;
        for id in 0..total {
            store.append_upsert_with_posting(id, None, &[id as f32, 0.0])?;
        }
        // Ensure tombstones and missing ids are still handled on the ordered-offset path.
        store.append_delete_batch(&[13, 1700, 2199])?;
        store.remap()?;

        let ids: Vec<u64> = (0..total).collect();
        let (found, missing) = store.distances_for_ids(&ids, &[0.0, 0.0]);
        assert_eq!(found.len(), (total as usize).saturating_sub(3));
        assert_eq!(missing, vec![13, 1700, 2199]);
        let found_zero = found
            .iter()
            .find(|(id, _)| *id == 0)
            .expect("id=0 should be present");
        assert!((found_zero.1 - 0.0).abs() <= f32::EPSILON);
        Ok(())
    }

    #[test]
    fn reopen_rebuild_offsets_preserves_latest_live_records() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        {
            let mut store = VectorBlockStore::open(dir.path(), 3, 7)?;
            store.append_upsert_with_posting(1, Some(3), &[1.0, 2.0, 3.0])?;
            store.append_upsert_with_posting(2, None, &[4.0, 5.0, 6.0])?;
            store.append_upsert_with_posting(1, None, &[7.0, 8.0, 9.0])?;
            store.append_delete_batch(&[2])?;
            store.remap()?;
            assert_eq!(store.live_len(), 1);
            assert_eq!(store.get(1), Some(vec![7.0, 8.0, 9.0]));
            assert_eq!(store.get(2), None);
        }
        {
            let store = VectorBlockStore::open(dir.path(), 3, 7)?;
            assert_eq!(store.live_len(), 1);
            assert_eq!(store.get(1), Some(vec![7.0, 8.0, 9.0]));
            assert_eq!(store.get(2), None);
        }
        Ok(())
    }

    #[test]
    fn reopen_recovers_when_index_sidecar_lags_vector_file() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let epoch = 11u64;
        {
            let mut store = VectorBlockStore::open(dir.path(), 3, epoch)?;
            store.append_upsert_with_posting(1, None, &[1.0, 2.0, 3.0])?;
            store.append_upsert_with_posting(2, None, &[4.0, 5.0, 6.0])?;
            store.append_upsert_with_posting(1, None, &[7.0, 8.0, 9.0])?;
            store.remap()?;
            assert_eq!(store.live_len(), 2);
        }

        let sidecar = index_file_path_for_epoch(dir.path(), epoch);
        let sidecar_len = std::fs::metadata(&sidecar)?.len();
        let trim = u64::try_from(INDEX_RECORD_SIZE).expect("index record size fits u64");
        assert!(
            sidecar_len > trim,
            "sidecar should contain at least one record"
        );
        let f = OpenOptions::new().write(true).open(&sidecar)?;
        f.set_len(sidecar_len - trim)?;

        {
            let store = VectorBlockStore::open(dir.path(), 3, epoch)?;
            assert_eq!(store.live_len(), 2);
            assert_eq!(store.get(1), Some(vec![7.0, 8.0, 9.0]));
            assert_eq!(store.get(2), Some(vec![4.0, 5.0, 6.0]));
        }
        Ok(())
    }
}
