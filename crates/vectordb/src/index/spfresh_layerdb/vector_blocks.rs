use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;
use memmap2::Mmap;
use rustc_hash::FxHashMap;

const VECTOR_BLOCKS_DIR: &str = "vector_blocks";
const VECTOR_BLOCK_FILE_EXT: &str = "vb";
const VECTOR_BLOCK_HEADER_TAG: &[u8] = b"vb1";
const VECTOR_BLOCK_TOMBSTONE: u8 = 1;
const VECTOR_BLOCK_LIVE: u8 = 0;
const VECTOR_BLOCK_HAS_POSTING: u8 = 1 << 1;
const REMAP_INTERVAL_RECORDS: usize = 1_024;

fn record_size(dim: usize) -> usize {
    8 + 1 + 8 + dim.saturating_mul(4)
}

fn header_size() -> usize {
    VECTOR_BLOCK_HEADER_TAG.len() + 4
}

fn file_path_for_epoch(root: &Path, epoch: u64) -> PathBuf {
    root.join(VECTOR_BLOCKS_DIR)
        .join(format!("epoch-{epoch:016x}.{VECTOR_BLOCK_FILE_EXT}"))
}

#[derive(Debug)]
pub(crate) struct VectorBlockStore {
    root: PathBuf,
    dim: usize,
    epoch: u64,
    file: std::fs::File,
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
    pub(crate) fn open(root: impl AsRef<Path>, dim: usize, epoch: u64) -> anyhow::Result<Self> {
        let root = root.as_ref().to_path_buf();
        let blocks_dir = root.join(VECTOR_BLOCKS_DIR);
        std::fs::create_dir_all(&blocks_dir)
            .with_context(|| format!("create vector blocks dir {}", blocks_dir.display()))?;
        let path = file_path_for_epoch(&root, epoch);
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

        let mut store = Self {
            root,
            dim,
            epoch,
            file,
            mmap: None,
            offsets: FxHashMap::default(),
            pending_remap_records: 0,
        };
        store.validate_header()?;
        store.rebuild_offsets()?;
        store.remap()?;
        Ok(store)
    }

    pub(crate) fn rotate_epoch(&mut self, epoch: u64) -> anyhow::Result<()> {
        if self.epoch == epoch {
            return Ok(());
        }
        let path = file_path_for_epoch(&self.root, epoch);
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
            let dim_u32 = u32::try_from(self.dim).context("vector blocks dim does not fit u32")?;
            header.extend_from_slice(&dim_u32.to_le_bytes());
            file.write_all(&header)
                .with_context(|| format!("write vector blocks header {}", path.display()))?;
            file.flush()
                .with_context(|| format!("flush vector blocks header {}", path.display()))?;
        }

        self.file = file;
        self.epoch = epoch;
        self.offsets.clear();
        self.pending_remap_records = 0;
        self.validate_header()?;
        self.rebuild_offsets()?;
        self.remap()?;
        Ok(())
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
        let mut buf = Vec::with_capacity(rec_size.saturating_mul(rows.len()));
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

            buf.extend_from_slice(&id.to_le_bytes());
            buf.push(flags);
            buf.extend_from_slice(&posting_u64.unwrap_or_default().to_le_bytes());
            for value in values {
                buf.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            self.offsets.insert(*id, offset);
            offset = offset
                .checked_add(u64::try_from(rec_size).context("vector block record size overflow")?)
                .ok_or_else(|| anyhow::anyhow!("vector block offset overflow"))?;
        }

        self.file
            .write_all(&buf)
            .context("append vector block upsert batch")?;
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
        let mut buf = Vec::with_capacity(rec_size.saturating_mul(ids.len()));
        self.file
            .seek(SeekFrom::End(0))
            .context("seek vector blocks end")?;
        for id in ids {
            buf.extend_from_slice(&id.to_le_bytes());
            buf.push(VECTOR_BLOCK_TOMBSTONE);
            buf.extend_from_slice(&0u64.to_le_bytes());
            buf.resize(buf.len().saturating_add(rec_size.saturating_sub(17)), 0);
            self.offsets.remove(id);
        }
        self.file
            .write_all(&buf)
            .context("append vector block tombstone batch")?;
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
        for id in ids {
            let Some(offset) = self.offsets.get(id).copied() else {
                missing.push(*id);
                continue;
            };
            let Some(start) = usize::try_from(offset).ok() else {
                missing.push(*id);
                continue;
            };
            let Some(end) = start.checked_add(rec_size) else {
                missing.push(*id);
                continue;
            };
            let Some(record) = mmap.get(start..end) else {
                missing.push(*id);
                continue;
            };
            let flags = record[8];
            if (flags & VECTOR_BLOCK_TOMBSTONE) != 0 {
                missing.push(*id);
                continue;
            }

            let mut distance = 0.0f32;
            let mut cursor = 17usize;
            for query_value in query {
                let Some(bits_bytes) = record.get(cursor..cursor + 4) else {
                    distance = f32::INFINITY;
                    break;
                };
                let mut bits = [0u8; 4];
                bits.copy_from_slice(bits_bytes);
                let value = f32::from_bits(u32::from_le_bytes(bits));
                let delta = *query_value - value;
                distance += delta * delta;
                cursor += 4;
            }
            if distance.is_finite() {
                found.push((*id, distance));
            } else {
                missing.push(*id);
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
            let bits_bytes = record.get(cursor..cursor + 4)?;
            let mut arr = [0u8; 4];
            arr.copy_from_slice(bits_bytes);
            values.push(f32::from_bits(u32::from_le_bytes(arr)));
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

    fn rebuild_offsets(&mut self) -> anyhow::Result<()> {
        self.offsets.clear();
        self.file
            .seek(SeekFrom::Start(0))
            .context("seek vector blocks before scan")?;
        let mut bytes = Vec::new();
        self.file
            .read_to_end(&mut bytes)
            .context("read vector blocks for offset scan")?;
        if bytes.len() < header_size() {
            return Ok(());
        }
        let rec_size = record_size(self.dim);
        let mut cursor = header_size();
        while cursor + rec_size <= bytes.len() {
            let record = &bytes[cursor..cursor + rec_size];
            let mut id_arr = [0u8; 8];
            id_arr.copy_from_slice(&record[..8]);
            let id = u64::from_le_bytes(id_arr);
            let tombstone = record[8];
            let offset = u64::try_from(cursor).context("vector block offset overflow")?;
            if tombstone == VECTOR_BLOCK_TOMBSTONE {
                self.offsets.remove(&id);
            } else {
                self.offsets.insert(id, offset);
            }
            cursor += rec_size;
        }
        self.file
            .seek(SeekFrom::End(0))
            .context("seek vector blocks end after scan")?;
        Ok(())
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
        // SAFETY: we only read through mmap snapshots and remap after flushing appends.
        let mmap = unsafe { Mmap::map(&self.file) }.context("mmap vector blocks file")?;
        self.mmap = Some(mmap);
        self.pending_remap_records = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::VectorBlockStore;

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
}
