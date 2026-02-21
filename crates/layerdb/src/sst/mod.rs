//! SSTable (Sorted String Table) format.
//!
//! v1 goals:
//! - Simple on-disk format with verifiable checksums.
//! - Point lookups via index + binary search within blocks.
//! - Iteration via block streaming.
//!
//! File layout:
//! ```text
//! [data blocks...]
//! [index block]
//! [properties block]
//! [footer]
//! ```
//!
//! Data block format:
//! ```text
//! [count u32]
//! repeated count times:
//!   [internal_key]
//!   [val_len u32][val bytes]
//! [entry_offsets u32 * count] (format v3+)
//! [trailer]
//! ```
//!
//! Trailer format:
//! - crc32c(u32) over block payload (everything before trailer)
//! - blake3(32 bytes) over block payload
//!
//! Index block entries map `last_internal_key_in_block -> {offset,len}`.

use std::cmp::Ordering;
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};

use crate::integrity::{BlockCrc32c, BlockHash, RecordHasher};
use crate::internal_key::{InternalKey, KeyKind};
use crate::range_tombstone::RangeTombstone;
use crate::{cache::BlockCacheKey, cache::BlockKind, cache::ClockProCache};

mod builder;
mod iter;

pub use builder::SstBuilder;
pub use iter::SstIter;

pub type DataBlockEntries = Vec<(InternalKey, Bytes)>;
pub type DataBlockCache = ClockProCache<BlockCacheKey, DataBlockEntries>;
pub type DataBlockCacheHandle = Arc<DataBlockCache>;

#[derive(Debug, thiserror::Error)]
pub enum SstError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("decode error: {0}")]
    Decode(#[from] crate::internal_key::DecodeError),

    #[error("sst corrupt: {0}")]
    Corrupt(&'static str),

    #[error("sst not found")]
    NotFound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRoot(pub [u8; 32]);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstProperties {
    pub smallest_user_key: Bytes,
    pub largest_user_key: Bytes,
    pub max_seqno: u64,
    pub entries: u64,
    pub data_bytes: u64,
    pub index_bytes: u64,
    pub table_root: TableRoot,

    /// SST format version.
    ///
    /// - v1: range tombstones are stored as point entries only.
    /// - v2: `range_tombstones` is populated from the write path.
    /// - v3: data blocks include a per-entry offset index for faster point lookups.
    #[serde(default = "default_sst_format_version")]
    pub format_version: u32,

    /// Range tombstones stored as metadata (v2+).
    ///
    /// When present, readers can avoid scanning the full table to discover
    /// tombstones.
    #[serde(default)]
    pub range_tombstones: Vec<RangeTombstone>,

    /// Optional bloom filter over point user keys.
    ///
    /// Encoded as bincode bytes for backward-compatible persistence.
    #[serde(default)]
    pub point_filter: Option<Vec<u8>>,
}

fn default_sst_format_version() -> u32 {
    1
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct BlockHandle {
    offset: u64,
    len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexEntry {
    last_key: InternalKey,
    handle: BlockHandle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Footer {
    index_offset: u64,
    index_len: u32,
    props_offset: u64,
    props_len: u32,
    table_root: TableRoot,
}

const MAGIC: &[u8; 8] = b"LAYERDB1";
// Footer is appended after the properties block.
//
// Layout:
// - index_offset (u64)
// - index_len (u32)
// - props_offset (u64)
// - props_len (u32)
// - table_root (32 bytes)
const FOOTER_SIZE: usize = 8 + 4 + 8 + 4 + 32;
const BLOCK_TRAILER_SIZE: usize = 4 + 32;

fn verified_block_payload(block: &[u8]) -> Result<&[u8], SstError> {
    if block.len() <= BLOCK_TRAILER_SIZE {
        return Err(SstError::Corrupt("block too small"));
    }

    let payload_end = block.len() - BLOCK_TRAILER_SIZE;
    let payload = &block[..payload_end];
    let crc_expected =
        u32::from_le_bytes(block[payload_end..(payload_end + 4)].try_into().unwrap());
    let hash_expected: [u8; 32] = block[(payload_end + 4)..].try_into().unwrap();

    if !RecordHasher::verify_crc32c(payload, BlockCrc32c(crc_expected)) {
        return Err(SstError::Corrupt("block crc mismatch"));
    }
    if !RecordHasher::verify_blake3(payload, BlockHash(hash_expected)) {
        return Err(SstError::Corrupt("block hash mismatch"));
    }

    Ok(payload)
}

fn point_get_from_payload(
    payload: &[u8],
    user_key: &[u8],
    snapshot_seqno: u64,
    format_version: u32,
) -> Result<Option<(u64, Option<Bytes>)>, SstError> {
    if format_version >= 3 {
        return point_get_from_payload_with_offset_index(payload, user_key, snapshot_seqno);
    }

    point_get_from_payload_linear(payload, user_key, snapshot_seqno)
}

fn point_get_from_payload_linear(
    payload: &[u8],
    user_key: &[u8],
    snapshot_seqno: u64,
) -> Result<Option<(u64, Option<Bytes>)>, SstError> {
    if payload.len() < 4 {
        return Err(SstError::Corrupt("block payload too small"));
    }

    let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut offset = 4usize;

    for _ in 0..count {
        if offset + 4 > payload.len() {
            return Err(SstError::Corrupt("truncated internal key"));
        }
        let user_key_len =
            u32::from_le_bytes(payload[offset..(offset + 4)].try_into().unwrap()) as usize;
        offset += 4;

        if offset + user_key_len + 8 + 1 > payload.len() {
            return Err(SstError::Corrupt("truncated internal key bytes"));
        }
        let entry_user_key = &payload[offset..(offset + user_key_len)];
        offset += user_key_len;
        let seqno = u64::from_le_bytes(payload[offset..(offset + 8)].try_into().unwrap());
        offset += 8;
        let kind =
            KeyKind::from_u8(payload[offset]).map_err(|_| SstError::Corrupt("unknown key kind"))?;
        offset += 1;

        if offset + 4 > payload.len() {
            return Err(SstError::Corrupt("truncated value len"));
        }
        let val_len =
            u32::from_le_bytes(payload[offset..(offset + 4)].try_into().unwrap()) as usize;
        offset += 4;
        if offset + val_len > payload.len() {
            return Err(SstError::Corrupt("truncated value bytes"));
        }
        let value = &payload[offset..(offset + val_len)];
        offset += val_len;

        match entry_user_key.cmp(user_key) {
            Ordering::Less => continue,
            Ordering::Greater => break,
            Ordering::Equal => {
                if seqno > snapshot_seqno {
                    continue;
                }

                return Ok(match kind {
                    KeyKind::Put => Some((seqno, Some(Bytes::copy_from_slice(value)))),
                    KeyKind::Del => Some((seqno, None)),
                    _ => None,
                });
            }
        }
    }

    Ok(None)
}

fn point_get_from_payload_with_offset_index(
    payload: &[u8],
    user_key: &[u8],
    snapshot_seqno: u64,
) -> Result<Option<(u64, Option<Bytes>)>, SstError> {
    type ParsedEntry<'a> = (&'a [u8], u64, KeyKind, &'a [u8]);

    fn parse_entry(
        payload: &[u8],
        entries_end: usize,
        entry_offset: usize,
    ) -> Result<ParsedEntry<'_>, SstError> {
        if entry_offset + 4 > entries_end {
            return Err(SstError::Corrupt("truncated internal key"));
        }

        let user_key_len =
            u32::from_le_bytes(payload[entry_offset..entry_offset + 4].try_into().unwrap())
                as usize;
        let mut offset = entry_offset + 4;
        if offset + user_key_len + 8 + 1 > entries_end {
            return Err(SstError::Corrupt("truncated internal key bytes"));
        }

        let entry_user_key = &payload[offset..offset + user_key_len];
        offset += user_key_len;
        let seqno = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let kind =
            KeyKind::from_u8(payload[offset]).map_err(|_| SstError::Corrupt("unknown key kind"))?;
        offset += 1;

        if offset + 4 > entries_end {
            return Err(SstError::Corrupt("truncated value len"));
        }
        let val_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + val_len > entries_end {
            return Err(SstError::Corrupt("truncated value bytes"));
        }

        Ok((
            entry_user_key,
            seqno,
            kind,
            &payload[offset..offset + val_len],
        ))
    }

    if payload.len() < 4 {
        return Err(SstError::Corrupt("block payload too small"));
    }

    let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    if count == 0 {
        return Ok(None);
    }

    let offsets_bytes = count
        .checked_mul(4)
        .ok_or(SstError::Corrupt("block offset index too large"))?;
    if payload.len() < 4 + offsets_bytes {
        return Err(SstError::Corrupt(
            "block payload too small for offset index",
        ));
    }
    let entries_end = payload.len() - offsets_bytes;
    if entries_end < 4 {
        return Err(SstError::Corrupt("block offset index overlaps header"));
    }

    let offset_at = |idx: usize| -> Result<usize, SstError> {
        if idx >= count {
            return Err(SstError::Corrupt("block offset index out of bounds"));
        }
        let start = entries_end + idx * 4;
        let raw = u32::from_le_bytes(payload[start..start + 4].try_into().unwrap()) as usize;
        if raw < 4 || raw >= entries_end {
            return Err(SstError::Corrupt("block offset index corrupt"));
        }
        Ok(raw)
    };

    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let entry_offset = offset_at(mid)?;
        let (entry_user_key, _seqno, _kind, _value) =
            parse_entry(payload, entries_end, entry_offset)?;

        match entry_user_key.cmp(user_key) {
            Ordering::Less => lo = mid + 1,
            Ordering::Greater | Ordering::Equal => hi = mid,
        }
    }

    if lo >= count {
        return Ok(None);
    }

    for idx in lo..count {
        let entry_offset = offset_at(idx)?;
        let (entry_user_key, seqno, kind, value) = parse_entry(payload, entries_end, entry_offset)?;
        if entry_user_key != user_key {
            break;
        }
        if seqno > snapshot_seqno {
            continue;
        }

        return Ok(match kind {
            KeyKind::Put => Some((seqno, Some(Bytes::copy_from_slice(value)))),
            KeyKind::Del => Some((seqno, None)),
            _ => None,
        });
    }

    Ok(None)
}

pub struct SstReader {
    sst_id: u64,
    mmap: Mmap,
    index: Vec<IndexEntry>,
    props: SstProperties,
    range_tombstones_cache: parking_lot::Mutex<Option<Vec<RangeTombstone>>>,
    data_block_cache: Option<DataBlockCacheHandle>,
    point_filter: Option<bloomfilter::Bloom<Bytes>>,

    io_ctx: Option<Arc<SstIoContext>>,
    io_file: Option<std::fs::File>,
    io_fixed_fd: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct SstIoContext {
    io: crate::io::UringExecutor,
    buf_pool: crate::io::BufPool,
}

impl SstIoContext {
    pub fn new(io: crate::io::UringExecutor, buf_pool: crate::io::BufPool) -> Self {
        Self { io, buf_pool }
    }

    pub fn io(&self) -> &crate::io::UringExecutor {
        &self.io
    }
}

impl SstReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SstError> {
        Self::open_with_cache(path, None)
    }

    pub fn open_with_io(
        path: impl AsRef<Path>,
        io_ctx: Arc<SstIoContext>,
        data_block_cache: Option<DataBlockCacheHandle>,
    ) -> Result<Self, SstError> {
        Self::open_inner(path.as_ref(), Some(io_ctx), data_block_cache)
    }

    pub fn open_with_cache(
        path: impl AsRef<Path>,
        data_block_cache: Option<DataBlockCacheHandle>,
    ) -> Result<Self, SstError> {
        Self::open_inner(path.as_ref(), None, data_block_cache)
    }

    fn open_inner(
        path: &Path,
        io_ctx: Option<Arc<SstIoContext>>,
        data_block_cache: Option<DataBlockCacheHandle>,
    ) -> Result<Self, SstError> {
        let path = path.to_path_buf();
        let keep_file = io_ctx.is_some();
        let file = std::fs::File::open(&path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        if mmap.len() < MAGIC.len() + FOOTER_SIZE {
            return Err(SstError::Corrupt("file too small"));
        }

        let footer_offset = mmap.len() - (MAGIC.len() + FOOTER_SIZE);
        if &mmap[(mmap.len() - MAGIC.len())..] != MAGIC {
            return Err(SstError::Corrupt("bad magic"));
        }
        let footer = decode_footer(&mmap[footer_offset..(footer_offset + FOOTER_SIZE)])?;

        let index_start = footer.index_offset as usize;
        let index_end = index_start + footer.index_len as usize;
        let props_start = footer.props_offset as usize;
        let props_end = props_start + footer.props_len as usize;
        if index_end > mmap.len() || props_end > mmap.len() {
            return Err(SstError::Corrupt("bad footer offsets"));
        }

        let index: Vec<IndexEntry> = bincode::deserialize(&mmap[index_start..index_end])
            .map_err(|_| SstError::Corrupt("index decode"))?;
        let props: SstProperties = bincode::deserialize(&mmap[props_start..props_end])
            .map_err(|_| SstError::Corrupt("props decode"))?;
        if props.table_root != footer.table_root {
            return Err(SstError::Corrupt("table root mismatch"));
        }

        let mut range_tombstones_cache = None;
        if props.format_version >= 2 {
            let mut cached = props.range_tombstones.clone();
            cached.sort_by(|a, b| b.seqno.cmp(&a.seqno));
            range_tombstones_cache = Some(cached);
        }

        let sst_id = file_id_from_path(&path).unwrap_or(0);
        let point_filter = props
            .point_filter
            .as_ref()
            .and_then(|raw| decode_point_filter(raw));

        let (io_file, io_fixed_fd) = if keep_file {
            let fixed = io_ctx
                .as_ref()
                .and_then(|ctx| ctx.io.register_file_blocking(&file));
            (Some(file), fixed)
        } else {
            (None, None)
        };

        Ok(Self {
            sst_id,
            mmap,
            index,
            props,
            range_tombstones_cache: parking_lot::Mutex::new(range_tombstones_cache),
            data_block_cache,
            point_filter,
            io_ctx,
            io_file,
            io_fixed_fd,
        })
    }

    fn io_fixed_fd(&self) -> Option<u32> {
        self.io_fixed_fd
    }

    pub fn properties(&self) -> &SstProperties {
        &self.props
    }

    pub fn get(
        &self,
        user_key: &[u8],
        snapshot_seqno: u64,
    ) -> Result<Option<(u64, Option<Bytes>)>, SstError> {
        let tombstone_seq = self.max_covering_tombstone_seq(user_key, snapshot_seqno)?;
        if self.point_filter.as_ref().is_some_and(|filter| {
            let key = Bytes::copy_from_slice(user_key);
            !filter.check(&key)
        }) {
            return Ok(tombstone_seq.map(|seq| (seq, None)));
        }

        let target = InternalKey::new(
            Bytes::copy_from_slice(user_key),
            snapshot_seqno,
            KeyKind::Meta,
        );
        let candidate = if let Some((block_no, block)) = self.find_block(&target) {
            if self.data_block_cache.is_some() {
                let entries = self.read_block(block_no, block)?;

                let pos = match entries.binary_search_by(|(k, _)| k.cmp(&target)) {
                    Ok(i) | Err(i) => i,
                };
                match entries.get(pos) {
                    Some((k, v)) if k.user_key.as_ref() == user_key => match k.kind {
                        KeyKind::Put => Some((k.seqno, Some(v.clone()))),
                        KeyKind::Del => Some((k.seqno, None)),
                        _ => None,
                    },
                    _ => None,
                }
            } else {
                self.get_from_block_handle_fast(block, user_key, snapshot_seqno)?
            }
        } else {
            None
        };

        match (candidate, tombstone_seq) {
            (Some((seq, value)), Some(tseq)) => {
                if tseq >= seq {
                    Ok(Some((tseq, None)))
                } else {
                    Ok(Some((seq, value)))
                }
            }
            (Some((seq, value)), None) => Ok(Some((seq, value))),
            (None, Some(tseq)) => Ok(Some((tseq, None))),
            (None, None) => Ok(None),
        }
    }

    pub fn range_tombstones(&self, snapshot_seqno: u64) -> Result<Vec<RangeTombstone>, SstError> {
        let all = self.load_range_tombstones_all()?;
        Ok(all
            .iter()
            .filter(|t| t.seqno <= snapshot_seqno)
            .cloned()
            .collect())
    }

    fn max_covering_tombstone_seq(
        &self,
        user_key: &[u8],
        snapshot_seqno: u64,
    ) -> Result<Option<u64>, SstError> {
        let all = self.load_range_tombstones_all()?;
        Ok(all
            .iter()
            .filter(|t| {
                t.seqno <= snapshot_seqno
                    && t.start_key.as_ref() <= user_key
                    && user_key < t.end_key.as_ref()
            })
            .map(|t| t.seqno)
            .max())
    }

    fn load_range_tombstones_all(&self) -> Result<Vec<RangeTombstone>, SstError> {
        if let Some(cached) = self.range_tombstones_cache.lock().as_ref() {
            return Ok(cached.clone());
        }

        if self.props.format_version >= 2 {
            let mut cached = self.props.range_tombstones.clone();
            cached.sort_by(|a, b| b.seqno.cmp(&a.seqno));
            *self.range_tombstones_cache.lock() = Some(cached.clone());
            return Ok(cached);
        }

        let mut out = Vec::new();
        let mut iter = self.iter(u64::MAX)?;
        iter.seek_to_first();
        for next in iter {
            let (key, seqno, kind, value) = next?;
            if kind != crate::db::OpKind::RangeDel {
                continue;
            }
            out.push(RangeTombstone {
                start_key: key,
                end_key: value,
                seqno,
            });
        }
        out.sort_by(|a, b| b.seqno.cmp(&a.seqno));
        *self.range_tombstones_cache.lock() = Some(out.clone());
        Ok(out)
    }

    pub fn iter(&self, snapshot_seqno: u64) -> Result<SstIter<'_>, SstError> {
        Ok(SstIter::new(self, snapshot_seqno))
    }

    fn find_block(&self, target: &InternalKey) -> Option<(u32, BlockHandle)> {
        if self.index.is_empty() {
            return None;
        }
        let mut lo = 0usize;
        let mut hi = self.index.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if &self.index[mid].last_key < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        self.index
            .get(lo)
            .map(|e| (lo.try_into().expect("sst block index fits u32"), e.handle))
    }

    fn read_block(
        &self,
        block_no: u32,
        handle: BlockHandle,
    ) -> Result<Arc<Vec<(InternalKey, Bytes)>>, SstError> {
        if let Some(cache) = &self.data_block_cache {
            let cache_key = BlockCacheKey::new(self.sst_id, BlockKind::Data, block_no);
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached);
            }

            let decoded = self.decode_block(handle)?;
            let decoded = Arc::new(decoded);
            cache.insert(cache_key, decoded.clone());
            return Ok(decoded);
        }

        Ok(Arc::new(self.decode_block(handle)?))
    }

    fn decode_block(&self, handle: BlockHandle) -> Result<Vec<(InternalKey, Bytes)>, SstError> {
        if let Some(io_ctx) = self.io_ctx.as_ref() {
            return self.decode_block_io(io_ctx, handle);
        }

        let start = handle.offset as usize;
        let end = start + handle.len as usize;
        if end > self.mmap.len() {
            return Err(SstError::Corrupt("block handle out of bounds"));
        }

        let payload = verified_block_payload(&self.mmap[start..end])?;

        if payload.len() < 4 {
            return Err(SstError::Corrupt("block payload too small"));
        }
        let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
        let mut offset = 4usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let (ikey, used) = InternalKey::decode(&payload[offset..])?;
            offset += used;
            if offset + 4 > payload.len() {
                return Err(SstError::Corrupt("truncated value"));
            }
            let val_len =
                u32::from_le_bytes(payload[offset..(offset + 4)].try_into().unwrap()) as usize;
            offset += 4;
            if offset + val_len > payload.len() {
                return Err(SstError::Corrupt("truncated value bytes"));
            }
            let value = Bytes::copy_from_slice(&payload[offset..(offset + val_len)]);
            offset += val_len;
            out.push((ikey, value));
        }
        Ok(out)
    }

    fn decode_block_io(
        &self,
        io_ctx: &SstIoContext,
        handle: BlockHandle,
    ) -> Result<Vec<(InternalKey, Bytes)>, SstError> {
        let mut buf = io_ctx.buf_pool.acquire(handle.len as usize);
        buf.resize(handle.len as usize, 0u8);

        let file = self
            .io_file
            .as_ref()
            .ok_or(SstError::Corrupt("io ctx missing file"))?;

        if let Some(buf_index) = buf.fixed_buf_index() {
            io_ctx
                .io
                .read_into_at_file_fixed_buf_blocking(
                    file,
                    self.io_fixed_fd(),
                    handle.offset,
                    buf_index,
                    &mut buf,
                )
                .map_err(|_| SstError::Corrupt("io read"))?;
        } else if let Some(fixed) = self.io_fixed_fd() {
            io_ctx
                .io
                .read_into_at_file_blocking_fixed(file, fixed, handle.offset, &mut buf)
                .map_err(|_| SstError::Corrupt("io read"))?;
        } else {
            io_ctx
                .io
                .read_into_at_file_blocking(file, handle.offset, &mut buf)
                .map_err(|_| SstError::Corrupt("io read"))?;
        }

        let payload = verified_block_payload(&buf)?;

        if payload.len() < 4 {
            return Err(SstError::Corrupt("block payload too small"));
        }
        let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
        let mut offset = 4usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let (ikey, used) = InternalKey::decode(&payload[offset..])?;
            offset += used;
            if offset + 4 > payload.len() {
                return Err(SstError::Corrupt("truncated value"));
            }
            let val_len =
                u32::from_le_bytes(payload[offset..(offset + 4)].try_into().unwrap()) as usize;
            offset += 4;
            if offset + val_len > payload.len() {
                return Err(SstError::Corrupt("truncated value bytes"));
            }
            let value = Bytes::copy_from_slice(&payload[offset..(offset + val_len)]);
            offset += val_len;
            out.push((ikey, value));
        }
        Ok(out)
    }

    fn get_from_block_handle_fast(
        &self,
        handle: BlockHandle,
        user_key: &[u8],
        snapshot_seqno: u64,
    ) -> Result<Option<(u64, Option<Bytes>)>, SstError> {
        if let Some(io_ctx) = self.io_ctx.as_ref() {
            let mut buf = io_ctx.buf_pool.acquire(handle.len as usize);
            buf.resize(handle.len as usize, 0u8);

            let file = self
                .io_file
                .as_ref()
                .ok_or(SstError::Corrupt("io ctx missing file"))?;

            if let Some(buf_index) = buf.fixed_buf_index() {
                io_ctx
                    .io
                    .read_into_at_file_fixed_buf_blocking(
                        file,
                        self.io_fixed_fd(),
                        handle.offset,
                        buf_index,
                        &mut buf,
                    )
                    .map_err(|_| SstError::Corrupt("io read"))?;
            } else if let Some(fixed) = self.io_fixed_fd() {
                io_ctx
                    .io
                    .read_into_at_file_blocking_fixed(file, fixed, handle.offset, &mut buf)
                    .map_err(|_| SstError::Corrupt("io read"))?;
            } else {
                io_ctx
                    .io
                    .read_into_at_file_blocking(file, handle.offset, &mut buf)
                    .map_err(|_| SstError::Corrupt("io read"))?;
            }

            let payload = verified_block_payload(&buf)?;
            return point_get_from_payload(
                payload,
                user_key,
                snapshot_seqno,
                self.props.format_version,
            );
        }

        let start = handle.offset as usize;
        let end = start + handle.len as usize;
        if end > self.mmap.len() {
            return Err(SstError::Corrupt("block handle out of bounds"));
        }

        let payload = verified_block_payload(&self.mmap[start..end])?;
        point_get_from_payload(payload, user_key, snapshot_seqno, self.props.format_version)
    }
}

impl Drop for SstReader {
    fn drop(&mut self) {
        let Some(io_ctx) = self.io_ctx.as_ref() else {
            return;
        };
        let Some(fixed) = self.io_fixed_fd.take() else {
            return;
        };
        io_ctx.io.unregister_file_blocking(fixed);
    }
}

fn encode_footer(footer: &Footer) -> Vec<u8> {
    let mut buf = Vec::with_capacity(FOOTER_SIZE);
    buf.extend_from_slice(&footer.index_offset.to_le_bytes());
    buf.extend_from_slice(&footer.index_len.to_le_bytes());
    buf.extend_from_slice(&footer.props_offset.to_le_bytes());
    buf.extend_from_slice(&footer.props_len.to_le_bytes());
    buf.extend_from_slice(&footer.table_root.0);
    debug_assert_eq!(buf.len(), FOOTER_SIZE);
    buf
}

fn decode_footer(input: &[u8]) -> Result<Footer, SstError> {
    if input.len() != FOOTER_SIZE {
        return Err(SstError::Corrupt("bad footer size"));
    }
    let index_offset = u64::from_le_bytes(input[0..8].try_into().unwrap());
    let index_len = u32::from_le_bytes(input[8..12].try_into().unwrap());
    let props_offset = u64::from_le_bytes(input[12..20].try_into().unwrap());
    let props_len = u32::from_le_bytes(input[20..24].try_into().unwrap());
    let table_root: [u8; 32] = input[24..56].try_into().unwrap();
    Ok(Footer {
        index_offset,
        index_len,
        props_offset,
        props_len,
        table_root: TableRoot(table_root),
    })
}

fn fsync_parent_dir(path: &Path) -> Result<(), SstError> {
    let parent = path
        .parent()
        .ok_or(SstError::Corrupt("missing parent dir"))?;
    let dir_fd = std::fs::File::open(parent)?;
    dir_fd.sync_all()?;
    Ok(())
}

pub fn file_id_from_path(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    if !name.starts_with("sst_") || !name.ends_with(".sst") {
        return None;
    }
    let inner = &name[4..(name.len() - 4)];
    u64::from_str_radix(inner, 16).ok()
}

fn _cmp_internal(a: &InternalKey, b: &InternalKey) -> Ordering {
    a.cmp(b)
}

fn build_point_filter(point_keys: &[Bytes]) -> Result<Option<Vec<u8>>, SstError> {
    if point_keys.is_empty() {
        return Ok(None);
    }

    let seed = blake3::hash(b"layerdb_point_filter_seed");
    let mut seed_bytes = [0u8; 32];
    seed_bytes.copy_from_slice(seed.as_bytes());

    let mut bloom =
        bloomfilter::Bloom::new_for_fp_rate_with_seed(point_keys.len(), 0.01, &seed_bytes);
    for key in point_keys {
        bloom.set(key);
    }

    let raw = bincode::serialize(&bloom).map_err(|_| SstError::Corrupt("point filter encode"))?;
    Ok(Some(raw))
}

fn decode_point_filter(raw: &[u8]) -> Option<bloomfilter::Bloom<Bytes>> {
    bincode::deserialize(raw).ok()
}
