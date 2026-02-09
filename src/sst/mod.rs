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

use bytes::Bytes;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};

use crate::internal_key::{InternalKey, KeyKind};

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
const FOOTER_SIZE: usize = 8 + 8 + 4 + 8 + 4 + 32;
const BLOCK_TRAILER_SIZE: usize = 4 + 32;

pub struct SstBuilder {
    block_size: usize,
    file: std::fs::File,
    path_tmp: PathBuf,
    path_final: PathBuf,
    buf: Vec<u8>,
    entries_in_block: u32,
    last_key: Option<InternalKey>,
    index: Vec<IndexEntry>,
    smallest_user_key: Option<Bytes>,
    largest_user_key: Option<Bytes>,
    max_seqno: u64,
    entries: u64,
    data_bytes: u64,
    table_hasher: blake3::Hasher,
}

impl SstBuilder {
    pub fn create(dir: &Path, file_id: u64, block_size: usize) -> Result<Self, SstError> {
        std::fs::create_dir_all(dir)?;
        let path_tmp = dir.join(format!("sst_{file_id:016x}.tmp"));
        let path_final = dir.join(format!("sst_{file_id:016x}.sst"));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path_tmp)?;
        Ok(Self {
            block_size,
            file,
            path_tmp,
            path_final,
            buf: Vec::with_capacity(block_size + 256),
            entries_in_block: 0,
            last_key: None,
            index: Vec::new(),
            smallest_user_key: None,
            largest_user_key: None,
            max_seqno: 0,
            entries: 0,
            data_bytes: 0,
            table_hasher: blake3::Hasher::new(),
        })
    }

    pub fn add(&mut self, key: &InternalKey, value: &[u8]) -> Result<(), SstError> {
        if let Some(last) = &self.last_key {
            if key < last {
                return Err(SstError::Corrupt(
                    "internal keys must be added in sorted order",
                ));
            }
        }

        if self.smallest_user_key.is_none() {
            self.smallest_user_key = Some(key.user_key.clone());
        }
        self.largest_user_key = Some(key.user_key.clone());
        self.last_key = Some(key.clone());
        self.max_seqno = self.max_seqno.max(key.seqno);
        self.entries += 1;

        if self.entries_in_block == 0 {
            self.buf.extend_from_slice(&0u32.to_le_bytes());
        }
        key.encode_into(&mut self.buf);
        let val_len: u32 = value
            .len()
            .try_into()
            .map_err(|_| SstError::Corrupt("value too large"))?;
        self.buf.extend_from_slice(&val_len.to_le_bytes());
        self.buf.extend_from_slice(value);
        self.entries_in_block += 1;
        self.buf[0..4].copy_from_slice(&self.entries_in_block.to_le_bytes());

        if self.buf.len() >= self.block_size {
            self.flush_block()?;
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<SstProperties, SstError> {
        if self.entries_in_block > 0 {
            self.flush_block()?;
        }

        let index_offset = self.file.stream_position()?;
        let index_bytes =
            bincode::serialize(&self.index).map_err(|_| SstError::Corrupt("index serialize"))?;
        self.file.write_all(&index_bytes)?;
        let index_len: u32 = index_bytes
            .len()
            .try_into()
            .map_err(|_| SstError::Corrupt("index too large"))?;

        let props_offset = self.file.stream_position()?;
        self.table_hasher.update(&index_bytes);
        let table_root = TableRoot(*self.table_hasher.finalize().as_bytes());
        let props = SstProperties {
            smallest_user_key: self.smallest_user_key.clone().unwrap_or_else(Bytes::new),
            largest_user_key: self.largest_user_key.clone().unwrap_or_else(Bytes::new),
            max_seqno: self.max_seqno,
            entries: self.entries,
            data_bytes: self.data_bytes,
            index_bytes: index_bytes.len() as u64,
            table_root,
        };
        let props_bytes =
            bincode::serialize(&props).map_err(|_| SstError::Corrupt("props serialize"))?;
        let props_len: u32 = props_bytes
            .len()
            .try_into()
            .map_err(|_| SstError::Corrupt("props too large"))?;
        self.file.write_all(&props_bytes)?;

        let footer = Footer {
            index_offset,
            index_len,
            props_offset,
            props_len,
            table_root,
        };
        let footer_bytes = encode_footer(&footer);
        self.file.write_all(&footer_bytes)?;
        self.file.sync_data()?;
        drop(self.file);

        std::fs::rename(&self.path_tmp, &self.path_final)?;
        fsync_parent_dir(&self.path_final)?;
        Ok(props)
    }

    fn flush_block(&mut self) -> Result<(), SstError> {
        let payload_len = self.buf.len();
        let crc = crc32c::crc32c(&self.buf);
        let hash = blake3::hash(&self.buf);
        self.table_hasher.update(hash.as_bytes());
        self.buf.extend_from_slice(&crc.to_le_bytes());
        self.buf.extend_from_slice(hash.as_bytes());

        let offset = self.file.stream_position()?;
        self.file.write_all(&self.buf)?;
        let len: u32 = self
            .buf
            .len()
            .try_into()
            .map_err(|_| SstError::Corrupt("block too large"))?;

        let last_key = self
            .last_key
            .clone()
            .ok_or(SstError::Corrupt("missing last key"))?;
        self.index.push(IndexEntry {
            last_key,
            handle: BlockHandle { offset, len },
        });

        self.data_bytes += payload_len as u64;
        self.buf.clear();
        self.entries_in_block = 0;
        Ok(())
    }
}

pub struct SstReader {
    path: PathBuf,
    mmap: Mmap,
    index: Vec<IndexEntry>,
    props: SstProperties,
}

impl SstReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SstError> {
        let path = path.as_ref().to_path_buf();
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

        Ok(Self {
            path,
            mmap,
            index,
            props,
        })
    }

    pub fn properties(&self) -> &SstProperties {
        &self.props
    }

    pub fn get(
        &self,
        user_key: &[u8],
        snapshot_seqno: u64,
    ) -> Result<Option<Option<Bytes>>, SstError> {
        let target = InternalKey::new(
            Bytes::copy_from_slice(user_key),
            snapshot_seqno,
            KeyKind::Meta,
        );
        let block = match self.find_block(&target) {
            None => return Ok(None),
            Some(h) => h,
        };
        let entries = self.read_block(block)?;

        let pos = match entries.binary_search_by(|(k, _)| k.cmp(&target)) {
            Ok(i) | Err(i) => i,
        };
        let (k, v) = match entries.get(pos) {
            None => return Ok(None),
            Some(entry) => entry.clone(),
        };
        if k.user_key.as_ref() != user_key {
            return Ok(None);
        }
        Ok(Some(match k.kind {
            KeyKind::Put => Some(v),
            KeyKind::Del => None,
            _ => None,
        }))
    }

    pub fn iter(&self, snapshot_seqno: u64) -> Result<SstIter<'_>, SstError> {
        Ok(SstIter {
            reader: self,
            snapshot_seqno,
            index_pos: 0,
            seek_target: None,
            entries: Vec::new(),
            entry_pos: 0,
        })
    }

    fn find_block(&self, target: &InternalKey) -> Option<BlockHandle> {
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
        self.index.get(lo).map(|e| e.handle)
    }

    fn read_block(&self, handle: BlockHandle) -> Result<Vec<(InternalKey, Bytes)>, SstError> {
        let start = handle.offset as usize;
        let end = start + handle.len as usize;
        if end > self.mmap.len() {
            return Err(SstError::Corrupt("block handle out of bounds"));
        }
        if handle.len as usize <= BLOCK_TRAILER_SIZE {
            return Err(SstError::Corrupt("block too small"));
        }

        let payload_end = end - BLOCK_TRAILER_SIZE;
        let payload = &self.mmap[start..payload_end];
        let crc_expected = u32::from_le_bytes(
            self.mmap[payload_end..(payload_end + 4)]
                .try_into()
                .unwrap(),
        );
        let hash_expected: [u8; 32] = self.mmap[(payload_end + 4)..end].try_into().unwrap();

        let crc_actual = crc32c::crc32c(payload);
        if crc_actual != crc_expected {
            return Err(SstError::Corrupt("block crc mismatch"));
        }
        let hash_actual = blake3::hash(payload);
        if hash_actual.as_bytes() != &hash_expected {
            return Err(SstError::Corrupt("block hash mismatch"));
        }

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
}

pub struct SstIter<'a> {
    reader: &'a SstReader,
    snapshot_seqno: u64,
    index_pos: usize,
    seek_target: Option<InternalKey>,
    entries: Vec<(InternalKey, Bytes)>,
    entry_pos: usize,
}

impl<'a> SstIter<'a> {
    pub fn seek_to_first(&mut self) {
        self.index_pos = 0;
        self.seek_target = None;
        self.entries.clear();
        self.entry_pos = 0;
    }

    pub fn seek(&mut self, user_key: &[u8]) {
        let target = InternalKey::new(Bytes::copy_from_slice(user_key), u64::MAX, KeyKind::Meta);
        self.index_pos = {
            let mut lo = 0usize;
            let mut hi = self.reader.index.len();
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if self.reader.index[mid].last_key < target {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            lo
        };
        self.seek_target = Some(target);
        self.entries.clear();
        self.entry_pos = 0;
    }

    pub fn next(&mut self) -> Option<Result<(Bytes, u64, crate::db::OpKind, Bytes), SstError>> {
        loop {
            if self.entry_pos >= self.entries.len() {
                if self.index_pos >= self.reader.index.len() {
                    return None;
                }
                let handle = self.reader.index[self.index_pos].handle;
                self.index_pos += 1;
                match self.reader.read_block(handle) {
                    Ok(entries) => {
                        self.entries = entries;
                        self.entry_pos = 0;
                        if let Some(target) = self.seek_target.take() {
                            self.entry_pos =
                                match self.entries.binary_search_by(|(k, _)| k.cmp(&target)) {
                                    Ok(i) | Err(i) => i,
                                };
                        }
                    }
                    Err(e) => return Some(Err(e)),
                }
                continue;
            }

            let (ikey, value) = self.entries[self.entry_pos].clone();
            self.entry_pos += 1;
            if ikey.seqno > self.snapshot_seqno {
                continue;
            }
            let kind = match ikey.kind {
                KeyKind::Put => crate::db::OpKind::Put,
                KeyKind::Del => crate::db::OpKind::Del,
                _ => continue,
            };
            return Some(Ok((ikey.user_key, ikey.seqno, kind, value)));
        }
    }
}

fn encode_footer(footer: &Footer) -> Vec<u8> {
    let mut buf = Vec::with_capacity(FOOTER_SIZE + MAGIC.len());
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
