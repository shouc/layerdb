use std::fmt::Write as _;
use std::ops::Bound;
use std::path::Path;

use anyhow::Context;
use bytes::Bytes;
use layerdb::{Db, Range, ReadOptions, WriteOptions};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::types::VectorRecord;

use super::config::{
    SpFreshLayerDbConfig, SpFreshPersistedMeta, INDEX_WAL_PREFIX, META_ACTIVE_GENERATION_KEY,
    META_CONFIG_KEY, META_INDEX_CHECKPOINT_KEY, META_INDEX_WAL_NEXT_SEQ_KEY,
    META_POSTING_EVENT_NEXT_SEQ_KEY, META_SCHEMA_VERSION, META_STARTUP_MANIFEST_KEY,
    POSTING_MAP_ROOT_PREFIX, POSTING_MEMBERS_ROOT_PREFIX, VECTOR_ROOT_PREFIX,
};

const VECTOR_ROW_BIN_TAG: &[u8] = b"vr3";
const VECTOR_ROW_FLAG_DELETED: u8 = 1 << 0;
const VECTOR_ROW_FLAG_HAS_POSTING: u8 = 1 << 1;

#[derive(Clone, Debug)]
pub(crate) struct DecodedVectorRow {
    pub row: VectorRecord,
    pub posting_id: Option<usize>,
}

fn read_u8(raw: &[u8], cursor: &mut usize) -> anyhow::Result<u8> {
    let Some(b) = raw.get(*cursor).copied() else {
        anyhow::bail!("decode buffer underflow for u8");
    };
    *cursor = cursor.saturating_add(1);
    Ok(b)
}

fn read_u32(raw: &[u8], cursor: &mut usize) -> anyhow::Result<u32> {
    let end = cursor.saturating_add(4);
    let Some(bytes) = raw.get(*cursor..end) else {
        anyhow::bail!("decode buffer underflow for u32");
    };
    let mut arr = [0u8; 4];
    arr.copy_from_slice(bytes);
    *cursor = end;
    Ok(u32::from_le_bytes(arr))
}

fn read_u64(raw: &[u8], cursor: &mut usize) -> anyhow::Result<u64> {
    let end = cursor.saturating_add(8);
    let Some(bytes) = raw.get(*cursor..end) else {
        anyhow::bail!("decode buffer underflow for u64");
    };
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    *cursor = end;
    Ok(u64::from_le_bytes(arr))
}

fn read_f32(raw: &[u8], cursor: &mut usize) -> anyhow::Result<f32> {
    let bits = read_u32(raw, cursor)?;
    Ok(f32::from_bits(bits))
}

pub(crate) fn encode_vector_row_value(row: &VectorRecord) -> anyhow::Result<Vec<u8>> {
    encode_vector_row_fields(row.id, row.version, row.deleted, &row.values, None)
}

pub(crate) fn encode_vector_row_value_with_posting(
    row: &VectorRecord,
    posting_id: Option<usize>,
) -> anyhow::Result<Vec<u8>> {
    encode_vector_row_fields(row.id, row.version, row.deleted, &row.values, posting_id)
}

pub(crate) fn encode_vector_row_fields(
    id: u64,
    version: u32,
    deleted: bool,
    values: &[f32],
    posting_id: Option<usize>,
) -> anyhow::Result<Vec<u8>> {
    let posting_u64 = posting_id
        .map(|v| u64::try_from(v).context("posting id does not fit u64"))
        .transpose()?;
    let mut flags = 0u8;
    if deleted {
        flags |= VECTOR_ROW_FLAG_DELETED;
    }
    if posting_u64.is_some() {
        flags |= VECTOR_ROW_FLAG_HAS_POSTING;
    }
    let mut out = Vec::with_capacity(
        VECTOR_ROW_BIN_TAG.len()
            + 1
            + 4
            + 8
            + 4
            + values.len().saturating_mul(4)
            + posting_u64.map_or(0, |_| 8),
    );
    out.extend_from_slice(VECTOR_ROW_BIN_TAG);
    out.push(flags);
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(&id.to_le_bytes());
    let dim = u32::try_from(values.len()).context("vector row dim does not fit u32")?;
    out.extend_from_slice(&dim.to_le_bytes());
    for value in values {
        out.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    if let Some(posting) = posting_u64 {
        out.extend_from_slice(&posting.to_le_bytes());
    }
    Ok(out)
}

pub(crate) fn decode_vector_row_value(raw: &[u8]) -> anyhow::Result<VectorRecord> {
    Ok(decode_vector_row_with_posting(raw)?.row)
}

pub(crate) fn decode_vector_row_with_posting(raw: &[u8]) -> anyhow::Result<DecodedVectorRow> {
    if !raw.starts_with(VECTOR_ROW_BIN_TAG) {
        anyhow::bail!("unsupported vector row tag");
    }
    let mut cursor = VECTOR_ROW_BIN_TAG.len();
    let flags = read_u8(raw, &mut cursor)?;
    let version = read_u32(raw, &mut cursor)?;
    let id = read_u64(raw, &mut cursor)?;
    let dim = usize::try_from(read_u32(raw, &mut cursor)?).context("vector row dim overflow")?;
    let mut values = Vec::with_capacity(dim);
    for _ in 0..dim {
        values.push(read_f32(raw, &mut cursor)?);
    }
    let posting_id = if (flags & VECTOR_ROW_FLAG_HAS_POSTING) != 0 {
        let posting_u64 = read_u64(raw, &mut cursor)?;
        Some(usize::try_from(posting_u64).context("posting id does not fit usize")?)
    } else {
        None
    };
    if cursor != raw.len() {
        anyhow::bail!("vector row trailing bytes");
    }
    Ok(DecodedVectorRow {
        row: VectorRecord {
            id,
            values,
            version,
            deleted: (flags & VECTOR_ROW_FLAG_DELETED) != 0,
        },
        posting_id,
    })
}

pub(crate) fn validate_config(cfg: &SpFreshLayerDbConfig) -> anyhow::Result<()> {
    if cfg.spfresh.dim == 0 {
        anyhow::bail!("spfresh dim must be > 0");
    }
    if cfg.spfresh.initial_postings == 0 {
        anyhow::bail!("spfresh initial_postings must be > 0");
    }
    if cfg.spfresh.nprobe == 0 {
        anyhow::bail!("spfresh nprobe must be > 0");
    }
    if cfg.spfresh.kmeans_iters == 0 {
        anyhow::bail!("spfresh kmeans_iters must be > 0");
    }
    if cfg.spfresh.split_limit < 4 {
        anyhow::bail!("spfresh split_limit must be >= 4");
    }
    if cfg.spfresh.merge_limit == 0 {
        anyhow::bail!("spfresh merge_limit must be > 0");
    }
    if cfg.spfresh.split_limit <= cfg.spfresh.merge_limit {
        anyhow::bail!(
            "spfresh split_limit ({}) must be > merge_limit ({})",
            cfg.spfresh.split_limit,
            cfg.spfresh.merge_limit
        );
    }
    if cfg.spfresh.reassign_range == 0 {
        anyhow::bail!("spfresh reassign_range must be > 0");
    }
    if cfg.rebuild_pending_ops == 0 {
        anyhow::bail!("spfresh rebuild_pending_ops must be > 0");
    }
    if cfg.rebuild_interval.is_zero() {
        anyhow::bail!("spfresh rebuild_interval must be > 0");
    }
    Ok(())
}

pub(crate) fn load_rows(db: &Db, generation: u64) -> anyhow::Result<Vec<VectorRecord>> {
    let mut out = Vec::new();
    let prefix = vector_prefix(generation);
    let prefix_bytes = prefix.as_bytes().to_vec();
    let end = prefix_exclusive_end(&prefix_bytes)?;
    let mut iter = db.iter(
        Range {
            start: Bound::Included(Bytes::from(prefix_bytes.clone())),
            end: Bound::Excluded(Bytes::from(end)),
        },
        ReadOptions::default(),
    )?;
    iter.seek_to_first();
    for next in iter {
        let (key, value) = next?;
        if !key.starts_with(prefix_bytes.as_slice()) {
            continue;
        }
        let Some(value) = value else {
            continue;
        };
        let row = decode_vector_row_value(value.as_ref())
            .with_context(|| format!("decode vector row key={}", String::from_utf8_lossy(&key)))?;
        if !row.deleted {
            out.push(row);
        }
    }
    Ok(out)
}

pub(crate) fn load_row(db: &Db, generation: u64, id: u64) -> anyhow::Result<Option<VectorRecord>> {
    let key = vector_key(generation, id);
    let raw = db
        .get(key.as_bytes(), ReadOptions::default())
        .with_context(|| format!("load vector row id={id} generation={generation}"))?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let row = decode_vector_row_value(raw.as_ref())
        .with_context(|| format!("decode vector row id={id} generation={generation}"))?;
    Ok((!row.deleted).then_some(row))
}

pub(crate) fn load_rows_with_posting_assignments(
    db: &Db,
    generation: u64,
) -> anyhow::Result<(Vec<VectorRecord>, FxHashMap<u64, usize>)> {
    let mut rows = Vec::new();
    let mut assignments = FxHashMap::default();
    let prefix = vector_prefix(generation);
    let prefix_bytes = prefix.as_bytes().to_vec();
    let end = prefix_exclusive_end(&prefix_bytes)?;
    let mut iter = db.iter(
        Range {
            start: Bound::Included(Bytes::from(prefix_bytes.clone())),
            end: Bound::Excluded(Bytes::from(end)),
        },
        ReadOptions::default(),
    )?;
    iter.seek_to_first();
    for next in iter {
        let (key, value) = next?;
        if !key.starts_with(prefix_bytes.as_slice()) {
            continue;
        }
        let Some(value) = value else {
            continue;
        };
        let decoded = decode_vector_row_with_posting(value.as_ref())
            .with_context(|| format!("decode vector row key={}", String::from_utf8_lossy(&key)))?;
        if decoded.row.deleted {
            continue;
        }
        if let Some(posting_id) = decoded.posting_id {
            assignments.insert(decoded.row.id, posting_id);
        }
        rows.push(decoded.row);
    }
    Ok((rows, assignments))
}

pub(crate) fn ensure_active_generation(db: &Db) -> anyhow::Result<u64> {
    match db
        .get(META_ACTIVE_GENERATION_KEY, ReadOptions::default())
        .context("read spfresh active generation")?
    {
        Some(bytes) => bincode::deserialize(bytes.as_ref()).context("decode active generation"),
        None => {
            set_active_generation(db, 0, true)?;
            Ok(0)
        }
    }
}

pub(crate) fn set_active_generation(db: &Db, generation: u64, sync: bool) -> anyhow::Result<()> {
    let bytes = bincode::serialize(&generation).context("encode active generation")?;
    db.put(META_ACTIVE_GENERATION_KEY, bytes, WriteOptions { sync })
        .context("persist active generation")
}

pub(crate) fn ensure_wal_next_seq(db: &Db) -> anyhow::Result<u64> {
    match db
        .get(META_INDEX_WAL_NEXT_SEQ_KEY, ReadOptions::default())
        .context("read spfresh wal next seq")?
    {
        Some(bytes) => bincode::deserialize(bytes.as_ref()).context("decode wal next seq"),
        None => {
            set_wal_next_seq(db, 0, true)?;
            Ok(0)
        }
    }
}

pub(crate) fn set_wal_next_seq(db: &Db, next_seq: u64, sync: bool) -> anyhow::Result<()> {
    let bytes = bincode::serialize(&next_seq).context("encode wal next seq")?;
    db.put(META_INDEX_WAL_NEXT_SEQ_KEY, bytes, WriteOptions { sync })
        .context("persist wal next seq")
}

pub(crate) fn ensure_posting_event_next_seq(db: &Db) -> anyhow::Result<u64> {
    match db
        .get(META_POSTING_EVENT_NEXT_SEQ_KEY, ReadOptions::default())
        .context("read spfresh posting-event next seq")?
    {
        Some(bytes) => {
            bincode::deserialize(bytes.as_ref()).context("decode posting-event next seq")
        }
        None => {
            set_posting_event_next_seq(db, 0, true)?;
            Ok(0)
        }
    }
}

pub(crate) fn set_posting_event_next_seq(db: &Db, next_seq: u64, sync: bool) -> anyhow::Result<()> {
    let bytes = bincode::serialize(&next_seq).context("encode posting-event next seq")?;
    db.put(
        META_POSTING_EVENT_NEXT_SEQ_KEY,
        bytes,
        WriteOptions { sync },
    )
    .context("persist posting-event next seq")
}

pub(crate) fn refresh_read_snapshot(db: &Db) -> anyhow::Result<()> {
    db.write_batch(
        vec![
            layerdb::Op::put("spfresh/meta/__refresh__", "1"),
            layerdb::Op::delete("spfresh/meta/__refresh__"),
        ],
        WriteOptions { sync: false },
    )
    .context("refresh layerdb read snapshot for recovery")
}

pub(crate) fn ensure_wal_exists(path: &Path) -> anyhow::Result<()> {
    let wal_dir = path.join("wal");
    if !wal_dir.is_dir() {
        anyhow::bail!("layerdb wal directory missing at {}", wal_dir.display());
    }

    let mut has_segment = false;
    for entry in std::fs::read_dir(&wal_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("wal_") && name.ends_with(".log") {
            has_segment = true;
            break;
        }
    }

    if !has_segment {
        anyhow::bail!(
            "layerdb wal contains no segment files in {}",
            wal_dir.display()
        );
    }
    Ok(())
}

pub(crate) fn load_metadata(db: &Db) -> anyhow::Result<Option<SpFreshPersistedMeta>> {
    let current = db
        .get(META_CONFIG_KEY, ReadOptions::default())
        .context("read spfresh metadata")?;
    let Some(value) = current else {
        return Ok(None);
    };
    let meta: SpFreshPersistedMeta =
        bincode::deserialize(value.as_ref()).context("decode spfresh metadata")?;
    Ok(Some(meta))
}

pub(crate) fn ensure_metadata(db: &Db, cfg: &SpFreshLayerDbConfig) -> anyhow::Result<()> {
    let expected = SpFreshPersistedMeta {
        schema_version: META_SCHEMA_VERSION,
        dim: cfg.spfresh.dim,
        initial_postings: cfg.spfresh.initial_postings,
        split_limit: cfg.spfresh.split_limit,
        merge_limit: cfg.spfresh.merge_limit,
        reassign_range: cfg.spfresh.reassign_range,
        nprobe: cfg.spfresh.nprobe,
        kmeans_iters: cfg.spfresh.kmeans_iters,
    };

    if let Some(actual) = load_metadata(db)? {
        if actual.schema_version != expected.schema_version {
            anyhow::bail!(
                "spfresh schema version mismatch: stored={} expected={}",
                actual.schema_version,
                expected.schema_version
            );
        }
        if actual.dim != expected.dim {
            anyhow::bail!(
                "spfresh dim mismatch: stored={} expected={}",
                actual.dim,
                expected.dim
            );
        }
        if actual.initial_postings != expected.initial_postings
            || actual.split_limit != expected.split_limit
            || actual.merge_limit != expected.merge_limit
            || actual.reassign_range != expected.reassign_range
            || actual.nprobe != expected.nprobe
            || actual.kmeans_iters != expected.kmeans_iters
        {
            anyhow::bail!(
                "spfresh config mismatch with stored metadata; use matching config for this index directory"
            );
        }
        let _ = ensure_active_generation(db)?;
        return Ok(());
    }

    let bytes = bincode::serialize(&expected).context("encode spfresh metadata")?;
    db.put(META_CONFIG_KEY, bytes, WriteOptions { sync: true })
        .context("persist spfresh metadata")?;
    set_active_generation(db, 0, true)?;
    Ok(())
}

pub(crate) fn load_index_checkpoint_bytes(db: &Db) -> anyhow::Result<Option<Vec<u8>>> {
    let current = db
        .get(META_INDEX_CHECKPOINT_KEY, ReadOptions::default())
        .context("read spfresh index checkpoint")?;
    Ok(current.map(|bytes| bytes.to_vec()))
}

pub(crate) fn persist_index_checkpoint_bytes(
    db: &Db,
    bytes: Vec<u8>,
    sync: bool,
) -> anyhow::Result<()> {
    db.put(META_INDEX_CHECKPOINT_KEY, bytes, WriteOptions { sync })
        .context("persist spfresh index checkpoint")
}

pub(crate) fn load_startup_manifest_bytes(db: &Db) -> anyhow::Result<Option<Vec<u8>>> {
    let current = db
        .get(META_STARTUP_MANIFEST_KEY, ReadOptions::default())
        .context("read spfresh startup manifest")?;
    Ok(current.map(|bytes| bytes.to_vec()))
}

pub(crate) fn persist_startup_manifest_bytes(
    db: &Db,
    bytes: Vec<u8>,
    sync: bool,
) -> anyhow::Result<()> {
    db.put(META_STARTUP_MANIFEST_KEY, bytes, WriteOptions { sync })
        .context("persist spfresh startup manifest")
}

pub(crate) fn wal_key(seq: u64) -> String {
    format!("{INDEX_WAL_PREFIX}{seq:020}")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum IndexWalEntry {
    Touch { id: u64 },
    TouchBatch { ids: Vec<u64> },
}

const WAL_BIN_TAG: &[u8] = b"wl2";
const WAL_KIND_TOUCH: u8 = 1;
const WAL_KIND_TOUCH_BATCH: u8 = 2;

#[cfg(test)]
pub(crate) fn encode_wal_entry(entry: &IndexWalEntry) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(WAL_BIN_TAG.len() + 1 + 8);
    out.extend_from_slice(WAL_BIN_TAG);
    match entry {
        IndexWalEntry::Touch { id } => {
            out.push(WAL_KIND_TOUCH);
            out.extend_from_slice(&id.to_le_bytes());
        }
        IndexWalEntry::TouchBatch { ids } => {
            out.push(WAL_KIND_TOUCH_BATCH);
            let len = u32::try_from(ids.len()).context("wal touch-batch len does not fit u32")?;
            out.extend_from_slice(&len.to_le_bytes());
            for id in ids {
                out.extend_from_slice(&id.to_le_bytes());
            }
        }
    }
    Ok(out)
}

pub(crate) fn encode_wal_touch_batch_ids(ids: &[u64]) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(WAL_BIN_TAG.len() + 1 + 4 + ids.len().saturating_mul(8));
    out.extend_from_slice(WAL_BIN_TAG);
    out.push(WAL_KIND_TOUCH_BATCH);
    let len = u32::try_from(ids.len()).context("wal touch-batch len does not fit u32")?;
    out.extend_from_slice(&len.to_le_bytes());
    for id in ids {
        out.extend_from_slice(&id.to_le_bytes());
    }
    Ok(out)
}

pub(crate) fn decode_wal_entry(raw: &[u8]) -> anyhow::Result<IndexWalEntry> {
    if !raw.starts_with(WAL_BIN_TAG) {
        anyhow::bail!("unsupported wal tag");
    }
    let mut cursor = WAL_BIN_TAG.len();
    let kind = read_u8(raw, &mut cursor)?;
    let entry = match kind {
        WAL_KIND_TOUCH => {
            let id = read_u64(raw, &mut cursor)?;
            IndexWalEntry::Touch { id }
        }
        WAL_KIND_TOUCH_BATCH => {
            let len = usize::try_from(read_u32(raw, &mut cursor)?)
                .context("wal touch-batch len overflow")?;
            let mut ids = Vec::with_capacity(len);
            for _ in 0..len {
                ids.push(read_u64(raw, &mut cursor)?);
            }
            IndexWalEntry::TouchBatch { ids }
        }
        _ => anyhow::bail!("unsupported wal kind {}", kind),
    };
    if cursor != raw.len() {
        anyhow::bail!("wal entry trailing bytes");
    }
    Ok(entry)
}

pub(crate) fn load_wal_entries_since(
    db: &Db,
    start_seq: u64,
) -> anyhow::Result<Vec<IndexWalEntry>> {
    let mut out = Vec::new();
    let prefix_bytes = INDEX_WAL_PREFIX.as_bytes().to_vec();
    let start = wal_key(start_seq).into_bytes();
    let end = prefix_exclusive_end(&prefix_bytes)?;
    let mut iter = db.iter(
        Range {
            start: Bound::Included(Bytes::from(start)),
            end: Bound::Excluded(Bytes::from(end)),
        },
        ReadOptions::default(),
    )?;
    iter.seek_to_first();
    for next in iter {
        let (key, value) = next?;
        if !key.starts_with(prefix_bytes.as_slice()) {
            continue;
        }
        let Some(value) = value else {
            continue;
        };
        let entry = decode_wal_entry(value.as_ref())
            .with_context(|| format!("decode wal entry key={}", String::from_utf8_lossy(&key)))?;
        out.push(entry);
    }
    Ok(out)
}

pub(crate) fn prune_wal_before(db: &Db, seq_exclusive: u64, sync: bool) -> anyhow::Result<()> {
    if seq_exclusive == 0 {
        return Ok(());
    }
    db.delete_range(
        wal_key(0).into_bytes(),
        wal_key(seq_exclusive).into_bytes(),
        WriteOptions { sync },
    )
    .context("prune spfresh wal")
}

pub(crate) fn vector_prefix(generation: u64) -> String {
    let mut key = String::with_capacity(VECTOR_ROOT_PREFIX.len() + 16 + 1);
    key.push_str(VECTOR_ROOT_PREFIX);
    let _ = write!(&mut key, "{generation:016x}/");
    key
}

pub(crate) fn vector_key(generation: u64, id: u64) -> String {
    let mut key = String::with_capacity(VECTOR_ROOT_PREFIX.len() + 16 + 1 + 20);
    key.push_str(VECTOR_ROOT_PREFIX);
    let _ = write!(&mut key, "{generation:016x}/{id:020}");
    key
}

pub(crate) fn posting_map_prefix(generation: u64) -> String {
    let mut key = String::with_capacity(POSTING_MAP_ROOT_PREFIX.len() + 16 + 1);
    key.push_str(POSTING_MAP_ROOT_PREFIX);
    let _ = write!(&mut key, "{generation:016x}/");
    key
}

pub(crate) fn posting_map_key(generation: u64, id: u64) -> String {
    let mut key = String::with_capacity(POSTING_MAP_ROOT_PREFIX.len() + 16 + 1 + 20);
    key.push_str(POSTING_MAP_ROOT_PREFIX);
    let _ = write!(&mut key, "{generation:016x}/{id:020}");
    key
}

#[cfg(test)]
pub(crate) fn posting_assignment_value(posting_id: usize) -> anyhow::Result<Vec<u8>> {
    let pid = u64::try_from(posting_id).context("posting id does not fit u64")?;
    bincode::serialize(&pid).context("encode posting assignment value")
}

#[allow(dead_code)]
pub(crate) fn load_posting_assignment(
    db: &Db,
    generation: u64,
    id: u64,
) -> anyhow::Result<Option<usize>> {
    let key = posting_map_key(generation, id);
    let raw = db
        .get(key.as_bytes(), ReadOptions::default())
        .with_context(|| format!("load posting assignment id={id} generation={generation}"))?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let posting_u64: u64 = bincode::deserialize(raw.as_ref())
        .with_context(|| format!("decode posting assignment id={id} generation={generation}"))?;
    let posting = usize::try_from(posting_u64).context("posting assignment does not fit usize")?;
    Ok(Some(posting))
}

pub(crate) fn posting_members_prefix(generation: u64, posting_id: usize) -> String {
    let mut key = String::with_capacity(POSTING_MEMBERS_ROOT_PREFIX.len() + 16 + 1 + 10 + 1);
    key.push_str(POSTING_MEMBERS_ROOT_PREFIX);
    let _ = write!(&mut key, "{generation:016x}/{posting_id:010}/");
    key
}

pub(crate) fn posting_members_generation_prefix(generation: u64) -> String {
    let mut key = String::with_capacity(POSTING_MEMBERS_ROOT_PREFIX.len() + 16);
    key.push_str(POSTING_MEMBERS_ROOT_PREFIX);
    let _ = write!(&mut key, "{generation:016x}");
    key
}

pub(crate) fn posting_member_event_key(
    generation: u64,
    posting_id: usize,
    seq: u64,
    id: u64,
) -> String {
    let mut key =
        String::with_capacity(POSTING_MEMBERS_ROOT_PREFIX.len() + 16 + 1 + 10 + 1 + 20 + 1 + 20);
    key.push_str(POSTING_MEMBERS_ROOT_PREFIX);
    let _ = write!(
        &mut key,
        "{generation:016x}/{posting_id:010}/{seq:020}/{id:020}"
    );
    key
}

const POSTING_MEMBER_EVENT_RKYV_V3_TAG: &[u8] = b"pk3";
const POSTING_MEMBER_EVENT_FLAG_TOMBSTONE: u8 = 1 << 0;

#[derive(Clone, Debug)]
pub(crate) struct PostingMember {
    pub id: u64,
    pub residual_scale: Option<f32>,
    pub residual_code: Option<Vec<i8>>,
}

pub(crate) fn posting_member_event_upsert_value_with_residual(
    id: u64,
    values: &[f32],
    centroid: &[f32],
) -> anyhow::Result<Vec<u8>> {
    debug_assert_eq!(values.len(), centroid.len());
    let mut max_abs = 0.0f32;
    for (v, c) in values.iter().zip(centroid.iter()) {
        let a = (*v - *c).abs();
        if a > max_abs {
            max_abs = a;
        }
    }
    let residual_scale = if values.is_empty() {
        1.0
    } else if max_abs <= 1e-9 {
        1e-9
    } else {
        max_abs / 127.0
    };
    let code_len =
        u32::try_from(values.len()).context("posting-member residual code len does not fit u32")?;
    let mut out =
        Vec::with_capacity(POSTING_MEMBER_EVENT_RKYV_V3_TAG.len() + 1 + 8 + 4 + 4 + values.len());
    out.extend_from_slice(POSTING_MEMBER_EVENT_RKYV_V3_TAG);
    out.push(0);
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&residual_scale.to_bits().to_le_bytes());
    out.extend_from_slice(&code_len.to_le_bytes());
    for (v, c) in values.iter().zip(centroid.iter()) {
        let q = ((*v - *c) / residual_scale).round().clamp(-127.0, 127.0) as i8;
        out.push(q as u8);
    }
    Ok(out)
}

pub(crate) fn posting_member_event_upsert_value_from_sketch(
    id: u64,
    residual_scale: f32,
    residual_code: &[i8],
) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(
        POSTING_MEMBER_EVENT_RKYV_V3_TAG.len() + 1 + 8 + 4 + 4 + residual_code.len(),
    );
    out.extend_from_slice(POSTING_MEMBER_EVENT_RKYV_V3_TAG);
    out.push(0);
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&residual_scale.to_bits().to_le_bytes());
    let code_len = u32::try_from(residual_code.len())
        .context("posting-member residual code len does not fit u32")?;
    out.extend_from_slice(&code_len.to_le_bytes());
    for value in residual_code {
        out.push(*value as u8);
    }
    Ok(out)
}

pub(crate) fn posting_member_event_tombstone_value(id: u64) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(POSTING_MEMBER_EVENT_RKYV_V3_TAG.len() + 1 + 8);
    out.extend_from_slice(POSTING_MEMBER_EVENT_RKYV_V3_TAG);
    out.push(POSTING_MEMBER_EVENT_FLAG_TOMBSTONE);
    out.extend_from_slice(&id.to_le_bytes());
    Ok(out)
}

#[derive(Clone, Debug)]
struct DecodedPostingMemberEvent {
    id: u64,
    tombstone: bool,
    residual_scale: Option<f32>,
    residual_code: Option<Vec<i8>>,
}

fn decode_posting_member_event(raw: &[u8]) -> anyhow::Result<DecodedPostingMemberEvent> {
    if !raw.starts_with(POSTING_MEMBER_EVENT_RKYV_V3_TAG) {
        anyhow::bail!("unsupported posting-member event tag");
    }
    let mut cursor = POSTING_MEMBER_EVENT_RKYV_V3_TAG.len();
    let flags = read_u8(raw, &mut cursor)?;
    let id = read_u64(raw, &mut cursor)?;
    if (flags & POSTING_MEMBER_EVENT_FLAG_TOMBSTONE) != 0 {
        if cursor != raw.len() {
            anyhow::bail!("posting-member tombstone trailing bytes");
        }
        return Ok(DecodedPostingMemberEvent {
            id,
            tombstone: true,
            residual_scale: None,
            residual_code: None,
        });
    }
    let scale = read_f32(raw, &mut cursor)?;
    let code_len =
        usize::try_from(read_u32(raw, &mut cursor)?).context("posting-member code len overflow")?;
    let end = cursor.saturating_add(code_len);
    let Some(code_raw) = raw.get(cursor..end) else {
        anyhow::bail!("posting-member code underflow");
    };
    let mut code = Vec::with_capacity(code_len);
    for byte in code_raw {
        code.push(*byte as i8);
    }
    cursor = end;
    if cursor != raw.len() {
        anyhow::bail!("posting-member event trailing bytes");
    }
    Ok(DecodedPostingMemberEvent {
        id,
        tombstone: false,
        residual_scale: Some(scale),
        residual_code: Some(code),
    })
}

#[derive(Clone, Debug)]
pub(crate) struct PostingMembersLoadResult {
    pub members: Vec<PostingMember>,
    pub scanned_events: usize,
}

pub(crate) fn load_posting_members(
    db: &Db,
    generation: u64,
    posting_id: usize,
) -> anyhow::Result<PostingMembersLoadResult> {
    let mut latest = FxHashMap::<u64, PostingMember>::default();
    let mut scanned_events = 0usize;
    let prefix = posting_members_prefix(generation, posting_id);
    let prefix_bytes = prefix.as_bytes().to_vec();
    let end = prefix_exclusive_end(&prefix_bytes)?;
    let mut iter = db.iter(
        Range {
            start: Bound::Included(Bytes::from(prefix_bytes.clone())),
            end: Bound::Excluded(Bytes::from(end)),
        },
        ReadOptions::default(),
    )?;
    iter.seek_to_first();
    for next in iter {
        let (key, value) = next?;
        if !key.starts_with(prefix_bytes.as_slice()) {
            continue;
        }
        let Some(value) = value else {
            continue;
        };
        scanned_events = scanned_events.saturating_add(1);
        let raw = value.as_ref();
        let decoded = decode_posting_member_event(raw).with_context(|| {
            format!(
                "decode posting-member event key={}",
                String::from_utf8_lossy(&key)
            )
        })?;
        if decoded.tombstone {
            latest.remove(&decoded.id);
            continue;
        }
        let Some(scale) = decoded.residual_scale else {
            anyhow::bail!("missing posting-member residual scale");
        };
        let Some(code) = decoded.residual_code else {
            anyhow::bail!("missing posting-member residual code");
        };
        latest.insert(
            decoded.id,
            PostingMember {
                id: decoded.id,
                residual_scale: Some(scale),
                residual_code: Some(code),
            },
        );
    }
    let members = latest.into_values().collect();
    Ok(PostingMembersLoadResult {
        members,
        scanned_events,
    })
}

pub(crate) fn prefix_exclusive_end(prefix: &[u8]) -> anyhow::Result<Vec<u8>> {
    if prefix.is_empty() {
        anyhow::bail!("prefix for range deletion must not be empty");
    }

    let mut out = prefix.to_vec();
    for idx in (0..out.len()).rev() {
        if out[idx] != u8::MAX {
            out[idx] = out[idx].saturating_add(1);
            out.truncate(idx + 1);
            return Ok(out);
        }
    }

    anyhow::bail!("prefix cannot be converted to exclusive range end")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_builders_match_expected_layouts() {
        let generation = 0x1a2bu64;
        let id = 987u64;
        let posting_id = 42usize;
        let seq = 55u64;

        assert_eq!(
            vector_prefix(generation),
            format!("{VECTOR_ROOT_PREFIX}{generation:016x}/")
        );
        assert_eq!(
            vector_key(generation, id),
            format!("{VECTOR_ROOT_PREFIX}{generation:016x}/{id:020}")
        );
        assert_eq!(
            posting_map_prefix(generation),
            format!("{POSTING_MAP_ROOT_PREFIX}{generation:016x}/")
        );
        assert_eq!(
            posting_map_key(generation, id),
            format!("{POSTING_MAP_ROOT_PREFIX}{generation:016x}/{id:020}")
        );
        assert_eq!(
            posting_members_generation_prefix(generation),
            format!("{POSTING_MEMBERS_ROOT_PREFIX}{generation:016x}")
        );
        assert_eq!(
            posting_members_prefix(generation, posting_id),
            format!("{POSTING_MEMBERS_ROOT_PREFIX}{generation:016x}/{posting_id:010}/")
        );
        assert_eq!(
            posting_member_event_key(generation, posting_id, seq, id),
            format!(
                "{POSTING_MEMBERS_ROOT_PREFIX}{generation:016x}/{posting_id:010}/{seq:020}/{id:020}"
            )
        );
    }

    #[test]
    fn touch_batch_slice_encoder_matches_enum_encoder() {
        let ids = vec![7u64, 11, 13, 17, 19];
        let via_enum = encode_wal_entry(&IndexWalEntry::TouchBatch { ids: ids.clone() })
            .expect("encode wal enum");
        let via_slice = encode_wal_touch_batch_ids(ids.as_slice()).expect("encode wal slice");
        assert_eq!(via_enum, via_slice);
        let decoded = decode_wal_entry(via_slice.as_slice()).expect("decode wal slice bytes");
        match decoded {
            IndexWalEntry::TouchBatch { ids: decoded_ids } => {
                assert_eq!(decoded_ids, ids);
            }
            other => panic!("expected touch-batch entry, got {other:?}"),
        }
    }
}
