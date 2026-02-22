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
    POSTING_MAP_ROOT_PREFIX, POSTING_MEMBERS_ROOT_PREFIX, POSTING_MEMBERS_SNAPSHOT_ROOT_PREFIX,
    VECTOR_ROOT_PREFIX,
};

const VECTOR_ROW_BIN_TAG: &[u8] = b"vr3";
const VECTOR_ROW_FLAG_DELETED: u8 = 1 << 0;
const VECTOR_ROW_FLAG_HAS_POSTING: u8 = 1 << 1;
const META_BIN_TAG: &[u8] = b"sm1";

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

fn decode_u64_fixed(raw: &[u8], field: &str) -> anyhow::Result<u64> {
    if raw.len() != 8 {
        anyhow::bail!("{field} has invalid fixed-u64 length: {}", raw.len());
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(raw);
    Ok(u64::from_le_bytes(arr))
}

pub(crate) fn encode_u64_fixed(value: u64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

fn parse_decimal_u64_ascii(raw: &[u8]) -> Option<u64> {
    if raw.is_empty() {
        return None;
    }
    let mut out = 0u64;
    for byte in raw {
        if !byte.is_ascii_digit() {
            return None;
        }
        out = out.checked_mul(10)?;
        out = out.checked_add(u64::from(byte - b'0'))?;
    }
    Some(out)
}

fn parse_wal_seq_key(key: &[u8]) -> Option<u64> {
    let prefix = INDEX_WAL_PREFIX.as_bytes();
    if !key.starts_with(prefix) {
        return None;
    }
    parse_decimal_u64_ascii(&key[prefix.len()..])
}

fn parse_posting_member_event_seq_key(key: &[u8]) -> Option<u64> {
    let prefix = POSTING_MEMBERS_ROOT_PREFIX.as_bytes();
    if !key.starts_with(prefix) {
        return None;
    }
    // Key layout:
    // {POSTING_MEMBERS_ROOT_PREFIX}{generation:016x}/{posting:010}/{seq:020}/{id:020}
    let generation_len = 16usize;
    let posting_len = 10usize;
    let seq_len = 20usize;
    let id_len = 20usize;
    let start = prefix.len();
    let seq_start = start
        .checked_add(generation_len + 1 + posting_len + 1)
        .filter(|idx| *idx <= key.len())?;
    let seq_end = seq_start.checked_add(seq_len)?;
    let id_end = seq_end.checked_add(1 + id_len)?;
    if id_end != key.len() {
        return None;
    }
    if key.get(start + generation_len) != Some(&b'/')
        || key.get(start + generation_len + 1 + posting_len) != Some(&b'/')
        || key.get(seq_end) != Some(&b'/')
    {
        return None;
    }
    parse_decimal_u64_ascii(key.get(seq_start..seq_end)?)
}

fn recover_wal_next_seq(db: &Db) -> anyhow::Result<u64> {
    let prefix_bytes = INDEX_WAL_PREFIX.as_bytes().to_vec();
    let end = prefix_exclusive_end(prefix_bytes.as_slice())?;
    let mut iter = db.iter(
        Range {
            start: Bound::Included(Bytes::from(prefix_bytes.clone())),
            end: Bound::Excluded(Bytes::from(end)),
        },
        ReadOptions::default(),
    )?;
    iter.seek_to_first();
    let mut max_seq: Option<u64> = None;
    for next in iter {
        let (key, value) = next?;
        if value.is_none() || !key.starts_with(prefix_bytes.as_slice()) {
            continue;
        }
        if let Some(seq) = parse_wal_seq_key(key.as_ref()) {
            max_seq = Some(max_seq.map_or(seq, |current| current.max(seq)));
        }
    }
    match max_seq {
        Some(v) => v
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("recover wal next seq overflow from max={v}")),
        None => Ok(0),
    }
}

fn recover_posting_event_next_seq(db: &Db) -> anyhow::Result<u64> {
    let prefix_bytes = POSTING_MEMBERS_ROOT_PREFIX.as_bytes().to_vec();
    let end = prefix_exclusive_end(prefix_bytes.as_slice())?;
    let mut iter = db.iter(
        Range {
            start: Bound::Included(Bytes::from(prefix_bytes.clone())),
            end: Bound::Excluded(Bytes::from(end)),
        },
        ReadOptions::default(),
    )?;
    iter.seek_to_first();
    let mut max_seq: Option<u64> = None;
    for next in iter {
        let (key, value) = next?;
        if value.is_none() || !key.starts_with(prefix_bytes.as_slice()) {
            continue;
        }
        if let Some(seq) = parse_posting_member_event_seq_key(key.as_ref()) {
            max_seq = Some(max_seq.map_or(seq, |current| current.max(seq)));
        }
    }
    match max_seq {
        Some(v) => v
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("recover posting-event next seq overflow from max={v}")),
        None => Ok(0),
    }
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
    if cfg.spfresh.diskmeta_probe_multiplier == 0 {
        anyhow::bail!("spfresh diskmeta_probe_multiplier must be > 0");
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
        Some(bytes) => decode_u64_fixed(bytes.as_ref(), "active generation"),
        None => {
            set_active_generation(db, 0, true)?;
            Ok(0)
        }
    }
}

pub(crate) fn set_active_generation(db: &Db, generation: u64, sync: bool) -> anyhow::Result<()> {
    let bytes = encode_u64_fixed(generation);
    db.put(META_ACTIVE_GENERATION_KEY, bytes, WriteOptions { sync })
        .context("persist active generation")
}

pub(crate) fn ensure_wal_next_seq(db: &Db) -> anyhow::Result<u64> {
    match db
        .get(META_INDEX_WAL_NEXT_SEQ_KEY, ReadOptions::default())
        .context("read spfresh wal next seq")?
    {
        Some(bytes) => match decode_u64_fixed(bytes.as_ref(), "wal next seq") {
            Ok(next) => Ok(next),
            Err(err) => {
                eprintln!("spfresh-layerdb: wal next seq decode failed, recovering: {err:#}");
                let recovered = recover_wal_next_seq(db)?;
                set_wal_next_seq(db, recovered, true)?;
                Ok(recovered)
            }
        },
        None => {
            let recovered = recover_wal_next_seq(db)?;
            set_wal_next_seq(db, recovered, true)?;
            Ok(recovered)
        }
    }
}

pub(crate) fn set_wal_next_seq(db: &Db, next_seq: u64, sync: bool) -> anyhow::Result<()> {
    let bytes = encode_u64_fixed(next_seq);
    db.put(META_INDEX_WAL_NEXT_SEQ_KEY, bytes, WriteOptions { sync })
        .context("persist wal next seq")
}

pub(crate) fn ensure_posting_event_next_seq(db: &Db) -> anyhow::Result<u64> {
    match db
        .get(META_POSTING_EVENT_NEXT_SEQ_KEY, ReadOptions::default())
        .context("read spfresh posting-event next seq")?
    {
        Some(bytes) => match decode_u64_fixed(bytes.as_ref(), "posting-event next seq") {
            Ok(next) => Ok(next),
            Err(err) => {
                eprintln!(
                    "spfresh-layerdb: posting-event next seq decode failed, recovering: {err:#}"
                );
                let recovered = recover_posting_event_next_seq(db)?;
                set_posting_event_next_seq(db, recovered, true)?;
                Ok(recovered)
            }
        },
        None => {
            let recovered = recover_posting_event_next_seq(db)?;
            set_posting_event_next_seq(db, recovered, true)?;
            Ok(recovered)
        }
    }
}

pub(crate) fn set_posting_event_next_seq(db: &Db, next_seq: u64, sync: bool) -> anyhow::Result<()> {
    let bytes = encode_u64_fixed(next_seq);
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

fn encode_spfresh_metadata(meta: &SpFreshPersistedMeta) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(META_BIN_TAG.len() + 4 + 8 * 8);
    out.extend_from_slice(META_BIN_TAG);
    out.extend_from_slice(&meta.schema_version.to_le_bytes());
    for (field, value) in [
        ("dim", meta.dim),
        ("initial_postings", meta.initial_postings),
        ("split_limit", meta.split_limit),
        ("merge_limit", meta.merge_limit),
        ("reassign_range", meta.reassign_range),
        ("nprobe", meta.nprobe),
        ("diskmeta_probe_multiplier", meta.diskmeta_probe_multiplier),
        ("kmeans_iters", meta.kmeans_iters),
    ] {
        let encoded = u64::try_from(value)
            .with_context(|| format!("spfresh metadata {field} does not fit u64"))?;
        out.extend_from_slice(&encoded.to_le_bytes());
    }
    Ok(out)
}

fn decode_spfresh_metadata(raw: &[u8]) -> anyhow::Result<SpFreshPersistedMeta> {
    if !raw.starts_with(META_BIN_TAG) {
        anyhow::bail!("unsupported spfresh metadata tag");
    }
    let mut cursor = META_BIN_TAG.len();
    let schema_version = read_u32(raw, &mut cursor)?;
    let mut decode_usize = |field: &str| -> anyhow::Result<usize> {
        let raw = read_u64(raw, &mut cursor)?;
        usize::try_from(raw).with_context(|| format!("spfresh metadata {field} overflow"))
    };
    let dim = decode_usize("dim")?;
    let initial_postings = decode_usize("initial_postings")?;
    let split_limit = decode_usize("split_limit")?;
    let merge_limit = decode_usize("merge_limit")?;
    let reassign_range = decode_usize("reassign_range")?;
    let nprobe = decode_usize("nprobe")?;
    let diskmeta_probe_multiplier = decode_usize("diskmeta_probe_multiplier")?;
    let kmeans_iters = decode_usize("kmeans_iters")?;
    if cursor != raw.len() {
        anyhow::bail!("spfresh metadata trailing bytes");
    }
    Ok(SpFreshPersistedMeta {
        schema_version,
        dim,
        initial_postings,
        split_limit,
        merge_limit,
        reassign_range,
        nprobe,
        diskmeta_probe_multiplier,
        kmeans_iters,
    })
}

pub(crate) fn load_metadata(db: &Db) -> anyhow::Result<Option<SpFreshPersistedMeta>> {
    let current = db
        .get(META_CONFIG_KEY, ReadOptions::default())
        .context("read spfresh metadata")?;
    let Some(value) = current else {
        return Ok(None);
    };
    let meta = decode_spfresh_metadata(value.as_ref()).context("decode spfresh metadata")?;
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
        diskmeta_probe_multiplier: cfg.spfresh.diskmeta_probe_multiplier,
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
            || actual.diskmeta_probe_multiplier != expected.diskmeta_probe_multiplier
            || actual.kmeans_iters != expected.kmeans_iters
        {
            anyhow::bail!(
                "spfresh config mismatch with stored metadata; use matching config for this index directory"
            );
        }
        let _ = ensure_active_generation(db)?;
        return Ok(());
    }

    let bytes = encode_spfresh_metadata(&expected).context("encode spfresh metadata")?;
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
    VectorUpsertBatch { rows: Vec<VectorWalUpsertRow> },
    VectorDeleteBatch { ids: Vec<u64> },
    DiskMetaUpsertBatch { rows: Vec<DiskMetaWalUpsertRow> },
    DiskMetaDeleteBatch { rows: Vec<DiskMetaWalDeleteRow> },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct VectorWalUpsertRow {
    pub id: u64,
    pub values: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct DiskMetaWalUpsertRow {
    pub id: u64,
    pub old: Option<(usize, Vec<f32>)>,
    pub new_posting: usize,
    pub new_values: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct DiskMetaWalDeleteRow {
    pub id: u64,
    pub old: Option<(usize, Vec<f32>)>,
}

const WAL_BIN_TAG: &[u8] = b"wl3";
const WAL_KIND_TOUCH: u8 = 1;
const WAL_KIND_TOUCH_BATCH: u8 = 2;
const WAL_KIND_VECTOR_UPSERT_BATCH: u8 = 3;
const WAL_KIND_VECTOR_DELETE_BATCH: u8 = 4;
const WAL_KIND_DISKMETA_UPSERT_BATCH: u8 = 5;
const WAL_KIND_DISKMETA_DELETE_BATCH: u8 = 6;
const WAL_DISKMETA_FLAG_HAS_OLD: u8 = 1 << 0;
const WAL_FRAME_HEADER_SIZE: usize = 4 + 4;

fn wal_crc32(payload: &[u8]) -> u32 {
    crc32fast::hash(payload)
}

fn encode_wal_frame(payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    let payload_len = u32::try_from(payload.len()).context("wal payload len does not fit u32")?;
    let crc = wal_crc32(payload);
    let mut out = Vec::with_capacity(WAL_BIN_TAG.len() + WAL_FRAME_HEADER_SIZE + payload.len());
    out.extend_from_slice(WAL_BIN_TAG);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

fn encode_wal_f32_vec(payload: &mut Vec<u8>, values: &[f32]) -> anyhow::Result<()> {
    let len = u32::try_from(values.len()).context("wal f32 vector len does not fit u32")?;
    payload.extend_from_slice(&len.to_le_bytes());
    for value in values {
        payload.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    Ok(())
}

fn decode_wal_f32_vec(payload: &[u8], cursor: &mut usize) -> anyhow::Result<Vec<f32>> {
    let len = usize::try_from(read_u32(payload, cursor)?).context("wal f32 vector len overflow")?;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_f32(payload, cursor)?);
    }
    Ok(values)
}

fn encode_wal_diskmeta_old_state(
    payload: &mut Vec<u8>,
    old: Option<(usize, &[f32])>,
) -> anyhow::Result<()> {
    let mut flags = 0u8;
    if old.is_some() {
        flags |= WAL_DISKMETA_FLAG_HAS_OLD;
    }
    payload.push(flags);
    if let Some((posting, values)) = old {
        let posting_u64 = u64::try_from(posting).context("wal diskmeta old posting overflow")?;
        payload.extend_from_slice(&posting_u64.to_le_bytes());
        encode_wal_f32_vec(payload, values)?;
    }
    Ok(())
}

fn decode_wal_diskmeta_old_state(
    payload: &[u8],
    cursor: &mut usize,
) -> anyhow::Result<Option<(usize, Vec<f32>)>> {
    let flags = read_u8(payload, cursor)?;
    let unknown_flags = flags & !WAL_DISKMETA_FLAG_HAS_OLD;
    if unknown_flags != 0 {
        anyhow::bail!("unsupported diskmeta wal old-state flags: {unknown_flags:#x}");
    }
    if (flags & WAL_DISKMETA_FLAG_HAS_OLD) == 0 {
        return Ok(None);
    }
    let posting_u64 = read_u64(payload, cursor)?;
    let posting =
        usize::try_from(posting_u64).context("wal diskmeta old posting does not fit usize")?;
    let values = decode_wal_f32_vec(payload, cursor)?;
    Ok(Some((posting, values)))
}

#[cfg(test)]
pub(crate) fn encode_wal_entry(entry: &IndexWalEntry) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(1 + 8);
    match entry {
        IndexWalEntry::Touch { id } => {
            payload.push(WAL_KIND_TOUCH);
            payload.extend_from_slice(&id.to_le_bytes());
        }
        IndexWalEntry::TouchBatch { ids } => {
            payload.push(WAL_KIND_TOUCH_BATCH);
            let len = u32::try_from(ids.len()).context("wal touch-batch len does not fit u32")?;
            payload.extend_from_slice(&len.to_le_bytes());
            for id in ids {
                payload.extend_from_slice(&id.to_le_bytes());
            }
        }
        IndexWalEntry::VectorUpsertBatch { rows } => {
            payload.push(WAL_KIND_VECTOR_UPSERT_BATCH);
            let len = u32::try_from(rows.len())
                .context("wal vector upsert-batch len does not fit u32")?;
            payload.extend_from_slice(&len.to_le_bytes());
            for row in rows {
                payload.extend_from_slice(&row.id.to_le_bytes());
                encode_wal_f32_vec(&mut payload, row.values.as_slice())?;
            }
        }
        IndexWalEntry::VectorDeleteBatch { ids } => {
            payload.push(WAL_KIND_VECTOR_DELETE_BATCH);
            let len =
                u32::try_from(ids.len()).context("wal vector delete-batch len does not fit u32")?;
            payload.extend_from_slice(&len.to_le_bytes());
            for id in ids {
                payload.extend_from_slice(&id.to_le_bytes());
            }
        }
        IndexWalEntry::DiskMetaUpsertBatch { rows } => {
            payload.push(WAL_KIND_DISKMETA_UPSERT_BATCH);
            let len = u32::try_from(rows.len())
                .context("wal diskmeta upsert-batch len does not fit u32")?;
            payload.extend_from_slice(&len.to_le_bytes());
            for row in rows {
                payload.extend_from_slice(&row.id.to_le_bytes());
                let old = row
                    .old
                    .as_ref()
                    .map(|(posting, values)| (*posting, values.as_slice()));
                encode_wal_diskmeta_old_state(&mut payload, old)?;
                let new_posting =
                    u64::try_from(row.new_posting).context("wal new posting does not fit u64")?;
                payload.extend_from_slice(&new_posting.to_le_bytes());
                encode_wal_f32_vec(&mut payload, row.new_values.as_slice())?;
            }
        }
        IndexWalEntry::DiskMetaDeleteBatch { rows } => {
            payload.push(WAL_KIND_DISKMETA_DELETE_BATCH);
            let len = u32::try_from(rows.len())
                .context("wal diskmeta delete-batch len does not fit u32")?;
            payload.extend_from_slice(&len.to_le_bytes());
            for row in rows {
                payload.extend_from_slice(&row.id.to_le_bytes());
                let old = row
                    .old
                    .as_ref()
                    .map(|(posting, values)| (*posting, values.as_slice()));
                encode_wal_diskmeta_old_state(&mut payload, old)?;
            }
        }
    }
    encode_wal_frame(payload.as_slice())
}

#[cfg(test)]
pub(crate) fn encode_wal_touch_batch_ids(ids: &[u64]) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(1 + 4 + ids.len().saturating_mul(8));
    payload.push(WAL_KIND_TOUCH_BATCH);
    let len = u32::try_from(ids.len()).context("wal touch-batch len does not fit u32")?;
    payload.extend_from_slice(&len.to_le_bytes());
    for id in ids {
        payload.extend_from_slice(&id.to_le_bytes());
    }
    encode_wal_frame(payload.as_slice())
}

pub(crate) fn encode_wal_vector_upsert_batch(rows: &[(u64, Vec<f32>)]) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(1 + 4 + rows.len().saturating_mul(16));
    payload.push(WAL_KIND_VECTOR_UPSERT_BATCH);
    let len = u32::try_from(rows.len()).context("wal vector upsert-batch len does not fit u32")?;
    payload.extend_from_slice(&len.to_le_bytes());
    for (id, values) in rows {
        payload.extend_from_slice(&id.to_le_bytes());
        encode_wal_f32_vec(&mut payload, values.as_slice())?;
    }
    encode_wal_frame(payload.as_slice())
}

pub(crate) fn encode_wal_vector_delete_batch(ids: &[u64]) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(1 + 4 + ids.len().saturating_mul(8));
    payload.push(WAL_KIND_VECTOR_DELETE_BATCH);
    let len = u32::try_from(ids.len()).context("wal vector delete-batch len does not fit u32")?;
    payload.extend_from_slice(&len.to_le_bytes());
    for id in ids {
        payload.extend_from_slice(&id.to_le_bytes());
    }
    encode_wal_frame(payload.as_slice())
}

pub(crate) fn encode_wal_diskmeta_upsert_batch(
    rows: &[(u64, Vec<f32>)],
    old_states: &FxHashMap<u64, Option<(usize, Vec<f32>)>>,
    new_postings: &FxHashMap<u64, usize>,
) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(1 + 4 + rows.len().saturating_mul(16));
    payload.push(WAL_KIND_DISKMETA_UPSERT_BATCH);
    let len =
        u32::try_from(rows.len()).context("wal diskmeta upsert-batch len does not fit u32")?;
    payload.extend_from_slice(&len.to_le_bytes());
    for (id, new_values) in rows {
        payload.extend_from_slice(&id.to_le_bytes());
        let old = old_states
            .get(id)
            .and_then(|state| state.as_ref())
            .map(|(posting, values)| (*posting, values.as_slice()));
        encode_wal_diskmeta_old_state(&mut payload, old)?;
        let new_posting = new_postings
            .get(id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("missing new posting for diskmeta wal id={id}"))?;
        let posting_u64 =
            u64::try_from(new_posting).context("wal diskmeta new posting does not fit u64")?;
        payload.extend_from_slice(&posting_u64.to_le_bytes());
        encode_wal_f32_vec(&mut payload, new_values.as_slice())?;
    }
    encode_wal_frame(payload.as_slice())
}

pub(crate) fn encode_wal_diskmeta_delete_batch(
    ids: &[u64],
    old_states: &FxHashMap<u64, Option<(usize, Vec<f32>)>>,
) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(1 + 4 + ids.len().saturating_mul(12));
    payload.push(WAL_KIND_DISKMETA_DELETE_BATCH);
    let len = u32::try_from(ids.len()).context("wal diskmeta delete-batch len does not fit u32")?;
    payload.extend_from_slice(&len.to_le_bytes());
    for id in ids {
        payload.extend_from_slice(&id.to_le_bytes());
        let old = old_states
            .get(id)
            .and_then(|state| state.as_ref())
            .map(|(posting, values)| (*posting, values.as_slice()));
        encode_wal_diskmeta_old_state(&mut payload, old)?;
    }
    encode_wal_frame(payload.as_slice())
}

pub(crate) fn decode_wal_entry(raw: &[u8]) -> anyhow::Result<IndexWalEntry> {
    if !raw.starts_with(WAL_BIN_TAG) {
        anyhow::bail!("unsupported wal tag");
    }
    let mut frame_cursor = WAL_BIN_TAG.len();
    let payload_len =
        usize::try_from(read_u32(raw, &mut frame_cursor)?).context("wal payload len overflow")?;
    let expected_crc = read_u32(raw, &mut frame_cursor)?;
    let payload_end = frame_cursor
        .checked_add(payload_len)
        .ok_or_else(|| anyhow::anyhow!("wal payload length overflow"))?;
    let Some(payload) = raw.get(frame_cursor..payload_end) else {
        anyhow::bail!("wal payload underflow");
    };
    if wal_crc32(payload) != expected_crc {
        anyhow::bail!("wal payload checksum mismatch");
    }
    if payload_end != raw.len() {
        anyhow::bail!("wal entry trailing bytes");
    }
    let mut cursor = 0usize;
    let kind = read_u8(payload, &mut cursor)?;
    let entry = match kind {
        WAL_KIND_TOUCH => {
            let id = read_u64(payload, &mut cursor)?;
            IndexWalEntry::Touch { id }
        }
        WAL_KIND_TOUCH_BATCH => {
            let len = usize::try_from(read_u32(payload, &mut cursor)?)
                .context("wal touch-batch len overflow")?;
            let mut ids = Vec::with_capacity(len);
            for _ in 0..len {
                ids.push(read_u64(payload, &mut cursor)?);
            }
            IndexWalEntry::TouchBatch { ids }
        }
        WAL_KIND_VECTOR_UPSERT_BATCH => {
            let len = usize::try_from(read_u32(payload, &mut cursor)?)
                .context("wal vector upsert-batch len overflow")?;
            let mut rows = Vec::with_capacity(len);
            for _ in 0..len {
                let id = read_u64(payload, &mut cursor)?;
                let values = decode_wal_f32_vec(payload, &mut cursor)?;
                rows.push(VectorWalUpsertRow { id, values });
            }
            IndexWalEntry::VectorUpsertBatch { rows }
        }
        WAL_KIND_VECTOR_DELETE_BATCH => {
            let len = usize::try_from(read_u32(payload, &mut cursor)?)
                .context("wal vector delete-batch len overflow")?;
            let mut ids = Vec::with_capacity(len);
            for _ in 0..len {
                ids.push(read_u64(payload, &mut cursor)?);
            }
            IndexWalEntry::VectorDeleteBatch { ids }
        }
        WAL_KIND_DISKMETA_UPSERT_BATCH => {
            let len = usize::try_from(read_u32(payload, &mut cursor)?)
                .context("wal diskmeta upsert-batch len overflow")?;
            let mut rows = Vec::with_capacity(len);
            for _ in 0..len {
                let id = read_u64(payload, &mut cursor)?;
                let old = decode_wal_diskmeta_old_state(payload, &mut cursor)?;
                let new_posting_u64 = read_u64(payload, &mut cursor)?;
                let new_posting = usize::try_from(new_posting_u64)
                    .context("wal diskmeta new posting does not fit usize")?;
                let new_values = decode_wal_f32_vec(payload, &mut cursor)?;
                rows.push(DiskMetaWalUpsertRow {
                    id,
                    old,
                    new_posting,
                    new_values,
                });
            }
            IndexWalEntry::DiskMetaUpsertBatch { rows }
        }
        WAL_KIND_DISKMETA_DELETE_BATCH => {
            let len = usize::try_from(read_u32(payload, &mut cursor)?)
                .context("wal diskmeta delete-batch len overflow")?;
            let mut rows = Vec::with_capacity(len);
            for _ in 0..len {
                let id = read_u64(payload, &mut cursor)?;
                let old = decode_wal_diskmeta_old_state(payload, &mut cursor)?;
                rows.push(DiskMetaWalDeleteRow { id, old });
            }
            IndexWalEntry::DiskMetaDeleteBatch { rows }
        }
        _ => anyhow::bail!("unsupported wal kind {}", kind),
    };
    if cursor != payload.len() {
        anyhow::bail!("wal entry trailing bytes");
    }
    Ok(entry)
}

pub(crate) fn visit_wal_entries_since<F>(
    db: &Db,
    start_seq: u64,
    mut on_entry: F,
) -> anyhow::Result<()>
where
    F: FnMut(IndexWalEntry) -> anyhow::Result<()>,
{
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
        on_entry(entry)?;
    }
    Ok(())
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
    Ok(pid.to_le_bytes().to_vec())
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
    let posting_u64 = decode_u64_fixed(raw.as_ref(), "posting assignment")
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

pub(crate) fn posting_members_snapshot_key(generation: u64, posting_id: usize) -> String {
    let mut key = String::with_capacity(POSTING_MEMBERS_SNAPSHOT_ROOT_PREFIX.len() + 16 + 1 + 10);
    key.push_str(POSTING_MEMBERS_SNAPSHOT_ROOT_PREFIX);
    let _ = write!(&mut key, "{generation:016x}/{posting_id:010}");
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
const POSTING_MEMBER_EVENT_FLAG_ID_ONLY: u8 = 1 << 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct PostingMember {
    pub id: u64,
    pub residual_scale: Option<f32>,
    pub residual_code: Option<Vec<i8>>,
}

const POSTING_MEMBERS_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const POSTING_MEMBERS_SNAPSHOT_BIN_TAG: &[u8] = b"pms1";
const POSTING_MEMBERS_SNAPSHOT_FLAG_HAS_SCALE: u8 = 1 << 0;
const POSTING_MEMBERS_SNAPSHOT_FLAG_HAS_CODE: u8 = 1 << 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct PostingMembersSnapshot {
    pub schema_version: u32,
    pub last_event_seq: u64,
    pub members: Vec<PostingMember>,
}

pub(crate) fn encode_posting_members_snapshot(
    last_event_seq: u64,
    members: &[PostingMember],
) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(
        POSTING_MEMBERS_SNAPSHOT_BIN_TAG.len()
            + 4
            + 8
            + 4
            + members.len().saturating_mul(8 + 1 + 4 + 16),
    );
    out.extend_from_slice(POSTING_MEMBERS_SNAPSHOT_BIN_TAG);
    out.extend_from_slice(&POSTING_MEMBERS_SNAPSHOT_SCHEMA_VERSION.to_le_bytes());
    out.extend_from_slice(&last_event_seq.to_le_bytes());
    let count =
        u32::try_from(members.len()).context("posting members snapshot len does not fit u32")?;
    out.extend_from_slice(&count.to_le_bytes());
    for member in members {
        out.extend_from_slice(&member.id.to_le_bytes());
        let mut flags = 0u8;
        if member.residual_scale.is_some() {
            flags |= POSTING_MEMBERS_SNAPSHOT_FLAG_HAS_SCALE;
        }
        if member.residual_code.is_some() {
            flags |= POSTING_MEMBERS_SNAPSHOT_FLAG_HAS_CODE;
        }
        out.push(flags);
        if let Some(scale) = member.residual_scale {
            out.extend_from_slice(&scale.to_bits().to_le_bytes());
        }
        if let Some(code) = member.residual_code.as_ref() {
            let len = u32::try_from(code.len())
                .context("posting member residual len does not fit u32")?;
            out.extend_from_slice(&len.to_le_bytes());
            for value in code {
                out.push(*value as u8);
            }
        }
    }
    Ok(out)
}

fn decode_posting_members_snapshot(raw: &[u8]) -> anyhow::Result<PostingMembersSnapshot> {
    if !raw.starts_with(POSTING_MEMBERS_SNAPSHOT_BIN_TAG) {
        anyhow::bail!("unsupported posting members snapshot tag");
    }
    let mut cursor = POSTING_MEMBERS_SNAPSHOT_BIN_TAG.len();
    let schema_version = read_u32(raw, &mut cursor)?;
    let last_event_seq = read_u64(raw, &mut cursor)?;
    let count = usize::try_from(read_u32(raw, &mut cursor)?)
        .context("posting members snapshot count overflow")?;
    let mut members = Vec::with_capacity(count);
    for _ in 0..count {
        let id = read_u64(raw, &mut cursor)?;
        let flags = read_u8(raw, &mut cursor)?;
        let unknown_flags = flags
            & !(POSTING_MEMBERS_SNAPSHOT_FLAG_HAS_SCALE | POSTING_MEMBERS_SNAPSHOT_FLAG_HAS_CODE);
        if unknown_flags != 0 {
            anyhow::bail!("posting member snapshot unknown flags: {unknown_flags:#x}");
        }
        let residual_scale = if (flags & POSTING_MEMBERS_SNAPSHOT_FLAG_HAS_SCALE) != 0 {
            Some(read_f32(raw, &mut cursor)?)
        } else {
            None
        };
        let residual_code = if (flags & POSTING_MEMBERS_SNAPSHOT_FLAG_HAS_CODE) != 0 {
            let len = usize::try_from(read_u32(raw, &mut cursor)?)
                .context("posting member residual code len overflow")?;
            let end = cursor
                .checked_add(len)
                .ok_or_else(|| anyhow::anyhow!("posting member residual code length overflow"))?;
            let Some(code_raw) = raw.get(cursor..end) else {
                anyhow::bail!("posting member residual code underflow");
            };
            let code = code_raw.iter().map(|byte| *byte as i8).collect::<Vec<_>>();
            cursor = end;
            Some(code)
        } else {
            None
        };
        members.push(PostingMember {
            id,
            residual_scale,
            residual_code,
        });
    }
    if cursor != raw.len() {
        anyhow::bail!("posting members snapshot trailing bytes");
    }
    Ok(PostingMembersSnapshot {
        schema_version,
        last_event_seq,
        members,
    })
}

#[cfg(test)]
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

pub(crate) fn posting_member_event_upsert_value_id_only(id: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(POSTING_MEMBER_EVENT_RKYV_V3_TAG.len() + 1 + 8);
    out.extend_from_slice(POSTING_MEMBER_EVENT_RKYV_V3_TAG);
    out.push(POSTING_MEMBER_EVENT_FLAG_ID_ONLY);
    out.extend_from_slice(&id.to_le_bytes());
    out
}

#[cfg(test)]
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
    if (flags & POSTING_MEMBER_EVENT_FLAG_ID_ONLY) != 0 {
        if cursor != raw.len() {
            anyhow::bail!("posting-member id-only trailing bytes");
        }
        return Ok(DecodedPostingMemberEvent {
            id,
            tombstone: false,
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
    pub max_event_seq: Option<u64>,
}

fn parse_posting_member_event_seq(key: &[u8], prefix: &[u8]) -> Option<u64> {
    let start = prefix.len();
    let end = start.checked_add(20)?;
    let seq_hex = key.get(start..end)?;
    let seq_str = std::str::from_utf8(seq_hex).ok()?;
    seq_str.parse::<u64>().ok()
}

pub(crate) fn load_posting_members(
    db: &Db,
    generation: u64,
    posting_id: usize,
) -> anyhow::Result<PostingMembersLoadResult> {
    let mut latest = FxHashMap::<u64, PostingMember>::default();
    let mut scanned_events = 0usize;
    let mut max_event_seq = None;
    let snapshot_key = posting_members_snapshot_key(generation, posting_id);
    if let Some(snapshot_raw) = db
        .get(snapshot_key.as_bytes(), ReadOptions::default())
        .with_context(|| {
            format!("load posting members snapshot generation={generation} posting={posting_id}")
        })?
    {
        let snapshot =
            decode_posting_members_snapshot(snapshot_raw.as_ref()).with_context(|| {
                format!(
                    "decode posting members snapshot generation={generation} posting={posting_id}"
                )
            })?;
        if snapshot.schema_version == POSTING_MEMBERS_SNAPSHOT_SCHEMA_VERSION {
            max_event_seq = Some(snapshot.last_event_seq);
            for member in snapshot.members {
                latest.insert(member.id, member);
            }
        }
    }

    let prefix = posting_members_prefix(generation, posting_id);
    let prefix_bytes = prefix.as_bytes().to_vec();
    let end = prefix_exclusive_end(&prefix_bytes)?;
    let start = if let Some(snapshot_seq) = max_event_seq {
        Bytes::from(posting_member_event_key(
            generation,
            posting_id,
            snapshot_seq.saturating_add(1),
            0,
        ))
    } else {
        Bytes::from(prefix_bytes.clone())
    };
    let mut iter = db.iter(
        Range {
            start: Bound::Included(start),
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
        if let Some(seq) = parse_posting_member_event_seq(&key, prefix_bytes.as_slice()) {
            max_event_seq = Some(max_event_seq.map_or(seq, |existing| existing.max(seq)));
        }
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
        latest.insert(
            decoded.id,
            PostingMember {
                id: decoded.id,
                residual_scale: decoded.residual_scale,
                residual_code: decoded.residual_code,
            },
        );
    }
    let members = latest.into_values().collect();
    Ok(PostingMembersLoadResult {
        members,
        scanned_events,
        max_event_seq,
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
        assert_eq!(
            posting_members_snapshot_key(generation, posting_id),
            format!("{POSTING_MEMBERS_SNAPSHOT_ROOT_PREFIX}{generation:016x}/{posting_id:010}")
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

    #[test]
    fn wal_decoder_rejects_checksum_corruption() {
        let ids = vec![101u64, 103, 107, 109];
        let mut encoded = encode_wal_touch_batch_ids(ids.as_slice()).expect("encode wal");
        let payload_start = WAL_BIN_TAG.len() + WAL_FRAME_HEADER_SIZE;
        encoded[payload_start + 2] ^= 0x5a;
        let err =
            decode_wal_entry(encoded.as_slice()).expect_err("checksum corruption should fail");
        assert!(
            format!("{err:#}").contains("checksum"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn diskmeta_wal_upsert_batch_codec_roundtrip() -> anyhow::Result<()> {
        let rows = vec![(7u64, vec![0.1f32, 0.2]), (11u64, vec![1.0f32, -1.5])];
        let mut old_states = FxHashMap::default();
        old_states.insert(7u64, Some((2usize, vec![0.05f32, 0.25])));
        old_states.insert(11u64, None);
        let mut new_postings = FxHashMap::default();
        new_postings.insert(7u64, 3usize);
        new_postings.insert(11u64, 4usize);

        let encoded =
            encode_wal_diskmeta_upsert_batch(rows.as_slice(), &old_states, &new_postings)?;
        let decoded = decode_wal_entry(encoded.as_slice())?;
        match decoded {
            IndexWalEntry::DiskMetaUpsertBatch { rows: decoded_rows } => {
                assert_eq!(decoded_rows.len(), 2);
                assert_eq!(decoded_rows[0].id, 7);
                assert_eq!(decoded_rows[0].old, Some((2usize, vec![0.05, 0.25])));
                assert_eq!(decoded_rows[0].new_posting, 3usize);
                assert_eq!(decoded_rows[0].new_values, vec![0.1, 0.2]);

                assert_eq!(decoded_rows[1].id, 11);
                assert_eq!(decoded_rows[1].old, None);
                assert_eq!(decoded_rows[1].new_posting, 4usize);
                assert_eq!(decoded_rows[1].new_values, vec![1.0, -1.5]);
            }
            other => panic!("expected diskmeta upsert-batch entry, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn vector_wal_upsert_batch_codec_roundtrip() -> anyhow::Result<()> {
        let rows = vec![(5u64, vec![0.25f32, -0.75]), (9u64, vec![1.0f32, 2.0])];
        let encoded = encode_wal_vector_upsert_batch(rows.as_slice())?;
        let decoded = decode_wal_entry(encoded.as_slice())?;
        match decoded {
            IndexWalEntry::VectorUpsertBatch { rows: decoded_rows } => {
                assert_eq!(decoded_rows.len(), 2);
                assert_eq!(decoded_rows[0].id, 5);
                assert_eq!(decoded_rows[0].values, vec![0.25, -0.75]);
                assert_eq!(decoded_rows[1].id, 9);
                assert_eq!(decoded_rows[1].values, vec![1.0, 2.0]);
            }
            other => panic!("expected vector upsert-batch entry, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn vector_wal_delete_batch_codec_roundtrip() -> anyhow::Result<()> {
        let ids = vec![101u64, 205u64, 307u64];
        let encoded = encode_wal_vector_delete_batch(ids.as_slice())?;
        let decoded = decode_wal_entry(encoded.as_slice())?;
        match decoded {
            IndexWalEntry::VectorDeleteBatch { ids: decoded_ids } => {
                assert_eq!(decoded_ids, ids);
            }
            other => panic!("expected vector delete-batch entry, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn diskmeta_wal_delete_batch_codec_roundtrip() -> anyhow::Result<()> {
        let ids = vec![31u64, 41u64];
        let mut old_states = FxHashMap::default();
        old_states.insert(31u64, Some((9usize, vec![0.3f32, -0.7])));
        old_states.insert(41u64, None);

        let encoded = encode_wal_diskmeta_delete_batch(ids.as_slice(), &old_states)?;
        let decoded = decode_wal_entry(encoded.as_slice())?;
        match decoded {
            IndexWalEntry::DiskMetaDeleteBatch { rows: decoded_rows } => {
                assert_eq!(decoded_rows.len(), 2);
                assert_eq!(decoded_rows[0].id, 31);
                assert_eq!(decoded_rows[0].old, Some((9usize, vec![0.3, -0.7])));
                assert_eq!(decoded_rows[1].id, 41);
                assert_eq!(decoded_rows[1].old, None);
            }
            other => panic!("expected diskmeta delete-batch entry, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn fixed_u64_codec_roundtrip() -> anyhow::Result<()> {
        let value = 0x1020_3040_5566_7788u64;
        let encoded = encode_u64_fixed(value);
        let decoded = decode_u64_fixed(encoded.as_slice(), "test_u64")?;
        assert_eq!(decoded, value);
        Ok(())
    }

    #[test]
    fn spfresh_metadata_binary_codec_roundtrip() -> anyhow::Result<()> {
        let expected = SpFreshPersistedMeta {
            schema_version: META_SCHEMA_VERSION,
            dim: 64,
            initial_postings: 128,
            split_limit: 256,
            merge_limit: 64,
            reassign_range: 16,
            nprobe: 8,
            diskmeta_probe_multiplier: 4,
            kmeans_iters: 20,
        };
        let encoded = encode_spfresh_metadata(&expected)?;
        assert!(encoded.starts_with(META_BIN_TAG));
        let decoded = decode_spfresh_metadata(encoded.as_slice())?;
        assert_eq!(decoded, expected);
        Ok(())
    }

    #[test]
    fn load_metadata_rejects_corrupt_payload() -> anyhow::Result<()> {
        let dir = tempfile::TempDir::new()?;
        let db = Db::open(dir.path(), layerdb::DbOptions::default())?;
        db.put(
            META_CONFIG_KEY,
            vec![0xaa, 0xbb, 0xcc],
            WriteOptions { sync: true },
        )?;
        let err = load_metadata(&db).expect_err("invalid metadata payload should fail closed");
        assert!(
            format!("{err:#}").contains("decode spfresh metadata"),
            "unexpected error: {err:#}"
        );
        Ok(())
    }

    #[test]
    fn ensure_metadata_persists_binary_codec_tag() -> anyhow::Result<()> {
        let dir = tempfile::TempDir::new()?;
        let db = Db::open(dir.path(), layerdb::DbOptions::default())?;
        let cfg = SpFreshLayerDbConfig::default();
        ensure_metadata(&db, &cfg)?;
        let raw = db
            .get(META_CONFIG_KEY, ReadOptions::default())?
            .ok_or_else(|| anyhow::anyhow!("metadata missing"))?;
        assert!(raw.as_ref().starts_with(META_BIN_TAG));
        Ok(())
    }

    #[test]
    fn posting_members_snapshot_binary_codec_roundtrip() -> anyhow::Result<()> {
        let members = vec![
            PostingMember {
                id: 10,
                residual_scale: None,
                residual_code: None,
            },
            PostingMember {
                id: 11,
                residual_scale: Some(1.25),
                residual_code: Some(vec![-4, 0, 7]),
            },
            PostingMember {
                id: 12,
                residual_scale: None,
                residual_code: Some(vec![]),
            },
        ];
        let encoded = encode_posting_members_snapshot(55, members.as_slice())?;
        assert!(encoded.starts_with(POSTING_MEMBERS_SNAPSHOT_BIN_TAG));
        let decoded = decode_posting_members_snapshot(encoded.as_slice())?;
        assert_eq!(
            decoded,
            PostingMembersSnapshot {
                schema_version: POSTING_MEMBERS_SNAPSHOT_SCHEMA_VERSION,
                last_event_seq: 55,
                members,
            }
        );
        Ok(())
    }

    #[test]
    fn posting_members_snapshot_binary_decoder_rejects_unknown_flags() -> anyhow::Result<()> {
        let members = vec![PostingMember {
            id: 77,
            residual_scale: None,
            residual_code: None,
        }];
        let mut encoded = encode_posting_members_snapshot(3, members.as_slice())?;
        let flags_offset =
            POSTING_MEMBERS_SNAPSHOT_BIN_TAG.len() + 4 + 8 + 4 + std::mem::size_of::<u64>();
        encoded[flags_offset] = 0x80;
        let err = decode_posting_members_snapshot(encoded.as_slice())
            .expect_err("unknown flags should fail decode");
        assert!(
            format!("{err:#}").contains("unknown flags"),
            "unexpected error: {err:#}"
        );
        Ok(())
    }

    #[test]
    fn ensure_wal_next_seq_recovers_from_corrupt_meta() -> anyhow::Result<()> {
        let dir = tempfile::TempDir::new()?;
        let db = Db::open(dir.path(), layerdb::DbOptions::default())?;
        db.put(
            wal_key(0),
            encode_wal_touch_batch_ids(&[1, 2, 3])?,
            WriteOptions { sync: true },
        )?;
        db.put(
            wal_key(1),
            encode_wal_touch_batch_ids(&[4, 5])?,
            WriteOptions { sync: true },
        )?;
        db.put(
            META_INDEX_WAL_NEXT_SEQ_KEY,
            b"bad".to_vec(),
            WriteOptions { sync: true },
        )?;

        let recovered = ensure_wal_next_seq(&db)?;
        assert_eq!(recovered, 2);
        let meta = db
            .get(META_INDEX_WAL_NEXT_SEQ_KEY, ReadOptions::default())?
            .ok_or_else(|| anyhow::anyhow!("wal next seq metadata missing after recovery"))?;
        assert_eq!(decode_u64_fixed(meta.as_ref(), "wal next seq")?, 2);
        Ok(())
    }

    #[test]
    fn ensure_posting_event_next_seq_recovers_from_corrupt_meta() -> anyhow::Result<()> {
        let dir = tempfile::TempDir::new()?;
        let db = Db::open(dir.path(), layerdb::DbOptions::default())?;
        let generation = 9u64;
        let posting_id = 7usize;
        db.put(
            posting_member_event_key(generation, posting_id, 4, 10),
            posting_member_event_tombstone_value(10)?,
            WriteOptions { sync: true },
        )?;
        db.put(
            posting_member_event_key(generation, posting_id, 8, 11),
            posting_member_event_tombstone_value(11)?,
            WriteOptions { sync: true },
        )?;
        db.put(
            META_POSTING_EVENT_NEXT_SEQ_KEY,
            b"oops".to_vec(),
            WriteOptions { sync: true },
        )?;

        let recovered = ensure_posting_event_next_seq(&db)?;
        assert_eq!(recovered, 9);
        let meta = db
            .get(META_POSTING_EVENT_NEXT_SEQ_KEY, ReadOptions::default())?
            .ok_or_else(|| {
                anyhow::anyhow!("posting-event next seq metadata missing after recovery")
            })?;
        assert_eq!(
            decode_u64_fixed(meta.as_ref(), "posting-event next seq")?,
            9
        );
        Ok(())
    }

    #[test]
    fn load_posting_members_rejects_corrupt_snapshot_payload() -> anyhow::Result<()> {
        let dir = tempfile::TempDir::new()?;
        let db = Db::open(dir.path(), layerdb::DbOptions::default())?;
        let generation = 7u64;
        let posting_id = 3usize;
        db.put(
            posting_members_snapshot_key(generation, posting_id),
            vec![0xde, 0xad, 0xbe, 0xef],
            WriteOptions { sync: true },
        )?;
        let err = load_posting_members(&db, generation, posting_id)
            .expect_err("corrupt snapshot payload should fail closed");
        assert!(
            format!("{err:#}").contains("decode posting members snapshot"),
            "unexpected error: {err:#}"
        );
        Ok(())
    }

    #[test]
    fn load_posting_members_merges_snapshot_with_delta_events() -> anyhow::Result<()> {
        let dir = tempfile::TempDir::new()?;
        let db = Db::open(dir.path(), layerdb::DbOptions::default())?;
        let generation = 7u64;
        let posting_id = 3usize;

        let snapshot_members = vec![
            PostingMember {
                id: 11,
                residual_scale: Some(1.0),
                residual_code: Some(vec![1, 2, 3]),
            },
            PostingMember {
                id: 12,
                residual_scale: Some(1.0),
                residual_code: Some(vec![4, 5, 6]),
            },
        ];
        db.put(
            posting_members_snapshot_key(generation, posting_id),
            encode_posting_members_snapshot(10, snapshot_members.as_slice())?,
            WriteOptions { sync: true },
        )?;
        db.put(
            posting_member_event_key(generation, posting_id, 11, 12),
            posting_member_event_tombstone_value(12)?,
            WriteOptions { sync: true },
        )?;
        db.put(
            posting_member_event_key(generation, posting_id, 12, 13),
            posting_member_event_upsert_value_from_sketch(13, 1.0, &[7, 8, 9])?,
            WriteOptions { sync: true },
        )?;

        let loaded = load_posting_members(&db, generation, posting_id)?;
        let mut ids: Vec<u64> = loaded.members.iter().map(|m| m.id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![11, 13]);
        assert_eq!(loaded.max_event_seq, Some(12));
        Ok(())
    }

    #[test]
    fn load_posting_members_accepts_id_only_events() -> anyhow::Result<()> {
        let dir = tempfile::TempDir::new()?;
        let db = Db::open(dir.path(), layerdb::DbOptions::default())?;
        let generation = 11u64;
        let posting_id = 5usize;
        db.put(
            posting_member_event_key(generation, posting_id, 1, 42),
            posting_member_event_upsert_value_id_only(42),
            WriteOptions { sync: true },
        )?;

        let loaded = load_posting_members(&db, generation, posting_id)?;
        assert_eq!(loaded.members.len(), 1);
        assert_eq!(loaded.members[0].id, 42);
        assert!(loaded.members[0].residual_scale.is_none());
        assert!(loaded.members[0].residual_code.is_none());
        assert_eq!(loaded.max_event_seq, Some(1));
        Ok(())
    }
}
