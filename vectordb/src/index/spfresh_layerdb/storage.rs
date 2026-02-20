use std::ops::Bound;
use std::path::Path;

use anyhow::Context;
use bytes::Bytes;
use layerdb::{Db, Range, ReadOptions, WriteOptions};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::types::VectorRecord;

use super::config::{
    SpFreshLayerDbConfig, SpFreshPersistedMeta, INDEX_WAL_PREFIX, META_ACTIVE_GENERATION_KEY,
    META_CONFIG_KEY, META_INDEX_CHECKPOINT_KEY, META_INDEX_WAL_NEXT_SEQ_KEY, META_SCHEMA_VERSION,
    POSTING_MAP_ROOT_PREFIX, POSTING_MEMBERS_ROOT_PREFIX, VECTOR_ROOT_PREFIX,
};

const VECTOR_ROW_RKYV_TAG: &[u8] = b"vr1";
const VECTOR_ROW_RKYV_V2_TAG: &[u8] = b"vr2";

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug)]
#[archive(check_bytes)]
struct VectorRowValueRkyv {
    id: u64,
    values: Vec<f32>,
    version: u32,
    deleted: bool,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug)]
#[archive(check_bytes)]
struct VectorRowValueRkyvV2 {
    id: u64,
    values: Vec<f32>,
    version: u32,
    deleted: bool,
    posting_id: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct DecodedVectorRow {
    pub row: VectorRecord,
    pub posting_id: Option<usize>,
}

pub(crate) fn encode_vector_row_value(row: &VectorRecord) -> anyhow::Result<Vec<u8>> {
    encode_vector_row_value_with_posting(row, None)
}

pub(crate) fn encode_vector_row_value_with_posting(
    row: &VectorRecord,
    posting_id: Option<usize>,
) -> anyhow::Result<Vec<u8>> {
    if let Some(posting_id) = posting_id {
        let posting_id = u64::try_from(posting_id).context("posting id does not fit u64")?;
        let payload = VectorRowValueRkyvV2 {
            id: row.id,
            values: row.values.clone(),
            version: row.version,
            deleted: row.deleted,
            posting_id: Some(posting_id),
        };
        let archived = rkyv::to_bytes::<_, 1_024>(&payload)
            .context("encode vector row value v2 with rkyv")?;
        let mut out = Vec::with_capacity(VECTOR_ROW_RKYV_V2_TAG.len() + archived.len());
        out.extend_from_slice(VECTOR_ROW_RKYV_V2_TAG);
        out.extend_from_slice(archived.as_ref());
        return Ok(out);
    }

    let payload = VectorRowValueRkyv {
        id: row.id,
        values: row.values.clone(),
        version: row.version,
        deleted: row.deleted,
    };
    let archived =
        rkyv::to_bytes::<_, 1_024>(&payload).context("encode vector row value with rkyv")?;
    let mut out = Vec::with_capacity(VECTOR_ROW_RKYV_TAG.len() + archived.len());
    out.extend_from_slice(VECTOR_ROW_RKYV_TAG);
    out.extend_from_slice(archived.as_ref());
    Ok(out)
}

pub(crate) fn decode_vector_row_value(raw: &[u8]) -> anyhow::Result<VectorRecord> {
    Ok(decode_vector_row_with_posting(raw)?.row)
}

pub(crate) fn decode_vector_row_with_posting(raw: &[u8]) -> anyhow::Result<DecodedVectorRow> {
    if raw.starts_with(VECTOR_ROW_RKYV_V2_TAG) {
        let mut aligned =
            rkyv::AlignedVec::with_capacity(raw.len() - VECTOR_ROW_RKYV_V2_TAG.len());
        aligned.extend_from_slice(&raw[VECTOR_ROW_RKYV_V2_TAG.len()..]);
        let archived = rkyv::check_archived_root::<VectorRowValueRkyvV2>(&aligned)
            .map_err(|err| anyhow::anyhow!("decode rkyv vector row v2: {err}"))?;
        let posting_id = match archived.posting_id.as_ref() {
            Some(v) => {
                Some(usize::try_from(*v).context("posting id does not fit usize")?)
            }
            None => None,
        };
        return Ok(DecodedVectorRow {
            row: VectorRecord {
                id: archived.id,
                values: archived.values.iter().copied().collect(),
                version: archived.version,
                deleted: archived.deleted,
            },
            posting_id,
        });
    }
    if raw.starts_with(VECTOR_ROW_RKYV_TAG) {
        let mut aligned = rkyv::AlignedVec::with_capacity(raw.len() - VECTOR_ROW_RKYV_TAG.len());
        aligned.extend_from_slice(&raw[VECTOR_ROW_RKYV_TAG.len()..]);
        let archived = rkyv::check_archived_root::<VectorRowValueRkyv>(&aligned)
            .map_err(|err| anyhow::anyhow!("decode rkyv vector row: {err}"))?;
        return Ok(DecodedVectorRow {
            row: VectorRecord {
                id: archived.id,
                values: archived.values.iter().copied().collect(),
                version: archived.version,
                deleted: archived.deleted,
            },
            posting_id: None,
        });
    }
    // Backward compatibility with legacy bincode row payloads.
    let row = bincode::deserialize::<VectorRecord>(raw)
        .context("decode legacy vector row payload")?;
    Ok(DecodedVectorRow {
        row,
        posting_id: None,
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
) -> anyhow::Result<(Vec<VectorRecord>, HashMap<u64, usize>)> {
    let mut rows = Vec::new();
    let mut assignments = HashMap::new();
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
        let decoded = decode_vector_row_with_posting(value.as_ref()).with_context(|| {
            format!("decode vector row key={}", String::from_utf8_lossy(&key))
        })?;
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

pub(crate) fn wal_key(seq: u64) -> String {
    format!("{INDEX_WAL_PREFIX}{seq:020}")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum IndexWalEntry {
    Upsert {
        id: u64,
        vector: Vec<f32>,
    },
    Delete {
        id: u64,
    },
    IdOnly {
        id: u64,
    },
    DiskMetaUpsert {
        id: u64,
        old: Option<(usize, Vec<f32>)>,
        new_posting: usize,
        new_vector: Vec<f32>,
    },
    DiskMetaDelete {
        id: u64,
        old: Option<(usize, Vec<f32>)>,
    },
}

pub(crate) fn encode_wal_entry(entry: &IndexWalEntry) -> anyhow::Result<Vec<u8>> {
    bincode::serialize(entry).context("encode wal entry")
}

pub(crate) fn decode_wal_entry(raw: &[u8]) -> anyhow::Result<IndexWalEntry> {
    if let Ok(entry) = bincode::deserialize::<IndexWalEntry>(raw) {
        return Ok(entry);
    }
    let id = bincode::deserialize::<u64>(raw).context("decode legacy wal id")?;
    Ok(IndexWalEntry::IdOnly { id })
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
    format!("{VECTOR_ROOT_PREFIX}{generation:016x}/")
}

pub(crate) fn vector_key(generation: u64, id: u64) -> String {
    format!("{}{id:020}", vector_prefix(generation))
}

pub(crate) fn posting_map_prefix(generation: u64) -> String {
    format!("{POSTING_MAP_ROOT_PREFIX}{generation:016x}/")
}

pub(crate) fn posting_map_key(generation: u64, id: u64) -> String {
    format!("{}{id:020}", posting_map_prefix(generation))
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
    format!(
        "{}/{posting_id:010}/",
        posting_members_generation_prefix(generation)
    )
}

pub(crate) fn posting_members_generation_prefix(generation: u64) -> String {
    format!("{POSTING_MEMBERS_ROOT_PREFIX}{generation:016x}")
}

pub(crate) fn posting_member_key(generation: u64, posting_id: usize, id: u64) -> String {
    format!("{}{id:020}", posting_members_prefix(generation, posting_id))
}

const POSTING_MEMBER_RKYV_TAG: &[u8] = b"rk1";
const POSTING_MEMBER_RKYV_V2_TAG: &[u8] = b"rk2";

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug)]
#[archive(check_bytes)]
struct PostingMemberValueRkyv {
    id: u64,
    values: Vec<f32>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug)]
#[archive(check_bytes)]
struct PostingMemberValueRkyvV2 {
    id: u64,
    values: Option<Vec<f32>>,
    residual_scale: f32,
    residual_code: Vec<i8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PostingMemberValueLegacy {
    id: u64,
    values: Vec<f32>,
}

#[derive(Clone, Debug)]
pub(crate) struct PostingMember {
    pub id: u64,
    pub values: Option<Vec<f32>>,
    pub residual_scale: Option<f32>,
    pub residual_code: Option<Vec<i8>>,
}

#[cfg(test)]
pub(crate) fn posting_member_value(id: u64, values: &[f32]) -> anyhow::Result<Vec<u8>> {
    let payload = PostingMemberValueRkyv {
        id,
        values: values.to_vec(),
    };
    let archived =
        rkyv::to_bytes::<_, 1_024>(&payload).context("encode posting member value with rkyv")?;
    let mut out = Vec::with_capacity(POSTING_MEMBER_RKYV_TAG.len() + archived.len());
    out.extend_from_slice(POSTING_MEMBER_RKYV_TAG);
    out.extend_from_slice(archived.as_ref());
    Ok(out)
}

fn quantize_residual(values: &[f32], centroid: &[f32]) -> (f32, Vec<i8>) {
    debug_assert_eq!(values.len(), centroid.len());
    if values.is_empty() {
        return (1.0, Vec::new());
    }
    let mut max_abs = 0.0f32;
    for (v, c) in values.iter().zip(centroid.iter()) {
        let a = (*v - *c).abs();
        if a > max_abs {
            max_abs = a;
        }
    }
    let scale = if max_abs <= 1e-9 { 1e-9 } else { max_abs / 127.0 };
    let mut code = Vec::with_capacity(values.len());
    for (v, c) in values.iter().zip(centroid.iter()) {
        let q = ((*v - *c) / scale).round().clamp(-127.0, 127.0) as i8;
        code.push(q);
    }
    (scale, code)
}

pub(crate) fn posting_member_value_with_residual(
    id: u64,
    values: &[f32],
    centroid: &[f32],
) -> anyhow::Result<Vec<u8>> {
    let (residual_scale, residual_code) = quantize_residual(values, centroid);
    let payload = PostingMemberValueRkyvV2 {
        id,
        values: Some(values.to_vec()),
        residual_scale,
        residual_code,
    };
    let archived = rkyv::to_bytes::<_, 1_024>(&payload)
        .context("encode posting member value (residual) with rkyv")?;
    let mut out = Vec::with_capacity(POSTING_MEMBER_RKYV_V2_TAG.len() + archived.len());
    out.extend_from_slice(POSTING_MEMBER_RKYV_V2_TAG);
    out.extend_from_slice(archived.as_ref());
    Ok(out)
}

pub(crate) fn posting_member_value_with_residual_only(
    id: u64,
    values: &[f32],
    centroid: &[f32],
) -> anyhow::Result<Vec<u8>> {
    let (residual_scale, residual_code) = quantize_residual(values, centroid);
    let payload = PostingMemberValueRkyvV2 {
        id,
        values: None,
        residual_scale,
        residual_code,
    };
    let archived = rkyv::to_bytes::<_, 1_024>(&payload)
        .context("encode posting member value (residual) with rkyv")?;
    let mut out = Vec::with_capacity(POSTING_MEMBER_RKYV_V2_TAG.len() + archived.len());
    out.extend_from_slice(POSTING_MEMBER_RKYV_V2_TAG);
    out.extend_from_slice(archived.as_ref());
    Ok(out)
}

pub(crate) fn load_posting_members(
    db: &Db,
    generation: u64,
    posting_id: usize,
) -> anyhow::Result<Vec<PostingMember>> {
    let mut out = Vec::new();
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
        let raw = value.as_ref();
        let decoded = (|| -> anyhow::Result<PostingMember> {
            if raw.starts_with(POSTING_MEMBER_RKYV_V2_TAG) {
                let mut aligned = rkyv::AlignedVec::with_capacity(
                    raw.len().saturating_sub(POSTING_MEMBER_RKYV_V2_TAG.len()),
                );
                aligned.extend_from_slice(&raw[POSTING_MEMBER_RKYV_V2_TAG.len()..]);
                let archived = rkyv::check_archived_root::<PostingMemberValueRkyvV2>(&aligned)
                    .map_err(|err| anyhow::anyhow!("decode rkyv posting member v2: {err}"))?;
                return Ok(PostingMember {
                    id: archived.id,
                    values: archived
                        .values
                        .as_ref()
                        .map(|vals| vals.iter().copied().collect()),
                    residual_scale: Some(archived.residual_scale),
                    residual_code: Some(archived.residual_code.iter().copied().collect()),
                });
            }
            if raw.starts_with(POSTING_MEMBER_RKYV_TAG) {
                let mut aligned = rkyv::AlignedVec::with_capacity(
                    raw.len().saturating_sub(POSTING_MEMBER_RKYV_TAG.len()),
                );
                aligned.extend_from_slice(&raw[POSTING_MEMBER_RKYV_TAG.len()..]);
                let archived = rkyv::check_archived_root::<PostingMemberValueRkyv>(&aligned)
                    .map_err(|err| anyhow::anyhow!("decode rkyv posting member: {err}"))?;
                return Ok(PostingMember {
                    id: archived.id,
                    values: Some(archived.values.iter().copied().collect()),
                    residual_scale: None,
                    residual_code: None,
                });
            }
            if let Ok(payload) = bincode::deserialize::<PostingMemberValueLegacy>(raw) {
                return Ok(PostingMember {
                    id: payload.id,
                    values: Some(payload.values),
                    residual_scale: None,
                    residual_code: None,
                });
            }
            let id = bincode::deserialize::<u64>(raw)?;
            Ok(PostingMember {
                id,
                values: None,
                residual_scale: None,
                residual_code: None,
            })
        })()
        .with_context(|| {
            format!(
                "decode posting member key={}",
                String::from_utf8_lossy(&key)
            )
        })?;
        out.push(decoded);
    }
    Ok(out)
}

pub(crate) fn load_posting_assignments(
    db: &Db,
    generation: u64,
) -> anyhow::Result<HashMap<u64, usize>> {
    let mut out = HashMap::new();
    let prefix = posting_map_prefix(generation);
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
        let posting_u64: u64 = bincode::deserialize(value.as_ref()).with_context(|| {
            format!(
                "decode posting assignment key={}",
                String::from_utf8_lossy(&key)
            )
        })?;
        let posting =
            usize::try_from(posting_u64).context("posting assignment does not fit usize")?;
        let key_str = String::from_utf8_lossy(&key);
        let Some(id_suffix) = key_str.rsplit('/').next() else {
            continue;
        };
        let id: u64 = id_suffix
            .parse()
            .with_context(|| format!("decode posting assignment id from key={key_str}"))?;
        out.insert(id, posting);
    }
    Ok(out)
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
