use std::ops::Bound;
use std::path::Path;

use anyhow::Context;
use bytes::Bytes;
use layerdb::{Db, Range, ReadOptions, WriteOptions};

use crate::types::VectorRecord;

use super::config::{
    SpFreshLayerDbConfig, SpFreshPersistedMeta, META_ACTIVE_GENERATION_KEY, META_CONFIG_KEY,
    META_SCHEMA_VERSION, VECTOR_ROOT_PREFIX,
};

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
        let row: VectorRecord = bincode::deserialize(value.as_ref())
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
    let row: VectorRecord = bincode::deserialize(raw.as_ref())
        .with_context(|| format!("decode vector row id={id} generation={generation}"))?;
    Ok((!row.deleted).then_some(row))
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

pub(crate) fn vector_prefix(generation: u64) -> String {
    format!("{VECTOR_ROOT_PREFIX}{generation:016x}/")
}

pub(crate) fn vector_key(generation: u64, id: u64) -> String {
    format!("{}{id:020}", vector_prefix(generation))
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
