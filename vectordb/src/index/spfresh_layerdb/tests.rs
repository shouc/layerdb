use std::fs;

use layerdb::DbOptions;
use tempfile::TempDir;

use crate::types::{VectorIndex, VectorRecord};

use super::{SpFreshLayerDbConfig, SpFreshLayerDbIndex};

#[test]
fn persists_and_recovers_vectors() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.upsert(1, vec![0.0; cfg.spfresh.dim]);
        idx.upsert(2, vec![1.0; cfg.spfresh.dim]);
        idx.delete(1);
        idx.force_rebuild()?;
    }

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 1);
    let got = idx.search(&vec![1.0; 64], 1);
    assert_eq!(got[0].id, 2);
    Ok(())
}

#[test]
fn wal_persists_indexed_vectors_across_restart() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.upsert(11, vec![0.25; cfg.spfresh.dim]);
        idx.upsert(12, vec![0.5; cfg.spfresh.dim]);
    }

    let wal_dir = dir.path().join("wal");
    assert!(wal_dir.is_dir());
    let has_segment = fs::read_dir(&wal_dir)?
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .any(|name| name.starts_with("wal_") && name.ends_with(".log"));
    assert!(has_segment, "expected wal segment in {}", wal_dir.display());

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 2);
    let got = idx.search(&vec![0.5; 64], 1);
    assert_eq!(got[0].id, 12);
    Ok(())
}

#[test]
fn rejects_invalid_config() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let mut cfg = SpFreshLayerDbConfig::default();
    cfg.spfresh.split_limit = 8;
    cfg.spfresh.merge_limit = 8;
    let err = match SpFreshLayerDbIndex::open(dir.path(), cfg) {
        Ok(_) => anyhow::bail!("expected config validation error"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("split_limit"));
    Ok(())
}

#[test]
fn rejects_mismatched_persisted_config() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.upsert(1, vec![0.1; cfg.spfresh.dim]);
        idx.close()?;
    }

    let mut different = cfg.clone();
    different.spfresh.nprobe = cfg.spfresh.nprobe + 1;
    let err = match SpFreshLayerDbIndex::open(dir.path(), different) {
        Ok(_) => anyhow::bail!("expected metadata mismatch"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("config mismatch"));
    Ok(())
}

#[test]
fn stats_track_operations() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    idx.try_upsert(1, vec![0.0; cfg.spfresh.dim])?;
    idx.try_upsert(2, vec![1.0; cfg.spfresh.dim])?;
    assert!(idx.try_delete(1)?);
    idx.force_rebuild()?;

    let stats = idx.stats();
    assert_eq!(stats.total_upserts, 2);
    assert_eq!(stats.total_deletes, 1);
    assert_eq!(stats.persist_errors, 0);
    assert!(stats.rebuild_successes >= 1);
    assert_eq!(stats.rebuild_failures, 0);
    assert_eq!(stats.last_rebuild_rows, 1);
    Ok(())
}

#[test]
fn bulk_load_replaces_existing_dataset() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    idx.try_upsert(1, vec![0.0; cfg.spfresh.dim])?;
    idx.try_upsert(2, vec![1.0; cfg.spfresh.dim])?;
    idx.try_bulk_load(&[VectorRecord::new(42, vec![0.42; cfg.spfresh.dim])])?;
    idx.close()?;

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 1);
    let got = idx.search(&vec![0.42; 64], 1);
    assert_eq!(got[0].id, 42);
    Ok(())
}

#[test]
fn health_check_reports_stats_and_integrity() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    idx.try_upsert(7, vec![0.7; cfg.spfresh.dim])?;
    let health = idx.health_check()?;
    assert_eq!(health.total_upserts, 1);
    assert_eq!(health.persist_errors, 0);
    Ok(())
}

#[test]
fn open_existing_uses_persisted_metadata() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(9, vec![0.9; cfg.spfresh.dim])?;
        idx.close()?;
    }

    let idx = SpFreshLayerDbIndex::open_existing(dir.path(), DbOptions::default())?;
    assert_eq!(idx.len(), 1);
    let got = idx.search(&vec![0.9; 64], 1);
    assert_eq!(got[0].id, 9);
    Ok(())
}

#[test]
fn s3_sync_and_thaw_round_trip() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    idx.try_upsert(7, vec![0.7; cfg.spfresh.dim])?;
    idx.try_upsert(9, vec![0.9; cfg.spfresh.dim])?;

    let moved = idx.sync_to_s3(None)?;
    assert!(moved >= 1, "expected at least one file frozen to s3 tier");
    assert!(!idx.frozen_objects().is_empty());

    let got_frozen = idx.search(&vec![0.9; cfg.spfresh.dim], 1);
    assert_eq!(got_frozen[0].id, 9);

    let thawed = idx.thaw_from_s3(None)?;
    assert!(thawed >= 1, "expected at least one file thawed from s3 tier");

    let got_thawed = idx.search(&vec![0.9; cfg.spfresh.dim], 1);
    assert_eq!(got_thawed[0].id, 9);
    Ok(())
}

#[test]
fn s3_gc_removes_local_emulation_orphans() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    idx.try_upsert(1, vec![0.1; cfg.spfresh.dim])?;
    let moved = idx.sync_to_s3(None)?;
    assert!(moved >= 1);

    let orphan_root = dir.path().join("sst_s3").join("L1").join("L1-deadbeef00000001");
    fs::create_dir_all(&orphan_root)?;
    let orphan_meta = orphan_root.join("meta.bin");
    let orphan_sb = orphan_root.join("sb_00000000.bin");
    fs::write(&orphan_meta, b"orphan-meta")?;
    fs::write(&orphan_sb, b"orphan-sb")?;
    assert!(orphan_meta.exists());
    assert!(orphan_sb.exists());

    let removed = idx.gc_orphaned_s3()?;
    assert!(removed >= 1);
    assert!(!orphan_meta.exists());
    assert!(!orphan_sb.exists());
    Ok(())
}
