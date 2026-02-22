use super::*;

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
    assert!(
        thawed >= 1,
        "expected at least one file thawed from s3 tier"
    );

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

    let orphan_root = dir
        .path()
        .join("sst_s3")
        .join("L1")
        .join("L1-deadbeef00000001");
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

#[test]
fn s3_sync_round_trip_against_minio_when_enabled() -> anyhow::Result<()> {
    if !minio_enabled() {
        eprintln!("skipping minio integration test (set LAYERDB_MINIO_INTEGRATION=1)");
        return Ok(());
    }

    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    if cfg.db_options.s3.is_none() {
        anyhow::bail!("minio integration requires LAYERDB_S3_* env vars");
    }

    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    idx.try_upsert(5, vec![0.5; cfg.spfresh.dim])?;
    idx.try_upsert(8, vec![0.8; cfg.spfresh.dim])?;

    let moved = idx.sync_to_s3(None)?;
    assert!(moved >= 1);
    assert!(!idx.frozen_objects().is_empty());

    let got_frozen = idx.search(&vec![0.8; cfg.spfresh.dim], 1);
    assert_eq!(got_frozen[0].id, 8);

    let thawed = idx.thaw_from_s3(None)?;
    assert!(thawed >= 1);

    let got_thawed = idx.search(&vec![0.8; cfg.spfresh.dim], 1);
    assert_eq!(got_thawed[0].id, 8);
    Ok(())
}
