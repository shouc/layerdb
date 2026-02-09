use layerdb::{Db, DbOptions, ReadOptions, WriteOptions};
use tempfile::TempDir;

fn options_nvme_only() -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 32 * 1024,
        memtable_bytes: 4 * 1024,
        fsync_writes: true,
        enable_hdd_tier: false,
        hot_levels_max: 2,
        ..Default::default()
    }
}

fn options_with_hdd() -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 32 * 1024,
        memtable_bytes: 4 * 1024,
        fsync_writes: true,
        enable_hdd_tier: true,
        hot_levels_max: 0,
        ..Default::default()
    }
}

fn count_ssts(dir: &std::path::Path) -> anyhow::Result<usize> {
    if !dir.exists() {
        return Ok(0);
    }
    Ok(std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|ext| ext.to_str()) == Some("sst"))
        .count())
}

#[test]
fn rebalance_moves_l1_files_to_hdd_and_preserves_reads() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    // Build DB with NVMe-only configuration first so L1 lives under `sst/`.
    {
        let db = Db::open(dir.path(), options_nvme_only())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
        db.put(&b"c"[..], &b"3"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;

        assert_eq!(
            db.get(b"a", ReadOptions::default())?,
            Some(bytes::Bytes::from("1"))
        );
    }

    // Reopen with HDD tier enabled and rebalance.
    {
        let db = Db::open(dir.path(), options_with_hdd())?;
        let moved = db.rebalance_tiers()?;
        assert!(moved >= 1, "expected at least one file move");

        assert_eq!(
            db.get(b"a", ReadOptions::default())?,
            Some(bytes::Bytes::from("1"))
        );
        assert_eq!(
            db.get(b"b", ReadOptions::default())?,
            Some(bytes::Bytes::from("2"))
        );
        assert_eq!(
            db.get(b"c", ReadOptions::default())?,
            Some(bytes::Bytes::from("3"))
        );
    }

    let nvme_count = count_ssts(&dir.path().join("sst"))?;
    let hdd_count = count_ssts(&dir.path().join("sst_hdd"))?;
    assert!(hdd_count >= 1, "expected moved files in sst_hdd/");
    assert!(nvme_count <= 1, "expected most files moved from sst/");

    // Restart should preserve manifest tier mapping.
    {
        let db = Db::open(dir.path(), options_with_hdd())?;
        assert_eq!(
            db.get(b"a", ReadOptions::default())?,
            Some(bytes::Bytes::from("1"))
        );
    }

    Ok(())
}
