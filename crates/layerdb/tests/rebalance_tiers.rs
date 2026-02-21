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

fn options_with_hdd_limited(max_moves: usize) -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 32 * 1024,
        memtable_bytes: 4 * 1024,
        fsync_writes: true,
        enable_hdd_tier: true,
        hot_levels_max: 0,
        tier_rebalance_max_moves: max_moves,
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

#[test]
fn rebalance_respects_max_move_budget_per_run() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options_nvme_only())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
        db.put(&b"c"[..], &b"3"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;

        db.put(&b"d"[..], &b"4"[..], WriteOptions { sync: true })?;
        db.put(&b"e"[..], &b"5"[..], WriteOptions { sync: true })?;
        db.put(&b"f"[..], &b"6"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;
    }

    let before_nvme = count_ssts(&dir.path().join("sst"))?;
    assert!(before_nvme >= 2, "expected at least two NVMe SSTs");

    {
        let db = Db::open(dir.path(), options_with_hdd_limited(1))?;
        let moved = db.rebalance_tiers()?;
        assert_eq!(moved, 1, "expected exactly one move due to budget");
    }

    let after_first_nvme = count_ssts(&dir.path().join("sst"))?;
    let after_first_hdd = count_ssts(&dir.path().join("sst_hdd"))?;
    assert!(after_first_hdd >= 1);
    assert_eq!(after_first_nvme + 1, before_nvme);

    {
        let db = Db::open(dir.path(), options_with_hdd_limited(1))?;
        let moved = db.rebalance_tiers()?;
        assert!(moved >= 1);
    }

    let after_second_nvme = count_ssts(&dir.path().join("sst"))?;
    assert!(after_second_nvme <= before_nvme.saturating_sub(2));

    Ok(())
}
