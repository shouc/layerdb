use layerdb::{Db, DbOptions, ReadOptions, WriteOptions};
use tempfile::TempDir;

fn hdd_options() -> DbOptions {
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

fn hdd_options_with_budget(budget_bytes: u64) -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 32 * 1024,
        memtable_bytes: 4 * 1024,
        fsync_writes: true,
        enable_hdd_tier: true,
        hot_levels_max: 0,
        hdd_compaction_budget_bytes: budget_bytes,
        ..Default::default()
    }
}

fn has_sst_files(dir: &std::path::Path) -> anyhow::Result<bool> {
    if !dir.exists() {
        return Ok(false);
    }
    for item in std::fs::read_dir(dir)? {
        let path = item?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("sst") {
            return Ok(true);
        }
    }
    Ok(false)
}

#[test]
fn l1_compaction_outputs_to_hdd_when_hot_levels_zero() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), hdd_options())?;

    db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
    db.put(&b"c"[..], &b"3"[..], WriteOptions { sync: true })?;

    db.compact_range(None)?;

    let hdd_dir = dir.path().join("sst_hdd");

    assert!(has_sst_files(&hdd_dir)?);

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

    drop(db);

    let reopened = Db::open(dir.path(), hdd_options())?;
    assert_eq!(
        reopened.get(b"a", ReadOptions::default())?,
        Some(bytes::Bytes::from("1"))
    );
    assert_eq!(
        reopened.get(b"b", ReadOptions::default())?,
        Some(bytes::Bytes::from("2"))
    );
    assert_eq!(
        reopened.get(b"c", ReadOptions::default())?,
        Some(bytes::Bytes::from("3"))
    );

    Ok(())
}

#[test]
fn hdd_compaction_budget_can_skip_compaction_runs() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), hdd_options_with_budget(0))?;

    db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
    db.put(&b"c"[..], &b"3"[..], WriteOptions { sync: true })?;

    db.compact_range(None)?;

    let hdd_dir = dir.path().join("sst_hdd");
    assert!(!has_sst_files(&hdd_dir)?);

    Ok(())
}
