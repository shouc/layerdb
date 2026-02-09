use layerdb::{Db, DbOptions, ReadOptions, WriteOptions};
use tempfile::TempDir;

fn options() -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 32 * 1024,
        memtable_bytes: 4 * 1024,
        fsync_writes: true,
        ..Default::default()
    }
}

#[test]
fn db_metrics_track_s3_activity_in_process() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    db.compact_range(None)?;
    let moved = db.freeze_level_to_s3(1, None)?;
    assert!(moved >= 1);

    let initial = db.metrics();
    assert!(initial.version.frozen_s3_files >= 1);
    assert!(initial.version.tier.s3_puts >= 1);

    let _ = db.get(b"a", ReadOptions::default())?;
    let after_first_get = db.metrics();
    assert!(after_first_get.version.tier.s3_gets >= 1);

    let _ = db.get(b"a", ReadOptions::default())?;
    let after_second_get = db.metrics();
    assert!(after_second_get.version.tier.s3_get_cache_hits >= 1);

    Ok(())
}

#[test]
fn db_metrics_include_level_debt_fields() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    db.compact_range(None)?;

    let metrics = db.metrics();
    assert!(metrics.version.levels.contains_key(&0));
    assert!(metrics.version.levels.contains_key(&1));
    assert!(metrics
        .version
        .compaction_debt_bytes_by_level
        .contains_key(&0));
    assert!(metrics
        .version
        .compaction_debt_bytes_by_level
        .contains_key(&1));

    Ok(())
}
