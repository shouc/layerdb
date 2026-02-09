use layerdb::{Db, DbOptions, WriteOptions};
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
fn scrub_report_counts_files_and_entries() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
    db.put(&b"c"[..], &b"3"[..], WriteOptions { sync: true })?;

    db.compact_range(None)?;

    let report = db.scrub_integrity()?;
    assert!(report.files_checked >= 1);
    let total_entries: u64 = report.entries_by_level.values().sum();
    assert!(total_entries >= 3);

    Ok(())
}

#[test]
fn scrub_report_empty_db_is_zero() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    let report = db.scrub_integrity()?;
    assert_eq!(report.files_checked, 0);
    assert!(report.entries_by_level.is_empty());

    Ok(())
}
