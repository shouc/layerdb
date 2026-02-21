use bytes::Bytes;
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
fn multi_get_reads_consistent_snapshot() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put("a", "1", WriteOptions { sync: true })?;
    db.put("b", "2", WriteOptions { sync: true })?;
    db.put("c", "3", WriteOptions { sync: true })?;

    let snap = db.create_snapshot()?;
    db.put("b", "9", WriteOptions { sync: true })?;
    db.delete("c", WriteOptions { sync: true })?;

    let keys = vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")];
    let latest = db.multi_get(&keys, ReadOptions::default())?;
    let at_snap = db.multi_get(
        &keys,
        ReadOptions {
            snapshot: Some(snap),
        },
    )?;

    assert_eq!(latest[0].as_deref(), Some(&b"1"[..]));
    assert_eq!(latest[1].as_deref(), Some(&b"9"[..]));
    assert_eq!(latest[2], None);

    assert_eq!(at_snap[0].as_deref(), Some(&b"1"[..]));
    assert_eq!(at_snap[1].as_deref(), Some(&b"2"[..]));
    assert_eq!(at_snap[2].as_deref(), Some(&b"3"[..]));

    db.release_snapshot(snap);
    Ok(())
}
