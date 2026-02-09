use layerdb::{Db, DbOptions, ReadOptions, SnapshotId, WriteOptions};
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
fn branch_checkout_changes_default_read_snapshot() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
    let snap_v1: SnapshotId = db.create_snapshot()?;

    db.put(&b"k"[..], &b"v2"[..], WriteOptions { sync: true })?;

    // By default, we see v2.
    assert_eq!(
        db.get(b"k", ReadOptions::default())?,
        Some(bytes::Bytes::from("v2"))
    );

    db.create_branch("v1", Some(snap_v1))?;
    assert!(db.list_branches().iter().any(|(n, _)| n == "v1"));

    db.checkout("v1")?;
    assert_eq!(db.current_branch(), "v1");

    // After checkout, default reads are at the branch seqno (v1).
    assert_eq!(
        db.get(b"k", ReadOptions::default())?,
        Some(bytes::Bytes::from("v1"))
    );

    // But explicit snapshots still work.
    assert_eq!(
        db.get(
            b"k",
            ReadOptions {
                snapshot: Some(snap_v1)
            }
        )?,
        Some(bytes::Bytes::from("v1"))
    );

    Ok(())
}

#[test]
fn branches_persist_across_restart() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
        let snap_v1 = db.create_snapshot()?;
        db.create_branch("v1", Some(snap_v1))?;
    }

    {
        let db = Db::open(dir.path(), options())?;
        let branches = db.list_branches();
        assert!(branches.iter().any(|(n, _)| n == "v1"));

        db.checkout("v1")?;
        assert_eq!(
            db.get(b"k", ReadOptions::default())?,
            Some(bytes::Bytes::from("v1"))
        );
    }

    Ok(())
}
