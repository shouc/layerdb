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

#[test]
fn branch_head_advances_with_writes_and_persists() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
        let snap_v1 = db.create_snapshot()?;
        db.create_branch("feature", Some(snap_v1))?;

        db.checkout("feature")?;
        db.put(&b"k"[..], &b"v2"[..], WriteOptions { sync: true })?;

        let feature_head = db
            .list_branches()
            .into_iter()
            .find(|(name, _)| name == "feature")
            .expect("feature branch exists")
            .1;
        let main_head = db
            .list_branches()
            .into_iter()
            .find(|(name, _)| name == "main")
            .expect("main branch exists")
            .1;
        assert!(feature_head > main_head);
    }

    {
        let db = Db::open(dir.path(), options())?;
        db.checkout("feature")?;
        assert_eq!(
            db.get(b"k", ReadOptions::default())?,
            Some(bytes::Bytes::from("v2"))
        );

        db.checkout("main")?;
        assert_eq!(
            db.get(b"k", ReadOptions::default())?,
            Some(bytes::Bytes::from("v1"))
        );
    }

    Ok(())
}

#[test]
fn reopen_defaults_to_main_branch_head() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
        let snap_v1 = db.create_snapshot()?;
        db.create_branch("feature", Some(snap_v1))?;

        db.checkout("feature")?;
        db.put(&b"k"[..], &b"v2"[..], WriteOptions { sync: true })?;
    }

    // New handle should default to `main`, not latest global seqno.
    {
        let db = Db::open(dir.path(), options())?;
        assert_eq!(db.current_branch(), "main");
        assert_eq!(
            db.get(b"k", ReadOptions::default())?,
            Some(bytes::Bytes::from("v1"))
        );

        db.checkout("feature")?;
        assert_eq!(
            db.get(b"k", ReadOptions::default())?,
            Some(bytes::Bytes::from("v2"))
        );
    }

    Ok(())
}

#[test]
fn create_branch_without_snapshot_uses_handle_default_snapshot() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
    let snap_v1 = db.create_snapshot()?;
    db.create_branch("from_v1", Some(snap_v1))?;

    db.put(&b"k"[..], &b"v2"[..], WriteOptions { sync: true })?;

    // Move this handle's default snapshot back to v1.
    db.checkout("from_v1")?;
    assert_eq!(
        db.get(b"k", ReadOptions::default())?,
        Some(bytes::Bytes::from("v1"))
    );

    // Branch with `None` should use this handle default (v1), not global latest (v2).
    db.create_branch("from_current", None)?;

    db.checkout("from_current")?;
    assert_eq!(
        db.get(b"k", ReadOptions::default())?,
        Some(bytes::Bytes::from("v1"))
    );

    Ok(())
}

#[test]
fn drop_branch_removes_and_persists() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
        let snap = db.create_snapshot()?;
        db.create_branch("feature", Some(snap))?;
        assert!(db.list_branches().iter().any(|(name, _)| name == "feature"));

        db.drop_branch("feature")?;
        assert!(db.list_branches().iter().all(|(name, _)| name != "feature"));
    }

    {
        let db = Db::open(dir.path(), options())?;
        assert!(db.list_branches().iter().all(|(name, _)| name != "feature"));
        let err = db
            .checkout("feature")
            .expect_err("deleted branch should not exist");
        assert!(format!("{err:#}").contains("unknown branch"));
    }

    Ok(())
}

#[test]
fn drop_current_branch_switches_handle_to_main() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
    let snap_v1 = db.create_snapshot()?;
    db.create_branch("feature", Some(snap_v1))?;
    db.checkout("feature")?;
    db.put(&b"k"[..], &b"v2"[..], WriteOptions { sync: true })?;

    assert_eq!(db.current_branch(), "feature");
    db.drop_branch("feature")?;

    assert_eq!(db.current_branch(), "main");
    assert_eq!(
        db.get(b"k", ReadOptions::default())?,
        Some(bytes::Bytes::from("v1"))
    );

    Ok(())
}

#[test]
fn drop_main_branch_is_rejected() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    let err = db
        .drop_branch("main")
        .expect_err("main branch deletion must fail");
    assert!(format!("{err:#}").contains("cannot delete main branch"));

    Ok(())
}

#[test]
fn compaction_preserves_branch_history_without_pinned_snapshot() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
    let snap_v1 = db.create_snapshot()?;
    db.create_branch("feature", Some(snap_v1))?;
    db.release_snapshot(snap_v1);

    db.delete(&b"k"[..], WriteOptions { sync: true })?;
    db.put(&b"k"[..], &b"v3"[..], WriteOptions { sync: true })?;

    db.compact_range(None)?;

    db.checkout("main")?;
    assert_eq!(
        db.get(b"k", ReadOptions::default())?,
        Some(bytes::Bytes::from("v3"))
    );

    db.checkout("feature")?;
    assert_eq!(
        db.get(b"k", ReadOptions::default())?,
        Some(bytes::Bytes::from("v1"))
    );

    Ok(())
}

#[test]
fn retention_floor_advances_after_drop_branch() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
    let snap_v1 = db.create_snapshot()?;
    db.create_branch("feature", Some(snap_v1))?;
    db.release_snapshot(snap_v1);

    db.put(&b"k"[..], &b"v2"[..], WriteOptions { sync: true })?;

    assert_eq!(db.retention_floor_seqno(), 1);

    db.drop_branch("feature")?;

    assert_eq!(db.retention_floor_seqno(), 2);

    Ok(())
}
