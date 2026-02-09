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

fn s3_sst_path(root: &std::path::Path, file_id: u64) -> std::path::PathBuf {
    root.join("sst_s3").join(format!("sst_{file_id:016x}.sst"))
}

fn cache_sst_path(root: &std::path::Path, file_id: u64) -> std::path::PathBuf {
    root.join("sst_cache")
        .join(format!("sst_{file_id:016x}.sst"))
}

fn nvme_sst_path(root: &std::path::Path, file_id: u64) -> std::path::PathBuf {
    root.join("sst").join(format!("sst_{file_id:016x}.sst"))
}

#[test]
fn freeze_level_moves_files_and_recovers() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
        db.put(&b"c"[..], &b"3"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;

        let moved = db.freeze_level_to_s3(1, None)?;
        assert!(moved >= 1, "expected at least one frozen file");
        assert!(!db.frozen_objects().is_empty());

        assert_eq!(
            db.get(b"a", ReadOptions::default())?,
            Some(bytes::Bytes::from("1"))
        );
        assert_eq!(
            db.get(b"b", ReadOptions::default())?,
            Some(bytes::Bytes::from("2"))
        );

        for frozen in db.frozen_objects() {
            assert!(
                s3_sst_path(dir.path(), frozen.file_id).exists(),
                "frozen file missing in sst_s3 for file_id={}",
                frozen.file_id
            );
        }
    }

    {
        let db = Db::open(dir.path(), options())?;
        let frozen = db.frozen_objects();
        assert!(!frozen.is_empty(), "frozen metadata should recover");
        for entry in &frozen {
            assert!(s3_sst_path(dir.path(), entry.file_id).exists());
        }

        assert_eq!(
            db.get(b"a", ReadOptions::default())?,
            Some(bytes::Bytes::from("1"))
        );
        assert_eq!(
            db.get(b"c", ReadOptions::default())?,
            Some(bytes::Bytes::from("3"))
        );
    }

    Ok(())
}

#[test]
fn compaction_cleans_up_frozen_inputs() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
    db.compact_range(None)?;

    let moved = db.freeze_level_to_s3(1, None)?;
    assert!(moved >= 1);

    let frozen_ids: Vec<u64> = db.frozen_objects().into_iter().map(|f| f.file_id).collect();
    assert!(!frozen_ids.is_empty());

    db.put(&b"k"[..], &b"v2"[..], WriteOptions { sync: true })?;
    db.compact_range(None)?;

    assert_eq!(
        db.get(b"k", ReadOptions::default())?,
        Some(bytes::Bytes::from("v2"))
    );

    let frozen_after = db.frozen_objects();
    for file_id in &frozen_ids {
        assert!(
            frozen_after.iter().all(|f| f.file_id != *file_id),
            "stale frozen metadata for file_id={file_id}"
        );
        assert!(
            !s3_sst_path(dir.path(), *file_id).exists(),
            "stale frozen file should be removed file_id={file_id}"
        );
    }

    Ok(())
}

#[test]
fn frozen_reads_materialize_local_cache() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;
        let moved = db.freeze_level_to_s3(1, None)?;
        assert!(moved >= 1);
    }

    let db = Db::open(dir.path(), options())?;
    let frozen = db.frozen_objects();
    assert!(!frozen.is_empty());

    for f in &frozen {
        assert!(!cache_sst_path(dir.path(), f.file_id).exists());
    }

    assert_eq!(
        db.get(b"a", ReadOptions::default())?,
        Some(bytes::Bytes::from("1"))
    );

    for f in &frozen {
        assert!(
            cache_sst_path(dir.path(), f.file_id).exists(),
            "expected cache materialization for file_id={}",
            f.file_id
        );
    }

    Ok(())
}

#[test]
fn thaw_level_moves_back_to_local_tier() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    db.compact_range(None)?;
    let moved = db.freeze_level_to_s3(1, None)?;
    assert!(moved >= 1);

    let frozen_ids: Vec<u64> = db.frozen_objects().into_iter().map(|f| f.file_id).collect();
    assert!(!frozen_ids.is_empty());

    let thawed = db.thaw_level_from_s3(1, None)?;
    assert!(thawed >= 1);
    assert!(db.frozen_objects().is_empty());

    for file_id in frozen_ids {
        assert!(!s3_sst_path(dir.path(), file_id).exists());
        assert!(nvme_sst_path(dir.path(), file_id).exists());
    }

    assert_eq!(
        db.get(b"a", ReadOptions::default())?,
        Some(bytes::Bytes::from("1"))
    );

    Ok(())
}

#[test]
fn gc_orphaned_s3_removes_unreferenced_files() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    db.compact_range(None)?;
    db.freeze_level_to_s3(1, None)?;

    let orphan_id = 0xDEADBEEFu64;
    let orphan_path = s3_sst_path(dir.path(), orphan_id);
    std::fs::create_dir_all(orphan_path.parent().expect("parent"))?;
    std::fs::write(&orphan_path, b"orphan")?;
    assert!(orphan_path.exists());

    let removed = db.gc_orphaned_s3_files()?;
    assert!(removed >= 1, "expected orphan cleanup to remove files");
    assert!(!orphan_path.exists());

    Ok(())
}
