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

fn s3_object_dir(root: &std::path::Path, level: u8, object_id: &str) -> std::path::PathBuf {
    root.join("sst_s3")
        .join(format!("L{level}"))
        .join(object_id)
}

fn s3_meta_path(root: &std::path::Path, level: u8, object_id: &str) -> std::path::PathBuf {
    s3_object_dir(root, level, object_id).join("meta.bin")
}

fn legacy_s3_sst_path(root: &std::path::Path, file_id: u64) -> std::path::PathBuf {
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
                s3_meta_path(dir.path(), frozen.level, &frozen.object_id).exists(),
                "frozen meta missing in sst_s3 for file_id={} object_id={}",
                frozen.file_id,
                frozen.object_id
            );
        }
    }

    {
        let db = Db::open(dir.path(), options())?;
        let frozen = db.frozen_objects();
        assert!(!frozen.is_empty(), "frozen metadata should recover");
        for entry in &frozen {
            assert!(s3_meta_path(dir.path(), entry.level, &entry.object_id).exists());
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
            !legacy_s3_sst_path(dir.path(), *file_id).exists(),
            "stale legacy frozen file should be removed file_id={file_id}"
        );
        // Superblock-based object dirs should be removed as well.
        assert!(
            !dir.path()
                .join("sst_s3")
                .join("L1")
                .join(format!("L1-{file_id:016x}"))
                .exists(),
            "stale frozen object dir should be removed file_id={file_id}"
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
fn frozen_read_detects_corrupted_superblock() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;
        let moved = db.freeze_level_to_s3(1, None)?;
        assert!(moved >= 1);
    }

    let frozen_db = Db::open(dir.path(), options())?;
    let frozen = frozen_db.frozen_objects();
    assert!(!frozen.is_empty());
    let first = &frozen[0];

    let superblock_path = dir
        .path()
        .join("sst_s3")
        .join(format!("L{}", first.level))
        .join(&first.object_id)
        .join("sb_00000000.bin");
    let mut bytes = std::fs::read(&superblock_path)?;
    assert!(!bytes.is_empty(), "superblock should not be empty");
    bytes[0] ^= 0x01;
    std::fs::write(&superblock_path, bytes)?;

    let err = frozen_db
        .get(b"a", ReadOptions::default())
        .expect_err("corrupted superblock must fail read");
    let message = format!("{err:#}");
    assert!(
        message.contains("superblock hash mismatch"),
        "unexpected error: {message}"
    );

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
        assert!(!legacy_s3_sst_path(dir.path(), file_id).exists());
        assert!(
            !dir.path()
                .join("sst_s3")
                .join("L1")
                .join(format!("L1-{file_id:016x}"))
                .exists(),
            "frozen object dir should be removed after thaw file_id={file_id}"
        );
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
    let orphan_path = legacy_s3_sst_path(dir.path(), orphan_id);
    std::fs::create_dir_all(orphan_path.parent().expect("parent"))?;
    std::fs::write(&orphan_path, b"orphan")?;
    assert!(orphan_path.exists());

    let orphan_object = dir
        .path()
        .join("sst_s3")
        .join("L1")
        .join("L1-deadbeef00000001");
    std::fs::create_dir_all(&orphan_object)?;
    std::fs::write(orphan_object.join("meta.bin"), b"orphan-meta")?;
    assert!(orphan_object.exists());

    let removed = db.gc_orphaned_s3_files()?;
    assert!(removed >= 1, "expected orphan cleanup to remove files");
    assert!(!orphan_path.exists());
    assert!(!orphan_object.exists());

    Ok(())
}
