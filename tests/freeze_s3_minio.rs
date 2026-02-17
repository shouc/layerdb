use layerdb::s3::S3ObjectStore;
use layerdb::{Db, DbOptions, ReadOptions, WriteOptions};

fn minio_enabled() -> bool {
    matches!(
        std::env::var("LAYERDB_MINIO_INTEGRATION")
            .ok()
            .as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

#[test]
fn freeze_round_trip_against_minio() -> anyhow::Result<()> {
    if !minio_enabled() {
        eprintln!("skipping minio integration test (set LAYERDB_MINIO_INTEGRATION=1)");
        return Ok(());
    }

    let options = DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 32 * 1024,
        memtable_bytes: 4 * 1024,
        fsync_writes: true,
        ..Default::default()
    };
    let s3 = options
        .s3
        .clone()
        .ok_or_else(|| anyhow::anyhow!("minio integration requires LAYERDB_S3_* env vars"))?;

    let tmp = tempfile::TempDir::new()?;
    let db = Db::open(tmp.path(), options)?;

    db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
    db.compact_range(None)?;

    let moved = db.freeze_level_to_s3(1, None)?;
    assert!(moved >= 1);
    let frozen = db.frozen_objects();
    assert!(!frozen.is_empty());

    let store = S3ObjectStore::for_options(Some(s3), tmp.path().join("unused"))?;
    for f in &frozen {
        let key = format!("L{}/{}/meta.bin", f.level, f.object_id);
        assert!(
            store.exists(&key)?,
            "missing uploaded meta key for file_id={}",
            f.file_id
        );
    }

    assert_eq!(
        db.get(b"a", ReadOptions::default())?,
        Some(bytes::Bytes::from("1"))
    );

    let orphan_meta = "L1/L1-deadbeef00000001/meta.bin";
    let orphan_sb = "L1/L1-deadbeef00000001/sb_00000000.bin";
    store.put(orphan_meta, b"orphan-meta")?;
    store.put(orphan_sb, b"orphan-sb")?;
    assert!(store.exists(orphan_meta)?);

    let removed = db.gc_orphaned_s3_files()?;
    assert!(removed >= 1);
    assert!(!store.exists(orphan_meta)?);
    assert!(!store.exists(orphan_sb)?);

    Ok(())
}
