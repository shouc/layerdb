use layerdb::{Db, DbOptions, ReadOptions, WriteOptions};

fn options() -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 16 * 1024,
        memtable_bytes: 1024,
        fsync_writes: true,
        io_max_in_flight: 32,
        ..Default::default()
    }
}

#[test]
fn sst_io_executor_reads_work_after_reopen_and_compaction() -> anyhow::Result<()> {
    let dir = tempfile::TempDir::new()?;

    let mut opts = options();
    opts.sst_use_io_executor_reads = true;

    {
        let db = Db::open(dir.path(), opts.clone())?;
        for i in 0..128u32 {
            let key = format!("k{i:03}");
            let value = format!("v{i:03}");
            db.put(key.clone(), value.clone(), WriteOptions { sync: true })?;
        }
        db.compact_range(None)?;
    }

    let db = Db::open(dir.path(), opts)?;
    for i in 0..128u32 {
        let key = format!("k{i:03}");
        let expected = bytes::Bytes::from(format!("v{i:03}"));
        assert_eq!(db.get(key.as_bytes(), ReadOptions::default())?, Some(expected));
    }

    Ok(())
}

#[test]
fn sst_io_executor_writes_persist_flush_and_compaction_outputs() -> anyhow::Result<()> {
    let dir = tempfile::TempDir::new()?;

    let mut opts = options();
    opts.sst_use_io_executor_reads = true;
    opts.sst_use_io_executor_writes = true;

    {
        let db = Db::open(dir.path(), opts.clone())?;
        for i in 0..180u32 {
            let key = format!("w{i:03}");
            let value = format!("x{i:03}");
            db.put(key.clone(), value.clone(), WriteOptions { sync: true })?;
        }

        db.compact_range(None)?;
        let metrics = db.metrics();
        assert!(metrics.version.levels.get(&1).is_some_and(|m| m.file_count > 0));
    }

    let db = Db::open(dir.path(), opts)?;
    for i in 0..180u32 {
        let key = format!("w{i:03}");
        let expected = bytes::Bytes::from(format!("x{i:03}"));
        assert_eq!(db.get(key.as_bytes(), ReadOptions::default())?, Some(expected));
    }

    db.compact_range(None)?;
    let metrics = db.metrics();
    assert!(metrics.version.levels.get(&1).is_some_and(|m| m.file_count > 0));

    Ok(())
}
