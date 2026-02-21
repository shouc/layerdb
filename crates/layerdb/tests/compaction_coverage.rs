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

fn sst_count(dir: &std::path::Path, subdir: &str) -> anyhow::Result<usize> {
    let p = dir.join(subdir);
    if !p.exists() {
        return Ok(0);
    }
    Ok(std::fs::read_dir(p)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("sst"))
        .count())
}

#[test]
fn compaction_retains_tombstone_when_lower_levels_not_covered() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put("k", "v1", WriteOptions { sync: true })?;
    db.compact_range(None)?;

    db.delete("k", WriteOptions { sync: true })?;
    db.compact_range(None)?;

    // Key must stay deleted after compaction even though older data existed in L1.
    assert_eq!(db.get("k", ReadOptions::default())?, None);

    // We should still have at least one SST holding visibility state.
    let total = sst_count(dir.path(), "sst")? + sst_count(dir.path(), "sst_hdd")?;
    assert!(
        total >= 1,
        "expected retained sst files for tombstone safety"
    );

    Ok(())
}

#[test]
fn compact_range_subset_does_not_drop_out_of_range_visibility() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.put("a", "1", WriteOptions { sync: true })?;
    db.put("m", "1", WriteOptions { sync: true })?;
    db.put("z", "1", WriteOptions { sync: true })?;
    db.compact_range(None)?;

    db.delete("m", WriteOptions { sync: true })?;
    db.compact_range(Some(layerdb::Range {
        start: std::ops::Bound::Included(bytes::Bytes::from_static(b"a")),
        end: std::ops::Bound::Excluded(bytes::Bytes::from_static(b"n")),
    }))?;

    assert_eq!(
        db.get("a", ReadOptions::default())?,
        Some(bytes::Bytes::from("1"))
    );
    assert_eq!(db.get("m", ReadOptions::default())?, None);
    assert_eq!(
        db.get("z", ReadOptions::default())?,
        Some(bytes::Bytes::from("1"))
    );

    Ok(())
}

#[test]
fn compaction_preserves_snapshot_for_non_overwritten_key() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), options())?;

    db.write_batch(
        vec![layerdb::Op::put("e", "0"), layerdb::Op::put("a", "0")],
        WriteOptions { sync: true },
    )?;

    let snap = db.create_snapshot()?;

    db.put("e", "0", WriteOptions { sync: true })?;
    db.compact_range(None)?;

    assert_eq!(
        db.get(
            "a",
            ReadOptions {
                snapshot: Some(snap)
            }
        )?,
        Some(bytes::Bytes::from("0"))
    );

    db.release_snapshot(snap);
    Ok(())
}
