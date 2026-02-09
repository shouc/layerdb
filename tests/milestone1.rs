use std::ops::Bound;

use layerdb::{Db, DbOptions, Op, Range, ReadOptions, WriteOptions};
use tempfile::TempDir;

fn small_options() -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 32 * 1024,
        memtable_bytes: 4 * 1024,
        fsync_writes: true,
        ..Default::default()
    }
}

#[test]
fn recover_from_wal_and_manifest() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    {
        let db = Db::open(dir.path(), small_options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
        db.delete(&b"a"[..], WriteOptions { sync: true })?;
    }

    {
        let db = Db::open(dir.path(), small_options())?;
        assert_eq!(db.get(b"a", ReadOptions::default())?, None);
        assert_eq!(
            db.get(b"b", ReadOptions::default())?,
            Some(bytes::Bytes::from("2"))
        );
    }
    Ok(())
}

#[test]
fn snapshot_and_compaction_preserve_visibility() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), small_options())?;

    db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
    let snap1 = db.create_snapshot()?;
    db.put(&b"k"[..], &b"v2"[..], WriteOptions { sync: true })?;
    db.delete(&b"k"[..], WriteOptions { sync: true })?;

    assert_eq!(
        db.get(
            b"k",
            ReadOptions {
                snapshot: Some(snap1),
            },
        )?,
        Some(bytes::Bytes::from("v1"))
    );
    assert_eq!(db.get(b"k", ReadOptions::default())?, None);

    db.compact_range(None)?;

    assert_eq!(
        db.get(
            b"k",
            ReadOptions {
                snapshot: Some(snap1),
            },
        )?,
        Some(bytes::Bytes::from("v1"))
    );
    assert_eq!(db.get(b"k", ReadOptions::default())?, None);
    Ok(())
}

#[test]
fn iter_returns_latest_visible_per_key() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), small_options())?;

    db.write_batch(
        vec![
            Op::put(&b"a"[..], &b"1"[..]),
            Op::put(&b"b"[..], &b"2"[..]),
            Op::put(&b"a"[..], &b"3"[..]),
            Op::delete(&b"b"[..]),
            Op::put(&b"c"[..], &b"4"[..]),
        ],
        WriteOptions { sync: true },
    )?;

    let mut iter = db.iter(
        Range {
            start: Bound::Included(bytes::Bytes::from("a")),
            end: Bound::Included(bytes::Bytes::from("z")),
        },
        ReadOptions::default(),
    )?;
    iter.seek_to_first();

    let mut out = Vec::new();
    while let Some(next) = iter.next() {
        let (k, v) = next?;
        out.push((k, v));
    }

    assert_eq!(
        out,
        vec![
            (bytes::Bytes::from("a"), Some(bytes::Bytes::from("3"))),
            (bytes::Bytes::from("c"), Some(bytes::Bytes::from("4"))),
        ]
    );
    Ok(())
}

#[test]
fn range_tombstones_hide_keys_across_snapshots() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), small_options())?;

    db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
    db.put(&b"c"[..], &b"3"[..], WriteOptions { sync: true })?;

    let before = db.create_snapshot()?;

    db.delete_range(&b"b"[..], &b"d"[..], WriteOptions { sync: true })?;

    assert_eq!(
        db.get(
            b"b",
            ReadOptions {
                snapshot: Some(before),
            },
        )?,
        Some(bytes::Bytes::from("2"))
    );
    assert_eq!(db.get(b"b", ReadOptions::default())?, None);
    assert_eq!(db.get(b"c", ReadOptions::default())?, None);
    assert_eq!(
        db.get(b"a", ReadOptions::default())?,
        Some(bytes::Bytes::from("1"))
    );

    let mut iter = db.iter(Range::all(), ReadOptions::default())?;
    iter.seek_to_first();
    let mut keys = Vec::new();
    while let Some(next) = iter.next() {
        let (k, v) = next?;
        if v.is_some() {
            keys.push(k);
        }
    }
    assert_eq!(keys, vec![bytes::Bytes::from("a")]);

    db.compact_range(None)?;

    assert_eq!(db.get(b"b", ReadOptions::default())?, None);
    assert_eq!(db.get(b"c", ReadOptions::default())?, None);
    assert_eq!(
        db.get(
            b"b",
            ReadOptions {
                snapshot: Some(before),
            },
        )?,
        Some(bytes::Bytes::from("2"))
    );

    Ok(())
}
