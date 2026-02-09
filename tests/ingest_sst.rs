use bytes::Bytes;
use layerdb::internal_key::{InternalKey, KeyKind};
use layerdb::sst::SstBuilder;
use layerdb::{Db, DbOptions, ReadOptions};
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

fn build_external_sst(path: &std::path::Path) -> anyhow::Result<()> {
    let mut builder = SstBuilder::create(path, 42, 4 * 1024)?;
    builder.add(
        &InternalKey::new(Bytes::from("a"), 20, KeyKind::Put),
        b"new-a",
    )?;
    builder.add(
        &InternalKey::new(Bytes::from("a"), 10, KeyKind::Put),
        b"old-a",
    )?;
    builder.add(
        &InternalKey::new(Bytes::from("b"), 30, KeyKind::Put),
        b"new-b",
    )?;
    let _props = builder.finish()?;
    Ok(())
}

fn build_external_high_seq_sst(path: &std::path::Path) -> anyhow::Result<()> {
    let mut builder = SstBuilder::create(path, 99, 4 * 1024)?;
    builder.add(
        &InternalKey::new(Bytes::from("k"), 100, KeyKind::Put),
        b"old-high",
    )?;
    let _props = builder.finish()?;
    Ok(())
}

fn build_empty_external_sst(path: &std::path::Path) -> anyhow::Result<()> {
    let builder = SstBuilder::create(path, 77, 4 * 1024)?;
    let _props = builder.finish()?;
    Ok(())
}

#[test]
fn ingest_sst_makes_data_visible_and_persists_manifest() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let ext_dir = TempDir::new()?;

    let source_path = ext_dir.path().join("sst_000000000000002a.sst");
    build_external_sst(ext_dir.path())?;
    assert!(source_path.exists());

    {
        let db = Db::open(dir.path(), options())?;
        db.ingest_sst(&source_path)?;

        assert_eq!(
            db.get(b"a", ReadOptions::default())?,
            Some(Bytes::from("new-a"))
        );
        assert_eq!(
            db.get(b"b", ReadOptions::default())?,
            Some(Bytes::from("new-b"))
        );
    }

    // Reopen should replay manifest and still see ingested file.
    {
        let db = Db::open(dir.path(), options())?;
        assert_eq!(
            db.get(b"a", ReadOptions::default())?,
            Some(Bytes::from("new-a"))
        );
        assert_eq!(
            db.get(b"b", ReadOptions::default())?,
            Some(Bytes::from("new-b"))
        );
    }

    Ok(())
}

#[test]
fn ingest_sst_assigns_new_file_id_and_keeps_source() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let ext_dir = TempDir::new()?;

    let source_path = ext_dir.path().join("sst_000000000000002a.sst");
    build_external_sst(ext_dir.path())?;

    let db = Db::open(dir.path(), options())?;
    db.ingest_sst(&source_path)?;

    let manifest = std::fs::read(dir.path().join("MANIFEST"))?;
    let mut offset = 0usize;
    let mut file_ids = Vec::new();
    while offset + 4 <= manifest.len() {
        let len = u32::from_le_bytes(manifest[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + len > manifest.len() {
            break;
        }

        let rec: layerdb::version::manifest::ManifestRecord =
            bincode::deserialize(&manifest[offset..offset + len])?;
        if let layerdb::version::manifest::ManifestRecord::AddFile(add) = rec {
            file_ids.push(add.file_id);
        }
        offset += len;
    }

    assert_eq!(file_ids.len(), 1);
    assert_ne!(file_ids[0], 42, "ingest should allocate local file id");

    let ingested_path = dir
        .path()
        .join("sst")
        .join(format!("sst_{:016x}.sst", file_ids[0]));
    assert!(ingested_path.exists());
    assert!(source_path.exists(), "source sst should be preserved");

    Ok(())
}

#[test]
fn writes_after_ingest_get_higher_seqnos() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let ext_dir = TempDir::new()?;

    let source_path = ext_dir.path().join("sst_0000000000000063.sst");
    build_external_high_seq_sst(ext_dir.path())?;

    let db = Db::open(dir.path(), options())?;
    db.ingest_sst(&source_path)?;

    db.put(&b"k"[..], &b"new"[..], layerdb::WriteOptions { sync: true })?;

    assert_eq!(
        db.get(b"k", ReadOptions::default())?,
        Some(Bytes::from("new"))
    );

    Ok(())
}

#[test]
fn ingest_rejects_empty_sst() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let ext_dir = TempDir::new()?;

    let source_path = ext_dir.path().join("sst_000000000000004d.sst");
    build_empty_external_sst(ext_dir.path())?;

    let db = Db::open(dir.path(), options())?;
    let err = db.ingest_sst(&source_path).expect_err("empty ingest should fail");
    let msg = format!("{err:#}");
    assert!(msg.contains("empty sst"), "unexpected error: {msg}");

    Ok(())
}
