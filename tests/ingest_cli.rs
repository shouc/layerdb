use std::process::Command;

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

fn build_external_sst(path: &std::path::Path) -> anyhow::Result<std::path::PathBuf> {
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
    Ok(path.join("sst_000000000000002a.sst"))
}

fn layerdb_bin() -> anyhow::Result<std::path::PathBuf> {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_layerdb") {
        return Ok(path.into());
    }

    let exe = std::env::current_exe()?;
    let deps_dir = exe
        .parent()
        .ok_or_else(|| anyhow::anyhow!("test binary has no parent"))?;
    let target_dir = deps_dir
        .parent()
        .ok_or_else(|| anyhow::anyhow!("deps dir has no parent"))?;
    let candidate = target_dir.join(if cfg!(windows) {
        "layerdb.exe"
    } else {
        "layerdb"
    });
    if candidate.exists() {
        return Ok(candidate);
    }

    anyhow::bail!(
        "layerdb binary not found (checked CARGO_BIN_EXE_layerdb and {})",
        candidate.display()
    )
}

#[test]
fn ingest_sst_cli_installs_external_table() -> anyhow::Result<()> {
    let db_dir = TempDir::new()?;
    let ext_dir = TempDir::new()?;
    let source = build_external_sst(ext_dir.path())?;

    let output = Command::new(layerdb_bin()?)
        .args([
            "ingest-sst",
            "--db",
            db_dir.path().to_str().expect("utf8 path"),
            "--sst",
            source.to_str().expect("utf8 path"),
        ])
        .output()?;

    assert!(
        output.status.success(),
        "ingest-sst failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout)
        .contains(&format!("ingest_sst source={}", source.display())));

    let db = Db::open(db_dir.path(), options())?;
    assert_eq!(
        db.get(b"a", ReadOptions::default())?,
        Some(Bytes::from("new-a"))
    );
    assert_eq!(
        db.get(b"b", ReadOptions::default())?,
        Some(Bytes::from("new-b"))
    );

    Ok(())
}
