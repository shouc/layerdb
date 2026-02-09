use std::process::Command;

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
fn freeze_and_thaw_cli_round_trip() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;
    }

    let freeze = Command::new(layerdb_bin()?)
        .args([
            "freeze-level",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--level",
            "1",
        ])
        .output()?;
    assert!(
        freeze.status.success(),
        "freeze-level failed: stdout={} stderr={}",
        String::from_utf8_lossy(&freeze.stdout),
        String::from_utf8_lossy(&freeze.stderr)
    );
    let freeze_stdout = String::from_utf8_lossy(&freeze.stdout);
    assert!(freeze_stdout.contains("freeze_level level=1 moved="));

    let thaw = Command::new(layerdb_bin()?)
        .args([
            "thaw-level",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--level",
            "1",
        ])
        .output()?;
    assert!(
        thaw.status.success(),
        "thaw-level failed: stdout={} stderr={}",
        String::from_utf8_lossy(&thaw.stdout),
        String::from_utf8_lossy(&thaw.stderr)
    );
    let thaw_stdout = String::from_utf8_lossy(&thaw.stdout);
    assert!(thaw_stdout.contains("thaw_level level=1 moved="));

    let db = Db::open(dir.path(), options())?;
    assert_eq!(
        db.get(b"a", ReadOptions::default())?,
        Some(bytes::Bytes::from("1"))
    );
    assert_eq!(
        db.get(b"b", ReadOptions::default())?,
        Some(bytes::Bytes::from("2"))
    );

    Ok(())
}

#[test]
fn gc_s3_cli_removes_orphan() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;
        let moved = db.freeze_level_to_s3(1, None)?;
        assert!(moved >= 1);
    }

    let orphan_path = dir.path().join("sst_s3").join("sst_deadbeef00000001.sst");
    std::fs::create_dir_all(orphan_path.parent().expect("parent"))?;
    std::fs::write(&orphan_path, b"orphan")?;
    assert!(orphan_path.exists());

    let gc = Command::new(layerdb_bin()?)
        .args(["gc-s3", "--db", dir.path().to_str().expect("utf8 path")])
        .output()?;
    assert!(
        gc.status.success(),
        "gc-s3 failed: stdout={} stderr={}",
        String::from_utf8_lossy(&gc.stdout),
        String::from_utf8_lossy(&gc.stderr)
    );
    let gc_stdout = String::from_utf8_lossy(&gc.stdout);
    assert!(gc_stdout.contains("gc_s3 removed="));
    assert!(!orphan_path.exists());

    Ok(())
}
