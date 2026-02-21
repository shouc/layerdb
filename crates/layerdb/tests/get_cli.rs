use std::process::Command;

use layerdb::{Db, DbOptions, SnapshotId, WriteOptions};
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
fn get_cli_supports_branch_override() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
        let snap: SnapshotId = db.create_snapshot()?;
        db.create_branch("feature", Some(snap))?;
        db.release_snapshot(snap);

        db.checkout("feature")?;
        db.put(&b"k"[..], &b"v2"[..], WriteOptions { sync: true })?;
    }

    let main = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
        ])
        .output()?;
    assert!(
        main.status.success(),
        "get(main) failed: stdout={} stderr={}",
        String::from_utf8_lossy(&main.stdout),
        String::from_utf8_lossy(&main.stderr)
    );
    assert!(
        String::from_utf8_lossy(&main.stdout).contains("value=v1"),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&main.stdout),
        String::from_utf8_lossy(&main.stderr)
    );

    let feature = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
            "--branch",
            "feature",
        ])
        .output()?;
    assert!(
        feature.status.success(),
        "get(feature) failed: stdout={} stderr={}",
        String::from_utf8_lossy(&feature.stdout),
        String::from_utf8_lossy(&feature.stderr)
    );
    assert!(
        String::from_utf8_lossy(&feature.stdout).contains("value=v2"),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&feature.stdout),
        String::from_utf8_lossy(&feature.stderr)
    );

    Ok(())
}
