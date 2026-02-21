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
fn retention_floor_cli_reflects_branch_pinned_history() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"k"[..], &b"v1"[..], WriteOptions { sync: true })?;
        let snap: SnapshotId = db.create_snapshot()?;
        db.create_branch("feature", Some(snap))?;
        db.release_snapshot(snap);

        db.put(&b"k"[..], &b"v2"[..], WriteOptions { sync: true })?;
        db.put(&b"k"[..], &b"v3"[..], WriteOptions { sync: true })?;
    }

    let output = Command::new(layerdb_bin()?)
        .args([
            "retention-floor",
            "--db",
            dir.path().to_str().expect("utf8 path"),
        ])
        .output()?;

    assert!(
        output.status.success(),
        "retention-floor failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("retention_floor seqno=1"),
        "stdout={stdout}"
    );

    Ok(())
}
