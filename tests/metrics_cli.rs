use std::process::Command;

use layerdb::{Db, DbOptions, WriteOptions};
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
fn metrics_cli_reports_core_sections() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
        let _ = db.get(b"a", layerdb::ReadOptions::default())?;
        db.compact_range(None)?;
    }

    let output = Command::new(layerdb_bin()?)
        .args(["metrics", "--db", dir.path().to_str().expect("utf8 path")])
        .output()?;

    assert!(
        output.status.success(),
        "metrics failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("metrics wal_last_durable_seqno="),
        "stdout={stdout}"
    );
    assert!(
        stdout.contains("metrics branch current="),
        "stdout={stdout}"
    );
    assert!(stdout.contains("metrics cache=reader"), "stdout={stdout}");
    assert!(stdout.contains("metrics level=0"), "stdout={stdout}");
    assert!(stdout.contains("metrics level=1"), "stdout={stdout}");
    assert!(stdout.contains("metrics compaction"), "stdout={stdout}");
    assert!(
        stdout.contains("metrics frozen_s3_files="),
        "stdout={stdout}"
    );

    Ok(())
}

#[test]
fn metrics_cli_reports_frozen_s3_counts() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;
        let moved = db.freeze_level_to_s3(1, None)?;
        assert!(moved >= 1);
    }

    let output = Command::new(layerdb_bin()?)
        .args(["metrics", "--db", dir.path().to_str().expect("utf8 path")])
        .output()?;

    assert!(
        output.status.success(),
        "metrics failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let frozen_line = stdout
        .lines()
        .find(|line| line.starts_with("metrics frozen_s3_files="))
        .ok_or_else(|| anyhow::anyhow!("missing frozen_s3_files line: {stdout}"))?;

    let count: usize = frozen_line
        .trim_start_matches("metrics frozen_s3_files=")
        .parse()?;
    assert!(count >= 1, "stdout={stdout}");

    Ok(())
}
