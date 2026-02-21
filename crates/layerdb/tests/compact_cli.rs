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
fn compact_range_cli_accepts_full_and_bounded_ranges() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
        db.put(&b"c"[..], &b"3"[..], WriteOptions { sync: true })?;
    }

    let full = Command::new(layerdb_bin()?)
        .args([
            "compact-range",
            "--db",
            dir.path().to_str().expect("utf8 path"),
        ])
        .output()?;
    assert!(
        full.status.success(),
        "compact-range full failed: stdout={} stderr={}",
        String::from_utf8_lossy(&full.stdout),
        String::from_utf8_lossy(&full.stderr)
    );
    assert!(String::from_utf8_lossy(&full.stdout).contains("compact_range start=- end=-"));

    let bounded = Command::new(layerdb_bin()?)
        .args([
            "compact-range",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--start",
            "b",
            "--end",
            "d",
        ])
        .output()?;
    assert!(
        bounded.status.success(),
        "compact-range bounded failed: stdout={} stderr={}",
        String::from_utf8_lossy(&bounded.stdout),
        String::from_utf8_lossy(&bounded.stderr)
    );
    assert!(String::from_utf8_lossy(&bounded.stdout).contains("compact_range start=b end=d"));

    let db = Db::open(dir.path(), options())?;
    assert_eq!(
        db.get(b"a", ReadOptions::default())?,
        Some(bytes::Bytes::from("1"))
    );
    assert_eq!(
        db.get(b"b", ReadOptions::default())?,
        Some(bytes::Bytes::from("2"))
    );
    assert_eq!(
        db.get(b"c", ReadOptions::default())?,
        Some(bytes::Bytes::from("3"))
    );

    Ok(())
}

#[test]
fn compact_range_cli_rejects_invalid_bounds() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    }

    let output = Command::new(layerdb_bin()?)
        .args([
            "compact-range",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--start",
            "z",
            "--end",
            "a",
        ])
        .output()?;

    assert!(
        !output.status.success(),
        "compact-range unexpectedly succeeded: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("invalid range: start must be < end"),
        "stderr={stderr}"
    );

    Ok(())
}
