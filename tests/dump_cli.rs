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

fn first_sst_path(root: &std::path::Path) -> anyhow::Result<std::path::PathBuf> {
    for dir in ["sst", "sst_hdd", "sst_s3"] {
        let dir_path = root.join(dir);
        if !dir_path.exists() {
            continue;
        }
        for entry in std::fs::read_dir(&dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("sst") {
                return Ok(path);
            }
        }
    }
    anyhow::bail!("no sst file found under {}", root.display())
}

#[test]
fn manifest_dump_cli_prints_records() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;
    }

    let output = Command::new(layerdb_bin()?)
        .args([
            "manifest-dump",
            "--db",
            dir.path().to_str().expect("utf8 path"),
        ])
        .output()?;

    assert!(
        output.status.success(),
        "manifest-dump failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("#0:"), "stdout={stdout}");
    assert!(stdout.contains("AddFile"), "stdout={stdout}");

    Ok(())
}

#[test]
fn sst_dump_cli_prints_properties_and_entries() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;
    }

    let sst = first_sst_path(dir.path())?;

    let output = Command::new(layerdb_bin()?)
        .args(["sst-dump", "--sst", sst.to_str().expect("utf8 path")])
        .output()?;

    assert!(
        output.status.success(),
        "sst-dump failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("props:"), "stdout={stdout}");
    assert!(stdout.contains("entries="), "stdout={stdout}");

    Ok(())
}
