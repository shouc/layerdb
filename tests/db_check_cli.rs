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
fn db_check_passes_for_valid_db() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let db = Db::open(dir.path(), options())?;
    db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
    db.compact_range(None)?;
    drop(db);

    let output = Command::new(layerdb_bin()?)
        .args(["db-check", "--db", dir.path().to_str().expect("utf8 path")])
        .output()?;

    assert!(
        output.status.success(),
        "db-check failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("db_check ok"));

    Ok(())
}

#[test]
fn db_check_fails_on_corrupted_sst() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let db = Db::open(dir.path(), options())?;
    db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
    db.compact_range(None)?;
    drop(db);

    let sst_dir = dir.path().join("sst");
    let sst_path = std::fs::read_dir(&sst_dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sst"))
        .expect("at least one sst file");

    let mut bytes = std::fs::read(&sst_path)?;
    assert!(bytes.len() > 64, "sst too small to corrupt");
    let idx = bytes.len() - 32;
    bytes[idx] ^= 0xAA;
    std::fs::write(&sst_path, bytes)?;

    let output = Command::new(layerdb_bin()?)
        .args(["db-check", "--db", dir.path().to_str().expect("utf8 path")])
        .output()?;

    assert!(
        !output.status.success(),
        "db-check unexpectedly succeeded: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    Ok(())
}

#[test]
fn db_check_fails_when_archived_branch_file_is_missing() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let out_dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        let snap = db.create_snapshot()?;
        db.create_branch("feature", Some(snap))?;
        db.release_snapshot(snap);
    }

    let archive = Command::new(layerdb_bin()?)
        .args([
            "archive-branch",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--name",
            "feature",
            "--out",
            out_dir.path().to_str().expect("utf8 path"),
        ])
        .output()?;
    assert!(
        archive.status.success(),
        "archive failed: stdout={} stderr={}",
        String::from_utf8_lossy(&archive.stdout),
        String::from_utf8_lossy(&archive.stderr)
    );

    let archive_stdout = String::from_utf8_lossy(&archive.stdout);
    let archived_path = archive_stdout
        .split_whitespace()
        .find_map(|part| part.strip_prefix("out="))
        .ok_or_else(|| anyhow::anyhow!("missing out path in output: {archive_stdout}"))?;

    let ok_output = Command::new(layerdb_bin()?)
        .args(["db-check", "--db", dir.path().to_str().expect("utf8 path")])
        .output()?;
    assert!(
        ok_output.status.success(),
        "db-check failed before archive removal: stdout={} stderr={}",
        String::from_utf8_lossy(&ok_output.stdout),
        String::from_utf8_lossy(&ok_output.stderr)
    );

    std::fs::remove_file(archived_path)?;

    let fail_output = Command::new(layerdb_bin()?)
        .args(["db-check", "--db", dir.path().to_str().expect("utf8 path")])
        .output()?;

    assert!(
        !fail_output.status.success(),
        "db-check unexpectedly succeeded: stdout={} stderr={}",
        String::from_utf8_lossy(&fail_output.stdout),
        String::from_utf8_lossy(&fail_output.stderr)
    );

    let combined = format!(
        "{} {}",
        String::from_utf8_lossy(&fail_output.stdout),
        String::from_utf8_lossy(&fail_output.stderr)
    );
    assert!(
        combined.contains("missing referenced archive"),
        "unexpected error output: {combined}"
    );

    Ok(())
}
