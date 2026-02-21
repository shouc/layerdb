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
fn archive_list_drop_and_gc_cli_work() -> anyhow::Result<()> {
    let db_dir = TempDir::new()?;
    let out_dir = TempDir::new()?;

    {
        let db = Db::open(db_dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        let snap = db.create_snapshot()?;
        db.create_branch("feature", Some(snap))?;
        db.release_snapshot(snap);
    }

    let archive = Command::new(layerdb_bin()?)
        .args([
            "archive-branch",
            "--db",
            db_dir.path().to_str().expect("utf8 path"),
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
    let archive_id = archive_stdout
        .split_whitespace()
        .find_map(|part| part.strip_prefix("archive_id="))
        .ok_or_else(|| anyhow::anyhow!("missing archive_id in output: {archive_stdout}"))?
        .to_string();

    let list = Command::new(layerdb_bin()?)
        .args([
            "list-archives",
            "--db",
            db_dir.path().to_str().expect("utf8 path"),
        ])
        .output()?;
    assert!(
        list.status.success(),
        "list-archives failed: stdout={} stderr={}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    let list_stdout = String::from_utf8_lossy(&list.stdout);
    assert!(
        list_stdout.contains(&format!("id={archive_id}")),
        "stdout={list_stdout}"
    );

    let drop = Command::new(layerdb_bin()?)
        .args([
            "drop-archive",
            "--db",
            db_dir.path().to_str().expect("utf8 path"),
            "--archive-id",
            &archive_id,
        ])
        .output()?;
    assert!(
        drop.status.success(),
        "drop-archive failed: stdout={} stderr={}",
        String::from_utf8_lossy(&drop.stdout),
        String::from_utf8_lossy(&drop.stderr)
    );

    let list_after_drop = Command::new(layerdb_bin()?)
        .args([
            "list-archives",
            "--db",
            db_dir.path().to_str().expect("utf8 path"),
        ])
        .output()?;
    assert!(list_after_drop.status.success());
    let list_after_drop_stdout = String::from_utf8_lossy(&list_after_drop.stdout);
    assert!(
        !list_after_drop_stdout.contains(&format!("id={archive_id}")),
        "stdout={list_after_drop_stdout}"
    );

    let archive2 = Command::new(layerdb_bin()?)
        .args([
            "archive-branch",
            "--db",
            db_dir.path().to_str().expect("utf8 path"),
            "--name",
            "feature",
            "--out",
            out_dir.path().to_str().expect("utf8 path"),
        ])
        .output()?;
    assert!(archive2.status.success());
    let archive2_stdout = String::from_utf8_lossy(&archive2.stdout);
    let archive2_id = archive2_stdout
        .split_whitespace()
        .find_map(|part| part.strip_prefix("archive_id="))
        .ok_or_else(|| anyhow::anyhow!("missing archive_id in output: {archive2_stdout}"))?
        .to_string();

    let archived_path = archive2_stdout
        .split_whitespace()
        .find_map(|part| part.strip_prefix("out="))
        .ok_or_else(|| anyhow::anyhow!("missing out path in output: {archive2_stdout}"))?;

    std::fs::remove_file(archived_path)?;

    let gc = Command::new(layerdb_bin()?)
        .args([
            "gc-archives",
            "--db",
            db_dir.path().to_str().expect("utf8 path"),
        ])
        .output()?;
    assert!(
        gc.status.success(),
        "gc-archives failed: stdout={} stderr={}",
        String::from_utf8_lossy(&gc.stdout),
        String::from_utf8_lossy(&gc.stderr)
    );
    let gc_stdout = String::from_utf8_lossy(&gc.stdout);
    assert!(
        gc_stdout.contains("gc_archives removed=1"),
        "stdout={gc_stdout}"
    );

    let list_after_gc = Command::new(layerdb_bin()?)
        .args([
            "list-archives",
            "--db",
            db_dir.path().to_str().expect("utf8 path"),
        ])
        .output()?;
    assert!(list_after_gc.status.success());
    let list_after_gc_stdout = String::from_utf8_lossy(&list_after_gc.stdout);
    assert!(
        !list_after_gc_stdout.contains(&format!("id={archive2_id}")),
        "stdout={list_after_gc_stdout}"
    );

    Ok(())
}
