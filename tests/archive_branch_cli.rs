use std::process::Command;

use bytes::Bytes;
use layerdb::sst::SstReader;
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
fn archive_branch_cli_exports_branch_snapshot_sst() -> anyhow::Result<()> {
    let db_dir = TempDir::new()?;
    let out_dir = TempDir::new()?;

    {
        let db = Db::open(db_dir.path(), options())?;
        db.put(&b"a"[..], &b"main-a"[..], WriteOptions { sync: true })?;
        db.put(&b"b"[..], &b"main-b"[..], WriteOptions { sync: true })?;

        let snap = db.create_snapshot()?;
        db.create_branch("feature", Some(snap))?;
        db.release_snapshot(snap);

        db.checkout("feature")?;
        db.put(&b"a"[..], &b"feature-a"[..], WriteOptions { sync: true })?;
        db.delete(&b"b"[..], WriteOptions { sync: true })?;
        db.put(&b"c"[..], &b"feature-c"[..], WriteOptions { sync: true })?;

        db.checkout("main")?;
        db.put(&b"d"[..], &b"main-d"[..], WriteOptions { sync: true })?;
    }

    let output = Command::new(layerdb_bin()?)
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
        output.status.success(),
        "archive-branch failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("archive_branch name=feature"),
        "stdout={stdout}"
    );
    assert!(stdout.contains("entries=2"), "stdout={stdout}");

    let archive_sst = std::fs::read_dir(out_dir.path())?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| path.extension().and_then(|e| e.to_str()) == Some("sst"))
        .ok_or_else(|| anyhow::anyhow!("no archive sst found in {}", out_dir.path().display()))?;

    let reader = SstReader::open(&archive_sst)?;
    let mut iter = reader.iter(u64::MAX)?;
    iter.seek_to_first();

    let mut items = Vec::new();
    while let Some(next) = iter.next() {
        let (key, _seq, _kind, value) = next?;
        items.push((key, value));
    }

    assert_eq!(
        items,
        vec![
            (Bytes::from("a"), Bytes::from("feature-a")),
            (Bytes::from("c"), Bytes::from("feature-c")),
        ]
    );

    Ok(())
}
