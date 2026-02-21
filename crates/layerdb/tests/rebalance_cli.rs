use std::process::Command;

use layerdb::{Db, DbOptions, ReadOptions, WriteOptions};
use tempfile::TempDir;

fn options_nvme_only() -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 32 * 1024,
        memtable_bytes: 4 * 1024,
        fsync_writes: true,
        enable_hdd_tier: false,
        hot_levels_max: 2,
        ..Default::default()
    }
}

fn options_with_hdd() -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 32 * 1024,
        memtable_bytes: 4 * 1024,
        fsync_writes: true,
        enable_hdd_tier: true,
        hot_levels_max: 0,
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

fn has_sst_files(dir: &std::path::Path) -> anyhow::Result<bool> {
    if !dir.exists() {
        return Ok(false);
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("sst") {
            return Ok(true);
        }
    }
    Ok(false)
}

#[test]
fn rebalance_tiers_cli_moves_files_and_preserves_reads() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options_nvme_only())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.put(&b"b"[..], &b"2"[..], WriteOptions { sync: true })?;
        db.put(&b"c"[..], &b"3"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;
    }

    let output = Command::new(layerdb_bin()?)
        .args([
            "rebalance-tiers",
            "--db",
            dir.path().to_str().expect("utf8 path"),
        ])
        .output()?;

    assert!(
        output.status.success(),
        "rebalance-tiers failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("rebalance_tiers moved="));
    assert!(has_sst_files(&dir.path().join("sst_hdd"))?);

    let db = Db::open(dir.path(), options_with_hdd())?;
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
