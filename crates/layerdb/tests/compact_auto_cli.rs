use std::process::Command;
use std::time::{Duration, Instant};

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

fn l0_pressure_options() -> DbOptions {
    DbOptions {
        memtable_shards: 2,
        wal_segment_bytes: 4 * 1024,
        memtable_bytes: 128,
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
fn compact_auto_cli_compacts_when_l0_is_over_threshold() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let before_l0_files = {
        let db = Db::open(dir.path(), l0_pressure_options())?;
        for i in 0..512u32 {
            db.put(
                format!("k{i:03}"),
                format!("value-{i:03}-xxxxxxxxxxxxxxxx"),
                WriteOptions { sync: true },
            )?;
        }

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let before = db.metrics();
            let l0_files = before
                .version
                .levels
                .get(&0)
                .map(|m| m.file_count)
                .unwrap_or(0);

            if before.version.should_compact && before.version.compaction_candidate_level == Some(0)
            {
                break l0_files;
            }

            if Instant::now() >= deadline {
                panic!("expected L0 compaction pressure, metrics={before:?}");
            }

            std::thread::sleep(Duration::from_millis(20));
        }
    };

    let output = Command::new(layerdb_bin()?)
        .args([
            "compact-auto",
            "--db",
            dir.path().to_str().expect("utf8 path"),
        ])
        .output()?;

    assert!(
        output.status.success(),
        "compact-auto failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("compact_auto compacted=true"),
        "stdout={stdout}"
    );

    let db = Db::open(dir.path(), options())?;
    let after = db.metrics();
    let after_l0 = after
        .version
        .levels
        .get(&0)
        .map(|m| m.file_count)
        .unwrap_or(0);
    assert!(
        after_l0 < before_l0_files,
        "expected fewer L0 files after compact-auto: before={before_l0_files} after={after_l0}"
    );

    Ok(())
}

#[test]
fn compact_auto_cli_noops_when_already_balanced() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    {
        let db = Db::open(dir.path(), options())?;
        db.put(&b"a"[..], &b"1"[..], WriteOptions { sync: true })?;
        db.compact_range(None)?;
    }

    let output = Command::new(layerdb_bin()?)
        .args([
            "compact-auto",
            "--db",
            dir.path().to_str().expect("utf8 path"),
        ])
        .output()?;

    assert!(
        output.status.success(),
        "compact-auto failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("compact_auto compacted=false"),
        "stdout={stdout}"
    );

    Ok(())
}
