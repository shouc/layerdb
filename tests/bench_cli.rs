use std::process::Command;

use tempfile::TempDir;

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
fn bench_cli_smoke_workload_runs() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = dir.path().join("smoke");

    let output = Command::new(layerdb_bin()?)
        .args([
            "bench",
            "--db",
            db.to_str().expect("utf8 path"),
            "--keys",
            "200",
            "--workload",
            "smoke",
        ])
        .output()?;

    assert!(
        output.status.success(),
        "bench smoke failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("bench workload=smoke keys=200"),
        "stdout={stdout}"
    );
    assert!(stdout.contains("write elapsed="), "stdout={stdout}");

    Ok(())
}

#[test]
fn bench_cli_delete_heavy_workload_runs() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = dir.path().join("delete_heavy");

    let output = Command::new(layerdb_bin()?)
        .args([
            "bench",
            "--db",
            db.to_str().expect("utf8 path"),
            "--keys",
            "200",
            "--workload",
            "delete-heavy",
        ])
        .output()?;

    assert!(
        output.status.success(),
        "bench delete-heavy failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("bench workload=delete-heavy keys=200"),
        "stdout={stdout}"
    );
    assert!(stdout.contains("elapsed="), "stdout={stdout}");

    Ok(())
}
