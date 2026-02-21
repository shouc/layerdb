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
fn scan_cli_rejects_invalid_bounds() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let out = Command::new(layerdb_bin()?)
        .args([
            "scan",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--start",
            "z",
            "--end",
            "a",
        ])
        .output()?;

    assert!(
        !out.status.success(),
        "invalid scan bounds unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("scan requires start < end"),
        "stderr missing invalid-bounds message: {stderr}"
    );

    Ok(())
}

#[test]
fn scan_cli_honors_half_open_bounds() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
        let put = Command::new(layerdb_bin()?)
            .args([
                "put",
                "--db",
                dir.path().to_str().expect("utf8 path"),
                "--key",
                key,
                "--value",
                value,
                "--sync",
            ])
            .output()?;
        assert!(put.status.success());
    }

    let scan = Command::new(layerdb_bin()?)
        .args([
            "scan",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--start",
            "b",
            "--end",
            "d",
        ])
        .output()?;

    assert!(scan.status.success());
    let stdout = String::from_utf8_lossy(&scan.stdout);
    assert!(!stdout.contains("a=1"), "stdout={stdout}");
    assert!(stdout.contains("b=2"), "stdout={stdout}");
    assert!(stdout.contains("c=3"), "stdout={stdout}");
    assert!(!stdout.contains("d=4"), "stdout={stdout}");

    Ok(())
}
