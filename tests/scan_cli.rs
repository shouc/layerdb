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
fn scan_cli_respects_range_tombstones() -> anyhow::Result<()> {
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

    let delete_range = Command::new(layerdb_bin()?)
        .args([
            "delete-range",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--start",
            "b",
            "--end",
            "d",
            "--sync",
        ])
        .output()?;
    assert!(delete_range.status.success());

    let scan = Command::new(layerdb_bin()?)
        .args(["scan", "--db", dir.path().to_str().expect("utf8 path")])
        .output()?;
    assert!(scan.status.success());
    let stdout = String::from_utf8_lossy(&scan.stdout);
    assert!(stdout.contains("a=1"), "stdout={stdout}");
    assert!(!stdout.contains("b=2"), "stdout={stdout}");
    assert!(!stdout.contains("c=3"), "stdout={stdout}");
    assert!(stdout.contains("d=4"), "stdout={stdout}");

    Ok(())
}

#[test]
fn scan_cli_honors_branch_target() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let put_main = Command::new(layerdb_bin()?)
        .args([
            "put",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
            "--value",
            "main",
            "--sync",
        ])
        .output()?;
    assert!(put_main.status.success());

    let create_branch = Command::new(layerdb_bin()?)
        .args([
            "create-branch",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--name",
            "feature",
        ])
        .output()?;
    assert!(create_branch.status.success());

    let put_feature = Command::new(layerdb_bin()?)
        .args([
            "put",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--branch",
            "feature",
            "--key",
            "k",
            "--value",
            "feature",
            "--sync",
        ])
        .output()?;
    assert!(put_feature.status.success());

    let scan_main = Command::new(layerdb_bin()?)
        .args(["scan", "--db", dir.path().to_str().expect("utf8 path")])
        .output()?;
    assert!(scan_main.status.success());
    assert!(String::from_utf8_lossy(&scan_main.stdout).contains("k=main"));

    let scan_feature = Command::new(layerdb_bin()?)
        .args([
            "scan",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--branch",
            "feature",
        ])
        .output()?;
    assert!(scan_feature.status.success());
    assert!(String::from_utf8_lossy(&scan_feature.stdout).contains("k=feature"));

    Ok(())
}
