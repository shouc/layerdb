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
fn delete_range_cli_rejects_invalid_bounds() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let output = Command::new(layerdb_bin()?)
        .args([
            "delete-range",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--start",
            "z",
            "--end",
            "a",
            "--sync",
        ])
        .output()?;

    assert!(!output.status.success(), "invalid bounds unexpectedly succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("delete-range requires start < end"),
        "stderr missing invalid-bounds message: {stderr}"
    );

    Ok(())
}

#[test]
fn delete_range_cli_tombstones_half_open_span() -> anyhow::Result<()> {
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

    let del_range = Command::new(layerdb_bin()?)
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
    assert!(
        del_range.status.success(),
        "delete-range failed: stdout={} stderr={}",
        String::from_utf8_lossy(&del_range.stdout),
        String::from_utf8_lossy(&del_range.stderr)
    );

    let get_a = Command::new(layerdb_bin()?)
        .args(["get", "--db", dir.path().to_str().expect("utf8 path"), "--key", "a"])
        .output()?;
    assert!(get_a.status.success());
    assert!(String::from_utf8_lossy(&get_a.stdout).contains("value=1"));

    let get_b = Command::new(layerdb_bin()?)
        .args(["get", "--db", dir.path().to_str().expect("utf8 path"), "--key", "b"])
        .output()?;
    assert!(get_b.status.success());
    assert!(String::from_utf8_lossy(&get_b.stdout).contains("not_found"));

    let get_c = Command::new(layerdb_bin()?)
        .args(["get", "--db", dir.path().to_str().expect("utf8 path"), "--key", "c"])
        .output()?;
    assert!(get_c.status.success());
    assert!(String::from_utf8_lossy(&get_c.stdout).contains("not_found"));

    let get_d = Command::new(layerdb_bin()?)
        .args(["get", "--db", dir.path().to_str().expect("utf8 path"), "--key", "d"])
        .output()?;
    assert!(get_d.status.success());
    assert!(String::from_utf8_lossy(&get_d.stdout).contains("value=4"));

    let check = Command::new(layerdb_bin()?)
        .args(["db-check", "--db", dir.path().to_str().expect("utf8 path")])
        .output()?;
    assert!(
        check.status.success(),
        "db-check failed: stdout={} stderr={}",
        String::from_utf8_lossy(&check.stdout),
        String::from_utf8_lossy(&check.stderr)
    );

    Ok(())
}


#[test]
fn delete_range_cli_honors_branch_target() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    for (key, value) in [("a", "1"), ("b", "2")] {
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

    let del_feature = Command::new(layerdb_bin()?)
        .args([
            "delete-range",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--branch",
            "feature",
            "--start",
            "a",
            "--end",
            "z",
            "--sync",
        ])
        .output()?;
    assert!(
        del_feature.status.success(),
        "delete-range(feature) failed: stdout={} stderr={}",
        String::from_utf8_lossy(&del_feature.stdout),
        String::from_utf8_lossy(&del_feature.stderr)
    );

    let get_main_a = Command::new(layerdb_bin()?)
        .args(["get", "--db", dir.path().to_str().expect("utf8 path"), "--key", "a"])
        .output()?;
    assert!(get_main_a.status.success());
    assert!(String::from_utf8_lossy(&get_main_a.stdout).contains("value=1"));

    let get_feature_a = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "a",
            "--branch",
            "feature",
        ])
        .output()?;
    assert!(get_feature_a.status.success());
    assert!(String::from_utf8_lossy(&get_feature_a.stdout).contains("not_found"));

    let get_main_b = Command::new(layerdb_bin()?)
        .args(["get", "--db", dir.path().to_str().expect("utf8 path"), "--key", "b"])
        .output()?;
    assert!(get_main_b.status.success());
    assert!(String::from_utf8_lossy(&get_main_b.stdout).contains("value=2"));

    Ok(())
}
