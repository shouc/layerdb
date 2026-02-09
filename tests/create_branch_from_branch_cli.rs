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
fn create_branch_cli_can_seed_from_branch_head() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let put_main_v1 = Command::new(layerdb_bin()?)
        .args([
            "put",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
            "--value",
            "main_v1",
            "--sync",
        ])
        .output()?;
    assert!(put_main_v1.status.success());

    let create_feature = Command::new(layerdb_bin()?)
        .args([
            "create-branch",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--name",
            "feature",
        ])
        .output()?;
    assert!(create_feature.status.success());

    let put_main_v2 = Command::new(layerdb_bin()?)
        .args([
            "put",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
            "--value",
            "main_v2",
            "--sync",
        ])
        .output()?;
    assert!(put_main_v2.status.success());

    let put_feature_v2 = Command::new(layerdb_bin()?)
        .args([
            "put",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--branch",
            "feature",
            "--key",
            "k",
            "--value",
            "feature_v2",
            "--sync",
        ])
        .output()?;
    assert!(put_feature_v2.status.success());

    let create_feature2 = Command::new(layerdb_bin()?)
        .args([
            "create-branch",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--name",
            "feature2",
            "--from-branch",
            "feature",
        ])
        .output()?;
    assert!(
        create_feature2.status.success(),
        "create-branch --from-branch failed: stdout={} stderr={}",
        String::from_utf8_lossy(&create_feature2.stdout),
        String::from_utf8_lossy(&create_feature2.stderr)
    );

    let get_feature2 = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
            "--branch",
            "feature2",
        ])
        .output()?;
    assert!(get_feature2.status.success());
    assert!(String::from_utf8_lossy(&get_feature2.stdout).contains("value=feature_v2"));

    let get_main = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
        ])
        .output()?;
    assert!(get_main.status.success());
    assert!(String::from_utf8_lossy(&get_main.stdout).contains("value=main_v2"));

    Ok(())
}
