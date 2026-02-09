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
fn write_batch_cli_applies_atomic_point_ops() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let put_seed = Command::new(layerdb_bin()?)
        .args([
            "put",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "seed",
            "--value",
            "v0",
            "--sync",
        ])
        .output()?;
    assert!(
        put_seed.status.success(),
        "put(seed) failed: stdout={} stderr={}",
        String::from_utf8_lossy(&put_seed.stdout),
        String::from_utf8_lossy(&put_seed.stderr)
    );

    let output = Command::new(layerdb_bin()?)
        .args([
            "write-batch",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--op",
            "put:a:1",
            "--op",
            "put:b:2",
            "--op",
            "del:seed",
            "--sync",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "write-batch failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let get_a = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "a",
        ])
        .output()?;
    assert!(get_a.status.success());
    assert!(String::from_utf8_lossy(&get_a.stdout).contains("value=1"));

    let get_b = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "b",
        ])
        .output()?;
    assert!(get_b.status.success());
    assert!(String::from_utf8_lossy(&get_b.stdout).contains("value=2"));

    let get_seed = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "seed",
        ])
        .output()?;
    assert!(get_seed.status.success());
    assert!(String::from_utf8_lossy(&get_seed.stdout).contains("not_found"));

    Ok(())
}

#[test]
fn write_batch_cli_honors_branch_target() -> anyhow::Result<()> {
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

    let output = Command::new(layerdb_bin()?)
        .args([
            "write-batch",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--branch",
            "feature",
            "--op",
            "put:k:feature_v2",
            "--sync",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "write-batch(feature) failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

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

    let get_feature = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
            "--branch",
            "feature",
        ])
        .output()?;
    assert!(get_feature.status.success());
    assert!(String::from_utf8_lossy(&get_feature.stdout).contains("value=feature_v2"));

    Ok(())
}

#[test]
fn write_batch_cli_rejects_invalid_spec() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let output = Command::new(layerdb_bin()?)
        .args([
            "write-batch",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--op",
            "bad",
            "--sync",
        ])
        .output()?;

    assert!(!output.status.success(), "invalid spec unexpectedly succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("invalid --op spec"),
        "stderr missing invalid-spec message: {stderr}"
    );

    Ok(())
}
