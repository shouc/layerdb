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
fn create_branch_cli_can_seed_from_seqno() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let put_v1 = Command::new(layerdb_bin()?)
        .args([
            "put",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
            "--value",
            "v1",
            "--sync",
        ])
        .output()?;
    assert!(put_v1.status.success());

    let put_v2 = Command::new(layerdb_bin()?)
        .args([
            "put",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
            "--value",
            "v2",
            "--sync",
        ])
        .output()?;
    assert!(put_v2.status.success());

    let create_old = Command::new(layerdb_bin()?)
        .args([
            "create-branch",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--name",
            "old",
            "--from-seqno",
            "1",
        ])
        .output()?;
    assert!(
        create_old.status.success(),
        "create-branch --from-seqno failed: stdout={} stderr={}",
        String::from_utf8_lossy(&create_old.stdout),
        String::from_utf8_lossy(&create_old.stderr)
    );

    let get_old = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
            "--branch",
            "old",
        ])
        .output()?;
    assert!(get_old.status.success());
    assert!(String::from_utf8_lossy(&get_old.stdout).contains("value=v1"));

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
    assert!(String::from_utf8_lossy(&get_main.stdout).contains("value=v2"));

    Ok(())
}

#[test]
fn create_branch_cli_rejects_future_seqno() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let put = Command::new(layerdb_bin()?)
        .args([
            "put",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
            "--value",
            "v1",
            "--sync",
        ])
        .output()?;
    assert!(put.status.success());

    let create = Command::new(layerdb_bin()?)
        .args([
            "create-branch",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--name",
            "future",
            "--from-seqno",
            "999",
        ])
        .output()?;

    assert!(
        !create.status.success(),
        "future seqno unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&create.stderr);
    assert!(
        stderr.contains("ahead of latest"),
        "stderr missing future-seqno message: {stderr}"
    );

    Ok(())
}

#[test]
fn create_branch_cli_rejects_conflicting_source_flags() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let put = Command::new(layerdb_bin()?)
        .args([
            "put",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
            "--value",
            "v1",
            "--sync",
        ])
        .output()?;
    assert!(put.status.success());

    let create = Command::new(layerdb_bin()?)
        .args([
            "create-branch",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--name",
            "bad",
            "--from-branch",
            "main",
            "--from-seqno",
            "1",
        ])
        .output()?;

    assert!(
        !create.status.success(),
        "conflicting flags unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&create.stderr);
    assert!(
        stderr.contains("cannot be used with") || stderr.contains("mutually exclusive"),
        "stderr missing conflict message: {stderr}"
    );

    Ok(())
}
