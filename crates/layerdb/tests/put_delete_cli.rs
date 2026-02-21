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
fn put_and_delete_cli_round_trip() -> anyhow::Result<()> {
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
    assert!(
        put.status.success(),
        "put failed: stdout={} stderr={}",
        String::from_utf8_lossy(&put.stdout),
        String::from_utf8_lossy(&put.stderr)
    );

    let get = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
        ])
        .output()?;
    assert!(get.status.success());
    assert!(String::from_utf8_lossy(&get.stdout).contains("value=v1"));

    let delete = Command::new(layerdb_bin()?)
        .args([
            "delete",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
            "--sync",
        ])
        .output()?;
    assert!(
        delete.status.success(),
        "delete failed: stdout={} stderr={}",
        String::from_utf8_lossy(&delete.stdout),
        String::from_utf8_lossy(&delete.stderr)
    );

    let get_after = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "k",
        ])
        .output()?;
    assert!(get_after.status.success());
    assert!(String::from_utf8_lossy(&get_after.stdout).contains("not_found"));

    Ok(())
}
