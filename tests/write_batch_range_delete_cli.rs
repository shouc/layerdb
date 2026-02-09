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
fn write_batch_cli_supports_range_delete_ops() -> anyhow::Result<()> {
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

    let output = Command::new(layerdb_bin()?)
        .args([
            "write-batch",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--op",
            "rdel:b:d",
            "--sync",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "write-batch(rdel) failed: stdout={} stderr={}",
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
    assert!(String::from_utf8_lossy(&get_b.stdout).contains("not_found"));

    let get_c = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "c",
        ])
        .output()?;
    assert!(get_c.status.success());
    assert!(String::from_utf8_lossy(&get_c.stdout).contains("not_found"));

    let get_d = Command::new(layerdb_bin()?)
        .args([
            "get",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--key",
            "d",
        ])
        .output()?;
    assert!(get_d.status.success());
    assert!(String::from_utf8_lossy(&get_d.stdout).contains("value=4"));

    Ok(())
}
