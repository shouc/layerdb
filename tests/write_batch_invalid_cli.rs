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

    assert!(
        !output.status.success(),
        "invalid spec unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("invalid --op spec"),
        "stderr missing invalid-spec message: {stderr}"
    );

    Ok(())
}
