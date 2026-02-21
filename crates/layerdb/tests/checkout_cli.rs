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
fn checkout_cli_succeeds_for_existing_branch() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

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

    let checkout = Command::new(layerdb_bin()?)
        .args([
            "checkout",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--name",
            "feature",
        ])
        .output()?;

    assert!(
        checkout.status.success(),
        "checkout failed: stdout={} stderr={}",
        String::from_utf8_lossy(&checkout.stdout),
        String::from_utf8_lossy(&checkout.stderr)
    );
    assert!(String::from_utf8_lossy(&checkout.stdout).contains("checkout name=feature"));

    Ok(())
}

#[test]
fn checkout_cli_rejects_unknown_branch() -> anyhow::Result<()> {
    let dir = TempDir::new()?;

    let checkout = Command::new(layerdb_bin()?)
        .args([
            "checkout",
            "--db",
            dir.path().to_str().expect("utf8 path"),
            "--name",
            "nope",
        ])
        .output()?;

    assert!(
        !checkout.status.success(),
        "checkout unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&checkout.stderr);
    assert!(
        stderr.contains("unknown branch"),
        "stderr missing unknown-branch message: {stderr}"
    );

    Ok(())
}
