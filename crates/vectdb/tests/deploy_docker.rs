use anyhow::Context;
use std::path::PathBuf;
use std::process::Command;

fn docker_integration_enabled() -> bool {
    matches!(
        std::env::var("VDB_DOCKER_INTEGRATION").ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

#[test]
fn sharded_deploy_docker_round_trip() -> anyhow::Result<()> {
    if !docker_integration_enabled() {
        eprintln!("skipping docker integration test (set VDB_DOCKER_INTEGRATION=1)");
        return Ok(());
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = manifest_dir.join("../../scripts/vectdb_deploy_integration.sh");
    anyhow::ensure!(
        script.exists(),
        "missing docker integration script at {}",
        script.display()
    );

    let status = Command::new("bash")
        .arg(&script)
        .status()
        .with_context(|| format!("run {}", script.display()))?;
    anyhow::ensure!(
        status.success(),
        "docker integration script failed with status {status}"
    );

    Ok(())
}
