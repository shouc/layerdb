use anyhow::Context;
use std::path::PathBuf;
use std::process::Command;

fn docker_load_enabled() -> bool {
    matches!(
        std::env::var("VDB_DOCKER_LOAD_TEST").ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

#[test]
fn sharded_deploy_docker_cluster_load() -> anyhow::Result<()> {
    if !docker_load_enabled() {
        eprintln!("skipping docker load test (set VDB_DOCKER_LOAD_TEST=1)");
        return Ok(());
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = manifest_dir.join("../../scripts/vectordb_deploy_load_test.sh");
    anyhow::ensure!(
        script.exists(),
        "missing docker load script at {}",
        script.display()
    );

    let mut cmd = Command::new("bash");
    cmd.arg(&script);
    cmd.env(
        "VDB_LOAD_TOTAL_WRITES",
        std::env::var("VDB_LOAD_TOTAL_WRITES").unwrap_or_else(|_| "400".to_string()),
    );
    cmd.env(
        "VDB_LOAD_CONCURRENCY",
        std::env::var("VDB_LOAD_CONCURRENCY").unwrap_or_else(|_| "8".to_string()),
    );
    cmd.env(
        "VDB_LOAD_SEARCH_EVERY",
        std::env::var("VDB_LOAD_SEARCH_EVERY").unwrap_or_else(|_| "10".to_string()),
    );
    cmd.env(
        "VDB_LOAD_COMMIT_MODE",
        std::env::var("VDB_LOAD_COMMIT_MODE").unwrap_or_else(|_| "durable".to_string()),
    );
    cmd.env(
        "VDB_LOAD_SAMPLE_COUNT",
        std::env::var("VDB_LOAD_SAMPLE_COUNT").unwrap_or_else(|_| "16".to_string()),
    );

    let status = cmd
        .status()
        .with_context(|| format!("run {}", script.display()))?;
    anyhow::ensure!(
        status.success(),
        "docker load script failed with status {status}"
    );

    Ok(())
}
