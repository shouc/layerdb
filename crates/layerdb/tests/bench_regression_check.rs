use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BaselineFile {
    benchmarks: BTreeMap<String, BaselineEntry>,
}

#[derive(Debug, Deserialize)]
struct BaselineEntry {
    criterion: String,
    baseline_ns: f64,
    max_regression_factor: f64,
}

#[derive(Debug, Deserialize)]
struct EstimateFile {
    mean: EstimateMean,
}

#[derive(Debug, Deserialize)]
struct EstimateMean {
    point_estimate: f64,
}

#[test]
fn benchmark_baselines_allow_current_results() -> anyhow::Result<()> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .ok_or_else(|| anyhow::anyhow!("resolve workspace root from {}", manifest_dir.display()))?;
    let baseline_path = workspace_root.join("benchmarks").join("baseline.json");
    let baseline: BaselineFile = serde_json::from_slice(&std::fs::read(&baseline_path)?)?;

    for (display, entry) in baseline.benchmarks {
        let est_path = workspace_root
            .join("target")
            .join("criterion")
            .join(&entry.criterion)
            .join("new")
            .join("estimates.json");
        if !est_path.exists() {
            continue;
        }

        let estimate: EstimateFile = serde_json::from_slice(&std::fs::read(&est_path)?)?;
        let allowed = entry.baseline_ns * entry.max_regression_factor;
        assert!(
            estimate.mean.point_estimate <= allowed,
            "benchmark regression: {display} point={} baseline={} allowed={} ({})",
            estimate.mean.point_estimate,
            entry.baseline_ns,
            allowed,
            est_path.display()
        );
    }

    Ok(())
}
