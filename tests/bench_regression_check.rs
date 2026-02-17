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
    let baseline: BaselineFile =
        serde_json::from_slice(&std::fs::read("benchmarks/baseline.json")?)?;

    for (display, entry) in baseline.benchmarks {
        let est_path = Path::new("target")
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
