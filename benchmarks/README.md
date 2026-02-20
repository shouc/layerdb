# Benchmark baselines

This directory stores CI-facing benchmark regression thresholds.

- `baseline.json` contains per-benchmark baseline latencies (`baseline_ns`) and
  an allowed multiplicative budget (`max_regression_factor`).
- `vectordb_gate.json` contains cross-engine thresholds for SPFresh vs LanceDB.
- Use `scripts/bench_regression.sh` to refresh a Criterion baseline and validate
  results against `baseline.json`.
- Use `scripts/vectordb_bench_gate.sh` to run reproducible SPFresh/LanceDB gate checks.
- Use `cargo run --bin engine_bench` for direct LayerDB vs RocksDB comparisons.
- See `benchmarks/rocksdb_comparison.md` for the latest fair-run analysis.
- See `benchmarks/vectordb_comparison.md` for vectordb and Milvus vector-search comparisons.

## Typical workflow

1. Run `scripts/bench_regression.sh` on a representative machine.
2. If expected improvements/regressions occur, update `benchmarks/baseline.json`
   with new medians and reviewed budgets.
3. Keep budgets conservative to catch meaningful slowdowns.
4. Run `scripts/vectordb_bench_gate.sh` before merging vector-index performance changes.
