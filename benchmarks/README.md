# Benchmark baselines

This directory stores CI-facing benchmark regression thresholds.

- `baseline.json` contains per-benchmark baseline latencies (`baseline_ns`) and
  an allowed multiplicative budget (`max_regression_factor`).
- Use `scripts/bench_regression.sh` to refresh a Criterion baseline and validate
  results against `baseline.json`.

## Typical workflow

1. Run `scripts/bench_regression.sh` on a representative machine.
2. If expected improvements/regressions occur, update `benchmarks/baseline.json`
   with new medians and reviewed budgets.
3. Keep budgets conservative to catch meaningful slowdowns.
