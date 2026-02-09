#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BASELINE_NAME="${BASELINE_NAME:-ci}"
COMPARE_NAME="${COMPARE_NAME:-ci_prev}"
WARMUP_SECS="${WARMUP_SECS:-0.01}"
MEASURE_SECS="${MEASURE_SECS:-0.01}"
SAMPLE_SIZE="${SAMPLE_SIZE:-10}"
THRESHOLD="${THRESHOLD:-0.30}"

BENCHES=(
  "fill/5k"
  "readrandom/5k"
  "readseq/5k"
  "overwrite/5k"
  "delete-heavy/5k"
  "scan-heavy/5x"
  "compact/10k"
)

run_once() {
  local filter="$1"
  shift
  cargo bench --bench regression -- "$filter" --exact "$@" --noplot --quiet
}

for bench in "${BENCHES[@]}"; do
  run_once "$bench" \
    --save-baseline "$BASELINE_NAME" \
    --sample-size "$SAMPLE_SIZE" \
    --warm-up-time "$WARMUP_SECS" \
    --measurement-time "$MEASURE_SECS"
done

has_compare_baseline() {
  local bench
  for bench in "${BENCHES[@]}"; do
    local criterion_name="${bench//\//_}"
    if [[ -d "target/criterion/$criterion_name/$COMPARE_NAME" ]]; then
      return 0
    fi
  done
  return 1
}

if has_compare_baseline; then
  for bench in "${BENCHES[@]}"; do
    run_once "$bench" \
      --baseline "$COMPARE_NAME" \
      --sample-size "$SAMPLE_SIZE" \
      --warm-up-time "$WARMUP_SECS" \
      --measurement-time "$MEASURE_SECS"
  done
fi

python3 "$ROOT_DIR/scripts/check_bench_regression.py" \
  "$ROOT_DIR/benchmarks/baseline.json" \
  "$ROOT_DIR/target/criterion" \
  "$BASELINE_NAME" \
  "$THRESHOLD"

for bench in "${BENCHES[@]}"; do
  criterion_name="${bench//\//_}"
  if [[ ! -d "target/criterion/$criterion_name/$BASELINE_NAME" ]]; then
    continue
  fi

  rm -rf "target/criterion/$criterion_name/$COMPARE_NAME"
  cp -R \
    "target/criterion/$criterion_name/$BASELINE_NAME" \
    "target/criterion/$criterion_name/$COMPARE_NAME"
done

echo "bench regression check passed"
