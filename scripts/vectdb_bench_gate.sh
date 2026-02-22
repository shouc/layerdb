#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONFIG="${CONFIG:-$ROOT_DIR/benchmarks/vectdb_gate.json}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/target/vectdb-gate}"

mkdir -p "$OUT_DIR"

DATASET="$OUT_DIR/dataset.json"
SPFRESH_JSON="$OUT_DIR/spfresh.json"
LANCEDB_JSON="$OUT_DIR/lancedb.json"
SUMMARY_JSON="$OUT_DIR/summary.json"

DIM="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["dataset"]["dim"])' "$CONFIG")"
BASE="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["dataset"]["base"])' "$CONFIG")"
UPDATES="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["dataset"]["updates"])' "$CONFIG")"
QUERIES="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["dataset"]["queries"])' "$CONFIG")"
K="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["dataset"]["k"])' "$CONFIG")"
SEED="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["dataset"]["seed"])' "$CONFIG")"

SP_SHARDS="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["spfresh"]["shards"])' "$CONFIG")"
SP_INITIAL_POSTINGS="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["spfresh"]["initial_postings"])' "$CONFIG")"
SP_NPROBE="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["spfresh"]["nprobe"])' "$CONFIG")"
SP_SPLIT_LIMIT="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["spfresh"]["split_limit"])' "$CONFIG")"
SP_MERGE_LIMIT="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["spfresh"]["merge_limit"])' "$CONFIG")"
SP_REASSIGN_RANGE="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["spfresh"]["reassign_range"])' "$CONFIG")"
SP_UPDATE_BATCH="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["spfresh"]["update_batch"])' "$CONFIG")"
SP_DISKMETA="$(python3 -c 'import json,sys;print("true" if json.load(open(sys.argv[1]))["spfresh"]["diskmeta"] else "false")' "$CONFIG")"
SP_DISKMETA_PROBE_MULTIPLIER="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["spfresh"].get("diskmeta_probe_multiplier", 1))' "$CONFIG")"

LC_NLIST="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["lancedb"]["nlist"])' "$CONFIG")"
LC_NPROBE="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["lancedb"]["nprobe"])' "$CONFIG")"
LC_UPDATE_BATCH="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["lancedb"]["update_batch"])' "$CONFIG")"
SP_EXACT_SHARD_PRUNE="$(python3 -c 'import json,sys;print("true" if json.load(open(sys.argv[1]))["spfresh"].get("exact_shard_prune", False) else "false")' "$CONFIG")"

FAIR_SEARCH_RUNS="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1])).get("fairness", {}).get("search_runs", 1))' "$CONFIG")"
FAIR_WARMUP_QUERIES="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1])).get("fairness", {}).get("warmup_queries", 0))' "$CONFIG")"
FAIR_MAINTENANCE_BEFORE_SEARCH="$(python3 -c 'import json,sys;print("true" if json.load(open(sys.argv[1])).get("fairness", {}).get("maintenance_before_search", True) else "false")' "$CONFIG")"

echo "Generating benchmark dataset..."
cargo run -p vectdb --bin vectdb-cli -- dump-dataset \
  --out "$DATASET" \
  --seed "$SEED" \
  --dim "$DIM" \
  --base "$BASE" \
  --updates "$UPDATES" \
  --queries "$QUERIES" >/dev/null

echo "Running SPFresh sharded benchmark..."
if [[ "$SP_DISKMETA" == "true" ]]; then
  DISKMETA_FLAG="--diskmeta"
else
  DISKMETA_FLAG=""
fi
if [[ "$SP_EXACT_SHARD_PRUNE" == "true" ]]; then
  EXACT_SHARD_PRUNE_FLAG="--exact-shard-prune"
else
  EXACT_SHARD_PRUNE_FLAG=""
fi
if [[ "$FAIR_MAINTENANCE_BEFORE_SEARCH" == "true" ]]; then
  SP_SKIP_FORCE_REBUILD_FLAG=""
  LC_OPTIMIZE_BEFORE_SEARCH_FLAG="--optimize-before-search"
else
  SP_SKIP_FORCE_REBUILD_FLAG="--skip-force-rebuild"
  LC_OPTIMIZE_BEFORE_SEARCH_FLAG=""
fi
cargo run --release -p vectdb --bin bench_spfresh_sharded -- \
  --dataset "$DATASET" \
  --k "$K" \
  --shards "$SP_SHARDS" \
  --initial-postings "$SP_INITIAL_POSTINGS" \
  --nprobe "$SP_NPROBE" \
  --split-limit "$SP_SPLIT_LIMIT" \
  --merge-limit "$SP_MERGE_LIMIT" \
  --reassign-range "$SP_REASSIGN_RANGE" \
  --diskmeta-probe-multiplier "$SP_DISKMETA_PROBE_MULTIPLIER" \
  --update-batch "$SP_UPDATE_BATCH" \
  --search-runs "$FAIR_SEARCH_RUNS" \
  --warmup-queries "$FAIR_WARMUP_QUERIES" \
  $SP_SKIP_FORCE_REBUILD_FLAG \
  $EXACT_SHARD_PRUNE_FLAG \
  $DISKMETA_FLAG \
  > "$SPFRESH_JSON"

echo "Running LanceDB benchmark..."
cargo run --release -p vectdb --bin bench_lancedb -- \
  --dataset "$DATASET" \
  --k "$K" \
  --nlist "$LC_NLIST" \
  --nprobe "$LC_NPROBE" \
  --update-batch "$LC_UPDATE_BATCH" \
  --search-runs "$FAIR_SEARCH_RUNS" \
  --warmup-queries "$FAIR_WARMUP_QUERIES" \
  $LC_OPTIMIZE_BEFORE_SEARCH_FLAG \
  > "$LANCEDB_JSON"

python3 "$ROOT_DIR/scripts/check_vectdb_gate.py" \
  "$CONFIG" \
  "$SPFRESH_JSON" \
  "$LANCEDB_JSON" \
  "$SUMMARY_JSON"
