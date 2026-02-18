# SPFresh at 10B-1T: OpenReview Research and Implementation

Date: February 18, 2026

## Goal

Design and implement a production-oriented scaling path for SPFresh from current single-index operation toward 10B-1T vectors, while preserving dynamic updates and restart safety.

## 2025-2026 OpenReview Research

The papers below were reviewed from OpenReview entries published/submitted in 2025-2026.

### 1) DistributedANN: Efficient Billion-Scale ANN Search Across Thousands of GPUs (VecDB 2025)

Link: https://openreview.net/forum?id=6AEsfCLRm3

Key points used:
- Explicit distributed architecture for very large ANN corpora.
- Reports practical scaling to tens of billions of vectors.
- Emphasizes balanced partitioning and communication efficiency.

Impact on this repo:
- Motivated sharded SPFresh architecture as the first practical step toward distributed scale.

### 2) DiskHIVF: Disk-Resident Hierarchical IVF for Billion-Scale ANN (ICLR 2026 submission)

Link: https://openreview.net/forum?id=aKCYSL14HX

Key points used:
- Disk-resident hierarchy to reduce random I/O and keep large indexes practical.
- Clear focus on dynamic updates in large-scale environments.

Impact on this repo:
- Reinforced LayerDB-backed, disk-oriented index design rather than pure in-memory expansion.

### 3) TurboQuant: Online Vector Quantization with Near-optimal Distortion Rate (ICLR 2026 submission)

Link: https://openreview.net/forum?id=tO3ASKZlok

Key points used:
- Online quantization that adapts to changing data without full retraining.
- Strong fit for dynamic vector DB workloads.

Impact on this repo:
- Informs next phase: shard-local online compression for memory/IO reduction.

### 4) Qinco2: Vector Compression and Search with Improved Implicit Neural Codebooks (ICLR 2025 poster)

Link: https://openreview.net/forum?id=2zMHHZ569S

Key points used:
- Compression-focused ANN with better speed/recall trade-offs.
- Practical signal for aggressive compression at large scale.

Impact on this repo:
- Supports adding compressed shard-local payloads as a planned phase after sharding.

### 5) Quantization-Enhanced HNSW for Dynamic Search (ICLR 2026 submission)

Link: https://openreview.net/forum?id=Z14gV0qz5r

Key points used:
- Quantization + graph search improves memory efficiency for dynamic settings.
- Highlights that dynamic ANN needs both update handling and compact payloads.

Impact on this repo:
- Supports hybrid direction: SPFresh routing + compressed vector storage.

### 6) Graph-based ANN with Multiple Filters (ICLR 2025 submission)

Link: https://openreview.net/forum?id=a2eBgp4sjH

Key points used:
- Filtering constraints are first-class in practical retrieval.

Impact on this repo:
- Suggests filtered posting lists per shard as a future extension.

### 7) Convexified Filtered ANN (ICLR 2026 submission)

Link: https://openreview.net/forum?id=23wfdcmzeQ

Key points used:
- Filter-aware pruning/routing can preserve speed while adding constraint support.

Impact on this repo:
- Strengthens roadmap for filter-native shard execution.

### 8) Versioned Unified Graph Index (ICLR 2026 submission; withdrawn)

Link: https://openreview.net/forum?id=nadglckd3z

Key points used:
- Version/time-aware indexing is useful for temporal retrieval.

Impact on this repo:
- Aligned with WAL/checkpoint-first design direction and possible version-aware retrieval.

Note: several 2026 entries are submissions/reviews, so they were used as directional engineering signals, not as final accepted-system claims.

## Synthesis: Practical Path to 10B-1T

From the research and current codebase constraints:

1. Shard-first architecture is mandatory.
2. Disk-backed durability and restart safety are mandatory.
3. Compression/quantization should be introduced shard-locally after sharding.
4. Filter/version support should be designed into shard execution plans.

## What Was Implemented

## A) Production sharded SPFresh index

Added `SpFreshLayerDbShardedIndex` on top of existing LayerDB-backed SPFresh:

- New module: `vectordb/src/index/spfresh_layerdb_sharded.rs`
- Exported in: `vectordb/src/index/mod.rs`

Capabilities:
- Deterministic routing (`id % shard_count`) for updates/deletes.
- Bulk-load partitioning by shard.
- Top-k fanout search across shards with global merge.
- Aggregated shard stats and health checks.
- S3 lifecycle passthrough (`sync_to_s3`, `thaw_from_s3`, `gc_orphaned_s3`).
- Graceful multi-shard close and restart recovery.

Validation added:
- `sharded_index_round_trip_across_restart`
- `sharded_delete_routes_to_single_shard`
- `sharded_stats_aggregate_across_shards`

## B) Benchmark tooling for direct comparison

Added benchmark binary:
- `vectordb/src/bin/bench_spfresh_sharded.rs`

Features:
- Consumes same dataset format as `bench_lancedb`.
- Reports build/update/search throughput and recall@k.
- Added `--durable` toggle.
  - `--durable` true: fsync/sync-on-write mode.
  - default false: benchmark throughput mode.

Also improved Lance benchmark robustness:
- `vectordb/src/bin/bench_lancedb.rs` now deduplicates duplicate IDs inside each update batch before merge-insert, preventing ambiguous merge failures with `--update-batch > 1`.

## Benchmarks vs LanceDB

All runs used the same exported datasets and `k=10`.

### Medium dataset

Dataset: `dim=64, base=10000, updates=2000, queries=200`

LanceDB (`nlist=96, nprobe=8, update-batch=128`):
- build_ms: 164.93
- update_qps: 8751.23
- search_qps: 419.59
- recall@k: 0.4640

SPFresh-sharded (`shards=4, nprobe=8`, non-durable mode):
- build_ms: 525.31
- update_qps: 6451.03
- search_qps: 1395.50
- recall@k: 1.0000

### Small dataset

Dataset: `dim=64, base=5000, updates=1000, queries=200`

LanceDB (`nlist=64, nprobe=8, update-batch=128`):
- build_ms: 73.93
- update_qps: 24018.66
- search_qps: 677.97
- recall@k: 0.5175

SPFresh-sharded (`shards=4, nprobe=8`, non-durable mode):
- build_ms: 256.68
- update_qps: 11491.13
- search_qps: 3115.95
- recall@k: 1.0000

SPFresh-sharded (`shards=4, nprobe=8`, durable mode):
- build_ms: 363.94
- update_qps: 202.91
- search_qps: 2867.04
- recall@k: 1.0000

Interpretation:
- Current SPFresh-sharded implementation strongly improves search throughput and recall on these workloads.
- LanceDB remains stronger on batched update throughput in benchmark mode.
- Durable SPFresh mode trades update throughput for stronger write durability, as expected.

## Production Readiness Checks Performed

- `cargo clippy -p vectordb --all-targets -- -D warnings`
- `cargo test -p vectordb -- --nocapture`
- `cargo test --workspace -- --nocapture`

All passed in this environment.

## Repro Commands

Dataset generation:

```bash
cargo run -p vectordb --bin vectordb-cli -- dump-dataset \
  --out output/bench/vdb_dataset_medium.json --seed 406 --dim 64 \
  --base 10000 --updates 2000 --queries 200
```

LanceDB:

```bash
cargo run --release -p vectordb --bin bench_lancedb -- \
  --dataset output/bench/vdb_dataset_medium.json \
  --k 10 --nlist 96 --nprobe 8 --update-batch 128
```

SPFresh-sharded:

```bash
cargo run --release -p vectordb --bin bench_spfresh_sharded -- \
  --dataset output/bench/vdb_dataset_medium.json \
  --k 10 --shards 4 --initial-postings 96 --nprobe 8 \
  --split-limit 256 --merge-limit 64 --reassign-range 16 \
  --rebuild-pending-ops 1000 --rebuild-interval-ms 250
```

Durable SPFresh-sharded:

```bash
cargo run --release -p vectordb --bin bench_spfresh_sharded -- \
  --dataset output/bench/vdb_dataset_medium.json \
  --k 10 --shards 4 --initial-postings 96 --nprobe 8 \
  --split-limit 256 --merge-limit 64 --reassign-range 16 \
  --rebuild-pending-ops 1000 --rebuild-interval-ms 250 \
  --durable
```

## Next engineering steps for 10B-1T

1. Add shard-local compressed payloads (online quantization path inspired by TurboQuant/Qinco2).
2. Add shard-parallel query fanout with bounded latency budget and adaptive probe control.
3. Add filtered posting metadata to avoid full-scan candidate generation under constraints.
4. Add distributed shard placement and rebalancing APIs to move beyond single-node limits.
