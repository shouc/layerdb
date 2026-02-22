# SPFresh at 10B-1T: OpenReview Research and Implementation

Date: February 20, 2026

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

- New module: `vectdb/src/index/spfresh_layerdb_sharded.rs`
- Exported in: `vectdb/src/index/mod.rs`

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
- `vectdb/src/bin/bench_spfresh_sharded.rs`

Features:
- Consumes same dataset format as `bench_lancedb`.
- Reports build/update/search throughput and recall@k.
- Added `--durable` toggle.
  - `--durable` true: fsync/sync-on-write mode.
  - default false: benchmark throughput mode.

Also improved Lance benchmark robustness:
- `vectdb/src/bin/bench_lancedb.rs` now deduplicates duplicate IDs inside each update batch before merge-insert, preventing ambiguous merge failures with `--update-batch > 1`.

## C) Off-heap SPFresh runtime mode (memory fix for very large datasets)

To align closer with SPFresh-style disk-first scaling, `SpFreshLayerDbIndex` now supports:

- `SpFreshMemoryMode::Resident` (existing behavior): full vectors kept in RAM.
- `SpFreshMemoryMode::OffHeap` (new): in-memory state keeps postings + id->posting mapping,
  while vector payloads stay in LayerDB and are fetched on demand with cache.
- `SpFreshMemoryMode::OffHeapDiskMeta` (new): in-memory state keeps centroids/statistics only;
  vector payload and posting metadata (`id->posting`, `posting->member`) stay in LayerDB, with
  bounded hot posting-list cache.

Implementation:
- New off-heap core: `vectdb/src/index/spfresh_offheap.rs`
- New disk-metadata core: `vectdb/src/index/spfresh_diskmeta.rs`
- LayerDB runtime enum + checkpoint support for resident/offheap/diskmeta:
  `vectdb/src/index/spfresh_layerdb/mod.rs`
- Diskmeta build now reuses offheap split/merge topology at build time and exports
  centroid/size + id->posting metadata from offheap:
  `vectdb/src/index/spfresh_offheap.rs`, `vectdb/src/index/spfresh_diskmeta.rs`
- LayerDB now exposes `Db::multi_get` (single-snapshot batched point reads), and diskmeta
  search uses batched row fetch on vector-cache misses:
  `src/db/mod.rs`, `vectdb/src/index/spfresh_layerdb/mod.rs`
- Diskmeta WAL now records replayable deltas (upsert/delete payloads) so startup can apply
  WAL tail directly to checkpointed diskmeta state instead of rebuilding from all rows:
  `vectdb/src/index/spfresh_layerdb/storage.rs`, `vectdb/src/index/spfresh_layerdb/mod.rs`
- Diskmeta posting-member values now persist vector payloads (with legacy id-only decode fallback),
  allowing posting scans to prefill vector cache and reduce random point lookups:
  `vectdb/src/index/spfresh_layerdb/storage.rs`, `vectdb/src/index/spfresh_layerdb/mod.rs`
- Search ranking now uses partial top-k selection (`select_nth_unstable_by`) instead of full candidate sort:
  `vectdb/src/index/spfresh.rs`, `vectdb/src/index/spfresh_offheap.rs`,
  `vectdb/src/index/spfresh_layerdb/mod.rs`
- Sharded SPFresh query fanout now runs in parallel across shards:
  `vectdb/src/index/spfresh_layerdb_sharded.rs`
- Diskmeta probe count is adaptive to centroid-distance confidence (fewer probes for confident
  queries, full probes for ambiguous ones):
  `vectdb/src/index/spfresh_diskmeta.rs`
- Rebuilder support in offheap mode:
  `vectdb/src/index/spfresh_layerdb/rebuilder.rs`
- New tests:
  - `offheap_persists_and_recovers_vectors`
  - `offheap_randomized_restarts_preserve_model_state`
  - `offheap_diskmeta_persists_and_recovers_vectors`
  - `offheap_diskmeta_bulk_load_populates_metadata`
  - `offheap_diskmeta_replays_wal_tail_without_row_rebuild`

This addresses the primary memory blocker for 100B+ planning on small machines:
resident RAM no longer needs to scale with full vector payload.

## D) Strict batched write path (insert/update focus)

Implemented a non-heuristic batch commit protocol in `SpFreshLayerDbIndex`:

- `try_upsert_batch(&[VectorRecord])` and `try_delete_batch(&[u64])`.
- One LayerDB write-batch persists:
  - all row mutations,
  - a contiguous WAL entry range,
  - one `index_wal_next_seq` advance.
- Per-batch deterministic semantics:
  - last-write-wins dedup for duplicate IDs inside the same upsert batch,
  - exact sequential application to in-memory index after durable commit.
- Diskmeta batches prefetch prior state with `multi_get` and compute exact centroid deltas.
- Single-row APIs (`try_upsert`, `try_delete`) now route through the same batched path.
- Sharded API now exposes batched routing:
  - `SpFreshLayerDbShardedIndex::try_upsert_batch`
  - `SpFreshLayerDbShardedIndex::try_delete_batch`
- Benchmark runner `bench_spfresh_sharded` now supports `--update-batch`.

Validation added:
- `upsert_batch_last_write_wins_and_recovers`
- `offheap_diskmeta_batch_upsert_delete_round_trip`
- `sharded_batch_upsert_delete_round_trip`

## E) macOS equivalent to io_uring: research + implementation

Primary-source findings:

- Apple File System Programming Guide describes asynchronous custom-file I/O via
  Grand Central Dispatch APIs (`dispatch_io_*`, `dispatch_read`, `dispatch_write`):
  https://developer.apple.com/library/archive/documentation/FileManagement/Conceptual/FileSystemProgrammingGuide/TechniquesforReadingandWritingCustomFiles/TechniquesforReadingandWritingCustomFiles.html
- `kqueue` man page documents that `EVFILT_AIO` is currently unsupported:
  https://www.manpagez.com/man/2/kqueue/
- Apple DTS discussion confirms there is no direct `io_uring`-style non-blocking file API on macOS;
  practical async file I/O is dispatch/thread-pool based:
  https://forums.swift.org/t/task-safe-way-to-write-a-file-asynchronously/54639

Implementation in LayerDB:

- Added `IoBackend::Kqueue` in `src/io/mod.rs`.
- Platform-aware backend default:
  - macOS: `IoBackend::Kqueue`
  - non-macOS: `IoBackend::Uring` preference (existing fallback behavior retained).
- Added capability API `UringExecutor::supports_kqueue()`.
- Updated backend docs/defaults in `src/db/options.rs`.

Design note:
- In this codebase, `Kqueue` is represented as the macOS async-runtime backend choice,
  while Linux keeps native `io_uring` acceleration where available.

## Benchmarks vs LanceDB

All runs used the same exported datasets and `k=10`.

### Medium dataset

Dataset: `dim=64, base=10000, updates=2000, queries=200`

LanceDB (`nlist=96, nprobe=8, update-batch=128`):
- build_ms: 98.55
- update_qps: 22455.04
- search_qps: 541.27
- recall@k: 0.4590

SPFresh-sharded (`shards=4, nprobe=8`, non-durable mode):
- build_ms: 651.26
- update_qps: 9134.62
- search_qps: 4075.67
- recall@k: 1.0000

SPFresh-sharded offheap (`shards=4, nprobe=8`, non-durable mode):
- build_ms: 652.74
- update_qps: 3232.95
- search_qps: 2523.20
- recall@k: 1.0000

SPFresh-sharded diskmeta (`shards=4, nprobe=8`, non-durable mode):
- build_ms: 536.60
- update_qps: 4052.13
- search_qps: 1058.76
- recall@k: 0.9810

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
- Offheap SPFresh substantially reduces memory residency pressure while preserving recall;
  throughput drops versus resident mode due to on-demand vector loads.
- Diskmeta mode pushes memory usage lower by moving posting metadata to LayerDB; after
  topology reuse + posting-value cache-prefill + batched reads + shard-parallel fanout +
  adaptive probing + WAL-delta startup replay, it is materially faster than the prior implementation
  while staying restart-safe/update-safe. In this profile, diskmeta now exceeds LanceDB search QPS
  while maintaining much higher recall.

### Batch-write sensitivity (latest)

Dataset: `dim=64, base=10000, updates=2000, queries=200`, non-durable, `shards=8`.

SPFresh-sharded offheap:
- `update_batch=1`: update_qps `4488.23`
- `update_batch=1024`: update_qps `5104.93` (+13.7%)

SPFresh-sharded diskmeta:
- `update_batch=1`: update_qps `5929.34`
- `update_batch=1024`: update_qps `8913.02` (+50.3%)

After shard-parallel batch fanout:
- SPFresh-sharded diskmeta, `update_batch=1024`: update_qps `26503.20`

LanceDB IVF-flat:
- `update_batch=1`: update_qps `13.17`, search_qps `15.12`, recall@k `0.5475`
- `update_batch=1024`: update_qps `40406.15`, search_qps `811.28`, recall@k `0.5195`

Interpretation:
- Batch commit materially improves SPFresh insert/update throughput, especially in diskmeta mode.
- SPFresh remains clearly stronger on search throughput at high recall in this workload.
- LanceDB remains stronger on large-batch update throughput.

## F) Implemented improvements from this cycle

The following production features were added in code:

1. Posting delta path (non-full-reload behavior):
- Diskmeta update/delete now apply targeted posting-cache deltas (`id` add/remove/replace)
  instead of invalidating and forcing full posting reload on next query.
- Added deterministic, budgeted compaction for posting-cache delta state:
  `posting_delta_compact_interval_ops`, `posting_delta_compact_budget_entries`.

2. Startup replay parallelism:
- Sharded startup/open is now parallelized, so shard-local WAL tail replay runs concurrently.

3. SIMD distance kernels:
- `l2` and `dot` now use runtime-dispatched SIMD (AVX2/FMA, NEON) with scalar fallback.

4. Two-level partitioning (IVF^2-style):
- Diskmeta index now builds/maintains coarse centroids over postings and probes coarse-first.
- Coarse topology refresh is deterministic and budgeted by mutation interval.

5. Residual-coded candidate rerank:
- Posting member payload supports residual code + scale (`rk2`), with compatibility fallback.
- Diskmeta search uses residual approximate scoring for candidate selection before exact rerank.

6. macOS async backend path:
- `IoBackend::Kqueue` async methods now execute via bounded blocking-executor path
  (explicit backend behavior rather than implicit tokio-file path).

7. Lock-free read path (diskmeta metadata):
- Added `arc-swap` diskmeta snapshot publication; query path reads snapshot lock-free.

8. Deterministic merge/compaction scheduler:
- Posting-cache delta compaction is deterministic and bounded per interval (see #1).

9. Columnar vector pages:
- Added Arrow-backed `VectorColumnarPage` and wired diskmeta rerank scanning through it.

10. Continuous benchmark gate:
- Added `scripts/vectdb_bench_gate.sh` + `scripts/check_vectdb_gate.py`.
- Added `benchmarks/vectdb_gate.json` thresholds for SPFresh-vs-LanceDB checks.

### Latest gate profile (same dataset/config as `vectdb_gate.json`)

SPFresh-sharded diskmeta (`shards=8`, `update_batch=1024`):
- recall@k: `1.0000`
- search_qps: `1348.10`
- update_qps: `21780.26`

LanceDB IVF-flat (`nlist=96`, `update_batch=1024`):
- recall@k: `0.4855`
- search_qps: `926.93`
- update_qps: `130054.16`

Ratios (SPFresh / LanceDB):
- search_qps: `1.45x`
- update_qps: `0.167x`

## G) Diskmeta row-v2 layout for update throughput

Implemented:
- Added vector-row v2 payload (`vr2`) with embedded optional `posting_id`.
- Diskmeta upserts now persist row+posting assignment in one row write and no longer require
  separate posting-assignment writes.
- Diskmeta state lookup now reads row payload first and only falls back to legacy posting-map keys
  when v2 assignment is absent.
- Rebuild path can recover assignments from row payload (plus legacy fallback), so startup does not
  depend on posting-map keys for new data.

Observed benchmark impact on the gate profile (`dim=64, base=10000, updates=2000, queries=200`):
- SPFresh-sharded diskmeta:
  - update_qps: `69340.41`
  - search_qps: `1486.97`
  - recall@k: `1.0000`
- LanceDB IVF-flat:
  - update_qps: `125796.05`
  - search_qps: `854.65`
  - recall@k: `0.4450`
- Ratios (SPFresh / LanceDB):
  - update_qps: `0.551x`
  - search_qps: `1.740x`

## H) Thin posting-member upserts + missing-row fallback

Implemented:
- Added `posting_member_value_with_residual_only(...)` and switched diskmeta `try_upsert_batch`
  to write residual-only posting-member payloads (no inline full vector payload) in the hot path.
- Kept bulk/rebuild posting-member encoding unchanged to preserve compatibility for older recovery
  paths.
- Added diskmeta startup WAL-tail replay cache seeding so exact vectors from tail upserts are
  immediately available in the offheap vector cache.
- Added search fallback sequence for diskmeta:
  1) initial exact load for rerank candidates,
  2) exact fallback load across all posting members when candidate rows are missing,
  3) residual-distance fallback for any still-missing ids (best-effort continuity under row loss).

Validation:
- New unit test `posting_member_residual_round_trip_omits_values_payload`.
- `cargo clippy -p vectdb --all-targets -- -D warnings`
- `cargo test -p vectdb spfresh_layerdb::tests:: -- --nocapture`
- `scripts/vectdb_bench_gate.sh`

Latest gate summary (`target/vectdb-gate/summary.json`):
- SPFresh-sharded diskmeta:
  - update_qps: `69565.22`
  - search_qps: `1405.27`
  - recall@k: `1.0000`
- LanceDB IVF-flat:
  - update_qps: `124680.51`
  - search_qps: `784.57`
  - recall@k: `0.4665`
- Ratios (SPFresh / LanceDB):
  - update_qps: `0.558x`
  - search_qps: `1.791x`

## I) Production Refactor: WAL v2 + Append Logs + Fixed Binary + Mmap + Manifest

Implemented in this cycle:

1. WAL v2 (lean diskmeta entries):
- Removed legacy/id-only WAL compatibility paths.
- WAL now stores only new-state payload for diskmeta (`id + new_posting + new_vector`), without
  embedding old `(posting, vector)` pre-image payload.
- Diskmeta startup with WAL tail now rebuilds from authoritative vector rows for exact centroid
  accounting under the lean WAL schema.

2. Append-only posting-member segments:
- Replaced point `put/delete` posting-member keys with append-only event keys:
  `posting/{generation}/{posting}/{seq}/{id}`.
- Added event types: upsert residual sketch and tombstone.
- Search-time loader materializes latest member state by replaying append events.
- Added deterministic compaction trigger that rewrites canonical posting segments when event bloat
  exceeds configured ratio.

3. Fixed-layout binary codecs in hot path:
- Replaced generic serde/rkyv hot-path encodes with fixed binary layout for:
  - vector rows (`vr3`)
  - posting-member events (`pk3`)
  - WAL entries (`wl2`)
- This avoids schema-agnostic decoding overhead in critical update/search paths.

4. Mmap vector block store:
- Added sidecar append-only vector block files (`vector_blocks/epoch-*.vb`) with id->offset map.
- Exact rerank/load path now checks:
  1) RAM vector cache
  2) mmap vector blocks
  3) LayerDB vector rows fallback
- Added tombstone records for deletes.

5. Startup manifest + epoch snapshots:
- Added persisted startup manifest (`spfresh/meta/startup_manifest`) containing:
  - generation
  - applied WAL seq
  - posting-event next seq
  - startup epoch
- Manifest is persisted atomically with checkpoint writes.
- Bulk-load path increments epoch and rotates vector-block epoch file.

6. Shard-local single-writer commit pipeline:
- Added a dedicated commit worker per `SpFreshLayerDbIndex` instance.
- Update paths now build ops locally and submit write batches through commit worker channel for
  deterministic serialized commit order per shard/index.

Validation:
- `cargo clippy -p vectdb --all-targets -- -D warnings`
- `cargo test -p vectdb spfresh_layerdb::tests:: -- --nocapture`
- `scripts/vectdb_bench_gate.sh`

Latest gate summary after full refactor (`target/vectdb-gate/summary.json`):
- SPFresh-sharded diskmeta:
  - update_qps: `64228.05`
  - search_qps: `1165.21`
  - recall@k: `1.0000`
- LanceDB IVF-flat:
  - update_qps: `122800.15`
  - search_qps: `779.72`
  - recall@k: `0.4700`
- Ratios (SPFresh / LanceDB):
  - update_qps: `0.523x`
  - search_qps: `1.494x`

## J) Mmap old-state lookup for diskmeta updates

Implemented:
- Extended vector-block records to persist optional `posting_id` together with vector payload.
- Diskmeta `load_diskmeta_states_for_ids(...)` now resolves old `(posting, vector)` primarily from
  mmap vector blocks and only falls back to LayerDB row reads for unresolved ids.
- Removed legacy posting-map assignment scan from rebuild and old-state lookup paths.

Outcome on benchmark gate (`target/vectdb-gate/summary.json`):
- SPFresh-sharded diskmeta:
  - update_qps: `141291.99`
  - search_qps: `1100.43`
  - recall@k: `1.0000`
- LanceDB IVF-flat:
  - update_qps: `124231.00`
  - search_qps: `843.24`
  - recall@k: `0.4895`
- Ratios (SPFresh / LanceDB):
  - update_qps: `1.137x`
  - search_qps: `1.305x`

This crosses both gate objectives in the same profile: higher update throughput and higher search throughput than LanceDB while retaining recall.

## Production Readiness Checks Performed

- `cargo clippy -p vectdb --all-targets -- -D warnings`
- `cargo test -p vectdb -- --nocapture`
- `cargo test --workspace -- --nocapture`

All passed in this environment.

## Repro Commands

Dataset generation:

```bash
cargo run -p vectdb --bin vectdb-cli -- dump-dataset \
  --out output/bench/vdb_dataset_medium.json --seed 406 --dim 64 \
  --base 10000 --updates 2000 --queries 200
```

LanceDB:

```bash
cargo run --release -p vectdb --bin bench_lancedb -- \
  --dataset output/bench/vdb_dataset_medium.json \
  --k 10 --nlist 96 --nprobe 8 --update-batch 128
```

SPFresh-sharded:

```bash
cargo run --release -p vectdb --bin bench_spfresh_sharded -- \
  --dataset output/bench/vdb_dataset_medium.json \
  --k 10 --shards 4 --initial-postings 96 --nprobe 8 \
  --split-limit 256 --merge-limit 64 --reassign-range 16 \
  --rebuild-pending-ops 1000 --rebuild-interval-ms 250
```

SPFresh-sharded offheap:

```bash
cargo run --release -p vectdb --bin bench_spfresh_sharded -- \
  --dataset output/bench/vdb_dataset_medium.json \
  --k 10 --shards 4 --initial-postings 96 --nprobe 8 \
  --split-limit 256 --merge-limit 64 --reassign-range 16 \
  --rebuild-pending-ops 1000 --rebuild-interval-ms 250 \
  --offheap
```

SPFresh-sharded diskmeta:

```bash
cargo run --release -p vectdb --bin bench_spfresh_sharded -- \
  --dataset output/bench/vdb_dataset_medium.json \
  --k 10 --shards 4 --initial-postings 96 --nprobe 8 \
  --split-limit 256 --merge-limit 64 --reassign-range 16 \
  --rebuild-pending-ops 1000 --rebuild-interval-ms 250 \
  --diskmeta
```

Durable SPFresh-sharded:

```bash
cargo run --release -p vectdb --bin bench_spfresh_sharded -- \
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
