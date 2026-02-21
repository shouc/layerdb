# Performance Step Benchmarks

Dataset and params are fixed across steps:
- `dim=64`
- `base=10000`
- `updates=2000`
- `queries=200`
- SPFresh: `shards=8`, `nprobe=8`, `diskmeta=true`, `update_batch=1024`
- LanceDB: `nlist=96`, `nprobe=8`, `update_batch=1024`

## Baseline (`bcd203b`)

SPFresh:
- `update_qps=200994.08`
- `search_qps=2654.65`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=131486.32`
- `search_qps=916.59`
- `recall_at_k=0.4970`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.5286`
- `search_qps_ratio=2.8962`

## Step 1 (`simd-dispatch-cache`)

Change:
- Cache runtime SIMD dispatch once for `dot`/`squared_l2` (x86/AVX2+FMA, x86/AVX2, ARM/NEON) to remove repeated feature probing in the hot path.

SPFresh:
- `update_qps=216926.61` (`+7.93%` vs baseline)
- `search_qps=2655.23` (`+0.02%` vs baseline)
- `recall_at_k=0.6030` (`no change`)

LanceDB:
- `update_qps=123028.03`
- `search_qps=831.82`
- `recall_at_k=0.4830`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7632`
- `search_qps_ratio=3.1921`

## Step 2 (`replication-snapshot-fallback`)

Change:
- Added follower snapshot install fallback when replication catch-up falls behind compacted journal window.
- Added deterministic journal reset-to-applied after snapshot install.
- Added integration scenario that stops a follower, forces log compaction, then validates snapshot recovery and correct query results on all replicas.

SPFresh:
- `update_qps=159289.57`
- `search_qps=2584.68`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=125083.06`
- `search_qps=871.81`
- `recall_at_k=0.4765`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.2735`
- `search_qps_ratio=2.9647`

Note:
- This step changes deploy replication behavior, not ANN kernels. Throughput variance here is from host-state noise during repeated benchmark runs.

## Step 3 (`sharded-owned-bulkload-and-partition-prealloc`)

Change:
- Added `try_bulk_load_owned(Vec<VectorRecord>)` in sharded SPFresh to avoid redundant cloning on owned ingest paths.
- Updated deploy snapshot install path and sharded benchmark builder to use owned bulk load.
- Reworked sharded partition/dedup helpers to pre-count by shard and preallocate per-shard `Vec`/`FxHashSet`, reducing hot-path allocation and hash pressure for large update batches.

SPFresh:
- `update_qps=178182.31` (`+19.54%` vs step2)
- `search_qps=2809.52` (`+8.55%` vs step2)
- `recall_at_k=0.6030` (`no change`)

LanceDB:
- `update_qps=130629.31`
- `search_qps=922.84`
- `recall_at_k=0.4690`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.3640`
- `search_qps_ratio=3.0444`
