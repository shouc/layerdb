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

## Step 4 (`simd-kernel-unroll-and-x86-sse2-fallback`)

Change:
- Unrolled AVX2/FMA and AVX2 non-FMA kernels in `dot`/`squared_l2` hot paths to reduce dependency-chain pressure.
- Added explicit x86 SSE2 SIMD fallback kernels so x86 hosts without AVX2 still stay on a SIMD path.
- Kept ARM NEON dispatch path and validated builds for both `x86_64-apple-darwin` and `aarch64-apple-darwin`.

Benchmark runs (2x):

SPFresh run1:
- `update_qps=183480.62`
- `search_qps=2082.94`
- `recall_at_k=0.6030`

SPFresh run2:
- `update_qps=199256.12`
- `search_qps=2298.22`
- `recall_at_k=0.6030`

LanceDB run1:
- `update_qps=104954.51`
- `search_qps=541.80`
- `recall_at_k=0.4820`

LanceDB run2:
- `update_qps=110682.98`
- `search_qps=629.60`
- `recall_at_k=0.4890`

Median SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7749`
- `search_qps_ratio=3.7401`

## Step 5 (`shard-routing-bitmask-fastpath`)

Change:
- Added deterministic shard-routing fast path for power-of-two shard counts (`id & (shard_count - 1)`) while preserving modulo routing for non-power-of-two shard layouts.
- This removes repeated integer-division/modulo cost from hot update/mutation partitioning paths for typical production shard counts (4/8/16/...).

SPFresh:
- `update_qps=213350.40`
- `search_qps=2366.64`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=98881.61`
- `search_qps=542.32`
- `recall_at_k=0.4900`

SPFresh/LanceDB ratio:
- `update_qps_ratio=2.1576`
- `search_qps_ratio=4.3639`

## Step 6 (`exact-kway-merge-and-shard-prune-routing`)

Change:
- Replaced sharded top-k merge with exact k-way merge over per-shard sorted neighbor streams, reducing merge overhead from scanning all shard candidates.
- Added an optional exact shard-pruning search mode (`exact_shard_prune`) that computes per-shard lower bounds and short-circuits shards that cannot improve current top-k, without recall loss.
- Added deterministic correctness test proving `exact_shard_prune` returns identical results to full-shard search.

SPFresh:
- `update_qps=202319.94`
- `search_qps=2748.48`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=114932.07`
- `search_qps=786.22`
- `recall_at_k=0.4860`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7603`
- `search_qps_ratio=3.4958`

## Step 7 (`vector-block-zero-copy-distance-scan`)

Change:
- Added batch distance evaluation directly from mmap-backed vector blocks (`VectorBlockStore::distances_for_ids`) to avoid per-row vector materialization in diskmeta search fallback paths.
- Wired diskmeta distance loading to use vector-block batch distance scanning first, then only hit LayerDB row payloads for truly unresolved ids.
- Added focused unit test for live/tombstoned/missing id behavior in block distance scanning.

SPFresh:
- `update_qps=197469.92`
- `search_qps=2735.85`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=118456.22`
- `search_qps=803.78`
- `recall_at_k=0.4610`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.6670`
- `search_qps_ratio=3.4037`
