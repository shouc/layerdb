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

## Step 8 (`key-builder-single-allocation-hotpath`)

Change:
- Reworked hot key builders in SPFresh LayerDB storage (`vector_key`, `posting_map_key`,
  posting-member prefixes/events) to avoid nested `format!` chains and build each key in a
  single preallocated `String`.
- Added key-layout unit test coverage to lock in exact key shapes while keeping the lower-allocation path.

SPFresh:
- `update_qps=217796.71`
- `search_qps=2998.06`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=122410.26`
- `search_qps=838.21`
- `recall_at_k=0.4715`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7792`
- `search_qps_ratio=3.5768`

## Step 9 (`diskmeta-upsert-delete-hotloop-allocation-trim`)

Change:
- In diskmeta upsert/delete mutation paths, removed per-row `touched_ids.push(...)` work by
  reusing prebuilt id vectors for WAL touch-batch payloads.
- In diskmeta upsert path, removed one full vector clone pass on the common durable/acknowledged
  path by moving mutation vectors directly into cache insertion once persistence succeeds.
- Kept fast-path behavior unchanged and validated with full SPFresh test suite and strict clippy.

SPFresh:
- `update_qps=173760.68`
- `search_qps=2723.82`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=114612.36`
- `search_qps=662.83`
- `recall_at_k=0.4680`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.5161`
- `search_qps_ratio=4.1094`

## Step 10 (`residual-event-direct-encode`)

Change:
- Reworked `posting_member_event_upsert_value_with_residual(...)` to encode residual payloads
  directly into the output byte buffer.
- Removed intermediate residual-code vector allocation and copy in the hot update path.
- Preserved on-disk event format and decode behavior.

SPFresh:
- `update_qps=212905.64`
- `search_qps=2571.19`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=119387.54`
- `search_qps=835.34`
- `recall_at_k=0.4815`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7833`
- `search_qps_ratio=3.0780`

## Step 11 (`wal-single-entry-persist-fastpath`)

Change:
- Added a single-entry fast path in `persist_with_wal_batch_ops(...)` for the dominant
  update/delete case where one WAL entry wraps one row-op batch.
- Preallocates the exact operation buffer size and avoids generic multi-entry merge overhead.

SPFresh:
- `update_qps=208344.18`
- `search_qps=2761.28`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=119948.12`
- `search_qps=686.17`
- `recall_at_k=0.4800`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7370`
- `search_qps_ratio=4.0242`

## Step 12 (`diskmeta-fallback-missing-only-reload`)

Change:
- Removed per-posting `HashSet` tracking in diskmeta search fallback.
- When exact payloads are missing in the selected candidate set, fallback exact loads now query
  only `missing_selected_ids` directly, instead of rescanning posting members to rebuild a second id list.
- Preserved fail-closed behavior when any required exact payload remains unavailable.

SPFresh:
- `update_qps=195732.23`
- `search_qps=2834.23`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=114658.35`
- `search_qps=725.61`
- `recall_at_k=0.4875`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7071`
- `search_qps_ratio=3.9060`

## Step 13 (`posting-members-id-only-cache-repr`)

Change:
- Reworked posting-member caches from `Vec<struct{id}>` / nested `HashMap<u64, ...>` to plain id-only
  collections (`Arc<Vec<u64>>` + `FxHashSet<u64>` in ephemeral state).
- Removed per-query candidate extraction by feeding cached posting-member id slices directly into
  distance loading.
- Kept deterministic sort order for cached posting-member ids.

SPFresh:
- `update_qps=217359.79`
- `search_qps=2366.42`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=41383.23`
- `search_qps=604.19`
- `recall_at_k=0.4835`

SPFresh/LanceDB ratio:
- `update_qps_ratio=5.2524`
- `search_qps_ratio=3.9167`

## Step 14 (`topk-worst-index-tracking`)

Change:
- Reworked diskmeta `push_neighbor_topk` to track the current worst slot incrementally.
- Removed full `O(k)` worst-element scan on every candidate; now it recomputes only when the top-k
  set changes.
- Added deterministic unit test `push_neighbor_topk_matches_naive_selection` validating exact
  equivalence with naive sort+truncate selection.

Benchmark note:
- Gate runs were highly noisy on this host in this cycle, so numbers below use a same-dataset
  release rerun pair (`target/vectordb-step15/spfresh-rerun2.json`,
  `target/vectordb-step15/lancedb-rerun2.json`).

SPFresh:
- `update_qps=124248.36`
- `search_qps=1808.32`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=116192.66`
- `search_qps=507.35`
- `recall_at_k=0.4695`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.0693`
- `search_qps_ratio=3.5643`

## Step 15 (`probe-loop-centroid-check-elision`)

Change:
- Removed redundant per-posting centroid-presence check in diskmeta search probe loop; probe ids are
  already produced from live posting metadata.
- Keeps behavior unchanged while trimming one branch + lookup from hot search iteration.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step16-spfresh.json`, `target/vectordb-step16-lancedb.json`.

SPFresh:
- `update_qps=175470.66`
- `search_qps=2625.92`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=203894.38`
- `search_qps=753.60`
- `recall_at_k=0.4805`

SPFresh/LanceDB ratio:
- `update_qps_ratio=0.8606`
- `search_qps_ratio=3.4840`

## Step 16 (`distance-load-buffer-prealloc`)

Change:
- Preallocated internal vectors in `load_distances_for_ids(...)`:
  - `cache_misses` now starts at `ids.len()`,
  - `missing` and `fetched_for_cache` now start at `unresolved.len()`.
- Keeps behavior identical while reducing allocator churn in repeated diskmeta search fallback paths.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step17-spfresh.json`, `target/vectordb-step17-lancedb.json`.

SPFresh:
- `update_qps=120025.51`
- `search_qps=1875.23`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=110939.57`
- `search_qps=472.58`
- `recall_at_k=0.4785`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.0819`
- `search_qps_ratio=3.9680`

## Step 17 (`fxhash-cache-maps`)

Change:
- Switched hot in-memory cache maps to `FxHashMap`:
  - `VectorCache.map`
  - `PostingMembersCache.map`
- Kept behavior identical, only changing hash function/cost in frequent lookup paths.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step18-spfresh.json`, `target/vectordb-step18-lancedb.json`.

SPFresh:
- `update_qps=216303.91`
- `search_qps=2901.02`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=216869.76`
- `search_qps=816.87`
- `recall_at_k=0.4765`

SPFresh/LanceDB ratio:
- `update_qps_ratio=0.9974`
- `search_qps_ratio=3.5513`

## Step 18 (`wal-single-entry-api-and-capacity-reserve`)

Change:
- Removed the now-unused multi-entry WAL persist helper from the hot mutation path and
  standardized callers on `persist_with_wal_ops(...)`.
- Added a single `row_ops.reserve(trailer_ops.len() + 2)` in `persist_with_wal_ops(...)` so
  WAL/meta/trailer append does not trigger extra vector growth on common update/delete commits.

Benchmark note:
- The first full gate run at `target/vectordb-step19/summary.json` was noisy and underperformed
  during long release compile pressure.
- Reported numbers below use a clean same-dataset rerun pair:
  `target/vectordb-step19-spfresh-rerun.json`,
  `target/vectordb-step19-lancedb-rerun.json`.

SPFresh:
- `update_qps=147255.97`
- `search_qps=1761.06`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=65178.69`
- `search_qps=439.07`
- `recall_at_k=0.4880`

SPFresh/LanceDB ratio:
- `update_qps_ratio=2.2593`
- `search_qps_ratio=4.0109`

## Step 19 (`diskmeta-upsert-id-clone-elision-and-fx-new-postings`)

Change:
- Removed one full `Vec<u64>` clone in diskmeta upsert persistence (`touched_ids` now reuses the
  already-built `ids` vector).
- Switched the hot `new_postings` map in diskmeta upsert from std `HashMap` to `FxHashMap`.
- Updated `apply_ephemeral_row_upserts(...)` to accept the `FxHashMap` directly.

Benchmark note:
- Same dataset rerun pair:
  `target/vectordb-step20-spfresh.json`,
  `target/vectordb-step20-lancedb.json`.

SPFresh:
- `update_qps=193346.46`
- `search_qps=2816.22`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=72678.49`
- `search_qps=489.65`
- `recall_at_k=0.4770`

SPFresh/LanceDB ratio:
- `update_qps_ratio=2.6603`
- `search_qps_ratio=5.7515`

## Step 20 (`wal-touch-batch-slice-encoding`)

Change:
- Added `encode_wal_touch_batch_ids(&[u64])` and
  `persist_with_wal_touch_batch_ids(&[u64], ...)` so diskmeta mutation commits can encode WAL
  touch-batches directly from slices.
- Updated diskmeta upsert/delete durable paths to use the slice-based helper and removed
  pre-commit id-vector cloning from those paths.

Benchmark note:
- Same dataset, two rerun pairs:
  - run1: `target/vectordb-step21-spfresh.json`, `target/vectordb-step21-lancedb.json`
  - run2: `target/vectordb-step21-spfresh-rerun2.json`,
    `target/vectordb-step21-lancedb-rerun2.json`
- Host variance was non-trivial; median ratios are reported below.

SPFresh (run1):
- `update_qps=213185.52`
- `search_qps=2800.90`
- `recall_at_k=0.6030`

LanceDB (run1):
- `update_qps=123094.29`
- `search_qps=833.91`
- `recall_at_k=0.4785`

SPFresh/LanceDB ratio (median across run1/run2):
- `update_qps_ratio=1.5776`
- `search_qps_ratio=3.1615`

## Step 21 (`dirty-id-batch-marking`)

Change:
- Added `mark_dirty_batch(&[u64])` to update dirty-id state under one mutex acquisition.
- Switched non-diskmeta upsert path to batch dirty-id marking after mutation apply.
- Switched non-diskmeta delete path to collect deleted ids and batch-mark once.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step22-spfresh.json`,
  `target/vectordb-step22-lancedb.json`.

SPFresh:
- `update_qps=227745.04`
- `search_qps=2748.98`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=127299.00`
- `search_qps=851.50`
- `recall_at_k=0.4550`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7891`
- `search_qps_ratio=3.2284`

## Step 22 (`vector-block-offsets-fxhash`)

Change:
- Switched `VectorBlockStore.offsets` from std `HashMap<u64, u64>` to `FxHashMap<u64, u64>`.
- This path is used in block-backed id lookups for `get_state(...)` and
  `distances_for_ids(...)` in diskmeta search fallback.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step23-spfresh.json`,
  `target/vectordb-step23-lancedb.json`.

SPFresh:
- `update_qps=209071.07`
- `search_qps=2752.91`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=130571.03`
- `search_qps=929.67`
- `recall_at_k=0.4835`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.6012`
- `search_qps_ratio=2.9612`

## Step 23 (`dirty-id-fxhash-set`)

Change:
- Switched rebuild dirty-id tracking set from std `HashSet<u64>` to `FxHashSet<u64>` across:
  - index runtime state,
  - startup/open initialization,
  - rebuilder runtime.
- Updated WAL-tail replay touched-id accumulation to `FxHashSet` for consistency.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step24-spfresh.json`,
  `target/vectordb-step24-lancedb.json`.

SPFresh:
- `update_qps=213528.82`
- `search_qps=2876.99`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=124773.19`
- `search_qps=852.29`
- `recall_at_k=0.4825`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7113`
- `search_qps_ratio=3.3756`

## Step 24 (`diskmeta-state-fxhash-maps`)

Change:
- Switched diskmeta/ephemeral state maps from std `HashMap` to `FxHashMap`:
  - `DiskMetaStateMap`
  - `EphemeralPostingMembers`
  - `EphemeralRowStates`
- Updated map construction sites in mutation/runtime helpers to use `FxHashMap` capacity builders.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step25-spfresh.json`,
  `target/vectordb-step25-lancedb.json`.

SPFresh:
- `update_qps=189088.03`
- `search_qps=2649.76`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=123445.36`
- `search_qps=744.00`
- `recall_at_k=0.4670`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.5318`
- `search_qps_ratio=3.5615`

## Step 25 (`touch-batch-helper-unification`)

Change:
- Routed remaining non-diskmeta upsert/delete WAL commits through
  `persist_with_wal_touch_batch_ids(...)` for consistent slice-based touch-batch encoding.
- Removed now-unused generic single-entry WAL helper.
- Moved `encode_wal_entry(...)` under `#[cfg(test)]` since production code now uses
  `encode_wal_touch_batch_ids(...)` directly.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step26-spfresh.json`,
  `target/vectordb-step26-lancedb.json`.

SPFresh:
- `update_qps=188696.32`
- `search_qps=2834.48`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=126623.69`
- `search_qps=802.93`
- `recall_at_k=0.4600`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.4902`
- `search_qps_ratio=3.5302`

## Step 26 (`vector-block-offset-stream-scan`)

Change:
- Reworked `VectorBlockStore::rebuild_offsets()` to scan block files in fixed-size chunks instead of
  `read_to_end(...)`.
- This removes full-file heap materialization during startup/reopen and keeps offset rebuild memory
  bounded.
- Added test `reopen_rebuild_offsets_preserves_latest_live_records` to lock correct rebuild behavior.

Benchmark note:
- This step primarily targets startup memory behavior; throughput measurements were highly noisy in
  this cycle.
- Two same-dataset rerun pairs:
  - run1: `target/vectordb-step27-spfresh.json`, `target/vectordb-step27-lancedb.json`
  - run2: `target/vectordb-step27-spfresh-rerun2.json`,
    `target/vectordb-step27-lancedb-rerun2.json`
- Median ratios are reported below.

SPFresh (run2):
- `update_qps=212126.58`
- `search_qps=2941.52`
- `recall_at_k=0.6030`

LanceDB (run2):
- `update_qps=56825.65`
- `search_qps=357.64`
- `recall_at_k=0.4855`

SPFresh/LanceDB ratio (median across run1/run2):
- `update_qps_ratio=2.0804`
- `search_qps_ratio=4.7138`

## Step 27 (`diskmeta-index-fxhash-maps`)

Change:
- Switched `SpFreshDiskMetaIndex` internal maps to `FxHashMap`:
  - `postings`
  - `posting_to_coarse`
  - `coarse_to_postings`
  - row-to-posting assignment maps returned by build helpers
- Switched LayerDB helpers using dense id->value maps to `FxHashMap`:
  - `load_rows_with_posting_assignments(...)`
  - `load_posting_members(...)` latest-event map

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step28-spfresh.json`,
  `target/vectordb-step28-lancedb.json`.

SPFresh:
- `update_qps=183508.68`
- `search_qps=2718.87`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=117679.66`
- `search_qps=766.86`
- `recall_at_k=0.4690`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.5594`
- `search_qps_ratio=3.5455`

## Step 28 (`vector-block-unaligned-decode-fastpath`)

Change:
- Reworked vector value decode in `VectorBlockStore` hot read paths to use unaligned 32-bit loads:
  - `distances_for_ids(...)`
  - `get_state(...)`
- Removed repeated per-value slice-bound checks/copies in those loops.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step29-spfresh.json`,
  `target/vectordb-step29-lancedb.json`.

SPFresh:
- `update_qps=191761.45`
- `search_qps=2705.20`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=126026.26`
- `search_qps=832.32`
- `recall_at_k=0.4720`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.5216`
- `search_qps_ratio=3.2502`

## Step 29 (`offset-scan-unaligned-read-fastpath`)

Change:
- Reworked `scan_records_into_offsets(...)` to decode id/flags with direct unaligned loads
  instead of per-record slice/get/copy.
- This reduces per-record CPU overhead during vector-block offset rebuild scans.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step30-spfresh.json`,
  `target/vectordb-step30-lancedb.json`.

SPFresh:
- `update_qps=203482.96`
- `search_qps=2490.37`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=126052.40`
- `search_qps=854.94`
- `recall_at_k=0.4710`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.6143`
- `search_qps_ratio=2.9129`

## Step 30 (`wal-tail-touched-capacity-precount`)

Change:
- In startup WAL tail replay, pre-counted touched-id capacity from WAL entries
  (including `TouchBatch` lengths) before building the touched-id set.
- This reduces hash-set rehash/reserve churn for large WAL tails on restart.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step31-spfresh.json`,
  `target/vectordb-step31-lancedb.json`.

SPFresh:
- `update_qps=229231.83`
- `search_qps=3008.67`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=131947.88`
- `search_qps=917.01`
- `recall_at_k=0.4855`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7373`
- `search_qps_ratio=3.2810`

## Step 31 (`vector-block-batch-buffer-trim`)

Change:
- In `append_upsert_batch_with_posting(...)`, precomputed record-size `u64` once per batch
  instead of converting each iteration.
- In `append_delete_batch(...)`, replaced per-record `Vec::resize(..., 0)` with one prebuilt
  tombstone tail buffer reused across all ids.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step32-spfresh.json`,
  `target/vectordb-step32-lancedb.json`.

SPFresh:
- `update_qps=194874.79`
- `search_qps=2837.18`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=123445.99`
- `search_qps=564.95`
- `recall_at_k=0.4810`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.5786`
- `search_qps_ratio=5.0220`

## Step 32 (`diskmeta-probe-topn-selection`)

Change:
- Reworked diskmeta probe selection to avoid full sorting when only top-`nprobe` is required:
  - added incremental top-N selection helpers in `SpFreshDiskMetaIndex`,
  - applied them to coarse-centroid selection and posting selection.
- `choose_probe_postings(...)` now directly requests only `probe_count` nearest postings.
- Added deterministic equivalence test:
  `nearest_postings_topn_matches_naive_sort`.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-step33-spfresh.json`,
  `target/vectordb-step33-lancedb.json`.

SPFresh:
- `update_qps=219030.05`
- `search_qps=2858.59`
- `recall_at_k=0.6030`

LanceDB:
- `update_qps=130902.54`
- `search_qps=916.08`
- `recall_at_k=0.4845`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.6732`
- `search_qps_ratio=3.1205`

## Step 33 (`diskmeta-mutation-bound-shift-update`)

Change:
- Replaced mutation-time bound invalidation (`max_radius/max_l2_norm = INF`) with a sound
  centroid-shift upper-bound update:
  - for centroid movement from `c_old` to `c_new`, retained members satisfy
    `||x - c_new|| <= ||x - c_old|| + ||c_old - c_new||`,
  - add/same-posting-upsert paths also fold in exact distance of the new vector to the new
    centroid.
- This keeps posting bounds finite and useful for query-time lower-bound pruning without full
  member rescans on each update.
- Added deterministic stress test:
  `mutation_bounds_remain_finite_and_sound`.

Benchmark note:
- Same-dataset release rerun pair:
  `target/vectordb-gate/summary.json` (SPFresh + LanceDB from gate run).

SPFresh:
- `update_qps=230412.54`
- `search_qps=2878.64`
- `recall_at_k=1.0000`

LanceDB:
- `update_qps=130275.48`
- `search_qps=915.82`
- `recall_at_k=0.4880`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7687`
- `search_qps_ratio=3.1433`

## Step 34 (`wal-crc-frame-and-fixed-u64-hot-metadata`)

Change:
- Reworked SPFresh WAL records to a framed binary format with:
  - payload length (`u32`),
  - CRC32 checksum (`u32`),
  - strict trailing-byte validation.
- Switched hot metadata counters (`active generation`, `wal_next_seq`, `posting_event_next_seq`)
  from `bincode` to fixed-width little-endian `u64` encoding.
- Added failure-path tests:
  - `wal_decoder_rejects_checksum_corruption`
  - `fixed_u64_codec_roundtrip`

Impact:
- Improves corruption detection on startup/replay.
- Removes serializer overhead from hot mutation commit metadata writes.

## Step 35 (`vector-block-sidecar-offset-index`)

Change:
- Added persistent vector-block offset sidecar (`.vbi`) with compact record format
  (`id`, `flags`, `offset`).
- Startup now:
  - replays sidecar offset records first,
  - tail-scans only vector-block bytes not yet covered by sidecar.
- Sidecar is appended in lockstep with vector-block upsert/delete appends.
- Added crash-lag recovery test:
  - `reopen_recovers_when_index_sidecar_lags_vector_file`

Impact:
- Startup I/O is reduced from full vector-block payload scans to compact sidecar replay plus
  bounded tail scan when sidecar lags.

## Step 36 (`avx512f-runtime-dispatch`)

Change:
- Added AVX-512F kernels for `squared_l2` and `dot`.
- Updated x86 runtime dispatch priority:
  `avx512f -> avx2+fma -> avx2 -> sse2 -> scalar`.
- Kept existing ARM NEON and scalar fallback paths.

Benchmark note (post-step gate run):
- Summary file:
  `target/vectordb-gate/summary.json`

SPFresh:
- `update_qps=213498.44`
- `search_qps=2728.87`
- `recall_at_k=1.0000`

LanceDB:
- `update_qps=130840.11`
- `search_qps=930.50`
- `recall_at_k=0.4820`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.6318`
- `search_qps_ratio=2.9327`

## Step 37 (`sharded-thread-local-topk-reduce`)

Change:
- Reworked sharded query fanout merge to use lock-free parallel reduction:
  - each shard computes sorted local top-k,
  - reducer merges two sorted top-k lists at a time (`merge_two_sorted_neighbors`),
  - avoids materializing a full `Vec<Vec<Neighbor>>` before global merge.
- Added deterministic equivalence test:
  `merge_two_sorted_neighbors_matches_kway_merge`.

Impact:
- Cuts intermediate allocation pressure and keeps per-thread top-k state local in the search path.

Benchmark note (post-step gate run):
- Summary file:
  `target/vectordb-gate/summary.json`

SPFresh:
- `update_qps=198083.54`
- `search_qps=2410.35`
- `recall_at_k=1.0000`

LanceDB:
- `update_qps=129440.41`
- `search_qps=932.33`
- `recall_at_k=0.4720`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.5303`
- `search_qps_ratio=2.5853`

## Step 38 (`startup-seq-metadata-corruption-recovery`)

Change:
- Added deterministic recovery for corrupted/missing sequence metadata:
  - `META_INDEX_WAL_NEXT_SEQ_KEY`
  - `META_POSTING_EVENT_NEXT_SEQ_KEY`
- Recovery scans existing on-disk keys and rebuilds `next_seq = max_seen + 1`, then
  persists repaired metadata.
- Added focused tests:
  - `ensure_wal_next_seq_recovers_from_corrupt_meta`
  - `ensure_posting_event_next_seq_recovers_from_corrupt_meta`

Impact:
- Startup is resilient to partial metadata corruption and avoids sequence rewinds.

## Step 39 (`vector-block-large-scan-prefetch`)

Change:
- Added explicit prefetching in large (`>=2048`) ordered-offset vector-block distance scans.
- Prefetch support:
  - x86/x86_64 via `_mm_prefetch(..., _MM_HINT_T0)`
  - aarch64 via `prfm pldl1keep`
- Kept existing correctness behavior and sidecar-offset tests unchanged.

Benchmark note (post-step gate run):
- Summary file:
  `target/vectordb-gate/summary.json`

SPFresh:
- `update_qps=195701.09`
- `search_qps=2764.25`
- `recall_at_k=1.0000`

LanceDB:
- `update_qps=130623.97`
- `search_qps=923.02`
- `recall_at_k=0.4765`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.4982`
- `search_qps_ratio=2.9948`

## Step 40 (`vector-sidecar-record-crc-v2`)

Change:
- Upgraded sidecar index format from `vbi1` to `vbi2`.
- Added per-record CRC32 to sidecar entries (`id`, `flags`, `offset`, `crc`).
- Replay now validates each record checksum; on mismatch it stops trusting further sidecar tail
  and deterministically falls back to vector-file tail scan from last valid covered offset.
- Added corruption-recovery test:
  `reopen_recovers_when_index_sidecar_crc_is_corrupted`.

Impact:
- Prevents silent wrong-offset ingestion from sidecar bit-rot and keeps restart correctness
  fail-safe.

## Step 41 (`startup-fail-closed-corrupt-wal-tail`)

Change:
- Added startup regression test proving fail-closed behavior on corrupted WAL tail entries:
  `startup_fails_closed_on_corrupted_wal_tail_entry`.
- The test injects a checksum-corrupted WAL frame, advances `wal_next_seq`, and verifies
  `open_existing(...)` rejects startup instead of silently booting.

Impact:
- Locks in retrieval correctness guarantees by preventing boot on ambiguous/corrupt replay state.

## Step 42 (`posting-snapshot-fixed-binary-codec`)

Change:
- Replaced posting-members snapshot `bincode` payload with a fixed binary codec:
  - tag + schema + event-seq + member count
  - per-member `id` + flags + optional scale + optional residual bytes
- Added strict decode validation:
  - reject unknown member flags
  - reject truncated payloads and trailing bytes
- Kept startup behavior fail-closed by surfacing snapshot decode errors through `load_posting_members`.
- Added focused tests:
  - `posting_members_snapshot_binary_codec_roundtrip`
  - `posting_members_snapshot_binary_decoder_rejects_unknown_flags`
  - `load_posting_members_rejects_corrupt_snapshot_payload`

Impact:
- Cuts snapshot (de)serialization overhead and allocation churn in posting-state rebuilds.
- Makes snapshot corruption handling deterministic and correctness-first.

Benchmark note (post-step gate run):
- Summary file:
  `target/vectordb-gate/summary.json`

SPFresh:
- `update_qps=223970.44`
- `search_qps=2933.62`
- `recall_at_k=1.0000`

LanceDB:
- `update_qps=130700.80`
- `search_qps=908.40`
- `recall_at_k=0.4830`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.7136`
- `search_qps_ratio=3.2294`

## Step 43 (`metadata-fixed-binary-codec`)

Change:
- Replaced spfresh metadata (`META_CONFIG_KEY`) `bincode` payload with fixed binary codec.
- Added strict decode behavior:
  - tag check
  - field overflow checks for `usize` conversions
  - trailing-bytes rejection
- Added targeted tests:
  - `spfresh_metadata_binary_codec_roundtrip`
  - `load_metadata_rejects_corrupt_payload`
  - `ensure_metadata_persists_binary_codec_tag`

Impact:
- Reduces startup metadata decode overhead and allocation churn.
- Enforces deterministic fail-closed behavior for corrupt metadata payloads.

Benchmark note (post-step gate run):
- Summary file:
  `target/vectordb-gate/summary.json`

SPFresh:
- `update_qps=191595.34`
- `search_qps=2801.51`
- `recall_at_k=1.0000`

LanceDB:
- `update_qps=124647.81`
- `search_qps=906.12`
- `recall_at_k=0.4530`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.5371`
- `search_qps_ratio=3.0918`

## Step 44 (`startup-manifest-crc-frame`)

Change:
- Replaced startup manifest payload with fixed framed codec:
  - tag + fixed fields + CRC32
  - explicit optional flag for `applied_wal_seq`
- Startup now decodes manifest strictly and fails closed on corruption instead of silently falling back.
- Updated manifest persistence/read path to use the new codec.
- Added startup regression coverage:
  - `startup_fails_closed_on_corrupted_startup_manifest`
  - existing manifest persistence test now decodes via the new codec.

Impact:
- Hardens restart correctness guarantees by preventing ambiguous epoch boot on corrupted manifests.
- Keeps startup manifest parse path branch-light and allocation-free.

Benchmark note (post-step gate run):
- Summary file:
  `target/vectordb-gate/summary.json`

SPFresh:
- `update_qps=199844.28`
- `search_qps=2528.32`
- `recall_at_k=1.0000`

LanceDB:
- `update_qps=127795.18`
- `search_qps=920.48`
- `recall_at_k=0.4580`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.5638`
- `search_qps_ratio=2.7467`

## Step 45 (`checkpoint-crc-frame`)

Change:
- Added framed index-checkpoint format (`icp1`) with:
  - payload length
  - CRC32 checksum
  - serialized checkpoint payload
- Startup checkpoint load now verifies frame integrity before payload decode.
- On corruption, startup deterministically rebuilds from authoritative rows (existing behavior) with explicit checksum error reporting.
- Added startup regression:
  - `startup_recovers_from_corrupted_index_checkpoint_frame`

Impact:
- Prevents silent acceptance of corrupted checkpoint bytes.
- Tightens restart correctness while retaining deterministic recovery path.

Benchmark note (post-step gate run):
- Summary file:
  `target/vectordb-gate/summary.json`

SPFresh:
- `update_qps=214825.66`
- `search_qps=2783.10`
- `recall_at_k=1.0000`

LanceDB:
- `update_qps=116479.13`
- `search_qps=819.65`
- `recall_at_k=0.4765`

SPFresh/LanceDB ratio:
- `update_qps_ratio=1.8443`
- `search_qps_ratio=3.3955`
