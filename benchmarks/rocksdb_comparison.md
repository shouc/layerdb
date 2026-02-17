# LayerDB vs RocksDB (Fair Compare)

Date: 2026-02-17

## Method

Runner: `cargo run --bin engine_bench -- --keys 5000 --value-bytes 128 --repeats 3`

Fairness controls:

- Same key count (`5000`) and value size (`128` bytes).
- Same workload definitions for both engines: `fill`, `fill-batch32`, `readrandom`, `readseq`, `overwrite`, `delete-heavy`, `compact`.
- Single-thread client for both engines.
- RocksDB compression disabled.
- WAL enabled and unsynced for both (LayerDB `fsync_writes=false`; RocksDB `sync=false`, WAL on).
- Fresh DB directory per workload for both engines.

Implementation details:

- LayerDB: current workspace code.
- RocksDB: Rust crate `rocksdb = 0.24.0` (binds to `librocksdb-sys 10.4.2`).
- `facebook/rocksdb` source was cloned to `/tmp/rocksdb-layerdb-20260217` for source parity checks.

## Results (Median of 3)

| workload      | layerdb qps | rocksdb qps | slower factor |
|---------------|-------------|-------------|---------------|
| fill          | 1770        | 6109        | 3.45x         |
| fill-batch32  | 24913       | 39101       | 1.57x         |
| readrandom    | 138757      | 150308      | 1.08x         |
| readseq       | 413747      | 453400      | 1.10x         |
| overwrite     | 1800        | 4733        | 2.63x         |
| delete-heavy  | 1758        | 5014        | 2.85x         |
| compact       | 16731       | 129782      | 7.76x         |

## Why We Were Slow (and Fixes Applied)

### 1) Per-write manifest fsync on hot path (major)

Issue:

- Every write advanced branch head by appending `BranchHead` to MANIFEST with `sync=true`.
- This forced directory/file sync behavior on each write.

Fix:

- Skip manifest persistence in `advance_current_branch()` for `main` branch (recoverable from WAL seqno).

Impact:

- Large write-path improvement (`fill`/`overwrite`/`delete-heavy` moved from ~50 ops/s class to thousands).

### 2) Full range-tombstone scan on every point lookup (major)

Issue:

- `MemTableManager::get()` computed `range_tombstones()` every point read, scanning memtable contents even when there were no range tombstones.

Fix:

- Added range-tombstone presence tracking in memtables and skip tombstone scan when count is zero.

Impact:

- `readrandom` improved drastically and is now close to RocksDB.

### 3) Small-batch write overhead in memtable insert path (moderate)

Issue:

- `MemTable::apply_batch()` used Rayon parallel insertion across shards even for tiny batches.

Fix:

- Added sequential fast path for small batches (`<=64` ops), keeping parallel path for larger batches.

Impact:

- Better foreground write latency and stronger `fill`/`overwrite` performance.

## Remaining Gaps

- `compact` is still the largest gap (~7.8x): LayerDB compaction is simpler and less optimized than RocksDB’s mature compaction pipeline.
- Single-op write workloads are still slower than RocksDB; batching helps significantly (`fill-batch32` improves to 1.57x gap), indicating per-request overhead remains important.
