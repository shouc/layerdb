# LayerDB vs RocksDB (Fair Compare)

Date: 2026-02-17

## Method

Runner:
```bash
cargo run --bin engine_bench -- --keys 5000 --value-bytes 128 --repeats 3
```

Fairness controls:
- same key count (`5000`) and value size (`128` bytes)
- same workload definitions for both engines (`fill`, `fill-batch32`, `readrandom`, `readseq`, `overwrite`, `delete-heavy`, `compact`)
- single-thread client for both
- RocksDB compression disabled
- WAL enabled and unsynced for both (LayerDB `fsync_writes=false`; RocksDB `sync=false`, WAL enabled)
- fresh DB directory per workload for both

Implementation details:
- LayerDB: current workspace code
- RocksDB: Rust crate `rocksdb = 0.24.0` (`librocksdb-sys 10.4.2`)
- RocksDB source checked from clone at `/tmp/rocksdb-layerdb-20260217`

## Results (Median of 3)

| workload      | layerdb qps | rocksdb qps | slower factor |
|---------------|-------------|-------------|---------------|
| fill          | 49,930      | 130,501     | 2.61x         |
| fill-batch32  | 393,559     | 779,418     | 1.98x         |
| readrandom    | 421,891     | 405,184     | 0.96x         |
| readseq       | 1,026,492   | 1,546,930   | 1.51x         |
| overwrite     | 47,380      | 106,895     | 2.26x         |
| delete-heavy  | 59,151      | 108,589     | 1.84x         |
| compact       | 49,515      | 655,290     | 13.23x        |

## Why We Are Still Slower

1. `compact` remains the dominant gap.
- LayerDB compaction is currently simpler and less optimized than RocksDB's mature compaction pipeline and scheduling heuristics.

2. Single-op write workloads still show a consistent per-request overhead.
- `fill`, `overwrite`, and `delete-heavy` are all slower than RocksDB.
- batching closes part of the gap (`fill-batch32` improves materially), indicating fixed per-operation costs remain a key bottleneck.

3. Read path is much closer.
- `readrandom` is now competitive and slightly faster in this run.
- `readseq` still trails, but by a smaller margin than write/compact paths.

## Practical Next Optimizations
- prioritize compaction pipeline improvements (selection, IO strategy, write amplification controls)
- reduce foreground per-write overhead for non-batched operations
- add compaction-stage profiling counters to isolate CPU vs IO bottlenecks
