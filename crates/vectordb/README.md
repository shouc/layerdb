# vectordb

Experimental vector index crate with:
- `SPFreshIndex`: partitioned incremental update with split/reassign
- `SpFreshLayerDbIndex`: SPFresh system architecture with LayerDB durability + background rebuild
- `SpFreshLayerDbShardedIndex`: shard-scaled SPFresh LayerDB index for larger corpora
- `AppendOnlyIndex`: partitioned append-only baseline
- `SaqIndex`: scalar additive quantization index inspired by arXiv:2509.12086

## CLI

Show version:
```bash
cargo run -p vectordb --bin vectordb-cli -- version
```

Fair benchmark across engines:
```bash
cargo run -p vectordb --bin vectordb-cli -- bench \
  --dim 64 --base 20000 --updates 2000 --queries 400 --k 10 \
  --initial-postings 128 --nprobe 12 --split-limit 256 --reassign-range 16 \
  --saq-total-bits 256 --saq-ivf-clusters 128
```

Use `--spfresh-offheap` (vector payload off-heap) or `--spfresh-diskmeta`
(vector payload + posting metadata off-heap) to run LayerDB-backed SPFresh variants in low-RAM modes.

For higher recall operating points in SPFresh, raise `--nprobe` (for example `--nprobe 32`).

Export a reproducible benchmark dataset:
```bash
cargo run -p vectordb --bin vectordb-cli -- dump-dataset \
  --out /tmp/vectordb_dataset.json --seed 404 --dim 64 --base 10000 --updates 2000 --queries 200
```

Compare the same dataset with Milvus:
```bash
python3 scripts/bench_milvus.py \
  --dataset /tmp/vectordb_dataset.json --k 10 --nprobe 8 --nlist 64 --update-batch 1
```

Compare the same dataset with LanceDB (Rust API):
```bash
cargo run --release -p vectordb --bin bench_lancedb -- \
  --dataset /tmp/vectordb_dataset.json --k 10 --nprobe 8 --nlist 64 --update-batch 1
```

Compare the same dataset with sharded SPFresh LayerDB:
```bash
cargo run --release -p vectordb --bin bench_spfresh_sharded -- \
  --dataset /tmp/vectordb_dataset.json --k 10 --shards 4 \
  --initial-postings 64 --nprobe 8 --split-limit 256 --merge-limit 64 --reassign-range 16
```

Run sharded SPFresh in off-heap mode (vectors on LayerDB, metadata in RAM):
```bash
cargo run --release -p vectordb --bin bench_spfresh_sharded -- \
  --dataset /tmp/vectordb_dataset.json --k 10 --shards 4 \
  --initial-postings 64 --nprobe 8 --split-limit 256 --merge-limit 64 --reassign-range 16 \
  --offheap
```

Run sharded SPFresh with disk-backed metadata (hot posting/member lists cached in RAM):
```bash
cargo run --release -p vectordb --bin bench_spfresh_sharded -- \
  --dataset /tmp/vectordb_dataset.json --k 10 --shards 4 \
  --initial-postings 64 --nprobe 8 --split-limit 256 --merge-limit 64 --reassign-range 16 \
  --diskmeta
```

Run the cross-engine benchmark gate (SPFresh vs LanceDB thresholds):
```bash
./scripts/vectordb_bench_gate.sh
```

Check SPFresh LayerDB index health:
```bash
cargo run -p vectordb --bin vectordb-cli -- spfresh-health \
  --db /path/to/spfresh-index --dim 64 --initial-postings 64 --split-limit 512 \
  --merge-limit 64 --reassign-range 64 --nprobe 8 --kmeans-iters 8
```

## Production Notes (LayerDB-backed SPFresh)
- `SpFreshLayerDbConfig::default()` is durability-first:
  - `db_options.fsync_writes=true`
  - `write_sync=true`
- Memory modes:
  - `SpFreshMemoryMode::Resident` (default): keeps full SPFresh vectors in RAM.
  - `SpFreshMemoryMode::OffHeap`: keeps SPFresh posting metadata in RAM and loads vector
    payloads from LayerDB on demand (with cache via `offheap_cache_capacity`).
  - `SpFreshMemoryMode::OffHeapDiskMeta`: keeps centroids/statistics in RAM while storing
    vector payload, `id -> posting`, and `posting -> member` metadata in LayerDB. Hot posting
    lists are cached in memory (`offheap_posting_cache_entries`), and posting scans can prefill
    vector cache from persisted member payloads.
- Startup behavior:
  - resident/offheap replay WAL tail from typed upsert/delete payloads (row rebuild fallback only
    for legacy WAL entries).
  - diskmeta replays WAL delta payloads directly from checkpoint, avoiding full-row startup rebuild
    on normal WAL-tail recovery.
- Diskmeta query path:
  - hierarchical coarse probing over postings (IVF^2-style),
  - residual-coded candidate rerank before exact distance,
  - Arrow columnar page scan for exact candidate scoring,
  - lock-free metadata snapshot reads in query path (`arc-swap`).
- prefer fallible APIs in services:
  - `try_upsert`, `try_delete`, `try_bulk_load`
  - `try_upsert_batch`, `try_delete_batch` for strict batched WAL commit path
  - `try_apply_batch(&[VectorMutation])` for mixed upsert/delete ingestion
  - `open_existing` to recover config from persisted metadata
  - `close` for graceful worker shutdown + final rebuild
  - `health_check` and `stats` for operational monitoring
  - `sync_to_s3`, `thaw_from_s3`, `gc_orphaned_s3` for S3 lifecycle management

S3 tiering usage (via LayerDB backend):
```rust
let moved = index.sync_to_s3(None)?;
let thawed = index.thaw_from_s3(None)?;
let removed = index.gc_orphaned_s3()?;
println!("moved={moved} thawed={thawed} removed={removed}");
```

To use real object storage (instead of local `sst_s3/` emulation), set
`SpFreshLayerDbConfig.db_options.s3` (or `LAYERDB_S3_*` env vars used by LayerDB defaults).

SAQ paper-style validation (vs uniform ablation):
```bash
cargo run -p vectordb --bin vectordb-cli -- saq-validate \
  --dim 96 --base 25000 --queries 500 --k 10 --total-bits 256 \
  --ivf-clusters 128 --nprobe 12
```
