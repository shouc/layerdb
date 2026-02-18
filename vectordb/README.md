# vectordb

Experimental vector index crate with:
- `SPFreshIndex`: partitioned incremental update with split/reassign
- `SpFreshLayerDbIndex`: SPFresh system architecture with LayerDB durability + background rebuild
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
- prefer fallible APIs in services:
  - `try_upsert`, `try_delete`, `try_bulk_load`
  - `open_existing` to recover config from persisted metadata
  - `close` for graceful worker shutdown + final rebuild
  - `health_check` and `stats` for operational monitoring

SAQ paper-style validation (vs uniform ablation):
```bash
cargo run -p vectordb --bin vectordb-cli -- saq-validate \
  --dim 96 --base 25000 --queries 500 --k 10 --total-bits 256 \
  --ivf-clusters 128 --nprobe 12
```
