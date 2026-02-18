# VectorDB Benchmark Report

Date: 2026-02-18

## Scope
This report covers:
- `SPFreshIndex` (in-memory incremental partition updates)
- `SpFreshLayerDbIndex` (full SPFresh system architecture on LayerDB)
- `AppendOnlyIndex` baseline
- `SaqIndex` and `SaqIndex` uniform ablation for arXiv:2509.12086 validation
- Milvus (`milvus-io/milvus`) IVF_FLAT comparison

## SPFresh System Architecture on LayerDB
`SpFreshLayerDbIndex` adds the paper-style system split between a foreground updater and asynchronous background maintenance:
- foreground updater persists each vector in LayerDB (`spfresh/v/{id}`) and applies low-latency in-memory assignment updates
- background rebuilder runs out-of-band and periodically rebuilds postings from persisted vectors
- recovery-on-open reconstructs the in-memory index from LayerDB state

This uses existing LayerDB WAL/LSM durability while preserving SPFresh search/update behavior.

## Fairness Controls
All engines in each run used:
- identical dataset seed and dimensions
- identical base vectors, update stream, and query set
- identical `k` and `nprobe`
- identical L2 distance metric
- recall measured against exact KNN after all updates

Milvus fairness setup:
- source cloned from `https://github.com/milvus-io/milvus.git` (`/tmp/milvus-layerdb-20260217b`)
- Milvus standalone launched via Docker Compose from repo deployment config
- index type `IVF_FLAT` with `nlist=64`
- updates applied one-by-one (`update_batch=1`) to match vectordb benchmark write mode

## Main Comparison (10k / 2k / 200)

Dataset exported once and reused:
```bash
cargo run -p vectordb --bin vectordb-cli -- dump-dataset \
  --out /tmp/vectordb_dataset_10000_2000_200_seed404.json \
  --seed 404 --dim 64 --base 10000 --updates 2000 --queries 200
```

vectordb run:
```bash
cargo run -p vectordb --bin vectordb-cli -- bench \
  --seed 404 --dim 64 --base 10000 --updates 2000 --queries 200 --k 10 \
  --initial-postings 64 --nprobe 8 --split-limit 64 --merge-limit 16 --reassign-range 32 \
  --saq-total-bits 256 --saq-ivf-clusters 64
```

milvus run:
```bash
python3 scripts/bench_milvus.py \
  --dataset /tmp/vectordb_dataset_10000_2000_200_seed404.json \
  --k 10 --nprobe 8 --nlist 64 --update-batch 1
```

| engine | build ms | update qps | search qps | recall@10 |
|---|---:|---:|---:|---:|
| spfresh | 5838.4 | 527 | 1204 | 0.2595 |
| spfresh-layerdb | 6061.6 | 491 | 475 | 0.4380 |
| append-only | 5863.5 | 3172 | 513 | 0.4165 |
| saq | 5987.7 | 8553 | 341 | 0.4035 |
| saq-uniform | 5834.0 | 9109 | 338 | 0.4045 |
| milvus-ivf-flat | 3311.2 | 139 | 205 | 0.5430 |

## Post-Hardening Re-Run (10k / 2k / 200)

After production-hardening changes (binary storage, atomic generation switch on bulk-load, incremental dirty-id rebuilder, tuned SPFresh probe fanout):

```bash
cargo run -p vectordb --bin vectordb-cli -- bench \
  --seed 404 --dim 64 --base 10000 --updates 2000 --queries 200 --k 10 \
  --initial-postings 64 --nprobe 8 --split-limit 64 --merge-limit 16 --reassign-range 32 \
  --saq-total-bits 256 --saq-ivf-clusters 64
```

| engine | build ms | update qps | search qps | recall@10 |
|---|---:|---:|---:|---:|
| spfresh | 5833.3 | 530 | 159 | 0.8275 |
| spfresh-layerdb | 5982.1 | 118 | 165 | 0.8355 |
| append-only | 5761.7 | 3377 | 523 | 0.4165 |
| saq | 5952.6 | 8206 | 329 | 0.4035 |
| saq-uniform | 5819.8 | 9213 | 345 | 0.4045 |

## Milvus Spot Check (2k / 400 / 100)

Dataset:
```bash
cargo run -p vectordb --bin vectordb-cli -- dump-dataset \
  --out /tmp/vectordb_eval_2000_400_100_seed777.json \
  --seed 777 --dim 64 --base 2000 --updates 400 --queries 100
```

vectordb:
```bash
cargo run -p vectordb --bin vectordb-cli -- bench \
  --seed 777 --dim 64 --base 2000 --updates 400 --queries 100 --k 10 \
  --initial-postings 64 --nprobe 8 --split-limit 64 --merge-limit 16 --reassign-range 32 \
  --saq-total-bits 256 --saq-ivf-clusters 64
```

milvus:
```bash
python3 scripts/bench_milvus.py \
  --dataset /tmp/vectordb_eval_2000_400_100_seed777.json \
  --k 10 --nprobe 8 --nlist 64 --update-batch 1
```

| engine | build ms | update qps | search qps | recall@10 |
|---|---:|---:|---:|---:|
| spfresh-layerdb | 1272.8 | 168 | 857 | 0.7510 |
| milvus-ivf-flat | 6306.1 | 79.7 | 423.7 | 0.5810 |

## Stress Comparison (20k / 5k / 400)

Dataset export:
```bash
cargo run --release -p vectordb --bin vectordb-cli -- dump-dataset \
  --out /tmp/vectordb_dataset_20000_5000_400_seed404.json \
  --seed 404 --dim 64 --base 20000 --updates 5000 --queries 400
```

vectordb run (`nprobe=8`):
```bash
cargo run --release -p vectordb --bin vectordb-cli -- bench \
  --seed 404 --dim 64 --base 20000 --updates 5000 --queries 400 --k 10 \
  --initial-postings 64 --nprobe 8 --split-limit 64 --merge-limit 16 --reassign-range 32 \
  --saq-total-bits 256 --saq-ivf-clusters 64
```

| engine | build ms | update qps | search qps | recall@10 |
|---|---:|---:|---:|---:|
| spfresh | 541.2 | 7985 | 2813 | 0.7030 |
| spfresh-layerdb | 720.2 | 175 | 3301 | 0.7018 |
| append-only | 532.0 | 12833 | 3992 | 0.4320 |
| saq | 560.5 | 123076 | 3434 | 0.4250 |
| saq-uniform | 546.1 | 136671 | 3368 | 0.4273 |

milvus run (`nprobe=8`):
```bash
python3 scripts/bench_milvus.py \
  --dataset /tmp/vectordb_dataset_20000_5000_400_seed404.json \
  --k 10 --nprobe 8 --nlist 64 --update-batch 1
```

| engine | build ms | update qps | search qps | recall@10 |
|---|---:|---:|---:|---:|
| milvus-ivf-flat | 3546.3 | 133.9 | 173.1 | 0.5512 |

High-recall operating point (`nprobe=32`):
```bash
cargo run --release -p vectordb --bin vectordb-cli -- bench \
  --seed 404 --dim 64 --base 20000 --updates 5000 --queries 400 --k 10 \
  --initial-postings 64 --nprobe 32 --split-limit 64 --merge-limit 16 --reassign-range 32 \
  --saq-total-bits 256 --saq-ivf-clusters 64

python3 scripts/bench_milvus.py \
  --dataset /tmp/vectordb_dataset_20000_5000_400_seed404.json \
  --k 10 --nprobe 32 --nlist 64 --update-batch 1
```

| engine | build ms | update qps | search qps | recall@10 |
|---|---:|---:|---:|---:|
| spfresh-layerdb | 691.4 | 172 | 1128 | 0.9605 |
| milvus-ivf-flat | 3765.4 | 148.6 | 177.5 | 0.8990 |

## Key Findings
- At `nprobe=8`, `spfresh-layerdb` outperformed Milvus IVF_FLAT on the 20k/5k/400 workload in recall (`0.7018` vs `0.5512`) and search throughput (`3301 qps` vs `173 qps`) under one-by-one updates.
- Tuning SPFresh probe scaling to make `nprobe` a first-class control (`max(nprobe, k) * 8`) enabled a high-recall mode at `nprobe=32`.
- In that high-recall mode, `spfresh-layerdb` reached `0.9605` recall@10 at `1128 qps`, while Milvus reached `0.8990` at `177.5 qps` on the same dataset and metric.
- SAQ variants remain the highest-update-throughput options in this crate, with lower recall than tuned SPFresh on these runs.

## SAQ Paper Validation (arXiv:2509.12086)
Implemented in `SaqIndex`:
- variance-aware dimension ordering
- joint segmentation + bit allocation dynamic programming
- scalar quantization + CAQ refinement
- uniform ablation (`use_joint_dp=false`, `use_variance_permutation=false`, `caq_rounds=0`)

Validation command:
```bash
cargo run -p vectordb --bin vectordb-cli -- saq-validate \
  --dim 96 --base 25000 --queries 500 --k 10 --seed 42 \
  --total-bits 256 --ivf-clusters 128 --nprobe 12
```
