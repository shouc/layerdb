# VectorDB Benchmark Report

Date: 2026-02-17

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

## Stress Comparison (20k / 5k / 400)

vectordb run:
```bash
cargo run -p vectordb --bin vectordb-cli -- bench \
  --seed 404 --dim 64 --base 20000 --updates 5000 --queries 400 --k 10 \
  --initial-postings 64 --nprobe 8 --split-limit 64 --merge-limit 16 --reassign-range 32 \
  --saq-total-bits 256 --saq-ivf-clusters 64
```

milvus run:
```bash
python3 scripts/bench_milvus.py \
  --dataset /tmp/vectordb_dataset_20000_5000_400_seed404.json \
  --k 10 --nprobe 8 --nlist 64 --update-batch 1
```

| engine | build ms | update qps | search qps | recall@10 |
|---|---:|---:|---:|---:|
| spfresh | 11618.5 | 373 | 893 | 0.1990 |
| spfresh-layerdb | 11985.2 | 371 | 229 | 0.4803 |
| append-only | 11594.3 | 1765 | 247 | 0.4320 |
| saq | 11954.6 | 6474 | 168 | 0.4250 |
| saq-uniform | 11669.7 | 6883 | 172 | 0.4273 |
| milvus-ivf-flat | 3749.1 | 141 | 155 | 0.5512 |

## Key Findings
- LayerDB-backed SPFresh substantially improves recall under heavy updates versus in-memory SPFresh by using background rebuild.
- `spfresh-layerdb` remains below Milvus recall in these settings but outperforms Milvus on one-by-one update throughput and search QPS.
- SAQ variants remain strong on update throughput but lower on recall than `spfresh-layerdb` and Milvus in these L2 IVF settings.

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
