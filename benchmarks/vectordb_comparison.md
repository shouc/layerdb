# VectorDB Benchmark Report

Date: 2026-02-17

## Scope
This report covers:
- `vectordb` crate with `SPFreshIndex`
- append-only partition baseline (`AppendOnlyIndex`, used as pinecore-like fallback)
- `SaqIndex` and `SaqIndex` uniform ablation for arXiv:2509.12086 validation

## Pinecore Baseline Availability
A public cloneable ANN repo named `pinecore` could not be located.
Attempts included:
- `git ls-remote https://github.com/pinecore/pinecore.git` -> not found
- `gh search repos pinecore` -> no relevant vector DB engine source

For fairness testing in this repo, we used `AppendOnlyIndex` as the pinecore-like baseline because it keeps the same partitioned IVF design but omits SPFresh split/reassign.

## Fairness Controls
All engines in each run used:
- the same dataset seed
- the same base vectors, update stream, and query set
- the same `k`, `nprobe`, and dimensionality
- recall measured against exact KNN over final post-update vectors

## Main Comparison (Moderate Update Pressure)
Command (seeded runs):
```bash
cargo run -p vectordb --bin vectordb-cli -- bench \
  --seed <101|202> --dim 64 --base 20000 --updates 2000 --queries 400 --k 10 \
  --initial-postings 128 --nprobe 12 --split-limit 256 --merge-limit 32 \
  --reassign-range 16 --saq-total-bits 256 --saq-ivf-clusters 128
```

Average of seeds 101 and 202:

| engine | build ms | update qps | search qps | recall@10 |
|---|---:|---:|---:|---:|
| spfresh | 23514.2 | 2445 | 308.5 | 0.4265 |
| append-only | 23774.9 | 2455.5 | 318.5 | 0.4265 |
| saq | 23581.1 | 5025 | 214.5 | 0.4228 |
| saq-uniform | 23315.0 | 5378 | 218.5 | 0.4244 |

Observations:
- `SPFreshIndex` and append-only are close under moderate pressure.
- `SaqIndex` has much faster update throughput than partition scans, but lower search QPS due decode math in query path.

## Stress Comparison (High Split/Reassign Pressure)
Command:
```bash
cargo run -p vectordb --bin vectordb-cli -- bench \
  --seed 404 --dim 64 --base 20000 --updates 5000 --queries 400 --k 10 \
  --initial-postings 64 --nprobe 8 --split-limit 64 --merge-limit 16 \
  --reassign-range 32 --saq-total-bits 256 --saq-ivf-clusters 64
```

| engine | build ms | update qps | search qps | recall@10 |
|---|---:|---:|---:|---:|
| spfresh | 11794.8 | 77 | 858 | 0.2103 |
| append-only | 11975.1 | 1754 | 243 | 0.4320 |
| saq | 12086.9 | 6422 | 165 | 0.4250 |
| saq-uniform | 11846.6 | 6840 | 168 | 0.4273 |

Root-cause analysis for SPFresh slowdown / accuracy drop:
- split/reassign runs synchronously in update path; no background rebuilder queue
- frequent centroid recomputation scans full posting vectors
- reassignment candidate rule is broad (`cond1 || cond2`) and can over-migrate vectors
- repeated split/merge churn causes assignment instability and poorer nearest-partition quality

## arXiv 2509.12086 Validation (SAQ)
Implemented components in `SaqIndex`:
- dimension-variance ordering
- joint segmentation + bit allocation DP
- scalar quantization
- CAQ refinement
- uniform ablation variant (`use_joint_dp=false`, `use_variance_permutation=false`, `caq_rounds=0`)

Validation command:
```bash
cargo run -p vectordb --bin vectordb-cli -- saq-validate \
  --dim 96 --base 25000 --queries 500 --k 10 --seed <42|77> \
  --total-bits 256 --ivf-clusters 128 --nprobe 12
```

Results:

| seed | variant | build ms | mse | search qps | recall@10 |
|---|---|---:|---:|---:|---:|
| 42 | saq | 41538.3 | 0.279788 | 125 | 0.5026 |
| 42 | uniform | 41235.6 | 0.213888 | 128 | 0.4338 |
| 77 | saq | 42052.8 | 0.279927 | 127 | 0.5068 |
| 77 | uniform | 41735.6 | 0.213953 | 129 | 0.4372 |

Interpretation:
- SAQ improved recall over uniform by ~15.9% (avg).
- SAQ increased MSE and was slightly slower in search.
- This is consistent with optimizing ranking quality (through segmentation + CAQ) rather than pure reconstruction error.

## Reproduction Shortcuts
- Main benchmark: `cargo run -p vectordb --bin vectordb-cli -- bench ...`
- SAQ validation: `cargo run -p vectordb --bin vectordb-cli -- saq-validate ...`
