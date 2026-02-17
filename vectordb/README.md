# vectordb

Experimental vector index crate with:
- `SPFreshIndex`: partitioned incremental update with split/reassign
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

SAQ paper-style validation (vs uniform ablation):
```bash
cargo run -p vectordb --bin vectordb-cli -- saq-validate \
  --dim 96 --base 25000 --queries 500 --k 10 --total-bits 256 \
  --ivf-clusters 128 --nprobe 12
```
