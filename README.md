# vectdb

Rust vector database centered on SPFresh indexes, including durable LayerDB-backed storage.

## CLI Examples

Show available commands:

```bash
cargo run -p vectdb --bin vectdb-cli -- --help
```

Print version:

```bash
cargo run -p vectdb --bin vectdb-cli -- version
```

Generate a reusable synthetic dataset:

```bash
cargo run -p vectdb --bin vectdb-cli -- dump-dataset \
  --out /tmp/vectdb_dataset.json \
  --seed 404 --dim 64 --base 10000 --updates 2000 --queries 200
```

Run engine comparison benchmark:

```bash
cargo run -p vectdb --bin vectdb-cli -- bench \
  --dim 64 --base 20000 --updates 2000 --queries 400 --k 10 \
  --spfresh-diskmeta --spfresh-diskmeta-probe-multiplier 8
```

Check a persisted SPFresh LayerDB index:

```bash
cargo run -p vectdb --bin vectdb-cli -- spfresh-health \
  --db /path/to/index --dim 64 --initial-postings 64 --diskmeta-probe-multiplier 1
```

## Deployment Examples

Start a node with etcd leader election:

```bash
cargo run -p vectdb --bin vectdb-deploy -- \
  --db-root /tmp/vectdb/node1 \
  --node-id node1 \
  --listen 0.0.0.0:8080 \
  --dim 4 \
  --initial-postings 4 \
  --etcd-endpoints http://127.0.0.1:2379 \
  --etcd-election-key /vectdb/prod/leader
```

Read node role and health:

```bash
curl -s http://127.0.0.1:8080/v1/role | jq
curl -s http://127.0.0.1:8080/v1/health | jq
```

Insert/update vectors:

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/mutations \
  -H 'content-type: application/json' \
  -d '{
    "mutations": [
      {"kind":"upsert","id":1,"values":[0.1,0.2,0.3,0.4]},
      {"kind":"upsert","id":2,"values":[0.2,0.1,0.0,0.9]}
    ],
    "commit_mode":"durable"
  }' | jq
```

Search:

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/search \
  -H 'content-type: application/json' \
  -d '{"query":[0.2,0.1,0.0,0.9],"k":2}' | jq
```

## Crate Usage Example

`Cargo.toml`:

```toml
[dependencies]
vectdb = { path = "crates/vectdb" }
anyhow = "1"
```

Code:

```rust
use vectdb::index::{SpFreshLayerDbConfig, SpFreshLayerDbIndex};
use vectdb::VectorIndex;

fn main() -> anyhow::Result<()> {
    let mut index = SpFreshLayerDbIndex::open("/tmp/vectdb-demo", SpFreshLayerDbConfig::default())?;
    index.try_upsert(1, vec![0.1; 64])?;
    index.try_upsert(2, vec![0.2; 64])?;

    let hits = index.search(&vec![0.2; 64], 1);
    println!("top id={} dist={}", hits[0].id, hits[0].distance);

    index.close()?;
    Ok(())
}
```

More CLI flags and mode-specific examples: `crates/vectdb/README.md`.
