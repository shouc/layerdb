# vectordb

Experimental Rust vector database focused on SPFresh indexes, including a durable LayerDB-backed mode.

## Build

```bash
cargo build --workspace
cargo test -p vectordb
```

## CLI

Show all commands:

```bash
cargo run -p vectordb --bin vectordb-cli -- --help
```

Common commands:

```bash
# version
cargo run -p vectordb --bin vectordb-cli -- version

# benchmark across engines
cargo run -p vectordb --bin vectordb-cli -- bench \
  --dim 64 --base 20000 --updates 2000 --queries 400 --k 10

# health check a persisted SPFresh LayerDB index
cargo run -p vectordb --bin vectordb-cli -- spfresh-health \
  --db /path/to/index --dim 64 --initial-postings 64

# run sharded deployment server (leader-gated writes + replication fanout)
cargo run -p vectordb --bin vectordb-deploy -- \
  --db-root /tmp/vectordb-cluster/node1 \
  --node-id node1 \
  --listen 0.0.0.0:8080 \
  --etcd-endpoints http://127.0.0.1:2379 \
  --etcd-election-key /vectordb/prod/leader \
  --self-url http://127.0.0.1:8080 \
  --replica-urls http://127.0.0.1:8081,http://127.0.0.1:8082
```

Deployment API (JSON):
- `GET /v1/role`
- `GET /v1/health`
- `POST /v1/search`
- `POST /v1/mutations` (leader only)
- `POST /v1/internal/replicate` (peer replication endpoint)

## Crate Usage

Add dependency (path for local workspace usage):

```toml
[dependencies]
vectordb = { path = "crates/vectordb" }
anyhow = "1"
```

Minimal durable index example:

```rust
use vectordb::index::{SpFreshLayerDbConfig, SpFreshLayerDbIndex};
use vectordb::VectorIndex;

fn main() -> anyhow::Result<()> {
    let mut index = SpFreshLayerDbIndex::open("/tmp/vectordb-demo", SpFreshLayerDbConfig::default())?;

    index.try_upsert(1, vec![0.1; 64])?;
    index.try_upsert(2, vec![0.2; 64])?;

    let hits = index.search(&vec![0.2; 64], 1);
    println!("top id={} dist={}", hits[0].id, hits[0].distance);

    index.close()?;
    Ok(())
}
```

For full benchmark and mode details, see `crates/vectordb/README.md`.
