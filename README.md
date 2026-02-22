# vectdb

Experimental Rust vector database focused on SPFresh indexes, including a durable LayerDB-backed mode.

## Build

```bash
cargo build --workspace
cargo test -p vectdb
```

## CLI

Show all commands:

```bash
cargo run -p vectdb --bin vectdb-cli -- --help
```

Common commands:

```bash
# version
cargo run -p vectdb --bin vectdb-cli -- version

# benchmark across engines
cargo run -p vectdb --bin vectdb-cli -- bench \
  --dim 64 --base 20000 --updates 2000 --queries 400 --k 10 \
  --spfresh-diskmeta --spfresh-diskmeta-probe-multiplier 8

# health check a persisted SPFresh LayerDB index
cargo run -p vectdb --bin vectdb-cli -- spfresh-health \
  --db /path/to/index --dim 64 --initial-postings 64 --diskmeta-probe-multiplier 1

# run sharded deployment server (leader-gated writes + replication fanout)
cargo run -p vectdb --bin vectdb-deploy -- \
  --db-root /tmp/vectdb-cluster/node1 \
  --node-id node1 \
  --listen 0.0.0.0:8080 \
  --etcd-endpoints http://127.0.0.1:2379 \
  --etcd-election-key /vectdb/prod/leader \
  --self-url http://127.0.0.1:8080 \
  --replica-urls http://127.0.0.1:8081,http://127.0.0.1:8082
```

Deployment API (JSON):
- `GET /v1/role`
- `GET /v1/health`
- `POST /v1/search`
- `POST /v1/mutations` (leader only)
- `POST /v1/internal/replicate` (peer replication endpoint)

Docker integration test (etcd + 3 deploy nodes):

```bash
# direct script
./scripts/vectdb_deploy_integration.sh

# cargo-gated integration test
VDB_DOCKER_INTEGRATION=1 cargo test -p vectdb --test deploy_docker -- --nocapture
```

Docker cluster load test:

```bash
# direct script (tune via VDB_LOAD_* env vars)
./scripts/vectdb_deploy_load_test.sh

# cargo-gated load test
VDB_DOCKER_LOAD_TEST=1 cargo test -p vectdb --test deploy_docker_load -- --nocapture
```

## Crate Usage

Add dependency (path for local workspace usage):

```toml
[dependencies]
vectdb = { path = "crates/vectdb" }
anyhow = "1"
```

Minimal durable index example:

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

For full benchmark and mode details, see `crates/vectdb/README.md`.
