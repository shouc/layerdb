# layerdb

Minimal LSM-tree key-value store in Rust.

## Status

Experimental project; APIs and on-disk formats may change.

## Quick start

```bash
cargo build
cargo test
cargo run --bin layerdb -- --help
```

## IO backend default

- Default build enables `native-uring`.
- Runtime default backend is platform-aware:
  - Linux: `io_uring` (automatic fallback to blocking IO when unavailable).
  - macOS: `kqueue` backend (`IoBackend::Kqueue`).
  - others: `io_uring` preference with fallback to blocking IO.

Native `io_uring` tries to enable SQPOLL mode by default (best effort). Disable with:

```bash
LAYERDB_URING_SQPOLL=0
```

On Linux, SST reads/writes are routed through the IO executor by default.
To force the legacy mmap-based reader, set:

- `DbOptions::sst_use_io_executor_reads = false`
- `DbOptions::sst_use_io_executor_writes = false`

This speeds up local NVMe-heavy workloads by reducing syscall overhead and enabling the
`io_uring` backend for SST reads/writes.

S3 frozen-level support uses an object-store abstraction:

- Default mode (no S3 config): local object emulation under `<db>/sst_s3`.
- Remote mode (`DbOptions::s3` or `LAYERDB_S3_*` env): direct S3/MinIO uploads and reads with retry.

Read-through cache still uses `<db>/sst_cache`.
`io_uring` is only used for local filesystem IO (NVMe/HDD/cache), not for network transfers.

Run MinIO integration verification:

```bash
./scripts/minio_integration.sh
```

## Example

```bash
cargo run --bin layerdb -- put --db /tmp/layerdb --key hello --value world
cargo run --bin layerdb -- get --db /tmp/layerdb --key hello
```

## Docs

- `docs/LAWS.md`
- `benchmarks/README.md`
- `vectordb/README.md`
- `benchmarks/vectordb_comparison.md`
