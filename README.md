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
- Runtime default backend is `io_uring` (Linux), with automatic fallback to blocking IO when unavailable.

On Linux, SST reads/writes are routed through the IO executor by default.
To force the legacy mmap-based reader, set:

- `DbOptions::sst_use_io_executor_reads = false`
- `DbOptions::sst_use_io_executor_writes = false`

This speeds up local NVMe-heavy workloads by reducing syscall overhead and enabling the
`io_uring` backend for SST reads/writes.

S3 support is *file-based* (see `freeze_level_to_s3` / `thaw_level_from_s3`): the database stores
objects under `<db>/sst_s3` and uses a local read-through cache (`<db>/sst_cache`).
`io_uring` is only used for local filesystem IO (NVMe/HDD/cache), not for network transfers.

## Example

```bash
cargo run --bin layerdb -- put --db /tmp/layerdb --key hello --value world
cargo run --bin layerdb -- get --db /tmp/layerdb --key hello
```

## Docs

- `docs/LAWS.md`
- `benchmarks/README.md`
