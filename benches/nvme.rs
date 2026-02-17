use std::ops::Bound;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use tempfile::TempDir;

use layerdb::{Db, DbOptions, Range, ReadOptions, WriteOptions};

const KEYS: u32 = 20_000;
const VALUE_BYTES: usize = 256;

fn options() -> DbOptions {
    DbOptions {
        memtable_shards: 4,
        wal_segment_bytes: 512 * 1024,
        memtable_bytes: 512 * 1024,
        fsync_writes: false,
        sst_use_io_executor_reads: true,
        sst_use_io_executor_writes: true,
        ..Default::default()
    }
}

fn options_cached() -> DbOptions {
    DbOptions {
        block_cache_entries: 256,
        ..options()
    }
}

fn key(i: u32) -> Bytes {
    Bytes::from(format!("k{:08}", i))
}

fn value(i: u32) -> Bytes {
    let mut buf = vec![b'x'; VALUE_BYTES];
    let tag = format!("v{:08}", i);
    let prefix = tag.as_bytes();
    buf[..prefix.len()].copy_from_slice(prefix);
    Bytes::from(buf)
}

fn open_temp_db() -> (TempDir, Db) {
    let dir = TempDir::new().expect("tempdir");
    let db = Db::open(dir.path(), options()).expect("open");
    (dir, db)
}

fn open_temp_db_cached() -> (TempDir, Db) {
    let dir = TempDir::new().expect("tempdir");
    let db = Db::open(dir.path(), options_cached()).expect("open");
    (dir, db)
}

fn preload(db: &Db) {
    for i in 0..KEYS {
        db.put(key(i), value(i), WriteOptions { sync: false })
            .expect("put");
    }

    // Ensure remaining memtable data is flushed before read benchmarks.
    db.compact_range(None).expect("compact");
}

fn bench_fill(c: &mut Criterion) {
    c.bench_function("nvme/fill/20k_256b", |b| {
        b.iter_batched(
            open_temp_db,
            |(_dir, db)| {
                for i in 0..KEYS {
                    db.put(key(i), value(i), WriteOptions { sync: false })
                        .expect("put");
                }
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_readrandom(c: &mut Criterion) {
    c.bench_function("nvme/readrandom/20k_256b", |b| {
        b.iter_batched(
            || {
                let (dir, db) = open_temp_db();
                preload(&db);

                let mut keys: Vec<u32> = (0..KEYS).collect();
                let mut rng = StdRng::seed_from_u64(0x5eed);
                keys.shuffle(&mut rng);

                (dir, db, keys)
            },
            |(_dir, db, keys)| {
                for k in keys {
                    let _ = db.get(key(k), ReadOptions::default()).expect("get");
                }
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_readrandom_cached(c: &mut Criterion) {
    c.bench_function("nvme/readrandom_cached/20k_256b", |b| {
        b.iter_batched(
            || {
                let (dir, db) = open_temp_db_cached();
                preload(&db);

                let mut keys: Vec<u32> = (0..KEYS).collect();
                let mut rng = StdRng::seed_from_u64(0x5eed);
                keys.shuffle(&mut rng);

                (dir, db, keys)
            },
            |(_dir, db, keys)| {
                for k in keys {
                    let _ = db.get(key(k), ReadOptions::default()).expect("get");
                }
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_readseq(c: &mut Criterion) {
    c.bench_function("nvme/readseq/20k_256b", |b| {
        b.iter_batched(
            || {
                let (dir, db) = open_temp_db();
                preload(&db);
                (dir, db)
            },
            |(_dir, db)| {
                let mut iter = db
                    .iter(
                        Range {
                            start: Bound::Included(Bytes::from_static(b"k")),
                            end: Bound::Unbounded,
                        },
                        ReadOptions::default(),
                    )
                    .expect("iter");
                iter.seek_to_first();
                for next in iter {
                    let _ = next.expect("next");
                }
            },
            BatchSize::LargeInput,
        );
    });
}

fn benches(c: &mut Criterion) {
    bench_fill(c);
    bench_readrandom(c);
    bench_readrandom_cached(c);
    bench_readseq(c);
}

criterion_group!(layerdb_nvme_benches, benches);
criterion_main!(layerdb_nvme_benches);
