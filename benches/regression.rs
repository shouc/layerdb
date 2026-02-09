use std::ops::Bound;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, Rng, SeedableRng};
use tempfile::TempDir;

use layerdb::{Db, DbOptions, Range, ReadOptions, WriteOptions};

const N: u32 = 5_000;

fn options() -> DbOptions {
    DbOptions {
        memtable_shards: 8,
        wal_segment_bytes: 8 * 1024 * 1024,
        memtable_bytes: 8 * 1024 * 1024,
        fsync_writes: false,
        ..Default::default()
    }
}

fn key(i: u32) -> Bytes {
    Bytes::from(format!("k{i:08}"))
}

fn value(i: u32) -> Bytes {
    Bytes::from(format!("v{i:08}"))
}

fn open_temp_db() -> (TempDir, Db) {
    let dir = TempDir::new().expect("tempdir");
    let db = Db::open(dir.path(), options()).expect("open");
    (dir, db)
}

fn preload(db: &Db, n: u32) {
    for i in 0..n {
        db.put(key(i), value(i), WriteOptions { sync: false })
            .expect("put");
    }
}

fn bench_fill(c: &mut Criterion) {
    c.bench_function("fill/5k", |b| {
        b.iter_batched(
            || open_temp_db(),
            |(_dir, db)| preload(&db, N),
            BatchSize::LargeInput,
        );
    });
}

fn bench_readrandom(c: &mut Criterion) {
    c.bench_function("readrandom/5k", |b| {
        b.iter_batched(
            || {
                let (dir, db) = open_temp_db();
                preload(&db, N);

                let mut keys: Vec<u32> = (0..N).collect();
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
    c.bench_function("readseq/5k", |b| {
        b.iter_batched(
            || {
                let (dir, db) = open_temp_db();
                preload(&db, N);
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
                while let Some(next) = iter.next() {
                    let _ = next.expect("next");
                }
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_overwrite(c: &mut Criterion) {
    c.bench_function("overwrite/5k", |b| {
        b.iter_batched(
            || {
                let (dir, db) = open_temp_db();
                preload(&db, N);
                (dir, db)
            },
            |(_dir, db)| {
                for i in 0..N {
                    db.put(key(i), value(i + 100_000), WriteOptions { sync: false })
                        .expect("put");
                }
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_delete_heavy(c: &mut Criterion) {
    c.bench_function("delete-heavy/5k", |b| {
        b.iter_batched(
            || {
                let (dir, db) = open_temp_db();
                preload(&db, N);
                (dir, db)
            },
            |(_dir, db)| {
                let mut rng = StdRng::seed_from_u64(0xdead_beef);
                for _ in 0..N {
                    let i: u32 = rng.gen_range(0..N);
                    db.delete(key(i), WriteOptions { sync: false }).expect("del");
                }
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_scan_heavy(c: &mut Criterion) {
    c.bench_function("scan-heavy/5x", |b| {
        b.iter_batched(
            || {
                let (dir, db) = open_temp_db();
                preload(&db, N * 2);
                (dir, db)
            },
            |(_dir, db)| {
                for scan in 0..5u32 {
                    let start = key(scan * 1_000);
                    let end = key((scan + 1) * 1_000);
                    let mut iter = db
                        .iter(
                            Range {
                                start: Bound::Included(start),
                                end: Bound::Excluded(end),
                            },
                            ReadOptions::default(),
                        )
                        .expect("iter");
                    iter.seek_to_first();
                    while let Some(next) = iter.next() {
                        let _ = next.expect("next");
                    }
                }
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_compact(c: &mut Criterion) {
    c.bench_function("compact/10k", |b| {
        b.iter_batched(
            || {
                let (dir, db) = open_temp_db();
                preload(&db, N * 2);
                (dir, db)
            },
            |(_dir, db)| {
                db.compact_range(None).expect("compact");
            },
            BatchSize::LargeInput,
        );
    });
}

fn benches(c: &mut Criterion) {
    bench_fill(c);
    bench_readrandom(c);
    bench_readseq(c);
    bench_overwrite(c);
    bench_delete_heavy(c);
    bench_scan_heavy(c);
    bench_compact(c);
}

criterion_group!(layerdb_regression_benches, benches);
criterion_main!(layerdb_regression_benches);
