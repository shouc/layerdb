use std::ops::Bound;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use rocksdb::{
    DBCompressionType, IteratorMode, Options as RocksOptions, WriteBatch as RocksWriteBatch,
    WriteOptions as RocksWriteOptions, DB as RocksDb,
};

use layerdb::{Db, DbOptions, Op, Range, ReadOptions, WriteOptions};

#[derive(Parser, Debug)]
#[command(about = "Fair LayerDB vs RocksDB benchmark runner")]
struct Args {
    #[arg(long, default_value_t = 100_000)]
    keys: usize,
    #[arg(long, default_value_t = 128)]
    value_bytes: usize,
    #[arg(long, default_value_t = 3)]
    repeats: usize,
}

#[derive(Clone)]
struct Inputs {
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    values_overwrite: Vec<Vec<u8>>,
    random_order: Vec<usize>,
}

#[derive(Clone, Copy)]
struct Stage {
    name: &'static str,
    op_count: usize,
    layerdb: fn(&Inputs) -> anyhow::Result<Duration>,
    rocksdb: fn(&Inputs) -> anyhow::Result<Duration>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.keys == 0 {
        anyhow::bail!("--keys must be > 0");
    }
    if args.value_bytes < 16 {
        anyhow::bail!("--value-bytes must be >= 16");
    }
    if args.repeats == 0 {
        anyhow::bail!("--repeats must be > 0");
    }

    let inputs = build_inputs(args.keys, args.value_bytes);
    let stages = vec![
        Stage {
            name: "fill",
            op_count: args.keys,
            layerdb: bench_fill_layerdb,
            rocksdb: bench_fill_rocksdb,
        },
        Stage {
            name: "fill-batch32",
            op_count: args.keys,
            layerdb: bench_fill_batch32_layerdb,
            rocksdb: bench_fill_batch32_rocksdb,
        },
        Stage {
            name: "readrandom",
            op_count: args.keys,
            layerdb: bench_readrandom_layerdb,
            rocksdb: bench_readrandom_rocksdb,
        },
        Stage {
            name: "readseq",
            op_count: args.keys,
            layerdb: bench_readseq_layerdb,
            rocksdb: bench_readseq_rocksdb,
        },
        Stage {
            name: "overwrite",
            op_count: args.keys,
            layerdb: bench_overwrite_layerdb,
            rocksdb: bench_overwrite_rocksdb,
        },
        Stage {
            name: "delete-heavy",
            op_count: args.keys,
            layerdb: bench_delete_heavy_layerdb,
            rocksdb: bench_delete_heavy_rocksdb,
        },
        Stage {
            name: "compact",
            op_count: args.keys,
            layerdb: bench_compact_layerdb,
            rocksdb: bench_compact_rocksdb,
        },
    ];

    println!(
        "engine_bench keys={} value_bytes={} repeats={}",
        args.keys, args.value_bytes, args.repeats
    );
    println!(
        "{:<14} {:>14} {:>14} {:>10} {:>14} {:>14}",
        "workload", "layerdb qps", "rocksdb qps", "slower", "layerdb ms", "rocksdb ms"
    );

    for stage in stages {
        let layer = run_repeated(args.repeats, || (stage.layerdb)(&inputs))?;
        let rocks = run_repeated(args.repeats, || (stage.rocksdb)(&inputs))?;
        let layer_median = median(layer);
        let rocks_median = median(rocks);

        let layer_qps = qps(stage.op_count, layer_median);
        let rocks_qps = qps(stage.op_count, rocks_median);
        let slower = if layer_qps == 0.0 {
            f64::INFINITY
        } else {
            rocks_qps / layer_qps
        };

        println!(
            "{:<14} {:>14.0} {:>14.0} {:>10.2} {:>14.1} {:>14.1}",
            stage.name,
            layer_qps,
            rocks_qps,
            slower,
            layer_median.as_secs_f64() * 1_000.0,
            rocks_median.as_secs_f64() * 1_000.0
        );
    }

    Ok(())
}

fn layerdb_options() -> DbOptions {
    DbOptions {
        fsync_writes: false,
        memtable_shards: 16,
        memtable_bytes: 64 * 1024 * 1024,
        wal_segment_bytes: 64 * 1024 * 1024,
        ..Default::default()
    }
}

fn rocksdb_options() -> RocksOptions {
    let mut opts = RocksOptions::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::None);
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_max_write_buffer_number(4);
    opts.set_target_file_size_base(64 * 1024 * 1024);
    opts.increase_parallelism(4);
    opts
}

fn build_inputs(count: usize, value_bytes: usize) -> Inputs {
    let mut keys = Vec::with_capacity(count);
    let mut values = Vec::with_capacity(count);
    let mut values_overwrite = Vec::with_capacity(count);

    for i in 0..count {
        keys.push(format!("k{i:016}").into_bytes());

        let mut value = vec![b'v'; value_bytes];
        let a = format!("{i:016x}");
        value[..16].copy_from_slice(a.as_bytes());
        values.push(value);

        let mut over = vec![b'o'; value_bytes];
        let b = format!("{:016x}", i.saturating_add(count));
        over[..16].copy_from_slice(b.as_bytes());
        values_overwrite.push(over);
    }

    let mut random_order: Vec<usize> = (0..count).collect();
    shuffle(&mut random_order, 0x5eed_f00d_dead_beef);

    Inputs {
        keys,
        values,
        values_overwrite,
        random_order,
    }
}

fn shuffle(values: &mut [usize], mut state: u64) {
    for i in (1..values.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        values.swap(i, j);
    }
}

fn run_repeated<F>(repeats: usize, mut f: F) -> anyhow::Result<Vec<Duration>>
where
    F: FnMut() -> anyhow::Result<Duration>,
{
    let mut out = Vec::with_capacity(repeats);
    for _ in 0..repeats {
        out.push(f()?);
    }
    Ok(out)
}

fn median(mut values: Vec<Duration>) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

fn qps(op_count: usize, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return f64::INFINITY;
    }
    op_count as f64 / elapsed.as_secs_f64()
}

fn preload_layerdb(db: &Db, inputs: &Inputs) -> anyhow::Result<()> {
    for i in 0..inputs.keys.len() {
        db.put(
            Bytes::copy_from_slice(&inputs.keys[i]),
            Bytes::copy_from_slice(&inputs.values[i]),
            WriteOptions { sync: false },
        )?;
    }
    Ok(())
}

fn preload_rocksdb(db: &RocksDb, inputs: &Inputs) -> anyhow::Result<()> {
    for i in 0..inputs.keys.len() {
        db.put(&inputs.keys[i], &inputs.values[i])?;
    }
    Ok(())
}

fn bench_fill_layerdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = Db::open(dir.path(), layerdb_options())?;
    let start = Instant::now();
    preload_layerdb(&db, inputs)?;
    Ok(start.elapsed())
}

fn bench_fill_rocksdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = RocksDb::open(&rocksdb_options(), dir.path())?;
    let start = Instant::now();
    preload_rocksdb(&db, inputs)?;
    Ok(start.elapsed())
}

fn bench_fill_batch32_layerdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = Db::open(dir.path(), layerdb_options())?;

    let start = Instant::now();
    for base in (0..inputs.keys.len()).step_by(32) {
        let end = (base + 32).min(inputs.keys.len());
        let mut batch = Vec::with_capacity(end - base);
        for i in base..end {
            batch.push(Op::put(
                Bytes::copy_from_slice(&inputs.keys[i]),
                Bytes::copy_from_slice(&inputs.values[i]),
            ));
        }
        db.write_batch(batch, WriteOptions { sync: false })?;
    }
    Ok(start.elapsed())
}

fn bench_fill_batch32_rocksdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = RocksDb::open(&rocksdb_options(), dir.path())?;

    let mut write_opts = RocksWriteOptions::default();
    write_opts.disable_wal(false);
    write_opts.set_sync(false);

    let start = Instant::now();
    for base in (0..inputs.keys.len()).step_by(32) {
        let end = (base + 32).min(inputs.keys.len());
        let mut batch = RocksWriteBatch::default();
        for i in base..end {
            batch.put(&inputs.keys[i], &inputs.values[i]);
        }
        db.write_opt(batch, &write_opts)?;
    }
    Ok(start.elapsed())
}

fn bench_readrandom_layerdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = Db::open(dir.path(), layerdb_options())?;
    preload_layerdb(&db, inputs)?;

    let start = Instant::now();
    for &idx in &inputs.random_order {
        let found = db.get(&inputs.keys[idx][..], ReadOptions::default())?;
        anyhow::ensure!(found.is_some(), "missing key {}", idx);
    }
    Ok(start.elapsed())
}

fn bench_readrandom_rocksdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = RocksDb::open(&rocksdb_options(), dir.path())?;
    preload_rocksdb(&db, inputs)?;

    let start = Instant::now();
    for &idx in &inputs.random_order {
        let found = db.get(&inputs.keys[idx])?;
        anyhow::ensure!(found.is_some(), "missing key {}", idx);
    }
    Ok(start.elapsed())
}

fn bench_readseq_layerdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = Db::open(dir.path(), layerdb_options())?;
    preload_layerdb(&db, inputs)?;

    let start = Instant::now();
    let mut iter = db.iter(
        Range {
            start: Bound::Included(Bytes::from_static(b"")),
            end: Bound::Unbounded,
        },
        ReadOptions::default(),
    )?;
    iter.seek_to_first();

    let mut seen = 0usize;
    while let Some(next) = iter.next() {
        let _ = next?;
        seen += 1;
    }
    anyhow::ensure!(seen == inputs.keys.len(), "readseq count mismatch");
    Ok(start.elapsed())
}

fn bench_readseq_rocksdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = RocksDb::open(&rocksdb_options(), dir.path())?;
    preload_rocksdb(&db, inputs)?;

    let start = Instant::now();
    let mut seen = 0usize;
    for item in db.iterator(IteratorMode::Start) {
        let _ = item?;
        seen += 1;
    }
    anyhow::ensure!(seen == inputs.keys.len(), "readseq count mismatch");
    Ok(start.elapsed())
}

fn bench_overwrite_layerdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = Db::open(dir.path(), layerdb_options())?;
    preload_layerdb(&db, inputs)?;

    let start = Instant::now();
    for i in 0..inputs.keys.len() {
        db.put(
            Bytes::copy_from_slice(&inputs.keys[i]),
            Bytes::copy_from_slice(&inputs.values_overwrite[i]),
            WriteOptions { sync: false },
        )?;
    }
    Ok(start.elapsed())
}

fn bench_overwrite_rocksdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = RocksDb::open(&rocksdb_options(), dir.path())?;
    preload_rocksdb(&db, inputs)?;

    let start = Instant::now();
    for i in 0..inputs.keys.len() {
        db.put(&inputs.keys[i], &inputs.values_overwrite[i])?;
    }
    Ok(start.elapsed())
}

fn bench_delete_heavy_layerdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = Db::open(dir.path(), layerdb_options())?;
    preload_layerdb(&db, inputs)?;

    let start = Instant::now();
    for &idx in &inputs.random_order {
        db.delete(Bytes::copy_from_slice(&inputs.keys[idx]), WriteOptions { sync: false })?;
    }
    Ok(start.elapsed())
}

fn bench_delete_heavy_rocksdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = RocksDb::open(&rocksdb_options(), dir.path())?;
    preload_rocksdb(&db, inputs)?;

    let start = Instant::now();
    for &idx in &inputs.random_order {
        db.delete(&inputs.keys[idx])?;
    }
    Ok(start.elapsed())
}

fn bench_compact_layerdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = Db::open(dir.path(), layerdb_options())?;
    preload_layerdb(&db, inputs)?;

    let start = Instant::now();
    db.compact_range(None)?;
    Ok(start.elapsed())
}

fn bench_compact_rocksdb(inputs: &Inputs) -> anyhow::Result<Duration> {
    let dir = tempfile::TempDir::new()?;
    let db = RocksDb::open(&rocksdb_options(), dir.path())?;
    preload_rocksdb(&db, inputs)?;

    let start = Instant::now();
    db.compact_range::<&[u8], &[u8]>(None, None);
    Ok(start.elapsed())
}
