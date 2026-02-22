use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;
use vectdb::ground_truth::recall_at_k;
use vectdb::index::{
    MutationCommitMode, SpFreshLayerDbConfig, SpFreshLayerDbShardedConfig,
    SpFreshLayerDbShardedIndex, SpFreshMemoryMode,
};
use vectdb::types::{VectorIndex, VectorRecord};

#[derive(Debug, Parser)]
#[command(name = "bench_spfresh_sharded")]
#[command(about = "Run sharded SPFresh LayerDB benchmark on vectdb exported dataset")]
struct Args {
    #[arg(long)]
    dataset: PathBuf,
    #[arg(long, default_value_t = 10)]
    k: usize,
    #[arg(long, default_value_t = 4)]
    shards: usize,
    #[arg(long, default_value_t = 64)]
    initial_postings: usize,
    #[arg(long, default_value_t = 8)]
    nprobe: usize,
    #[arg(long, default_value_t = 1)]
    diskmeta_probe_multiplier: usize,
    #[arg(long, default_value_t = 512)]
    split_limit: usize,
    #[arg(long, default_value_t = 64)]
    merge_limit: usize,
    #[arg(long, default_value_t = 16)]
    reassign_range: usize,
    #[arg(long, default_value_t = 2000)]
    rebuild_pending_ops: usize,
    #[arg(long, default_value_t = 500)]
    rebuild_interval_ms: u64,
    #[arg(long)]
    db: Option<PathBuf>,
    #[arg(long)]
    keep_db: bool,
    #[arg(long, default_value_t = false)]
    durable: bool,
    #[arg(long, default_value_t = false)]
    offheap: bool,
    #[arg(long, default_value_t = false)]
    diskmeta: bool,
    #[arg(long, default_value_t = 1024)]
    update_batch: usize,
    #[arg(long, default_value_t = false)]
    exact_shard_prune: bool,
    #[arg(long, default_value_t = 0)]
    warmup_queries: usize,
    #[arg(long, default_value_t = 1)]
    search_runs: usize,
    #[arg(long, default_value_t = false)]
    skip_force_rebuild: bool,
}

#[derive(Debug, Deserialize)]
struct Dataset {
    base: Vec<DatasetRow>,
    updates: Vec<(u64, Vec<f32>)>,
    queries: Vec<Vec<f32>>,
}

#[derive(Debug, Deserialize)]
struct DatasetRow {
    id: u64,
    values: Vec<f32>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.k == 0 {
        anyhow::bail!("--k must be > 0");
    }
    if args.shards == 0 {
        anyhow::bail!("--shards must be > 0");
    }
    if args.nprobe == 0 {
        anyhow::bail!("--nprobe must be > 0");
    }
    if args.diskmeta_probe_multiplier == 0 {
        anyhow::bail!("--diskmeta-probe-multiplier must be > 0");
    }
    if args.offheap && args.diskmeta {
        anyhow::bail!("--offheap and --diskmeta are mutually exclusive");
    }
    if args.update_batch == 0 {
        anyhow::bail!("--update-batch must be > 0");
    }
    if args.search_runs == 0 {
        anyhow::bail!("--search-runs must be > 0");
    }

    let dataset: Dataset = serde_json::from_slice(
        &fs::read(&args.dataset).with_context(|| format!("read {}", args.dataset.display()))?,
    )
    .with_context(|| format!("parse dataset json {}", args.dataset.display()))?;
    if dataset.base.is_empty() {
        anyhow::bail!("base dataset is empty");
    }
    let dim = dataset.base[0].values.len();
    if dim == 0 {
        anyhow::bail!("vector dim must be > 0");
    }
    for row in &dataset.base {
        if row.values.len() != dim {
            anyhow::bail!("base vector dim mismatch for id={}", row.id);
        }
    }
    for (id, vector) in &dataset.updates {
        if vector.len() != dim {
            anyhow::bail!("update vector dim mismatch for id={id}");
        }
    }
    for (idx, query) in dataset.queries.iter().enumerate() {
        if query.len() != dim {
            anyhow::bail!("query dim mismatch at index={idx}");
        }
    }

    let temp_db;
    let db_path = if let Some(path) = args.db.clone() {
        path
    } else {
        temp_db = tempfile::tempdir().context("create temp sharded db dir")?;
        temp_db.path().to_path_buf()
    };
    prepare_db_path(&db_path, args.keep_db)?;

    let mut shard_cfg = SpFreshLayerDbConfig {
        spfresh: vectdb::index::SpFreshConfig {
            dim,
            initial_postings: args.initial_postings,
            split_limit: args.split_limit,
            merge_limit: args.merge_limit,
            reassign_range: args.reassign_range,
            nprobe: args.nprobe,
            diskmeta_probe_multiplier: args.diskmeta_probe_multiplier,
            kmeans_iters: 8,
        },
        rebuild_pending_ops: args.rebuild_pending_ops.max(1),
        rebuild_interval: Duration::from_millis(args.rebuild_interval_ms.max(1)),
        ..Default::default()
    };
    shard_cfg.memory_mode = if args.diskmeta {
        SpFreshMemoryMode::OffHeapDiskMeta
    } else if args.offheap {
        SpFreshMemoryMode::OffHeap
    } else {
        SpFreshMemoryMode::Resident
    };
    let commit_mode = if args.durable {
        MutationCommitMode::Durable
    } else {
        MutationCommitMode::Acknowledged
    };
    if !args.durable {
        shard_cfg.write_sync = false;
        shard_cfg.db_options.fsync_writes = false;
    }
    let memory_mode = shard_cfg.memory_mode;
    let cfg = SpFreshLayerDbShardedConfig {
        shard_count: args.shards,
        shard: shard_cfg,
        exact_shard_prune: args.exact_shard_prune,
    };

    let base_rows: Vec<VectorRecord> = dataset
        .base
        .iter()
        .map(|r| VectorRecord::new(r.id, r.values.clone()))
        .collect();

    let build_start = Instant::now();
    let mut index = SpFreshLayerDbShardedIndex::open(&db_path, cfg)?;
    index.try_bulk_load_owned(base_rows)?;
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;

    let update_start = Instant::now();
    for chunk in dataset.updates.chunks(args.update_batch) {
        let rows: Vec<VectorRecord> = chunk
            .iter()
            .map(|(id, v)| VectorRecord::new(*id, v.clone()))
            .collect();
        let _ = index.try_upsert_batch_owned_with_commit_mode(rows, commit_mode)?;
    }
    let update_s = update_start.elapsed().as_secs_f64().max(1e-9);
    let update_qps = dataset.updates.len() as f64 / update_s;

    let maintenance_start = Instant::now();
    if !args.skip_force_rebuild {
        index.force_rebuild()?;
    }
    let maintenance_ms = maintenance_start.elapsed().as_secs_f64() * 1000.0;

    let (final_ids, final_vectors) = final_rows_after_updates(&dataset);
    let expected = exact_topk_ids(&final_ids, &final_vectors, &dataset.queries, args.k);

    let warmup_queries = args.warmup_queries.min(dataset.queries.len());
    for query in dataset.queries.iter().take(warmup_queries) {
        let _ = index.search(query, args.k);
    }

    let mut search_qps_runs = Vec::with_capacity(args.search_runs);
    let mut recall_runs = Vec::with_capacity(args.search_runs);
    for _run_idx in 0..args.search_runs {
        let search_start = Instant::now();
        let mut recall_sum = 0.0f64;
        for (i, query) in dataset.queries.iter().enumerate() {
            let got = index.search(query, args.k);
            recall_sum += recall_at_k(&got, &expected[i], args.k) as f64;
        }
        let search_s = search_start.elapsed().as_secs_f64().max(1e-9);
        search_qps_runs.push(dataset.queries.len() as f64 / search_s);
        recall_runs.push(recall_sum / dataset.queries.len() as f64);
    }
    let search_qps = median_f64(search_qps_runs.as_slice());
    let recall_at_k = median_f64(recall_runs.as_slice());

    let output = serde_json::json!({
        "engine": "spfresh-layerdb-sharded",
        "dataset": args.dataset,
        "db": db_path,
        "dim": dim,
        "base": dataset.base.len(),
        "updates": dataset.updates.len(),
        "queries": dataset.queries.len(),
        "k": args.k,
        "shards": args.shards,
        "initial_postings": args.initial_postings,
        "nprobe": args.nprobe,
        "diskmeta_probe_multiplier": args.diskmeta_probe_multiplier,
        "split_limit": args.split_limit,
        "merge_limit": args.merge_limit,
        "reassign_range": args.reassign_range,
        "durable": args.durable,
        "offheap": args.offheap,
        "diskmeta": args.diskmeta,
        "update_batch": args.update_batch,
        "warmup_queries": warmup_queries,
        "search_runs": args.search_runs,
        "search_qps_runs": search_qps_runs,
        "recall_at_k_runs": recall_runs,
        "skip_force_rebuild": args.skip_force_rebuild,
        "maintenance_ms": maintenance_ms,
        "exact_shard_prune": args.exact_shard_prune,
        "memory_mode": format!("{:?}", memory_mode),
        "build_ms": build_ms,
        "update_qps": update_qps,
        "search_qps": search_qps,
        "recall_at_k": recall_at_k,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);

    index.close()?;
    Ok(())
}

fn prepare_db_path(path: &Path, keep_db: bool) -> Result<()> {
    if keep_db {
        fs::create_dir_all(path).with_context(|| format!("create db dir {}", path.display()))?;
        return Ok(());
    }
    if path.exists() {
        fs::remove_dir_all(path).with_context(|| format!("remove db dir {}", path.display()))?;
    }
    fs::create_dir_all(path).with_context(|| format!("create db dir {}", path.display()))?;
    Ok(())
}

fn final_rows_after_updates(dataset: &Dataset) -> (Vec<u64>, Vec<Vec<f32>>) {
    let mut rows: HashMap<u64, Vec<f32>> = dataset
        .base
        .iter()
        .map(|r| (r.id, r.values.clone()))
        .collect();
    for (id, vec) in &dataset.updates {
        rows.insert(*id, vec.clone());
    }
    let mut ids: Vec<u64> = rows.keys().copied().collect();
    ids.sort_unstable();
    let vectors = ids
        .iter()
        .map(|id| rows.get(id).cloned().expect("id exists"))
        .collect();
    (ids, vectors)
}

fn exact_topk_ids(
    ids: &[u64],
    vectors: &[Vec<f32>],
    queries: &[Vec<f32>],
    k: usize,
) -> Vec<Vec<vectdb::Neighbor>> {
    let mut out = Vec::with_capacity(queries.len());
    for query in queries {
        let mut pairs: Vec<vectdb::Neighbor> = ids
            .iter()
            .zip(vectors.iter())
            .map(|(id, vector)| vectdb::Neighbor {
                id: *id,
                distance: squared_l2(query, vector),
            })
            .collect();
        pairs.sort_by(|a, b| {
            a.distance
                .total_cmp(&b.distance)
                .then_with(|| a.id.cmp(&b.id))
        });
        if pairs.len() > k {
            pairs.truncate(k);
        }
        out.push(pairs);
    }
    out
}

fn squared_l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = x - y;
            d * d
        })
        .sum()
}

fn median_f64(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        (sorted[mid - 1] + sorted[mid]) * 0.5
    }
}
