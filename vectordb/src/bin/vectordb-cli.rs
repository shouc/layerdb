use std::collections::HashMap;
use std::fs;
use std::time::Instant;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use vectordb::dataset::{generate_synthetic, SyntheticConfig};
use vectordb::ground_truth::{exact_knn, recall_at_k};
use vectordb::index::{
    AppendOnlyConfig, AppendOnlyIndex, SaqConfig, SaqIndex, SpFreshConfig, SpFreshIndex,
    SpFreshLayerDbConfig, SpFreshLayerDbIndex,
};
use vectordb::types::{VectorIndex, VectorRecord};

#[derive(Debug, Parser)]
#[command(name = "vectordb-cli")]
#[command(about = "VectorDB command line utility")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Version,
    Bench(BenchArgs),
    SaqValidate(SaqValidateArgs),
    DumpDataset(DumpDatasetArgs),
}

#[derive(Debug, Parser)]
struct BenchArgs {
    #[arg(long, default_value_t = 64)]
    dim: usize,
    #[arg(long, default_value_t = 50_000)]
    base: usize,
    #[arg(long, default_value_t = 5_000)]
    updates: usize,
    #[arg(long, default_value_t = 300)]
    queries: usize,
    #[arg(long, default_value_t = 10)]
    k: usize,
    #[arg(long, default_value_t = 0x5EED_1234_ABCD_u64)]
    seed: u64,
    #[arg(long, default_value_t = 128)]
    initial_postings: usize,
    #[arg(long, default_value_t = 8)]
    nprobe: usize,
    #[arg(long, default_value_t = 512)]
    split_limit: usize,
    #[arg(long, default_value_t = 64)]
    merge_limit: usize,
    #[arg(long, default_value_t = 16)]
    reassign_range: usize,
    #[arg(long, default_value_t = 256)]
    saq_total_bits: usize,
    #[arg(long, default_value_t = 64)]
    saq_ivf_clusters: usize,
    #[arg(long, default_value_t = 2000)]
    spfresh_rebuild_pending_ops: usize,
    #[arg(long, default_value_t = 500)]
    spfresh_rebuild_interval_ms: u64,
}

#[derive(Debug, Parser)]
struct SaqValidateArgs {
    #[arg(long, default_value_t = 96)]
    dim: usize,
    #[arg(long, default_value_t = 25_000)]
    base: usize,
    #[arg(long, default_value_t = 500)]
    queries: usize,
    #[arg(long, default_value_t = 10)]
    k: usize,
    #[arg(long, default_value_t = 42_u64)]
    seed: u64,
    #[arg(long, default_value_t = 256)]
    total_bits: usize,
    #[arg(long, default_value_t = 128)]
    ivf_clusters: usize,
    #[arg(long, default_value_t = 12)]
    nprobe: usize,
}

#[derive(Debug, Parser)]
struct DumpDatasetArgs {
    #[arg(long)]
    out: String,
    #[arg(long, default_value_t = 64)]
    dim: usize,
    #[arg(long, default_value_t = 50_000)]
    base: usize,
    #[arg(long, default_value_t = 5_000)]
    updates: usize,
    #[arg(long, default_value_t = 300)]
    queries: usize,
    #[arg(long, default_value_t = 0x5EED_1234_ABCD_u64)]
    seed: u64,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Version => {
            println!("{}", vectordb::VERSION);
        }
        Command::Bench(args) => run_bench(&args)?,
        Command::SaqValidate(args) => run_saq_validate(&args)?,
        Command::DumpDataset(args) => run_dump_dataset(&args)?,
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct EngineStats {
    name: &'static str,
    build_ms: f64,
    update_qps: f64,
    search_qps: f64,
    avg_recall: f64,
}

fn run_bench(args: &BenchArgs) -> Result<()> {
    if args.k == 0 {
        anyhow::bail!("--k must be > 0");
    }
    let data = generate_synthetic(&SyntheticConfig {
        seed: args.seed,
        dim: args.dim,
        base_count: args.base,
        update_count: args.updates,
        query_count: args.queries,
    });

    let exact_rows = final_rows_after_updates(&data.base, &data.updates);
    let exact_answers: Vec<Vec<vectordb::Neighbor>> = data
        .queries
        .iter()
        .map(|q| exact_knn(&exact_rows, q, args.k))
        .collect();

    let mut stats = Vec::new();
    stats.push(bench_spfresh(args, &data, &exact_answers));
    stats.push(bench_spfresh_layerdb(args, &data, &exact_answers)?);
    stats.push(bench_append_only(args, &data, &exact_answers));
    stats.push(bench_saq(args, &data, &exact_answers));
    stats.push(bench_saq_uniform(args, &data, &exact_answers));

    println!(
        "vectordb bench dim={} base={} updates={} queries={} k={}",
        args.dim, args.base, args.updates, args.queries, args.k
    );
    println!(
        "{:<14} {:>10} {:>12} {:>12} {:>12}",
        "engine", "build ms", "update qps", "search qps", "recall@k"
    );
    for row in stats {
        println!(
            "{:<14} {:>10.1} {:>12.0} {:>12.0} {:>12.4}",
            row.name, row.build_ms, row.update_qps, row.search_qps, row.avg_recall
        );
    }

    println!();
    println!("fairness:");
    println!("- same synthetic dataset seed={} for all engines", args.seed);
    println!("- same update stream and same query set for all engines");
    println!("- recall measured against exact KNN after all updates");
    println!("- same nprobe={} and top-k={}", args.nprobe, args.k);

    Ok(())
}

fn run_saq_validate(args: &SaqValidateArgs) -> Result<()> {
    if args.k == 0 {
        anyhow::bail!("--k must be > 0");
    }

    let (base, queries) = generate_anisotropic(args.seed, args.base, args.queries, args.dim);
    let exact_answers: Vec<Vec<vectordb::Neighbor>> = queries
        .iter()
        .map(|q| exact_knn(&base, q, args.k))
        .collect();

    let saq_cfg = SaqConfig {
        dim: args.dim,
        total_bits: args.total_bits,
        ivf_clusters: args.ivf_clusters,
        nprobe: args.nprobe,
        use_joint_dp: true,
        use_variance_permutation: true,
        caq_rounds: 1,
        ..Default::default()
    };
    let uniform_cfg = SaqConfig {
        dim: args.dim,
        total_bits: args.total_bits,
        ivf_clusters: args.ivf_clusters,
        nprobe: args.nprobe,
        use_joint_dp: false,
        use_variance_permutation: false,
        caq_rounds: 0,
        ..Default::default()
    };

    let saq_build_start = Instant::now();
    let saq_index = SaqIndex::build(saq_cfg, &base);
    let saq_build_ms = saq_build_start.elapsed().as_secs_f64() * 1000.0;

    let uniform_build_start = Instant::now();
    let uniform_index = SaqIndex::build(uniform_cfg, &base);
    let uniform_build_ms = uniform_build_start.elapsed().as_secs_f64() * 1000.0;

    let base_vectors: Vec<Vec<f32>> = base.iter().map(|r| r.values.clone()).collect();
    let saq_mse = saq_index.quantization_mse(&base_vectors).unwrap_or(f64::NAN);
    let uniform_mse = uniform_index.quantization_mse(&base_vectors).unwrap_or(f64::NAN);

    let saq_search_start = Instant::now();
    let mut saq_recall = 0.0f64;
    for (i, q) in queries.iter().enumerate() {
        let got = saq_index.search(q, args.k);
        saq_recall += recall_at_k(&got, &exact_answers[i], args.k) as f64;
    }
    let saq_search_qps = queries.len() as f64 / saq_search_start.elapsed().as_secs_f64().max(1e-9);

    let uniform_search_start = Instant::now();
    let mut uniform_recall = 0.0f64;
    for (i, q) in queries.iter().enumerate() {
        let got = uniform_index.search(q, args.k);
        uniform_recall += recall_at_k(&got, &exact_answers[i], args.k) as f64;
    }
    let uniform_search_qps =
        queries.len() as f64 / uniform_search_start.elapsed().as_secs_f64().max(1e-9);

    println!(
        "saq-validate dim={} base={} queries={} k={} total_bits={}",
        args.dim, args.base, args.queries, args.k, args.total_bits
    );
    println!(
        "{:<14} {:>10} {:>10} {:>12} {:>12}",
        "variant", "build ms", "mse", "search qps", "recall@k"
    );
    println!(
        "{:<14} {:>10.1} {:>10.6} {:>12.0} {:>12.4}",
        "saq", saq_build_ms, saq_mse, saq_search_qps, saq_recall / queries.len() as f64
    );
    println!(
        "{:<14} {:>10.1} {:>10.6} {:>12.0} {:>12.4}",
        "uniform", uniform_build_ms, uniform_mse, uniform_search_qps, uniform_recall / queries.len() as f64
    );
    println!();
    println!("validation design:");
    println!("- anisotropic dataset: larger variance in leading dimensions");
    println!("- same IVF cluster count, nprobe, and bit budget for both variants");
    println!("- SAQ variant enables joint DP + variance-aware ordering + CAQ");
    println!("- uniform variant disables those paper-specific components");
    Ok(())
}

fn run_dump_dataset(args: &DumpDatasetArgs) -> Result<()> {
    let data = generate_synthetic(&SyntheticConfig {
        seed: args.seed,
        dim: args.dim,
        base_count: args.base,
        update_count: args.updates,
        query_count: args.queries,
    });
    let encoded = serde_json::to_vec_pretty(&data)?;
    fs::write(&args.out, encoded)?;
    println!(
        "wrote dataset to {} (dim={} base={} updates={} queries={} seed={})",
        args.out, args.dim, args.base, args.updates, args.queries, args.seed
    );
    Ok(())
}

fn final_rows_after_updates(base: &[VectorRecord], updates: &[(u64, Vec<f32>)]) -> Vec<VectorRecord> {
    let mut map: HashMap<u64, VectorRecord> = base.iter().cloned().map(|r| (r.id, r)).collect();
    for (id, v) in updates {
        map.insert(*id, VectorRecord::new(*id, v.clone()));
    }
    map.into_values().collect()
}

fn bench_spfresh(
    args: &BenchArgs,
    data: &vectordb::dataset::SyntheticDataset,
    exact_answers: &[Vec<vectordb::Neighbor>],
) -> EngineStats {
    let cfg = SpFreshConfig {
        dim: args.dim,
        initial_postings: args.initial_postings,
        split_limit: args.split_limit,
        merge_limit: args.merge_limit,
        reassign_range: args.reassign_range,
        nprobe: args.nprobe,
        kmeans_iters: 8,
    };

    let build_start = Instant::now();
    let mut index = SpFreshIndex::build(cfg, &data.base);
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;

    let update_start = Instant::now();
    for (id, v) in &data.updates {
        index.upsert(*id, v.clone());
    }
    let update_s = update_start.elapsed().as_secs_f64().max(1e-9);
    let update_qps = data.updates.len() as f64 / update_s;

    let search_start = Instant::now();
    let mut recall_sum = 0.0f64;
    for (i, q) in data.queries.iter().enumerate() {
        let got = index.search(q, exact_answers[i].len());
        recall_sum += recall_at_k(&got, &exact_answers[i], got.len()) as f64;
    }
    let search_s = search_start.elapsed().as_secs_f64().max(1e-9);
    let search_qps = data.queries.len() as f64 / search_s;

    EngineStats {
        name: "spfresh",
        build_ms,
        update_qps,
        search_qps,
        avg_recall: recall_sum / data.queries.len() as f64,
    }
}

fn bench_append_only(
    args: &BenchArgs,
    data: &vectordb::dataset::SyntheticDataset,
    exact_answers: &[Vec<vectordb::Neighbor>],
) -> EngineStats {
    let cfg = AppendOnlyConfig {
        dim: args.dim,
        initial_postings: args.initial_postings,
        nprobe: args.nprobe,
        kmeans_iters: 8,
    };

    let build_start = Instant::now();
    let mut index = AppendOnlyIndex::build(cfg, &data.base);
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;

    let update_start = Instant::now();
    for (id, v) in &data.updates {
        index.upsert(*id, v.clone());
    }
    let update_s = update_start.elapsed().as_secs_f64().max(1e-9);
    let update_qps = data.updates.len() as f64 / update_s;

    let search_start = Instant::now();
    let mut recall_sum = 0.0f64;
    for (i, q) in data.queries.iter().enumerate() {
        let got = index.search(q, exact_answers[i].len());
        recall_sum += recall_at_k(&got, &exact_answers[i], got.len()) as f64;
    }
    let search_s = search_start.elapsed().as_secs_f64().max(1e-9);
    let search_qps = data.queries.len() as f64 / search_s;

    EngineStats {
        name: "append-only",
        build_ms,
        update_qps,
        search_qps,
        avg_recall: recall_sum / data.queries.len() as f64,
    }
}

fn bench_spfresh_layerdb(
    args: &BenchArgs,
    data: &vectordb::dataset::SyntheticDataset,
    exact_answers: &[Vec<vectordb::Neighbor>],
) -> Result<EngineStats> {
    let sp_cfg = SpFreshConfig {
        dim: args.dim,
        initial_postings: args.initial_postings,
        split_limit: args.split_limit,
        merge_limit: args.merge_limit,
        reassign_range: args.reassign_range,
        nprobe: args.nprobe,
        kmeans_iters: 8,
    };
    let cfg = SpFreshLayerDbConfig {
        spfresh: sp_cfg,
        rebuild_pending_ops: args.spfresh_rebuild_pending_ops.max(1),
        rebuild_interval: Duration::from_millis(args.spfresh_rebuild_interval_ms.max(1)),
        ..Default::default()
    };

    let db_dir = tempfile::TempDir::new()?;

    let build_start = Instant::now();
    let mut index = SpFreshLayerDbIndex::open(db_dir.path(), cfg)?;
    index.try_bulk_load(&data.base)?;
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;

    let update_start = Instant::now();
    for (id, v) in &data.updates {
        index.try_upsert(*id, v.clone())?;
    }
    let update_s = update_start.elapsed().as_secs_f64().max(1e-9);
    let update_qps = data.updates.len() as f64 / update_s;

    index.force_rebuild()?;

    let search_start = Instant::now();
    let mut recall_sum = 0.0f64;
    for (i, q) in data.queries.iter().enumerate() {
        let got = index.search(q, exact_answers[i].len());
        recall_sum += recall_at_k(&got, &exact_answers[i], got.len()) as f64;
    }
    let search_s = search_start.elapsed().as_secs_f64().max(1e-9);
    let search_qps = data.queries.len() as f64 / search_s;

    Ok(EngineStats {
        name: "spfresh-layerdb",
        build_ms,
        update_qps,
        search_qps,
        avg_recall: recall_sum / data.queries.len() as f64,
    })
}

fn bench_saq(
    args: &BenchArgs,
    data: &vectordb::dataset::SyntheticDataset,
    exact_answers: &[Vec<vectordb::Neighbor>],
) -> EngineStats {
    let cfg = SaqConfig {
        dim: args.dim,
        total_bits: args.saq_total_bits,
        ivf_clusters: args.saq_ivf_clusters,
        nprobe: args.nprobe,
        ..Default::default()
    };

    let build_start = Instant::now();
    let mut index = SaqIndex::build(cfg, &data.base);
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;

    let update_start = Instant::now();
    for (id, v) in &data.updates {
        index.upsert(*id, v.clone());
    }
    let update_s = update_start.elapsed().as_secs_f64().max(1e-9);
    let update_qps = data.updates.len() as f64 / update_s;

    let search_start = Instant::now();
    let mut recall_sum = 0.0f64;
    for (i, q) in data.queries.iter().enumerate() {
        let got = index.search(q, exact_answers[i].len());
        recall_sum += recall_at_k(&got, &exact_answers[i], got.len()) as f64;
    }
    let search_s = search_start.elapsed().as_secs_f64().max(1e-9);
    let search_qps = data.queries.len() as f64 / search_s;

    EngineStats {
        name: "saq",
        build_ms,
        update_qps,
        search_qps,
        avg_recall: recall_sum / data.queries.len() as f64,
    }
}

fn bench_saq_uniform(
    args: &BenchArgs,
    data: &vectordb::dataset::SyntheticDataset,
    exact_answers: &[Vec<vectordb::Neighbor>],
) -> EngineStats {
    let cfg = SaqConfig {
        dim: args.dim,
        total_bits: args.saq_total_bits,
        ivf_clusters: args.saq_ivf_clusters,
        nprobe: args.nprobe,
        use_joint_dp: false,
        use_variance_permutation: false,
        caq_rounds: 0,
        ..Default::default()
    };

    let build_start = Instant::now();
    let mut index = SaqIndex::build(cfg, &data.base);
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;

    let update_start = Instant::now();
    for (id, v) in &data.updates {
        index.upsert(*id, v.clone());
    }
    let update_s = update_start.elapsed().as_secs_f64().max(1e-9);
    let update_qps = data.updates.len() as f64 / update_s;

    let search_start = Instant::now();
    let mut recall_sum = 0.0f64;
    for (i, q) in data.queries.iter().enumerate() {
        let got = index.search(q, exact_answers[i].len());
        recall_sum += recall_at_k(&got, &exact_answers[i], got.len()) as f64;
    }
    let search_s = search_start.elapsed().as_secs_f64().max(1e-9);
    let search_qps = data.queries.len() as f64 / search_s;

    EngineStats {
        name: "saq-uniform",
        build_ms,
        update_qps,
        search_qps,
        avg_recall: recall_sum / data.queries.len() as f64,
    }
}

fn generate_anisotropic(
    seed: u64,
    base_count: usize,
    query_count: usize,
    dim: usize,
) -> (Vec<VectorRecord>, Vec<Vec<f32>>) {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    let mut rng = StdRng::seed_from_u64(seed);
    let mut base = Vec::with_capacity(base_count);
    for id in 0..base_count as u64 {
        let mut values = Vec::with_capacity(dim);
        for d in 0..dim {
            let scale = if d < dim / 4 {
                3.0
            } else if d < dim / 2 {
                1.5
            } else {
                0.5
            };
            values.push(rng.gen_range(-1.0..1.0) * scale);
        }
        base.push(VectorRecord::new(id, values));
    }

    let mut queries = Vec::with_capacity(query_count);
    for _ in 0..query_count {
        let mut q = Vec::with_capacity(dim);
        for d in 0..dim {
            let scale = if d < dim / 4 {
                3.0
            } else if d < dim / 2 {
                1.5
            } else {
                0.5
            };
            q.push(rng.gen_range(-1.0..1.0) * scale);
        }
        queries.push(q);
    }
    (base, queries)
}
