use std::collections::HashMap;
use std::time::Instant;

use anyhow::Result;
use clap::{Parser, Subcommand};
use vectordb::dataset::{generate_synthetic, SyntheticConfig};
use vectordb::ground_truth::{exact_knn, recall_at_k};
use vectordb::index::{
    AppendOnlyConfig, AppendOnlyIndex, SaqConfig, SaqIndex, SpFreshConfig, SpFreshIndex,
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
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Version => {
            println!("{}", vectordb::VERSION);
        }
        Command::Bench(args) => run_bench(&args)?,
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
    stats.push(bench_append_only(args, &data, &exact_answers));
    stats.push(bench_saq(args, &data, &exact_answers));

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
    println!();
    println!(
        "note: no public cloneable `pinecore` ANN engine repository was found during setup; \
using append-only partition baseline as pinecore-like reference."
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
