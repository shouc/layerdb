use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow_array::types::Float32Type;
use arrow_array::{
    Array, FixedSizeListArray, Int32Array, Int64Array, RecordBatch, RecordBatchIterator,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use futures::TryStreamExt;
use lancedb::index::vector::IvfFlatIndexBuilder;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::{connect, DistanceType};
use serde::Deserialize;

#[derive(Debug, Parser)]
#[command(name = "bench_lancedb")]
#[command(about = "Run LanceDB benchmark on vectordb exported dataset")]
struct Args {
    #[arg(long)]
    dataset: PathBuf,
    #[arg(long, default_value_t = 10)]
    k: usize,
    #[arg(long, default_value_t = 64)]
    nlist: usize,
    #[arg(long, default_value_t = 8)]
    nprobe: usize,
    #[arg(long, default_value_t = 1)]
    update_batch: usize,
    #[arg(long)]
    db: Option<PathBuf>,
    #[arg(long, default_value = "vectors")]
    table: String,
    #[arg(long)]
    keep_db: bool,
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.k == 0 {
        anyhow::bail!("--k must be > 0");
    }
    if args.update_batch == 0 {
        anyhow::bail!("--update-batch must be > 0");
    }
    if args.nlist == 0 {
        anyhow::bail!("--nlist must be > 0");
    }
    if args.nprobe == 0 {
        anyhow::bail!("--nprobe must be > 0");
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
        temp_db = tempfile::tempdir().context("create temp lancedb dir")?;
        temp_db.path().to_path_buf()
    };
    prepare_db_path(&db_path, args.keep_db)?;

    let db = connect(db_path.to_string_lossy().as_ref())
        .execute()
        .await
        .context("connect lancedb")?;

    let base_rows: Vec<(u64, Vec<f32>)> = dataset
        .base
        .iter()
        .map(|r| (r.id, r.values.clone()))
        .collect();
    let base_data = make_reader(&base_rows, dim)?;

    let build_start = Instant::now();
    let table = db
        .create_table(&args.table, base_data)
        .execute()
        .await
        .with_context(|| format!("create table {}", args.table))?;
    table
        .create_index(
            &["vector"],
            Index::IvfFlat(
                IvfFlatIndexBuilder::default()
                    .num_partitions(args.nlist as u32)
                    .distance_type(DistanceType::L2),
            ),
        )
        .execute()
        .await
        .context("create vector index")?;
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;

    let update_start = Instant::now();
    for chunk in dataset.updates.chunks(args.update_batch) {
        // Merge-insert requires one source row per id in each batch.
        // Keep the last update for duplicate ids within a chunk.
        let mut dedup: HashMap<u64, Vec<f32>> = HashMap::with_capacity(chunk.len());
        for (id, values) in chunk {
            dedup.insert(*id, values.clone());
        }
        let batch_rows: Vec<(u64, Vec<f32>)> = dedup.into_iter().collect();
        let mut merge = table.merge_insert(&["id"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        merge
            .execute(make_reader(&batch_rows, dim)?)
            .await
            .context("merge insert update batch")?;
    }
    let update_s = update_start.elapsed().as_secs_f64().max(1e-9);
    let update_qps = dataset.updates.len() as f64 / update_s;

    let (final_ids, final_vectors) = final_rows_after_updates(&dataset);
    let expected = exact_topk_ids(&final_ids, &final_vectors, &dataset.queries, args.k);

    let search_start = Instant::now();
    let mut recall_sum = 0.0;
    for (i, query) in dataset.queries.iter().enumerate() {
        let results = table
            .query()
            .nearest_to(query.as_slice())?
            .distance_type(DistanceType::L2)
            .nprobes(args.nprobe)
            .limit(args.k)
            .execute()
            .await
            .context("execute lancedb vector query")?
            .try_collect::<Vec<RecordBatch>>()
            .await
            .context("collect lancedb query results")?;
        let got = extract_ids(&results, args.k)?;
        recall_sum += recall_at_k(&got, &expected[i], args.k);
    }
    let search_s = search_start.elapsed().as_secs_f64().max(1e-9);
    let search_qps = dataset.queries.len() as f64 / search_s;

    let output = serde_json::json!({
        "engine": "lancedb-ivf-flat",
        "dataset": args.dataset,
        "db": db_path,
        "table": args.table,
        "dim": dim,
        "base": dataset.base.len(),
        "updates": dataset.updates.len(),
        "queries": dataset.queries.len(),
        "k": args.k,
        "nlist": args.nlist,
        "nprobe": args.nprobe,
        "update_batch": args.update_batch,
        "build_ms": build_ms,
        "update_qps": update_qps,
        "search_qps": search_qps,
        "recall_at_k": recall_sum / dataset.queries.len() as f64,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
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

fn schema(dim: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            true,
        ),
    ]))
}

fn make_reader(
    rows: &[(u64, Vec<f32>)],
    dim: usize,
) -> Result<Box<dyn arrow_array::RecordBatchReader + Send>> {
    let mut ids = Vec::with_capacity(rows.len());
    let mut vectors = Vec::with_capacity(rows.len());
    for (id, values) in rows {
        let id = i64::try_from(*id).with_context(|| format!("id {id} does not fit in i64"))?;
        if values.len() != dim {
            anyhow::bail!(
                "vector dim mismatch in batch: got={}, expected={dim}",
                values.len()
            );
        }
        ids.push(id);
        vectors.push(Some(values.iter().copied().map(Some).collect::<Vec<_>>()));
    }

    let schema = schema(dim);
    let id_array = Arc::new(Int64Array::from(ids));
    let vector_array =
        Arc::new(FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(vectors, dim as i32));
    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, vector_array])
        .context("build batch")?;
    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    Ok(Box::new(reader))
}

fn extract_ids(batches: &[RecordBatch], k: usize) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    for batch in batches {
        let id_idx = batch
            .schema()
            .index_of("id")
            .context("missing id column in query output")?;
        let id_col = batch.column(id_idx);

        if let Some(ids) = id_col.as_any().downcast_ref::<Int64Array>() {
            for i in 0..ids.len() {
                if ids.is_null(i) {
                    continue;
                }
                out.push(ids.value(i) as u64);
                if out.len() >= k {
                    return Ok(out);
                }
            }
            continue;
        }
        if let Some(ids) = id_col.as_any().downcast_ref::<UInt64Array>() {
            for i in 0..ids.len() {
                if ids.is_null(i) {
                    continue;
                }
                out.push(ids.value(i));
                if out.len() >= k {
                    return Ok(out);
                }
            }
            continue;
        }
        if let Some(ids) = id_col.as_any().downcast_ref::<Int32Array>() {
            for i in 0..ids.len() {
                if ids.is_null(i) {
                    continue;
                }
                out.push(ids.value(i) as u64);
                if out.len() >= k {
                    return Ok(out);
                }
            }
            continue;
        }

        anyhow::bail!("unsupported id column type: {:?}", id_col.data_type());
    }
    Ok(out)
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
) -> Vec<Vec<u64>> {
    let k_eff = k.max(1).min(vectors.len());
    let mut out = Vec::with_capacity(queries.len());
    for query in queries {
        let mut pairs: Vec<(u64, f32)> = ids
            .iter()
            .zip(vectors.iter())
            .map(|(id, vector)| (*id, squared_l2(query, vector)))
            .collect();
        if k_eff < pairs.len() {
            pairs.select_nth_unstable_by(k_eff - 1, |a, b| a.1.total_cmp(&b.1));
            pairs.truncate(k_eff);
        }
        pairs.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        out.push(pairs.into_iter().map(|(id, _)| id).collect());
    }
    out
}

fn recall_at_k(got: &[u64], expected: &[u64], k: usize) -> f64 {
    if k == 0 {
        return 0.0;
    }
    let got_set: std::collections::HashSet<u64> = got.iter().take(k).copied().collect();
    let expected_set: std::collections::HashSet<u64> = expected.iter().take(k).copied().collect();
    got_set.intersection(&expected_set).count() as f64 / k as f64
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
