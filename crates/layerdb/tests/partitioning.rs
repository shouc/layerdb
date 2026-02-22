use std::collections::BTreeMap;

use bytes::Bytes;
use layerdb::{Db, DbOptions, Op, Range, ReadOptions, WriteOptions};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkloadResult {
    snapshot_before_mutations: Vec<Option<Bytes>>,
    snapshot_mid_mutations: Vec<Option<Bytes>>,
    latest_before_compaction: Vec<Option<Bytes>>,
    latest_after_compaction: Vec<Option<Bytes>>,
    latest_after_reopen: Vec<Option<Bytes>>,
    iter_after_reopen: Vec<(Bytes, Bytes)>,
}

fn test_options(memtable_shards: usize) -> DbOptions {
    DbOptions {
        memtable_shards,
        wal_segment_bytes: 16 * 1024,
        memtable_bytes: 2 * 1024,
        fsync_writes: true,
        ..Default::default()
    }
}

fn key_set(count: usize) -> Vec<Bytes> {
    (0..count)
        .map(|idx| Bytes::from(format!("k{idx:03}")))
        .collect()
}

fn collect_values(
    db: &Db,
    keys: &[Bytes],
    snapshot: Option<layerdb::SnapshotId>,
) -> anyhow::Result<Vec<Option<Bytes>>> {
    db.multi_get(keys, ReadOptions { snapshot })
}

fn collect_iter_live(db: &Db) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
    let mut iter = db.iter(Range::all(), ReadOptions::default())?;
    iter.seek_to_first();
    let mut out = Vec::new();
    for next in iter {
        let (key, value) = next?;
        if let Some(value) = value {
            out.push((key, value));
        }
    }
    Ok(out)
}

fn expected_live_pairs(keys: &[Bytes], values: &[Option<Bytes>]) -> Vec<(Bytes, Bytes)> {
    keys.iter()
        .cloned()
        .zip(values.iter().cloned())
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .collect()
}

fn model_values(model: &BTreeMap<Bytes, Bytes>, keys: &[Bytes]) -> Vec<Option<Bytes>> {
    keys.iter().map(|key| model.get(key).cloned()).collect()
}

fn model_delete_range(model: &mut BTreeMap<Bytes, Bytes>, start: &Bytes, end: &Bytes) {
    let to_remove: Vec<Bytes> = model
        .range(start.clone()..end.clone())
        .map(|(key, _)| key.clone())
        .collect();
    for key in to_remove {
        model.remove(&key);
    }
}

fn run_semantic_workload(memtable_shards: usize) -> anyhow::Result<WorkloadResult> {
    let dir = TempDir::new()?;
    let db = Db::open(dir.path(), test_options(memtable_shards))?;
    let keys = key_set(256);

    let initial: Vec<Op> = keys
        .iter()
        .enumerate()
        .map(|(idx, key)| Op::put(key.clone(), format!("v{idx:03}")))
        .collect();
    db.write_batch(initial, WriteOptions { sync: true })?;
    let snapshot_before_mutations = db.create_snapshot()?;

    db.write_batch(
        vec![
            Op::put(keys[10].clone(), "overwrite-10-a"),
            Op::put(keys[20].clone(), "overwrite-20"),
            Op::delete(keys[10].clone()),
            Op::put(keys[10].clone(), "overwrite-10-final"),
            Op::delete(keys[200].clone()),
        ],
        WriteOptions { sync: true },
    )?;

    let updates: Vec<Op> = (0..keys.len())
        .step_by(3)
        .map(|idx| Op::put(keys[idx].clone(), format!("u{idx:03}")))
        .collect();
    db.write_batch(updates, WriteOptions { sync: true })?;

    let deletes: Vec<Op> = (0..keys.len())
        .step_by(5)
        .map(|idx| Op::delete(keys[idx].clone()))
        .collect();
    db.write_batch(deletes, WriteOptions { sync: true })?;

    db.write_batch(
        vec![
            Op::delete_range("k080", "k160"),
            Op::put("k155", "late-155"),
            Op::delete_range("k030", "k040"),
            Op::put("k035", "resurrect-35"),
        ],
        WriteOptions { sync: true },
    )?;

    let snapshot_mid_mutations = db.create_snapshot()?;

    let final_updates: Vec<Op> = (0..keys.len())
        .step_by(7)
        .map(|idx| Op::put(keys[idx].clone(), format!("f{idx:03}")))
        .collect();
    db.write_batch(final_updates, WriteOptions { sync: true })?;
    db.write_batch(
        vec![
            Op::delete_range("k150", "k180"),
            Op::put("k170", "revive-170"),
        ],
        WriteOptions { sync: true },
    )?;

    let snapshot_before_values = collect_values(&db, &keys, Some(snapshot_before_mutations))?;
    let snapshot_mid_values = collect_values(&db, &keys, Some(snapshot_mid_mutations))?;
    let latest_before_compaction = collect_values(&db, &keys, None)?;

    db.compact_range(None)?;

    let snapshot_before_after_compaction =
        collect_values(&db, &keys, Some(snapshot_before_mutations))?;
    let snapshot_mid_after_compaction = collect_values(&db, &keys, Some(snapshot_mid_mutations))?;
    let latest_after_compaction = collect_values(&db, &keys, None)?;

    db.release_snapshot(snapshot_before_mutations);
    db.release_snapshot(snapshot_mid_mutations);

    assert_eq!(snapshot_before_values, snapshot_before_after_compaction);
    assert_eq!(snapshot_mid_values, snapshot_mid_after_compaction);
    assert_eq!(latest_before_compaction, latest_after_compaction);

    drop(db);

    let reopened = Db::open(dir.path(), test_options(memtable_shards))?;
    let latest_after_reopen = collect_values(&reopened, &keys, None)?;
    let iter_after_reopen = collect_iter_live(&reopened)?;
    assert_eq!(latest_before_compaction, latest_after_reopen);
    assert_eq!(
        iter_after_reopen,
        expected_live_pairs(&keys, &latest_after_reopen)
    );

    Ok(WorkloadResult {
        snapshot_before_mutations: snapshot_before_values,
        snapshot_mid_mutations: snapshot_mid_values,
        latest_before_compaction,
        latest_after_compaction,
        latest_after_reopen,
        iter_after_reopen,
    })
}

fn run_randomized_workload(memtable_shards: usize, seed: u64) -> anyhow::Result<WorkloadResult> {
    let dir = TempDir::new()?;
    let mut options = test_options(memtable_shards);
    options.wal_segment_bytes = 3 * 1024;
    options.memtable_bytes = 768;
    let db = Db::open(dir.path(), options.clone())?;

    let keys = key_set(96);
    let mut model: BTreeMap<Bytes, Bytes> = BTreeMap::new();
    let mut snapshots: Vec<(layerdb::SnapshotId, BTreeMap<Bytes, Bytes>)> = Vec::new();
    let mut rng = StdRng::seed_from_u64(seed);
    for step in 0..480usize {
        let action = rng.gen_range(0..100usize);
        if action < 48 {
            let idx = rng.gen_range(0..keys.len());
            let key = keys[idx].clone();
            let value = Bytes::from(format!("p{step:03}-{idx:02}"));
            db.put(key.clone(), value.clone(), WriteOptions { sync: true })?;
            model.insert(key, value);
        } else if action < 68 {
            let idx = rng.gen_range(0..keys.len());
            let key = keys[idx].clone();
            db.delete(key.clone(), WriteOptions { sync: true })?;
            model.remove(&key);
        } else if action < 80 {
            let start = rng.gen_range(0..keys.len().saturating_sub(1));
            let end = rng.gen_range(start + 1..keys.len());
            let start_key = keys[start].clone();
            let end_key = keys[end].clone();
            db.delete_range(
                start_key.clone(),
                end_key.clone(),
                WriteOptions { sync: true },
            )?;
            model_delete_range(&mut model, &start_key, &end_key);
        } else {
            let batch_len = rng.gen_range(2..10usize);
            let mut batch = Vec::with_capacity(batch_len);
            for local in 0..batch_len {
                let kind = rng.gen_range(0..10usize);
                if kind < 5 {
                    let idx = rng.gen_range(0..keys.len());
                    let key = keys[idx].clone();
                    let value = Bytes::from(format!("b{step:03}-{local:02}-{idx:02}"));
                    batch.push(Op::put(key.clone(), value.clone()));
                    model.insert(key, value);
                } else if kind < 8 {
                    let idx = rng.gen_range(0..keys.len());
                    let key = keys[idx].clone();
                    batch.push(Op::delete(key.clone()));
                    model.remove(&key);
                } else {
                    let start = rng.gen_range(0..keys.len().saturating_sub(1));
                    let end = rng.gen_range(start + 1..keys.len());
                    let start_key = keys[start].clone();
                    let end_key = keys[end].clone();
                    batch.push(Op::delete_range(start_key.clone(), end_key.clone()));
                    model_delete_range(&mut model, &start_key, &end_key);
                }
            }
            db.write_batch(batch, WriteOptions { sync: true })?;
        }

        if step % 39 == 0 {
            let snapshot = db.create_snapshot()?;
            snapshots.push((snapshot, model.clone()));
            if snapshots.len() > 4 {
                let (oldest, _) = snapshots.remove(0);
                db.release_snapshot(oldest);
            }
        }
        if step % 27 == 0 {
            let latest = collect_values(&db, &keys, None)?;
            assert_eq!(latest, model_values(&model, &keys));
            for (snapshot, snapshot_model) in &snapshots {
                let at_snapshot = collect_values(&db, &keys, Some(*snapshot))?;
                assert_eq!(at_snapshot, model_values(snapshot_model, &keys));
            }
        }
    }

    let snapshot_before_mutations = snapshots
        .first()
        .map(|(_, m)| model_values(m, &keys))
        .unwrap_or_else(|| vec![None; keys.len()]);
    let snapshot_mid_mutations = snapshots
        .last()
        .map(|(_, m)| model_values(m, &keys))
        .unwrap_or_else(|| vec![None; keys.len()]);
    let latest_before_compaction = collect_values(&db, &keys, None)?;
    assert_eq!(latest_before_compaction, model_values(&model, &keys));

    db.compact_range(None)?;
    let latest_after_compaction = collect_values(&db, &keys, None)?;
    assert_eq!(latest_after_compaction, model_values(&model, &keys));
    for (snapshot, snapshot_model) in &snapshots {
        let at_snapshot = collect_values(&db, &keys, Some(*snapshot))?;
        assert_eq!(at_snapshot, model_values(snapshot_model, &keys));
    }
    for (snapshot, _) in snapshots.drain(..) {
        db.release_snapshot(snapshot);
    }

    drop(db);
    let reopened = Db::open(dir.path(), options)?;
    let latest_after_reopen = collect_values(&reopened, &keys, None)?;
    assert_eq!(latest_after_reopen, model_values(&model, &keys));
    let iter_after_reopen = collect_iter_live(&reopened)?;
    assert_eq!(
        iter_after_reopen,
        expected_live_pairs(&keys, &latest_after_reopen)
    );

    Ok(WorkloadResult {
        snapshot_before_mutations,
        snapshot_mid_mutations,
        latest_before_compaction,
        latest_after_compaction,
        latest_after_reopen,
        iter_after_reopen,
    })
}

fn run_rotation_workload(memtable_shards: usize) -> anyhow::Result<Vec<Option<Bytes>>> {
    let dir = TempDir::new()?;
    let mut options = test_options(memtable_shards);
    options.wal_segment_bytes = 2 * 1024;
    options.memtable_bytes = 768;

    let db = Db::open(dir.path(), options.clone())?;
    let keys = key_set(512);

    for start in (0..keys.len()).step_by(64) {
        let end = (start + 64).min(keys.len());
        let mut batch = Vec::new();
        for (offset, key) in keys[start..end].iter().enumerate() {
            let idx = start + offset;
            batch.push(Op::put(key.clone(), format!("seed-{idx:03}")));
        }
        db.write_batch(batch, WriteOptions { sync: true })?;
    }

    let deletes: Vec<Op> = (0..keys.len())
        .step_by(4)
        .map(|idx| Op::delete(keys[idx].clone()))
        .collect();
    db.write_batch(deletes, WriteOptions { sync: true })?;

    let updates: Vec<Op> = (0..keys.len())
        .step_by(9)
        .map(|idx| Op::put(keys[idx].clone(), format!("final-{idx:03}")))
        .collect();
    db.write_batch(updates, WriteOptions { sync: true })?;
    db.compact_range(None)?;
    drop(db);

    let reopened = Db::open(dir.path(), options)?;
    collect_values(&reopened, &keys, None)
}

#[test]
fn partitioned_memtables_match_single_shard_semantics() -> anyhow::Result<()> {
    let baseline = run_semantic_workload(1)?;
    for shard_count in [0usize, 2, 3, 7, 16] {
        let candidate = run_semantic_workload(shard_count)?;
        assert_eq!(
            candidate, baseline,
            "partitioned workload mismatch for memtable_shards={shard_count}"
        );
    }
    Ok(())
}

#[test]
fn zero_memtable_shards_matches_single_shard() -> anyhow::Result<()> {
    let zero = run_rotation_workload(0)?;
    let one = run_rotation_workload(1)?;
    assert_eq!(zero, one);
    Ok(())
}

#[test]
fn partitioned_memtables_survive_rotation_compaction_and_restart() -> anyhow::Result<()> {
    let baseline = run_rotation_workload(1)?;
    for shard_count in [4usize, 13] {
        let candidate = run_rotation_workload(shard_count)?;
        assert_eq!(
            candidate, baseline,
            "rotation/recovery mismatch for memtable_shards={shard_count}"
        );
    }
    Ok(())
}

#[test]
fn partitioned_memtables_randomized_semantics_match_single_shard() -> anyhow::Result<()> {
    let baseline = run_randomized_workload(1, 0x55AA_1234)?;
    for shard_count in [2usize, 5, 31] {
        let candidate = run_randomized_workload(shard_count, 0x55AA_1234)?;
        assert_eq!(
            candidate, baseline,
            "randomized partitioning mismatch for memtable_shards={shard_count}"
        );
    }
    Ok(())
}

#[test]
fn partitioned_memtables_randomized_semantics_match_single_shard_multiple_seeds(
) -> anyhow::Result<()> {
    for seed in [0x1020_3040u64, 0x0BAD_F00Du64] {
        let baseline = run_randomized_workload(1, seed)?;
        let candidate = run_randomized_workload(64, seed)?;
        assert_eq!(
            candidate, baseline,
            "randomized mismatch for large memtable_shards at seed={seed}"
        );
    }
    Ok(())
}

#[test]
fn partitioned_memtables_with_more_shards_than_keys_match_single_shard() -> anyhow::Result<()> {
    let baseline = run_semantic_workload(1)?;
    let candidate = run_semantic_workload(512)?;
    assert_eq!(candidate, baseline);
    Ok(())
}
