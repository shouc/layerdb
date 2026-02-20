use std::collections::HashMap;
use std::fs;

use layerdb::{Db, DbOptions, Op, WriteOptions};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

use crate::types::{VectorIndex, VectorRecord};

use super::{SpFreshLayerDbConfig, SpFreshLayerDbIndex, SpFreshMemoryMode, VectorMutation};

fn minio_enabled() -> bool {
    matches!(
        std::env::var("LAYERDB_MINIO_INTEGRATION").ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

fn assert_index_matches_model(
    idx: &SpFreshLayerDbIndex,
    expected: &HashMap<u64, Vec<f32>>,
) -> anyhow::Result<()> {
    assert_eq!(idx.len(), expected.len());
    if expected.is_empty() {
        return Ok(());
    }

    let k = expected.len();
    for (id, vector) in expected {
        let got = idx.search(vector, k);
        anyhow::ensure!(
            got.iter().any(|n| n.id == *id),
            "expected id={} missing from search results",
            id
        );
    }
    Ok(())
}

#[test]
fn persists_and_recovers_vectors() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.upsert(1, vec![0.0; cfg.spfresh.dim]);
        idx.upsert(2, vec![1.0; cfg.spfresh.dim]);
        idx.delete(1);
        idx.force_rebuild()?;
    }

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 1);
    let got = idx.search(&vec![1.0; 64], 1);
    assert_eq!(got[0].id, 2);
    Ok(())
}

#[test]
fn offheap_persists_and_recovers_vectors() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig {
        memory_mode: SpFreshMemoryMode::OffHeap,
        spfresh: crate::index::SpFreshConfig {
            dim: 16,
            ..Default::default()
        },
        ..Default::default()
    };

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        assert_eq!(idx.memory_mode(), SpFreshMemoryMode::OffHeap);
        idx.try_upsert(1, vec![0.0; cfg.spfresh.dim])?;
        idx.try_upsert(2, vec![1.0; cfg.spfresh.dim])?;
        assert!(idx.try_delete(1)?);
        idx.force_rebuild()?;
        idx.close()?;
    }

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    assert_eq!(idx.memory_mode(), SpFreshMemoryMode::OffHeap);
    assert_eq!(idx.len(), 1);
    let got = idx.search(&vec![1.0; cfg.spfresh.dim], 1);
    assert_eq!(got[0].id, 2);

    let idx_existing = SpFreshLayerDbIndex::open_existing(dir.path(), cfg.db_options.clone())?;
    assert_eq!(idx_existing.memory_mode(), SpFreshMemoryMode::OffHeap);
    let got_existing = idx_existing.search(&vec![1.0; cfg.spfresh.dim], 1);
    assert_eq!(got_existing[0].id, 2);
    Ok(())
}

#[test]
fn wal_persists_indexed_vectors_across_restart() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.upsert(11, vec![0.25; cfg.spfresh.dim]);
        idx.upsert(12, vec![0.5; cfg.spfresh.dim]);
    }

    let wal_dir = dir.path().join("wal");
    assert!(wal_dir.is_dir());
    let has_segment = fs::read_dir(&wal_dir)?
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .any(|name| name.starts_with("wal_") && name.ends_with(".log"));
    assert!(has_segment, "expected wal segment in {}", wal_dir.display());

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 2);
    let got = idx.search(&vec![0.5; 64], 1);
    assert_eq!(got[0].id, 12);
    Ok(())
}

#[test]
fn rejects_invalid_config() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let mut cfg = SpFreshLayerDbConfig::default();
    cfg.spfresh.split_limit = 8;
    cfg.spfresh.merge_limit = 8;
    let err = match SpFreshLayerDbIndex::open(dir.path(), cfg) {
        Ok(_) => anyhow::bail!("expected config validation error"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("split_limit"));
    Ok(())
}

#[test]
fn rejects_mismatched_persisted_config() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.upsert(1, vec![0.1; cfg.spfresh.dim]);
        idx.close()?;
    }

    let mut different = cfg.clone();
    different.spfresh.nprobe = cfg.spfresh.nprobe + 1;
    let err = match SpFreshLayerDbIndex::open(dir.path(), different) {
        Ok(_) => anyhow::bail!("expected metadata mismatch"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("config mismatch"));
    Ok(())
}

#[test]
fn stats_track_operations() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    idx.try_upsert(1, vec![0.0; cfg.spfresh.dim])?;
    idx.try_upsert(2, vec![1.0; cfg.spfresh.dim])?;
    assert!(idx.try_delete(1)?);
    idx.force_rebuild()?;

    let stats = idx.stats();
    assert_eq!(stats.total_upserts, 2);
    assert_eq!(stats.total_deletes, 1);
    assert_eq!(stats.persist_errors, 0);
    assert!(stats.rebuild_successes >= 1);
    assert_eq!(stats.rebuild_failures, 0);
    assert_eq!(stats.last_rebuild_rows, 1);
    Ok(())
}

#[test]
fn bulk_load_replaces_existing_dataset() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    idx.try_upsert(1, vec![0.0; cfg.spfresh.dim])?;
    idx.try_upsert(2, vec![1.0; cfg.spfresh.dim])?;
    idx.try_bulk_load(&[VectorRecord::new(42, vec![0.42; cfg.spfresh.dim])])?;
    idx.close()?;

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 1);
    let got = idx.search(&vec![0.42; 64], 1);
    assert_eq!(got[0].id, 42);
    Ok(())
}

#[test]
fn health_check_reports_stats_and_integrity() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    idx.try_upsert(7, vec![0.7; cfg.spfresh.dim])?;
    let health = idx.health_check()?;
    assert_eq!(health.total_upserts, 1);
    assert_eq!(health.persist_errors, 0);
    Ok(())
}

#[test]
fn open_existing_uses_persisted_metadata() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(9, vec![0.9; cfg.spfresh.dim])?;
        idx.close()?;
    }

    let idx = SpFreshLayerDbIndex::open_existing(dir.path(), DbOptions::default())?;
    assert_eq!(idx.len(), 1);
    let got = idx.search(&vec![0.9; 64], 1);
    assert_eq!(got[0].id, 9);
    Ok(())
}

#[test]
fn startup_can_boot_from_checkpoint_without_vector_rows() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(1, vec![0.1; cfg.spfresh.dim])?;
        idx.try_upsert(2, vec![0.2; cfg.spfresh.dim])?;
        idx.close()?;
    }

    let db = Db::open(dir.path(), cfg.db_options.clone())?;
    let generation = super::storage::ensure_active_generation(&db)?;
    let prefix = super::storage::vector_prefix(generation);
    let prefix_bytes = prefix.into_bytes();
    let end = super::storage::prefix_exclusive_end(&prefix_bytes)?;
    db.delete_range(prefix_bytes, end, WriteOptions { sync: true })?;

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 2);
    let got = idx.search(&vec![0.2; 64], 2);
    assert!(got.iter().any(|n| n.id == 1));
    assert!(got.iter().any(|n| n.id == 2));
    Ok(())
}

#[test]
fn startup_replays_wal_tail_after_checkpoint() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(1, vec![0.1; cfg.spfresh.dim])?;
        idx.close()?;
    }

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(2, vec![0.2; cfg.spfresh.dim])?;
    }

    let db = Db::open(dir.path(), cfg.db_options.clone())?;
    let generation = super::storage::ensure_active_generation(&db)?;
    db.delete(
        super::storage::vector_key(generation, 1),
        WriteOptions { sync: true },
    )?;

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 2);
    let got = idx.search(&vec![0.2; 64], 2);
    assert!(got.iter().any(|n| n.id == 1));
    assert!(got.iter().any(|n| n.id == 2));
    Ok(())
}

#[test]
fn startup_replays_typed_wal_tail_without_vector_rows() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(1, vec![0.1; cfg.spfresh.dim])?;
        idx.close()?;
    }

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(2, vec![0.2; cfg.spfresh.dim])?;
    }

    let db = Db::open(dir.path(), cfg.db_options.clone())?;
    let generation = super::storage::ensure_active_generation(&db)?;
    db.delete(
        super::storage::vector_key(generation, 2),
        WriteOptions { sync: true },
    )?;

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 2);
    let got = idx.search(&vec![0.2; 64], 2);
    assert!(got.iter().any(|n| n.id == 1));
    assert!(got.iter().any(|n| n.id == 2));
    Ok(())
}

#[test]
fn offheap_diskmeta_replays_wal_tail_without_row_rebuild() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig {
        memory_mode: SpFreshMemoryMode::OffHeapDiskMeta,
        spfresh: crate::index::SpFreshConfig {
            dim: 16,
            initial_postings: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(1, vec![0.1; cfg.spfresh.dim])?;
        idx.close()?;
    }

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(2, vec![0.2; cfg.spfresh.dim])?;
    }

    let db = Db::open(dir.path(), cfg.db_options.clone())?;
    let generation = super::storage::ensure_active_generation(&db)?;
    let prefix = super::storage::vector_prefix(generation);
    let prefix_bytes = prefix.into_bytes();
    let end = super::storage::prefix_exclusive_end(&prefix_bytes)?;
    db.delete_range(prefix_bytes, end, WriteOptions { sync: true })?;

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.memory_mode(), SpFreshMemoryMode::OffHeapDiskMeta);
    assert_eq!(idx.len(), 2);
    let got = idx.search(&[0.2; 16], 2);
    assert!(got.iter().any(|n| n.id == 1));
    assert!(got.iter().any(|n| n.id == 2));
    Ok(())
}

#[test]
fn offheap_diskmeta_rebuilds_without_posting_map_keys() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig {
        memory_mode: SpFreshMemoryMode::OffHeapDiskMeta,
        spfresh: crate::index::SpFreshConfig {
            dim: 16,
            initial_postings: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(1, vec![0.1; cfg.spfresh.dim])?;
        idx.try_upsert(2, vec![0.2; cfg.spfresh.dim])?;
        idx.try_upsert(3, vec![0.3; cfg.spfresh.dim])?;
        idx.close()?;
    }

    let db = Db::open(dir.path(), cfg.db_options.clone())?;
    db.delete(super::config::META_INDEX_CHECKPOINT_KEY, WriteOptions { sync: true })?;
    let generation = super::storage::ensure_active_generation(&db)?;
    let prefix = super::storage::posting_map_prefix(generation);
    let prefix_bytes = prefix.into_bytes();
    let end = super::storage::prefix_exclusive_end(&prefix_bytes)?;
    db.delete_range(prefix_bytes, end, WriteOptions { sync: true })?;

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.memory_mode(), SpFreshMemoryMode::OffHeapDiskMeta);
    assert_eq!(idx.len(), 3);
    let got = idx.search(&[0.3; 16], 3);
    assert!(got.iter().any(|n| n.id == 1));
    assert!(got.iter().any(|n| n.id == 2));
    assert!(got.iter().any(|n| n.id == 3));
    Ok(())
}

#[test]
fn posting_metadata_helpers_round_trip() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    {
        let idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.close()?;
    }

    let db = Db::open(dir.path(), cfg.db_options.clone())?;
    let generation = super::storage::ensure_active_generation(&db)?;
    db.write_batch(
        vec![
            Op::put(
                super::storage::posting_map_key(generation, 11),
                super::storage::posting_assignment_value(7)?,
            ),
            Op::put(
                super::storage::posting_map_key(generation, 12),
                super::storage::posting_assignment_value(7)?,
            ),
            Op::put(
                super::storage::posting_member_key(generation, 7, 11),
                super::storage::posting_member_value(11, &vec![0.11; cfg.spfresh.dim])?,
            ),
            Op::put(
                super::storage::posting_member_key(generation, 7, 12),
                super::storage::posting_member_value(12, &vec![0.12; cfg.spfresh.dim])?,
            ),
            Op::put(
                super::storage::posting_member_key(generation, 7, 13),
                bincode::serialize(&13u64)?,
            ),
        ],
        WriteOptions { sync: true },
    )?;

    assert_eq!(
        super::storage::load_posting_assignment(&db, generation, 11)?,
        Some(7)
    );
    assert_eq!(
        super::storage::load_posting_assignment(&db, generation, 999)?,
        None
    );

    let mut posting_members = super::storage::load_posting_members(&db, generation, 7)?;
    posting_members.sort_by_key(|entry| entry.id);
    assert_eq!(posting_members.len(), 3);
    let members: Vec<u64> = posting_members.iter().map(|entry| entry.id).collect();
    assert_eq!(members, vec![11, 12, 13]);
    assert_eq!(posting_members[0].id, 11);
    assert_eq!(
        posting_members[0].values.as_ref().map(Vec::len),
        Some(cfg.spfresh.dim)
    );
    assert_eq!(posting_members[1].id, 12);
    assert_eq!(
        posting_members[1].values.as_ref().map(Vec::len),
        Some(cfg.spfresh.dim)
    );
    assert_eq!(posting_members[2].id, 13);
    assert!(posting_members[2].values.is_none());

    let map_prefix = super::storage::posting_map_prefix(generation).into_bytes();
    let map_end = super::storage::prefix_exclusive_end(&map_prefix)?;
    db.delete_range(map_prefix, map_end, WriteOptions { sync: true })?;
    assert_eq!(
        super::storage::load_posting_assignment(&db, generation, 11)?,
        None
    );
    Ok(())
}

#[test]
fn posting_member_residual_round_trip_omits_values_payload() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    {
        let idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.close()?;
    }

    let db = Db::open(dir.path(), cfg.db_options.clone())?;
    let generation = super::storage::ensure_active_generation(&db)?;
    let centroid = vec![0.25f32; cfg.spfresh.dim];
    let vector = vec![0.75f32; cfg.spfresh.dim];
    db.write_batch(
        vec![Op::put(
            super::storage::posting_member_key(generation, 5, 42),
            super::storage::posting_member_value_with_residual_only(42, &vector, &centroid)?,
        )],
        WriteOptions { sync: true },
    )?;

    let members = super::storage::load_posting_members(&db, generation, 5)?;
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].id, 42);
    assert!(members[0].values.is_none());
    assert!(members[0].residual_scale.is_some());
    assert_eq!(
        members[0].residual_code.as_ref().map(Vec::len),
        Some(cfg.spfresh.dim)
    );
    Ok(())
}

#[test]
fn randomized_restarts_preserve_model_state() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let mut cfg = SpFreshLayerDbConfig::default();
    cfg.spfresh.dim = 16;
    cfg.spfresh.initial_postings = 8;

    let mut rng = StdRng::seed_from_u64(0x5EED_BEEF);
    let mut expected: HashMap<u64, Vec<f32>> = HashMap::new();
    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;

    let total_ops = 2_000usize;
    let restart_every = 125usize;
    for step in 1..=total_ops {
        let id = rng.gen_range(0..500) as u64;
        if rng.gen_bool(0.25) {
            let model_deleted = expected.remove(&id).is_some();
            let deleted = idx.try_delete(id)?;
            assert_eq!(deleted, model_deleted);
        } else {
            let vector: Vec<f32> = (0..cfg.spfresh.dim).map(|_| rng.gen::<f32>()).collect();
            idx.try_upsert(id, vector.clone())?;
            expected.insert(id, vector);
        }

        if step % restart_every == 0 {
            if rng.gen_bool(0.5) {
                idx.close()?;
            } else {
                drop(idx);
            }
            idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
            assert_index_matches_model(&idx, &expected)?;
        }
    }

    idx.close()?;
    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_index_matches_model(&idx, &expected)?;
    Ok(())
}

#[test]
fn offheap_randomized_restarts_preserve_model_state() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig {
        memory_mode: SpFreshMemoryMode::OffHeap,
        spfresh: crate::index::SpFreshConfig {
            dim: 16,
            initial_postings: 8,
            ..Default::default()
        },
        ..Default::default()
    };

    let mut rng = StdRng::seed_from_u64(0x000A_11CE_BEEF);
    let mut expected: HashMap<u64, Vec<f32>> = HashMap::new();
    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;

    let total_ops = 1_000usize;
    let restart_every = 80usize;
    for step in 1..=total_ops {
        let id = rng.gen_range(0..400) as u64;
        if rng.gen_bool(0.25) {
            let model_deleted = expected.remove(&id).is_some();
            let deleted = idx.try_delete(id)?;
            assert_eq!(deleted, model_deleted);
        } else {
            let vector: Vec<f32> = (0..cfg.spfresh.dim).map(|_| rng.gen::<f32>()).collect();
            idx.try_upsert(id, vector.clone())?;
            expected.insert(id, vector);
        }

        if step % restart_every == 0 {
            idx.close()?;
            idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
            assert_eq!(idx.memory_mode(), SpFreshMemoryMode::OffHeap);
            assert_index_matches_model(&idx, &expected)?;
        }
    }

    idx.close()?;
    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.memory_mode(), SpFreshMemoryMode::OffHeap);
    assert_index_matches_model(&idx, &expected)?;
    Ok(())
}

#[test]
fn offheap_diskmeta_persists_and_recovers_vectors() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig {
        memory_mode: SpFreshMemoryMode::OffHeapDiskMeta,
        spfresh: crate::index::SpFreshConfig {
            dim: 16,
            initial_postings: 4,
            ..Default::default()
        },
        ..Default::default()
    };

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        assert_eq!(idx.memory_mode(), SpFreshMemoryMode::OffHeapDiskMeta);
        idx.try_upsert(1, vec![0.0; cfg.spfresh.dim])?;
        idx.try_upsert(2, vec![1.0; cfg.spfresh.dim])?;
        assert!(idx.try_delete(1)?);
        idx.close()?;
    }

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    assert_eq!(idx.memory_mode(), SpFreshMemoryMode::OffHeapDiskMeta);
    assert_eq!(idx.len(), 1);
    let got = idx.search(&vec![1.0; cfg.spfresh.dim], 1);
    assert_eq!(got[0].id, 2);

    let idx_existing = SpFreshLayerDbIndex::open_existing(dir.path(), cfg.db_options.clone())?;
    assert_eq!(
        idx_existing.memory_mode(),
        SpFreshMemoryMode::OffHeapDiskMeta
    );
    let got_existing = idx_existing.search(&vec![1.0; cfg.spfresh.dim], 1);
    assert_eq!(got_existing[0].id, 2);
    Ok(())
}

#[test]
fn offheap_diskmeta_bulk_load_populates_metadata() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig {
        memory_mode: SpFreshMemoryMode::OffHeapDiskMeta,
        spfresh: crate::index::SpFreshConfig {
            dim: 8,
            initial_postings: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let rows = vec![
        VectorRecord::new(10, vec![0.1; cfg.spfresh.dim]),
        VectorRecord::new(11, vec![0.2; cfg.spfresh.dim]),
        VectorRecord::new(12, vec![0.3; cfg.spfresh.dim]),
    ];

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_bulk_load(&rows)?;
        idx.close()?;
    }

    let db = Db::open(dir.path(), cfg.db_options.clone())?;
    let generation = super::storage::ensure_active_generation(&db)?;
    for row in rows {
        let mut posting = super::storage::load_posting_assignment(&db, generation, row.id)?;
        if posting.is_none() {
            let raw = db.get(
                super::storage::vector_key(generation, row.id),
                layerdb::ReadOptions::default(),
            )?;
            if let Some(raw) = raw {
                let decoded = super::storage::decode_vector_row_with_posting(raw.as_ref())?;
                posting = decoded.posting_id;
            }
        }
        assert!(
            posting.is_some(),
            "missing posting assignment (map+row) for id={}",
            row.id
        );
    }
    Ok(())
}

#[test]
fn upsert_batch_last_write_wins_and_recovers() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        let upserts = vec![
            VectorRecord::new(1, vec![0.1; cfg.spfresh.dim]),
            VectorRecord::new(2, vec![0.2; cfg.spfresh.dim]),
            VectorRecord::new(1, vec![0.3; cfg.spfresh.dim]),
        ];
        assert_eq!(idx.try_upsert_batch(&upserts)?, 2);
        idx.close()?;
    }

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 2);
    let got = idx.search(&vec![0.3; 64], 1);
    assert_eq!(got[0].id, 1);
    Ok(())
}

#[test]
fn offheap_diskmeta_batch_upsert_delete_round_trip() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig {
        memory_mode: SpFreshMemoryMode::OffHeapDiskMeta,
        spfresh: crate::index::SpFreshConfig {
            dim: 16,
            initial_postings: 4,
            ..Default::default()
        },
        ..Default::default()
    };

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        let upserts = vec![
            VectorRecord::new(1, vec![0.1; cfg.spfresh.dim]),
            VectorRecord::new(2, vec![0.2; cfg.spfresh.dim]),
            VectorRecord::new(3, vec![0.3; cfg.spfresh.dim]),
        ];
        assert_eq!(idx.try_upsert_batch(&upserts)?, 3);
        assert_eq!(idx.try_delete_batch(&[2, 2, 99])?, 1);
        idx.close()?;
    }

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 2);
    let got = idx.search(&[0.3; 16], 2);
    assert!(got.iter().any(|n| n.id == 1));
    assert!(got.iter().any(|n| n.id == 3));
    assert!(got.iter().all(|n| n.id != 2));
    Ok(())
}

#[test]
fn mixed_mutation_batch_last_write_wins() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        let result = idx.try_apply_batch(&[
            VectorMutation::Upsert(VectorRecord::new(1, vec![0.1; cfg.spfresh.dim])),
            VectorMutation::Upsert(VectorRecord::new(2, vec![0.2; cfg.spfresh.dim])),
            VectorMutation::Delete { id: 1 },
            VectorMutation::Upsert(VectorRecord::new(1, vec![0.15; cfg.spfresh.dim])),
            VectorMutation::Delete { id: 2 },
        ])?;
        assert_eq!(result.upserts, 1);
        assert_eq!(result.deletes, 0);
        idx.close()?;
    }

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 1);
    let got = idx.search(&vec![0.15; 64], 1);
    assert_eq!(got[0].id, 1);
    Ok(())
}

#[test]
fn s3_sync_and_thaw_round_trip() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    idx.try_upsert(7, vec![0.7; cfg.spfresh.dim])?;
    idx.try_upsert(9, vec![0.9; cfg.spfresh.dim])?;

    let moved = idx.sync_to_s3(None)?;
    assert!(moved >= 1, "expected at least one file frozen to s3 tier");
    assert!(!idx.frozen_objects().is_empty());

    let got_frozen = idx.search(&vec![0.9; cfg.spfresh.dim], 1);
    assert_eq!(got_frozen[0].id, 9);

    let thawed = idx.thaw_from_s3(None)?;
    assert!(
        thawed >= 1,
        "expected at least one file thawed from s3 tier"
    );

    let got_thawed = idx.search(&vec![0.9; cfg.spfresh.dim], 1);
    assert_eq!(got_thawed[0].id, 9);
    Ok(())
}

#[test]
fn s3_gc_removes_local_emulation_orphans() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    idx.try_upsert(1, vec![0.1; cfg.spfresh.dim])?;
    let moved = idx.sync_to_s3(None)?;
    assert!(moved >= 1);

    let orphan_root = dir
        .path()
        .join("sst_s3")
        .join("L1")
        .join("L1-deadbeef00000001");
    fs::create_dir_all(&orphan_root)?;
    let orphan_meta = orphan_root.join("meta.bin");
    let orphan_sb = orphan_root.join("sb_00000000.bin");
    fs::write(&orphan_meta, b"orphan-meta")?;
    fs::write(&orphan_sb, b"orphan-sb")?;
    assert!(orphan_meta.exists());
    assert!(orphan_sb.exists());

    let removed = idx.gc_orphaned_s3()?;
    assert!(removed >= 1);
    assert!(!orphan_meta.exists());
    assert!(!orphan_sb.exists());
    Ok(())
}

#[test]
fn s3_sync_round_trip_against_minio_when_enabled() -> anyhow::Result<()> {
    if !minio_enabled() {
        eprintln!("skipping minio integration test (set LAYERDB_MINIO_INTEGRATION=1)");
        return Ok(());
    }

    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();
    if cfg.db_options.s3.is_none() {
        anyhow::bail!("minio integration requires LAYERDB_S3_* env vars");
    }

    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    idx.try_upsert(5, vec![0.5; cfg.spfresh.dim])?;
    idx.try_upsert(8, vec![0.8; cfg.spfresh.dim])?;

    let moved = idx.sync_to_s3(None)?;
    assert!(moved >= 1);
    assert!(!idx.frozen_objects().is_empty());

    let got_frozen = idx.search(&vec![0.8; cfg.spfresh.dim], 1);
    assert_eq!(got_frozen[0].id, 8);

    let thawed = idx.thaw_from_s3(None)?;
    assert!(thawed >= 1);

    let got_thawed = idx.search(&vec![0.8; cfg.spfresh.dim], 1);
    assert_eq!(got_thawed[0].id, 8);
    Ok(())
}
