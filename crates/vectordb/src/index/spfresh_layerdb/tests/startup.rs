use super::*;

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
fn offheap_diskmeta_wal_tail_rebuilds_from_rows() -> anyhow::Result<()> {
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

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.memory_mode(), SpFreshMemoryMode::OffHeapDiskMeta);
    assert_eq!(idx.len(), 2);
    let got = idx.search(&[0.2; 16], 2);
    assert!(got.iter().any(|n| n.id == 1));
    assert!(got.iter().any(|n| n.id == 2));
    Ok(())
}

#[test]
fn offheap_diskmeta_search_uses_vector_blocks_when_rows_missing() -> anyhow::Result<()> {
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
        idx.close()?;
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
    db.delete(
        super::config::META_INDEX_CHECKPOINT_KEY,
        WriteOptions { sync: true },
    )?;
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
                super::storage::posting_member_event_key(generation, 7, 1, 11),
                super::storage::posting_member_event_upsert_value_with_residual(
                    11,
                    &vec![0.11; cfg.spfresh.dim],
                    &vec![0.0; cfg.spfresh.dim],
                )?,
            ),
            Op::put(
                super::storage::posting_member_event_key(generation, 7, 2, 12),
                super::storage::posting_member_event_upsert_value_with_residual(
                    12,
                    &vec![0.12; cfg.spfresh.dim],
                    &vec![0.0; cfg.spfresh.dim],
                )?,
            ),
            Op::put(
                super::storage::posting_member_event_key(generation, 7, 3, 13),
                super::storage::posting_member_event_upsert_value_with_residual(
                    13,
                    &vec![0.13; cfg.spfresh.dim],
                    &vec![0.0; cfg.spfresh.dim],
                )?,
            ),
            Op::put(
                super::storage::posting_member_event_key(generation, 7, 4, 13),
                super::storage::posting_member_event_tombstone_value(13)?,
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

    let loaded = super::storage::load_posting_members(&db, generation, 7)?;
    assert_eq!(loaded.scanned_events, 4);
    let mut posting_members = loaded.members;
    posting_members.sort_by_key(|entry| entry.id);
    posting_members.dedup_by_key(|entry| entry.id);
    posting_members.shrink_to_fit();
    let members: Vec<u64> = posting_members.iter().map(|entry| entry.id).collect();
    assert_eq!(members, vec![11, 12]);
    assert_eq!(posting_members.len(), 2);
    assert_eq!(posting_members[0].id, 11);
    assert_eq!(
        posting_members[0].residual_code.as_ref().map(Vec::len),
        Some(cfg.spfresh.dim)
    );
    assert_eq!(posting_members[1].id, 12);
    assert_eq!(
        posting_members[1].residual_code.as_ref().map(Vec::len),
        Some(cfg.spfresh.dim)
    );

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
            super::storage::posting_member_event_key(generation, 5, 1, 42),
            super::storage::posting_member_event_upsert_value_with_residual(
                42, &vector, &centroid,
            )?,
        )],
        WriteOptions { sync: true },
    )?;

    let members = super::storage::load_posting_members(&db, generation, 5)?.members;
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].id, 42);
    assert!(members[0].residual_scale.is_some());
    assert_eq!(
        members[0].residual_code.as_ref().map(Vec::len),
        Some(cfg.spfresh.dim)
    );
    Ok(())
}
