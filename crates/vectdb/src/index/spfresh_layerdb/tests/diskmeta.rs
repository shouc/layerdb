use super::*;

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
fn offheap_diskmeta_ignores_unsafe_fast_path_and_keeps_restart_durability() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let mut cfg = SpFreshLayerDbConfig {
        memory_mode: SpFreshMemoryMode::OffHeapDiskMeta,
        spfresh: crate::index::SpFreshConfig {
            dim: 16,
            initial_postings: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    cfg.write_sync = false;
    cfg.db_options.fsync_writes = false;
    cfg.unsafe_nondurable_fast_path = true;

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(1, vec![0.0; cfg.spfresh.dim])?;
        idx.try_upsert(2, vec![1.0; cfg.spfresh.dim])?;
        assert!(idx.try_delete(1)?);
        idx.close()?;
    }

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 1);
    let got = idx.search(&[1.0; 16], 1);
    assert_eq!(got[0].id, 2);
    Ok(())
}

#[test]
fn acknowledged_commit_mode_round_trip_on_clean_restart() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig {
        write_sync: false,
        db_options: layerdb::DbOptions {
            fsync_writes: false,
            ..Default::default()
        },
        ..Default::default()
    };

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        let rows = vec![
            VectorRecord::new(1, vec![0.1; cfg.spfresh.dim]),
            VectorRecord::new(2, vec![0.2; cfg.spfresh.dim]),
        ];
        assert_eq!(
            idx.try_upsert_batch_with_commit_mode(&rows, MutationCommitMode::Acknowledged)?,
            2
        );
        assert_eq!(
            idx.try_delete_batch_with_commit_mode(&[1], MutationCommitMode::Acknowledged)?,
            1
        );
        idx.close()?;
    }

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 1);
    let got = idx.search(&[0.2; 64], 1);
    assert_eq!(got[0].id, 2);
    Ok(())
}

#[test]
fn acknowledged_commit_mode_persists_without_explicit_close() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig {
        write_sync: false,
        db_options: layerdb::DbOptions {
            fsync_writes: false,
            ..Default::default()
        },
        ..Default::default()
    };

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        let rows = vec![
            VectorRecord::new(10, vec![0.1; cfg.spfresh.dim]),
            VectorRecord::new(11, vec![0.2; cfg.spfresh.dim]),
            VectorRecord::new(12, vec![0.3; cfg.spfresh.dim]),
        ];
        assert_eq!(
            idx.try_upsert_batch_with_commit_mode(&rows, MutationCommitMode::Acknowledged)?,
            rows.len()
        );
        assert_eq!(
            idx.try_delete_batch_with_commit_mode(&[10], MutationCommitMode::Acknowledged)?,
            1
        );
        drop(idx);
    }

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    assert_eq!(idx.len(), 2);
    let got = idx.search(&[0.25; 64], 2);
    assert_eq!(got.len(), 2);
    assert!(got.iter().all(|n| n.id != 10));
    Ok(())
}

#[test]
fn diskmeta_search_fails_closed_when_exact_payloads_are_missing() -> anyhow::Result<()> {
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
    db.delete(
        super::storage::vector_key(generation, 1),
        WriteOptions { sync: true },
    )?;
    db.delete(
        super::storage::vector_key(generation, 2),
        WriteOptions { sync: true },
    )?;
    drop(db);
    fs::remove_dir_all(dir.path().join("vector_blocks"))?;

    let idx = SpFreshLayerDbIndex::open(dir.path(), cfg)?;
    let got = idx.search(&[0.2; 16], 1);
    assert!(
        got.is_empty(),
        "search should fail closed when vectors are missing"
    );
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
fn offheap_diskmeta_same_posting_update_skips_redundant_membership_event() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig {
        memory_mode: SpFreshMemoryMode::OffHeapDiskMeta,
        spfresh: crate::index::SpFreshConfig {
            dim: 8,
            initial_postings: 1,
            nprobe: 1,
            ..Default::default()
        },
        ..Default::default()
    };

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(7, vec![0.1; cfg.spfresh.dim])?;
        idx.try_upsert(7, vec![0.11; cfg.spfresh.dim])?;
        idx.close()?;
    }

    let db = Db::open(dir.path(), cfg.db_options.clone())?;
    let generation = super::storage::ensure_active_generation(&db)?;
    let posting_event_next_seq = super::storage::ensure_posting_event_next_seq(&db)?;
    assert_eq!(
        posting_event_next_seq, 1,
        "same-posting update should not append a second membership event"
    );

    let row_raw = db
        .get(
            super::storage::vector_key(generation, 7),
            layerdb::ReadOptions::default(),
        )?
        .ok_or_else(|| anyhow::anyhow!("vector row missing for id=7"))?;
    let decoded = super::storage::decode_vector_row_with_posting(row_raw.as_ref())?;
    let posting_id = decoded
        .posting_id
        .ok_or_else(|| anyhow::anyhow!("posting id missing for id=7"))?;
    let loaded_members = super::storage::load_posting_members(&db, generation, posting_id)?;
    let ids: Vec<u64> = loaded_members
        .members
        .into_iter()
        .map(|member| member.id)
        .collect();
    assert_eq!(ids, vec![7]);
    assert_eq!(
        loaded_members.scanned_events, 1,
        "expected exactly one posting event for repeated same-posting upserts"
    );
    Ok(())
}
