use super::*;

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
fn startup_manifest_persists_with_checkpoint() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(10, vec![0.10; cfg.spfresh.dim])?;
        idx.close()?;
    }

    let db = Db::open(dir.path(), cfg.db_options.clone())?;
    let raw = db
        .get(
            super::config::META_STARTUP_MANIFEST_KEY,
            layerdb::ReadOptions::default(),
        )?
        .ok_or_else(|| anyhow::anyhow!("startup manifest missing"))?;
    let manifest: super::PersistedStartupManifest = super::decode_startup_manifest(raw.as_ref())?;
    assert_eq!(
        manifest.schema_version,
        super::STARTUP_MANIFEST_SCHEMA_VERSION
    );
    assert_eq!(
        manifest.generation,
        super::storage::ensure_active_generation(&db)?
    );
    assert_eq!(
        manifest.posting_event_next_seq,
        super::storage::ensure_posting_event_next_seq(&db)?
    );
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
fn push_neighbor_topk_matches_naive_selection() {
    let candidates = vec![
        crate::Neighbor {
            id: 10,
            distance: 0.5,
        },
        crate::Neighbor {
            id: 7,
            distance: 0.2,
        },
        crate::Neighbor {
            id: 3,
            distance: 0.2,
        },
        crate::Neighbor {
            id: 8,
            distance: 0.7,
        },
        crate::Neighbor {
            id: 2,
            distance: 0.1,
        },
        crate::Neighbor {
            id: 11,
            distance: 0.3,
        },
    ];

    let k = 3usize;
    let mut top = Vec::new();
    let mut worst_idx = None;
    for candidate in candidates.clone() {
        SpFreshLayerDbIndex::push_neighbor_topk(&mut top, &mut worst_idx, candidate, k);
    }
    top.sort_by(SpFreshLayerDbIndex::neighbor_cmp);

    let mut expected = candidates;
    expected.sort_by(SpFreshLayerDbIndex::neighbor_cmp);
    expected.truncate(k);

    assert_eq!(top.len(), expected.len());
    assert_eq!(
        top.iter().map(|n| n.id).collect::<Vec<_>>(),
        expected.iter().map(|n| n.id).collect::<Vec<_>>()
    );
    for (got, want) in top.iter().zip(expected.iter()) {
        assert!((got.distance - want.distance).abs() <= f32::EPSILON);
    }
}
