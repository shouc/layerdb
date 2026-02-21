use super::*;

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
fn mixed_mutation_batch_owned_last_write_wins() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();

    {
        let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
        let result = idx.try_apply_batch_owned(vec![
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
fn mixed_mutation_batch_rejects_invalid_upsert_even_if_overwritten() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbConfig::default();

    let mut idx = SpFreshLayerDbIndex::open(dir.path(), cfg.clone())?;
    let bad_then_good = vec![
        VectorMutation::Upsert(VectorRecord::new(1, vec![0.1; cfg.spfresh.dim - 1])),
        VectorMutation::Upsert(VectorRecord::new(1, vec![0.2; cfg.spfresh.dim])),
    ];
    assert!(idx.try_apply_batch(&bad_then_good).is_err());
    assert!(idx.try_apply_batch_owned(bad_then_good).is_err());
    Ok(())
}
