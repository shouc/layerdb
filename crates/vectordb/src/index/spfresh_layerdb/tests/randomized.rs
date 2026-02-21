use super::*;

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
