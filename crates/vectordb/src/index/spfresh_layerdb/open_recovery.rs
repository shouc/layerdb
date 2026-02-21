use super::*;

impl SpFreshLayerDbIndex {
    pub fn open(path: impl AsRef<Path>, mut cfg: SpFreshLayerDbConfig) -> anyhow::Result<Self> {
        validate_config(&cfg)?;
        if cfg.unsafe_nondurable_fast_path {
            eprintln!(
                "spfresh-layerdb: unsafe_nondurable_fast_path is ignored; durable path is always enforced"
            );
            cfg.unsafe_nondurable_fast_path = false;
        }
        let db_path = path.as_ref();
        let db = Db::open(db_path, cfg.db_options.clone()).context("open layerdb for spfresh")?;
        ensure_wal_exists(db_path)?;
        refresh_read_snapshot(&db)?;
        ensure_metadata(&db, &cfg)?;
        Self::open_with_db(db_path, db, cfg)
    }

    pub fn open_existing(path: impl AsRef<Path>, db_options: DbOptions) -> anyhow::Result<Self> {
        let db_path = path.as_ref();
        let db = Db::open(db_path, db_options.clone()).context("open layerdb for spfresh")?;
        ensure_wal_exists(db_path)?;
        refresh_read_snapshot(&db)?;
        let meta = load_metadata(&db)?.ok_or_else(|| {
            anyhow::anyhow!(
                "missing spfresh metadata in {}; initialize with open()",
                db_path.display()
            )
        })?;

        let cfg = SpFreshLayerDbConfig {
            spfresh: SpFreshConfig {
                dim: meta.dim,
                initial_postings: meta.initial_postings,
                split_limit: meta.split_limit,
                merge_limit: meta.merge_limit,
                reassign_range: meta.reassign_range,
                nprobe: meta.nprobe,
                kmeans_iters: meta.kmeans_iters,
            },
            db_options,
            ..Default::default()
        };

        validate_config(&cfg)?;
        ensure_metadata(&db, &cfg)?;
        Self::open_with_db(db_path, db, cfg)
    }

    fn open_with_db(db_path: &Path, db: Db, mut cfg: SpFreshLayerDbConfig) -> anyhow::Result<Self> {
        let generation = ensure_active_generation(&db)?;
        let wal_next_seq = ensure_wal_next_seq(&db)?;
        let posting_event_next_seq = ensure_posting_event_next_seq(&db)?;
        let manifest_epoch = load_startup_manifest_bytes(&db)?
            .and_then(|raw| bincode::deserialize::<PersistedStartupManifest>(&raw).ok())
            .filter(|m| {
                m.schema_version == STARTUP_MANIFEST_SCHEMA_VERSION && m.generation == generation
            })
            .map(|m| m.epoch)
            .unwrap_or(0);
        let (mut index_state, applied_wal_seq) =
            Self::load_or_rebuild_index(&db, &cfg, generation, wal_next_seq)?;
        cfg.memory_mode = match &index_state {
            RuntimeSpFreshIndex::Resident(_) => SpFreshMemoryMode::Resident,
            RuntimeSpFreshIndex::OffHeap(_) => SpFreshMemoryMode::OffHeap,
            RuntimeSpFreshIndex::OffHeapDiskMeta(_) => SpFreshMemoryMode::OffHeapDiskMeta,
        };
        let vector_cache = Arc::new(Mutex::new(VectorCache::new(cfg.offheap_cache_capacity)));
        let vector_blocks = Arc::new(Mutex::new(VectorBlockStore::open(
            db_path,
            cfg.spfresh.dim,
            manifest_epoch,
        )?));
        let posting_members_cache = Arc::new(Mutex::new(PostingMembersCache::new(
            cfg.offheap_posting_cache_entries,
            cfg.posting_delta_compact_interval_ops,
            cfg.posting_delta_compact_budget_entries,
        )));
        let ephemeral_posting_members = Arc::new(Mutex::new(None));
        let ephemeral_row_states = Arc::new(Mutex::new(None));
        let replay_from = applied_wal_seq.map_or(0, |seq| seq.saturating_add(1));
        if replay_from < wal_next_seq {
            if matches!(index_state, RuntimeSpFreshIndex::OffHeapDiskMeta(_)) {
                // WAL v2 for diskmeta only stores new-state payloads. To preserve exact centroid
                // accounting, rebuild from authoritative rows whenever a tail exists.
                let (rows, assignments) = load_rows_with_posting_assignments(&db, generation)?;
                let (rebuilt, _assigned_now) =
                    SpFreshDiskMetaIndex::build_from_rows_with_assignments(
                        cfg.spfresh.clone(),
                        &rows,
                        Some(&assignments),
                    );
                index_state = RuntimeSpFreshIndex::OffHeapDiskMeta(rebuilt);
            } else {
                Self::replay_wal_tail(
                    &db,
                    &vector_cache,
                    &vector_blocks,
                    generation,
                    &mut index_state,
                    replay_from,
                )?;
            }
        }
        let index = Arc::new(RwLock::new(index_state));
        let diskmeta_search_snapshot = Arc::new(ArcSwapOption::empty());
        let initial_snapshot = {
            let guard = lock_read(&index);
            Self::extract_diskmeta_snapshot(&guard)
        };
        if let Some(snapshot) = initial_snapshot {
            diskmeta_search_snapshot.store(Some(snapshot));
        }
        let active_generation = Arc::new(AtomicU64::new(generation));
        let update_gate = Arc::new(RwLock::new(()));
        let dirty_ids = Arc::new(Mutex::new(HashSet::new()));
        let pending_ops = Arc::new(AtomicUsize::new(0));
        let wal_next_seq = Arc::new(AtomicU64::new(wal_next_seq));
        let posting_event_next_seq = Arc::new(AtomicU64::new(posting_event_next_seq));
        let startup_epoch = Arc::new(AtomicU64::new(manifest_epoch));
        let stop_worker = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(SpFreshLayerDbStatsInner::default());
        let (commit_tx, commit_rx) = mpsc::channel::<CommitRequest>();
        let (rebuild_tx, rebuild_rx) = mpsc::channel::<()>();

        let commit_db = db.clone();
        let commit_worker = std::thread::spawn(move || {
            while let Ok(req) = commit_rx.recv() {
                match req {
                    CommitRequest::Write { ops, sync, resp } => {
                        let result = commit_db
                            .write_batch(ops, WriteOptions { sync })
                            .context("spfresh-layerdb commit worker write batch");
                        let _ = resp.send(result);
                    }
                    CommitRequest::Shutdown => break,
                }
            }
        });

        let worker = spawn_rebuilder(
            RebuilderRuntime {
                db: db.clone(),
                rebuild_pending_ops: cfg.rebuild_pending_ops.max(1),
                rebuild_interval: cfg.rebuild_interval,
                active_generation: active_generation.clone(),
                index: index.clone(),
                update_gate: update_gate.clone(),
                dirty_ids: dirty_ids.clone(),
                pending_ops: pending_ops.clone(),
                vector_cache: vector_cache.clone(),
                vector_blocks: vector_blocks.clone(),
                stats: stats.clone(),
                stop_worker: stop_worker.clone(),
            },
            rebuild_rx,
        );

        let out = Self {
            cfg,
            db_path: db_path.to_path_buf(),
            db,
            active_generation,
            index,
            update_gate,
            dirty_ids,
            pending_ops,
            vector_cache,
            vector_blocks,
            posting_members_cache,
            ephemeral_posting_members,
            ephemeral_row_states,
            diskmeta_search_snapshot,
            wal_next_seq,
            posting_event_next_seq,
            startup_epoch,
            commit_tx,
            commit_worker: Some(commit_worker),
            pending_commit_acks: Arc::new(Mutex::new(VecDeque::new())),
            commit_error: Arc::new(Mutex::new(None)),
            max_async_commit_inflight: ASYNC_COMMIT_MAX_INFLIGHT,
            rebuild_tx,
            stop_worker,
            worker: Some(worker),
            stats,
        };
        if out.use_nondurable_fast_path()
            && out.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta
        {
            out.refresh_ephemeral_row_states_from_storage()?;
            out.refresh_ephemeral_posting_members_from_storage()?;
        }
        Ok(out)
    }

    fn load_or_rebuild_index(
        db: &Db,
        cfg: &SpFreshLayerDbConfig,
        generation: u64,
        wal_next_seq: u64,
    ) -> anyhow::Result<(RuntimeSpFreshIndex, Option<u64>)> {
        if let Some(raw) = load_index_checkpoint_bytes(db)? {
            match bincode::deserialize::<PersistedIndexCheckpoint>(raw.as_ref()) {
                Ok(checkpoint)
                    if checkpoint.schema_version
                        == config::META_INDEX_CHECKPOINT_SCHEMA_VERSION
                        && checkpoint.generation == generation =>
                {
                    return Ok((checkpoint.index, checkpoint.applied_wal_seq));
                }
                Ok(_) => {}
                Err(err) => {
                    eprintln!(
                        "spfresh-layerdb checkpoint decode failed, rebuilding index: {err:#}"
                    );
                }
            }
        }

        let applied_wal_seq = wal_next_seq.checked_sub(1);
        let index = match cfg.memory_mode {
            SpFreshMemoryMode::Resident => {
                let rows = load_rows(db, generation)?;
                RuntimeSpFreshIndex::Resident(SpFreshIndex::build(cfg.spfresh.clone(), &rows))
            }
            SpFreshMemoryMode::OffHeap => {
                let rows = load_rows(db, generation)?;
                RuntimeSpFreshIndex::OffHeap(SpFreshOffHeapIndex::build(cfg.spfresh.clone(), &rows))
            }
            SpFreshMemoryMode::OffHeapDiskMeta => {
                let (rows, assignments) = load_rows_with_posting_assignments(db, generation)?;
                let (index, _assigned_now) = SpFreshDiskMetaIndex::build_from_rows_with_assignments(
                    cfg.spfresh.clone(),
                    &rows,
                    Some(&assignments),
                );
                RuntimeSpFreshIndex::OffHeapDiskMeta(index)
            }
        };
        Ok((index, applied_wal_seq))
    }

    pub(super) fn extract_diskmeta_snapshot(
        index: &RuntimeSpFreshIndex,
    ) -> Option<Arc<SpFreshDiskMetaIndex>> {
        match index {
            RuntimeSpFreshIndex::OffHeapDiskMeta(index) => Some(Arc::new(index.clone())),
            _ => None,
        }
    }

    fn replay_wal_tail(
        db: &Db,
        vector_cache: &Arc<Mutex<VectorCache>>,
        vector_blocks: &Arc<Mutex<VectorBlockStore>>,
        generation: u64,
        index: &mut RuntimeSpFreshIndex,
        from_seq: u64,
    ) -> anyhow::Result<()> {
        let entries = load_wal_entries_since(db, from_seq)?;
        if entries.is_empty() {
            return Ok(());
        }

        let mut touched = HashSet::with_capacity(entries.len());
        for entry in entries {
            match entry {
                IndexWalEntry::Touch { id } => {
                    touched.insert(id);
                }
                IndexWalEntry::TouchBatch { ids } => {
                    touched.extend(ids);
                }
            }
        }

        for id in touched {
            let resolved =
                Self::load_vector_for_id(db, vector_cache, vector_blocks, generation, id)?;
            match resolved {
                Some(values) => match index {
                    RuntimeSpFreshIndex::Resident(index) => index.upsert(id, values),
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader = Self::loader_for(
                            db,
                            vector_cache,
                            vector_blocks,
                            generation,
                            Some((id, values.clone())),
                        );
                        index.upsert_with(id, values, &mut loader)?;
                    }
                    RuntimeSpFreshIndex::OffHeapDiskMeta(index) => {
                        let posting = index.choose_posting(&values).unwrap_or_default();
                        index.apply_upsert(None, posting, values);
                    }
                },
                None => match index {
                    RuntimeSpFreshIndex::Resident(index) => {
                        let _ = index.delete(id);
                    }
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader =
                            Self::loader_for(db, vector_cache, vector_blocks, generation, None);
                        let _ = index.delete_with(id, &mut loader)?;
                    }
                    RuntimeSpFreshIndex::OffHeapDiskMeta(index) => {
                        let _ = index.apply_delete(None);
                    }
                },
            }
        }
        Ok(())
    }

    pub(super) fn persist_index_checkpoint(&self) -> anyhow::Result<()> {
        self.flush_pending_commits()?;
        let next_wal_seq = self.wal_next_seq.load(Ordering::Relaxed);
        let snapshot = lock_read(&self.index).clone();
        let generation = self.active_generation.load(Ordering::Relaxed);
        let posting_event_next_seq = self.posting_event_next_seq.load(Ordering::Relaxed);
        let epoch = self.startup_epoch.load(Ordering::Relaxed);
        let checkpoint = PersistedIndexCheckpoint {
            schema_version: config::META_INDEX_CHECKPOINT_SCHEMA_VERSION,
            generation,
            applied_wal_seq: next_wal_seq.checked_sub(1),
            index: snapshot,
        };
        let bytes = bincode::serialize(&checkpoint).context("encode spfresh index checkpoint")?;
        persist_index_checkpoint_bytes(&self.db, bytes, true)?;
        let manifest = PersistedStartupManifest {
            schema_version: STARTUP_MANIFEST_SCHEMA_VERSION,
            generation,
            applied_wal_seq: next_wal_seq.checked_sub(1),
            posting_event_next_seq,
            epoch,
        };
        let manifest_bytes =
            bincode::serialize(&manifest).context("encode spfresh startup manifest")?;
        persist_startup_manifest_bytes(&self.db, manifest_bytes, true)?;
        if let Err(err) = prune_wal_before(&self.db, next_wal_seq, false) {
            eprintln!("spfresh-layerdb wal prune failed: {err:#}");
        }
        Ok(())
    }

    pub(super) fn runtime(&self) -> RebuilderRuntime {
        RebuilderRuntime {
            db: self.db.clone(),
            rebuild_pending_ops: self.cfg.rebuild_pending_ops.max(1),
            rebuild_interval: self.cfg.rebuild_interval,
            active_generation: self.active_generation.clone(),
            index: self.index.clone(),
            update_gate: self.update_gate.clone(),
            dirty_ids: self.dirty_ids.clone(),
            pending_ops: self.pending_ops.clone(),
            vector_cache: self.vector_cache.clone(),
            vector_blocks: self.vector_blocks.clone(),
            stats: self.stats.clone(),
            stop_worker: self.stop_worker.clone(),
        }
    }
}
