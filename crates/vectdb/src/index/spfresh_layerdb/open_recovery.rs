use super::*;

const STARTUP_MANIFEST_BIN_TAG: &[u8] = b"smf1";
const STARTUP_MANIFEST_FLAG_HAS_APPLIED_WAL_SEQ: u8 = 1 << 0;
const STARTUP_MANIFEST_CRC_SIZE: usize = 4;
const STARTUP_MANIFEST_BIN_ENCODED_LEN: usize =
    STARTUP_MANIFEST_BIN_TAG.len() + 4 + 8 + 1 + 8 + 8 + 8 + 4;
const INDEX_CHECKPOINT_FRAME_TAG: &[u8] = b"icp1";
const INDEX_CHECKPOINT_FRAME_HEADER_SIZE: usize = INDEX_CHECKPOINT_FRAME_TAG.len() + 4 + 4;

fn read_manifest_u8(raw: &[u8], cursor: &mut usize) -> anyhow::Result<u8> {
    let Some(byte) = raw.get(*cursor).copied() else {
        anyhow::bail!("startup manifest decode underflow for u8");
    };
    *cursor = cursor.saturating_add(1);
    Ok(byte)
}

fn read_manifest_u32(raw: &[u8], cursor: &mut usize) -> anyhow::Result<u32> {
    let end = cursor.saturating_add(4);
    let Some(bytes) = raw.get(*cursor..end) else {
        anyhow::bail!("startup manifest decode underflow for u32");
    };
    let mut arr = [0u8; 4];
    arr.copy_from_slice(bytes);
    *cursor = end;
    Ok(u32::from_le_bytes(arr))
}

fn read_manifest_u64(raw: &[u8], cursor: &mut usize) -> anyhow::Result<u64> {
    let end = cursor.saturating_add(8);
    let Some(bytes) = raw.get(*cursor..end) else {
        anyhow::bail!("startup manifest decode underflow for u64");
    };
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    *cursor = end;
    Ok(u64::from_le_bytes(arr))
}

fn encode_startup_manifest(manifest: &PersistedStartupManifest) -> Vec<u8> {
    let mut out = Vec::with_capacity(STARTUP_MANIFEST_BIN_ENCODED_LEN);
    out.extend_from_slice(STARTUP_MANIFEST_BIN_TAG);
    out.extend_from_slice(&manifest.schema_version.to_le_bytes());
    out.extend_from_slice(&manifest.generation.to_le_bytes());
    let mut flags = 0u8;
    if manifest.applied_wal_seq.is_some() {
        flags |= STARTUP_MANIFEST_FLAG_HAS_APPLIED_WAL_SEQ;
    }
    out.push(flags);
    out.extend_from_slice(&manifest.applied_wal_seq.unwrap_or_default().to_le_bytes());
    out.extend_from_slice(&manifest.posting_event_next_seq.to_le_bytes());
    out.extend_from_slice(&manifest.epoch.to_le_bytes());
    let payload_start = STARTUP_MANIFEST_BIN_TAG.len();
    let checksum = crc32fast::hash(&out[payload_start..]);
    out.extend_from_slice(&checksum.to_le_bytes());
    out
}

pub(super) fn decode_startup_manifest(raw: &[u8]) -> anyhow::Result<PersistedStartupManifest> {
    if !raw.starts_with(STARTUP_MANIFEST_BIN_TAG) {
        anyhow::bail!("unsupported startup manifest tag");
    }
    if raw.len() != STARTUP_MANIFEST_BIN_ENCODED_LEN {
        anyhow::bail!(
            "invalid startup manifest length: {} (expected {})",
            raw.len(),
            STARTUP_MANIFEST_BIN_ENCODED_LEN
        );
    }
    let payload_start = STARTUP_MANIFEST_BIN_TAG.len();
    let checksum_offset = raw.len().saturating_sub(STARTUP_MANIFEST_CRC_SIZE);
    let expected = u32::from_le_bytes(
        raw[checksum_offset..]
            .try_into()
            .map_err(|_| anyhow::anyhow!("startup manifest checksum parse failed"))?,
    );
    let actual = crc32fast::hash(&raw[payload_start..checksum_offset]);
    if expected != actual {
        anyhow::bail!(
            "startup manifest checksum mismatch: expected={expected:#010x} actual={actual:#010x}"
        );
    }
    let mut cursor = payload_start;
    let schema_version = read_manifest_u32(raw, &mut cursor)?;
    let generation = read_manifest_u64(raw, &mut cursor)?;
    let flags = read_manifest_u8(raw, &mut cursor)?;
    let unknown_flags = flags & !STARTUP_MANIFEST_FLAG_HAS_APPLIED_WAL_SEQ;
    if unknown_flags != 0 {
        anyhow::bail!("startup manifest unknown flags: {unknown_flags:#x}");
    }
    let applied_raw = read_manifest_u64(raw, &mut cursor)?;
    let posting_event_next_seq = read_manifest_u64(raw, &mut cursor)?;
    let epoch = read_manifest_u64(raw, &mut cursor)?;
    if cursor != checksum_offset {
        anyhow::bail!("startup manifest payload length mismatch");
    }
    let applied_wal_seq = if (flags & STARTUP_MANIFEST_FLAG_HAS_APPLIED_WAL_SEQ) != 0 {
        Some(applied_raw)
    } else {
        None
    };
    Ok(PersistedStartupManifest {
        schema_version,
        generation,
        applied_wal_seq,
        posting_event_next_seq,
        epoch,
    })
}

fn encode_checkpoint_frame(payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    let payload_len = u32::try_from(payload.len()).context("index checkpoint payload too large")?;
    let checksum = crc32fast::hash(payload);
    let mut out = Vec::with_capacity(INDEX_CHECKPOINT_FRAME_HEADER_SIZE + payload.len());
    out.extend_from_slice(INDEX_CHECKPOINT_FRAME_TAG);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

fn decode_checkpoint_frame(raw: &[u8]) -> anyhow::Result<&[u8]> {
    if !raw.starts_with(INDEX_CHECKPOINT_FRAME_TAG) {
        anyhow::bail!("unsupported index checkpoint frame tag");
    }
    if raw.len() < INDEX_CHECKPOINT_FRAME_HEADER_SIZE {
        anyhow::bail!("index checkpoint frame underflow");
    }
    let mut len_arr = [0u8; 4];
    len_arr.copy_from_slice(
        raw.get(INDEX_CHECKPOINT_FRAME_TAG.len()..INDEX_CHECKPOINT_FRAME_TAG.len() + 4)
            .ok_or_else(|| anyhow::anyhow!("index checkpoint frame missing length"))?,
    );
    let payload_len =
        usize::try_from(u32::from_le_bytes(len_arr)).context("index checkpoint length overflow")?;
    let mut checksum_arr = [0u8; 4];
    checksum_arr.copy_from_slice(
        raw.get(INDEX_CHECKPOINT_FRAME_TAG.len() + 4..INDEX_CHECKPOINT_FRAME_HEADER_SIZE)
            .ok_or_else(|| anyhow::anyhow!("index checkpoint frame missing checksum"))?,
    );
    let expected_checksum = u32::from_le_bytes(checksum_arr);
    let payload_end = INDEX_CHECKPOINT_FRAME_HEADER_SIZE
        .checked_add(payload_len)
        .ok_or_else(|| anyhow::anyhow!("index checkpoint frame payload length overflow"))?;
    if payload_end != raw.len() {
        anyhow::bail!(
            "index checkpoint frame length mismatch: payload_len={} total_len={}",
            payload_len,
            raw.len()
        );
    }
    let payload = raw
        .get(INDEX_CHECKPOINT_FRAME_HEADER_SIZE..payload_end)
        .ok_or_else(|| anyhow::anyhow!("index checkpoint payload underflow"))?;
    let actual_checksum = crc32fast::hash(payload);
    if actual_checksum != expected_checksum {
        anyhow::bail!(
            "index checkpoint checksum mismatch: expected={expected_checksum:#010x} actual={actual_checksum:#010x}"
        );
    }
    Ok(payload)
}

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
                diskmeta_probe_multiplier: meta.diskmeta_probe_multiplier,
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
        let manifest_epoch = match load_startup_manifest_bytes(&db)? {
            None => 0,
            Some(raw) => {
                let manifest = decode_startup_manifest(raw.as_ref())
                    .context("decode spfresh startup manifest")?;
                if manifest.schema_version == STARTUP_MANIFEST_SCHEMA_VERSION
                    && manifest.generation == generation
                {
                    manifest.epoch
                } else {
                    0
                }
            }
        };
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
                let replayed_exactly =
                    if let RuntimeSpFreshIndex::OffHeapDiskMeta(index) = &mut index_state {
                        Self::replay_wal_tail_diskmeta_exact(&db, index, replay_from)?
                    } else {
                        false
                    };
                if !replayed_exactly {
                    // Legacy WAL tails without diskmeta deltas cannot preserve exact centroid
                    // accounting; rebuild from authoritative rows in that case.
                    let (rows, assignments) = load_rows_with_posting_assignments(&db, generation)?;
                    let (rebuilt, _assigned_now) =
                        SpFreshDiskMetaIndex::build_from_rows_with_assignments(
                            cfg.spfresh.clone(),
                            &rows,
                            Some(&assignments),
                        );
                    index_state = RuntimeSpFreshIndex::OffHeapDiskMeta(rebuilt);
                }
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
        let dirty_ids = Arc::new(Mutex::new(FxHashSet::default()));
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
            const GROUP_COMMIT_MAX_REQUESTS: usize = 32;
            const GROUP_COMMIT_MAX_OPS: usize = 16_384;
            let mut deferred = None;
            loop {
                let req = match deferred.take() {
                    Some(req) => req,
                    None => match commit_rx.recv() {
                        Ok(req) => req,
                        Err(_) => break,
                    },
                };
                match req {
                    CommitRequest::Write { ops, sync, resp } => {
                        let mut grouped = Vec::with_capacity(GROUP_COMMIT_MAX_REQUESTS);
                        let mut grouped_ops = 0usize;
                        grouped_ops = grouped_ops.saturating_add(ops.len());
                        grouped.push((ops, sync, resp));
                        let mut saw_shutdown = false;

                        loop {
                            if grouped.len() >= GROUP_COMMIT_MAX_REQUESTS
                                || grouped_ops >= GROUP_COMMIT_MAX_OPS
                            {
                                break;
                            }
                            match commit_rx.try_recv() {
                                Ok(CommitRequest::Write { ops, sync, resp }) => {
                                    let next_grouped_ops = grouped_ops.saturating_add(ops.len());
                                    if next_grouped_ops > GROUP_COMMIT_MAX_OPS {
                                        deferred = Some(CommitRequest::Write { ops, sync, resp });
                                        break;
                                    }
                                    grouped_ops = next_grouped_ops;
                                    grouped.push((ops, sync, resp));
                                }
                                Ok(CommitRequest::Shutdown) => {
                                    saw_shutdown = true;
                                    break;
                                }
                                Err(mpsc::TryRecvError::Empty) => break,
                                Err(mpsc::TryRecvError::Disconnected) => break,
                            }
                        }

                        let need_sync = grouped.iter().any(|(_, sync, _)| *sync);
                        let mut merged_ops = Vec::with_capacity(grouped_ops);
                        let mut responders = Vec::with_capacity(grouped.len());
                        for (ops, _sync, resp) in grouped {
                            merged_ops.extend(ops);
                            responders.push(resp);
                        }

                        let result = commit_db
                            .write_batch(merged_ops, WriteOptions { sync: need_sync })
                            .context("spfresh-layerdb commit worker grouped write batch");
                        let err_text = result.err().map(|err| format!("{err:#}"));
                        for resp in responders {
                            let send_result = match err_text.as_ref() {
                                Some(text) => Err(anyhow::anyhow!("{text}")),
                                None => Ok(()),
                            };
                            let _ = resp.send(send_result);
                        }
                        if saw_shutdown {
                            break;
                        }
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
            match decode_checkpoint_frame(raw.as_ref()).and_then(|payload| {
                let mut aligned = rkyv::AlignedVec::with_capacity(payload.len());
                aligned.extend_from_slice(payload);
                // Safety: payload integrity is verified by CRC in decode_checkpoint_frame.
                let archived =
                    unsafe { rkyv::archived_root::<PersistedIndexCheckpoint>(aligned.as_ref()) };
                let checkpoint: PersistedIndexCheckpoint =
                    archived.deserialize(&mut rkyv::Infallible).map_err(|err| {
                        anyhow::anyhow!("deserialize archived index checkpoint: {err}")
                    })?;
                Ok(checkpoint)
            }) {
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

    fn replay_wal_tail_diskmeta_exact(
        db: &Db,
        index: &mut SpFreshDiskMetaIndex,
        from_seq: u64,
    ) -> anyhow::Result<bool> {
        let mut replay_supported = true;
        visit_wal_entries_since(db, from_seq, |entry| {
            if !replay_supported {
                return Ok(());
            }
            match entry {
                IndexWalEntry::DiskMetaUpsertBatch { rows } => {
                    for row in rows {
                        index.apply_upsert(row.old, row.new_posting, row.new_values);
                    }
                }
                IndexWalEntry::DiskMetaDeleteBatch { rows } => {
                    for row in rows {
                        let _ = index.apply_delete(row.old);
                    }
                }
                IndexWalEntry::Touch { .. }
                | IndexWalEntry::TouchBatch { .. }
                | IndexWalEntry::VectorUpsertBatch { .. }
                | IndexWalEntry::VectorDeleteBatch { .. } => {
                    replay_supported = false;
                }
            }
            Ok(())
        })?;
        Ok(replay_supported)
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
        let mut touched = FxHashSet::default();
        visit_wal_entries_since(db, from_seq, |entry| {
            match entry {
                IndexWalEntry::Touch { id } => {
                    touched.insert(id);
                }
                IndexWalEntry::TouchBatch { ids } => {
                    touched.extend(ids);
                }
                IndexWalEntry::VectorUpsertBatch { rows } => match index {
                    RuntimeSpFreshIndex::Resident(index) => {
                        for row in rows {
                            index.upsert(row.id, row.values);
                        }
                    }
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        {
                            let mut cache = lock_mutex(vector_cache);
                            for row in &rows {
                                cache.put(row.id, row.values.clone());
                            }
                        }
                        let mut loader =
                            Self::loader_for(db, vector_cache, vector_blocks, generation, None);
                        for row in rows {
                            index.upsert_with(row.id, row.values, &mut loader)?;
                        }
                    }
                    RuntimeSpFreshIndex::OffHeapDiskMeta(index) => {
                        for row in rows {
                            let posting = index.choose_posting(&row.values).unwrap_or_default();
                            index.apply_upsert(None, posting, row.values);
                        }
                    }
                },
                IndexWalEntry::VectorDeleteBatch { ids } => match index {
                    RuntimeSpFreshIndex::Resident(index) => {
                        for id in ids {
                            let _ = index.delete(id);
                        }
                    }
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader =
                            Self::loader_for(db, vector_cache, vector_blocks, generation, None);
                        for id in ids {
                            let _ = index.delete_with(id, &mut loader)?;
                        }
                    }
                    RuntimeSpFreshIndex::OffHeapDiskMeta(index) => {
                        for _ in ids {
                            let _ = index.apply_delete(None);
                        }
                    }
                },
                IndexWalEntry::DiskMetaUpsertBatch { rows } => {
                    touched.extend(rows.into_iter().map(|row| row.id));
                }
                IndexWalEntry::DiskMetaDeleteBatch { rows } => {
                    touched.extend(rows.into_iter().map(|row| row.id));
                }
            }
            Ok(())
        })?;
        if touched.is_empty() {
            return Ok(());
        }

        match index {
            RuntimeSpFreshIndex::Resident(index) => {
                for id in touched {
                    let resolved =
                        Self::load_vector_for_id(db, vector_cache, vector_blocks, generation, id)?;
                    match resolved {
                        Some(values) => index.upsert(id, values),
                        None => {
                            let _ = index.delete(id);
                        }
                    }
                }
            }
            RuntimeSpFreshIndex::OffHeap(index) => {
                let mut loader =
                    Self::loader_for(db, vector_cache, vector_blocks, generation, None);
                for id in touched {
                    let resolved =
                        Self::load_vector_for_id(db, vector_cache, vector_blocks, generation, id)?;
                    match resolved {
                        Some(values) => {
                            index.upsert_with(id, values, &mut loader)?;
                        }
                        None => {
                            let _ = index.delete_with(id, &mut loader)?;
                        }
                    }
                }
            }
            RuntimeSpFreshIndex::OffHeapDiskMeta(index) => {
                for id in touched {
                    let resolved =
                        Self::load_vector_for_id(db, vector_cache, vector_blocks, generation, id)?;
                    match resolved {
                        Some(values) => {
                            let posting = index.choose_posting(&values).unwrap_or_default();
                            index.apply_upsert(None, posting, values);
                        }
                        None => {
                            let _ = index.apply_delete(None);
                        }
                    }
                }
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
        let payload = rkyv::to_bytes::<_, 4096>(&checkpoint)
            .context("archive spfresh index checkpoint payload")?;
        let bytes =
            encode_checkpoint_frame(payload.as_ref()).context("frame spfresh index checkpoint")?;
        persist_index_checkpoint_bytes(&self.db, bytes, true)?;
        let manifest = PersistedStartupManifest {
            schema_version: STARTUP_MANIFEST_SCHEMA_VERSION,
            generation,
            applied_wal_seq: next_wal_seq.checked_sub(1),
            posting_event_next_seq,
            epoch,
        };
        let manifest_bytes = encode_startup_manifest(&manifest);
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
