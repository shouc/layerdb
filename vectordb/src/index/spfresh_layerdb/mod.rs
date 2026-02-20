mod config;
mod rebuilder;
mod stats;
mod storage;
mod sync_utils;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Instant;

use anyhow::Context;
use layerdb::{Db, DbOptions, WriteOptions};
use serde::{Deserialize, Serialize};

use crate::types::{Neighbor, VectorIndex, VectorRecord};

use super::spfresh_offheap::SpFreshOffHeapIndex;
use super::{SpFreshConfig, SpFreshIndex};
use rebuilder::{rebuild_once, spawn_rebuilder, RebuilderRuntime};
use stats::SpFreshLayerDbStatsInner;
use storage::{
    ensure_active_generation, ensure_metadata, ensure_wal_exists, ensure_wal_next_seq, load_metadata,
    load_row, load_rows, load_index_checkpoint_bytes, load_wal_touched_ids_since,
    persist_index_checkpoint_bytes, prefix_exclusive_end, prune_wal_before, refresh_read_snapshot,
    set_active_generation, validate_config, wal_key, vector_key, vector_prefix,
};
use sync_utils::{lock_mutex, lock_read, lock_write};

pub use config::{SpFreshLayerDbConfig, SpFreshMemoryMode};
pub use stats::SpFreshLayerDbStats;

#[derive(Clone, Debug, Serialize, Deserialize)]
enum RuntimeSpFreshIndex {
    Resident(SpFreshIndex),
    OffHeap(SpFreshOffHeapIndex),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedIndexCheckpoint {
    schema_version: u32,
    generation: u64,
    applied_wal_seq: Option<u64>,
    index: RuntimeSpFreshIndex,
}

#[derive(Debug)]
struct VectorCache {
    capacity: usize,
    map: HashMap<u64, Vec<f32>>,
    order: VecDeque<u64>,
}

impl VectorCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&self, id: u64) -> Option<Vec<f32>> {
        self.map.get(&id).cloned()
    }

    fn put(&mut self, id: u64, values: Vec<f32>) {
        if self.capacity == 0 {
            return;
        }
        if let std::collections::hash_map::Entry::Occupied(mut existing) = self.map.entry(id) {
            existing.insert(values);
            return;
        }
        if self.map.len() >= self.capacity {
            while let Some(oldest) = self.order.pop_front() {
                if self.map.remove(&oldest).is_some() {
                    break;
                }
            }
        }
        self.order.push_back(id);
        self.map.insert(id, values);
    }

    fn remove(&mut self, id: u64) {
        self.map.remove(&id);
    }
}

pub struct SpFreshLayerDbIndex {
    cfg: SpFreshLayerDbConfig,
    db_path: PathBuf,
    db: Db,
    active_generation: Arc<AtomicU64>,
    index: Arc<RwLock<RuntimeSpFreshIndex>>,
    update_gate: Arc<RwLock<()>>,
    dirty_ids: Arc<Mutex<HashSet<u64>>>,
    pending_ops: Arc<AtomicUsize>,
    vector_cache: Arc<Mutex<VectorCache>>,
    wal_next_seq: Arc<AtomicU64>,
    rebuild_tx: mpsc::Sender<()>,
    stop_worker: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    stats: Arc<SpFreshLayerDbStatsInner>,
}

impl SpFreshLayerDbIndex {
    pub fn open(path: impl AsRef<Path>, cfg: SpFreshLayerDbConfig) -> anyhow::Result<Self> {
        validate_config(&cfg)?;
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
        let (mut index_state, applied_wal_seq) =
            Self::load_or_rebuild_index(&db, &cfg, generation, wal_next_seq)?;
        cfg.memory_mode = match &index_state {
            RuntimeSpFreshIndex::Resident(_) => SpFreshMemoryMode::Resident,
            RuntimeSpFreshIndex::OffHeap(_) => SpFreshMemoryMode::OffHeap,
        };
        let vector_cache = Arc::new(Mutex::new(VectorCache::new(cfg.offheap_cache_capacity)));
        let replay_from = applied_wal_seq.map_or(0, |seq| seq.saturating_add(1));
        if replay_from < wal_next_seq {
            Self::replay_wal_tail(&db, &vector_cache, generation, &mut index_state, replay_from)?;
        }
        let index = Arc::new(RwLock::new(index_state));
        let active_generation = Arc::new(AtomicU64::new(generation));
        let update_gate = Arc::new(RwLock::new(()));
        let dirty_ids = Arc::new(Mutex::new(HashSet::new()));
        let pending_ops = Arc::new(AtomicUsize::new(0));
        let wal_next_seq = Arc::new(AtomicU64::new(wal_next_seq));
        let stop_worker = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(SpFreshLayerDbStatsInner::default());
        let (rebuild_tx, rebuild_rx) = mpsc::channel::<()>();

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
                stats: stats.clone(),
                stop_worker: stop_worker.clone(),
            },
            rebuild_rx,
        );

        Ok(Self {
            cfg,
            db_path: db_path.to_path_buf(),
            db,
            active_generation,
            index,
            update_gate,
            dirty_ids,
            pending_ops,
            vector_cache,
            wal_next_seq,
            rebuild_tx,
            stop_worker,
            worker: Some(worker),
            stats,
        })
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
                    if checkpoint.schema_version == config::META_INDEX_CHECKPOINT_SCHEMA_VERSION
                        && checkpoint.generation == generation =>
                {
                    return Ok((checkpoint.index, checkpoint.applied_wal_seq));
                }
                Ok(_) => {}
                Err(err) => {
                    eprintln!("spfresh-layerdb checkpoint decode failed, rebuilding index: {err:#}");
                }
            }
        }

        let rows = load_rows(db, generation)?;
        let applied_wal_seq = wal_next_seq.checked_sub(1);
        let index = match cfg.memory_mode {
            SpFreshMemoryMode::Resident => {
                RuntimeSpFreshIndex::Resident(SpFreshIndex::build(cfg.spfresh.clone(), &rows))
            }
            SpFreshMemoryMode::OffHeap => {
                RuntimeSpFreshIndex::OffHeap(SpFreshOffHeapIndex::build(cfg.spfresh.clone(), &rows))
            }
        };
        Ok((index, applied_wal_seq))
    }

    fn replay_wal_tail(
        db: &Db,
        vector_cache: &Arc<Mutex<VectorCache>>,
        generation: u64,
        index: &mut RuntimeSpFreshIndex,
        from_seq: u64,
    ) -> anyhow::Result<()> {
        let touched_ids = load_wal_touched_ids_since(db, from_seq)?;
        if touched_ids.is_empty() {
            return Ok(());
        }

        let mut unique = HashSet::with_capacity(touched_ids.len());
        for id in touched_ids {
            unique.insert(id);
        }
        for id in unique {
            match load_row(db, generation, id)? {
                Some(row) => match index {
                    RuntimeSpFreshIndex::Resident(index) => index.upsert(row.id, row.values),
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader =
                            Self::loader_for(db, vector_cache, generation, Some((row.id, row.values.clone())));
                        index.upsert_with(row.id, row.values, &mut loader)?;
                    }
                },
                None => match index {
                    RuntimeSpFreshIndex::Resident(index) => {
                        let _ = index.delete(id);
                    }
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader = Self::loader_for(db, vector_cache, generation, None);
                        let _ = index.delete_with(id, &mut loader)?;
                    }
                },
            }
        }
        Ok(())
    }

    fn persist_index_checkpoint(&self) -> anyhow::Result<()> {
        let next_wal_seq = self.wal_next_seq.load(Ordering::Relaxed);
        let snapshot = lock_read(&self.index).clone();
        let checkpoint = PersistedIndexCheckpoint {
            schema_version: config::META_INDEX_CHECKPOINT_SCHEMA_VERSION,
            generation: self.active_generation.load(Ordering::Relaxed),
            applied_wal_seq: next_wal_seq.checked_sub(1),
            index: snapshot,
        };
        let bytes = bincode::serialize(&checkpoint).context("encode spfresh index checkpoint")?;
        persist_index_checkpoint_bytes(&self.db, bytes, self.cfg.write_sync)?;
        if let Err(err) = prune_wal_before(&self.db, next_wal_seq, false) {
            eprintln!("spfresh-layerdb wal prune failed: {err:#}");
        }
        Ok(())
    }

    fn runtime(&self) -> RebuilderRuntime {
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
            stats: self.stats.clone(),
            stop_worker: self.stop_worker.clone(),
        }
    }

    fn load_vector_for_id(
        db: &Db,
        vector_cache: &Arc<Mutex<VectorCache>>,
        generation: u64,
        id: u64,
    ) -> anyhow::Result<Option<Vec<f32>>> {
        if let Some(values) = lock_mutex(vector_cache).get(id) {
            return Ok(Some(values));
        }
        let Some(row) = load_row(db, generation, id)? else {
            return Ok(None);
        };
        let values = row.values;
        lock_mutex(vector_cache).put(id, values.clone());
        Ok(Some(values))
    }

    fn loader_for<'a>(
        db: &'a Db,
        vector_cache: &'a Arc<Mutex<VectorCache>>,
        generation: u64,
        override_row: Option<(u64, Vec<f32>)>,
    ) -> impl FnMut(u64) -> anyhow::Result<Option<Vec<f32>>> + 'a {
        move |id| {
            if let Some((override_id, values)) = &override_row {
                if *override_id == id {
                    return Ok(Some(values.clone()));
                }
            }
            Self::load_vector_for_id(db, vector_cache, generation, id)
        }
    }

    fn mark_dirty(&self, id: u64) {
        let mut dirty = lock_mutex(&self.dirty_ids);
        dirty.insert(id);
        self.pending_ops.store(dirty.len(), Ordering::Relaxed);
    }

    pub fn bulk_load(&mut self, rows: &[VectorRecord]) -> anyhow::Result<()> {
        self.try_bulk_load(rows)
    }

    pub fn try_bulk_load(&mut self, rows: &[VectorRecord]) -> anyhow::Result<()> {
        let _update_guard = lock_write(&self.update_gate);

        let old_generation = self.active_generation.load(Ordering::Relaxed);
        let new_generation = old_generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("active generation overflow"))?;

        for batch in rows.chunks(1_024) {
            let mut ops = Vec::with_capacity(batch.len());
            for row in batch {
                let value = bincode::serialize(row)
                    .with_context(|| format!("serialize vector row id={}", row.id))?;
                ops.push(layerdb::Op::put(vector_key(new_generation, row.id), value));
            }
            self.db
                .write_batch(
                    ops,
                    WriteOptions {
                        sync: self.cfg.write_sync,
                    },
                )
                .context("persist spfresh bulk rows")?;
        }
        set_active_generation(&self.db, new_generation, self.cfg.write_sync)?;
        self.active_generation.store(new_generation, Ordering::Relaxed);

        // best-effort cleanup of the old generation after pointer switch.
        let old_prefix = vector_prefix(old_generation);
        let old_prefix_bytes = old_prefix.as_bytes().to_vec();
        let old_end = prefix_exclusive_end(&old_prefix_bytes)?;
        if let Err(err) = self.db.delete_range(
            old_prefix_bytes,
            old_end,
            WriteOptions {
                sync: false,
            },
        ) {
            eprintln!(
                "spfresh-layerdb bulk-load old generation cleanup failed: {err:#}"
            );
        }

        *lock_write(&self.index) = match self.cfg.memory_mode {
            SpFreshMemoryMode::Resident => {
                RuntimeSpFreshIndex::Resident(SpFreshIndex::build(self.cfg.spfresh.clone(), rows))
            }
            SpFreshMemoryMode::OffHeap => RuntimeSpFreshIndex::OffHeap(SpFreshOffHeapIndex::build(
                self.cfg.spfresh.clone(),
                rows,
            )),
        };
        {
            let mut cache = lock_mutex(&self.vector_cache);
            cache.map.clear();
            cache.order.clear();
        }
        lock_mutex(&self.dirty_ids).clear();
        self.pending_ops.store(0, Ordering::Relaxed);
        self.stats.set_last_rebuild_rows(rows.len());
        self.persist_index_checkpoint()?;
        Ok(())
    }

    pub fn force_rebuild(&self) -> anyhow::Result<()> {
        let runtime = self.runtime();
        rebuild_once(&runtime)
    }

    fn persist_with_wal(&self, id: u64, vector_op: layerdb::Op) -> anyhow::Result<()> {
        let seq = self.wal_next_seq.load(Ordering::Relaxed);
        let next_seq = seq
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("spfresh wal sequence overflow"))?;
        let wal_id = bincode::serialize(&id).context("encode spfresh wal id")?;
        let wal_next = bincode::serialize(&next_seq).context("encode spfresh wal next seq")?;
        self.db
            .write_batch(
                vec![
                    vector_op,
                    layerdb::Op::put(wal_key(seq), wal_id),
                    layerdb::Op::put(config::META_INDEX_WAL_NEXT_SEQ_KEY, wal_next),
                ],
                WriteOptions {
                    sync: self.cfg.write_sync,
                },
            )
            .with_context(|| format!("persist vector+wal id={id} seq={seq}"))?;
        self.wal_next_seq.store(next_seq, Ordering::Relaxed);
        Ok(())
    }

    fn persist_upsert(&self, id: u64, vector: &[f32]) -> anyhow::Result<()> {
        let generation = self.active_generation.load(Ordering::Relaxed);
        let row = VectorRecord::new(id, vector.to_vec());
        let value = bincode::serialize(&row).context("serialize vector row")?;
        self.persist_with_wal(id, layerdb::Op::put(vector_key(generation, id), value))
    }

    fn persist_delete(&self, id: u64) -> anyhow::Result<()> {
        let generation = self.active_generation.load(Ordering::Relaxed);
        self.persist_with_wal(id, layerdb::Op::delete(vector_key(generation, id)))
    }

    pub fn try_upsert(&mut self, id: u64, vector: Vec<f32>) -> anyhow::Result<()> {
        if vector.len() != self.cfg.spfresh.dim {
            anyhow::bail!(
                "invalid vector dim for id={id}: got {}, expected {}",
                vector.len(),
                self.cfg.spfresh.dim
            );
        }

        let _update_guard = lock_read(&self.update_gate);
        let persist_started = Instant::now();
        if let Err(err) = self.persist_upsert(id, &vector) {
            self.stats
                .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
            self.stats.inc_persist_errors();
            return Err(err);
        }
        self.stats
            .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
        let cache_vector = vector.clone();
        {
            let mut index = lock_write(&self.index);
            match &mut *index {
                RuntimeSpFreshIndex::Resident(index) => index.upsert(id, vector.clone()),
                RuntimeSpFreshIndex::OffHeap(index) => {
                    let generation = self.active_generation.load(Ordering::Relaxed);
                    let mut loader =
                        Self::loader_for(&self.db, &self.vector_cache, generation, Some((id, vector.clone())));
                    index
                        .upsert_with(id, vector, &mut loader)
                        .with_context(|| format!("offheap upsert id={id}"))?;
                }
            }
        }
        lock_mutex(&self.vector_cache).put(id, cache_vector);
        self.mark_dirty(id);
        self.stats.inc_upserts();
        let pending = self.pending_ops.load(Ordering::Relaxed);
        if pending == self.cfg.rebuild_pending_ops.max(1) {
            let _ = self.rebuild_tx.send(());
        }
        Ok(())
    }

    pub fn try_delete(&mut self, id: u64) -> anyhow::Result<bool> {
        let _update_guard = lock_read(&self.update_gate);
        let persist_started = Instant::now();
        if let Err(err) = self.persist_delete(id) {
            self.stats
                .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
            self.stats.inc_persist_errors();
            return Err(err);
        }
        self.stats
            .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
        let deleted = {
            let mut index = lock_write(&self.index);
            match &mut *index {
                RuntimeSpFreshIndex::Resident(index) => index.delete(id),
                RuntimeSpFreshIndex::OffHeap(index) => {
                    let generation = self.active_generation.load(Ordering::Relaxed);
                    let mut loader = Self::loader_for(&self.db, &self.vector_cache, generation, None);
                    index
                        .delete_with(id, &mut loader)
                        .with_context(|| format!("offheap delete id={id}"))?
                }
            }
        };
        lock_mutex(&self.vector_cache).remove(id);
        if deleted {
            self.mark_dirty(id);
            self.stats.inc_deletes();
            let pending = self.pending_ops.load(Ordering::Relaxed);
            if pending == self.cfg.rebuild_pending_ops.max(1) {
                let _ = self.rebuild_tx.send(());
            }
        }
        Ok(deleted)
    }

    pub fn close(mut self) -> anyhow::Result<()> {
        self.stop_worker.store(true, Ordering::Relaxed);
        let _ = self.rebuild_tx.send(());
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                anyhow::bail!("spfresh-layerdb background worker panicked");
            }
        }
        self.force_rebuild()?;
        self.persist_index_checkpoint()?;
        Ok(())
    }

    pub fn stats(&self) -> SpFreshLayerDbStats {
        self.stats
            .snapshot(self.pending_ops.load(Ordering::Relaxed) as u64)
    }

    pub fn memory_mode(&self) -> SpFreshMemoryMode {
        self.cfg.memory_mode
    }

    pub fn health_check(&self) -> anyhow::Result<SpFreshLayerDbStats> {
        ensure_wal_exists(&self.db_path)?;
        ensure_metadata(&self.db, &self.cfg)?;
        let _ = ensure_active_generation(&self.db)?;
        Ok(self.stats())
    }

    /// Flush and compact vector data, then freeze level-1 SSTs into S3 tier.
    ///
    /// This is the primary durability/tiering operation for SPFresh-on-LayerDB.
    pub fn sync_to_s3(&self, max_files: Option<usize>) -> anyhow::Result<usize> {
        let _update_guard = lock_write(&self.update_gate);
        self.db
            .compact_range(None)
            .context("compact before freeze-to-s3")?;
        self.db
            .freeze_level_to_s3(1, max_files)
            .context("freeze level-1 to s3")
    }

    /// Thaw frozen level-1 SSTs back to local tier.
    pub fn thaw_from_s3(&self, max_files: Option<usize>) -> anyhow::Result<usize> {
        let _update_guard = lock_write(&self.update_gate);
        self.db
            .thaw_level_from_s3(1, max_files)
            .context("thaw level-1 from s3")
    }

    /// Garbage collect orphaned S3 objects that are no longer referenced.
    pub fn gc_orphaned_s3(&self) -> anyhow::Result<usize> {
        self.db.gc_orphaned_s3_files().context("gc orphaned s3 files")
    }

    pub fn frozen_objects(&self) -> Vec<layerdb::version::FrozenObjectMeta> {
        self.db.frozen_objects()
    }
}

impl Drop for SpFreshLayerDbIndex {
    fn drop(&mut self) {
        self.stop_worker.store(true, Ordering::Relaxed);
        let _ = self.rebuild_tx.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl VectorIndex for SpFreshLayerDbIndex {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        self.try_upsert(id, vector)
            .unwrap_or_else(|err| panic!("spfresh-layerdb upsert failed for id={id}: {err:#}"));
    }

    fn delete(&mut self, id: u64) -> bool {
        self.try_delete(id)
            .unwrap_or_else(|err| panic!("spfresh-layerdb delete failed for id={id}: {err:#}"))
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<Neighbor> {
        let started = Instant::now();
        let generation = self.active_generation.load(Ordering::Relaxed);
        let out = match &*lock_read(&self.index) {
            RuntimeSpFreshIndex::Resident(index) => index.search(query, k),
            RuntimeSpFreshIndex::OffHeap(index) => {
                let mut loader = Self::loader_for(&self.db, &self.vector_cache, generation, None);
                match index.search_with(query, k, &mut loader) {
                    Ok(out) => out,
                    Err(err) => panic!("spfresh-layerdb offheap search failed: {err:#}"),
                }
            }
        };
        self.stats
            .record_search(started.elapsed().as_micros() as u64);
        out
    }

    fn len(&self) -> usize {
        match &*lock_read(&self.index) {
            RuntimeSpFreshIndex::Resident(index) => index.len(),
            RuntimeSpFreshIndex::OffHeap(index) => index.len(),
        }
    }
}
