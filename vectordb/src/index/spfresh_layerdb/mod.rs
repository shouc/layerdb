mod config;
mod rebuilder;
mod stats;
mod storage;
mod sync_utils;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::JoinHandle;

use anyhow::Context;
use layerdb::{Db, DbOptions, WriteOptions};

use crate::types::{Neighbor, VectorIndex, VectorRecord};

use super::{SpFreshConfig, SpFreshIndex};
use rebuilder::{rebuild_once, spawn_rebuilder, RebuilderRuntime};
use stats::SpFreshLayerDbStatsInner;
use storage::{
    ensure_active_generation, ensure_metadata, ensure_wal_exists, load_metadata, load_rows,
    prefix_exclusive_end, refresh_read_snapshot, set_active_generation, validate_config,
    vector_key, vector_prefix,
};
use sync_utils::{lock_mutex, lock_read, lock_write};

pub use config::SpFreshLayerDbConfig;
pub use stats::SpFreshLayerDbStats;

pub struct SpFreshLayerDbIndex {
    cfg: SpFreshLayerDbConfig,
    db_path: PathBuf,
    db: Db,
    active_generation: Arc<AtomicU64>,
    index: Arc<RwLock<SpFreshIndex>>,
    update_gate: Arc<Mutex<()>>,
    dirty_ids: Arc<Mutex<HashSet<u64>>>,
    pending_ops: Arc<AtomicUsize>,
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

    fn open_with_db(db_path: &Path, db: Db, cfg: SpFreshLayerDbConfig) -> anyhow::Result<Self> {
        let generation = ensure_active_generation(&db)?;
        let rows = load_rows(&db, generation)?;
        let index = Arc::new(RwLock::new(SpFreshIndex::build(cfg.spfresh.clone(), &rows)));
        let active_generation = Arc::new(AtomicU64::new(generation));
        let update_gate = Arc::new(Mutex::new(()));
        let dirty_ids = Arc::new(Mutex::new(HashSet::new()));
        let pending_ops = Arc::new(AtomicUsize::new(0));
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
            rebuild_tx,
            stop_worker,
            worker: Some(worker),
            stats,
        })
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
            stats: self.stats.clone(),
            stop_worker: self.stop_worker.clone(),
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
        let _update_guard = lock_mutex(&self.update_gate);

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

        *lock_write(&self.index) = SpFreshIndex::build(self.cfg.spfresh.clone(), rows);
        lock_mutex(&self.dirty_ids).clear();
        self.pending_ops.store(0, Ordering::Relaxed);
        self.stats.set_last_rebuild_rows(rows.len());
        Ok(())
    }

    pub fn force_rebuild(&self) -> anyhow::Result<()> {
        let runtime = self.runtime();
        rebuild_once(&runtime)
    }

    fn persist_upsert(&self, id: u64, vector: &[f32]) -> anyhow::Result<()> {
        let generation = self.active_generation.load(Ordering::Relaxed);
        let row = VectorRecord::new(id, vector.to_vec());
        let value = bincode::serialize(&row).context("serialize vector row")?;
        self.db
            .put(
                vector_key(generation, id),
                value,
                WriteOptions {
                    sync: self.cfg.write_sync,
                },
            )
            .with_context(|| format!("persist vector id={id}"))?;
        Ok(())
    }

    fn persist_delete(&self, id: u64) -> anyhow::Result<()> {
        let generation = self.active_generation.load(Ordering::Relaxed);
        self.db
            .delete(
                vector_key(generation, id),
                WriteOptions {
                    sync: self.cfg.write_sync,
                },
            )
            .with_context(|| format!("delete vector id={id}"))?;
        Ok(())
    }

    pub fn try_upsert(&mut self, id: u64, vector: Vec<f32>) -> anyhow::Result<()> {
        if vector.len() != self.cfg.spfresh.dim {
            anyhow::bail!(
                "invalid vector dim for id={id}: got {}, expected {}",
                vector.len(),
                self.cfg.spfresh.dim
            );
        }

        let _update_guard = lock_mutex(&self.update_gate);
        if let Err(err) = self.persist_upsert(id, &vector) {
            self.stats.inc_persist_errors();
            return Err(err);
        }
        lock_write(&self.index).upsert(id, vector);
        self.mark_dirty(id);
        self.stats.inc_upserts();
        let pending = self.pending_ops.load(Ordering::Relaxed);
        if pending == self.cfg.rebuild_pending_ops.max(1) {
            let _ = self.rebuild_tx.send(());
        }
        Ok(())
    }

    pub fn try_delete(&mut self, id: u64) -> anyhow::Result<bool> {
        let _update_guard = lock_mutex(&self.update_gate);
        if let Err(err) = self.persist_delete(id) {
            self.stats.inc_persist_errors();
            return Err(err);
        }
        let deleted = lock_write(&self.index).delete(id);
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
        Ok(())
    }

    pub fn stats(&self) -> SpFreshLayerDbStats {
        self.stats
            .snapshot(self.pending_ops.load(Ordering::Relaxed) as u64)
    }

    pub fn health_check(&self) -> anyhow::Result<SpFreshLayerDbStats> {
        ensure_wal_exists(&self.db_path)?;
        ensure_metadata(&self.db, &self.cfg)?;
        let _ = ensure_active_generation(&self.db)?;
        Ok(self.stats())
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
        if let Err(err) = self.try_upsert(id, vector) {
            eprintln!("spfresh-layerdb upsert failed for id={id}: {err:#}");
        }
    }

    fn delete(&mut self, id: u64) -> bool {
        match self.try_delete(id) {
            Ok(deleted) => deleted,
            Err(err) => {
                eprintln!("spfresh-layerdb delete failed for id={id}: {err:#}");
                false
            }
        }
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<Neighbor> {
        lock_read(&self.index).search(query, k)
    }

    fn len(&self) -> usize {
        lock_read(&self.index).len()
    }
}
