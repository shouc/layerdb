use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::Context;
use layerdb::{Db, DbOptions, Range, ReadOptions, WriteOptions};

use crate::types::{Neighbor, VectorIndex, VectorRecord};

use super::{SpFreshConfig, SpFreshIndex};

const VECTOR_PREFIX: &str = "spfresh/v/";

#[derive(Clone, Debug)]
pub struct SpFreshLayerDbConfig {
    pub spfresh: SpFreshConfig,
    pub db_options: DbOptions,
    pub write_sync: bool,
    pub rebuild_pending_ops: usize,
    pub rebuild_interval: Duration,
}

impl Default for SpFreshLayerDbConfig {
    fn default() -> Self {
        Self {
            spfresh: SpFreshConfig::default(),
            db_options: DbOptions {
                fsync_writes: false,
                ..Default::default()
            },
            write_sync: false,
            rebuild_pending_ops: 2_000,
            rebuild_interval: Duration::from_millis(500),
        }
    }
}

pub struct SpFreshLayerDbIndex {
    cfg: SpFreshLayerDbConfig,
    db: Db,
    index: Arc<RwLock<SpFreshIndex>>,
    update_gate: Arc<Mutex<()>>,
    pending_ops: Arc<AtomicUsize>,
    rebuild_tx: mpsc::Sender<()>,
    stop_worker: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl SpFreshLayerDbIndex {
    pub fn open(path: impl AsRef<Path>, cfg: SpFreshLayerDbConfig) -> anyhow::Result<Self> {
        let db = Db::open(path, cfg.db_options.clone()).context("open layerdb for spfresh")?;
        refresh_read_snapshot(&db)?;
        let rows = load_rows(&db)?;
        let index = SpFreshIndex::build(cfg.spfresh.clone(), &rows);
        let index = Arc::new(RwLock::new(index));
        let update_gate = Arc::new(Mutex::new(()));
        let pending_ops = Arc::new(AtomicUsize::new(0));
        let stop_worker = Arc::new(AtomicBool::new(false));
        let (rebuild_tx, rebuild_rx) = mpsc::channel::<()>();

        let worker = spawn_rebuilder(
            db.clone(),
            cfg.spfresh.clone(),
            cfg.rebuild_pending_ops.max(1),
            cfg.rebuild_interval,
            index.clone(),
            update_gate.clone(),
            pending_ops.clone(),
            rebuild_rx,
            stop_worker.clone(),
        );

        Ok(Self {
            cfg,
            db,
            index,
            update_gate,
            pending_ops,
            rebuild_tx,
            stop_worker,
            worker: Some(worker),
        })
    }

    pub fn bulk_load(&mut self, rows: &[VectorRecord]) -> anyhow::Result<()> {
        let _update_guard = lock_mutex(&self.update_gate);
        let existing = list_vector_keys(&self.db)?;
        if !existing.is_empty() {
            for batch in existing.chunks(2_048) {
                let ops: Vec<layerdb::Op> = batch
                    .iter()
                    .map(|key| layerdb::Op::delete(key.clone()))
                    .collect();
                self.db
                    .write_batch(ops, WriteOptions { sync: self.cfg.write_sync })
                    .context("clear existing spfresh rows")?;
            }
        }

        for batch in rows.chunks(1_024) {
            let mut ops = Vec::with_capacity(batch.len());
            for row in batch {
                let value = serde_json::to_vec(row)
                    .with_context(|| format!("serialize vector row id={}", row.id))?;
                ops.push(layerdb::Op::put(vector_key(row.id), value));
            }
            self.db
                .write_batch(ops, WriteOptions { sync: self.cfg.write_sync })
                .context("persist spfresh bulk rows")?;
        }

        *lock_write(&self.index) = SpFreshIndex::build(self.cfg.spfresh.clone(), rows);
        self.pending_ops.store(0, Ordering::Relaxed);
        Ok(())
    }

    pub fn force_rebuild(&self) -> anyhow::Result<()> {
        rebuild_once(
            &self.db,
            &self.cfg.spfresh,
            &self.index,
            &self.update_gate,
            &self.pending_ops,
        )
    }

    fn persist_upsert(&self, id: u64, vector: &[f32]) -> anyhow::Result<()> {
        let row = VectorRecord::new(id, vector.to_vec());
        let value = serde_json::to_vec(&row).context("serialize vector row")?;
        self.db
            .put(vector_key(id), value, WriteOptions { sync: self.cfg.write_sync })
            .with_context(|| format!("persist vector id={id}"))?;
        Ok(())
    }

    fn persist_delete(&self, id: u64) -> anyhow::Result<()> {
        self.db
            .delete(vector_key(id), WriteOptions { sync: self.cfg.write_sync })
            .with_context(|| format!("delete vector id={id}"))?;
        Ok(())
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
        if vector.len() != self.cfg.spfresh.dim {
            return;
        }

        let _update_guard = lock_mutex(&self.update_gate);
        if let Err(err) = self.persist_upsert(id, &vector) {
            eprintln!("spfresh-layerdb upsert persist failed for id={id}: {err:#}");
            return;
        }

        lock_write(&self.index).upsert(id, vector);
        let pending = self.pending_ops.fetch_add(1, Ordering::Relaxed) + 1;
        if pending == self.cfg.rebuild_pending_ops.max(1) {
            let _ = self.rebuild_tx.send(());
        }
    }

    fn delete(&mut self, id: u64) -> bool {
        let _update_guard = lock_mutex(&self.update_gate);
        if let Err(err) = self.persist_delete(id) {
            eprintln!("spfresh-layerdb delete persist failed for id={id}: {err:#}");
            return false;
        }
        let deleted = lock_write(&self.index).delete(id);
        if deleted {
            let pending = self.pending_ops.fetch_add(1, Ordering::Relaxed) + 1;
            if pending == self.cfg.rebuild_pending_ops.max(1) {
                let _ = self.rebuild_tx.send(());
            }
        }
        deleted
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<Neighbor> {
        lock_read(&self.index).search(query, k)
    }

    fn len(&self) -> usize {
        lock_read(&self.index).len()
    }
}

fn spawn_rebuilder(
    db: Db,
    spfresh_cfg: SpFreshConfig,
    rebuild_pending_ops: usize,
    rebuild_interval: Duration,
    index: Arc<RwLock<SpFreshIndex>>,
    update_gate: Arc<Mutex<()>>,
    pending_ops: Arc<AtomicUsize>,
    rebuild_rx: mpsc::Receiver<()>,
    stop_worker: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let rebuild_pending_ops = rebuild_pending_ops.max(1);
    std::thread::spawn(move || {
        while !stop_worker.load(Ordering::Relaxed) {
            match rebuild_rx.recv_timeout(rebuild_interval) {
                Ok(_) => {
                    if pending_ops.load(Ordering::Relaxed) > 0 {
                        if let Err(err) =
                            rebuild_once(&db, &spfresh_cfg, &index, &update_gate, &pending_ops)
                        {
                            eprintln!("spfresh-layerdb background rebuild failed: {err:#}");
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if pending_ops.load(Ordering::Relaxed) >= rebuild_pending_ops {
                        if let Err(err) =
                            rebuild_once(&db, &spfresh_cfg, &index, &update_gate, &pending_ops)
                        {
                            eprintln!("spfresh-layerdb background rebuild failed: {err:#}");
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    })
}

fn rebuild_once(
    db: &Db,
    spfresh_cfg: &SpFreshConfig,
    index: &Arc<RwLock<SpFreshIndex>>,
    update_gate: &Arc<Mutex<()>>,
    pending_ops: &Arc<AtomicUsize>,
) -> anyhow::Result<()> {
    let _update_guard = lock_mutex(update_gate);
    if pending_ops.load(Ordering::Relaxed) == 0 {
        return Ok(());
    }
    let rows = load_rows(db)?;
    let rebuilt = SpFreshIndex::build(spfresh_cfg.clone(), &rows);
    *lock_write(index) = rebuilt;
    pending_ops.store(0, Ordering::Relaxed);
    Ok(())
}

fn load_rows(db: &Db) -> anyhow::Result<Vec<VectorRecord>> {
    let mut out = Vec::new();
    let mut iter = db.iter(Range::all(), ReadOptions::default())?;
    iter.seek_to_first();
    while let Some(next) = iter.next() {
        let (key, value) = next?;
        if !key.starts_with(VECTOR_PREFIX.as_bytes()) {
            continue;
        }
        let Some(value) = value else {
            continue;
        };
        let row: VectorRecord = serde_json::from_slice(value.as_ref())
            .with_context(|| format!("decode vector row key={}", String::from_utf8_lossy(&key)))?;
        if !row.deleted {
            out.push(row);
        }
    }
    Ok(out)
}

fn list_vector_keys(db: &Db) -> anyhow::Result<Vec<String>> {
    let mut out = Vec::new();
    let mut iter = db.iter(Range::all(), ReadOptions::default())?;
    iter.seek_to_first();
    while let Some(next) = iter.next() {
        let (key, _value) = next?;
        if key.starts_with(VECTOR_PREFIX.as_bytes()) {
            out.push(String::from_utf8_lossy(&key).to_string());
        }
    }
    Ok(out)
}

fn refresh_read_snapshot(db: &Db) -> anyhow::Result<()> {
    db.write_batch(
        vec![
            layerdb::Op::put("spfresh/meta/__refresh__", "1"),
            layerdb::Op::delete("spfresh/meta/__refresh__"),
        ],
        WriteOptions { sync: false },
    )
    .context("refresh layerdb read snapshot for recovery")
}

fn vector_key(id: u64) -> String {
    format!("{VECTOR_PREFIX}{id:020}")
}

fn lock_mutex<T>(m: &Arc<Mutex<T>>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn lock_read<T>(m: &Arc<RwLock<T>>) -> RwLockReadGuard<'_, T> {
    match m.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn lock_write<T>(m: &Arc<RwLock<T>>) -> RwLockWriteGuard<'_, T> {
    match m.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::types::VectorIndex;

    use super::{SpFreshLayerDbConfig, SpFreshLayerDbIndex};

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
}
