use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::Context;
use layerdb::{Db, DbOptions, Range, ReadOptions, WriteOptions};
use serde::{Deserialize, Serialize};

use crate::types::{Neighbor, VectorIndex, VectorRecord};

use super::{SpFreshConfig, SpFreshIndex};

const VECTOR_PREFIX: &str = "spfresh/v/";
const META_CONFIG_KEY: &str = "spfresh/meta/config";
const META_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpFreshPersistedMeta {
    schema_version: u32,
    dim: usize,
    initial_postings: usize,
    split_limit: usize,
    merge_limit: usize,
    reassign_range: usize,
    nprobe: usize,
    kmeans_iters: usize,
}

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
                fsync_writes: true,
                ..Default::default()
            },
            write_sync: true,
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
        validate_config(&cfg)?;
        let db_path = path.as_ref();
        let db = Db::open(db_path, cfg.db_options.clone()).context("open layerdb for spfresh")?;
        ensure_wal_exists(db_path)?;
        refresh_read_snapshot(&db)?;
        ensure_metadata(&db, &cfg)?;
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
        self.try_bulk_load(rows)
    }

    pub fn try_bulk_load(&mut self, rows: &[VectorRecord]) -> anyhow::Result<()> {
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

    pub fn try_upsert(&mut self, id: u64, vector: Vec<f32>) -> anyhow::Result<()> {
        if vector.len() != self.cfg.spfresh.dim {
            anyhow::bail!(
                "invalid vector dim for id={id}: got {}, expected {}",
                vector.len(),
                self.cfg.spfresh.dim
            );
        }

        let _update_guard = lock_mutex(&self.update_gate);
        self.persist_upsert(id, &vector)?;
        lock_write(&self.index).upsert(id, vector);
        let pending = self.pending_ops.fetch_add(1, Ordering::Relaxed) + 1;
        if pending == self.cfg.rebuild_pending_ops.max(1) {
            let _ = self.rebuild_tx.send(());
        }
        Ok(())
    }

    pub fn try_delete(&mut self, id: u64) -> anyhow::Result<bool> {
        let _update_guard = lock_mutex(&self.update_gate);
        self.persist_delete(id)?;
        let deleted = lock_write(&self.index).delete(id);
        if deleted {
            let pending = self.pending_ops.fetch_add(1, Ordering::Relaxed) + 1;
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

fn validate_config(cfg: &SpFreshLayerDbConfig) -> anyhow::Result<()> {
    if cfg.spfresh.dim == 0 {
        anyhow::bail!("spfresh dim must be > 0");
    }
    if cfg.spfresh.initial_postings == 0 {
        anyhow::bail!("spfresh initial_postings must be > 0");
    }
    if cfg.spfresh.nprobe == 0 {
        anyhow::bail!("spfresh nprobe must be > 0");
    }
    if cfg.spfresh.kmeans_iters == 0 {
        anyhow::bail!("spfresh kmeans_iters must be > 0");
    }
    if cfg.spfresh.split_limit < 4 {
        anyhow::bail!("spfresh split_limit must be >= 4");
    }
    if cfg.spfresh.merge_limit == 0 {
        anyhow::bail!("spfresh merge_limit must be > 0");
    }
    if cfg.spfresh.split_limit <= cfg.spfresh.merge_limit {
        anyhow::bail!(
            "spfresh split_limit ({}) must be > merge_limit ({})",
            cfg.spfresh.split_limit,
            cfg.spfresh.merge_limit
        );
    }
    if cfg.spfresh.reassign_range == 0 {
        anyhow::bail!("spfresh reassign_range must be > 0");
    }
    if cfg.rebuild_pending_ops == 0 {
        anyhow::bail!("spfresh rebuild_pending_ops must be > 0");
    }
    if cfg.rebuild_interval.is_zero() {
        anyhow::bail!("spfresh rebuild_interval must be > 0");
    }
    Ok(())
}

fn ensure_metadata(db: &Db, cfg: &SpFreshLayerDbConfig) -> anyhow::Result<()> {
    let expected = SpFreshPersistedMeta {
        schema_version: META_SCHEMA_VERSION,
        dim: cfg.spfresh.dim,
        initial_postings: cfg.spfresh.initial_postings,
        split_limit: cfg.spfresh.split_limit,
        merge_limit: cfg.spfresh.merge_limit,
        reassign_range: cfg.spfresh.reassign_range,
        nprobe: cfg.spfresh.nprobe,
        kmeans_iters: cfg.spfresh.kmeans_iters,
    };

    let current = db
        .get(META_CONFIG_KEY, ReadOptions::default())
        .context("read spfresh metadata")?;
    if let Some(value) = current {
        let actual: SpFreshPersistedMeta =
            serde_json::from_slice(value.as_ref()).context("decode spfresh metadata")?;
        if actual.schema_version != expected.schema_version {
            anyhow::bail!(
                "spfresh schema version mismatch: stored={} expected={}",
                actual.schema_version,
                expected.schema_version
            );
        }
        if actual.dim != expected.dim {
            anyhow::bail!("spfresh dim mismatch: stored={} expected={}", actual.dim, expected.dim);
        }
        if actual.initial_postings != expected.initial_postings
            || actual.split_limit != expected.split_limit
            || actual.merge_limit != expected.merge_limit
            || actual.reassign_range != expected.reassign_range
            || actual.nprobe != expected.nprobe
            || actual.kmeans_iters != expected.kmeans_iters
        {
            anyhow::bail!(
                "spfresh config mismatch with stored metadata; use matching config for this index directory"
            );
        }
        return Ok(());
    }

    let bytes = serde_json::to_vec(&expected).context("encode spfresh metadata")?;
    db.put(META_CONFIG_KEY, bytes, WriteOptions { sync: true })
        .context("persist spfresh metadata")?;
    Ok(())
}

fn ensure_wal_exists(path: &Path) -> anyhow::Result<()> {
    let wal_dir = path.join("wal");
    if !wal_dir.is_dir() {
        anyhow::bail!("layerdb wal directory missing at {}", wal_dir.display());
    }

    let mut has_segment = false;
    for entry in std::fs::read_dir(&wal_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("wal_") && name.ends_with(".log") {
            has_segment = true;
            break;
        }
    }

    if !has_segment {
        anyhow::bail!("layerdb wal contains no segment files in {}", wal_dir.display());
    }
    Ok(())
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
    use std::fs;

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
}
