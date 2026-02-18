use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::JoinHandle;

use layerdb::Db;

use crate::index::SpFreshIndex;
use crate::types::VectorIndex;

use super::stats::SpFreshLayerDbStatsInner;
use super::storage::load_row;
use super::sync_utils::lock_write;

pub(crate) struct RebuilderRuntime {
    pub db: Db,
    pub rebuild_pending_ops: usize,
    pub rebuild_interval: std::time::Duration,
    pub active_generation: Arc<AtomicU64>,
    pub index: Arc<RwLock<SpFreshIndex>>,
    pub update_gate: Arc<Mutex<()>>,
    pub dirty_ids: Arc<Mutex<std::collections::HashSet<u64>>>,
    pub pending_ops: Arc<AtomicUsize>,
    pub stats: Arc<SpFreshLayerDbStatsInner>,
    pub stop_worker: Arc<AtomicBool>,
}

impl RebuilderRuntime {
    fn has_pending_work(&self) -> bool {
        self.pending_ops.load(Ordering::Relaxed) > 0
    }

    fn over_threshold(&self) -> bool {
        self.pending_ops.load(Ordering::Relaxed) >= self.rebuild_pending_ops.max(1)
    }
}

pub(crate) fn spawn_rebuilder(
    runtime: RebuilderRuntime,
    rebuild_rx: mpsc::Receiver<()>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        while !runtime.stop_worker.load(Ordering::Relaxed) {
            match rebuild_rx.recv_timeout(runtime.rebuild_interval) {
                Ok(_) => {
                    if runtime.has_pending_work() {
                        if let Err(err) = rebuild_once(&runtime) {
                            runtime.stats.inc_rebuild_failures();
                            eprintln!("spfresh-layerdb background rebuild failed: {err:#}");
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if runtime.over_threshold() {
                        if let Err(err) = rebuild_once(&runtime) {
                            runtime.stats.inc_rebuild_failures();
                            eprintln!("spfresh-layerdb background rebuild failed: {err:#}");
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    })
}

pub(crate) fn rebuild_once(runtime: &RebuilderRuntime) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let _update_guard = match runtime.update_gate.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };

    if !runtime.has_pending_work() {
        return Ok(());
    }

    let dirty_ids: Vec<u64> = {
        let guard = match runtime.dirty_ids.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.iter().copied().collect()
    };
    if dirty_ids.is_empty() {
        runtime.pending_ops.store(0, Ordering::Relaxed);
        return Ok(());
    }

    let generation = runtime.active_generation.load(Ordering::Relaxed);
    let mut index = lock_write(&runtime.index);
    for id in &dirty_ids {
        match load_row(&runtime.db, generation, *id)? {
            Some(row) => index.upsert(row.id, row.values),
            None => {
                let _ = index.delete(*id);
            }
        }
    }
    let live_rows = index.len();
    drop(index);

    let mut guard = match runtime.dirty_ids.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    for id in &dirty_ids {
        guard.remove(id);
    }
    runtime.pending_ops.store(guard.len(), Ordering::Relaxed);
    runtime.stats.inc_rebuild_successes();
    runtime
        .stats
        .add_rebuild_applied_ids(dirty_ids.len() as u64);
    runtime
        .stats
        .set_last_rebuild_duration_ms(started.elapsed().as_millis() as u64);
    runtime.stats.set_last_rebuild_rows(live_rows);
    Ok(())
}
