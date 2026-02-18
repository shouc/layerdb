use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::JoinHandle;

use layerdb::Db;

use crate::index::{SpFreshConfig, SpFreshIndex};

use super::stats::SpFreshLayerDbStatsInner;
use super::storage::load_rows;
use super::sync_utils::lock_write;

pub(crate) struct RebuilderRuntime {
    pub db: Db,
    pub spfresh_cfg: SpFreshConfig,
    pub rebuild_pending_ops: usize,
    pub rebuild_interval: std::time::Duration,
    pub active_generation: Arc<AtomicU64>,
    pub index: Arc<RwLock<SpFreshIndex>>,
    pub update_gate: Arc<Mutex<()>>,
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
    let _update_guard = match runtime.update_gate.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };

    if !runtime.has_pending_work() {
        return Ok(());
    }

    let generation = runtime.active_generation.load(Ordering::Relaxed);
    let rows = load_rows(&runtime.db, generation)?;
    let rebuilt = SpFreshIndex::build(runtime.spfresh_cfg.clone(), &rows);
    *lock_write(&runtime.index) = rebuilt;
    runtime.stats.inc_rebuild_successes();
    runtime.stats.set_last_rebuild_rows(rows.len());
    runtime.pending_ops.store(0, Ordering::Relaxed);
    Ok(())
}
