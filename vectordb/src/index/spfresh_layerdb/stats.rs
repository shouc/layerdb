use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpFreshLayerDbStats {
    pub total_upserts: u64,
    pub total_deletes: u64,
    pub persist_errors: u64,
    pub rebuild_successes: u64,
    pub rebuild_failures: u64,
    pub last_rebuild_rows: u64,
    pub pending_ops: u64,
}

#[derive(Debug, Default)]
pub(crate) struct SpFreshLayerDbStatsInner {
    total_upserts: AtomicU64,
    total_deletes: AtomicU64,
    persist_errors: AtomicU64,
    rebuild_successes: AtomicU64,
    rebuild_failures: AtomicU64,
    last_rebuild_rows: AtomicU64,
}

impl SpFreshLayerDbStatsInner {
    pub(crate) fn snapshot(&self, pending_ops: u64) -> SpFreshLayerDbStats {
        SpFreshLayerDbStats {
            total_upserts: self.total_upserts.load(Ordering::Relaxed),
            total_deletes: self.total_deletes.load(Ordering::Relaxed),
            persist_errors: self.persist_errors.load(Ordering::Relaxed),
            rebuild_successes: self.rebuild_successes.load(Ordering::Relaxed),
            rebuild_failures: self.rebuild_failures.load(Ordering::Relaxed),
            last_rebuild_rows: self.last_rebuild_rows.load(Ordering::Relaxed),
            pending_ops,
        }
    }

    pub(crate) fn inc_upserts(&self) {
        self.total_upserts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_deletes(&self) {
        self.total_deletes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_persist_errors(&self) {
        self.persist_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_rebuild_successes(&self) {
        self.rebuild_successes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_rebuild_failures(&self) {
        self.rebuild_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_last_rebuild_rows(&self, rows: usize) {
        self.last_rebuild_rows.store(rows as u64, Ordering::Relaxed);
    }
}
