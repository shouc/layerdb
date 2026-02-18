use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpFreshLayerDbStats {
    pub total_upserts: u64,
    pub total_deletes: u64,
    pub persist_errors: u64,
    pub total_persist_upsert_us: u64,
    pub total_persist_delete_us: u64,
    pub rebuild_successes: u64,
    pub rebuild_failures: u64,
    pub total_rebuild_applied_ids: u64,
    pub last_rebuild_duration_ms: u64,
    pub last_rebuild_rows: u64,
    pub total_searches: u64,
    pub total_search_latency_us: u64,
    pub pending_ops: u64,
}

#[derive(Debug, Default)]
pub(crate) struct SpFreshLayerDbStatsInner {
    total_upserts: AtomicU64,
    total_deletes: AtomicU64,
    persist_errors: AtomicU64,
    total_persist_upsert_us: AtomicU64,
    total_persist_delete_us: AtomicU64,
    rebuild_successes: AtomicU64,
    rebuild_failures: AtomicU64,
    total_rebuild_applied_ids: AtomicU64,
    last_rebuild_duration_ms: AtomicU64,
    last_rebuild_rows: AtomicU64,
    total_searches: AtomicU64,
    total_search_latency_us: AtomicU64,
}

impl SpFreshLayerDbStatsInner {
    pub(crate) fn snapshot(&self, pending_ops: u64) -> SpFreshLayerDbStats {
        SpFreshLayerDbStats {
            total_upserts: self.total_upserts.load(Ordering::Relaxed),
            total_deletes: self.total_deletes.load(Ordering::Relaxed),
            persist_errors: self.persist_errors.load(Ordering::Relaxed),
            total_persist_upsert_us: self.total_persist_upsert_us.load(Ordering::Relaxed),
            total_persist_delete_us: self.total_persist_delete_us.load(Ordering::Relaxed),
            rebuild_successes: self.rebuild_successes.load(Ordering::Relaxed),
            rebuild_failures: self.rebuild_failures.load(Ordering::Relaxed),
            total_rebuild_applied_ids: self.total_rebuild_applied_ids.load(Ordering::Relaxed),
            last_rebuild_duration_ms: self.last_rebuild_duration_ms.load(Ordering::Relaxed),
            last_rebuild_rows: self.last_rebuild_rows.load(Ordering::Relaxed),
            total_searches: self.total_searches.load(Ordering::Relaxed),
            total_search_latency_us: self.total_search_latency_us.load(Ordering::Relaxed),
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

    pub(crate) fn add_persist_upsert_us(&self, micros: u64) {
        self.total_persist_upsert_us
            .fetch_add(micros, Ordering::Relaxed);
    }

    pub(crate) fn add_persist_delete_us(&self, micros: u64) {
        self.total_persist_delete_us
            .fetch_add(micros, Ordering::Relaxed);
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

    pub(crate) fn add_rebuild_applied_ids(&self, count: u64) {
        self.total_rebuild_applied_ids
            .fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn set_last_rebuild_duration_ms(&self, millis: u64) {
        self.last_rebuild_duration_ms.store(millis, Ordering::Relaxed);
    }

    pub(crate) fn record_search(&self, latency_us: u64) {
        self.total_searches.fetch_add(1, Ordering::Relaxed);
        self.total_search_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
    }
}
