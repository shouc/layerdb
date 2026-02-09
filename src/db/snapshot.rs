use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotId(pub u64);

/// Tracks pinned snapshots and the minimum pinned sequence.
///
/// `SnapshotId` is separate from the pinned seqno. A snapshot pins a seqno at
/// the time it is created.
#[derive(Debug)]
pub struct SnapshotTracker {
    next_id: AtomicU64,
    inner: Mutex<SnapshotTrackerInner>,
}

#[derive(Debug, Default)]
struct SnapshotTrackerInner {
    latest_seqno: u64,
    pinned: std::collections::BTreeMap<u64, u64>,
}

impl SnapshotTracker {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            inner: Mutex::new(SnapshotTrackerInner::default()),
        }
    }

    pub fn set_latest_seqno(&self, seqno: u64) {
        let mut guard = self.inner.lock();
        guard.latest_seqno = guard.latest_seqno.max(seqno);
    }

    pub fn latest_seqno(&self) -> u64 {
        self.inner.lock().latest_seqno
    }

    pub fn min_pinned_seqno(&self) -> u64 {
        let guard = self.inner.lock();
        guard
            .pinned
            .values()
            .copied()
            .min()
            .unwrap_or(guard.latest_seqno)
    }

    pub fn create_snapshot(&self) -> anyhow::Result<SnapshotId> {
        self.create_snapshot_at(self.latest_seqno())
    }

    pub fn create_snapshot_at(&self, seqno: u64) -> anyhow::Result<SnapshotId> {
        let latest = self.latest_seqno();
        if seqno > latest {
            anyhow::bail!("snapshot seqno {seqno} is ahead of latest {latest}");
        }

        let id = SnapshotId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.inner.lock().pinned.insert(id.0, seqno);
        Ok(id)
    }

    pub fn drop_snapshot(&self, id: SnapshotId) {
        self.inner.lock().pinned.remove(&id.0);
    }

    pub fn resolve_read_snapshot(&self, id: Option<SnapshotId>) -> anyhow::Result<u64> {
        match id {
            None => Ok(self.latest_seqno()),
            Some(snapshot_id) => {
                let guard = self.inner.lock();
                guard
                    .pinned
                    .get(&snapshot_id.0)
                    .copied()
                    .ok_or_else(|| anyhow::anyhow!("unknown snapshot id"))
            }
        }
    }
}
