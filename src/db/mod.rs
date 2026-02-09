mod options;
pub(crate) mod snapshot;

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Context;
use rayon::prelude::*;

use crate::memtable::MemTableManager;
use crate::version::VersionSet;
use crate::wal::Wal;

pub use options::{DbOptions, ReadOptions, WriteOptions};
pub use snapshot::SnapshotId;

pub type Value = bytes::Bytes;

#[derive(Debug, Clone)]
pub(crate) struct LookupResult {
    pub seqno: u64,
    pub value: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Put,
    Del,
    /// Range deletion tombstone.
    ///
    /// v2: stored and applied separately from point keys.
    RangeDel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op {
    pub kind: OpKind,
    pub key: bytes::Bytes,
    pub value: bytes::Bytes,
}

impl Op {
    pub fn put(key: impl Into<bytes::Bytes>, value: impl Into<bytes::Bytes>) -> Self {
        Self {
            kind: OpKind::Put,
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn delete(key: impl Into<bytes::Bytes>) -> Self {
        Self {
            kind: OpKind::Del,
            key: key.into(),
            value: bytes::Bytes::new(),
        }
    }

    /// Delete all keys in `[start, end)`.
    pub fn delete_range(start: impl Into<bytes::Bytes>, end: impl Into<bytes::Bytes>) -> Self {
        Self {
            kind: OpKind::RangeDel,
            key: start.into(),
            value: end.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Range {
    pub start: Bound<bytes::Bytes>,
    pub end: Bound<bytes::Bytes>,
}

impl Range {
    pub fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

/// Database handle.
///
/// Semantics (v1):
/// - Read-your-writes per handle: each handle tracks an acknowledged sequence
///   number and uses that as the default snapshot for reads.
/// - Explicit snapshots provide consistent reads at a seqno.
pub struct Db {
    inner: Arc<DbInner>,
    read_snapshot: Arc<AtomicU64>,
}

struct DbInner {
    dir: PathBuf,
    options: DbOptions,
    wal: Wal,
    memtables: Arc<MemTableManager>,
    versions: Arc<VersionSet>,
}

impl Db {
    pub fn open(path: impl AsRef<Path>, options: DbOptions) -> anyhow::Result<Self> {
        let dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).with_context(|| format!("create dir {dir:?}"))?;

        let versions = Arc::new(VersionSet::recover(&dir, &options).context("recover versionset")?);
        let memtables = Arc::new(MemTableManager::new(options.memtable_shards));
        let wal =
            Wal::open(&dir, &options, memtables.clone(), versions.clone()).context("open wal")?;
        let read_snapshot = Arc::new(AtomicU64::new(versions.current_branch_seqno()));

        Ok(Self {
            inner: Arc::new(DbInner {
                dir,
                options,
                wal,
                memtables,
                versions,
            }),
            read_snapshot,
        })
    }

    pub fn put(
        &self,
        key: impl Into<bytes::Bytes>,
        value: impl Into<bytes::Bytes>,
        opts: WriteOptions,
    ) -> anyhow::Result<()> {
        self.write_batch(vec![Op::put(key, value)], opts)
    }

    pub fn delete(&self, key: impl Into<bytes::Bytes>, opts: WriteOptions) -> anyhow::Result<()> {
        self.write_batch(vec![Op::delete(key)], opts)
    }

    pub fn delete_range(
        &self,
        start: impl Into<bytes::Bytes>,
        end: impl Into<bytes::Bytes>,
        opts: WriteOptions,
    ) -> anyhow::Result<()> {
        self.write_batch(vec![Op::delete_range(start, end)], opts)
    }

    pub fn write_batch(&self, ops: Vec<Op>, opts: WriteOptions) -> anyhow::Result<()> {
        self.inner.wal.write_batch(&ops, opts)?;
        self.read_snapshot
            .store(self.inner.wal.last_acknowledged_seqno(), Ordering::Relaxed);
        Ok(())
    }

    pub fn create_snapshot(&self) -> anyhow::Result<SnapshotId> {
        self.inner
            .versions
            .snapshots()
            .create_snapshot_at(self.default_read_snapshot())
    }

    pub fn release_snapshot(&self, snapshot: SnapshotId) {
        self.inner.versions.snapshots().drop_snapshot(snapshot);
    }

    pub fn create_branch(
        &self,
        name: impl AsRef<str>,
        from_snapshot: Option<SnapshotId>,
    ) -> anyhow::Result<()> {
        let seqno = match from_snapshot {
            Some(snapshot) => self
                .inner
                .versions
                .snapshots()
                .resolve_read_snapshot(Some(snapshot))?,
            None => self.default_read_snapshot(),
        };
        self.inner.versions.create_branch(name.as_ref(), seqno)
    }

    pub fn checkout(&self, branch: impl AsRef<str>) -> anyhow::Result<()> {
        let seqno = self.inner.versions.checkout_branch(branch.as_ref())?;
        self.read_snapshot.store(seqno, Ordering::Relaxed);
        Ok(())
    }

    pub fn list_branches(&self) -> Vec<(String, u64)> {
        self.inner.versions.list_branches()
    }

    pub fn current_branch(&self) -> String {
        self.inner.versions.current_branch()
    }

    pub fn get(&self, key: impl AsRef<[u8]>, opts: ReadOptions) -> anyhow::Result<Option<Value>> {
        let snapshot = match opts.snapshot {
            Some(snapshot) => self
                .inner
                .versions
                .snapshots()
                .resolve_read_snapshot(Some(snapshot))?,
            None => self.default_read_snapshot(),
        };

        let mem = self
            .inner
            .memtables
            .get(key.as_ref(), snapshot)
            .context("memtable get")?;
        let sst = self
            .inner
            .versions
            .get(key.as_ref(), snapshot)
            .context("sst get")?;

        let chosen = match (mem, sst) {
            (Some(a), Some(b)) => {
                if a.seqno >= b.seqno {
                    Some(a)
                } else {
                    Some(b)
                }
            }
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        Ok(chosen.and_then(|r| r.value))
    }

    pub fn iter(&self, range: Range, opts: ReadOptions) -> anyhow::Result<crate::db::DbIterator> {
        let snapshot = match opts.snapshot {
            Some(snapshot) => self
                .inner
                .versions
                .snapshots()
                .resolve_read_snapshot(Some(snapshot))?,
            None => self.default_read_snapshot(),
        };

        let mut range_tombstones = self.inner.memtables.range_tombstones(snapshot);
        range_tombstones.extend(self.inner.versions.range_tombstones(snapshot)?);

        crate::db::iterator::DbIterator::new(
            self.inner.memtables.iter(range.clone(), snapshot)?,
            self.inner.versions.iter(range, snapshot)?,
            snapshot,
            range_tombstones,
        )
    }

    pub fn compact_range(&self, range: Option<Range>) -> anyhow::Result<()> {
        // Ensure current mutable memtable is flushed before manual compaction.
        self.inner.wal.force_rotate_for_flush()?;
        // v1: manual compaction triggers a conservative L0->L1 compaction,
        // optionally constrained to an input key range.
        self.inner
            .versions
            .compact_l0_to_l1(range.as_ref(), &self.inner.options)
    }

    pub fn ingest_sst(&self, sst_path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = sst_path.as_ref();
        let (_file_id, max_seqno) = self
            .inner
            .versions
            .ingest_external_sst(path)
            .with_context(|| format!("ingest sst {}", path.display()))?;

        self.inner
            .wal
            .ensure_next_seqno_at_least(max_seqno.saturating_add(1));
        self.read_snapshot
            .store(self.inner.versions.latest_seqno(), Ordering::Relaxed);
        Ok(())
    }

    pub fn rebalance_tiers(&self) -> anyhow::Result<usize> {
        self.inner.versions.rebalance_level_tiers()
    }

    pub fn freeze_level_to_s3(&self, level: u8, max_files: Option<usize>) -> anyhow::Result<usize> {
        self.inner.versions.freeze_level_to_s3(level, max_files)
    }

    pub fn frozen_objects(&self) -> Vec<crate::version::FrozenObjectMeta> {
        self.inner.versions.frozen_objects_snapshot()
    }

    pub fn scrub_integrity(&self) -> anyhow::Result<ScrubReport> {
        let files = self.inner.versions.all_files_snapshot();

        let results: Vec<anyhow::Result<(u8, u64)>> = files
            .par_iter()
            .map(|add| {
                let path = self
                    .inner
                    .versions
                    .resolve_sst_path(add.level, add.file_id)
                    .with_context(|| format!("resolve sst for scrub file_id={}", add.file_id))?;
                let reader = crate::sst::SstReader::open(&path)
                    .with_context(|| format!("open sst for scrub {}", path.display()))?;

                let mut iter = reader.iter(u64::MAX)?;
                iter.seek_to_first();
                let mut entries = 0u64;
                while let Some(next) = iter.next() {
                    let _ = next?;
                    entries += 1;
                }

                Ok((add.level, entries))
            })
            .collect();

        let mut report = ScrubReport::default();
        for result in results {
            let (level, entries) = result?;
            *report.entries_by_level.entry(level).or_default() += entries;
            report.files_checked += 1;
        }
        Ok(report)
    }

    fn default_read_snapshot(&self) -> u64 {
        self.read_snapshot.load(Ordering::Relaxed)
    }
}

pub mod iterator;

pub use iterator::DbIterator;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScrubReport {
    pub files_checked: usize,
    pub entries_by_level: std::collections::BTreeMap<u8, u64>,
}
