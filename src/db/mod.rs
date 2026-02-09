mod options;
pub(crate) mod snapshot;

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;

use crate::memtable::MemTableManager;
use crate::version::VersionSet;
use crate::wal::Wal;

pub use options::{DbOptions, ReadOptions, WriteOptions};
pub use snapshot::SnapshotId;

pub type Value = bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Put,
    Del,
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
        let wal = Wal::open(&dir, &options, memtables.clone(), versions.clone()).context("open wal")?;

        Ok(Self {
            inner: Arc::new(DbInner {
                dir,
                options,
                wal,
                memtables,
                versions,
            }),
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

    pub fn write_batch(&self, ops: Vec<Op>, opts: WriteOptions) -> anyhow::Result<()> {
        self.inner.wal.write_batch(&ops, opts)
    }

    pub fn create_snapshot(&self) -> anyhow::Result<SnapshotId> {
        self.inner.versions.snapshots().create_snapshot()
    }

    pub fn get(
        &self,
        key: impl AsRef<[u8]>,
        opts: ReadOptions,
    ) -> anyhow::Result<Option<Value>> {
        let snapshot = self.inner.versions.snapshots().resolve_read_snapshot(opts.snapshot)?;

        if let Some(value) = self
            .inner
            .memtables
            .get(key.as_ref(), snapshot)
            .context("memtable get")?
        {
            return Ok(value);
        }

        if let Some(value) = self
            .inner
            .versions
            .get(key.as_ref(), snapshot)
            .context("sst get")?
        {
            return Ok(value);
        }

        Ok(None)
    }

    pub fn iter(&self, range: Range, opts: ReadOptions) -> anyhow::Result<crate::db::DbIterator> {
        let snapshot = self.inner.versions.snapshots().resolve_read_snapshot(opts.snapshot)?;
        crate::db::iterator::DbIterator::new(
            self.inner.memtables.iter(range.clone(), snapshot)?,
            self.inner.versions.iter(range, snapshot)?,
            snapshot,
        )
    }

    pub fn compact_range(&self, _range: Option<Range>) -> anyhow::Result<()> {
        // v1: manual compaction triggers a conservative full L0->L1 compaction.
        self.inner.versions.compact_l0_to_l1(&self.inner.options)
    }

    pub fn ingest_sst(&self, _sst_path: impl AsRef<Path>) -> anyhow::Result<()> {
        anyhow::bail!("ingest_sst not implemented in v1")
    }
}

pub mod iterator;

pub use iterator::DbIterator;
