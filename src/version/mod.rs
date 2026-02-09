use std::path::{Path, PathBuf};

use crate::db::{DbOptions, Range, Value};
use crate::db::snapshot::SnapshotTracker;

/// Placeholder for version set + manifest.
#[derive(Debug)]
pub struct VersionSet {
    dir: PathBuf,
    snapshots: SnapshotTracker,
}

impl VersionSet {
    pub fn recover(dir: &Path, _options: &DbOptions) -> anyhow::Result<Self> {
        Ok(Self {
            dir: dir.to_path_buf(),
            snapshots: SnapshotTracker::new(),
        })
    }

    pub fn snapshots(&self) -> &SnapshotTracker {
        &self.snapshots
    }

    pub fn get(&self, _key: &[u8], _snapshot_seqno: u64) -> anyhow::Result<Option<Option<Value>>> {
        Ok(None)
    }

    pub fn iter(&self, _range: Range, _snapshot_seqno: u64) -> anyhow::Result<SstIter> {
        Ok(SstIter {})
    }

    pub fn compact_l0_to_l1(&self, _options: &DbOptions) -> anyhow::Result<()> {
        // v1: implement conservative L0->L1 compaction.
        Ok(())
    }
}

pub struct SstIter {}

impl SstIter {
    pub fn seek_to_first(&mut self) {}
    pub fn seek(&mut self, _key: &[u8]) {}
    pub fn next(&mut self) -> Option<anyhow::Result<(bytes::Bytes, u64, crate::db::OpKind, bytes::Bytes)>> {
        None
    }
}

