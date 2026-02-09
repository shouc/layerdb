mod manifest;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use crate::db::iterator::range_contains;
use crate::db::{DbOptions, OpKind, Range, Value};
use crate::db::snapshot::SnapshotTracker;
use crate::internal_key::{InternalKey, KeyKind};
use crate::sst::{SstReader, SstProperties};
use crate::version::manifest::{AddFile, Manifest, ManifestRecord};

/// Placeholder for version set + manifest.
#[derive(Debug)]
pub struct VersionSet {
    dir: PathBuf,
    snapshots: Arc<SnapshotTracker>,
    next_file_id: AtomicU64,
    files: parking_lot::RwLock<Vec<AddFile>>, // v1: L0 only
    manifest: parking_lot::Mutex<Manifest>,
}

impl VersionSet {
    pub fn recover(dir: &Path, _options: &DbOptions) -> anyhow::Result<Self> {
        let (manifest, state) = Manifest::open(dir)?;
        let mut files = Vec::new();
        let mut max_file_id = 0u64;
        for (_level, level_files) in state.levels {
            for (_id, add) in level_files {
                max_file_id = max_file_id.max(add.file_id);
                files.push(add);
            }
        }
        files.sort_by(|a, b| a.file_id.cmp(&b.file_id));
        Ok(Self {
            dir: dir.to_path_buf(),
            snapshots: Arc::new(SnapshotTracker::new()),
            next_file_id: AtomicU64::new(max_file_id.saturating_add(1).max(1)),
            files: parking_lot::RwLock::new(files),
            manifest: parking_lot::Mutex::new(manifest),
        })
    }

    pub fn snapshots(&self) -> &SnapshotTracker {
        &self.snapshots
    }

    pub(crate) fn snapshots_handle(&self) -> Arc<SnapshotTracker> {
        self.snapshots.clone()
    }

    pub(crate) fn allocate_file_id(&self) -> u64 {
        self.next_file_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn get(&self, key: &[u8], snapshot_seqno: u64) -> anyhow::Result<Option<Option<Value>>> {
        // v1: L0 files only, searched newest-first.
        let files = self.files.read();
        for add in files.iter().rev() {
            if !range_contains(
                &(std::ops::Bound::Included(add.smallest_user_key.clone()), std::ops::Bound::Included(add.largest_user_key.clone())),
                key,
            ) {
                continue;
            }
            let path = self
                .dir
                .join("sst")
                .join(format!("sst_{:016}.sst", add.file_id));
            if !path.exists() {
                continue;
            }
            let reader = SstReader::open(&path)?;
            if let Some(v) = reader.get(key, snapshot_seqno)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    pub fn iter(&self, range: Range, snapshot_seqno: u64) -> anyhow::Result<SstIter> {
        let files = self.files.read().clone();
        SstIter::new(self.dir.clone(), files, snapshot_seqno, range)
    }

    pub fn compact_l0_to_l1(&self, _options: &DbOptions) -> anyhow::Result<()> {
        // v1: implement conservative L0->L1 compaction.
        Ok(())
    }

    pub(crate) fn install_sst(&self, file_id: u64, props: &SstProperties) -> anyhow::Result<()> {
        let add = AddFile {
            file_id,
            level: 0,
            smallest_user_key: props.smallest_user_key.clone(),
            largest_user_key: props.largest_user_key.clone(),
            table_root: props.table_root,
            size_bytes: props.data_bytes + props.index_bytes,
        };
        {
            let mut manifest = self.manifest.lock();
            manifest.append(&ManifestRecord::AddFile(add.clone()), true)?;
            manifest.sync_dir()?;
        }
        self.files.write().push(add);
        Ok(())
    }
}

pub struct SstIter {
    entries: Vec<SstEntry>,
    index: usize,
    snapshot_seqno: u64,
}

#[derive(Debug, Clone)]
struct SstEntry {
    key: InternalKey,
    value: Bytes,
}

impl SstIter {
    fn new(dir: PathBuf, files: Vec<AddFile>, snapshot_seqno: u64, range: Range) -> anyhow::Result<Self> {
        let bounds = crate::memtable::bounds_from_range(&range);
        let mut entries = Vec::new();

        for file in files {
            let path = dir.join("sst").join(format!("sst_{:016}.sst", file.file_id));
            let reader = match SstReader::open(&path) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let mut iter = reader.iter(snapshot_seqno)?;
            iter.seek_to_first();
            while let Some(next) = iter.next() {
                let (user_key, seqno, kind, value) = next?;
                if !range_contains(&bounds, user_key.as_ref()) {
                    continue;
                }
                let key_kind = match kind {
                    OpKind::Put => KeyKind::Put,
                    OpKind::Del => KeyKind::Del,
                };
                entries.push(SstEntry {
                    key: InternalKey::new(user_key, seqno, key_kind),
                    value,
                });
            }
        }

        entries.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(Self {
            entries,
            index: 0,
            snapshot_seqno,
        })
    }
}

impl SstIter {
    pub fn seek_to_first(&mut self) {
        self.index = 0;
    }

    pub fn seek(&mut self, user_key: &[u8]) {
        let target = InternalKey::new(Bytes::copy_from_slice(user_key), self.snapshot_seqno, KeyKind::Meta);
        self.index = match self
            .entries
            .binary_search_by(|entry| entry.key.cmp(&target))
        {
            Ok(i) | Err(i) => i,
        };
    }

    pub fn next(&mut self) -> Option<anyhow::Result<(Bytes, u64, OpKind, Bytes)>> {
        let entry = self.entries.get(self.index)?.clone();
        self.index += 1;
        let kind = match entry.key.kind {
            KeyKind::Put => OpKind::Put,
            KeyKind::Del => OpKind::Del,
            other => return Some(Err(anyhow::anyhow!("unexpected key kind: {other:?}"))),
        };
        Some(Ok((entry.key.user_key, entry.key.seqno, kind, entry.value)))
    }
}
