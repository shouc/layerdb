mod compaction_ops;
mod io_tiers;
mod iter;
pub mod manifest;

use std::collections::{BTreeMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::db::iterator::range_contains;
use crate::db::snapshot::SnapshotTracker;
use crate::db::{DbOptions, LookupResult, OpKind, Range, Value};
use crate::internal_key::{InternalKey, KeyKind};
use crate::range_tombstone::RangeTombstone;
use crate::sst::{SstIoContext, SstProperties, SstReader};
use crate::tier::StorageTier;
use crate::version::manifest::{
    AddFile, BranchArchive, DeleteFile, DropBranch, DropBranchArchive, FreezeFile, Manifest,
    ManifestRecord, MoveFile, VersionEdit,
};
pub use iter::SstIter;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LevelMetricsSnapshot {
    pub file_count: usize,
    pub bytes: u64,
    pub overlap_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TierMetricsSnapshot {
    pub s3_gets: u64,
    pub s3_get_cache_hits: u64,
    pub s3_puts: u64,
    pub s3_deletes: u64,
}

#[derive(Debug, Default)]
struct TierCounters {
    s3_gets: AtomicU64,
    s3_get_cache_hits: AtomicU64,
    s3_puts: AtomicU64,
    s3_deletes: AtomicU64,
}

impl TierCounters {
    fn snapshot(&self) -> TierMetricsSnapshot {
        TierMetricsSnapshot {
            s3_gets: self.s3_gets.load(Ordering::Relaxed),
            s3_get_cache_hits: self.s3_get_cache_hits.load(Ordering::Relaxed),
            s3_puts: self.s3_puts.load(Ordering::Relaxed),
            s3_deletes: self.s3_deletes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct VersionMetrics {
    pub reader_cache: crate::cache::CacheStats,
    pub data_block_cache: Option<crate::cache::CacheStats>,
    pub levels: BTreeMap<u8, LevelMetricsSnapshot>,
    pub compaction_debt_bytes_by_level: BTreeMap<u8, u64>,
    pub l0_file_debt: usize,
    pub compaction_candidate_level: Option<u8>,
    pub compaction_candidate_score: Option<f64>,
    pub should_compact: bool,
    pub frozen_s3_files: usize,
    pub tier: TierMetricsSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenObjectMeta {
    pub file_id: u64,
    pub level: u8,
    pub object_id: String,
    pub object_version: Option<String>,
    pub superblock_bytes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3SuperblockMeta {
    id: u32,
    len: u32,
    hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3ObjectMetaFile {
    file_id: u64,
    level: u8,
    object_id: String,
    superblock_bytes: u32,
    total_bytes: u64,
    superblocks: Vec<S3SuperblockMeta>,
}

/// Version set + manifest.
#[derive(Debug)]
pub struct VersionSet {
    dir: PathBuf,
    options: DbOptions,
    snapshots: Arc<SnapshotTracker>,
    next_file_id: AtomicU64,
    levels: parking_lot::RwLock<Levels>,
    manifest: parking_lot::Mutex<Manifest>,
    reader_cache: crate::cache::ClockProCache<PathBuf, SstReader>,
    data_block_cache: Option<crate::sst::DataBlockCacheHandle>,
    sst_io_ctx: Option<Arc<SstIoContext>>,
    branches: RwLock<std::collections::BTreeMap<String, u64>>,
    current_branch: RwLock<String>,
    frozen_objects: RwLock<std::collections::BTreeMap<u64, FrozenObjectMeta>>,
    branch_archives: RwLock<std::collections::BTreeMap<String, BranchArchive>>,
    tier_counters: TierCounters,
    s3_store: crate::s3::S3ObjectStore,
}

#[derive(Debug, Default, Clone)]
struct Levels {
    /// L0 is searched newest-first and may overlap.
    l0: Vec<AddFile>,
    /// L1 is non-overlapping and sorted by user key range.
    l1: Vec<AddFile>,
}

impl Levels {
    pub(crate) fn all_files(&self) -> Vec<AddFile> {
        let mut out = Vec::with_capacity(self.l0.len() + self.l1.len());
        out.extend(self.l0.clone());
        out.extend(self.l1.clone());
        out
    }
}

impl VersionSet {
    pub(crate) fn all_files_snapshot(&self) -> Vec<AddFile> {
        self.levels.read().all_files()
    }

    pub fn recover(dir: &Path, options: &DbOptions) -> anyhow::Result<Self> {
        let (manifest, state) = Manifest::open(dir)?;
        let mut l0 = Vec::new();
        let mut l1 = Vec::new();
        let mut max_file_id = 0u64;
        let mut max_seqno = 0u64;
        for (_level, level_files) in state.levels {
            for (_id, add) in level_files {
                max_file_id = max_file_id.max(add.file_id);
                max_seqno = max_seqno.max(add.max_seqno);
                match add.level {
                    0 => l0.push(add),
                    1 => l1.push(add),
                    _ => {
                        // v1: ignore unknown levels.
                    }
                }
            }
        }
        l0.sort_by(|a, b| a.file_id.cmp(&b.file_id));
        l1.sort_by(|a, b| a.smallest_user_key.cmp(&b.smallest_user_key));
        let snapshots = Arc::new(SnapshotTracker::new());
        snapshots.set_latest_seqno(max_seqno);

        let mut branches = state.branches;
        branches.entry("main".to_string()).or_insert(max_seqno);

        let mut frozen_objects = std::collections::BTreeMap::new();
        for (file_id, freeze) in state.frozen_objects {
            frozen_objects.insert(
                file_id,
                FrozenObjectMeta {
                    file_id,
                    level: freeze.level,
                    object_id: freeze.object_id,
                    object_version: freeze.object_version,
                    superblock_bytes: freeze.superblock_bytes,
                },
            );
        }

        let branch_archives = state.branch_archives;

        let sst_io_ctx = if options.sst_use_io_executor_reads || options.sst_use_io_executor_writes
        {
            let io = crate::io::UringExecutor::with_backend(
                options.io_max_in_flight.max(1),
                options.io_backend,
            );

            #[cfg(all(feature = "native-uring", target_os = "linux"))]
            let buf_pool = io
                .native_uring()
                .map(|native| {
                    crate::io::BufPool::with_native_uring(
                        native,
                        [4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024],
                        64,
                    )
                })
                .unwrap_or_else(crate::io::BufPool::default);

            #[cfg(not(all(feature = "native-uring", target_os = "linux")))]
            let buf_pool = crate::io::BufPool::default();

            Some(Arc::new(SstIoContext::new(io, buf_pool)))
        } else {
            None
        };

        let s3_store =
            crate::s3::S3ObjectStore::for_options(options.s3.clone(), dir.join("sst_s3"))
                .context("initialize s3 object store")?;

        Ok(Self {
            dir: dir.to_path_buf(),
            options: options.clone(),
            snapshots,
            next_file_id: AtomicU64::new(max_file_id.saturating_add(1).max(1)),
            levels: parking_lot::RwLock::new(Levels { l0, l1 }),
            manifest: parking_lot::Mutex::new(manifest),
            reader_cache: crate::cache::ClockProCache::new(options.sst_reader_cache_entries),
            data_block_cache: (options.block_cache_entries > 0).then(|| {
                Arc::new(crate::cache::ClockProCache::new(
                    options.block_cache_entries,
                ))
            }),
            sst_io_ctx,
            branches: RwLock::new(branches),
            current_branch: RwLock::new("main".to_string()),
            frozen_objects: RwLock::new(frozen_objects),
            branch_archives: RwLock::new(branch_archives),
            tier_counters: TierCounters::default(),
            s3_store,
        })
    }

    pub fn frozen_objects_snapshot(&self) -> Vec<FrozenObjectMeta> {
        self.frozen_objects.read().values().cloned().collect()
    }

    pub fn create_branch(&self, name: &str, from_seqno: u64) -> anyhow::Result<()> {
        if name.is_empty() {
            anyhow::bail!("branch name cannot be empty");
        }
        if !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.')
        {
            anyhow::bail!("branch name contains invalid characters");
        }

        {
            let branches = self.branches.read();
            if branches.contains_key(name) {
                anyhow::bail!("branch already exists: {name}");
            }
        }

        {
            let mut manifest = self.manifest.lock();
            manifest.append(
                &ManifestRecord::BranchHead(crate::version::manifest::BranchHead {
                    name: name.to_string(),
                    seqno: from_seqno,
                }),
                true,
            )?;
            manifest.sync_dir()?;
        }

        self.branches.write().insert(name.to_string(), from_seqno);
        Ok(())
    }

    pub fn delete_branch(&self, name: &str) -> anyhow::Result<()> {
        if name == "main" {
            anyhow::bail!("cannot delete main branch");
        }

        {
            let branches = self.branches.read();
            if !branches.contains_key(name) {
                anyhow::bail!("unknown branch: {name}");
            }
        }

        {
            let mut manifest = self.manifest.lock();
            manifest.append(
                &ManifestRecord::DropBranch(DropBranch {
                    name: name.to_string(),
                }),
                true,
            )?;
            manifest.sync_dir()?;
        }

        self.branches.write().remove(name);

        if self.current_branch() == name {
            *self.current_branch.write() = "main".to_string();
        }

        Ok(())
    }

    pub fn checkout_branch(&self, name: &str) -> anyhow::Result<u64> {
        let seqno = {
            let branches = self.branches.read();
            branches
                .get(name)
                .copied()
                .ok_or_else(|| anyhow::anyhow!("unknown branch: {name}"))?
        };
        let previous = self.current_branch();
        if previous == "main" && name != "main" {
            let main_head = self.current_branch_seqno();
            self.persist_branch_head("main", main_head)?;
        }
        *self.current_branch.write() = name.to_string();
        Ok(seqno)
    }

    pub fn list_branches(&self) -> Vec<(String, u64)> {
        self.branches
            .read()
            .iter()
            .map(|(name, seq)| (name.clone(), *seq))
            .collect()
    }

    pub fn list_branch_archives(&self) -> Vec<BranchArchive> {
        self.branch_archives.read().values().cloned().collect()
    }

    pub fn add_branch_archive(&self, archive: BranchArchive) -> anyhow::Result<()> {
        {
            let mut manifest = self.manifest.lock();
            manifest.append(&ManifestRecord::BranchArchive(archive.clone()), true)?;
            manifest.sync_dir()?;
        }

        self.branch_archives
            .write()
            .insert(archive.archive_id.clone(), archive);
        Ok(())
    }

    pub fn drop_branch_archive(&self, archive_id: &str) -> anyhow::Result<()> {
        {
            let mut manifest = self.manifest.lock();
            manifest.append(
                &ManifestRecord::DropBranchArchive(DropBranchArchive {
                    archive_id: archive_id.to_string(),
                }),
                true,
            )?;
            manifest.sync_dir()?;
        }

        self.branch_archives.write().remove(archive_id);
        Ok(())
    }

    pub fn current_branch(&self) -> String {
        self.current_branch.read().clone()
    }

    pub fn current_branch_seqno(&self) -> u64 {
        let name = self.current_branch();
        self.branches
            .read()
            .get(&name)
            .copied()
            .unwrap_or_else(|| self.latest_seqno())
    }

    fn persist_branch_head(&self, name: &str, seqno: u64) -> anyhow::Result<()> {
        let mut manifest = self.manifest.lock();
        manifest.append(
            &ManifestRecord::BranchHead(crate::version::manifest::BranchHead {
                name: name.to_string(),
                seqno,
            }),
            true,
        )?;
        manifest.sync_dir()?;
        Ok(())
    }

    pub fn advance_current_branch(&self, seqno: u64) -> anyhow::Result<()> {
        let branch_name = self.current_branch();
        let current_head = self.branches.read().get(&branch_name).copied().unwrap_or(0);
        if seqno <= current_head {
            return Ok(());
        }

        // Hot-path optimization: the main branch head can be reconstructed from
        // WAL/manifest file metadata during recovery, so avoid per-write
        // manifest fsync for foreground puts/deletes.
        if branch_name == "main" {
            let mut branches = self.branches.write();
            let head = branches.entry(branch_name).or_insert(0);
            if seqno > *head {
                *head = seqno;
            }
            return Ok(());
        }

        self.persist_branch_head(&branch_name, seqno)?;

        let mut branches = self.branches.write();
        let head = branches.entry(branch_name).or_insert(0);
        if seqno > *head {
            *head = seqno;
        }
        Ok(())
    }

    pub fn persist_main_branch_head(&self) -> anyhow::Result<()> {
        let seqno = self.branches.read().get("main").copied().unwrap_or(0);
        self.persist_branch_head("main", seqno)
    }
}

fn parse_object_file_id(object_id: &str) -> Option<u64> {
    let (_prefix, hex) = object_id.rsplit_once('-')?;
    u64::from_str_radix(hex, 16).ok()
}

fn parse_file_id_from_s3_key(key: &str) -> Option<u64> {
    let mut parts = key.split('/');
    let level = parts.next()?;
    if !level.starts_with('L') {
        return None;
    }
    let object_id = parts.next()?;
    parse_object_file_id(object_id)
}

fn range_overlaps_file(
    bounds: &(std::ops::Bound<Bytes>, std::ops::Bound<Bytes>),
    file_smallest: &[u8],
    file_largest: &[u8],
) -> bool {
    let lower_ok = match &bounds.0 {
        std::ops::Bound::Unbounded => true,
        std::ops::Bound::Included(start) => file_largest >= start.as_ref(),
        std::ops::Bound::Excluded(start) => file_largest > start.as_ref(),
    };

    let upper_ok = match &bounds.1 {
        std::ops::Bound::Unbounded => true,
        std::ops::Bound::Included(end) => file_smallest <= end.as_ref(),
        std::ops::Bound::Excluded(end) => file_smallest < end.as_ref(),
    };

    lower_ok && upper_ok
}

fn find_l1_file<'a>(l1: &'a [AddFile], key: &[u8]) -> Option<&'a AddFile> {
    let mut lo = 0usize;
    let mut hi = l1.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let f = &l1[mid];
        if key < f.smallest_user_key.as_ref() {
            hi = mid;
        } else if key > f.largest_user_key.as_ref() {
            lo = mid + 1;
        } else {
            return Some(f);
        }
    }
    None
}

fn overlaps(a_smallest: &[u8], a_largest: &[u8], b_smallest: &[u8], b_largest: &[u8]) -> bool {
    !(a_largest < b_smallest || b_largest < a_smallest)
}

fn parse_sst_file_id(name: &str) -> Option<u64> {
    if !name.starts_with("sst_") || !name.ends_with(".sst") {
        return None;
    }
    let hex = &name[4..name.len() - 4];
    u64::from_str_radix(hex, 16).ok()
}

fn estimate_overlap_bytes(files: &[AddFile]) -> u64 {
    if files.len() < 2 {
        return 0;
    }

    let mut overlap = 0u64;
    for left in 0..files.len() {
        for right in (left + 1)..files.len() {
            if overlaps(
                files[left].smallest_user_key.as_ref(),
                files[left].largest_user_key.as_ref(),
                files[right].smallest_user_key.as_ref(),
                files[right].largest_user_key.as_ref(),
            ) {
                overlap =
                    overlap.saturating_add(files[left].size_bytes.min(files[right].size_bytes));
            }
        }
    }

    overlap
}

fn compaction_inputs_cover_overlaps(
    all_l0: &[AddFile],
    all_l1: &[AddFile],
    selected_l0: &[AddFile],
    selected_l1: &[AddFile],
    smallest: &[u8],
    largest: &[u8],
) -> bool {
    let selected_l0_ids: HashSet<u64> = selected_l0.iter().map(|f| f.file_id).collect();
    let selected_l1_ids: HashSet<u64> = selected_l1.iter().map(|f| f.file_id).collect();

    for file in all_l0 {
        if overlaps(
            smallest,
            largest,
            file.smallest_user_key.as_ref(),
            file.largest_user_key.as_ref(),
        ) && !selected_l0_ids.contains(&file.file_id)
        {
            return false;
        }
    }

    for file in all_l1 {
        if overlaps(
            smallest,
            largest,
            file.smallest_user_key.as_ref(),
            file.largest_user_key.as_ref(),
        ) && !selected_l1_ids.contains(&file.file_id)
        {
            return false;
        }
    }

    true
}

fn compact_user_key_entries(
    entries: Vec<(InternalKey, Bytes)>,
    min_snapshot_seqno: u64,
    can_drop_obsolete_point_tombstones: bool,
) -> Vec<(InternalKey, Bytes)> {
    let mut out = Vec::new();
    let mut kept_one_below_min = false;

    for (ikey, value) in entries {
        match ikey.kind {
            KeyKind::Put | KeyKind::Del => {
                if ikey.seqno >= min_snapshot_seqno {
                    out.push((ikey, value));
                    continue;
                }

                if kept_one_below_min {
                    continue;
                }

                kept_one_below_min = true;
                if ikey.kind == KeyKind::Put || !can_drop_obsolete_point_tombstones {
                    out.push((ikey, value));
                }
            }
            KeyKind::RangeDel => {
                out.push((ikey, value));
            }
            _ => {}
        }
    }

    out
}

fn drop_obsolete_range_tombstones_bottommost(
    entries: Vec<(InternalKey, Bytes)>,
    min_snapshot_seqno: u64,
    can_drop_obsolete_range_tombstones: bool,
) -> Vec<(InternalKey, Bytes)> {
    if !can_drop_obsolete_range_tombstones {
        return entries;
    }

    let droppable: Vec<RangeTombstone> = entries
        .iter()
        .filter(|(key, _)| key.kind == KeyKind::RangeDel && key.seqno < min_snapshot_seqno)
        .map(|(key, value)| RangeTombstone::new(key.user_key.clone(), value.clone(), key.seqno))
        .collect();

    if droppable.is_empty() {
        return entries;
    }

    entries
        .into_iter()
        .filter(|(key, value)| match key.kind {
            KeyKind::RangeDel => !droppable.iter().any(|t| {
                t.seqno == key.seqno
                    && t.start_key == key.user_key
                    && t.end_key.as_ref() == value.as_ref()
            }),
            KeyKind::Put | KeyKind::Del => !droppable.iter().any(|t| {
                key.seqno < t.seqno
                    && t.start_key.as_ref() <= key.user_key.as_ref()
                    && key.user_key.as_ref() < t.end_key.as_ref()
            }),
            _ => true,
        })
        .collect()
}
