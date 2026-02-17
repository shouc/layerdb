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

use crate::cache::BlockCacheKey;
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
    data_block_cache:
        Option<Arc<crate::cache::ClockProCache<BlockCacheKey, Vec<(InternalKey, Bytes)>>>>,
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

        let sst_io_ctx = if options.sst_use_io_executor_reads || options.sst_use_io_executor_writes {
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

        let s3_store = crate::s3::S3ObjectStore::for_options(options.s3.clone(), dir.join("sst_s3"))
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
        self.branch_archives
            .read()
            .values()
            .cloned()
            .collect()
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

    pub fn advance_current_branch(&self, seqno: u64) -> anyhow::Result<()> {
        let branch_name = self.current_branch();
        let current_head = self.branches.read().get(&branch_name).copied().unwrap_or(0);
        if seqno <= current_head {
            return Ok(());
        }

        {
            let mut manifest = self.manifest.lock();
            manifest.append(
                &ManifestRecord::BranchHead(crate::version::manifest::BranchHead {
                    name: branch_name.clone(),
                    seqno,
                }),
                true,
            )?;
            manifest.sync_dir()?;
        }

        let mut branches = self.branches.write();
        let head = branches.entry(branch_name).or_insert(0);
        if seqno > *head {
            *head = seqno;
        }
        Ok(())
    }

    fn cached_reader(&self, path: &Path) -> anyhow::Result<Arc<SstReader>> {
        let key = path.to_path_buf();
        if let Some(reader) = self.reader_cache.get(&key) {
            return Ok(reader);
        }

        let reader = if self.options.sst_use_io_executor_reads {
            let io_ctx = self
                .sst_io_ctx
                .as_ref()
                .context("sst io executor requested but not configured")?;
            Arc::new(SstReader::open_with_io(
                path,
                io_ctx.clone(),
                self.data_block_cache.clone(),
            )?)
        } else {
            Arc::new(SstReader::open_with_cache(
                path,
                self.data_block_cache.clone(),
            )?)
        };

        self.reader_cache.insert(key, reader.clone());
        Ok(reader)
    }

    fn sst_root_dir(&self, level: u8) -> PathBuf {
        if self.options.enable_hdd_tier && level > self.options.hot_levels_max {
            self.dir.join("sst_hdd")
        } else {
            self.dir.join("sst")
        }
    }

    fn tier_for_level(&self, level: u8) -> StorageTier {
        if self.options.enable_hdd_tier && level > self.options.hot_levels_max {
            StorageTier::Hdd
        } else {
            StorageTier::Nvme
        }
    }

    pub(crate) fn resolve_sst_path(&self, level: u8, file_id: u64) -> anyhow::Result<PathBuf> {
        let primary = self.sst_path(level, file_id);
        let secondary = if primary.parent().is_some_and(|p| p.ends_with("sst_hdd")) {
            self.dir.join("sst").join(format!("sst_{file_id:016x}.sst"))
        } else {
            self.dir
                .join("sst_hdd")
                .join(format!("sst_{file_id:016x}.sst"))
        };

        for candidate in [&primary, &secondary] {
            if candidate.exists() {
                return Ok(candidate.clone());
            }
        }

        let cache = self.sst_cache_path(file_id);
        if cache.exists() {
            let has_s3 = self
                .frozen_objects
                .read()
                .get(&file_id)
                .is_some_and(|frozen| {
                    self.s3_store
                        .exists(&self.s3_object_meta_key(frozen.level, &frozen.object_id))
                        .unwrap_or(false)
                });
            if has_s3 {
                self.tier_counters
                    .s3_get_cache_hits
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Ok(cache);
        }

        if let Some(frozen) = self.frozen_objects.read().get(&file_id).cloned() {
            if self
                .s3_store
                .exists(&self.s3_object_meta_key(frozen.level, &frozen.object_id))
                .unwrap_or(false)
            {
                return self.hydrate_s3_object_to_cache(&frozen);
            }
        }

        anyhow::bail!(
            "manifest references missing sst file_id={file_id} level={level}: primary={primary:?} secondary={secondary:?} cache={cache:?}"
        )
    }

    fn sst_path(&self, level: u8, file_id: u64) -> PathBuf {
        self.sst_root_dir(level)
            .join(format!("sst_{file_id:016x}.sst"))
    }

    fn sst_path_for_tier(&self, tier: StorageTier, file_id: u64) -> PathBuf {
        let dir = match tier {
            StorageTier::Nvme => self.dir.join("sst"),
            StorageTier::Hdd => self.dir.join("sst_hdd"),
            StorageTier::S3 => self.dir.join("sst_s3"),
        };
        dir.join(format!("sst_{file_id:016x}.sst"))
    }

    fn sst_cache_path(&self, file_id: u64) -> PathBuf {
        self.dir
            .join("sst_cache")
            .join(format!("sst_{file_id:016x}.sst"))
    }

    fn hydrate_s3_object_to_cache(&self, frozen: &FrozenObjectMeta) -> anyhow::Result<PathBuf> {
        let cache = self.sst_cache_path(frozen.file_id);
        if cache.exists() {
            self.tier_counters
                .s3_get_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(cache);
        }

        if let Some(parent) = cache.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let tmp = cache.with_extension("tmp");
        let _ = std::fs::remove_file(&tmp);
        let meta = self.load_s3_object_meta(frozen.level, &frozen.object_id)?;
        if meta.file_id != frozen.file_id {
            anyhow::bail!(
                "s3 meta file_id mismatch for {}: expected {} got {}",
                frozen.object_id,
                frozen.file_id,
                meta.file_id
            );
        }

        let mut out = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&tmp)
            .with_context(|| format!("open cache tmp {}", tmp.display()))?;

        for sb in &meta.superblocks {
            let key = self.s3_superblock_key(meta.level, &meta.object_id, sb.id);
            let data = self
                .s3_store
                .get(&key)
                .with_context(|| format!("read superblock key={key}"))?;
            if data.len() != sb.len as usize {
                anyhow::bail!(
                    "superblock length mismatch for {} sb={} expected {} got {}",
                    meta.object_id,
                    sb.id,
                    sb.len,
                    data.len()
                );
            }
            let hash = blake3::hash(&data);
            if hash.as_bytes() != sb.hash.as_slice() {
                anyhow::bail!(
                    "superblock hash mismatch for {} sb={}",
                    meta.object_id,
                    sb.id
                );
            }
            out.write_all(&data)
                .with_context(|| format!("write cache tmp {}", tmp.display()))?;
            self.tier_counters.s3_gets.fetch_add(1, Ordering::Relaxed);
        }

        out.sync_data()
            .with_context(|| format!("sync cache tmp {}", tmp.display()))?;
        drop(out);

        std::fs::rename(&tmp, &cache)
            .with_context(|| format!("rename cache {} -> {}", tmp.display(), cache.display()))?;

        let parent = cache
            .parent()
            .ok_or_else(|| anyhow::anyhow!("cache file has no parent"))?;
        let dir_fd = std::fs::File::open(parent)?;
        dir_fd.sync_all()?;

        Ok(cache)
    }

    fn s3_level_prefix(&self, level: u8) -> String {
        format!("L{level}/")
    }

    fn s3_object_prefix(&self, level: u8, object_id: &str) -> String {
        format!("{}{}", self.s3_level_prefix(level), object_id)
    }

    fn s3_object_meta_key(&self, level: u8, object_id: &str) -> String {
        format!("{}/meta.bin", self.s3_object_prefix(level, object_id))
    }

    fn s3_superblock_key(&self, level: u8, object_id: &str, superblock_id: u32) -> String {
        format!(
            "{}/sb_{superblock_id:08}.bin",
            self.s3_object_prefix(level, object_id)
        )
    }

    fn load_s3_object_meta(&self, level: u8, object_id: &str) -> anyhow::Result<S3ObjectMetaFile> {
        let key = self.s3_object_meta_key(level, object_id);
        let bytes = self
            .s3_store
            .get(&key)
            .with_context(|| format!("read s3 meta key={key}"))?;
        let meta: S3ObjectMetaFile =
            bincode::deserialize(&bytes).with_context(|| format!("decode s3 meta key={key}"))?;
        Ok(meta)
    }

    fn write_s3_object_from_file(
        &self,
        frozen: &FrozenObjectMeta,
        src: &Path,
    ) -> anyhow::Result<Option<String>> {
        let mut file = std::fs::File::open(src)
            .with_context(|| format!("open sst for freeze {}", src.display()))?;
        let mut superblocks = Vec::new();
        let mut total_bytes = 0u64;
        let mut uploaded: Vec<String> = Vec::new();
        let chunk_len: usize = frozen
            .superblock_bytes
            .try_into()
            .context("superblock_bytes overflow")?;
        if chunk_len == 0 {
            anyhow::bail!("superblock_bytes must be > 0");
        }

        let mut id = 0u32;
        loop {
            let mut buf = vec![0u8; chunk_len];
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            buf.truncate(n);

            let hash = blake3::hash(&buf);
            let sb_key = self.s3_superblock_key(frozen.level, &frozen.object_id, id);
            if let Err(err) = self.s3_store.put(&sb_key, &buf) {
                for key in uploaded {
                    let _ = self.s3_store.delete(&key);
                }
                return Err(err).with_context(|| format!("upload superblock key={sb_key}"));
            }
            uploaded.push(sb_key.clone());

            superblocks.push(S3SuperblockMeta {
                id,
                len: n as u32,
                hash: *hash.as_bytes(),
            });
            total_bytes = total_bytes.saturating_add(n as u64);
            id = id.saturating_add(1);

            self.tier_counters.s3_puts.fetch_add(1, Ordering::Relaxed);
        }

        let meta = S3ObjectMetaFile {
            file_id: frozen.file_id,
            level: frozen.level,
            object_id: frozen.object_id.clone(),
            superblock_bytes: frozen.superblock_bytes,
            total_bytes,
            superblocks,
        };
        let meta_bytes = bincode::serialize(&meta).context("encode s3 meta")?;
        let meta_key = self.s3_object_meta_key(frozen.level, &frozen.object_id);
        let version = match self.s3_store.put(&meta_key, &meta_bytes) {
            Ok(version) => version,
            Err(err) => {
                for key in uploaded {
                    let _ = self.s3_store.delete(&key);
                }
                return Err(err).with_context(|| format!("upload s3 meta key={meta_key}"));
            }
        };
        self.tier_counters.s3_puts.fetch_add(1, Ordering::Relaxed);
        Ok(version)
    }

    fn delete_s3_object(&self, frozen: &FrozenObjectMeta) -> anyhow::Result<usize> {
        let meta = match self.load_s3_object_meta(frozen.level, &frozen.object_id) {
            Ok(meta) => meta,
            Err(_) => return Ok(0),
        };

        let mut removed = 0usize;
        for sb in meta.superblocks {
            let key = self.s3_superblock_key(meta.level, &meta.object_id, sb.id);
            self.s3_store
                .delete(&key)
                .with_context(|| format!("delete superblock key={key}"))?;
            removed += 1;
        }

        let meta_key = self.s3_object_meta_key(meta.level, &meta.object_id);
        self.s3_store
            .delete(&meta_key)
            .with_context(|| format!("delete meta key={meta_key}"))?;
        removed += 1;

        Ok(removed)
    }

    pub fn snapshots(&self) -> &SnapshotTracker {
        &self.snapshots
    }

    pub(crate) fn latest_seqno(&self) -> u64 {
        self.snapshots.latest_seqno()
    }

    pub(crate) fn min_retained_seqno(&self) -> u64 {
        let min_snapshot = self.snapshots.min_pinned_seqno();
        let min_branch = self
            .branches
            .read()
            .values()
            .copied()
            .min()
            .unwrap_or(min_snapshot);
        min_snapshot.min(min_branch)
    }

    pub(crate) fn snapshots_handle(&self) -> Arc<SnapshotTracker> {
        self.snapshots.clone()
    }

    pub fn metrics(&self) -> VersionMetrics {
        let mut levels = BTreeMap::new();
        let mut compaction_debt_bytes_by_level = BTreeMap::new();
        let mut compaction_level_metrics = BTreeMap::new();

        {
            let guard = self.levels.read();
            for (level, files) in [(0u8, &guard.l0), (1u8, &guard.l1)] {
                let bytes = files.iter().map(|f| f.size_bytes).sum();
                let file_count = files.len();
                let overlap_bytes = estimate_overlap_bytes(files);
                levels.insert(
                    level,
                    LevelMetricsSnapshot {
                        file_count,
                        bytes,
                        overlap_bytes,
                    },
                );
                compaction_level_metrics.insert(
                    level,
                    crate::compaction::LevelMetrics {
                        bytes,
                        file_count,
                        overlap_bytes,
                    },
                );
            }
        }

        let compaction_options = crate::compaction::CompactionOptions::default();
        let candidate = crate::compaction::CompactionPicker::pick_highest_score(
            &compaction_level_metrics,
            &compaction_options,
        );

        for (level, metrics) in &compaction_level_metrics {
            let target = compaction_options
                .target_level_bytes
                .get(level)
                .copied()
                .unwrap_or(0);
            let debt = metrics.bytes.saturating_sub(target);
            compaction_debt_bytes_by_level.insert(*level, debt);
        }

        let l0_file_debt = compaction_level_metrics
            .get(&0)
            .map(|m| {
                m.file_count
                    .saturating_sub(compaction_options.l0_file_trigger)
            })
            .unwrap_or(0);

        VersionMetrics {
            reader_cache: self.reader_cache.stats(),
            data_block_cache: self.data_block_cache.as_ref().map(|cache| cache.stats()),
            levels,
            compaction_debt_bytes_by_level,
            l0_file_debt,
            compaction_candidate_level: candidate.as_ref().map(|c| c.level),
            compaction_candidate_score: candidate.as_ref().map(|c| c.score),
            should_compact: crate::compaction::CompactionPicker::should_compact(
                &compaction_level_metrics,
                &compaction_options,
            ),
            frozen_s3_files: self.frozen_objects.read().len(),
            tier: self.tier_counters.snapshot(),
        }
    }

    pub(crate) fn allocate_file_id(&self) -> u64 {
        self.next_file_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn get(
        &self,
        key: &[u8],
        snapshot_seqno: u64,
    ) -> anyhow::Result<Option<LookupResult>> {
        let (l0_files, l1_files) = {
            let levels = self.levels.read();
            (levels.l0.clone(), levels.l1.clone())
        };

        let mut candidate: Option<(u64, Option<Value>)> = None;

        // L0: searched newest-first; may overlap.
        for add in l0_files.iter().rev() {
            if !range_contains(
                &(
                    std::ops::Bound::Included(add.smallest_user_key.clone()),
                    std::ops::Bound::Included(add.largest_user_key.clone()),
                ),
                key,
            ) {
                continue;
            }
            let path = self.resolve_sst_path(add.level, add.file_id)?;
            let reader = self.cached_reader(&path)?;
            if let Some((seqno, v)) = reader.get(key, snapshot_seqno)? {
                match &candidate {
                    Some((best_seq, _)) if *best_seq >= seqno => {}
                    _ => candidate = Some((seqno, v)),
                }
            }
        }

        // L1: non-overlapping; binary search by key range.
        if let Some(add) = find_l1_file(&l1_files, key) {
            let path = self.resolve_sst_path(add.level, add.file_id)?;
            let reader = self.cached_reader(&path)?;
            if let Some((seqno, v)) = reader.get(key, snapshot_seqno)? {
                match &candidate {
                    Some((best_seq, _)) if *best_seq >= seqno => {}
                    _ => candidate = Some((seqno, v)),
                }
            }
        }

        let tombstone_seq = self
            .range_tombstones(snapshot_seqno)?
            .iter()
            .filter(|t| t.start_key.as_ref() <= key && key < t.end_key.as_ref())
            .map(|t| t.seqno)
            .max();

        let result = match (candidate, tombstone_seq) {
            (Some((seq, value)), Some(tseq)) => {
                if tseq >= seq {
                    LookupResult {
                        seqno: tseq,
                        value: None,
                    }
                } else {
                    LookupResult { seqno: seq, value }
                }
            }
            (Some((seq, value)), None) => LookupResult { seqno: seq, value },
            (None, Some(tseq)) => LookupResult {
                seqno: tseq,
                value: None,
            },
            (None, None) => return Ok(None),
        };

        Ok(Some(result))
    }

    pub fn rebalance_level_tiers(&self) -> anyhow::Result<usize> {
        let mut moves = Vec::new();
        {
            let guard = self.levels.read();
            for file in guard.l0.iter().chain(guard.l1.iter()) {
                if file.tier == StorageTier::S3 {
                    continue;
                }
                let target_tier = self.tier_for_level(file.level);
                if file.tier != target_tier {
                    moves.push((file.file_id, file.level, file.tier, target_tier));
                }
            }
        }

        let max_moves = self.options.tier_rebalance_max_moves;
        if max_moves == 0 {
            return Ok(0);
        }

        for (file_id, level, from_tier, to_tier) in moves.iter().take(max_moves) {
            self.move_file_between_tiers(*file_id, *level, *from_tier, *to_tier)?;
        }

        Ok(moves.len().min(max_moves))
    }

    pub fn freeze_level_to_s3(&self, level: u8, max_files: Option<usize>) -> anyhow::Result<usize> {
        let files = {
            let guard = self.levels.read();
            match level {
                0 => guard.l0.clone(),
                1 => guard.l1.clone(),
                _ => anyhow::bail!("unsupported level for freeze: {level}"),
            }
        };

        let limit = max_files.unwrap_or(usize::MAX);
        let mut moved = 0usize;
        for add in files {
            if moved >= limit {
                break;
            }
            if add.tier == StorageTier::S3 {
                continue;
            }
            self.freeze_file_to_s3(&add)?;
            moved += 1;
        }

        Ok(moved)
    }

    pub fn thaw_level_from_s3(&self, level: u8, max_files: Option<usize>) -> anyhow::Result<usize> {
        let files = {
            let guard = self.levels.read();
            match level {
                0 => guard.l0.clone(),
                1 => guard.l1.clone(),
                _ => anyhow::bail!("unsupported level for thaw: {level}"),
            }
        };

        let target_tier = self.tier_for_level(level);
        if target_tier == StorageTier::S3 {
            anyhow::bail!("cannot thaw level {level} into s3 tier");
        }

        let limit = max_files.unwrap_or(usize::MAX);
        let mut moved = 0usize;
        for add in files {
            if moved >= limit {
                break;
            }
            if add.tier != StorageTier::S3 {
                continue;
            }
            self.move_file_between_tiers(add.file_id, add.level, StorageTier::S3, target_tier)?;
            moved += 1;
        }

        Ok(moved)
    }

    pub fn gc_orphaned_local_files(&self) -> anyhow::Result<usize> {
        let referenced: std::collections::HashSet<u64> = {
            let guard = self.levels.read();
            guard
                .l0
                .iter()
                .chain(guard.l1.iter())
                .map(|file| file.file_id)
                .collect()
        };

        let mut removed = 0usize;
        for dir in [
            self.dir.join("sst"),
            self.dir.join("sst_hdd"),
            self.dir.join("sst_cache"),
        ] {
            if !dir.exists() {
                continue;
            }

            for entry in std::fs::read_dir(&dir)? {
                let path = entry?.path();
                if path.extension().and_then(|ext| ext.to_str()) != Some("sst") {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                let Some(file_id) = parse_sst_file_id(name) else {
                    continue;
                };

                if referenced.contains(&file_id) {
                    continue;
                }

                std::fs::remove_file(&path)?;
                removed += 1;
            }
        }

        Ok(removed)
    }

    pub fn gc_orphaned_s3_files(&self) -> anyhow::Result<usize> {
        let referenced: std::collections::HashSet<u64> = {
            let guard = self.levels.read();
            guard
                .l0
                .iter()
                .chain(guard.l1.iter())
                .filter(|file| file.tier == StorageTier::S3)
                .map(|file| file.file_id)
                .collect()
        };

        let mut deleted_keys = 0usize;
        let mut deleted_file_ids = std::collections::HashSet::new();
        let keys = self.s3_store.list("")?;
        for key in keys {
            let Some(file_id) = parse_file_id_from_s3_key(&key) else {
                continue;
            };
            if referenced.contains(&file_id) {
                continue;
            }

            self.s3_store
                .delete(&key)
                .with_context(|| format!("delete orphaned s3 key={key}"))?;
            deleted_keys += 1;
            deleted_file_ids.insert(file_id);
            let _ = std::fs::remove_file(self.sst_cache_path(file_id));
        }

        if deleted_keys > 0 {
            self.tier_counters
                .s3_deletes
                .fetch_add(deleted_keys as u64, Ordering::Relaxed);
        }

        Ok(deleted_file_ids.len())
    }

    fn freeze_file_to_s3(&self, add: &AddFile) -> anyhow::Result<()> {
        let src = self.resolve_sst_path(add.level, add.file_id)?;
        let mut frozen = FrozenObjectMeta {
            file_id: add.file_id,
            level: add.level,
            object_id: format!("L{}-{:016x}", add.level, add.file_id),
            object_version: None,
            superblock_bytes: 8 * 1024 * 1024,
        };

        frozen.object_version = self.write_s3_object_from_file(&frozen, &src)?;
        self.mark_file_frozen(
            add.file_id,
            frozen.object_id.clone(),
            frozen.object_version.clone(),
            frozen.superblock_bytes,
        )?;

        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(self.sst_cache_path(add.file_id));

        Ok(())
    }

    pub fn mark_file_frozen(
        &self,
        file_id: u64,
        object_id: impl Into<String>,
        object_version: Option<String>,
        superblock_bytes: u32,
    ) -> anyhow::Result<()> {
        let mut target: Option<AddFile> = None;
        {
            let guard = self.levels.read();
            for add in guard.l0.iter().chain(guard.l1.iter()) {
                if add.file_id == file_id {
                    target = Some(add.clone());
                    break;
                }
            }
        }

        let Some(add) = target else {
            anyhow::bail!("unknown file_id for freeze: {file_id}");
        };

        let object_id = object_id.into();
        let freeze = FreezeFile {
            file_id,
            level: add.level,
            object_id: object_id.clone(),
            object_version: object_version.clone(),
            superblock_bytes,
        };

        {
            let mut manifest = self.manifest.lock();
            manifest.append(&ManifestRecord::FreezeFile(freeze), true)?;
            manifest.sync_dir()?;
        }

        {
            let mut levels = self.levels.write();
            for file in levels.l0.iter_mut() {
                if file.file_id == file_id {
                    file.tier = StorageTier::S3;
                }
            }
            for file in levels.l1.iter_mut() {
                if file.file_id == file_id {
                    file.tier = StorageTier::S3;
                }
            }
        }

        self.frozen_objects.write().insert(
            file_id,
            FrozenObjectMeta {
                file_id,
                level: add.level,
                object_id,
                object_version,
                superblock_bytes,
            },
        );
        Ok(())
    }

    fn move_file_between_tiers(
        &self,
        file_id: u64,
        level: u8,
        from_tier: StorageTier,
        to_tier: StorageTier,
    ) -> anyhow::Result<()> {
        if to_tier == StorageTier::S3 {
            let add = {
                let guard = self.levels.read();
                guard
                    .l0
                    .iter()
                    .chain(guard.l1.iter())
                    .find(|f| f.file_id == file_id && f.level == level)
                    .cloned()
            };
            let Some(add) = add else {
                anyhow::bail!("unknown sst for freeze file_id={file_id} level={level}");
            };
            if add.tier != StorageTier::S3 {
                self.freeze_file_to_s3(&add)?;
            }
            return Ok(());
        }

        let frozen = if from_tier == StorageTier::S3 {
            self.frozen_objects.read().get(&file_id).cloned()
        } else {
            None
        };
        let dst = self.sst_path_for_tier(to_tier, file_id);
        let dst_tmp = dst.with_extension("tmp");
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let src = self.sst_path_for_tier(from_tier, file_id);

        if from_tier == StorageTier::S3 {
            let frozen = frozen
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing frozen metadata for file_id={file_id}"))?;
            let meta = self.load_s3_object_meta(frozen.level, &frozen.object_id)?;
            let mut out = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .open(&dst_tmp)
                .with_context(|| format!("open dst tmp {}", dst_tmp.display()))?;
            for sb in &meta.superblocks {
                let key = self.s3_superblock_key(meta.level, &meta.object_id, sb.id);
                let data = self
                    .s3_store
                    .get(&key)
                    .with_context(|| format!("read superblock key={key}"))?;
                if data.len() != sb.len as usize {
                    anyhow::bail!(
                        "superblock length mismatch for {} sb={} expected {} got {}",
                        meta.object_id,
                        sb.id,
                        sb.len,
                        data.len()
                    );
                }
                let hash = blake3::hash(&data);
                if hash.as_bytes() != sb.hash.as_slice() {
                    anyhow::bail!("superblock hash mismatch for {} sb={}", meta.object_id, sb.id);
                }
                out.write_all(&data)
                    .with_context(|| format!("write dst tmp {}", dst_tmp.display()))?;
                self.tier_counters.s3_gets.fetch_add(1, Ordering::Relaxed);
            }
            out.sync_data()?;
        } else {
            if !src.exists() {
                // Recovery fallback: if file already moved, do not fail hard.
                if self.sst_path_for_tier(to_tier, file_id).exists() {
                    return Ok(());
                }
                anyhow::bail!(
                    "cannot move missing sst file_id={} from tier {:?}: {}",
                    file_id,
                    from_tier,
                    src.display()
                );
            }

            std::fs::copy(&src, &dst_tmp)?;
            {
                let fd = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&dst_tmp)?;
                fd.sync_data()?;
            }
        }

        std::fs::rename(&dst_tmp, &dst)?;
        {
            let parent = dst
                .parent()
                .ok_or_else(|| anyhow::anyhow!("destination has no parent"))?;
            let dir_fd = std::fs::File::open(parent)?;
            dir_fd.sync_all()?;
        }

        {
            let mut manifest = self.manifest.lock();
            manifest.append(
                &ManifestRecord::MoveFile(MoveFile {
                    file_id,
                    level,
                    tier: to_tier,
                }),
                true,
            )?;
            manifest.sync_dir()?;
        }

        {
            let mut levels = self.levels.write();
            for file in levels.l0.iter_mut() {
                if file.file_id == file_id && file.level == level {
                    file.tier = to_tier;
                }
            }
            for file in levels.l1.iter_mut() {
                if file.file_id == file_id && file.level == level {
                    file.tier = to_tier;
                }
            }
        }

        if to_tier != StorageTier::S3 {
            self.frozen_objects.write().remove(&file_id);
        }

        if from_tier == StorageTier::S3 {
            if let Some(frozen) = frozen.as_ref() {
                let deleted = self.delete_s3_object(frozen)?;
                if deleted > 0 {
                    self.tier_counters
                        .s3_deletes
                        .fetch_add(deleted as u64, Ordering::Relaxed);
                }
            }
        } else {
            let _ = std::fs::remove_file(src);
        }
        let _ = std::fs::remove_file(self.sst_cache_path(file_id));
        Ok(())
    }

    pub(crate) fn range_tombstones(
        &self,
        snapshot_seqno: u64,
    ) -> anyhow::Result<Vec<RangeTombstone>> {
        let guard = self.levels.read();
        let mut out = Vec::new();

        for file in guard.l0.iter().chain(guard.l1.iter()) {
            let path = self.resolve_sst_path(file.level, file.file_id)?;
            let reader = self.cached_reader(&path)?;
            out.extend(reader.range_tombstones(snapshot_seqno)?);
        }

        out.sort_by(|a, b| b.seqno.cmp(&a.seqno));
        Ok(out)
    }

    pub fn iter(&self, range: Range, snapshot_seqno: u64) -> anyhow::Result<SstIter> {
        let guard = self.levels.read();
        let mut paths = Vec::with_capacity(guard.l0.len() + guard.l1.len());
        for file in guard.l0.iter().chain(guard.l1.iter()) {
            paths.push(self.resolve_sst_path(file.level, file.file_id)?);
        }
        SstIter::new(paths, snapshot_seqno, range)
    }

    pub fn compact_l0_to_l1(
        &self,
        range: Option<&Range>,
        options: &DbOptions,
    ) -> anyhow::Result<()> {
        let (l0, l1) = {
            let guard = self.levels.read();
            if guard.l0.is_empty() {
                return Ok(());
            }
            (guard.l0.clone(), guard.l1.clone())
        };

        let range_bounds = range.map(crate::memtable::bounds_from_range);
        let l0_in_range: Vec<AddFile> = if let Some(bounds) = &range_bounds {
            l0.into_iter()
                .filter(|file| {
                    range_overlaps_file(
                        bounds,
                        file.smallest_user_key.as_ref(),
                        file.largest_user_key.as_ref(),
                    )
                })
                .collect()
        } else {
            l0
        };

        let target_tier = self.tier_for_level(1);
        if target_tier == StorageTier::Hdd {
            let budget = options.hdd_compaction_budget_bytes;
            if budget == 0 {
                return Ok(());
            }

            let mut total = 0u64;
            let mut limited = Vec::new();
            for add in l0_in_range {
                if !limited.is_empty() && total >= budget {
                    break;
                }

                limited.push(add.clone());
                total = total.saturating_add(add.size_bytes);
            }

            if limited.is_empty() {
                return Ok(());
            }

            return self.compact_l0_to_l1_inputs(limited, l1, range);
        }

        self.compact_l0_to_l1_inputs(l0_in_range, l1, range)
    }

    fn compact_l0_to_l1_inputs(
        &self,
        l0_in_range: Vec<AddFile>,
        l1: Vec<AddFile>,
        _range: Option<&Range>,
    ) -> anyhow::Result<()> {

        if l0_in_range.is_empty() {
            return Ok(());
        }

        let mut smallest: Option<Bytes> = None;
        let mut largest: Option<Bytes> = None;
        for file in &l0_in_range {
            smallest = Some(match smallest {
                None => file.smallest_user_key.clone(),
                Some(s) => std::cmp::min(s, file.smallest_user_key.clone()),
            });
            largest = Some(match largest {
                None => file.largest_user_key.clone(),
                Some(l) => std::cmp::max(l, file.largest_user_key.clone()),
            });
        }
        let smallest = smallest.unwrap_or_else(Bytes::new);
        let largest = largest.unwrap_or_else(Bytes::new);

        let mut compact_inputs = Vec::new();
        compact_inputs.extend(l0_in_range.clone());
        let mut l1_inputs = Vec::new();
        for file in &l1 {
            if overlaps(
                &smallest,
                &largest,
                &file.smallest_user_key,
                &file.largest_user_key,
            ) {
                l1_inputs.push(file.clone());
                compact_inputs.push(file.clone());
            }
        }

        let can_drop_obsolete_point_tombstones =
            self.compaction_inputs_fully_cover_range(&l0_in_range, &l1_inputs, &smallest, &largest);

        let mut entries = Vec::new();
        for file in &compact_inputs {
            let path = self.resolve_sst_path(file.level, file.file_id)?;
            let reader = self.cached_reader(&path)?;
            let mut iter = reader.iter(u64::MAX)?;
            iter.seek_to_first();
            while let Some(next) = iter.next() {
                let (user_key, seqno, kind, value) = next?;
                let key_kind = match kind {
                    OpKind::Put => KeyKind::Put,
                    OpKind::Del => KeyKind::Del,
                    OpKind::RangeDel => KeyKind::RangeDel,
                };
                entries.push((InternalKey::new(user_key, seqno, key_kind), value));
            }
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let min_snapshot_seqno = self.min_retained_seqno();
        let mut out_entries = Vec::with_capacity(entries.len());
        let mut idx = 0usize;
        while idx < entries.len() {
            let user_key = entries[idx].0.user_key.clone();
            let mut group = Vec::new();
            while idx < entries.len() && entries[idx].0.user_key == user_key {
                group.push(entries[idx].clone());
                idx += 1;
            }

            out_entries.extend(compact_user_key_entries(
                group,
                min_snapshot_seqno,
                can_drop_obsolete_point_tombstones,
            ));
        }

        out_entries = drop_obsolete_range_tombstones_bottommost(
            out_entries,
            min_snapshot_seqno,
            can_drop_obsolete_point_tombstones,
        );

        if out_entries.is_empty() {
            return Ok(());
        }

        let out_file_id = self.allocate_file_id();
        let sst_dir = self.sst_root_dir(1);
        let mut builder = if self.options.sst_use_io_executor_writes {
            let io = self
                .sst_io_ctx
                .as_ref()
                .context("sst io executor writes requested but not configured")?
                .io()
                .clone();
            crate::sst::SstBuilder::create_with_io(&sst_dir, out_file_id, 64 * 1024, io)?
        } else {
            crate::sst::SstBuilder::create(&sst_dir, out_file_id, 64 * 1024)?
        };
        for (key, value) in &out_entries {
            builder.add(key, value.as_ref())?;
        }
        let props = builder.finish()?;
        self.apply_compaction_edit(compact_inputs, vec![(out_file_id, props)])?;
        Ok(())
    }

    pub(crate) fn install_sst(&self, file_id: u64, props: &SstProperties) -> anyhow::Result<()> {
        let add = AddFile {
            file_id,
            level: 0,
            smallest_user_key: props.smallest_user_key.clone(),
            largest_user_key: props.largest_user_key.clone(),
            max_seqno: props.max_seqno,
            table_root: props.table_root,
            size_bytes: props.data_bytes + props.index_bytes,
            tier: self.tier_for_level(0),
            sst_format_version: props.format_version,
        };
        {
            let mut manifest = self.manifest.lock();
            manifest.append(&ManifestRecord::AddFile(add.clone()), true)?;
            manifest.sync_dir()?;
        }
        self.levels.write().l0.push(add);
        self.snapshots.set_latest_seqno(props.max_seqno);
        Ok(())
    }

    pub fn ingest_external_sst(&self, source_path: &Path) -> anyhow::Result<(u64, u64)> {
        let source_reader = SstReader::open(source_path)
            .with_context(|| format!("open source sst {}", source_path.display()))?;
        let props = source_reader.properties().clone();

        if props.entries == 0 {
            anyhow::bail!("cannot ingest empty sst: {}", source_path.display());
        }
        if props.smallest_user_key > props.largest_user_key {
            anyhow::bail!(
                "invalid sst key range: smallest > largest ({})",
                source_path.display()
            );
        }

        let file_id = self.allocate_file_id();
        let sst_dir = self.sst_root_dir(0);
        std::fs::create_dir_all(&sst_dir)
            .with_context(|| format!("create sst dir {}", sst_dir.display()))?;

        let tmp_path = sst_dir.join(format!("sst_{file_id:016x}.tmp"));
        let final_path = sst_dir.join(format!("sst_{file_id:016x}.sst"));

        std::fs::copy(source_path, &tmp_path).with_context(|| {
            format!(
                "copy sst {} -> {}",
                source_path.display(),
                tmp_path.display()
            )
        })?;

        let tmp_fd = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&tmp_path)
            .with_context(|| format!("open tmp sst {}", tmp_path.display()))?;
        tmp_fd
            .sync_data()
            .with_context(|| format!("sync tmp sst {}", tmp_path.display()))?;
        drop(tmp_fd);

        std::fs::rename(&tmp_path, &final_path).with_context(|| {
            format!("rename {} -> {}", tmp_path.display(), final_path.display())
        })?;
        let dir_fd = std::fs::File::open(&sst_dir)
            .with_context(|| format!("open sst dir {}", sst_dir.display()))?;
        dir_fd
            .sync_all()
            .with_context(|| format!("sync sst dir {}", sst_dir.display()))?;

        self.install_sst(file_id, &props)?;
        self.advance_current_branch(props.max_seqno)?;
        Ok((file_id, props.max_seqno))
    }

    fn apply_compaction_edit(
        &self,
        inputs: Vec<AddFile>,
        outputs: Vec<(u64, SstProperties)>,
    ) -> anyhow::Result<()> {
        let frozen_to_remove: Vec<FrozenObjectMeta> = {
            let frozen = self.frozen_objects.read();
            inputs
                .iter()
                .filter_map(|input| frozen.get(&input.file_id).cloned())
                .collect()
        };

        let mut adds = Vec::new();
        for (file_id, props) in outputs {
            adds.push(AddFile {
                file_id,
                level: 1,
                smallest_user_key: props.smallest_user_key.clone(),
                largest_user_key: props.largest_user_key.clone(),
                max_seqno: props.max_seqno,
                table_root: props.table_root,
                size_bytes: props.data_bytes + props.index_bytes,
                tier: self.tier_for_level(1),
                sst_format_version: props.format_version,
            });
        }

        let mut deletes = Vec::new();
        for input in &inputs {
            deletes.push(DeleteFile {
                file_id: input.file_id,
                level: input.level,
            });
        }

        {
            let mut manifest = self.manifest.lock();
            manifest.append(
                &ManifestRecord::VersionEdit(VersionEdit {
                    adds: adds.clone(),
                    deletes: deletes.clone(),
                }),
                true,
            )?;
            manifest.sync_dir()?;
        }

        {
            let mut guard = self.levels.write();
            // Remove deleted inputs.
            for del in &deletes {
                match del.level {
                    0 => guard.l0.retain(|f| f.file_id != del.file_id),
                    1 => guard.l1.retain(|f| f.file_id != del.file_id),
                    _ => {}
                }
            }

            // Add compaction outputs to L1.
            guard.l1.extend(adds);
            guard
                .l1
                .sort_by(|a, b| a.smallest_user_key.cmp(&b.smallest_user_key));
        }

        {
            let mut frozen = self.frozen_objects.write();
            for input in &inputs {
                frozen.remove(&input.file_id);
            }
        }

        for frozen in frozen_to_remove {
            if let Ok(deleted) = self.delete_s3_object(&frozen) {
                if deleted > 0 {
                    self.tier_counters
                        .s3_deletes
                        .fetch_add(deleted as u64, Ordering::Relaxed);
                }
            }
        }

        // Once manifest deletions are durable, remove old files from disk.
        for input in &inputs {
            let path_nvme = self
                .dir
                .join("sst")
                .join(format!("sst_{:016x}.sst", input.file_id));
            let path_hdd = self
                .dir
                .join("sst_hdd")
                .join(format!("sst_{:016x}.sst", input.file_id));
            let path_cache = self
                .dir
                .join("sst_cache")
                .join(format!("sst_{:016x}.sst", input.file_id));
            let _ = std::fs::remove_file(path_nvme);
            let _ = std::fs::remove_file(path_hdd);
            let _ = std::fs::remove_file(path_cache);
        }
        Ok(())
    }

    fn compaction_inputs_fully_cover_range(
        &self,
        selected_l0: &[AddFile],
        selected_l1: &[AddFile],
        smallest: &[u8],
        largest: &[u8],
    ) -> bool {
        let guard = self.levels.read();
        compaction_inputs_cover_overlaps(
            &guard.l0,
            &guard.l1,
            selected_l0,
            selected_l1,
            smallest,
            largest,
        )
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
        .filter_map(|(key, value)| {
            (key.kind == KeyKind::RangeDel && key.seqno < min_snapshot_seqno)
                .then(|| RangeTombstone::new(key.user_key.clone(), value.clone(), key.seqno))
        })
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
    fn new(paths: Vec<PathBuf>, snapshot_seqno: u64, range: Range) -> anyhow::Result<Self> {
        let bounds = crate::memtable::bounds_from_range(&range);
        let mut entries = Vec::new();

        for path in paths {
            let reader = SstReader::open(&path)?;
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
                    OpKind::RangeDel => KeyKind::RangeDel,
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
        let target = InternalKey::new(
            Bytes::copy_from_slice(user_key),
            self.snapshot_seqno,
            KeyKind::Meta,
        );
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
            KeyKind::RangeDel => OpKind::RangeDel,
            other => return Some(Err(anyhow::anyhow!("unexpected key kind: {other:?}"))),
        };
        Some(Ok((entry.key.user_key, entry.key.seqno, kind, entry.value)))
    }
}
