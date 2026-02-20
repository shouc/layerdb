mod config;
mod rebuilder;
mod stats;
mod storage;
mod sync_utils;
mod vector_blocks;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Instant;

use anyhow::Context;
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use layerdb::{Db, DbOptions, ReadOptions, WriteOptions};
use serde::{Deserialize, Serialize};

use crate::linalg::squared_l2;
use crate::types::{Neighbor, VectorIndex, VectorRecord};

use super::spfresh_diskmeta::SpFreshDiskMetaIndex;
use super::spfresh_offheap::SpFreshOffHeapIndex;
use super::{SpFreshConfig, SpFreshIndex};
use rebuilder::{rebuild_once, spawn_rebuilder, RebuilderRuntime};
use stats::SpFreshLayerDbStatsInner;
use vector_blocks::VectorBlockStore;
use storage::{
    decode_vector_row_value, decode_vector_row_with_posting, encode_vector_row_value,
    encode_vector_row_fields, encode_vector_row_value_with_posting, encode_wal_entry, ensure_active_generation,
    ensure_metadata, ensure_posting_event_next_seq, ensure_wal_exists, ensure_wal_next_seq,
    load_index_checkpoint_bytes, load_metadata, load_posting_members, load_row, load_rows,
    load_rows_with_posting_assignments, load_startup_manifest_bytes, load_wal_entries_since,
    persist_index_checkpoint_bytes, persist_startup_manifest_bytes, posting_map_prefix,
    posting_member_event_key, posting_member_event_tombstone_value,
    posting_member_event_upsert_value_from_sketch, posting_member_event_upsert_value_with_residual,
    posting_members_generation_prefix, posting_members_prefix, prefix_exclusive_end,
    prune_wal_before, refresh_read_snapshot, set_active_generation, set_posting_event_next_seq,
    validate_config, vector_key, vector_prefix, wal_key, IndexWalEntry, PostingMember,
};
use sync_utils::{lock_mutex, lock_read, lock_write};

pub use config::{SpFreshLayerDbConfig, SpFreshMemoryMode};
pub use stats::SpFreshLayerDbStats;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VectorMutation {
    Upsert(VectorRecord),
    Delete { id: u64 },
}

impl VectorMutation {
    fn id(&self) -> u64 {
        match self {
            Self::Upsert(row) => row.id,
            Self::Delete { id } => *id,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorMutationBatchResult {
    pub upserts: usize,
    pub deletes: usize,
}

type DiskMetaRowState = Option<(usize, Vec<f32>)>;
type DiskMetaStateMap = HashMap<u64, DiskMetaRowState>;
type DistanceRow = (u64, f32);
type DistanceLoadResult = (Vec<DistanceRow>, Vec<u64>);
const DISKMETA_RERANK_FACTOR: usize = 8;
const POSTING_LOG_COMPACT_MIN_EVENTS: usize = 512;
const POSTING_LOG_COMPACT_FACTOR: usize = 3;
const ASYNC_COMMIT_MAX_INFLIGHT: usize = 8;

#[derive(Clone, Debug, Serialize, Deserialize)]
enum RuntimeSpFreshIndex {
    Resident(SpFreshIndex),
    OffHeap(SpFreshOffHeapIndex),
    OffHeapDiskMeta(SpFreshDiskMetaIndex),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedIndexCheckpoint {
    schema_version: u32,
    generation: u64,
    applied_wal_seq: Option<u64>,
    index: RuntimeSpFreshIndex,
}

const STARTUP_MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedStartupManifest {
    schema_version: u32,
    generation: u64,
    applied_wal_seq: Option<u64>,
    posting_event_next_seq: u64,
    epoch: u64,
}

#[derive(Debug)]
struct VectorCache {
    capacity: usize,
    map: HashMap<u64, Vec<f32>>,
    order: VecDeque<u64>,
}

impl VectorCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&self, id: u64) -> Option<Vec<f32>> {
        self.map.get(&id).cloned()
    }

    fn distance_to(&self, id: u64, query: &[f32]) -> Option<f32> {
        self.map
            .get(&id)
            .map(|values| squared_l2(query, values.as_slice()))
    }

    fn put(&mut self, id: u64, values: Vec<f32>) {
        if self.capacity == 0 {
            return;
        }
        if let std::collections::hash_map::Entry::Occupied(mut existing) = self.map.entry(id) {
            existing.insert(values);
            return;
        }
        if self.map.len() >= self.capacity {
            while let Some(oldest) = self.order.pop_front() {
                if self.map.remove(&oldest).is_some() {
                    break;
                }
            }
        }
        self.order.push_back(id);
        self.map.insert(id, values);
    }

    fn remove(&mut self, id: u64) {
        self.map.remove(&id);
    }
}

#[derive(Clone, Debug)]
struct PostingMemberSketch {
    id: u64,
    residual_scale: Option<f32>,
    residual_code: Option<Vec<i8>>,
}

impl PostingMemberSketch {
    fn from_loaded(member: &PostingMember) -> Self {
        Self {
            id: member.id,
            residual_scale: member.residual_scale,
            residual_code: member.residual_code.clone(),
        }
    }
}

#[derive(Debug)]
struct PostingMembersCache {
    capacity: usize,
    map: HashMap<(u64, usize), Arc<Vec<PostingMemberSketch>>>,
    order: VecDeque<(u64, usize)>,
    mutation_ops: usize,
    compact_interval_ops: usize,
    compact_budget_entries: usize,
}

impl PostingMembersCache {
    fn new(capacity: usize, compact_interval_ops: usize, compact_budget_entries: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: HashMap::new(),
            order: VecDeque::new(),
            mutation_ops: 0,
            compact_interval_ops: compact_interval_ops.max(1),
            compact_budget_entries: compact_budget_entries.max(1),
        }
    }

    fn get(&self, generation: u64, posting_id: usize) -> Option<Arc<Vec<PostingMemberSketch>>> {
        self.map.get(&(generation, posting_id)).cloned()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn put_arc(
        &mut self,
        generation: u64,
        posting_id: usize,
        members: Arc<Vec<PostingMemberSketch>>,
    ) {
        if self.capacity == 0 {
            return;
        }
        if let std::collections::hash_map::Entry::Occupied(mut existing) =
            self.map.entry((generation, posting_id))
        {
            existing.insert(members);
            return;
        }
        if self.map.len() >= self.capacity {
            while let Some(oldest) = self.order.pop_front() {
                if self.map.remove(&oldest).is_some() {
                    break;
                }
            }
        }
        self.order.push_back((generation, posting_id));
        self.map.insert((generation, posting_id), members);
    }

    fn apply_upsert_delta(
        &mut self,
        generation: u64,
        posting_id: usize,
        member: PostingMemberSketch,
    ) {
        let key = (generation, posting_id);
        let Some(current) = self.map.get(&key).cloned() else {
            return;
        };
        let mut next: Vec<PostingMemberSketch> = current.as_ref().clone();
        if let Some(existing) = next.iter_mut().find(|m| m.id == member.id) {
            *existing = member;
        } else {
            next.push(member);
        }
        self.map.insert(key, Arc::new(next));
        self.maybe_compact();
    }

    fn apply_delete_delta(&mut self, generation: u64, posting_id: usize, id: u64) {
        let key = (generation, posting_id);
        let Some(current) = self.map.get(&key).cloned() else {
            return;
        };
        let mut next: Vec<PostingMemberSketch> = current
            .iter()
            .filter(|m| m.id != id)
            .cloned()
            .collect();
        next.shrink_to_fit();
        self.map.insert(key, Arc::new(next));
        self.maybe_compact();
    }

    fn maybe_compact(&mut self) {
        self.mutation_ops = self.mutation_ops.saturating_add(1);
        if !self.mutation_ops.is_multiple_of(self.compact_interval_ops) {
            return;
        }

        let mut compacted = 0usize;
        let mut cursor = 0usize;
        while compacted < self.compact_budget_entries && cursor < self.order.len() {
            let key = self.order[cursor];
            cursor += 1;
            let Some(current) = self.map.get(&key).cloned() else {
                continue;
            };
            if current.len() < 2 {
                continue;
            }
            let mut next = current.as_ref().clone();
            next.sort_by_key(|m| m.id);
            next.dedup_by_key(|m| m.id);
            next.shrink_to_fit();
            self.map.insert(key, Arc::new(next));
            compacted += 1;
        }
    }

    fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }
}

enum CommitRequest {
    Write {
        ops: Vec<layerdb::Op>,
        sync: bool,
        resp: mpsc::Sender<anyhow::Result<()>>,
    },
    Shutdown,
}

pub struct SpFreshLayerDbIndex {
    cfg: SpFreshLayerDbConfig,
    db_path: PathBuf,
    db: Db,
    active_generation: Arc<AtomicU64>,
    index: Arc<RwLock<RuntimeSpFreshIndex>>,
    update_gate: Arc<RwLock<()>>,
    dirty_ids: Arc<Mutex<HashSet<u64>>>,
    pending_ops: Arc<AtomicUsize>,
    vector_cache: Arc<Mutex<VectorCache>>,
    vector_blocks: Arc<Mutex<VectorBlockStore>>,
    posting_members_cache: Arc<Mutex<PostingMembersCache>>,
    diskmeta_search_snapshot: Arc<ArcSwapOption<SpFreshDiskMetaIndex>>,
    wal_next_seq: Arc<AtomicU64>,
    posting_event_next_seq: Arc<AtomicU64>,
    startup_epoch: Arc<AtomicU64>,
    commit_tx: mpsc::Sender<CommitRequest>,
    commit_worker: Option<JoinHandle<()>>,
    pending_commit_acks: Arc<Mutex<VecDeque<mpsc::Receiver<anyhow::Result<()>>>>>,
    commit_error: Arc<Mutex<Option<String>>>,
    max_async_commit_inflight: usize,
    rebuild_tx: mpsc::Sender<()>,
    stop_worker: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    stats: Arc<SpFreshLayerDbStatsInner>,
}

impl SpFreshLayerDbIndex {
    pub fn open(path: impl AsRef<Path>, cfg: SpFreshLayerDbConfig) -> anyhow::Result<Self> {
        validate_config(&cfg)?;
        let db_path = path.as_ref();
        let db = Db::open(db_path, cfg.db_options.clone()).context("open layerdb for spfresh")?;
        ensure_wal_exists(db_path)?;
        refresh_read_snapshot(&db)?;
        ensure_metadata(&db, &cfg)?;
        Self::open_with_db(db_path, db, cfg)
    }

    pub fn open_existing(path: impl AsRef<Path>, db_options: DbOptions) -> anyhow::Result<Self> {
        let db_path = path.as_ref();
        let db = Db::open(db_path, db_options.clone()).context("open layerdb for spfresh")?;
        ensure_wal_exists(db_path)?;
        refresh_read_snapshot(&db)?;
        let meta = load_metadata(&db)?.ok_or_else(|| {
            anyhow::anyhow!(
                "missing spfresh metadata in {}; initialize with open()",
                db_path.display()
            )
        })?;

        let cfg = SpFreshLayerDbConfig {
            spfresh: SpFreshConfig {
                dim: meta.dim,
                initial_postings: meta.initial_postings,
                split_limit: meta.split_limit,
                merge_limit: meta.merge_limit,
                reassign_range: meta.reassign_range,
                nprobe: meta.nprobe,
                kmeans_iters: meta.kmeans_iters,
            },
            db_options,
            ..Default::default()
        };

        validate_config(&cfg)?;
        ensure_metadata(&db, &cfg)?;
        Self::open_with_db(db_path, db, cfg)
    }

    fn open_with_db(db_path: &Path, db: Db, mut cfg: SpFreshLayerDbConfig) -> anyhow::Result<Self> {
        let generation = ensure_active_generation(&db)?;
        let wal_next_seq = ensure_wal_next_seq(&db)?;
        let posting_event_next_seq = ensure_posting_event_next_seq(&db)?;
        let manifest_epoch = load_startup_manifest_bytes(&db)?
            .and_then(|raw| bincode::deserialize::<PersistedStartupManifest>(&raw).ok())
            .filter(|m| {
                m.schema_version == STARTUP_MANIFEST_SCHEMA_VERSION && m.generation == generation
            })
            .map(|m| m.epoch)
            .unwrap_or(0);
        let (mut index_state, applied_wal_seq) =
            Self::load_or_rebuild_index(&db, &cfg, generation, wal_next_seq)?;
        cfg.memory_mode = match &index_state {
            RuntimeSpFreshIndex::Resident(_) => SpFreshMemoryMode::Resident,
            RuntimeSpFreshIndex::OffHeap(_) => SpFreshMemoryMode::OffHeap,
            RuntimeSpFreshIndex::OffHeapDiskMeta(_) => SpFreshMemoryMode::OffHeapDiskMeta,
        };
        let vector_cache = Arc::new(Mutex::new(VectorCache::new(cfg.offheap_cache_capacity)));
        let vector_blocks = Arc::new(Mutex::new(VectorBlockStore::open(
            db_path,
            cfg.spfresh.dim,
            manifest_epoch,
        )?));
        let posting_members_cache = Arc::new(Mutex::new(PostingMembersCache::new(
            cfg.offheap_posting_cache_entries,
            cfg.posting_delta_compact_interval_ops,
            cfg.posting_delta_compact_budget_entries,
        )));
        let replay_from = applied_wal_seq.map_or(0, |seq| seq.saturating_add(1));
        if replay_from < wal_next_seq {
            if matches!(index_state, RuntimeSpFreshIndex::OffHeapDiskMeta(_)) {
                // WAL v2 for diskmeta only stores new-state payloads. To preserve exact centroid
                // accounting, rebuild from authoritative rows whenever a tail exists.
                let (rows, assignments) = load_rows_with_posting_assignments(&db, generation)?;
                let (rebuilt, _assigned_now) = SpFreshDiskMetaIndex::build_from_rows_with_assignments(
                    cfg.spfresh.clone(),
                    &rows,
                    Some(&assignments),
                );
                index_state = RuntimeSpFreshIndex::OffHeapDiskMeta(rebuilt);
            } else {
                Self::replay_wal_tail(
                    &db,
                    &vector_cache,
                    &vector_blocks,
                    generation,
                    &mut index_state,
                    replay_from,
                )?;
            }
        }
        let index = Arc::new(RwLock::new(index_state));
        let diskmeta_search_snapshot = Arc::new(ArcSwapOption::empty());
        let initial_snapshot = {
            let guard = lock_read(&index);
            Self::extract_diskmeta_snapshot(&guard)
        };
        if let Some(snapshot) = initial_snapshot {
            diskmeta_search_snapshot.store(Some(snapshot));
        }
        let active_generation = Arc::new(AtomicU64::new(generation));
        let update_gate = Arc::new(RwLock::new(()));
        let dirty_ids = Arc::new(Mutex::new(HashSet::new()));
        let pending_ops = Arc::new(AtomicUsize::new(0));
        let wal_next_seq = Arc::new(AtomicU64::new(wal_next_seq));
        let posting_event_next_seq = Arc::new(AtomicU64::new(posting_event_next_seq));
        let startup_epoch = Arc::new(AtomicU64::new(manifest_epoch));
        let stop_worker = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(SpFreshLayerDbStatsInner::default());
        let (commit_tx, commit_rx) = mpsc::channel::<CommitRequest>();
        let (rebuild_tx, rebuild_rx) = mpsc::channel::<()>();

        let commit_db = db.clone();
        let commit_worker = std::thread::spawn(move || {
            while let Ok(req) = commit_rx.recv() {
                match req {
                    CommitRequest::Write { ops, sync, resp } => {
                        let result = commit_db
                            .write_batch(ops, WriteOptions { sync })
                            .context("spfresh-layerdb commit worker write batch");
                        let _ = resp.send(result);
                    }
                    CommitRequest::Shutdown => break,
                }
            }
        });

        let worker = spawn_rebuilder(
            RebuilderRuntime {
                db: db.clone(),
                rebuild_pending_ops: cfg.rebuild_pending_ops.max(1),
                rebuild_interval: cfg.rebuild_interval,
                active_generation: active_generation.clone(),
                index: index.clone(),
                update_gate: update_gate.clone(),
                dirty_ids: dirty_ids.clone(),
                pending_ops: pending_ops.clone(),
                vector_cache: vector_cache.clone(),
                vector_blocks: vector_blocks.clone(),
                stats: stats.clone(),
                stop_worker: stop_worker.clone(),
            },
            rebuild_rx,
        );

        Ok(Self {
            cfg,
            db_path: db_path.to_path_buf(),
            db,
            active_generation,
            index,
            update_gate,
            dirty_ids,
            pending_ops,
            vector_cache,
            vector_blocks,
            posting_members_cache,
            diskmeta_search_snapshot,
            wal_next_seq,
            posting_event_next_seq,
            startup_epoch,
            commit_tx,
            commit_worker: Some(commit_worker),
            pending_commit_acks: Arc::new(Mutex::new(VecDeque::new())),
            commit_error: Arc::new(Mutex::new(None)),
            max_async_commit_inflight: ASYNC_COMMIT_MAX_INFLIGHT,
            rebuild_tx,
            stop_worker,
            worker: Some(worker),
            stats,
        })
    }

    fn load_or_rebuild_index(
        db: &Db,
        cfg: &SpFreshLayerDbConfig,
        generation: u64,
        wal_next_seq: u64,
    ) -> anyhow::Result<(RuntimeSpFreshIndex, Option<u64>)> {
        if let Some(raw) = load_index_checkpoint_bytes(db)? {
            match bincode::deserialize::<PersistedIndexCheckpoint>(raw.as_ref()) {
                Ok(checkpoint)
                    if checkpoint.schema_version
                        == config::META_INDEX_CHECKPOINT_SCHEMA_VERSION
                        && checkpoint.generation == generation =>
                {
                    return Ok((checkpoint.index, checkpoint.applied_wal_seq));
                }
                Ok(_) => {}
                Err(err) => {
                    eprintln!(
                        "spfresh-layerdb checkpoint decode failed, rebuilding index: {err:#}"
                    );
                }
            }
        }

        let applied_wal_seq = wal_next_seq.checked_sub(1);
        let index = match cfg.memory_mode {
            SpFreshMemoryMode::Resident => {
                let rows = load_rows(db, generation)?;
                RuntimeSpFreshIndex::Resident(SpFreshIndex::build(cfg.spfresh.clone(), &rows))
            }
            SpFreshMemoryMode::OffHeap => {
                let rows = load_rows(db, generation)?;
                RuntimeSpFreshIndex::OffHeap(SpFreshOffHeapIndex::build(cfg.spfresh.clone(), &rows))
            }
            SpFreshMemoryMode::OffHeapDiskMeta => {
                let (rows, assignments) = load_rows_with_posting_assignments(db, generation)?;
                let (index, _assigned_now) = SpFreshDiskMetaIndex::build_from_rows_with_assignments(
                    cfg.spfresh.clone(),
                    &rows,
                    Some(&assignments),
                );
                RuntimeSpFreshIndex::OffHeapDiskMeta(index)
            }
        };
        Ok((index, applied_wal_seq))
    }

    fn extract_diskmeta_snapshot(
        index: &RuntimeSpFreshIndex,
    ) -> Option<Arc<SpFreshDiskMetaIndex>> {
        match index {
            RuntimeSpFreshIndex::OffHeapDiskMeta(index) => Some(Arc::new(index.clone())),
            _ => None,
        }
    }

    fn replay_wal_tail(
        db: &Db,
        vector_cache: &Arc<Mutex<VectorCache>>,
        vector_blocks: &Arc<Mutex<VectorBlockStore>>,
        generation: u64,
        index: &mut RuntimeSpFreshIndex,
        from_seq: u64,
    ) -> anyhow::Result<()> {
        let entries = load_wal_entries_since(db, from_seq)?;
        if entries.is_empty() {
            return Ok(());
        }

        let mut touched = HashSet::with_capacity(entries.len());
        for entry in entries {
            match entry {
                IndexWalEntry::Touch { id } => {
                    touched.insert(id);
                }
                IndexWalEntry::TouchBatch { ids } => {
                    touched.extend(ids);
                }
            }
        }

        for id in touched {
            let resolved = Self::load_vector_for_id(db, vector_cache, vector_blocks, generation, id)?;
            match resolved {
                Some(values) => match index {
                    RuntimeSpFreshIndex::Resident(index) => index.upsert(id, values),
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader = Self::loader_for(
                            db,
                            vector_cache,
                            vector_blocks,
                            generation,
                            Some((id, values.clone())),
                        );
                        index.upsert_with(id, values, &mut loader)?;
                    }
                    RuntimeSpFreshIndex::OffHeapDiskMeta(index) => {
                        let posting = index.choose_posting(&values).unwrap_or_default();
                        index.apply_upsert(None, posting, values);
                    }
                },
                None => match index {
                    RuntimeSpFreshIndex::Resident(index) => {
                        let _ = index.delete(id);
                    }
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader =
                            Self::loader_for(db, vector_cache, vector_blocks, generation, None);
                        let _ = index.delete_with(id, &mut loader)?;
                    }
                    RuntimeSpFreshIndex::OffHeapDiskMeta(index) => {
                        let _ = index.apply_delete(None);
                    }
                },
            }
        }
        Ok(())
    }

    fn persist_index_checkpoint(&self) -> anyhow::Result<()> {
        self.flush_pending_commits()?;
        let next_wal_seq = self.wal_next_seq.load(Ordering::Relaxed);
        let snapshot = lock_read(&self.index).clone();
        let generation = self.active_generation.load(Ordering::Relaxed);
        let posting_event_next_seq = self.posting_event_next_seq.load(Ordering::Relaxed);
        let epoch = self.startup_epoch.load(Ordering::Relaxed);
        let checkpoint = PersistedIndexCheckpoint {
            schema_version: config::META_INDEX_CHECKPOINT_SCHEMA_VERSION,
            generation,
            applied_wal_seq: next_wal_seq.checked_sub(1),
            index: snapshot,
        };
        let bytes = bincode::serialize(&checkpoint).context("encode spfresh index checkpoint")?;
        persist_index_checkpoint_bytes(&self.db, bytes, self.cfg.write_sync)?;
        let manifest = PersistedStartupManifest {
            schema_version: STARTUP_MANIFEST_SCHEMA_VERSION,
            generation,
            applied_wal_seq: next_wal_seq.checked_sub(1),
            posting_event_next_seq,
            epoch,
        };
        let manifest_bytes =
            bincode::serialize(&manifest).context("encode spfresh startup manifest")?;
        persist_startup_manifest_bytes(&self.db, manifest_bytes, self.cfg.write_sync)?;
        if let Err(err) = prune_wal_before(&self.db, next_wal_seq, false) {
            eprintln!("spfresh-layerdb wal prune failed: {err:#}");
        }
        Ok(())
    }

    fn runtime(&self) -> RebuilderRuntime {
        RebuilderRuntime {
            db: self.db.clone(),
            rebuild_pending_ops: self.cfg.rebuild_pending_ops.max(1),
            rebuild_interval: self.cfg.rebuild_interval,
            active_generation: self.active_generation.clone(),
            index: self.index.clone(),
            update_gate: self.update_gate.clone(),
            dirty_ids: self.dirty_ids.clone(),
            pending_ops: self.pending_ops.clone(),
            vector_cache: self.vector_cache.clone(),
            vector_blocks: self.vector_blocks.clone(),
            stats: self.stats.clone(),
            stop_worker: self.stop_worker.clone(),
        }
    }

    fn load_vector_for_id(
        db: &Db,
        vector_cache: &Arc<Mutex<VectorCache>>,
        vector_blocks: &Arc<Mutex<VectorBlockStore>>,
        generation: u64,
        id: u64,
    ) -> anyhow::Result<Option<Vec<f32>>> {
        if let Some(values) = lock_mutex(vector_cache).get(id) {
            return Ok(Some(values));
        }
        if let Some(values) = lock_mutex(vector_blocks).get(id) {
            lock_mutex(vector_cache).put(id, values.clone());
            return Ok(Some(values));
        }
        let Some(row) = load_row(db, generation, id)? else {
            return Ok(None);
        };
        let values = row.values;
        lock_mutex(vector_cache).put(id, values.clone());
        Ok(Some(values))
    }

    fn load_distances_for_ids(
        db: &Db,
        vector_cache: &Arc<Mutex<VectorCache>>,
        vector_blocks: &Arc<Mutex<VectorBlockStore>>,
        generation: u64,
        ids: &[u64],
        query: &[f32],
    ) -> anyhow::Result<DistanceLoadResult> {
        let mut out = Vec::with_capacity(ids.len());
        let mut cache_misses = Vec::new();
        {
            let cache = lock_mutex(vector_cache);
            for id in ids.iter().copied() {
                if let Some(distance) = cache.distance_to(id, query) {
                    out.push((id, distance));
                } else {
                    cache_misses.push(id);
                }
            }
        }
        if cache_misses.is_empty() {
            return Ok((out, Vec::new()));
        }

        let mut unresolved = Vec::new();
        let mut fetched_for_cache = Vec::new();
        {
            let blocks = lock_mutex(vector_blocks);
            for id in cache_misses {
                if let Some(values) = blocks.get(id) {
                    let distance = squared_l2(query, values.as_slice());
                    out.push((id, distance));
                    fetched_for_cache.push((id, values));
                } else {
                    unresolved.push(id);
                }
            }
        }

        let mut missing = Vec::new();
        if !unresolved.is_empty() {
            let keys: Vec<Bytes> = unresolved
                .iter()
                .map(|id| Bytes::from(vector_key(generation, *id)))
                .collect();
            let rows = db
                .multi_get(&keys, ReadOptions::default())
                .context("diskmeta multi_get fallback vector rows for distance")?;
            for (id, row_raw) in unresolved.into_iter().zip(rows.into_iter()) {
                let Some(raw) = row_raw else {
                    missing.push(id);
                    continue;
                };
                let row = decode_vector_row_value(raw.as_ref()).with_context(|| {
                    format!(
                        "decode vector row for distance id={id} generation={generation}"
                    )
                })?;
                if row.deleted {
                    missing.push(id);
                    continue;
                }
                let distance = squared_l2(query, row.values.as_slice());
                out.push((id, distance));
                fetched_for_cache.push((id, row.values));
            }
        }

        if !fetched_for_cache.is_empty() {
            let mut cache = lock_mutex(vector_cache);
            for (id, values) in fetched_for_cache {
                cache.put(id, values);
            }
        }
        Ok((out, missing))
    }

    fn loader_for<'a>(
        db: &'a Db,
        vector_cache: &'a Arc<Mutex<VectorCache>>,
        vector_blocks: &'a Arc<Mutex<VectorBlockStore>>,
        generation: u64,
        override_row: Option<(u64, Vec<f32>)>,
    ) -> impl FnMut(u64) -> anyhow::Result<Option<Vec<f32>>> + 'a {
        move |id| {
            if let Some((override_id, values)) = &override_row {
                if *override_id == id {
                    return Ok(Some(values.clone()));
                }
            }
            Self::load_vector_for_id(db, vector_cache, vector_blocks, generation, id)
        }
    }

    fn load_posting_members_for(
        &self,
        generation: u64,
        posting_id: usize,
    ) -> anyhow::Result<Arc<Vec<PostingMemberSketch>>> {
        if let Some(ids) = lock_mutex(&self.posting_members_cache).get(generation, posting_id) {
            return Ok(ids);
        }
        let loaded = load_posting_members(&self.db, generation, posting_id)?;
        if loaded.scanned_events >= POSTING_LOG_COMPACT_MIN_EVENTS
            && loaded.scanned_events > loaded.members.len().saturating_mul(POSTING_LOG_COMPACT_FACTOR)
        {
            if let Err(err) =
                self.compact_posting_members_log(generation, posting_id, &loaded.members)
            {
                eprintln!(
                    "spfresh-layerdb posting log compaction failed generation={} posting={}: {err:#}",
                    generation, posting_id
                );
            }
        }
        let mut sketches = Vec::with_capacity(loaded.members.len());
        for member in loaded.members {
            sketches.push(PostingMemberSketch::from_loaded(&member));
        }
        let sketches = Arc::new(sketches);
        lock_mutex(&self.posting_members_cache).put_arc(generation, posting_id, sketches.clone());
        Ok(sketches)
    }

    fn compact_posting_members_log(
        &self,
        generation: u64,
        posting_id: usize,
        members: &[PostingMember],
    ) -> anyhow::Result<()> {
        let mut canonical = members.to_vec();
        canonical.sort_by_key(|m| m.id);

        let mut next_seq = self.posting_event_next_seq.load(Ordering::Relaxed);
        let prefix = posting_members_prefix(generation, posting_id);
        let prefix_bytes = prefix.into_bytes();
        let end = prefix_exclusive_end(&prefix_bytes)?;
        let mut ops = vec![layerdb::Op::delete_range(prefix_bytes, end)];
        for member in canonical {
            let seq = next_seq;
            next_seq = next_seq.saturating_add(1);
            let scale = member.residual_scale.unwrap_or(1.0);
            let code = member.residual_code.unwrap_or_default();
            let value = posting_member_event_upsert_value_from_sketch(member.id, scale, &code)?;
            ops.push(layerdb::Op::put(
                posting_member_event_key(generation, posting_id, seq, member.id),
                value,
            ));
        }
        let posting_event_next =
            bincode::serialize(&next_seq).context("encode posting-event next seq")?;
        ops.push(layerdb::Op::put(
            config::META_POSTING_EVENT_NEXT_SEQ_KEY,
            posting_event_next,
        ));
        self.submit_commit(ops, false, true)
            .context("compact posting-member append log")?;
        self.posting_event_next_seq.store(next_seq, Ordering::Relaxed);
        Ok(())
    }

    fn set_commit_error(&self, err: anyhow::Error) {
        let mut slot = lock_mutex(&self.commit_error);
        if slot.is_none() {
            *slot = Some(format!("{err:#}"));
        }
    }

    fn check_commit_error(&self) -> anyhow::Result<()> {
        if let Some(err) = lock_mutex(&self.commit_error).clone() {
            anyhow::bail!("async commit pipeline failed: {err}");
        }
        Ok(())
    }

    fn handle_commit_result(&self, result: anyhow::Result<()>) -> anyhow::Result<()> {
        if let Err(err) = result {
            self.set_commit_error(anyhow::anyhow!("{err:#}"));
            return Err(err);
        }
        Ok(())
    }

    fn poll_pending_commits(&self) -> anyhow::Result<()> {
        loop {
            let next = {
                let mut pending = lock_mutex(&self.pending_commit_acks);
                let Some(front) = pending.front() else {
                    break;
                };
                match front.try_recv() {
                    Ok(result) => {
                        pending.pop_front();
                        Some(result)
                    }
                    Err(mpsc::TryRecvError::Empty) => None,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        pending.pop_front();
                        Some(Err(anyhow::anyhow!("commit worker response channel disconnected")))
                    }
                }
            };
            let Some(result) = next else {
                break;
            };
            self.handle_commit_result(result)?;
        }
        self.check_commit_error()
    }

    fn throttle_pending_commits(&self) -> anyhow::Result<()> {
        while lock_mutex(&self.pending_commit_acks).len() > self.max_async_commit_inflight.max(1) {
            let Some(rx) = lock_mutex(&self.pending_commit_acks).pop_front() else {
                break;
            };
            let result = rx.recv().context("receive throttled async commit result")?;
            self.handle_commit_result(result)?;
        }
        self.check_commit_error()
    }

    fn flush_pending_commits(&self) -> anyhow::Result<()> {
        loop {
            let next = lock_mutex(&self.pending_commit_acks).pop_front();
            let Some(rx) = next else {
                break;
            };
            let result = rx.recv().context("receive async commit result")?;
            self.handle_commit_result(result)?;
        }
        self.check_commit_error()
    }

    fn submit_commit(
        &self,
        ops: Vec<layerdb::Op>,
        sync: bool,
        wait_for_ack: bool,
    ) -> anyhow::Result<()> {
        self.poll_pending_commits()?;
        let (resp_tx, resp_rx) = mpsc::channel::<anyhow::Result<()>>();
        self.commit_tx
            .send(CommitRequest::Write {
                ops,
                sync,
                resp: resp_tx,
            })
            .context("send commit request")?;
        if wait_for_ack {
            let result = resp_rx.recv().context("receive commit result")?;
            self.handle_commit_result(result)
        } else {
            lock_mutex(&self.pending_commit_acks).push_back(resp_rx);
            self.throttle_pending_commits()?;
            self.poll_pending_commits()
        }
    }

    fn neighbor_cmp(a: &Neighbor, b: &Neighbor) -> std::cmp::Ordering {
        a.distance
            .total_cmp(&b.distance)
            .then_with(|| a.id.cmp(&b.id))
    }

    fn push_neighbor_topk(top: &mut Vec<Neighbor>, candidate: Neighbor, k: usize) {
        if k == 0 {
            return;
        }
        if top.len() < k {
            top.push(candidate);
            return;
        }
        let mut worst_idx = 0usize;
        for idx in 1..top.len() {
            if Self::neighbor_cmp(&top[idx], &top[worst_idx]).is_gt() {
                worst_idx = idx;
            }
        }
        if Self::neighbor_cmp(&candidate, &top[worst_idx]).is_lt() {
            top[worst_idx] = candidate;
        }
    }

    fn approx_distance_from_residual(
        query: &[f32],
        centroid: &[f32],
        member: &PostingMemberSketch,
    ) -> Option<f32> {
        let scale = member.residual_scale?;
        let code = member.residual_code.as_ref()?;
        if centroid.len() != query.len() || code.len() != query.len() {
            return None;
        }
        let mut sum = 0.0f32;
        for i in 0..query.len() {
            let approx = centroid[i] + scale * code[i] as f32;
            let d = query[i] - approx;
            sum += d * d;
        }
        Some(sum)
    }

    fn select_diskmeta_candidates(
        query: &[f32],
        centroid: &[f32],
        members: &[PostingMemberSketch],
        k: usize,
    ) -> Vec<u64> {
        if k == 0 || members.is_empty() {
            return Vec::new();
        }
        if members
            .iter()
            .any(|m| m.residual_scale.is_none() || m.residual_code.is_none())
        {
            return members.iter().map(|m| m.id).collect();
        }
        let rerank = k
            .saturating_mul(DISKMETA_RERANK_FACTOR)
            .max(k)
            .min(members.len());
        let mut scored: Vec<(u64, f32)> = members
            .iter()
            .map(|member| {
                let approx = Self::approx_distance_from_residual(query, centroid, member)
                    .unwrap_or(f32::INFINITY);
                (member.id, approx)
            })
            .collect();
        scored.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        scored.truncate(rerank.max(1));
        scored.into_iter().map(|(id, _)| id).collect()
    }

    fn residual_sketch_for_member(id: u64, values: &[f32], centroid: &[f32]) -> PostingMemberSketch {
        if values.len() != centroid.len() || values.is_empty() {
            return PostingMemberSketch {
                id,
                residual_scale: None,
                residual_code: None,
            };
        }
        let mut max_abs = 0.0f32;
        for (v, c) in values.iter().zip(centroid.iter()) {
            let a = (*v - *c).abs();
            if a > max_abs {
                max_abs = a;
            }
        }
        let scale = if max_abs <= 1e-9 { 1e-9 } else { max_abs / 127.0 };
        let mut code = Vec::with_capacity(values.len());
        for (v, c) in values.iter().zip(centroid.iter()) {
            let q = ((*v - *c) / scale).round().clamp(-127.0, 127.0) as i8;
            code.push(q);
        }
        PostingMemberSketch {
            id,
            residual_scale: Some(scale),
            residual_code: Some(code),
        }
    }

    fn mark_dirty(&self, id: u64) {
        let mut dirty = lock_mutex(&self.dirty_ids);
        dirty.insert(id);
        self.pending_ops.store(dirty.len(), Ordering::Relaxed);
    }

    fn maybe_prewarm_vector_cache_from_blocks(&self) {
        let capacity = lock_mutex(&self.vector_cache).capacity;
        if capacity == 0 {
            return;
        }

        let warmed = {
            let blocks = lock_mutex(&self.vector_blocks);
            if blocks.live_len() > capacity {
                Vec::new()
            } else {
                let ids = blocks.live_ids();
                let mut rows = Vec::with_capacity(ids.len());
                for id in ids {
                    if let Some(values) = blocks.get(id) {
                        rows.push((id, values));
                    }
                }
                rows
            }
        };
        if warmed.is_empty() {
            return;
        }

        let mut cache = lock_mutex(&self.vector_cache);
        for (id, values) in warmed {
            cache.put(id, values);
        }
    }

    fn maybe_prewarm_posting_members_cache(&self) {
        if self.cfg.memory_mode != SpFreshMemoryMode::OffHeapDiskMeta {
            return;
        }
        let generation = self.active_generation.load(Ordering::Relaxed);
        let Some(index) = self.diskmeta_search_snapshot.load_full() else {
            return;
        };
        let posting_ids = index.posting_ids();
        if posting_ids.is_empty() {
            return;
        }
        let capacity = lock_mutex(&self.posting_members_cache).capacity();
        if posting_ids.len() > capacity {
            return;
        }

        for posting_id in posting_ids {
            if let Err(err) = self.load_posting_members_for(generation, posting_id) {
                eprintln!(
                    "spfresh-layerdb posting-members cache prewarm failed generation={} posting={}: {err:#}",
                    generation, posting_id
                );
                break;
            }
        }
    }

    pub fn bulk_load(&mut self, rows: &[VectorRecord]) -> anyhow::Result<()> {
        self.try_bulk_load(rows)
    }

    pub fn try_bulk_load(&mut self, rows: &[VectorRecord]) -> anyhow::Result<()> {
        let _update_guard = lock_write(&self.update_gate);
        self.flush_pending_commits()?;

        let old_generation = self.active_generation.load(Ordering::Relaxed);
        let new_generation = old_generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("active generation overflow"))?;
        let mut disk_assignments = None;
        let mut disk_index = None;
        let mut posting_event_next_seq = self.posting_event_next_seq.load(Ordering::Relaxed);
        if self.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta {
            let (index, assignments) =
                SpFreshDiskMetaIndex::build_with_assignments(self.cfg.spfresh.clone(), rows);
            disk_assignments = Some(assignments);
            disk_index = Some(index);
        }

        for batch in rows.chunks(1_024) {
            let mut ops = Vec::with_capacity(batch.len().saturating_mul(3));
            for row in batch {
                if let Some(assignments) = &disk_assignments {
                    let posting = assignments.get(&row.id).copied().ok_or_else(|| {
                        anyhow::anyhow!("missing diskmeta assignment for id={}", row.id)
                    })?;
                    let value = encode_vector_row_value_with_posting(row, Some(posting))
                        .with_context(|| format!("serialize vector row id={}", row.id))?;
                    ops.push(layerdb::Op::put(vector_key(new_generation, row.id), value));
                    let centroid = disk_index
                        .as_ref()
                        .and_then(|idx| idx.posting_centroid(posting))
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "missing centroid for posting {} during diskmeta bulk-load",
                                posting
                            )
                        })?;
                    let event_seq = posting_event_next_seq;
                    posting_event_next_seq = posting_event_next_seq.saturating_add(1);
                    ops.push(layerdb::Op::put(
                        posting_member_event_key(new_generation, posting, event_seq, row.id),
                        posting_member_event_upsert_value_with_residual(
                            row.id,
                            &row.values,
                            centroid,
                        )?,
                    ));
                } else {
                    let value = encode_vector_row_value(row)
                        .with_context(|| format!("serialize vector row id={}", row.id))?;
                    ops.push(layerdb::Op::put(vector_key(new_generation, row.id), value));
                }
            }
            self.submit_commit(ops, self.cfg.write_sync, true)
                .context("persist spfresh bulk rows")?;
        }
        set_active_generation(&self.db, new_generation, self.cfg.write_sync)?;
        self.active_generation
            .store(new_generation, Ordering::Relaxed);
        let new_epoch = self.startup_epoch.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        {
            let mut blocks = lock_mutex(&self.vector_blocks);
            if let Err(err) = blocks.rotate_epoch(new_epoch) {
                eprintln!("spfresh-layerdb vector blocks rotate failed: {err:#}");
            } else {
                for row in rows {
                    let posting = disk_assignments
                        .as_ref()
                        .and_then(|assignments| assignments.get(&row.id).copied());
                    if let Err(err) = blocks.append_upsert_with_posting(row.id, posting, &row.values)
                    {
                        eprintln!("spfresh-layerdb vector blocks bulk append failed: {err:#}");
                        break;
                    }
                }
            }
        }
        if self.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta {
            set_posting_event_next_seq(&self.db, posting_event_next_seq, self.cfg.write_sync)?;
            self.posting_event_next_seq
                .store(posting_event_next_seq, Ordering::Relaxed);
        }

        // best-effort cleanup of the old generation after pointer switch.
        for prefix in [
            vector_prefix(old_generation),
            posting_map_prefix(old_generation),
            posting_members_generation_prefix(old_generation),
        ] {
            let prefix_bytes = prefix.as_bytes().to_vec();
            let end = prefix_exclusive_end(&prefix_bytes)?;
            if let Err(err) = self
                .db
                .delete_range(prefix_bytes, end, WriteOptions { sync: false })
            {
                eprintln!("spfresh-layerdb bulk-load cleanup failed for prefix={prefix}: {err:#}");
            }
        }

        *lock_write(&self.index) = match self.cfg.memory_mode {
            SpFreshMemoryMode::Resident => {
                RuntimeSpFreshIndex::Resident(SpFreshIndex::build(self.cfg.spfresh.clone(), rows))
            }
            SpFreshMemoryMode::OffHeap => RuntimeSpFreshIndex::OffHeap(SpFreshOffHeapIndex::build(
                self.cfg.spfresh.clone(),
                rows,
            )),
            SpFreshMemoryMode::OffHeapDiskMeta => {
                RuntimeSpFreshIndex::OffHeapDiskMeta(disk_index.unwrap_or_else(|| {
                    SpFreshDiskMetaIndex::build_with_assignments(self.cfg.spfresh.clone(), rows).0
                }))
            }
        };
        let snapshot = {
            let guard = lock_read(&self.index);
            Self::extract_diskmeta_snapshot(&guard)
        };
        self.diskmeta_search_snapshot.store(snapshot);
        {
            let mut cache = lock_mutex(&self.vector_cache);
            cache.map.clear();
            cache.order.clear();
        }
        lock_mutex(&self.posting_members_cache).clear();
        self.maybe_prewarm_vector_cache_from_blocks();
        self.maybe_prewarm_posting_members_cache();
        lock_mutex(&self.dirty_ids).clear();
        self.pending_ops.store(0, Ordering::Relaxed);
        self.stats.set_last_rebuild_rows(rows.len());
        self.persist_index_checkpoint()?;
        Ok(())
    }

    pub fn force_rebuild(&self) -> anyhow::Result<()> {
        self.flush_pending_commits()?;
        let runtime = self.runtime();
        rebuild_once(&runtime)?;
        self.maybe_prewarm_vector_cache_from_blocks();
        self.maybe_prewarm_posting_members_cache();
        Ok(())
    }

    fn persist_with_wal_batch_ops(
        &self,
        entries: Vec<(IndexWalEntry, Vec<layerdb::Op>)>,
        mut trailer_ops: Vec<layerdb::Op>,
    ) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let start_seq = self.wal_next_seq.load(Ordering::Relaxed);
        let mut next_seq = start_seq;
        let mut ops = Vec::new();
        for (entry, mut row_ops) in entries {
            let wal_value = encode_wal_entry(&entry)?;
            row_ops.push(layerdb::Op::put(wal_key(next_seq), wal_value));
            ops.extend(row_ops);
            next_seq = next_seq
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("spfresh wal sequence overflow"))?;
        }
        let wal_next = bincode::serialize(&next_seq).context("encode spfresh wal next seq")?;
        ops.push(layerdb::Op::put(
            config::META_INDEX_WAL_NEXT_SEQ_KEY,
            wal_next,
        ));
        ops.append(&mut trailer_ops);
        self.submit_commit(ops, self.cfg.write_sync, self.cfg.write_sync)
            .with_context(|| {
                format!(
                    "persist vector+wal batch count={} seq_start={} seq_end={}",
                    next_seq.saturating_sub(start_seq),
                    start_seq,
                    next_seq.saturating_sub(1)
                )
            })?;
        self.wal_next_seq.store(next_seq, Ordering::Relaxed);
        Ok(())
    }

    fn dedup_last_upserts(rows: &[VectorRecord]) -> Vec<(u64, Vec<f32>)> {
        let mut seen = HashSet::with_capacity(rows.len());
        let mut out_rev = Vec::with_capacity(rows.len());
        for row in rows.iter().rev() {
            if seen.insert(row.id) {
                out_rev.push((row.id, row.values.clone()));
            }
        }
        out_rev.reverse();
        out_rev
    }

    fn dedup_last_upserts_owned(rows: Vec<VectorRecord>) -> Vec<(u64, Vec<f32>)> {
        let mut seen = HashSet::with_capacity(rows.len());
        let mut out_rev = Vec::with_capacity(rows.len());
        for row in rows.into_iter().rev() {
            if seen.insert(row.id) {
                out_rev.push((row.id, row.values));
            }
        }
        out_rev.reverse();
        out_rev
    }

    fn dedup_ids(ids: &[u64]) -> Vec<u64> {
        let mut seen = HashSet::with_capacity(ids.len());
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if seen.insert(*id) {
                out.push(*id);
            }
        }
        out
    }

    fn dedup_last_mutations(mutations: &[VectorMutation]) -> Vec<VectorMutation> {
        let mut seen = HashSet::with_capacity(mutations.len());
        let mut out_rev = Vec::with_capacity(mutations.len());
        for mutation in mutations.iter().rev() {
            if seen.insert(mutation.id()) {
                out_rev.push(mutation.clone());
            }
        }
        out_rev.reverse();
        out_rev
    }

    fn load_diskmeta_states_for_ids(
        &self,
        generation: u64,
        ids: &[u64],
    ) -> anyhow::Result<DiskMetaStateMap> {
        let mut out = HashMap::with_capacity(ids.len());
        if ids.is_empty() {
            return Ok(out);
        }

        let mut unresolved = Vec::new();
        {
            let blocks = lock_mutex(&self.vector_blocks);
            for id in ids.iter().copied() {
                if let Some(state) = blocks.get_state(id) {
                    if let Some(posting_id) = state.posting_id {
                        out.insert(id, Some((posting_id, state.values)));
                    } else {
                        unresolved.push(id);
                    }
                } else {
                    unresolved.push(id);
                }
            }
        }

        if unresolved.is_empty() {
            return Ok(out);
        }

        let row_keys: Vec<Bytes> = unresolved
            .iter()
            .map(|id| Bytes::from(vector_key(generation, *id)))
            .collect();
        let row_values = self
            .db
            .multi_get(&row_keys, ReadOptions::default())
            .context("diskmeta multi_get fallback vector rows")?;

        for (id, row_raw) in unresolved.into_iter().zip(row_values.into_iter()) {
            let Some(raw) = row_raw else {
                out.insert(id, None);
                continue;
            };
            let decoded = decode_vector_row_with_posting(raw.as_ref())
                .with_context(|| format!("decode vector row id={id} generation={generation}"))?;
            if decoded.row.deleted {
                out.insert(id, None);
                continue;
            }
            if let Some(posting_id) = decoded.posting_id {
                out.insert(id, Some((posting_id, decoded.row.values)));
            } else {
                out.insert(id, None);
            }
        }
        Ok(out)
    }

    pub fn try_upsert_batch(&mut self, rows: &[VectorRecord]) -> anyhow::Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        for row in rows {
            if row.values.len() != self.cfg.spfresh.dim {
                anyhow::bail!(
                    "invalid vector dim for id={}: got {}, expected {}",
                    row.id,
                    row.values.len(),
                    self.cfg.spfresh.dim
                );
            }
        }

        // Last-write-wins dedup keeps persistence/apply work proportional to unique ids.
        let mutations = Self::dedup_last_upserts(rows);
        self.try_upsert_batch_mutations(mutations)
    }

    pub fn try_upsert_batch_owned(&mut self, rows: Vec<VectorRecord>) -> anyhow::Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        for row in &rows {
            if row.values.len() != self.cfg.spfresh.dim {
                anyhow::bail!(
                    "invalid vector dim for id={}: got {}, expected {}",
                    row.id,
                    row.values.len(),
                    self.cfg.spfresh.dim
                );
            }
        }
        let mutations = Self::dedup_last_upserts_owned(rows);
        self.try_upsert_batch_mutations(mutations)
    }

    fn try_upsert_batch_mutations(
        &mut self,
        mutations: Vec<(u64, Vec<f32>)>,
    ) -> anyhow::Result<usize> {
        if mutations.is_empty() {
            return Ok(0);
        }
        self.poll_pending_commits()?;

        let _update_guard = lock_read(&self.update_gate);
        let generation = self.active_generation.load(Ordering::Relaxed);
        let persist_started = Instant::now();

        if self.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta {
            let ids: Vec<u64> = mutations.iter().map(|(id, _)| *id).collect();
            let states = self.load_diskmeta_states_for_ids(generation, &ids)?;
            let mut shadow = match &*lock_read(&self.index) {
                RuntimeSpFreshIndex::OffHeapDiskMeta(index) => index.clone(),
                _ => anyhow::bail!("diskmeta upsert batch called for non-diskmeta index"),
            };
            let mut posting_event_next_seq =
                self.posting_event_next_seq.load(Ordering::Relaxed);

            let mut batch_ops = Vec::with_capacity(mutations.len().saturating_mul(3));
            let mut touched_ids = Vec::with_capacity(mutations.len());
            let mut new_postings = HashMap::with_capacity(mutations.len());
            let mut cache_deltas = Vec::with_capacity(mutations.len());
            for (id, vector) in &mutations {
                let old = states.get(id).and_then(|state| state.as_ref());
                let new_posting = shadow.choose_posting(vector).unwrap_or(0);
                let centroid = shadow
                    .posting_centroid(new_posting)
                    .unwrap_or(vector.as_slice());
                let sketch = Self::residual_sketch_for_member(*id, vector, centroid);
                let value = encode_vector_row_fields(*id, 0, false, vector.as_slice(), Some(new_posting))
                    .context("serialize vector row")?;
                let upsert_event_seq = posting_event_next_seq;
                posting_event_next_seq = posting_event_next_seq.saturating_add(1);
                batch_ops.push(layerdb::Op::put(vector_key(generation, *id), value));
                batch_ops.push(layerdb::Op::put(
                    posting_member_event_key(generation, new_posting, upsert_event_seq, *id),
                    posting_member_event_upsert_value_with_residual(*id, vector, centroid)?,
                ));
                if let Some((old_posting, _)) = old {
                    if *old_posting != new_posting {
                        let tombstone_event_seq = posting_event_next_seq;
                        posting_event_next_seq = posting_event_next_seq.saturating_add(1);
                        batch_ops.push(layerdb::Op::put(posting_member_event_key(
                            generation,
                            *old_posting,
                            tombstone_event_seq,
                            *id,
                        ), posting_member_event_tombstone_value(*id)?));
                    }
                }
                touched_ids.push(*id);
                let old_posting = old.map(|(posting, _)| *posting);
                cache_deltas.push((*id, old_posting, new_posting, sketch));
                shadow.apply_upsert_ref(
                    old.map(|(posting, values)| (*posting, values.as_slice())),
                    new_posting,
                    vector.as_slice(),
                );
                new_postings.insert(*id, new_posting);
            }
            let posting_event_next = bincode::serialize(&posting_event_next_seq)
                .context("encode posting-event next seq")?;
            let trailer_ops = vec![layerdb::Op::put(
                config::META_POSTING_EVENT_NEXT_SEQ_KEY,
                posting_event_next,
            )];

            let persist_entries = vec![(
                IndexWalEntry::TouchBatch { ids: touched_ids },
                batch_ops,
            )];
            if let Err(err) = self.persist_with_wal_batch_ops(persist_entries, trailer_ops) {
                self.stats
                    .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
                self.stats.inc_persist_errors();
                return Err(err);
            }
            self.stats
                .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
            self.posting_event_next_seq
                .store(posting_event_next_seq, Ordering::Relaxed);
            {
                let mut blocks = lock_mutex(&self.vector_blocks);
                if let Err(err) =
                    blocks.append_upsert_batch_with_posting(&mutations, |id| new_postings.get(&id).copied())
                {
                    eprintln!("spfresh-layerdb vector blocks upsert append failed: {err:#}");
                }
            }

            let snapshot = {
                let mut index = lock_write(&self.index);
                let RuntimeSpFreshIndex::OffHeapDiskMeta(index) = &mut *index else {
                    anyhow::bail!("diskmeta upsert batch called for non-diskmeta runtime");
                };
                for (id, vector) in &mutations {
                    let Some(new_posting) = new_postings.get(id).copied() else {
                        continue;
                    };
                    let old = states.get(id).and_then(|state| state.as_ref());
                    index.apply_upsert_ref(
                        old.map(|(posting, values)| (*posting, values.as_slice())),
                        new_posting,
                        vector.as_slice(),
                    );
                }
                Some(Arc::new(index.clone()))
            };
            self.diskmeta_search_snapshot.store(snapshot);
            {
                let mut cache = lock_mutex(&self.vector_cache);
                for (id, vector) in &mutations {
                    cache.put(*id, vector.clone());
                }
            }
            {
                let mut members_cache = lock_mutex(&self.posting_members_cache);
                for (id, old_posting, new_posting, sketch) in cache_deltas {
                    if let Some(old_posting) = old_posting {
                        if old_posting != new_posting {
                            members_cache.apply_delete_delta(generation, old_posting, id);
                        }
                    }
                    members_cache.apply_upsert_delta(generation, new_posting, sketch);
                }
            }
            for _ in 0..mutations.len() {
                self.stats.inc_upserts();
            }
            return Ok(mutations.len());
        }

        let mut batch_ops = Vec::with_capacity(mutations.len());
        let mut touched_ids = Vec::with_capacity(mutations.len());
        for (id, vector) in &mutations {
            let value = encode_vector_row_fields(*id, 0, false, vector.as_slice(), None)
                .context("serialize vector row")?;
            batch_ops.push(layerdb::Op::put(vector_key(generation, *id), value));
            touched_ids.push(*id);
        }
        let persist_entries = vec![(
            IndexWalEntry::TouchBatch { ids: touched_ids },
            batch_ops,
        )];
        if let Err(err) = self.persist_with_wal_batch_ops(persist_entries, Vec::new()) {
            self.stats
                .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
            self.stats.inc_persist_errors();
            return Err(err);
        }
        self.stats
            .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
        {
            let mut blocks = lock_mutex(&self.vector_blocks);
            if let Err(err) = blocks.append_upsert_batch_with_posting(&mutations, |_| None) {
                eprintln!("spfresh-layerdb vector blocks upsert append failed: {err:#}");
            }
        }

        {
            let mut index = lock_write(&self.index);
            for (id, vector) in &mutations {
                match &mut *index {
                    RuntimeSpFreshIndex::Resident(index) => index.upsert(*id, vector.clone()),
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader = Self::loader_for(
                            &self.db,
                            &self.vector_cache,
                            &self.vector_blocks,
                            generation,
                            Some((*id, vector.clone())),
                        );
                        index
                            .upsert_with(*id, vector.clone(), &mut loader)
                            .with_context(|| format!("offheap upsert id={id}"))?;
                    }
                    RuntimeSpFreshIndex::OffHeapDiskMeta(_) => {
                        anyhow::bail!("non-diskmeta upsert batch called for diskmeta runtime");
                    }
                }
            }
        }
        {
            let mut cache = lock_mutex(&self.vector_cache);
            for (id, vector) in &mutations {
                cache.put(*id, vector.clone());
            }
        }
        for (id, _) in &mutations {
            self.mark_dirty(*id);
        }
        for _ in 0..mutations.len() {
            self.stats.inc_upserts();
        }
        let pending = self.pending_ops.load(Ordering::Relaxed);
        if pending >= self.cfg.rebuild_pending_ops.max(1) {
            let _ = self.rebuild_tx.send(());
        }
        Ok(mutations.len())
    }

    pub fn try_delete_batch(&mut self, ids: &[u64]) -> anyhow::Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mutations = Self::dedup_ids(ids);
        if mutations.is_empty() {
            return Ok(0);
        }
        self.poll_pending_commits()?;

        let _update_guard = lock_read(&self.update_gate);
        let generation = self.active_generation.load(Ordering::Relaxed);
        let persist_started = Instant::now();

        if self.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta {
            let states = self.load_diskmeta_states_for_ids(generation, &mutations)?;
            let mut batch_ops = Vec::with_capacity(mutations.len().saturating_mul(2));
            let mut touched_ids = Vec::with_capacity(mutations.len());
            let mut apply_entries = Vec::with_capacity(mutations.len());
            let mut posting_event_next_seq =
                self.posting_event_next_seq.load(Ordering::Relaxed);
            for id in &mutations {
                let old = states.get(id).cloned().unwrap_or(None);
                batch_ops.push(layerdb::Op::delete(vector_key(generation, *id)));
                if let Some((old_posting, _)) = &old {
                    let tombstone_event_seq = posting_event_next_seq;
                    posting_event_next_seq = posting_event_next_seq.saturating_add(1);
                    batch_ops.push(layerdb::Op::put(
                        posting_member_event_key(generation, *old_posting, tombstone_event_seq, *id),
                        posting_member_event_tombstone_value(*id)?,
                    ));
                }
                touched_ids.push(*id);
                apply_entries.push((*id, old));
            }
            let posting_event_next = bincode::serialize(&posting_event_next_seq)
                .context("encode posting-event next seq")?;
            let trailer_ops = vec![layerdb::Op::put(
                config::META_POSTING_EVENT_NEXT_SEQ_KEY,
                posting_event_next,
            )];
            let persist_entries = vec![(
                IndexWalEntry::TouchBatch { ids: touched_ids },
                batch_ops,
            )];
            if let Err(err) = self.persist_with_wal_batch_ops(persist_entries, trailer_ops) {
                self.stats
                    .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
                self.stats.inc_persist_errors();
                return Err(err);
            }
            self.stats
                .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
            self.posting_event_next_seq
                .store(posting_event_next_seq, Ordering::Relaxed);
            {
                let mut blocks = lock_mutex(&self.vector_blocks);
                if let Err(err) = blocks.append_delete_batch(&mutations) {
                    eprintln!("spfresh-layerdb vector blocks tombstone append failed: {err:#}");
                }
            }

            let mut deleted = 0usize;
            let snapshot = {
                let mut index = lock_write(&self.index);
                let RuntimeSpFreshIndex::OffHeapDiskMeta(index) = &mut *index else {
                    anyhow::bail!("diskmeta delete batch called for non-diskmeta runtime");
                };
                for (_id, old) in apply_entries {
                    if index.apply_delete(old) {
                        deleted += 1;
                    }
                }
                Some(Arc::new(index.clone()))
            };
            self.diskmeta_search_snapshot.store(snapshot);
            {
                let mut cache = lock_mutex(&self.vector_cache);
                for id in &mutations {
                    cache.remove(*id);
                }
            }
            {
                let mut members_cache = lock_mutex(&self.posting_members_cache);
                for id in &mutations {
                    if let Some(Some((old_posting, _))) = states.get(id) {
                        members_cache.apply_delete_delta(generation, *old_posting, *id);
                    }
                }
            }
            for _ in 0..deleted {
                self.stats.inc_deletes();
            }
            return Ok(deleted);
        }

        let mut batch_ops = Vec::with_capacity(mutations.len());
        let mut touched_ids = Vec::with_capacity(mutations.len());
        for id in &mutations {
            batch_ops.push(layerdb::Op::delete(vector_key(generation, *id)));
            touched_ids.push(*id);
        }
        let persist_entries = vec![(
            IndexWalEntry::TouchBatch { ids: touched_ids },
            batch_ops,
        )];
        if let Err(err) = self.persist_with_wal_batch_ops(persist_entries, Vec::new()) {
            self.stats
                .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
            self.stats.inc_persist_errors();
            return Err(err);
        }
        self.stats
            .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
        {
            let mut blocks = lock_mutex(&self.vector_blocks);
            if let Err(err) = blocks.append_delete_batch(&mutations) {
                eprintln!("spfresh-layerdb vector blocks tombstone append failed: {err:#}");
            }
        }

        let mut deleted = 0usize;
        {
            let mut index = lock_write(&self.index);
            for id in &mutations {
                let was_deleted = match &mut *index {
                    RuntimeSpFreshIndex::Resident(index) => index.delete(*id),
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader = Self::loader_for(
                            &self.db,
                            &self.vector_cache,
                            &self.vector_blocks,
                            generation,
                            None,
                        );
                        index
                            .delete_with(*id, &mut loader)
                            .with_context(|| format!("offheap delete id={id}"))?
                    }
                    RuntimeSpFreshIndex::OffHeapDiskMeta(_) => {
                        anyhow::bail!("non-diskmeta delete batch called for diskmeta runtime");
                    }
                };
                if was_deleted {
                    deleted += 1;
                    self.mark_dirty(*id);
                }
            }
        }
        {
            let mut cache = lock_mutex(&self.vector_cache);
            for id in &mutations {
                cache.remove(*id);
            }
        }
        for _ in 0..deleted {
            self.stats.inc_deletes();
        }
        if deleted > 0 {
            let pending = self.pending_ops.load(Ordering::Relaxed);
            if pending >= self.cfg.rebuild_pending_ops.max(1) {
                let _ = self.rebuild_tx.send(());
            }
        }
        Ok(deleted)
    }

    pub fn try_apply_batch(
        &mut self,
        mutations: &[VectorMutation],
    ) -> anyhow::Result<VectorMutationBatchResult> {
        if mutations.is_empty() {
            return Ok(VectorMutationBatchResult::default());
        }
        let deduped = Self::dedup_last_mutations(mutations);
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();
        for mutation in deduped {
            match mutation {
                VectorMutation::Upsert(row) => upserts.push(row),
                VectorMutation::Delete { id } => deletes.push(id),
            }
        }
        let upserted = self.try_upsert_batch(&upserts)?;
        let deleted = self.try_delete_batch(&deletes)?;
        Ok(VectorMutationBatchResult {
            upserts: upserted,
            deletes: deleted,
        })
    }

    pub fn try_upsert(&mut self, id: u64, vector: Vec<f32>) -> anyhow::Result<()> {
        let row = VectorRecord::new(id, vector);
        let _ = self.try_upsert_batch(&[row])?;
        Ok(())
    }

    pub fn try_delete(&mut self, id: u64) -> anyhow::Result<bool> {
        Ok(self.try_delete_batch(&[id])? > 0)
    }

    pub fn close(mut self) -> anyhow::Result<()> {
        self.stop_worker.store(true, Ordering::Relaxed);
        let _ = self.rebuild_tx.send(());
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                anyhow::bail!("spfresh-layerdb background worker panicked");
            }
        }
        self.flush_pending_commits()?;
        self.force_rebuild()?;
        self.persist_index_checkpoint()?;
        let _ = self.commit_tx.send(CommitRequest::Shutdown);
        if let Some(worker) = self.commit_worker.take() {
            if worker.join().is_err() {
                anyhow::bail!("spfresh-layerdb commit worker panicked");
            }
        }
        Ok(())
    }

    pub fn stats(&self) -> SpFreshLayerDbStats {
        self.stats
            .snapshot(self.pending_ops.load(Ordering::Relaxed) as u64)
    }

    pub fn memory_mode(&self) -> SpFreshMemoryMode {
        self.cfg.memory_mode
    }

    pub fn health_check(&self) -> anyhow::Result<SpFreshLayerDbStats> {
        ensure_wal_exists(&self.db_path)?;
        ensure_metadata(&self.db, &self.cfg)?;
        let _ = ensure_active_generation(&self.db)?;
        Ok(self.stats())
    }

    /// Flush and compact vector data, then freeze level-1 SSTs into S3 tier.
    ///
    /// This is the primary durability/tiering operation for SPFresh-on-LayerDB.
    pub fn sync_to_s3(&self, max_files: Option<usize>) -> anyhow::Result<usize> {
        let _update_guard = lock_write(&self.update_gate);
        self.db
            .compact_range(None)
            .context("compact before freeze-to-s3")?;
        self.db
            .freeze_level_to_s3(1, max_files)
            .context("freeze level-1 to s3")
    }

    /// Thaw frozen level-1 SSTs back to local tier.
    pub fn thaw_from_s3(&self, max_files: Option<usize>) -> anyhow::Result<usize> {
        let _update_guard = lock_write(&self.update_gate);
        self.db
            .thaw_level_from_s3(1, max_files)
            .context("thaw level-1 from s3")
    }

    /// Garbage collect orphaned S3 objects that are no longer referenced.
    pub fn gc_orphaned_s3(&self) -> anyhow::Result<usize> {
        self.db
            .gc_orphaned_s3_files()
            .context("gc orphaned s3 files")
    }

    pub fn frozen_objects(&self) -> Vec<layerdb::version::FrozenObjectMeta> {
        self.db.frozen_objects()
    }
}

impl Drop for SpFreshLayerDbIndex {
    fn drop(&mut self) {
        self.stop_worker.store(true, Ordering::Relaxed);
        let _ = self.rebuild_tx.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        if let Err(err) = self.flush_pending_commits() {
            eprintln!("spfresh-layerdb flush pending commits on drop failed: {err:#}");
        }
        let _ = self.commit_tx.send(CommitRequest::Shutdown);
        if let Some(worker) = self.commit_worker.take() {
            let _ = worker.join();
        }
    }
}

impl VectorIndex for SpFreshLayerDbIndex {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        self.try_upsert(id, vector)
            .unwrap_or_else(|err| panic!("spfresh-layerdb upsert failed for id={id}: {err:#}"));
    }

    fn delete(&mut self, id: u64) -> bool {
        self.try_delete(id)
            .unwrap_or_else(|err| panic!("spfresh-layerdb delete failed for id={id}: {err:#}"))
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<Neighbor> {
        let started = Instant::now();
        let generation = self.active_generation.load(Ordering::Relaxed);
        let diskmeta_snapshot = self.diskmeta_search_snapshot.load_full();
        let out = if let Some(index) = diskmeta_snapshot {
            let mut top = Vec::with_capacity(k);
            let postings = index.choose_probe_postings(query, k);
            for posting_id in postings {
                let Some(centroid) = index.posting_centroid(posting_id) else {
                    continue;
                };
                let members = self
                    .load_posting_members_for(generation, posting_id)
                    .unwrap_or_else(|err| panic!("offheap-diskmeta load members failed: {err:#}"));
                let selected_ids =
                    Self::select_diskmeta_candidates(query, centroid, members.as_ref(), k);
                if selected_ids.is_empty() {
                    continue;
                }
                let (selected_exact, missing_selected_ids) = Self::load_distances_for_ids(
                    &self.db,
                    &self.vector_cache,
                    &self.vector_blocks,
                    generation,
                    &selected_ids,
                    query,
                )
                .unwrap_or_else(|err| {
                    panic!("offheap-diskmeta load selected distances failed: {err:#}")
                });
                let mut exact_loaded_ids = HashSet::with_capacity(selected_ids.len());
                for (id, distance) in selected_exact {
                    Self::push_neighbor_topk(&mut top, Neighbor { id, distance }, k);
                    exact_loaded_ids.insert(id);
                }

                // If selected candidates miss exact payloads, retry exact evaluation across all
                // members before falling back to residual-only distance estimates.
                if !missing_selected_ids.is_empty() {
                    let fallback_ids: Vec<u64> = members
                        .iter()
                        .map(|member| member.id)
                        .filter(|id| !exact_loaded_ids.contains(id))
                        .collect();
                    let (fallback_exact, fallback_missing) = Self::load_distances_for_ids(
                        &self.db,
                        &self.vector_cache,
                        &self.vector_blocks,
                        generation,
                        &fallback_ids,
                        query,
                    )
                    .unwrap_or_else(|err| {
                        panic!("offheap-diskmeta load fallback distances failed: {err:#}")
                    });
                    for (id, distance) in fallback_exact {
                        Self::push_neighbor_topk(&mut top, Neighbor { id, distance }, k);
                        exact_loaded_ids.insert(id);
                    }
                    if !fallback_missing.is_empty() {
                        let lookup: HashMap<u64, &PostingMemberSketch> =
                            members.iter().map(|member| (member.id, member)).collect();
                        for id in fallback_missing {
                            let Some(member) = lookup.get(&id) else {
                                continue;
                            };
                            let Some(distance) =
                                Self::approx_distance_from_residual(query, centroid, member)
                            else {
                                continue;
                            };
                            Self::push_neighbor_topk(&mut top, Neighbor { id, distance }, k);
                        }
                    }
                }
            }
            top.sort_by(Self::neighbor_cmp);
            top
        } else {
            match &*lock_read(&self.index) {
                RuntimeSpFreshIndex::Resident(index) => index.search(query, k),
                RuntimeSpFreshIndex::OffHeap(index) => {
                    let mut loader = Self::loader_for(
                        &self.db,
                        &self.vector_cache,
                        &self.vector_blocks,
                        generation,
                        None,
                    );
                    match index.search_with(query, k, &mut loader) {
                        Ok(out) => out,
                        Err(err) => panic!("spfresh-layerdb offheap search failed: {err:#}"),
                    }
                }
                RuntimeSpFreshIndex::OffHeapDiskMeta(_) => unreachable!("diskmeta handled above"),
            }
        };
        self.stats
            .record_search(started.elapsed().as_micros() as u64);
        out
    }

    fn len(&self) -> usize {
        match &*lock_read(&self.index) {
            RuntimeSpFreshIndex::Resident(index) => index.len(),
            RuntimeSpFreshIndex::OffHeap(index) => index.len(),
            RuntimeSpFreshIndex::OffHeapDiskMeta(index) => index.len(),
        }
    }
}
