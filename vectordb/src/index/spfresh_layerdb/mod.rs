mod config;
mod rebuilder;
mod stats;
mod storage;
mod sync_utils;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Instant;

use anyhow::Context;
use bytes::Bytes;
use layerdb::{Db, DbOptions, ReadOptions, WriteOptions};
use serde::{Deserialize, Serialize};

use crate::types::{Neighbor, VectorIndex, VectorRecord};

use super::spfresh_diskmeta::SpFreshDiskMetaIndex;
use super::spfresh_offheap::SpFreshOffHeapIndex;
use super::{SpFreshConfig, SpFreshIndex};
use rebuilder::{rebuild_once, spawn_rebuilder, RebuilderRuntime};
use stats::SpFreshLayerDbStatsInner;
use storage::{
    decode_vector_row_value, encode_vector_row_value, encode_wal_entry, ensure_active_generation,
    ensure_metadata, ensure_wal_exists, ensure_wal_next_seq, load_index_checkpoint_bytes,
    load_metadata, load_posting_assignments, load_posting_members, load_row, load_rows,
    load_wal_entries_since, persist_index_checkpoint_bytes, posting_assignment_value,
    posting_map_key, posting_map_prefix, posting_member_key, posting_member_value_with_residual,
    posting_members_generation_prefix, prefix_exclusive_end, prune_wal_before,
    refresh_read_snapshot, set_active_generation, validate_config, vector_key, vector_prefix,
    wal_key, IndexWalEntry, PostingMember,
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
const DISKMETA_RERANK_FACTOR: usize = 8;

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
}

impl PostingMembersCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&self, generation: u64, posting_id: usize) -> Option<Arc<Vec<PostingMemberSketch>>> {
        self.map.get(&(generation, posting_id)).cloned()
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
    }

    fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }
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
    posting_members_cache: Arc<Mutex<PostingMembersCache>>,
    wal_next_seq: Arc<AtomicU64>,
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
        let (mut index_state, applied_wal_seq) =
            Self::load_or_rebuild_index(&db, &cfg, generation, wal_next_seq)?;
        cfg.memory_mode = match &index_state {
            RuntimeSpFreshIndex::Resident(_) => SpFreshMemoryMode::Resident,
            RuntimeSpFreshIndex::OffHeap(_) => SpFreshMemoryMode::OffHeap,
            RuntimeSpFreshIndex::OffHeapDiskMeta(_) => SpFreshMemoryMode::OffHeapDiskMeta,
        };
        let vector_cache = Arc::new(Mutex::new(VectorCache::new(cfg.offheap_cache_capacity)));
        let posting_members_cache = Arc::new(Mutex::new(PostingMembersCache::new(
            cfg.offheap_posting_cache_entries,
        )));
        let replay_from = applied_wal_seq.map_or(0, |seq| seq.saturating_add(1));
        if replay_from < wal_next_seq {
            if matches!(index_state, RuntimeSpFreshIndex::OffHeapDiskMeta(_)) {
                if let Err(err) = Self::replay_wal_tail_diskmeta(&db, &mut index_state, replay_from)
                {
                    eprintln!(
                        "spfresh-layerdb diskmeta wal replay failed, rebuilding from rows: {err:#}"
                    );
                    let rows = load_rows(&db, generation)?;
                    let assignments = load_posting_assignments(&db, generation)?;
                    let (rebuilt, _assigned_now) =
                        SpFreshDiskMetaIndex::build_from_rows_with_assignments(
                            cfg.spfresh.clone(),
                            &rows,
                            Some(&assignments),
                        );
                    index_state = RuntimeSpFreshIndex::OffHeapDiskMeta(rebuilt);
                }
            } else {
                Self::replay_wal_tail(
                    &db,
                    &vector_cache,
                    generation,
                    &mut index_state,
                    replay_from,
                )?;
            }
        }
        let index = Arc::new(RwLock::new(index_state));
        let active_generation = Arc::new(AtomicU64::new(generation));
        let update_gate = Arc::new(RwLock::new(()));
        let dirty_ids = Arc::new(Mutex::new(HashSet::new()));
        let pending_ops = Arc::new(AtomicUsize::new(0));
        let wal_next_seq = Arc::new(AtomicU64::new(wal_next_seq));
        let stop_worker = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(SpFreshLayerDbStatsInner::default());
        let (rebuild_tx, rebuild_rx) = mpsc::channel::<()>();

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
            posting_members_cache,
            wal_next_seq,
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

        let rows = load_rows(db, generation)?;
        let applied_wal_seq = wal_next_seq.checked_sub(1);
        let index = match cfg.memory_mode {
            SpFreshMemoryMode::Resident => {
                RuntimeSpFreshIndex::Resident(SpFreshIndex::build(cfg.spfresh.clone(), &rows))
            }
            SpFreshMemoryMode::OffHeap => {
                RuntimeSpFreshIndex::OffHeap(SpFreshOffHeapIndex::build(cfg.spfresh.clone(), &rows))
            }
            SpFreshMemoryMode::OffHeapDiskMeta => {
                let assignments = load_posting_assignments(db, generation)?;
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

    fn replay_wal_tail(
        db: &Db,
        vector_cache: &Arc<Mutex<VectorCache>>,
        generation: u64,
        index: &mut RuntimeSpFreshIndex,
        from_seq: u64,
    ) -> anyhow::Result<()> {
        #[derive(Debug)]
        enum ReplayAction {
            Upsert(Vec<f32>),
            Delete,
            LookupRowState,
        }

        let entries = load_wal_entries_since(db, from_seq)?;
        if entries.is_empty() {
            return Ok(());
        }

        let mut latest: HashMap<u64, ReplayAction> = HashMap::with_capacity(entries.len());
        for entry in entries {
            match entry {
                IndexWalEntry::Upsert { id, vector } => {
                    latest.insert(id, ReplayAction::Upsert(vector));
                }
                IndexWalEntry::Delete { id } => {
                    latest.insert(id, ReplayAction::Delete);
                }
                IndexWalEntry::IdOnly { id }
                | IndexWalEntry::DiskMetaUpsert { id, .. }
                | IndexWalEntry::DiskMetaDelete { id, .. } => {
                    latest.insert(id, ReplayAction::LookupRowState);
                }
            }
        }

        for (id, action) in latest {
            match action {
                ReplayAction::Upsert(values) => match index {
                    RuntimeSpFreshIndex::Resident(index) => index.upsert(id, values),
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader = Self::loader_for(
                            db,
                            vector_cache,
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
                ReplayAction::Delete => match index {
                    RuntimeSpFreshIndex::Resident(index) => {
                        let _ = index.delete(id);
                    }
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader = Self::loader_for(db, vector_cache, generation, None);
                        let _ = index.delete_with(id, &mut loader)?;
                    }
                    RuntimeSpFreshIndex::OffHeapDiskMeta(index) => {
                        let _ = index.apply_delete(None);
                    }
                },
                ReplayAction::LookupRowState => match load_row(db, generation, id)? {
                    Some(row) => match index {
                        RuntimeSpFreshIndex::Resident(index) => index.upsert(row.id, row.values),
                        RuntimeSpFreshIndex::OffHeap(index) => {
                            let mut loader = Self::loader_for(
                                db,
                                vector_cache,
                                generation,
                                Some((row.id, row.values.clone())),
                            );
                            index.upsert_with(row.id, row.values, &mut loader)?;
                        }
                        RuntimeSpFreshIndex::OffHeapDiskMeta(index) => {
                            let posting = index.choose_posting(&row.values).unwrap_or_default();
                            index.apply_upsert(None, posting, row.values);
                        }
                    },
                    None => match index {
                        RuntimeSpFreshIndex::Resident(index) => {
                            let _ = index.delete(id);
                        }
                        RuntimeSpFreshIndex::OffHeap(index) => {
                            let mut loader = Self::loader_for(db, vector_cache, generation, None);
                            let _ = index.delete_with(id, &mut loader)?;
                        }
                        RuntimeSpFreshIndex::OffHeapDiskMeta(index) => {
                            let _ = index.apply_delete(None);
                        }
                    },
                },
            }
        }
        Ok(())
    }

    fn replay_wal_tail_diskmeta(
        db: &Db,
        index: &mut RuntimeSpFreshIndex,
        from_seq: u64,
    ) -> anyhow::Result<()> {
        let RuntimeSpFreshIndex::OffHeapDiskMeta(index) = index else {
            anyhow::bail!("diskmeta wal replay called for non-diskmeta index");
        };
        let entries = load_wal_entries_since(db, from_seq)?;
        for entry in entries {
            match entry {
                IndexWalEntry::Upsert { .. } | IndexWalEntry::Delete { .. } => {
                    anyhow::bail!("non-diskmeta wal entry cannot be replayed for diskmeta")
                }
                IndexWalEntry::IdOnly { .. } => {
                    anyhow::bail!("legacy id-only wal entry cannot be replayed for diskmeta")
                }
                IndexWalEntry::DiskMetaUpsert {
                    old,
                    new_posting,
                    new_vector,
                    ..
                } => index.apply_upsert(old, new_posting, new_vector),
                IndexWalEntry::DiskMetaDelete { old, .. } => {
                    let _ = index.apply_delete(old);
                }
            }
        }
        Ok(())
    }

    fn persist_index_checkpoint(&self) -> anyhow::Result<()> {
        let next_wal_seq = self.wal_next_seq.load(Ordering::Relaxed);
        let snapshot = lock_read(&self.index).clone();
        let checkpoint = PersistedIndexCheckpoint {
            schema_version: config::META_INDEX_CHECKPOINT_SCHEMA_VERSION,
            generation: self.active_generation.load(Ordering::Relaxed),
            applied_wal_seq: next_wal_seq.checked_sub(1),
            index: snapshot,
        };
        let bytes = bincode::serialize(&checkpoint).context("encode spfresh index checkpoint")?;
        persist_index_checkpoint_bytes(&self.db, bytes, self.cfg.write_sync)?;
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
            stats: self.stats.clone(),
            stop_worker: self.stop_worker.clone(),
        }
    }

    fn load_vector_for_id(
        db: &Db,
        vector_cache: &Arc<Mutex<VectorCache>>,
        generation: u64,
        id: u64,
    ) -> anyhow::Result<Option<Vec<f32>>> {
        if let Some(values) = lock_mutex(vector_cache).get(id) {
            return Ok(Some(values));
        }
        let Some(row) = load_row(db, generation, id)? else {
            return Ok(None);
        };
        let values = row.values;
        lock_mutex(vector_cache).put(id, values.clone());
        Ok(Some(values))
    }

    fn load_vectors_for_ids(
        db: &Db,
        vector_cache: &Arc<Mutex<VectorCache>>,
        generation: u64,
        ids: &[u64],
    ) -> anyhow::Result<Vec<(u64, Vec<f32>)>> {
        let mut out = Vec::with_capacity(ids.len());
        let mut misses = Vec::new();
        {
            let cache = lock_mutex(vector_cache);
            for id in ids.iter().copied() {
                if let Some(values) = cache.get(id) {
                    out.push((id, values));
                } else {
                    misses.push(id);
                }
            }
        }
        if misses.is_empty() {
            return Ok(out);
        }

        let keys: Vec<Bytes> = misses
            .iter()
            .map(|id| Bytes::from(vector_key(generation, *id)))
            .collect();
        let rows = db
            .multi_get(&keys, ReadOptions::default())
            .context("diskmeta multi_get vector rows")?;
        let mut fetched = Vec::new();
        for (id, raw) in misses.into_iter().zip(rows.into_iter()) {
            let Some(raw) = raw else {
                continue;
            };
            let row = decode_vector_row_value(raw.as_ref())
                .with_context(|| format!("decode vector row id={id} generation={generation}"))?;
            if row.deleted {
                continue;
            }
            out.push((id, row.values.clone()));
            fetched.push((id, row.values));
        }
        if !fetched.is_empty() {
            let mut cache = lock_mutex(vector_cache);
            for (id, values) in fetched {
                cache.put(id, values);
            }
        }
        Ok(out)
    }

    fn loader_for<'a>(
        db: &'a Db,
        vector_cache: &'a Arc<Mutex<VectorCache>>,
        generation: u64,
        override_row: Option<(u64, Vec<f32>)>,
    ) -> impl FnMut(u64) -> anyhow::Result<Option<Vec<f32>>> + 'a {
        move |id| {
            if let Some((override_id, values)) = &override_row {
                if *override_id == id {
                    return Ok(Some(values.clone()));
                }
            }
            Self::load_vector_for_id(db, vector_cache, generation, id)
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
        let members = load_posting_members(&self.db, generation, posting_id)?;
        let mut sketches = Vec::with_capacity(members.len());
        {
            let mut vector_cache = lock_mutex(&self.vector_cache);
            for member in members {
                sketches.push(PostingMemberSketch::from_loaded(&member));
                if let Some(values) = member.values {
                    vector_cache.put(member.id, values);
                }
            }
        }
        let sketches = Arc::new(sketches);
        lock_mutex(&self.posting_members_cache).put_arc(generation, posting_id, sketches.clone());
        Ok(sketches)
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

    pub fn bulk_load(&mut self, rows: &[VectorRecord]) -> anyhow::Result<()> {
        self.try_bulk_load(rows)
    }

    pub fn try_bulk_load(&mut self, rows: &[VectorRecord]) -> anyhow::Result<()> {
        let _update_guard = lock_write(&self.update_gate);

        let old_generation = self.active_generation.load(Ordering::Relaxed);
        let new_generation = old_generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("active generation overflow"))?;
        let mut disk_assignments = None;
        let mut disk_index = None;
        if self.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta {
            let (index, assignments) =
                SpFreshDiskMetaIndex::build_with_assignments(self.cfg.spfresh.clone(), rows);
            disk_assignments = Some(assignments);
            disk_index = Some(index);
        }

        for batch in rows.chunks(1_024) {
            let mut ops = Vec::with_capacity(batch.len().saturating_mul(3));
            for row in batch {
                let value = encode_vector_row_value(row)
                    .with_context(|| format!("serialize vector row id={}", row.id))?;
                ops.push(layerdb::Op::put(vector_key(new_generation, row.id), value));
                if let Some(assignments) = &disk_assignments {
                    let posting = assignments.get(&row.id).copied().ok_or_else(|| {
                        anyhow::anyhow!("missing diskmeta assignment for id={}", row.id)
                    })?;
                    let centroid = disk_index
                        .as_ref()
                        .and_then(|idx| idx.posting_centroid(posting))
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "missing centroid for posting {} during diskmeta bulk-load",
                                posting
                            )
                        })?;
                    ops.push(layerdb::Op::put(
                        posting_map_key(new_generation, row.id),
                        posting_assignment_value(posting)?,
                    ));
                    ops.push(layerdb::Op::put(
                        posting_member_key(new_generation, posting, row.id),
                        posting_member_value_with_residual(row.id, &row.values, centroid)?,
                    ));
                }
            }
            self.db
                .write_batch(
                    ops,
                    WriteOptions {
                        sync: self.cfg.write_sync,
                    },
                )
                .context("persist spfresh bulk rows")?;
        }
        set_active_generation(&self.db, new_generation, self.cfg.write_sync)?;
        self.active_generation
            .store(new_generation, Ordering::Relaxed);

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
        {
            let mut cache = lock_mutex(&self.vector_cache);
            cache.map.clear();
            cache.order.clear();
        }
        lock_mutex(&self.posting_members_cache).clear();
        lock_mutex(&self.dirty_ids).clear();
        self.pending_ops.store(0, Ordering::Relaxed);
        self.stats.set_last_rebuild_rows(rows.len());
        self.persist_index_checkpoint()?;
        Ok(())
    }

    pub fn force_rebuild(&self) -> anyhow::Result<()> {
        let runtime = self.runtime();
        rebuild_once(&runtime)
    }

    fn persist_with_wal_batch_ops(
        &self,
        entries: Vec<(IndexWalEntry, Vec<layerdb::Op>)>,
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
        self.db
            .write_batch(
                ops,
                WriteOptions {
                    sync: self.cfg.write_sync,
                },
            )
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

        let posting_keys: Vec<Bytes> = ids
            .iter()
            .map(|id| Bytes::from(posting_map_key(generation, *id)))
            .collect();
        let row_keys: Vec<Bytes> = ids
            .iter()
            .map(|id| Bytes::from(vector_key(generation, *id)))
            .collect();
        let posting_values = self
            .db
            .multi_get(&posting_keys, ReadOptions::default())
            .context("diskmeta multi_get posting assignments")?;
        let row_values = self
            .db
            .multi_get(&row_keys, ReadOptions::default())
            .context("diskmeta multi_get vector rows")?;

        for ((id, posting_raw), row_raw) in ids
            .iter()
            .copied()
            .zip(posting_values.into_iter())
            .zip(row_values.into_iter())
        {
            let posting = match posting_raw {
                Some(raw) => {
                    let pid_u64: u64 = bincode::deserialize(raw.as_ref())
                        .with_context(|| format!("decode posting assignment id={id}"))?;
                    Some(usize::try_from(pid_u64).context("posting id does not fit usize")?)
                }
                None => None,
            };
            let row = match row_raw {
                Some(raw) => Some(decode_vector_row_value(raw.as_ref()).with_context(|| {
                    format!("decode vector row id={id} generation={generation}")
                })?),
                None => None,
            };
            let state = match (posting, row) {
                (Some(posting), Some(row)) if !row.deleted => Some((posting, row.values)),
                _ => None,
            };
            out.insert(id, state);
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
        if mutations.is_empty() {
            return Ok(0);
        }

        let _update_guard = lock_read(&self.update_gate);
        let generation = self.active_generation.load(Ordering::Relaxed);
        let persist_started = Instant::now();

        if self.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta {
            let ids: Vec<u64> = mutations.iter().map(|(id, _)| *id).collect();
            let mut states = self.load_diskmeta_states_for_ids(generation, &ids)?;
            let mut shadow = match &*lock_read(&self.index) {
                RuntimeSpFreshIndex::OffHeapDiskMeta(index) => index.clone(),
                _ => anyhow::bail!("diskmeta upsert batch called for non-diskmeta index"),
            };

            let mut persist_entries = Vec::with_capacity(mutations.len());
            let mut apply_entries = Vec::with_capacity(mutations.len());
            let mut cache_deltas = Vec::with_capacity(mutations.len());
            for (id, vector) in &mutations {
                let old = states.get(id).cloned().unwrap_or(None);
                let new_posting = shadow.choose_posting(vector).unwrap_or(0);
                let centroid = shadow
                    .posting_centroid(new_posting)
                    .unwrap_or(vector.as_slice());
                let sketch = Self::residual_sketch_for_member(*id, vector, centroid);
                let row = VectorRecord::new(*id, vector.clone());
                let value = encode_vector_row_value(&row).context("serialize vector row")?;
                let mut ops = vec![
                    layerdb::Op::put(vector_key(generation, *id), value),
                    layerdb::Op::put(
                        posting_map_key(generation, *id),
                        posting_assignment_value(new_posting)?,
                    ),
                    layerdb::Op::put(
                        posting_member_key(generation, new_posting, *id),
                        posting_member_value_with_residual(*id, vector, centroid)?,
                    ),
                ];
                if let Some((old_posting, _)) = &old {
                    if *old_posting != new_posting {
                        ops.push(layerdb::Op::delete(posting_member_key(
                            generation,
                            *old_posting,
                            *id,
                        )));
                    }
                }
                persist_entries.push((
                    IndexWalEntry::DiskMetaUpsert {
                        id: *id,
                        old: old.clone(),
                        new_posting,
                        new_vector: vector.clone(),
                    },
                    ops,
                ));
                apply_entries.push((*id, old.clone(), new_posting, vector.clone()));
                cache_deltas.push((*id, old.as_ref().map(|(posting, _)| *posting), new_posting, sketch));
                shadow.apply_upsert(old, new_posting, vector.clone());
                states.insert(*id, Some((new_posting, vector.clone())));
            }

            if let Err(err) = self.persist_with_wal_batch_ops(persist_entries) {
                self.stats
                    .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
                self.stats.inc_persist_errors();
                return Err(err);
            }
            self.stats
                .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);

            {
                let mut index = lock_write(&self.index);
                let RuntimeSpFreshIndex::OffHeapDiskMeta(index) = &mut *index else {
                    anyhow::bail!("diskmeta upsert batch called for non-diskmeta runtime");
                };
                for (_id, old, new_posting, vector) in apply_entries {
                    index.apply_upsert(old, new_posting, vector);
                }
            }
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

        let mut persist_entries = Vec::with_capacity(mutations.len());
        for (id, vector) in &mutations {
            let row = VectorRecord::new(*id, vector.clone());
            let value = encode_vector_row_value(&row).context("serialize vector row")?;
            persist_entries.push((
                IndexWalEntry::Upsert {
                    id: *id,
                    vector: vector.clone(),
                },
                vec![layerdb::Op::put(vector_key(generation, *id), value)],
            ));
        }
        if let Err(err) = self.persist_with_wal_batch_ops(persist_entries) {
            self.stats
                .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);
            self.stats.inc_persist_errors();
            return Err(err);
        }
        self.stats
            .add_persist_upsert_us(persist_started.elapsed().as_micros() as u64);

        {
            let mut index = lock_write(&self.index);
            for (id, vector) in &mutations {
                match &mut *index {
                    RuntimeSpFreshIndex::Resident(index) => index.upsert(*id, vector.clone()),
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader = Self::loader_for(
                            &self.db,
                            &self.vector_cache,
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

        let _update_guard = lock_read(&self.update_gate);
        let generation = self.active_generation.load(Ordering::Relaxed);
        let persist_started = Instant::now();

        if self.cfg.memory_mode == SpFreshMemoryMode::OffHeapDiskMeta {
            let states = self.load_diskmeta_states_for_ids(generation, &mutations)?;
            let mut persist_entries = Vec::with_capacity(mutations.len());
            let mut apply_entries = Vec::with_capacity(mutations.len());
            for id in &mutations {
                let old = states.get(id).cloned().unwrap_or(None);
                let mut ops = vec![
                    layerdb::Op::delete(vector_key(generation, *id)),
                    layerdb::Op::delete(posting_map_key(generation, *id)),
                ];
                if let Some((old_posting, _)) = &old {
                    ops.push(layerdb::Op::delete(posting_member_key(
                        generation,
                        *old_posting,
                        *id,
                    )));
                }
                persist_entries.push((
                    IndexWalEntry::DiskMetaDelete {
                        id: *id,
                        old: old.clone(),
                    },
                    ops,
                ));
                apply_entries.push((*id, old));
            }
            if let Err(err) = self.persist_with_wal_batch_ops(persist_entries) {
                self.stats
                    .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
                self.stats.inc_persist_errors();
                return Err(err);
            }
            self.stats
                .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);

            let mut deleted = 0usize;
            {
                let mut index = lock_write(&self.index);
                let RuntimeSpFreshIndex::OffHeapDiskMeta(index) = &mut *index else {
                    anyhow::bail!("diskmeta delete batch called for non-diskmeta runtime");
                };
                for (_id, old) in apply_entries {
                    if index.apply_delete(old) {
                        deleted += 1;
                    }
                }
            }
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

        let mut persist_entries = Vec::with_capacity(mutations.len());
        for id in &mutations {
            persist_entries.push((
                IndexWalEntry::Delete { id: *id },
                vec![layerdb::Op::delete(vector_key(generation, *id))],
            ));
        }
        if let Err(err) = self.persist_with_wal_batch_ops(persist_entries) {
            self.stats
                .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);
            self.stats.inc_persist_errors();
            return Err(err);
        }
        self.stats
            .add_persist_delete_us(persist_started.elapsed().as_micros() as u64);

        let mut deleted = 0usize;
        {
            let mut index = lock_write(&self.index);
            for id in &mutations {
                let was_deleted = match &mut *index {
                    RuntimeSpFreshIndex::Resident(index) => index.delete(*id),
                    RuntimeSpFreshIndex::OffHeap(index) => {
                        let mut loader =
                            Self::loader_for(&self.db, &self.vector_cache, generation, None);
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
        self.force_rebuild()?;
        self.persist_index_checkpoint()?;
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
        let diskmeta_snapshot = {
            let guard = lock_read(&self.index);
            match &*guard {
                RuntimeSpFreshIndex::OffHeapDiskMeta(index) => Some(index.clone()),
                _ => None,
            }
        };
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
                let candidate_ids =
                    Self::select_diskmeta_candidates(query, centroid, members.as_ref(), k);
                if candidate_ids.is_empty() {
                    continue;
                }
                let vectors =
                    Self::load_vectors_for_ids(&self.db, &self.vector_cache, generation, &candidate_ids)
                        .unwrap_or_else(|err| panic!("offheap-diskmeta load vectors failed: {err:#}"));
                for (id, values) in vectors {
                    Self::push_neighbor_topk(
                        &mut top,
                        Neighbor {
                            id,
                            distance: crate::linalg::squared_l2(query, &values),
                        },
                        k,
                    );
                }
            }
            top.sort_by(Self::neighbor_cmp);
            top
        } else {
            match &*lock_read(&self.index) {
                RuntimeSpFreshIndex::Resident(index) => index.search(query, k),
                RuntimeSpFreshIndex::OffHeap(index) => {
                    let mut loader = Self::loader_for(&self.db, &self.vector_cache, generation, None);
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
