mod commit_pipeline;
mod config;
mod lifecycle;
mod mutations;
mod open_recovery;
mod rebuilder;
mod runtime_helpers;
mod stats;
mod storage;
mod sync_utils;
mod vector_blocks;

#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Instant;

use anyhow::Context;
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use layerdb::{Db, DbOptions, ReadOptions, WriteOptions};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

use crate::linalg::squared_l2;
use crate::types::{Neighbor, VectorIndex, VectorRecord};

use super::spfresh_diskmeta::SpFreshDiskMetaIndex;
use super::spfresh_offheap::SpFreshOffHeapIndex;
use super::{SpFreshConfig, SpFreshIndex};
use rebuilder::{rebuild_once, spawn_rebuilder, RebuilderRuntime};
use stats::SpFreshLayerDbStatsInner;
use storage::{
    decode_vector_row_value, decode_vector_row_with_posting, encode_posting_members_snapshot,
    encode_u64_fixed, encode_vector_row_fields, encode_vector_row_value,
    encode_vector_row_value_with_posting, encode_wal_diskmeta_delete_batch,
    encode_wal_diskmeta_upsert_batch, encode_wal_vector_delete_batch,
    encode_wal_vector_upsert_batch, ensure_active_generation, ensure_metadata,
    ensure_posting_event_next_seq, ensure_wal_exists, ensure_wal_next_seq,
    load_index_checkpoint_bytes, load_metadata, load_posting_members, load_row, load_rows,
    load_rows_with_posting_assignments, load_startup_manifest_bytes,
    persist_index_checkpoint_bytes, persist_startup_manifest_bytes, posting_map_prefix,
    posting_member_event_key, posting_member_event_tombstone_value,
    posting_member_event_upsert_value_id_only, posting_members_generation_prefix,
    posting_members_prefix, posting_members_snapshot_key, prefix_exclusive_end, prune_wal_before,
    refresh_read_snapshot, set_active_generation, set_posting_event_next_seq, validate_config,
    vector_key, vector_prefix, visit_wal_entries_since, wal_key, IndexWalEntry, PostingMember,
};
use sync_utils::{lock_mutex, lock_read, lock_write};
use vector_blocks::VectorBlockStore;

pub use config::{SpFreshLayerDbConfig, SpFreshMemoryMode};
pub use stats::SpFreshLayerDbStats;

#[derive(Clone, Debug, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize)]
pub enum VectorMutation {
    Upsert(VectorRecord),
    Delete { id: u64 },
}

impl VectorMutation {
    pub(crate) fn id(&self) -> u64 {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationCommitMode {
    Durable,
    Acknowledged,
}

type DiskMetaRowState = Option<(usize, Vec<f32>)>;
type DiskMetaStateMap = FxHashMap<u64, DiskMetaRowState>;
type EphemeralPostingMembers = FxHashMap<usize, FxHashSet<u64>>;
type EphemeralRowStates = FxHashMap<u64, (usize, Vec<f32>)>;
type DistanceRow = (u64, f32);
type DistanceLoadResult = (Vec<DistanceRow>, Vec<u64>);
const POSTING_LOG_COMPACT_MIN_EVENTS: usize = 512;
const POSTING_LOG_COMPACT_FACTOR: usize = 3;
const ASYNC_COMMIT_MAX_INFLIGHT: usize = 8;

#[derive(Clone, Debug, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize)]
enum RuntimeSpFreshIndex {
    Resident(SpFreshIndex),
    OffHeap(SpFreshOffHeapIndex),
    OffHeapDiskMeta(SpFreshDiskMetaIndex),
}

#[derive(Clone, Debug, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize)]
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
    map: FxHashMap<u64, Vec<f32>>,
    order: VecDeque<u64>,
}

impl VectorCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: FxHashMap::default(),
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
        if let Some(existing) = self.map.get_mut(&id) {
            *existing = values;
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

#[derive(Debug)]
struct PostingMembersCache {
    capacity: usize,
    map: FxHashMap<(u64, usize), Arc<Vec<u64>>>,
    order: VecDeque<(u64, usize)>,
}

impl PostingMembersCache {
    fn new(capacity: usize, _compact_interval_ops: usize, _compact_budget_entries: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: FxHashMap::default(),
            order: VecDeque::new(),
        }
    }

    fn get(&self, generation: u64, posting_id: usize) -> Option<Arc<Vec<u64>>> {
        self.map.get(&(generation, posting_id)).cloned()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn put_arc(&mut self, generation: u64, posting_id: usize, members: Arc<Vec<u64>>) {
        if self.capacity == 0 {
            return;
        }
        if let Some(existing) = self.map.get_mut(&(generation, posting_id)) {
            *existing = members;
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

    fn invalidate(&mut self, generation: u64, posting_id: usize) {
        self.map.remove(&(generation, posting_id));
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
    dirty_ids: Arc<Mutex<FxHashSet<u64>>>,
    pending_ops: Arc<AtomicUsize>,
    vector_cache: Arc<Mutex<VectorCache>>,
    vector_blocks: Arc<Mutex<VectorBlockStore>>,
    posting_members_cache: Arc<Mutex<PostingMembersCache>>,
    ephemeral_posting_members: Arc<Mutex<Option<EphemeralPostingMembers>>>,
    ephemeral_row_states: Arc<Mutex<Option<EphemeralRowStates>>>,
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
