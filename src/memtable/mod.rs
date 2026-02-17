use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;

use crate::db::iterator::range_contains;
use crate::db::{LookupResult, Op, OpKind, Range, Value};
use crate::internal_key::{InternalKey, KeyKind};
use crate::range_tombstone::RangeTombstone;

mod iter;

pub use iter::{bounds_from_range, MemTableIter, MergedMemAndSstIter};

#[derive(Debug)]
pub(crate) struct MemTableManager {
    shard_count: usize,
    mutable: RwLock<Arc<MemTable>>,
    immutables: Mutex<VecDeque<Arc<MemTable>>>,
}

#[derive(Debug)]
pub(crate) struct MemTable {
    pub(crate) wal_segment_id: u64,
    shards: Vec<MemTableShard>,
    approximate_bytes: AtomicU64,
    range_tombstone_count: AtomicU64,
}

#[derive(Debug)]
struct MemTableShard {
    map: SkipMap<InternalKey, Bytes>,
}

#[derive(Debug, Clone)]
struct InternalEntry {
    key: InternalKey,
    value: Bytes,
}

impl MemTableManager {
    pub fn new(shards: usize) -> Self {
        Self::with_wal_segment_id(shards, 1)
    }

    pub(crate) fn with_wal_segment_id(shards: usize, wal_segment_id: u64) -> Self {
        let shard_count = shards.max(1);
        Self {
            shard_count,
            mutable: RwLock::new(Arc::new(MemTable::new(shard_count, wal_segment_id))),
            immutables: Mutex::new(VecDeque::new()),
        }
    }

    pub(crate) fn reset_for_wal_recovery(&self, wal_segment_id: u64) {
        *self.mutable.write() = Arc::new(MemTable::new(self.shard_count, wal_segment_id));
        self.immutables.lock().clear();
    }

    pub(crate) fn rotate_memtable(&self, new_wal_segment_id: u64) -> Arc<MemTable> {
        let mut guard = self.mutable.write();
        let old = std::mem::replace(
            &mut *guard,
            Arc::new(MemTable::new(self.shard_count, new_wal_segment_id)),
        );
        self.immutables.lock().push_front(old.clone());
        old
    }

    pub(crate) fn oldest_immutable(&self) -> Option<Arc<MemTable>> {
        self.immutables.lock().back().cloned()
    }

    pub(crate) fn drop_oldest_immutable_if_segment_id(&self, wal_segment_id: u64) -> bool {
        let mut guard = self.immutables.lock();
        match guard.back() {
            Some(mem) if mem.wal_segment_id == wal_segment_id => {
                guard.pop_back();
                true
            }
            _ => false,
        }
    }

    pub(crate) fn mutable_approximate_bytes(&self) -> u64 {
        self.mutable.read().approximate_bytes()
    }

    pub fn apply_batch(&self, seqno_base: u64, ops: &[Op]) -> anyhow::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        let table = self.mutable.read().clone();
        table.apply_batch(self.shard_count, seqno_base, ops);
        Ok(())
    }

    /// Returns:
    /// - `Ok(None)` if key not present
    /// - `Ok(Some(None))` if tombstone present
    /// - `Ok(Some(Some(value)))` if value present
    pub fn get(&self, key: &[u8], snapshot_seqno: u64) -> anyhow::Result<Option<LookupResult>> {
        let mut candidate: Option<(u64, Option<Value>)> = None;

        let mutable = self.mutable.read().clone();
        let mut has_range_tombstones = mutable.has_range_tombstones();
        if let Some((seqno, v)) = mutable.get(key, snapshot_seqno) {
            candidate = Some((seqno, v));
        }

        for mem in self.immutables.lock().iter() {
            has_range_tombstones |= mem.has_range_tombstones();
            if let Some((seqno, v)) = mem.get(key, snapshot_seqno) {
                match &candidate {
                    Some((best_seq, _)) if *best_seq >= seqno => {}
                    _ => candidate = Some((seqno, v)),
                }
            }
        }

        let tombstone_seq = if has_range_tombstones {
            self.range_tombstones(snapshot_seqno)
                .iter()
                .filter(|t| t.start_key.as_ref() <= key && key < t.end_key.as_ref())
                .map(|t| t.seqno)
                .max()
        } else {
            None
        };

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

    pub fn range_tombstones(&self, snapshot_seqno: u64) -> Vec<RangeTombstone> {
        let mut out = Vec::new();

        let mutable = self.mutable.read().clone();
        mutable.collect_range_tombstones(snapshot_seqno, &mut out);

        for mem in self.immutables.lock().iter() {
            mem.collect_range_tombstones(snapshot_seqno, &mut out);
        }

        out.sort_by(|a, b| b.seqno.cmp(&a.seqno));
        out
    }

    pub fn iter(&self, range: Range, snapshot_seqno: u64) -> anyhow::Result<MemTableIter> {
        let bounds = bounds_from_range(&range);
        let mut entries = Vec::new();

        let mutable = self.mutable.read().clone();
        mutable.collect_range(&bounds, snapshot_seqno, &mut entries);

        for mem in self.immutables.lock().iter() {
            mem.collect_range(&bounds, snapshot_seqno, &mut entries);
        }

        entries.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(MemTableIter::new(entries, snapshot_seqno))
    }
}

impl MemTable {
    fn new(shard_count: usize, wal_segment_id: u64) -> Self {
        Self {
            wal_segment_id,
            shards: (0..shard_count)
                .map(|_| MemTableShard {
                    map: SkipMap::new(),
                })
                .collect(),
            approximate_bytes: AtomicU64::new(0),
            range_tombstone_count: AtomicU64::new(0),
        }
    }

    pub(crate) fn approximate_bytes(&self) -> u64 {
        self.approximate_bytes.load(AtomicOrdering::Relaxed)
    }

    fn has_range_tombstones(&self) -> bool {
        self.range_tombstone_count.load(AtomicOrdering::Relaxed) > 0
    }

    pub(crate) fn to_sorted_entries(&self) -> Vec<(InternalKey, Bytes)> {
        let mut out: Vec<(InternalKey, Bytes)> = Vec::new();
        for shard in &self.shards {
            for entry in shard.map.iter() {
                out.push((entry.key().clone(), entry.value().clone()));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn apply_batch(&self, shard_count: usize, seqno_base: u64, ops: &[Op]) {
        // Fast path: small foreground batches are latency-sensitive.
        // Avoid Rayon scheduling overhead and insert directly.
        if ops.len() <= 64 {
            for (idx, op) in ops.iter().enumerate() {
                let seqno = seqno_base + idx as u64;
                let user_key = op.key.clone();
                let shard = shard_for_key(shard_count, user_key.as_ref());
                let (kind, value) = match op.kind {
                    OpKind::Put => (KeyKind::Put, op.value.clone()),
                    OpKind::Del => (KeyKind::Del, Bytes::new()),
                    OpKind::RangeDel => (KeyKind::RangeDel, op.value.clone()),
                };
                let entry = InternalEntry {
                    key: InternalKey::new(user_key, seqno, kind),
                    value,
                };
                if matches!(entry.key.kind, KeyKind::RangeDel) {
                    self.range_tombstone_count
                        .fetch_add(1, AtomicOrdering::Relaxed);
                }
                self.shards[shard]
                    .map
                    .insert(entry.key.clone(), entry.value.clone());
                let bytes = entry.key.user_key.len() as u64 + entry.value.len() as u64 + 16;
                self.approximate_bytes
                    .fetch_add(bytes, AtomicOrdering::Relaxed);
            }
            return;
        }

        let mut per_shard: Vec<Vec<InternalEntry>> = (0..shard_count).map(|_| Vec::new()).collect();
        for (idx, op) in ops.iter().enumerate() {
            let seqno = seqno_base + idx as u64;
            let user_key = op.key.clone();
            let shard = shard_for_key(shard_count, user_key.as_ref());
            let (kind, value) = match op.kind {
                OpKind::Put => (KeyKind::Put, op.value.clone()),
                OpKind::Del => (KeyKind::Del, Bytes::new()),
                OpKind::RangeDel => (KeyKind::RangeDel, op.value.clone()),
            };

            per_shard[shard].push(InternalEntry {
                key: InternalKey::new(user_key, seqno, kind),
                value,
            });
        }

        let approx = &self.approximate_bytes;
        let range_tombstone_count = &self.range_tombstone_count;
        self.shards
            .par_iter()
            .enumerate()
            .for_each(|(shard_idx, shard)| {
                let local = &per_shard[shard_idx];
                for entry in local {
                    shard.map.insert(entry.key.clone(), entry.value.clone());
                    if matches!(entry.key.kind, KeyKind::RangeDel) {
                        range_tombstone_count.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    let bytes = entry.key.user_key.len() as u64 + entry.value.len() as u64 + 16;
                    approx.fetch_add(bytes, AtomicOrdering::Relaxed);
                }
            });
    }

    fn get(&self, user_key: &[u8], snapshot_seqno: u64) -> Option<(u64, Option<Value>)> {
        let shard = shard_for_key(self.shards.len(), user_key);
        let start = InternalKey::new(Bytes::copy_from_slice(user_key), u64::MAX, KeyKind::Meta);
        let end = InternalKey::new(Bytes::copy_from_slice(user_key), 0, KeyKind::Del);
        for entry in self.shards[shard].map.range(start..=end) {
            let ikey = entry.key();
            if ikey.seqno > snapshot_seqno {
                continue;
            }
            return match ikey.kind {
                KeyKind::Put => Some((ikey.seqno, Some(entry.value().clone()))),
                KeyKind::Del => Some((ikey.seqno, None)),
                _ => continue,
            };
        }
        None
    }

    fn collect_range(
        &self,
        bounds: &(Bound<Bytes>, Bound<Bytes>),
        snapshot_seqno: u64,
        out: &mut Vec<InternalEntry>,
    ) {
        let per_shard: Vec<Vec<InternalEntry>> = self
            .shards
            .par_iter()
            .map(|shard| {
                let mut local = Vec::new();
                for entry in shard.map.iter() {
                    let ikey = entry.key();
                    if ikey.seqno > snapshot_seqno {
                        continue;
                    }
                    if !range_contains(bounds, ikey.user_key.as_ref()) {
                        continue;
                    }
                    match ikey.kind {
                        KeyKind::Put | KeyKind::Del => {}
                        _ => continue,
                    }
                    local.push(InternalEntry {
                        key: ikey.clone(),
                        value: entry.value().clone(),
                    });
                }
                local
            })
            .collect();
        out.extend(per_shard.into_iter().flatten());
    }

    fn collect_range_tombstones(&self, snapshot_seqno: u64, out: &mut Vec<RangeTombstone>) {
        for shard in &self.shards {
            for entry in shard.map.iter() {
                let ikey = entry.key();
                if ikey.seqno > snapshot_seqno {
                    continue;
                }
                if ikey.kind != KeyKind::RangeDel {
                    continue;
                }
                out.push(RangeTombstone {
                    start_key: ikey.user_key.clone(),
                    end_key: entry.value().clone(),
                    seqno: ikey.seqno,
                });
            }
        }
    }
}

fn shard_for_key(shard_count: usize, user_key: &[u8]) -> usize {
    use std::hash::Hasher;
    let mut hasher = ahash::AHasher::default();
    hasher.write(user_key);
    (hasher.finish() as usize) % shard_count
}
