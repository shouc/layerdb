use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;

use crate::db::iterator::range_contains;
use crate::db::{Op, OpKind, Range, Value};
use crate::internal_key::{InternalKey, KeyKind};

#[derive(Debug)]
pub struct MemTableManager {
    shard_count: usize,
    mutable: RwLock<Arc<MemTable>>,
    immutables: Mutex<VecDeque<Arc<MemTable>>>,
}

#[derive(Debug)]
pub(crate) struct MemTable {
    pub(crate) wal_segment_id: u64,
    shards: Vec<MemTableShard>,
    approximate_bytes: AtomicU64,
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

    pub(crate) fn mutable_wal_segment_id(&self) -> u64 {
        self.mutable.read().wal_segment_id
    }

    pub(crate) fn rotate_memtable(&self, new_wal_segment_id: u64) -> Arc<MemTable> {
        let mut guard = self.mutable.write();
        let old = std::mem::replace(&mut *guard, Arc::new(MemTable::new(self.shard_count, new_wal_segment_id)));
        self.immutables.lock().push_front(old.clone());
        old
    }

    pub(crate) fn take_oldest_immutable(&self) -> Option<Arc<MemTable>> {
        self.immutables.lock().pop_back()
    }

    pub(crate) fn requeue_immutable_oldest(&self, mem: Arc<MemTable>) {
        self.immutables.lock().push_back(mem);
    }

    pub fn approximate_bytes(&self) -> u64 {
        let mutable = self.mutable.read();
        let immutable_bytes: u64 = self
            .immutables
            .lock()
            .iter()
            .map(|m| m.approximate_bytes())
            .sum();
        mutable.approximate_bytes() + immutable_bytes
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
    pub fn get(&self, key: &[u8], snapshot_seqno: u64) -> anyhow::Result<Option<Option<Value>>> {
        let mutable = self.mutable.read().clone();
        if let Some(v) = mutable.get(key, snapshot_seqno) {
            return Ok(Some(v));
        }

        for mem in self.immutables.lock().iter() {
            if let Some(v) = mem.get(key, snapshot_seqno) {
                return Ok(Some(v));
            }
        }

        Ok(None)
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
                .map(|_| MemTableShard { map: SkipMap::new() })
                .collect(),
            approximate_bytes: AtomicU64::new(0),
        }
    }

    pub(crate) fn approximate_bytes(&self) -> u64 {
        self.approximate_bytes.load(AtomicOrdering::Relaxed)
    }

    pub(crate) fn to_sorted_entries(&self) -> Vec<(InternalKey, Bytes)> {
        let per_shard: Vec<Vec<(InternalKey, Bytes)>> = self
            .shards
            .par_iter()
            .map(|shard| {
                shard
                    .map
                    .iter()
                    .map(|e| (e.key().clone(), e.value().clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        let mut out: Vec<(InternalKey, Bytes)> = per_shard.into_iter().flatten().collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn apply_batch(&self, shard_count: usize, seqno_base: u64, ops: &[Op]) {
        let mut per_shard: Vec<Vec<InternalEntry>> = (0..shard_count).map(|_| Vec::new()).collect();
        for (idx, op) in ops.iter().enumerate() {
            let seqno = seqno_base + idx as u64;
            let user_key = op.key.clone();
            let shard = shard_for_key(shard_count, user_key.as_ref());
            let (kind, value) = match op.kind {
                OpKind::Put => (KeyKind::Put, op.value.clone()),
                OpKind::Del => (KeyKind::Del, Bytes::new()),
            };

            per_shard[shard].push(InternalEntry {
                key: InternalKey::new(user_key, seqno, kind),
                value,
            });
        }

        let approx = &self.approximate_bytes;
        self.shards
            .par_iter()
            .enumerate()
            .for_each(|(shard_idx, shard)| {
                let local = &per_shard[shard_idx];
                for entry in local {
                    shard.map.insert(entry.key.clone(), entry.value.clone());
                    let bytes = entry.key.user_key.len() as u64 + entry.value.len() as u64 + 16;
                    approx.fetch_add(bytes, AtomicOrdering::Relaxed);
                }
            });
    }

    fn get(&self, user_key: &[u8], snapshot_seqno: u64) -> Option<Option<Value>> {
        let shard = shard_for_key(self.shards.len(), user_key);
        let start = InternalKey::new(Bytes::copy_from_slice(user_key), u64::MAX, KeyKind::Meta);
        let end = InternalKey::new(Bytes::copy_from_slice(user_key), 0, KeyKind::Del);
        for entry in self.shards[shard].map.range(start..=end) {
            let ikey = entry.key();
            if ikey.seqno > snapshot_seqno {
                continue;
            }
            return match ikey.kind {
                KeyKind::Put => Some(Some(entry.value().clone())),
                KeyKind::Del => Some(None),
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
}

pub struct MemTableIter {
    entries: Vec<InternalEntry>,
    index: usize,
    snapshot_seqno: u64,
}

impl MemTableIter {
    fn new(entries: Vec<InternalEntry>, snapshot_seqno: u64) -> Self {
        Self {
            entries,
            index: 0,
            snapshot_seqno,
        }
    }

    pub fn seek_to_first(&mut self) {
        self.index = 0;
    }

    pub fn seek(&mut self, key: &[u8]) {
        let target = InternalKey::new(Bytes::copy_from_slice(key), self.snapshot_seqno, KeyKind::Meta);
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
            other => {
                return Some(Err(anyhow::anyhow!(
                    "unexpected key kind in memtable iterator: {other:?}"
                )))
            }
        };
        Some(Ok((
            entry.key.user_key,
            entry.key.seqno,
            kind,
            entry.value,
        )))
    }
}

pub struct MergedMemAndSstIter {
    mem: MemTableIter,
    sst: crate::version::SstIter,
    next_mem: Option<(Bytes, u64, OpKind, Bytes)>,
    next_sst: Option<(Bytes, u64, OpKind, Bytes)>,
    skip_user_key: Option<Bytes>,
}

impl MergedMemAndSstIter {
    pub fn new(
        mem: MemTableIter,
        sst: crate::version::SstIter,
        _snapshot_seqno: u64,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            mem,
            sst,
            next_mem: None,
            next_sst: None,
            skip_user_key: None,
        })
    }

    pub fn seek_to_first(&mut self) {
        self.mem.seek_to_first();
        self.sst.seek_to_first();
        self.next_mem = None;
        self.next_sst = None;
        self.skip_user_key = None;
    }

    pub fn seek(&mut self, key: &[u8]) {
        self.mem.seek(key);
        self.sst.seek(key);
        self.next_mem = None;
        self.next_sst = None;
        self.skip_user_key = None;
    }

    pub fn next(&mut self) -> Option<anyhow::Result<(Bytes, Option<Value>)>> {
        loop {
            if self.next_mem.is_none() {
                match self.mem.next() {
                    Some(Ok(v)) => self.next_mem = Some(v),
                    Some(Err(e)) => return Some(Err(e)),
                    None => {}
                }
            }
            if self.next_sst.is_none() {
                if let Some(next) = self.sst.next() {
                    match next {
                        Ok(v) => self.next_sst = Some(v),
                        Err(e) => return Some(Err(e)),
                    }
                }
            }

            let next = match (&self.next_mem, &self.next_sst) {
                (None, None) => return None,
                (Some(_), None) => self.next_mem.take(),
                (None, Some(_)) => self.next_sst.take(),
                (Some(m), Some(s)) => {
                    if cmp_internal_tuple(m, s) != Ordering::Greater {
                        self.next_mem.take()
                    } else {
                        self.next_sst.take()
                    }
                }
            };

            let (user_key, _seqno, kind, value) = match next {
                Some(v) => v,
                None => continue,
            };

            if let Some(skip) = &self.skip_user_key {
                if skip.as_ref() == user_key.as_ref() {
                    continue;
                }
            }
            self.skip_user_key = Some(user_key.clone());

            match kind {
                OpKind::Put => return Some(Ok((user_key, Some(value)))),
                OpKind::Del => continue,
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

fn kind_rank(kind: OpKind) -> u8 {
    match kind {
        OpKind::Del => 0,
        OpKind::Put => 1,
    }
}

fn cmp_internal_tuple(a: &(Bytes, u64, OpKind, Bytes), b: &(Bytes, u64, OpKind, Bytes)) -> Ordering {
    match a.0.cmp(&b.0) {
        Ordering::Equal => match b.1.cmp(&a.1) {
            Ordering::Equal => kind_rank(b.2).cmp(&kind_rank(a.2)),
            other => other,
        },
        other => other,
    }
}

pub fn bounds_from_range(range: &Range) -> (Bound<Bytes>, Bound<Bytes>) {
    (range.start.clone(), range.end.clone())
}
