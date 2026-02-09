use std::ops::Bound;

use crate::db::{Op, OpKind, Range, Value};

/// v1 placeholder: this module will evolve into a sharded in-memory map.
///
/// Public API contract we need:
/// - point lookup at a snapshot seqno
/// - iterator over user keys in a range at a snapshot seqno
/// - apply batches (puts/deletes) with assigned seqnos

#[derive(Debug)]
pub struct MemTableManager {
    shards: usize,
}

impl MemTableManager {
    pub fn new(shards: usize) -> Self {
        Self {
            shards: shards.max(1),
        }
    }

    pub fn apply_batch(&self, _seqno_base: u64, _ops: &[Op]) -> anyhow::Result<()> {
        Ok(())
    }

    /// Returns:
    /// - `Ok(None)` if key not present
    /// - `Ok(Some(None))` if tombstone present
    /// - `Ok(Some(Some(value)))` if value present
    pub fn get(&self, _key: &[u8], _snapshot_seqno: u64) -> anyhow::Result<Option<Option<Value>>> {
        Ok(None)
    }

    pub fn iter(&self, _range: Range, _snapshot_seqno: u64) -> anyhow::Result<MemTableIter> {
        Ok(MemTableIter {})
    }
}

pub struct MemTableIter {}

impl MemTableIter {
    pub fn seek_to_first(&mut self) {}

    pub fn seek(&mut self, _key: &[u8]) {}

    pub fn next(&mut self) -> Option<anyhow::Result<(bytes::Bytes, u64, OpKind, bytes::Bytes)>> {
        None
    }
}

pub struct MergedMemAndSstIter {
    _snapshot_seqno: u64,
}

impl MergedMemAndSstIter {
    pub fn new(
        _mem: MemTableIter,
        _sst: crate::version::SstIter,
        snapshot_seqno: u64,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            _snapshot_seqno: snapshot_seqno,
        })
    }

    pub fn seek_to_first(&mut self) {}

    pub fn seek(&mut self, _key: &[u8]) {}

    pub fn next(&mut self) -> Option<anyhow::Result<(bytes::Bytes, Option<Value>)>> {
        None
    }
}

pub fn bounds_from_range(range: &Range) -> (Bound<bytes::Bytes>, Bound<bytes::Bytes>) {
    (range.start.clone(), range.end.clone())
}

