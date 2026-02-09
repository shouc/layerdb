use std::ops::Bound;

use crate::db::Value;
use crate::range_tombstone::RangeTombstone;

/// Public iterator over user keys.
///
/// Yield order: ascending user key.
pub struct DbIterator {
    merged: crate::memtable::MergedMemAndSstIter,
    range_tombstones: Vec<RangeTombstone>,
    snapshot_seqno: u64,
}

impl DbIterator {
    pub(crate) fn new(
        mem: crate::memtable::MemTableIter,
        sst: crate::version::SstIter,
        snapshot_seqno: u64,
        mut range_tombstones: Vec<RangeTombstone>,
    ) -> anyhow::Result<Self> {
        range_tombstones.sort_by(|a, b| b.seqno.cmp(&a.seqno));
        Ok(Self {
            merged: crate::memtable::MergedMemAndSstIter::new(mem, sst, snapshot_seqno)?,
            range_tombstones,
            snapshot_seqno,
        })
    }

    pub fn seek_to_first(&mut self) {
        self.merged.seek_to_first();
    }

    pub fn seek(&mut self, key: impl AsRef<[u8]>) {
        self.merged.seek(key.as_ref());
    }

    pub fn next(&mut self) -> Option<anyhow::Result<(bytes::Bytes, Option<Value>)>> {
        loop {
            let (key, seqno, value) = match self.merged.next()? {
                Ok(v) => v,
                Err(e) => return Some(Err(e)),
            };
            if is_deleted_by_range_tombstone(
                &self.range_tombstones,
                key.as_ref(),
                seqno,
                self.snapshot_seqno,
            ) {
                continue;
            }
            return Some(Ok((key, Some(value))));
        }
    }
}

fn is_deleted_by_range_tombstone(
    tombstones: &[RangeTombstone],
    key: &[u8],
    key_seqno: u64,
    snapshot_seqno: u64,
) -> bool {
    tombstones.iter().any(|t| {
        t.seqno <= snapshot_seqno
            && t.seqno >= key_seqno
            && t.start_key.as_ref() <= key
            && key < t.end_key.as_ref()
    })
}

pub fn range_contains(range: &(Bound<bytes::Bytes>, Bound<bytes::Bytes>), key: &[u8]) -> bool {
    let start_ok = match &range.0 {
        Bound::Unbounded => true,
        Bound::Included(k) => key >= k.as_ref(),
        Bound::Excluded(k) => key > k.as_ref(),
    };
    let end_ok = match &range.1 {
        Bound::Unbounded => true,
        Bound::Included(k) => key <= k.as_ref(),
        Bound::Excluded(k) => key < k.as_ref(),
    };
    start_ok && end_ok
}
