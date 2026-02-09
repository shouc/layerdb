use std::ops::Bound;

use crate::db::Value;

/// Public iterator over user keys.
///
/// Yield order: ascending user key.
pub struct DbIterator {
    merged: crate::memtable::MergedMemAndSstIter,
}

impl DbIterator {
    pub(crate) fn new(
        mem: crate::memtable::MemTableIter,
        sst: crate::version::SstIter,
        snapshot_seqno: u64,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            merged: crate::memtable::MergedMemAndSstIter::new(mem, sst, snapshot_seqno)?,
        })
    }

    pub fn seek_to_first(&mut self) {
        self.merged.seek_to_first();
    }

    pub fn seek(&mut self, key: impl AsRef<[u8]>) {
        self.merged.seek(key.as_ref());
    }

    pub fn next(&mut self) -> Option<anyhow::Result<(bytes::Bytes, Option<Value>)>> {
        self.merged.next()
    }
}

pub fn range_contains(
    range: &(Bound<bytes::Bytes>, Bound<bytes::Bytes>),
    key: &[u8],
) -> bool {
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

