use std::cmp::Ordering;
use std::ops::Bound;

use bytes::Bytes;

use crate::db::{OpKind, Range, Value};
use crate::internal_key::{InternalKey, KeyKind};

use super::InternalEntry;

pub struct MemTableIter {
    entries: Vec<InternalEntry>,
    index: usize,
    snapshot_seqno: u64,
}

impl MemTableIter {
    pub(super) fn new(entries: Vec<InternalEntry>, snapshot_seqno: u64) -> Self {
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
        let target = InternalKey::new(
            Bytes::copy_from_slice(key),
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
}

impl Iterator for MemTableIter {
    type Item = anyhow::Result<(Bytes, u64, OpKind, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.entries.get(self.index)?.clone();
        self.index += 1;
        let kind = match entry.key.kind {
            KeyKind::Put => OpKind::Put,
            KeyKind::Del => OpKind::Del,
            KeyKind::RangeDel => OpKind::RangeDel,
            other => {
                return Some(Err(anyhow::anyhow!(
                    "unexpected key kind in memtable iterator: {other:?}"
                )))
            }
        };
        Some(Ok((entry.key.user_key, entry.key.seqno, kind, entry.value)))
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
}

impl Iterator for MergedMemAndSstIter {
    type Item = anyhow::Result<(Bytes, u64, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
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

            let (user_key, seqno, kind, value) = match next {
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
                OpKind::Put => return Some(Ok((user_key, seqno, value))),
                OpKind::Del => continue,
                OpKind::RangeDel => {
                    // Range tombstones are handled by the DB-level iterator.
                    continue;
                }
            }
        }
    }
}

fn kind_rank(kind: OpKind) -> u8 {
    match kind {
        OpKind::Del => 0,
        OpKind::Put => 1,
        OpKind::RangeDel => 2,
    }
}

fn cmp_internal_tuple(
    a: &(Bytes, u64, OpKind, Bytes),
    b: &(Bytes, u64, OpKind, Bytes),
) -> Ordering {
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
