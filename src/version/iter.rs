use std::path::PathBuf;

use bytes::Bytes;

use crate::db::iterator::range_contains;
use crate::db::{OpKind, Range};
use crate::internal_key::{InternalKey, KeyKind};
use crate::sst::SstReader;

#[derive(Debug, Clone)]
struct SstEntry {
    key: InternalKey,
    value: Bytes,
}

pub struct SstIter {
    entries: Vec<SstEntry>,
    index: usize,
    snapshot_seqno: u64,
}

impl SstIter {
    pub(crate) fn new(
        paths: Vec<PathBuf>,
        snapshot_seqno: u64,
        range: Range,
    ) -> anyhow::Result<Self> {
        let bounds = crate::memtable::bounds_from_range(&range);
        let mut entries = Vec::new();

        for path in paths {
            let reader = SstReader::open(&path)?;
            let mut iter = reader.iter(snapshot_seqno)?;
            iter.seek_to_first();
            for next in iter {
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
}

impl Iterator for SstIter {
    type Item = anyhow::Result<(Bytes, u64, OpKind, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
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
