use std::sync::Arc;

use bytes::Bytes;

use crate::db::OpKind;
use crate::internal_key::{InternalKey, KeyKind};

use super::{SstError, SstReader};

pub struct SstIter<'a> {
    reader: &'a SstReader,
    snapshot_seqno: u64,
    index_pos: usize,
    seek_target: Option<InternalKey>,
    entries: Arc<Vec<(InternalKey, Bytes)>>,
    entry_pos: usize,
}

impl<'a> SstIter<'a> {
    pub(super) fn new(reader: &'a SstReader, snapshot_seqno: u64) -> Self {
        Self {
            reader,
            snapshot_seqno,
            index_pos: 0,
            seek_target: None,
            entries: Arc::new(Vec::new()),
            entry_pos: 0,
        }
    }

    pub fn seek_to_first(&mut self) {
        self.index_pos = 0;
        self.seek_target = None;
        self.entries = Arc::new(Vec::new());
        self.entry_pos = 0;
    }

    pub fn seek(&mut self, user_key: &[u8]) {
        let target = InternalKey::new(Bytes::copy_from_slice(user_key), u64::MAX, KeyKind::Meta);
        self.index_pos = {
            let mut lo = 0usize;
            let mut hi = self.reader.index.len();
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if self.reader.index[mid].last_key < target {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            lo
        };
        self.seek_target = Some(target);
        self.entries = Arc::new(Vec::new());
        self.entry_pos = 0;
    }
}

impl Iterator for SstIter<'_> {
    type Item = Result<(Bytes, u64, OpKind, Bytes), SstError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.entry_pos >= self.entries.len() {
                if self.index_pos >= self.reader.index.len() {
                    return None;
                }
                let block_no: u32 = self.index_pos.try_into().expect("sst block index fits u32");
                let handle = self.reader.index[self.index_pos].handle;
                self.index_pos += 1;
                match self.reader.read_block(block_no, handle) {
                    Ok(entries) => {
                        self.entries = entries;
                        self.entry_pos = 0;
                        if let Some(target) = self.seek_target.take() {
                            self.entry_pos =
                                match self.entries.binary_search_by(|(k, _)| k.cmp(&target)) {
                                    Ok(i) | Err(i) => i,
                                };
                        }
                    }
                    Err(e) => return Some(Err(e)),
                }
                continue;
            }

            let (ikey, value) = &self.entries[self.entry_pos];
            self.entry_pos += 1;
            if ikey.seqno > self.snapshot_seqno {
                continue;
            }
            let kind = match ikey.kind {
                KeyKind::Put => OpKind::Put,
                KeyKind::Del => OpKind::Del,
                KeyKind::RangeDel => OpKind::RangeDel,
                _ => continue,
            };
            return Some(Ok((ikey.user_key.clone(), ikey.seqno, kind, value.clone())));
        }
    }
}
