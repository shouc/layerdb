use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeTombstone {
    pub start_key: Bytes,
    pub end_key: Bytes,
    pub seqno: u64,
}

impl RangeTombstone {
    pub fn new(start_key: Bytes, end_key: Bytes, seqno: u64) -> Self {
        Self {
            start_key,
            end_key,
            seqno,
        }
    }

    pub fn covers(&self, key: &[u8], snapshot_seqno: u64) -> bool {
        self.seqno <= snapshot_seqno
            && self.start_key.as_ref() <= key
            && key < self.end_key.as_ref()
    }

    pub fn is_valid(&self) -> bool {
        self.start_key.as_ref() < self.end_key.as_ref()
    }
}
