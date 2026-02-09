//! Internal key format and ordering.
//!
//! LayerDB orders entries by the tuple `(user_key ASC, seqno DESC, kind)`.
//! This ensures a merged iterator can decide visibility by selecting the first
//! entry for a user key that is visible at a snapshot.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("truncated input")]
    Truncated,

    #[error("unknown key kind: {0}")]
    UnknownKind(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum KeyKind {
    /// A tombstone for a point key.
    Del = 0,
    /// A point key/value.
    Put = 1,
    /// Placeholder for merge operands.
    Merge = 2,
    /// Placeholder for range tombstones.
    RangeDel = 3,
    /// Placeholder for internal metadata keys.
    Meta = 4,
}

impl KeyKind {
    pub fn is_range_tombstone(self) -> bool {
        matches!(self, Self::RangeDel)
    }
}

impl KeyKind {
    pub fn from_u8(value: u8) -> Result<Self, DecodeError> {
        match value {
            0 => Ok(Self::Del),
            1 => Ok(Self::Put),
            2 => Ok(Self::Merge),
            3 => Ok(Self::RangeDel),
            4 => Ok(Self::Meta),
            other => Err(DecodeError::UnknownKind(other)),
        }
    }

    pub fn is_tombstone(self) -> bool {
        matches!(self, Self::Del | Self::RangeDel)
    }
}

/// Internal key `(user_key, seqno, kind)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InternalKey {
    pub user_key: Bytes,
    pub seqno: u64,
    pub kind: KeyKind,
}

impl InternalKey {
    pub fn new(user_key: Bytes, seqno: u64, kind: KeyKind) -> Self {
        Self {
            user_key,
            seqno,
            kind,
        }
    }

    pub fn encoded_len(&self) -> usize {
        4 + self.user_key.len() + 8 + 1
    }

    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        buf
    }

    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        let user_key_len: u32 = self
            .user_key
            .len()
            .try_into()
            .expect("user_key too large to encode");
        buf.extend_from_slice(&user_key_len.to_le_bytes());
        buf.extend_from_slice(self.user_key.as_ref());
        buf.extend_from_slice(&self.seqno.to_le_bytes());
        buf.push(self.kind as u8);
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), DecodeError> {
        if input.len() < 4 {
            return Err(DecodeError::Truncated);
        }
        let user_key_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let needed = 4 + user_key_len + 8 + 1;
        if input.len() < needed {
            return Err(DecodeError::Truncated);
        }

        let user_key = Bytes::copy_from_slice(&input[4..(4 + user_key_len)]);
        let seqno_offset = 4 + user_key_len;
        let seqno = u64::from_le_bytes(input[seqno_offset..(seqno_offset + 8)].try_into().unwrap());
        let kind = KeyKind::from_u8(input[seqno_offset + 8])?;

        Ok((
            Self {
                user_key,
                seqno,
                kind,
            },
            needed,
        ))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.user_key.cmp(&other.user_key) {
            Ordering::Equal => match other.seqno.cmp(&self.seqno) {
                Ordering::Equal => (other.kind as u8).cmp(&(self.kind as u8)),
                other => other,
            },
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_key_ordering() {
        let k1 = InternalKey::new(Bytes::from_static(b"a"), 10, KeyKind::Put);
        let k2 = InternalKey::new(Bytes::from_static(b"a"), 9, KeyKind::Put);
        let k3 = InternalKey::new(Bytes::from_static(b"b"), 10, KeyKind::Put);

        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn encode_roundtrip() {
        let key = InternalKey::new(Bytes::from_static(b"hello"), 42, KeyKind::Del);
        let enc = key.encode_to_vec();
        let (dec, used) = InternalKey::decode(&enc).unwrap();
        assert_eq!(used, enc.len());
        assert_eq!(dec, key);
    }
}
