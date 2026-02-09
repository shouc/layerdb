//! Integrity and verification helpers.
//!
//! LayerDB uses:
//! - `crc32c` for quick corruption detection.
//! - `blake3` hashes for stronger integrity.
//!
//! The SST format currently stores both for each data block.
//! This module centralizes the hash functions so other parts of the system can
//! evolve without duplicating logic.

use bytes::Bytes;

use crate::sst::TableRoot;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockCrc32c(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHash(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableRootHash(pub [u8; 32]);

#[derive(Debug, Clone)]
pub struct RecordHasher;

impl RecordHasher {
    pub fn crc32c(data: &[u8]) -> BlockCrc32c {
        BlockCrc32c(crc32c::crc32c(data))
    }

    pub fn blake3(data: &[u8]) -> BlockHash {
        BlockHash(*blake3::hash(data).as_bytes())
    }

    pub fn verify_crc32c(data: &[u8], expected: BlockCrc32c) -> bool {
        Self::crc32c(data) == expected
    }

    pub fn verify_blake3(data: &[u8], expected: BlockHash) -> bool {
        Self::blake3(data) == expected
    }
}

#[derive(Debug, Clone)]
pub struct TableRootBuilder {
    hasher: blake3::Hasher,
}

impl Default for TableRootBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TableRootBuilder {
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
        }
    }

    pub fn update_block_hash(&mut self, hash: BlockHash) {
        self.hasher.update(&hash.0);
    }

    pub fn update_bytes(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    pub fn finalize(self) -> TableRoot {
        TableRoot(*self.hasher.finalize().as_bytes())
    }
}

pub fn decode_table_root(bytes: Bytes) -> anyhow::Result<TableRoot> {
    if bytes.len() != 32 {
        anyhow::bail!("expected table root length 32, got {}", bytes.len());
    }
    let mut root = [0u8; 32];
    root.copy_from_slice(&bytes);
    Ok(TableRoot(root))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_helpers_round_trip() {
        let data = b"hello";
        let crc = RecordHasher::crc32c(data);
        let hash = RecordHasher::blake3(data);
        assert!(RecordHasher::verify_crc32c(data, crc));
        assert!(RecordHasher::verify_blake3(data, hash));
    }

    #[test]
    fn table_root_builder_accumulates() {
        let mut builder = TableRootBuilder::new();
        builder.update_block_hash(RecordHasher::blake3(b"a"));
        builder.update_block_hash(RecordHasher::blake3(b"b"));
        let root1 = builder.finalize();

        let mut builder2 = TableRootBuilder::new();
        builder2.update_bytes(RecordHasher::blake3(b"a").0.as_ref());
        builder2.update_bytes(RecordHasher::blake3(b"b").0.as_ref());
        let root2 = builder2.finalize();

        assert_eq!(root1, root2);
    }
}
