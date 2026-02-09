//! `layerdb` is a log-structured merge-tree (LSM) based key-value store.
//!
//! This crate is intentionally opinionated about correctness first:
//! - Internal keys are ordered by `(user_key ASC, seqno DESC, kind)`.
//! - Reads are snapshot-safe (consistent reads at a sequence number).
//! - Deletes are tombstones.
//! - WAL + manifest follow a strict fsync/rename discipline.
//!
//! The implementation is being built in milestones. Milestone 1 targets a
//! single local filesystem tier with WAL + memtable + flush to L0 SST, manifest
//! recovery, snapshots, iter, and L0->L1 compaction.

pub mod cache;
pub mod compaction;
pub mod db;
pub mod internal_key;
pub mod integrity;
pub mod io;
pub mod memtable;
pub mod sst;
pub mod tier;
pub mod version;
pub mod wal;

pub use db::{Db, DbOptions, ReadOptions, SnapshotId, WriteOptions};
pub use db::{Op, Range, Value};
