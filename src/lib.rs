//! `layerdb` is a log-structured merge-tree (LSM) based key-value store.
//!
//! This crate is intentionally opinionated about correctness first:
//! - Internal keys are ordered by `(user_key ASC, seqno DESC, kind)`.
//! - Reads are snapshot-safe (consistent reads at a sequence number).
//! - Deletes are tombstones.
//! - WAL + manifest follow a strict fsync/rename discipline.
//!
//! The implementation is milestone-driven and currently includes:
//! - local WAL/memtable/flush/compaction with snapshot-safe reads,
//! - range tombstones + conservative dropping,
//! - async IO executor abstractions,
//! - NVMe/HDD tier routing,
//! - S3 frozen-level lifecycle (freeze/thaw/gc + read-through cache),
//! - integrity hashing with foreground + background scrub support,
//! - optional branch heads persisted in manifest.

pub mod cache;
pub mod compaction;
pub mod db;
pub mod integrity;
pub mod internal_key;
pub mod io;
pub mod memtable;
pub mod range_tombstone;
pub mod sst;
pub mod tier;
pub mod version;
pub mod wal;

pub use db::{BackgroundScrubber, BackgroundScrubberState, ScrubReport};
pub use db::{Db, DbMetrics, DbOptions, ReadOptions, SnapshotId, WriteOptions};
pub use db::{Op, Range, Value};
