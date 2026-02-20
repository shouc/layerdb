use std::time::Duration;

use layerdb::DbOptions;
use serde::{Deserialize, Serialize};

use crate::index::SpFreshConfig;

pub(crate) const VECTOR_ROOT_PREFIX: &str = "spfresh/v/";
pub(crate) const POSTING_MAP_ROOT_PREFIX: &str = "spfresh/m/";
pub(crate) const POSTING_MEMBERS_ROOT_PREFIX: &str = "spfresh/p/";
pub(crate) const META_CONFIG_KEY: &str = "spfresh/meta/config";
pub(crate) const META_ACTIVE_GENERATION_KEY: &str = "spfresh/meta/active_generation";
pub(crate) const META_INDEX_CHECKPOINT_KEY: &str = "spfresh/meta/index_checkpoint";
pub(crate) const META_STARTUP_MANIFEST_KEY: &str = "spfresh/meta/startup_manifest";
pub(crate) const META_INDEX_WAL_NEXT_SEQ_KEY: &str = "spfresh/meta/index_wal_next_seq";
pub(crate) const META_POSTING_EVENT_NEXT_SEQ_KEY: &str = "spfresh/meta/posting_event_next_seq";
pub(crate) const INDEX_WAL_PREFIX: &str = "spfresh/wal/";
pub(crate) const META_SCHEMA_VERSION: u32 = 1;
pub(crate) const META_INDEX_CHECKPOINT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SpFreshPersistedMeta {
    pub schema_version: u32,
    pub dim: usize,
    pub initial_postings: usize,
    pub split_limit: usize,
    pub merge_limit: usize,
    pub reassign_range: usize,
    pub nprobe: usize,
    pub kmeans_iters: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpFreshMemoryMode {
    Resident,
    OffHeap,
    OffHeapDiskMeta,
}

#[derive(Clone, Debug)]
pub struct SpFreshLayerDbConfig {
    pub spfresh: SpFreshConfig,
    pub db_options: DbOptions,
    pub write_sync: bool,
    pub rebuild_pending_ops: usize,
    pub rebuild_interval: Duration,
    pub memory_mode: SpFreshMemoryMode,
    pub offheap_cache_capacity: usize,
    pub offheap_posting_cache_entries: usize,
    pub posting_delta_compact_interval_ops: usize,
    pub posting_delta_compact_budget_entries: usize,
}

impl Default for SpFreshLayerDbConfig {
    fn default() -> Self {
        Self {
            spfresh: SpFreshConfig::default(),
            db_options: DbOptions {
                fsync_writes: true,
                ..Default::default()
            },
            write_sync: true,
            rebuild_pending_ops: 2_000,
            rebuild_interval: Duration::from_millis(500),
            memory_mode: SpFreshMemoryMode::Resident,
            offheap_cache_capacity: 131_072,
            offheap_posting_cache_entries: 2_048,
            posting_delta_compact_interval_ops: 4_096,
            posting_delta_compact_budget_entries: 4,
        }
    }
}
