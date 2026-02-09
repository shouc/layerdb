#[derive(Debug, Clone)]
pub struct DbOptions {
    pub memtable_shards: usize,
    pub wal_segment_bytes: u64,
    pub memtable_bytes: u64,
    pub fsync_writes: bool,

    /// Enable a secondary HDD tier rooted at `<db>/sst_hdd`.
    pub enable_hdd_tier: bool,

    /// Highest (hottest) level that should stay on NVMe.
    ///
    /// v1 only has levels 0 and 1.
    pub hot_levels_max: u8,

    /// Upper bound on L0 input bytes compacted per run when output tier is HDD.
    ///
    /// Helps bound HDD compaction work and foreground latency impact.
    /// `0` disables HDD-targeted compaction runs.
    pub hdd_compaction_budget_bytes: u64,

    /// Maximum files to move per `rebalance_tiers` run.
    ///
    /// Keeps HDD/NVMe mover work bounded so foreground requests stay smooth.
    /// `0` disables tier moves.
    pub tier_rebalance_max_moves: usize,

    /// Maximum number of SST readers to keep open.
    pub sst_reader_cache_entries: usize,

    /// Maximum number of data blocks to cache.
    ///
    /// Enabled for SST data-block cache lookups.
    pub block_cache_entries: usize,

    /// Maximum in-flight operations for the async IO executor.
    pub io_max_in_flight: usize,

    /// Use io-executor + buffer pool for SST reads instead of mmap.
    pub sst_use_io_executor_reads: bool,

    /// Use io-executor for SST writes (flush/compaction builders).
    pub sst_use_io_executor_writes: bool,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            memtable_shards: 16,
            wal_segment_bytes: 64 * 1024 * 1024,
            memtable_bytes: 64 * 1024 * 1024,
            fsync_writes: true,

            enable_hdd_tier: false,
            hot_levels_max: 2,
            hdd_compaction_budget_bytes: u64::MAX,
            tier_rebalance_max_moves: usize::MAX,
            sst_reader_cache_entries: 128,
            block_cache_entries: 0,
            io_max_in_flight: 256,
            sst_use_io_executor_reads: false,
            sst_use_io_executor_writes: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct WriteOptions {
    pub sync: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ReadOptions {
    pub snapshot: Option<crate::db::SnapshotId>,
}
