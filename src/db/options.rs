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

    /// Maximum number of SST readers to keep open.
    pub sst_reader_cache_entries: usize,

    /// Maximum number of data blocks to cache.
    ///
    /// Currently unused; reserved for milestone 5.
    pub block_cache_entries: usize,

    /// Maximum in-flight operations for the async IO executor.
    pub io_max_in_flight: usize,
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
            sst_reader_cache_entries: 128,
            block_cache_entries: 0,
            io_max_in_flight: 256,
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
