#[derive(Debug, Clone)]
pub struct DbOptions {
    pub memtable_shards: usize,
    pub wal_segment_bytes: u64,
    pub fsync_writes: bool,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            memtable_shards: 16,
            wal_segment_bytes: 64 * 1024 * 1024,
            fsync_writes: true,
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

