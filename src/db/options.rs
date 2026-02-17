#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Options {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
    pub secure: bool,
    pub retry_max_attempts: u32,
    pub retry_base_delay_ms: u64,
    pub auto_create_bucket: bool,
}

impl S3Options {
    pub fn from_env() -> Option<Self> {
        let endpoint = std::env::var("LAYERDB_S3_ENDPOINT").ok()?;
        let bucket = std::env::var("LAYERDB_S3_BUCKET").ok()?;
        let access_key = std::env::var("LAYERDB_S3_ACCESS_KEY").ok()?;
        let secret_key = std::env::var("LAYERDB_S3_SECRET_KEY").ok()?;

        let region = std::env::var("LAYERDB_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let prefix = std::env::var("LAYERDB_S3_PREFIX").unwrap_or_else(|_| "layerdb".to_string());
        let secure = std::env::var("LAYERDB_S3_SECURE")
            .ok()
            .map(|v| parse_bool_env(&v))
            .unwrap_or(false);
        let retry_max_attempts = std::env::var("LAYERDB_S3_RETRY_MAX_ATTEMPTS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(5);
        let retry_base_delay_ms = std::env::var("LAYERDB_S3_RETRY_BASE_DELAY_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(100);
        let auto_create_bucket = std::env::var("LAYERDB_S3_AUTO_CREATE_BUCKET")
            .ok()
            .map(|v| parse_bool_env(&v))
            .unwrap_or(true);

        Some(Self {
            endpoint,
            region,
            bucket,
            prefix,
            access_key,
            secret_key,
            secure,
            retry_max_attempts,
            retry_base_delay_ms,
            auto_create_bucket,
        })
    }
}

fn parse_bool_env(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

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

    /// Select which IO backend the executor should use.
    ///
    /// - `Tokio` uses async tokio file APIs.
    /// - `Blocking` uses std file APIs.
    /// - `Uring` attempts to use the `io-uring` crate (Linux only).
    ///
    /// Default prefers `Uring`; runtime will gracefully fall back when native
    /// io_uring is unavailable.
    pub io_backend: crate::io::IoBackend,

    /// Use io-executor + buffer pool for SST reads instead of mmap.
    pub sst_use_io_executor_reads: bool,

    /// Use io-executor for SST writes (flush/compaction builders).
    pub sst_use_io_executor_writes: bool,

    /// Object storage settings used by frozen-level S3 operations.
    ///
    /// If unset, tier-freeze/thaw uses local filesystem emulation under `sst_s3/`.
    pub s3: Option<S3Options>,
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
            io_backend: crate::io::IoBackend::Uring,
            sst_use_io_executor_reads: cfg!(target_os = "linux"),
            sst_use_io_executor_writes: cfg!(target_os = "linux"),
            s3: None,
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
