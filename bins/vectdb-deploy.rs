use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::Context;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use etcd_client::{
    Client as EtcdClient, Compare, CompareOp, ConnectOptions, PutOptions, Txn, TxnOp,
};
use futures::future::BoxFuture;
use futures::stream::{self, FuturesUnordered, StreamExt};
use reqwest::header::CONTENT_TYPE;
use reqwest::Client;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Infallible, Serialize as RkyvSerialize};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use vectdb::cluster::etcd_leader_endpoint_key;
use vectdb::index::{
    MutationCommitMode, SpFreshConfig, SpFreshLayerDbConfig, SpFreshLayerDbShardedConfig,
    SpFreshLayerDbShardedIndex, SpFreshLayerDbShardedStats, SpFreshMemoryMode, VectorMutation,
    VectorMutationBatchResult,
};
use vectdb::topology::ClusterTopology;
use vectdb::types::{Neighbor, VectorIndex, VectorRecord};

#[derive(Debug, Parser)]
#[command(name = "vectdb-deploy")]
#[command(about = "Partitioned and replicated VectDB deployment server")]
struct Args {
    #[arg(long)]
    db_root: PathBuf,

    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: String,

    #[arg(long)]
    node_id: String,

    #[arg(long)]
    self_url: String,

    #[arg(long, value_delimiter = ',', num_args = 0..)]
    replica_urls: Vec<String>,

    #[arg(long, value_delimiter = ',', default_value = "http://127.0.0.1:2379")]
    etcd_endpoints: Vec<String>,

    #[arg(long, default_value = "/vectdb/deploy/leader")]
    etcd_election_key: String,

    #[arg(long, default_value_t = 10)]
    etcd_lease_ttl_secs: i64,

    #[arg(long, default_value_t = 500)]
    etcd_retry_ms: u64,

    #[arg(long, default_value_t = 1_500)]
    etcd_connect_timeout_ms: u64,

    #[arg(long, default_value_t = 2_000)]
    etcd_request_timeout_ms: u64,

    #[arg(long)]
    etcd_username: Option<String>,

    #[arg(long)]
    etcd_password: Option<String>,

    #[arg(long)]
    internal_auth_token: Option<String>,

    #[arg(long, default_value_t = 1_500)]
    replication_timeout_ms: u64,

    #[arg(long, default_value_t = 120_000)]
    replication_snapshot_timeout_ms: u64,

    #[arg(long, default_value_t = 500_000)]
    replication_log_max_entries: usize,

    #[arg(long, default_value_t = 512)]
    replication_catchup_batch: usize,

    #[arg(long, default_value_t = 16)]
    logical_partitions: usize,

    #[arg(long, default_value_t = 0)]
    replication_factor: usize,

    #[arg(long, default_value_t = 4)]
    shards: usize,

    #[arg(long, default_value_t = 64)]
    dim: usize,

    #[arg(long, default_value_t = 64)]
    initial_postings: usize,

    #[arg(long, default_value_t = 512)]
    split_limit: usize,

    #[arg(long, default_value_t = 64)]
    merge_limit: usize,

    #[arg(long, default_value_t = 64)]
    reassign_range: usize,

    #[arg(long, default_value_t = 8)]
    nprobe: usize,
    #[arg(long, default_value_t = 1)]
    diskmeta_probe_multiplier: usize,

    #[arg(long, default_value_t = 8)]
    kmeans_iters: usize,

    #[arg(long, value_enum, default_value_t = MemoryModeArg::Resident)]
    memory_mode: MemoryModeArg,

    #[arg(long, default_value_t = 2_000)]
    rebuild_pending_ops: usize,

    #[arg(long, default_value_t = 500)]
    rebuild_interval_ms: u64,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum MemoryModeArg {
    Resident,
    Offheap,
    Diskmeta,
}

impl From<MemoryModeArg> for SpFreshMemoryMode {
    fn from(value: MemoryModeArg) -> Self {
        match value {
            MemoryModeArg::Resident => SpFreshMemoryMode::Resident,
            MemoryModeArg::Offheap => SpFreshMemoryMode::OffHeap,
            MemoryModeArg::Diskmeta => SpFreshMemoryMode::OffHeapDiskMeta,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CommitModeArg {
    Durable,
    Acknowledged,
}

impl From<CommitModeArg> for MutationCommitMode {
    fn from(value: CommitModeArg) -> Self {
        match value {
            CommitModeArg::Durable => MutationCommitMode::Durable,
            CommitModeArg::Acknowledged => MutationCommitMode::Acknowledged,
        }
    }
}

impl From<MutationCommitMode> for CommitModeArg {
    fn from(value: MutationCommitMode) -> Self {
        match value {
            MutationCommitMode::Durable => CommitModeArg::Durable,
            MutationCommitMode::Acknowledged => CommitModeArg::Acknowledged,
        }
    }
}

const REPLICATION_JOURNAL_SCHEMA_VERSION: u32 = 2;
const REPLICATION_PAYLOAD_SCHEMA_VERSION: u32 = 1;
const SNAPSHOT_PAYLOAD_SCHEMA_VERSION: u32 = 1;
const INTERNAL_PARTITION_MUTATIONS_SCHEMA_VERSION: u32 = 1;
const INTERNAL_PARTITION_SEARCH_SCHEMA_VERSION: u32 = 1;
const CLUSTER_TOPOLOGY_SCHEMA_VERSION: u32 = 1;
const LOG_RANGE_FRAME_MAX_ENTRIES: usize = 4096;
const REPLICATION_LOG_RECORD_HEADER_BYTES: u64 = 20;
const INTERNAL_AUTH_HEADER: &str = "x-vectdb-internal-token";

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
enum WireMutation {
    Upsert { id: u64, values: Vec<f32> },
    Delete { id: u64 },
}

impl WireMutation {
    fn into_runtime(self) -> VectorMutation {
        match self {
            Self::Upsert { id, values } => VectorMutation::Upsert(VectorRecord::new(id, values)),
            Self::Delete { id } => VectorMutation::Delete { id },
        }
    }

    fn id(&self) -> u64 {
        match self {
            Self::Upsert { id, .. } | Self::Delete { id } => *id,
        }
    }
}

impl From<MutationInput> for WireMutation {
    fn from(value: MutationInput) -> Self {
        match value {
            MutationInput::Upsert { id, values } => Self::Upsert { id, values },
            MutationInput::Delete { id } => Self::Delete { id },
        }
    }
}

impl From<&MutationInput> for WireMutation {
    fn from(value: &MutationInput) -> Self {
        match value {
            MutationInput::Upsert { id, values } => Self::Upsert {
                id: *id,
                values: values.clone(),
            },
            MutationInput::Delete { id } => Self::Delete { id: *id },
        }
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
struct ReplicationPayload {
    schema_version: u32,
    partition_id: u32,
    seq: u64,
    term: u64,
    commit_mode: u8,
    mutations: Vec<WireMutation>,
}

impl ReplicationPayload {
    fn into_runtime_mutations(self) -> Vec<VectorMutation> {
        self.mutations
            .into_iter()
            .map(WireMutation::into_runtime)
            .collect()
    }

    fn commit_mode(&self) -> anyhow::Result<MutationCommitMode> {
        decode_commit_mode(self.commit_mode)
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
struct WireSnapshotRow {
    id: u64,
    values: Vec<f32>,
}

impl From<VectorRecord> for WireSnapshotRow {
    fn from(value: VectorRecord) -> Self {
        Self {
            id: value.id,
            values: value.values,
        }
    }
}

impl WireSnapshotRow {
    fn into_runtime(self) -> VectorRecord {
        VectorRecord::new(self.id, self.values)
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
struct SnapshotPayload {
    schema_version: u32,
    partition_id: u32,
    applied_seq: u64,
    term: u64,
    rows: Vec<WireSnapshotRow>,
}

impl SnapshotPayload {
    fn into_runtime_rows(self) -> Vec<VectorRecord> {
        self.rows
            .into_iter()
            .map(WireSnapshotRow::into_runtime)
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplicationRejectResponse {
    error: String,
    expected_seq: u64,
    first_available_seq: u64,
    last_available_seq: u64,
    max_term_seen: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct LogRangeRequest {
    partition_id: u32,
    from_seq: u64,
    max_entries: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ReplicationState {
    first_seq: u64,
    next_seq: u64,
    last_applied_seq: u64,
    max_term_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplicationJournalMeta {
    schema_version: u32,
    first_seq: u64,
    next_seq: u64,
    last_applied_seq: u64,
    max_term_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClusterTopologyEnvelope {
    schema_version: u32,
    topology: ClusterTopology,
}

#[derive(Debug, Clone, Copy)]
struct JournalIndexEntry {
    offset: u64,
    len: u32,
}

struct ReplicationJournal {
    dir: PathBuf,
    log_path: PathBuf,
    meta_path: PathBuf,
    file: File,
    index: BTreeMap<u64, JournalIndexEntry>,
    meta: ReplicationJournalMeta,
    max_entries: usize,
}

impl ReplicationJournal {
    fn open(root: &Path, max_entries: usize) -> anyhow::Result<Self> {
        if max_entries == 0 {
            anyhow::bail!("replication log max entries must be > 0");
        }
        let dir = root.join("replication");
        fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        let log_path = dir.join("ops.log");
        let meta_path = dir.join("meta.json");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&log_path)
            .with_context(|| format!("open {}", log_path.display()))?;

        let mut index = BTreeMap::new();
        let mut offset = 0u64;
        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    return Err(err).with_context(|| format!("read {}", log_path.display()))
                }
            }
            let len = u32::from_le_bytes(len_buf);
            let mut seq_buf = [0u8; 8];
            file.read_exact(&mut seq_buf)
                .with_context(|| format!("read {} seq header", log_path.display()))?;
            let seq = u64::from_le_bytes(seq_buf);
            let mut checksum_buf = [0u8; 8];
            file.read_exact(&mut checksum_buf)
                .with_context(|| format!("read {} checksum header", log_path.display()))?;
            let checksum = u64::from_le_bytes(checksum_buf);
            let mut raw = vec![0u8; len as usize];
            file.read_exact(&mut raw)
                .with_context(|| format!("read {} payload len={}", log_path.display(), len))?;
            let actual_checksum = checksum_replication_record(seq, raw.as_slice());
            if checksum != actual_checksum {
                anyhow::bail!(
                    "replication log checksum mismatch at seq={} got={} expected={}",
                    seq,
                    checksum,
                    actual_checksum
                );
            }
            index.insert(
                seq,
                JournalIndexEntry {
                    offset: offset + REPLICATION_LOG_RECORD_HEADER_BYTES,
                    len,
                },
            );
            offset = offset.saturating_add(REPLICATION_LOG_RECORD_HEADER_BYTES + len as u64);
        }
        file.seek(SeekFrom::End(0))
            .context("seek replication log to end")?;

        let mut meta = if meta_path.exists() {
            let raw =
                fs::read(&meta_path).with_context(|| format!("read {}", meta_path.display()))?;
            let loaded: ReplicationJournalMeta =
                serde_json::from_slice(&raw).context("decode replication meta json")?;
            if loaded.schema_version != REPLICATION_JOURNAL_SCHEMA_VERSION {
                anyhow::bail!(
                    "replication meta schema mismatch: got {}, expected {}",
                    loaded.schema_version,
                    REPLICATION_JOURNAL_SCHEMA_VERSION
                );
            }
            loaded
        } else {
            ReplicationJournalMeta {
                schema_version: REPLICATION_JOURNAL_SCHEMA_VERSION,
                first_seq: 1,
                next_seq: 1,
                last_applied_seq: 0,
                max_term_seen: 0,
            }
        };

        if let Some((&min_seq, _)) = index.first_key_value() {
            let max_seq = index
                .last_key_value()
                .map(|(seq, _)| *seq)
                .unwrap_or(min_seq);
            meta.first_seq = min_seq;
            // The on-disk log is authoritative after restart; never advance next_seq past it.
            meta.next_seq = max_seq.saturating_add(1);
            if meta.last_applied_seq.saturating_add(1) < min_seq {
                meta.last_applied_seq = min_seq.saturating_sub(1);
            }
            if meta.last_applied_seq > max_seq {
                meta.last_applied_seq = max_seq;
            }
        } else {
            meta.first_seq = meta.next_seq.max(1);
            meta.next_seq = meta
                .next_seq
                .max(meta.last_applied_seq.saturating_add(1))
                .max(1);
        }

        let mut out = Self {
            dir,
            log_path,
            meta_path,
            file,
            index,
            meta,
            max_entries,
        };
        out.persist_meta(true)?;
        out.compact_if_needed()?;
        Ok(out)
    }

    fn snapshot_state(&self) -> ReplicationState {
        ReplicationState {
            first_seq: self.first_seq(),
            next_seq: self.next_seq(),
            last_applied_seq: self.last_applied_seq(),
            max_term_seen: self.max_term_seen(),
        }
    }

    fn first_seq(&self) -> u64 {
        self.index
            .first_key_value()
            .map(|(seq, _)| *seq)
            .unwrap_or(self.meta.next_seq.max(1))
    }

    fn next_seq(&self) -> u64 {
        self.meta.next_seq
    }

    fn last_seq(&self) -> u64 {
        self.meta.next_seq.saturating_sub(1)
    }

    fn last_applied_seq(&self) -> u64 {
        self.meta.last_applied_seq
    }

    fn expected_next_seq(&self) -> u64 {
        self.meta.last_applied_seq.saturating_add(1)
    }

    fn max_term_seen(&self) -> u64 {
        self.meta.max_term_seen
    }

    #[cfg(test)]
    fn append_entry(&mut self, seq: u64, raw_payload: &[u8], sync: bool) -> anyhow::Result<()> {
        if seq != self.meta.next_seq {
            anyhow::bail!(
                "invalid replication seq: got {}, expected {}",
                seq,
                self.meta.next_seq
            );
        }
        let len = u32::try_from(raw_payload.len()).context("payload too large for log entry")?;
        let record_start = self
            .file
            .seek(SeekFrom::End(0))
            .context("seek replication log to append")?;
        let record_checksum = checksum_replication_record(seq, raw_payload);
        self.file
            .write_all(&len.to_le_bytes())
            .context("append replication record length")?;
        self.file
            .write_all(&seq.to_le_bytes())
            .context("append replication record seq")?;
        self.file
            .write_all(&record_checksum.to_le_bytes())
            .context("append replication record checksum")?;
        self.file
            .write_all(raw_payload)
            .context("append replication record payload")?;
        if sync {
            self.file.sync_data().context("sync replication record")?;
        }
        self.index.insert(
            seq,
            JournalIndexEntry {
                offset: record_start + REPLICATION_LOG_RECORD_HEADER_BYTES,
                len,
            },
        );
        self.meta.next_seq = seq.saturating_add(1);
        self.meta.first_seq = self.first_seq();
        self.persist_meta(sync)?;
        self.compact_if_needed()?;
        Ok(())
    }

    #[cfg(test)]
    fn mark_applied(&mut self, seq: u64, term: u64, sync: bool) -> anyhow::Result<()> {
        if seq < self.meta.last_applied_seq {
            return Ok(());
        }
        self.meta.last_applied_seq = seq;
        if term > self.meta.max_term_seen {
            self.meta.max_term_seen = term;
        }
        self.persist_meta(sync)
    }

    fn append_and_mark_applied(
        &mut self,
        seq: u64,
        term: u64,
        raw_payload: &[u8],
        sync: bool,
    ) -> anyhow::Result<()> {
        if seq != self.meta.next_seq {
            anyhow::bail!(
                "invalid replication seq: got {}, expected {}",
                seq,
                self.meta.next_seq
            );
        }
        let len = u32::try_from(raw_payload.len()).context("payload too large for log entry")?;
        let record_start = self
            .file
            .seek(SeekFrom::End(0))
            .context("seek replication log to append")?;
        let record_checksum = checksum_replication_record(seq, raw_payload);
        self.file
            .write_all(&len.to_le_bytes())
            .context("append replication record length")?;
        self.file
            .write_all(&seq.to_le_bytes())
            .context("append replication record seq")?;
        self.file
            .write_all(&record_checksum.to_le_bytes())
            .context("append replication record checksum")?;
        self.file
            .write_all(raw_payload)
            .context("append replication record payload")?;
        if sync {
            self.file.sync_data().context("sync replication record")?;
        }
        self.index.insert(
            seq,
            JournalIndexEntry {
                offset: record_start + REPLICATION_LOG_RECORD_HEADER_BYTES,
                len,
            },
        );
        self.meta.next_seq = seq.saturating_add(1);
        self.meta.first_seq = self.first_seq();
        if seq >= self.meta.last_applied_seq {
            self.meta.last_applied_seq = seq;
        }
        if term > self.meta.max_term_seen {
            self.meta.max_term_seen = term;
        }
        self.persist_meta(sync)?;
        self.compact_if_needed()?;
        Ok(())
    }

    fn reset_to_applied(&mut self, applied_seq: u64, term: u64, sync: bool) -> anyhow::Result<()> {
        self.file
            .set_len(0)
            .context("truncate replication log during snapshot reset")?;
        self.file
            .seek(SeekFrom::Start(0))
            .context("rewind replication log during snapshot reset")?;
        if sync {
            self.file
                .sync_data()
                .context("sync replication log during snapshot reset")?;
        }
        self.index.clear();
        self.meta.first_seq = applied_seq.saturating_add(1).max(1);
        self.meta.next_seq = applied_seq.saturating_add(1).max(1);
        self.meta.last_applied_seq = applied_seq;
        self.meta.max_term_seen = term.max(self.meta.max_term_seen);
        self.persist_meta(sync)
    }

    fn read_entry(&mut self, seq: u64) -> anyhow::Result<Option<Bytes>> {
        let Some(entry) = self.index.get(&seq).copied() else {
            return Ok(None);
        };
        self.file
            .seek(SeekFrom::Start(entry.offset))
            .with_context(|| format!("seek replication seq={seq}"))?;
        let mut raw = vec![0u8; entry.len as usize];
        self.file
            .read_exact(&mut raw)
            .with_context(|| format!("read replication seq={seq}"))?;
        Ok(Some(Bytes::from(raw)))
    }

    fn read_range(&mut self, from_seq: u64, max_entries: usize) -> anyhow::Result<Vec<Bytes>> {
        let with_seq = self.read_range_with_seq(from_seq, max_entries)?;
        Ok(with_seq.into_iter().map(|(_, raw)| raw).collect())
    }

    fn read_range_with_seq(
        &mut self,
        from_seq: u64,
        max_entries: usize,
    ) -> anyhow::Result<Vec<(u64, Bytes)>> {
        let capped = max_entries.clamp(1, LOG_RANGE_FRAME_MAX_ENTRIES);
        let mut out = Vec::with_capacity(capped);
        let seqs: Vec<u64> = self
            .index
            .range(from_seq..)
            .map(|(seq, _)| *seq)
            .take(capped)
            .collect();
        for seq in seqs {
            if let Some(raw) = self.read_entry(seq)? {
                out.push((seq, raw));
            }
        }
        Ok(out)
    }

    fn persist_meta(&self, sync: bool) -> anyhow::Result<()> {
        let raw = serde_json::to_vec(&self.meta).context("encode replication meta")?;
        let tmp = self.meta_path.with_extension("json.tmp");
        fs::write(&tmp, raw).with_context(|| format!("write {}", tmp.display()))?;
        if sync {
            let file = OpenOptions::new()
                .read(true)
                .open(&tmp)
                .with_context(|| format!("open {}", tmp.display()))?;
            file.sync_data()
                .with_context(|| format!("sync {}", tmp.display()))?;
        }
        fs::rename(&tmp, &self.meta_path)
            .with_context(|| format!("rename {} -> {}", tmp.display(), self.meta_path.display()))?;
        if sync {
            let dir = OpenOptions::new()
                .read(true)
                .open(&self.dir)
                .with_context(|| format!("open {}", self.dir.display()))?;
            let _ = dir.sync_data();
        }
        Ok(())
    }

    fn compact_if_needed(&mut self) -> anyhow::Result<()> {
        if self.index.len() <= self.max_entries {
            return Ok(());
        }
        let keep_from = self
            .index
            .keys()
            .nth(self.index.len().saturating_sub(self.max_entries))
            .copied()
            .unwrap_or(self.meta.next_seq);
        if keep_from <= self.first_seq() {
            return Ok(());
        }

        let tmp_path = self.log_path.with_extension("log.tmp");
        let mut tmp = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .with_context(|| format!("open {}", tmp_path.display()))?;
        let mut new_index = BTreeMap::new();
        let mut offset = 0u64;
        let seqs: Vec<u64> = self.index.range(keep_from..).map(|(seq, _)| *seq).collect();
        for seq in seqs {
            let Some(raw) = self.read_entry(seq)? else {
                anyhow::bail!("missing seq={} during compaction", seq);
            };
            let len = u32::try_from(raw.len()).context("compaction payload too large")?;
            let record_checksum = checksum_replication_record(seq, raw.as_ref());
            tmp.write_all(&len.to_le_bytes())
                .context("write compacted replication record length")?;
            tmp.write_all(&seq.to_le_bytes())
                .context("write compacted replication record seq")?;
            tmp.write_all(&record_checksum.to_le_bytes())
                .context("write compacted replication record checksum")?;
            tmp.write_all(raw.as_ref())
                .context("write compacted replication record payload")?;
            new_index.insert(
                seq,
                JournalIndexEntry {
                    offset: offset + REPLICATION_LOG_RECORD_HEADER_BYTES,
                    len,
                },
            );
            offset = offset.saturating_add(REPLICATION_LOG_RECORD_HEADER_BYTES + len as u64);
        }
        tmp.sync_data()
            .with_context(|| format!("sync {}", tmp_path.display()))?;
        drop(tmp);

        fs::rename(&tmp_path, &self.log_path).with_context(|| {
            format!(
                "rename compacted log {} -> {}",
                tmp_path.display(),
                self.log_path.display()
            )
        })?;

        self.file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&self.log_path)
            .with_context(|| format!("reopen {}", self.log_path.display()))?;
        self.file
            .seek(SeekFrom::End(0))
            .context("seek compacted replication log end")?;
        self.index = new_index;
        self.meta.first_seq = self.first_seq();
        if self.meta.last_applied_seq.saturating_add(1) < self.meta.first_seq {
            self.meta.last_applied_seq = self.meta.first_seq.saturating_sub(1);
        }
        self.persist_meta(true)
    }
}

fn checksum_fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn checksum_replication_record(seq: u64, payload: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for byte in seq.to_le_bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    for byte in payload {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn encode_commit_mode(mode: MutationCommitMode) -> u8 {
    match mode {
        MutationCommitMode::Durable => 0,
        MutationCommitMode::Acknowledged => 1,
    }
}

fn decode_commit_mode(mode: u8) -> anyhow::Result<MutationCommitMode> {
    match mode {
        0 => Ok(MutationCommitMode::Durable),
        1 => Ok(MutationCommitMode::Acknowledged),
        _ => anyhow::bail!("invalid commit mode={mode}"),
    }
}

fn encode_framed_payload_bytes(payload_bytes: &[u8], label: &str) -> anyhow::Result<Bytes> {
    let checksum = checksum_fnv1a64(payload_bytes);
    let mut out = Vec::with_capacity(12 + payload_bytes.len());
    out.extend_from_slice(&checksum.to_le_bytes());
    let len =
        u32::try_from(payload_bytes.len()).with_context(|| format!("{label} payload too large"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(payload_bytes);
    Ok(Bytes::from(out))
}

fn encode_replication_payload(payload: &ReplicationPayload) -> anyhow::Result<Bytes> {
    let payload_bytes =
        rkyv::to_bytes::<_, 4096>(payload).context("serialize replication payload")?;
    encode_framed_payload_bytes(payload_bytes.as_ref(), "replication")
}

fn decode_framed_payload<'a>(raw: &'a [u8], label: &str) -> anyhow::Result<&'a [u8]> {
    if raw.len() < 12 {
        anyhow::bail!("{label} payload too short: {}", raw.len());
    }
    let checksum = u64::from_le_bytes(
        raw[0..8]
            .try_into()
            .expect("checked exact checksum header length"),
    );
    let len = u32::from_le_bytes(
        raw[8..12]
            .try_into()
            .expect("checked exact payload length header"),
    ) as usize;
    if raw.len() != 12 + len {
        anyhow::bail!(
            "{label} payload length mismatch: raw={}, header_len={}",
            raw.len(),
            len
        );
    }
    let payload_bytes = &raw[12..];
    let actual_checksum = checksum_fnv1a64(payload_bytes);
    if checksum != actual_checksum {
        anyhow::bail!(
            "{label} checksum mismatch: got {}, expected {}",
            checksum,
            actual_checksum
        );
    }
    Ok(payload_bytes)
}

#[cfg(test)]
fn decode_replication_payload_seq(raw: &[u8]) -> anyhow::Result<u64> {
    let payload_bytes = decode_framed_payload(raw, "replication")?;
    let mut aligned = rkyv::AlignedVec::with_capacity(payload_bytes.len());
    aligned.extend_from_slice(payload_bytes);
    let archived = rkyv::check_archived_root::<ReplicationPayload>(aligned.as_ref())
        .map_err(|err| anyhow::anyhow!("validate archived replication payload: {err}"))?;
    if archived.schema_version != REPLICATION_PAYLOAD_SCHEMA_VERSION {
        anyhow::bail!(
            "replication payload schema mismatch: got {}, expected {}",
            archived.schema_version,
            REPLICATION_PAYLOAD_SCHEMA_VERSION
        );
    }
    Ok(archived.seq)
}

fn decode_replication_payload(raw: &[u8]) -> anyhow::Result<ReplicationPayload> {
    let payload_bytes = decode_framed_payload(raw, "replication")?;
    let mut aligned = rkyv::AlignedVec::with_capacity(payload_bytes.len());
    aligned.extend_from_slice(payload_bytes);
    let archived = rkyv::check_archived_root::<ReplicationPayload>(aligned.as_ref())
        .map_err(|err| anyhow::anyhow!("validate archived replication payload: {err}"))?;
    let payload: ReplicationPayload = archived
        .deserialize(&mut Infallible)
        .map_err(|err| anyhow::anyhow!("deserialize replication payload: {err}"))?;
    if payload.schema_version != REPLICATION_PAYLOAD_SCHEMA_VERSION {
        anyhow::bail!(
            "replication payload schema mismatch: got {}, expected {}",
            payload.schema_version,
            REPLICATION_PAYLOAD_SCHEMA_VERSION
        );
    }
    Ok(payload)
}

fn encode_snapshot_payload(payload: &SnapshotPayload) -> anyhow::Result<Bytes> {
    let payload_bytes = rkyv::to_bytes::<_, 4096>(payload).context("serialize snapshot payload")?;
    encode_framed_payload_bytes(payload_bytes.as_ref(), "snapshot")
}

fn decode_snapshot_payload(raw: &[u8]) -> anyhow::Result<SnapshotPayload> {
    let payload_bytes = decode_framed_payload(raw, "snapshot")?;
    let mut aligned = rkyv::AlignedVec::with_capacity(payload_bytes.len());
    aligned.extend_from_slice(payload_bytes);
    let archived = rkyv::check_archived_root::<SnapshotPayload>(aligned.as_ref())
        .map_err(|err| anyhow::anyhow!("validate archived snapshot payload: {err}"))?;
    let payload: SnapshotPayload = archived
        .deserialize(&mut Infallible)
        .map_err(|err| anyhow::anyhow!("deserialize snapshot payload: {err}"))?;
    if payload.schema_version != SNAPSHOT_PAYLOAD_SCHEMA_VERSION {
        anyhow::bail!(
            "snapshot payload schema mismatch: got {}, expected {}",
            payload.schema_version,
            SNAPSHOT_PAYLOAD_SCHEMA_VERSION
        );
    }
    Ok(payload)
}

fn encode_internal_partition_mutations_wire_request(
    payload: &InternalPartitionMutationsWireRequest,
) -> anyhow::Result<Bytes> {
    let payload_bytes =
        rkyv::to_bytes::<_, 4096>(payload).context("serialize internal mutations request")?;
    encode_framed_payload_bytes(payload_bytes.as_ref(), "internal-mutations-request")
}

fn decode_internal_partition_mutations_wire_request(
    raw: &[u8],
) -> anyhow::Result<InternalPartitionMutationsWireRequest> {
    let payload_bytes = decode_framed_payload(raw, "internal-mutations-request")?;
    let mut aligned = rkyv::AlignedVec::with_capacity(payload_bytes.len());
    aligned.extend_from_slice(payload_bytes);
    let archived =
        rkyv::check_archived_root::<InternalPartitionMutationsWireRequest>(aligned.as_ref())
            .map_err(|err| {
                anyhow::anyhow!("validate archived internal mutations request: {err}")
            })?;
    let payload: InternalPartitionMutationsWireRequest = archived
        .deserialize(&mut Infallible)
        .map_err(|err| anyhow::anyhow!("deserialize internal mutations request: {err}"))?;
    if payload.schema_version != INTERNAL_PARTITION_MUTATIONS_SCHEMA_VERSION {
        anyhow::bail!(
            "internal mutations schema mismatch: got {}, expected {}",
            payload.schema_version,
            INTERNAL_PARTITION_MUTATIONS_SCHEMA_VERSION
        );
    }
    Ok(payload)
}

fn encode_internal_partition_mutations_wire_response(
    payload: &InternalPartitionMutationsWireResponse,
) -> anyhow::Result<Bytes> {
    let payload_bytes =
        rkyv::to_bytes::<_, 4096>(payload).context("serialize internal mutations response")?;
    encode_framed_payload_bytes(payload_bytes.as_ref(), "internal-mutations-response")
}

fn decode_internal_partition_mutations_wire_response(
    raw: &[u8],
) -> anyhow::Result<InternalPartitionMutationsWireResponse> {
    let payload_bytes = decode_framed_payload(raw, "internal-mutations-response")?;
    let mut aligned = rkyv::AlignedVec::with_capacity(payload_bytes.len());
    aligned.extend_from_slice(payload_bytes);
    let archived =
        rkyv::check_archived_root::<InternalPartitionMutationsWireResponse>(aligned.as_ref())
            .map_err(|err| {
                anyhow::anyhow!("validate archived internal mutations response: {err}")
            })?;
    let payload: InternalPartitionMutationsWireResponse = archived
        .deserialize(&mut Infallible)
        .map_err(|err| anyhow::anyhow!("deserialize internal mutations response: {err}"))?;
    if payload.schema_version != INTERNAL_PARTITION_MUTATIONS_SCHEMA_VERSION {
        anyhow::bail!(
            "internal mutations schema mismatch: got {}, expected {}",
            payload.schema_version,
            INTERNAL_PARTITION_MUTATIONS_SCHEMA_VERSION
        );
    }
    Ok(payload)
}

fn encode_internal_partition_search_wire_request(
    payload: &InternalPartitionSearchWireRequest,
) -> anyhow::Result<Bytes> {
    let payload_bytes =
        rkyv::to_bytes::<_, 4096>(payload).context("serialize internal search request")?;
    encode_framed_payload_bytes(payload_bytes.as_ref(), "internal-search-request")
}

fn decode_internal_partition_search_wire_request(
    raw: &[u8],
) -> anyhow::Result<InternalPartitionSearchWireRequest> {
    let payload_bytes = decode_framed_payload(raw, "internal-search-request")?;
    let mut aligned = rkyv::AlignedVec::with_capacity(payload_bytes.len());
    aligned.extend_from_slice(payload_bytes);
    let archived =
        rkyv::check_archived_root::<InternalPartitionSearchWireRequest>(aligned.as_ref())
            .map_err(|err| anyhow::anyhow!("validate archived internal search request: {err}"))?;
    let payload: InternalPartitionSearchWireRequest = archived
        .deserialize(&mut Infallible)
        .map_err(|err| anyhow::anyhow!("deserialize internal search request: {err}"))?;
    if payload.schema_version != INTERNAL_PARTITION_SEARCH_SCHEMA_VERSION {
        anyhow::bail!(
            "internal search schema mismatch: got {}, expected {}",
            payload.schema_version,
            INTERNAL_PARTITION_SEARCH_SCHEMA_VERSION
        );
    }
    Ok(payload)
}

fn encode_internal_partition_search_wire_response(
    payload: &InternalPartitionSearchWireResponse,
) -> anyhow::Result<Bytes> {
    let payload_bytes =
        rkyv::to_bytes::<_, 4096>(payload).context("serialize internal search response")?;
    encode_framed_payload_bytes(payload_bytes.as_ref(), "internal-search-response")
}

fn decode_internal_partition_search_wire_response(
    raw: &[u8],
) -> anyhow::Result<InternalPartitionSearchWireResponse> {
    let payload_bytes = decode_framed_payload(raw, "internal-search-response")?;
    let mut aligned = rkyv::AlignedVec::with_capacity(payload_bytes.len());
    aligned.extend_from_slice(payload_bytes);
    let archived =
        rkyv::check_archived_root::<InternalPartitionSearchWireResponse>(aligned.as_ref())
            .map_err(|err| anyhow::anyhow!("validate archived internal search response: {err}"))?;
    let payload: InternalPartitionSearchWireResponse = archived
        .deserialize(&mut Infallible)
        .map_err(|err| anyhow::anyhow!("deserialize internal search response: {err}"))?;
    if payload.schema_version != INTERNAL_PARTITION_SEARCH_SCHEMA_VERSION {
        anyhow::bail!(
            "internal search schema mismatch: got {}, expected {}",
            payload.schema_version,
            INTERNAL_PARTITION_SEARCH_SCHEMA_VERSION
        );
    }
    Ok(payload)
}

fn encode_payload_frame(entries: &[Bytes]) -> anyhow::Result<Bytes> {
    let count = u32::try_from(entries.len()).context("too many framed entries")?;
    let mut out = Vec::with_capacity(
        4 + entries
            .iter()
            .map(|entry| 4usize.saturating_add(entry.len()))
            .sum::<usize>(),
    );
    out.extend_from_slice(&count.to_le_bytes());
    for entry in entries {
        let len = u32::try_from(entry.len()).context("framed entry too large")?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(entry);
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
fn decode_payload_frame(raw: &[u8]) -> anyhow::Result<Vec<Bytes>> {
    if raw.len() < 4 {
        anyhow::bail!("payload frame too short");
    }
    let mut offset = 0usize;
    let count =
        u32::from_le_bytes(raw[offset..offset + 4].try_into().expect("checked header")) as usize;
    offset += 4;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > raw.len() {
            anyhow::bail!("payload frame truncated at entry length");
        }
        let len = u32::from_le_bytes(
            raw[offset..offset + 4]
                .try_into()
                .expect("checked entry length"),
        ) as usize;
        offset += 4;
        if offset + len > raw.len() {
            anyhow::bail!("payload frame truncated at entry payload");
        }
        out.push(Bytes::copy_from_slice(&raw[offset..offset + len]));
        offset += len;
    }
    if offset != raw.len() {
        anyhow::bail!("payload frame trailing bytes: {}", raw.len() - offset);
    }
    Ok(out)
}

#[derive(Debug)]
struct EtcdLeaderElector {
    node_id: String,
    cfg: EtcdElectionConfig,
    is_leader: AtomicBool,
    ownership: Mutex<Option<LeaderOwnership>>,
}

#[derive(Debug, Clone)]
struct EtcdElectionConfig {
    endpoints: Vec<String>,
    election_key: String,
    leader_endpoint_key: String,
    leader_endpoint_value: String,
    lease_ttl_secs: i64,
    retry_backoff: Duration,
    connect_timeout: Duration,
    request_timeout: Duration,
    username: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct LeaderOwnership {
    lease_id: i64,
}

impl EtcdLeaderElector {
    fn new(node_id: String, cfg: EtcdElectionConfig) -> anyhow::Result<Self> {
        if cfg.endpoints.is_empty() {
            anyhow::bail!("--etcd-endpoints must not be empty");
        }
        if cfg.election_key.is_empty() {
            anyhow::bail!("--etcd-election-key must not be empty");
        }
        if cfg.leader_endpoint_value.is_empty() {
            anyhow::bail!("leader endpoint value must not be empty");
        }
        if cfg.lease_ttl_secs < 2 {
            anyhow::bail!("--etcd-lease-ttl-secs must be >= 2");
        }
        if cfg.username.is_some() ^ cfg.password.is_some() {
            anyhow::bail!("--etcd-username and --etcd-password must be set together");
        }

        Ok(Self {
            node_id,
            cfg,
            is_leader: AtomicBool::new(false),
            ownership: Mutex::new(None),
        })
    }

    fn connect_options(&self) -> ConnectOptions {
        let mut options = ConnectOptions::new()
            .with_require_leader(true)
            .with_connect_timeout(self.cfg.connect_timeout)
            .with_timeout(self.cfg.request_timeout)
            .with_keep_alive(Duration::from_secs(2), Duration::from_secs(2))
            .with_keep_alive_while_idle(true);
        if let (Some(username), Some(password)) = (&self.cfg.username, &self.cfg.password) {
            options = options.with_user(username.clone(), password.clone());
        }
        options
    }

    async fn connect(&self) -> anyhow::Result<EtcdClient> {
        EtcdClient::connect(self.cfg.endpoints.clone(), Some(self.connect_options()))
            .await
            .with_context(|| format!("connect to etcd endpoints {}", self.cfg.endpoints.join(",")))
    }

    fn mark_leader(&self, lease_id: i64) {
        if let Ok(mut ownership) = self.ownership.lock() {
            *ownership = Some(LeaderOwnership { lease_id });
        }
        self.is_leader.store(true, Ordering::Relaxed);
    }

    fn clear_leader(&self) -> Option<LeaderOwnership> {
        self.is_leader.store(false, Ordering::Relaxed);
        match self.ownership.lock() {
            Ok(mut ownership) => ownership.take(),
            Err(_) => None,
        }
    }

    fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
    }

    fn role(&self) -> &'static str {
        if self.is_leader() {
            "leader"
        } else {
            "follower"
        }
    }

    fn current_term(&self) -> u64 {
        match self.ownership.lock() {
            Ok(ownership) => ownership
                .and_then(|owned| u64::try_from(owned.lease_id).ok())
                .unwrap_or(0),
            Err(_) => 0,
        }
    }

    async fn release(&self) {
        let Some(lease) = self.clear_leader() else {
            return;
        };
        if let Ok(mut client) = self.connect().await {
            let _ = client.lease_revoke(lease.lease_id).await;
        }
    }

    async fn run(self: Arc<Self>) {
        loop {
            if let Err(err) = self.election_round().await {
                self.clear_leader();
                eprintln!("leader-election error: {err:#}");
            }
            tokio::time::sleep(self.cfg.retry_backoff).await;
        }
    }

    async fn election_round(&self) -> anyhow::Result<()> {
        let mut client = self.connect().await?;
        let lease = client
            .lease_grant(self.cfg.lease_ttl_secs, None)
            .await
            .context("grant etcd lease")?;
        let lease_id = lease.id();

        let then_ops = vec![
            TxnOp::put(
                self.cfg.election_key.as_bytes(),
                self.node_id.as_bytes(),
                Some(PutOptions::new().with_lease(lease_id)),
            ),
            TxnOp::put(
                self.cfg.leader_endpoint_key.as_bytes(),
                self.cfg.leader_endpoint_value.as_bytes(),
                Some(PutOptions::new().with_lease(lease_id)),
            ),
        ];
        let tx = Txn::new()
            .when(vec![Compare::version(
                self.cfg.election_key.as_bytes(),
                CompareOp::Equal,
                0,
            )])
            .and_then(then_ops);

        let txn = client.txn(tx).await.context("acquire etcd leader key")?;
        if !txn.succeeded() {
            let _ = client.lease_revoke(lease_id).await;
            return Ok(());
        }

        self.mark_leader(lease_id);

        let keepalive_every = Duration::from_secs((self.cfg.lease_ttl_secs / 3).max(1) as u64);
        let (mut keeper, mut stream) = client
            .lease_keep_alive(lease_id)
            .await
            .context("open etcd keepalive stream")?;

        keeper
            .keep_alive()
            .await
            .context("send initial keepalive")?;
        stream
            .message()
            .await
            .context("read initial keepalive response")?
            .ok_or_else(|| anyhow::anyhow!("etcd keepalive stream closed"))?;

        loop {
            tokio::time::sleep(keepalive_every).await;
            if !self.is_leader() {
                break;
            }

            keeper
                .keep_alive()
                .await
                .context("send keepalive heartbeat")?;
            let ack = stream
                .message()
                .await
                .context("read keepalive heartbeat response")?
                .ok_or_else(|| anyhow::anyhow!("etcd keepalive stream closed"))?;
            if ack.ttl() <= 0 {
                anyhow::bail!("etcd lease ttl expired");
            }

            let response = client
                .get(self.cfg.election_key.as_bytes(), None)
                .await
                .context("verify leader key ownership")?;
            let Some(kv) = response.kvs().first() else {
                anyhow::bail!("leader key deleted");
            };
            if kv.lease() != lease_id {
                anyhow::bail!("leader key lease mismatch");
            }
            if kv.value() != self.node_id.as_bytes() {
                anyhow::bail!("leader key value mismatch");
            }
            let endpoint_response = client
                .get(self.cfg.leader_endpoint_key.as_bytes(), None)
                .await
                .context("verify leader endpoint key ownership")?;
            let Some(endpoint_kv) = endpoint_response.kvs().first() else {
                anyhow::bail!("leader endpoint key deleted");
            };
            if endpoint_kv.lease() != lease_id {
                anyhow::bail!("leader endpoint key lease mismatch");
            }
            if endpoint_kv.value() != self.cfg.leader_endpoint_value.as_bytes() {
                anyhow::bail!("leader endpoint key value mismatch");
            }
        }

        let _ = client.lease_revoke(lease_id).await;
        self.clear_leader();
        Ok(())
    }
}

struct AppState {
    node_id: String,
    self_url: String,
    topology: Arc<ClusterTopology>,
    partitions: HashMap<u32, Arc<PartitionRuntime>>,
    preferred_partition_endpoints: RwLock<HashMap<u32, String>>,
    replication_timeout: Duration,
    replication_snapshot_timeout: Duration,
    replication_catchup_batch: usize,
    leader: Arc<EtcdLeaderElector>,
    internal_auth_token: Option<String>,
    http: Client,
}

struct PartitionRuntime {
    partition_id: u32,
    replication_workers: Vec<ReplicationWorkerHandle>,
    quorum_size: usize,
    index: Arc<RwLock<SpFreshLayerDbShardedIndex>>,
    leader: Arc<EtcdLeaderElector>,
    journal: Arc<Mutex<ReplicationJournal>>,
    mutation_gate: Arc<AsyncMutex<()>>,
}

#[derive(Clone)]
struct ReplicationWorkerHandle {
    peer: String,
    tx: mpsc::Sender<ReplicationWorkerItem>,
}

struct ReplicationWorkerItem {
    seq: u64,
    leader_term: u64,
    payload: Bytes,
    response: oneshot::Sender<Result<(), String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ErrorResponse {
    error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    replication_reject: Option<ReplicationRejectResponse>,
}

type ApiError = (StatusCode, Json<ErrorResponse>);
type ApiResult<T> = Result<Json<T>, ApiError>;

#[derive(Debug, Serialize)]
struct RoleResponse {
    node_id: String,
    role: String,
    leader: bool,
    replication: ReplicationState,
    local_partition_replication: Vec<PartitionReplicationState>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    node_id: String,
    role: String,
    leader: bool,
    stats: SpFreshLayerDbShardedStats,
    replication: ReplicationState,
    local_partition_replication: Vec<PartitionReplicationState>,
}

#[derive(Debug, Clone, Serialize)]
struct PartitionReplicationState {
    partition_id: u32,
    leader: bool,
    first_seq: u64,
    next_seq: u64,
    last_applied_seq: u64,
    max_term_seen: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MutationInput {
    Upsert { id: u64, values: Vec<f32> },
    Delete { id: u64 },
}

impl MutationInput {
    fn id(&self) -> u64 {
        match self {
            Self::Upsert { id, .. } | Self::Delete { id } => *id,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ApplyMutationsRequest {
    mutations: Vec<MutationInput>,
    #[serde(default)]
    commit_mode: Option<CommitModeArg>,
    #[serde(default)]
    allow_partial: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct InternalPartitionMutationsRequest {
    partition_id: u32,
    mutations: Vec<MutationInput>,
    #[serde(default)]
    commit_mode: Option<CommitModeArg>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ReplicationSummary {
    attempted: usize,
    succeeded: usize,
    failed: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ApplyMutationsResponse {
    applied_upserts: usize,
    applied_deletes: usize,
    replicated: ReplicationSummary,
}

#[derive(Debug, Serialize)]
struct SnapshotInstallResponse {
    applied_seq: u64,
    rows: usize,
}

#[derive(Debug, Deserialize)]
struct SearchRequest {
    query: Vec<f32>,
    k: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct InternalPartitionSearchRequest {
    partition_id: u32,
    query: Vec<f32>,
    k: usize,
}

#[derive(Debug, Deserialize, Serialize)]
struct SearchResponse {
    neighbors: Vec<Neighbor>,
}

#[derive(Debug, Deserialize)]
struct SyncToS3Request {
    max_files_per_shard: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct InternalPartitionRequest {
    partition_id: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct InternalPartitionSyncRequest {
    partition_id: u32,
    #[serde(default)]
    max_files_per_shard: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SyncToS3Response {
    moved_files: usize,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
struct InternalPartitionMutationsWireRequest {
    schema_version: u32,
    partition_id: u32,
    commit_mode: u8,
    mutations: Vec<WireMutation>,
}

impl InternalPartitionMutationsWireRequest {
    fn from_parts(
        partition_id: u32,
        commit_mode: CommitModeArg,
        mutations: &[MutationInput],
    ) -> Self {
        let mode = commit_mode;
        Self {
            schema_version: INTERNAL_PARTITION_MUTATIONS_SCHEMA_VERSION,
            partition_id,
            commit_mode: encode_commit_mode(mode.into()),
            mutations: mutations.iter().map(WireMutation::from).collect(),
        }
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
struct InternalReplicationSummaryWire {
    attempted: u64,
    succeeded: u64,
    failed: Vec<String>,
}

impl From<ReplicationSummary> for InternalReplicationSummaryWire {
    fn from(value: ReplicationSummary) -> Self {
        Self {
            attempted: value.attempted as u64,
            succeeded: value.succeeded as u64,
            failed: value.failed,
        }
    }
}

impl TryFrom<InternalReplicationSummaryWire> for ReplicationSummary {
    type Error = anyhow::Error;

    fn try_from(value: InternalReplicationSummaryWire) -> Result<Self, Self::Error> {
        Ok(Self {
            attempted: usize::try_from(value.attempted)
                .context("internal replication attempted does not fit usize")?,
            succeeded: usize::try_from(value.succeeded)
                .context("internal replication succeeded does not fit usize")?,
            failed: value.failed,
        })
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
struct InternalPartitionMutationsWireResponse {
    schema_version: u32,
    applied_upserts: u64,
    applied_deletes: u64,
    replicated: InternalReplicationSummaryWire,
}

impl From<ApplyMutationsResponse> for InternalPartitionMutationsWireResponse {
    fn from(value: ApplyMutationsResponse) -> Self {
        Self {
            schema_version: INTERNAL_PARTITION_MUTATIONS_SCHEMA_VERSION,
            applied_upserts: value.applied_upserts as u64,
            applied_deletes: value.applied_deletes as u64,
            replicated: value.replicated.into(),
        }
    }
}

impl TryFrom<InternalPartitionMutationsWireResponse> for ApplyMutationsResponse {
    type Error = anyhow::Error;

    fn try_from(value: InternalPartitionMutationsWireResponse) -> Result<Self, Self::Error> {
        if value.schema_version != INTERNAL_PARTITION_MUTATIONS_SCHEMA_VERSION {
            anyhow::bail!(
                "internal mutations schema mismatch: got {}, expected {}",
                value.schema_version,
                INTERNAL_PARTITION_MUTATIONS_SCHEMA_VERSION
            );
        }
        Ok(Self {
            applied_upserts: usize::try_from(value.applied_upserts)
                .context("internal response upserts does not fit usize")?,
            applied_deletes: usize::try_from(value.applied_deletes)
                .context("internal response deletes does not fit usize")?,
            replicated: value.replicated.try_into()?,
        })
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
struct InternalPartitionSearchWireRequest {
    schema_version: u32,
    partition_id: u32,
    query: Vec<f32>,
    k: u32,
}

impl InternalPartitionSearchWireRequest {
    fn from_parts(partition_id: u32, query: &[f32], k: usize) -> anyhow::Result<Self> {
        Ok(Self {
            schema_version: INTERNAL_PARTITION_SEARCH_SCHEMA_VERSION,
            partition_id,
            query: query.to_vec(),
            k: u32::try_from(k).context("search k too large for internal wire")?,
        })
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
struct InternalNeighborWire {
    id: u64,
    distance: f32,
}

impl From<Neighbor> for InternalNeighborWire {
    fn from(value: Neighbor) -> Self {
        Self {
            id: value.id,
            distance: value.distance,
        }
    }
}

impl From<InternalNeighborWire> for Neighbor {
    fn from(value: InternalNeighborWire) -> Self {
        Self {
            id: value.id,
            distance: value.distance,
        }
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
struct InternalPartitionSearchWireResponse {
    schema_version: u32,
    neighbors: Vec<InternalNeighborWire>,
}

impl From<SearchResponse> for InternalPartitionSearchWireResponse {
    fn from(value: SearchResponse) -> Self {
        Self {
            schema_version: INTERNAL_PARTITION_SEARCH_SCHEMA_VERSION,
            neighbors: value
                .neighbors
                .into_iter()
                .map(InternalNeighborWire::from)
                .collect(),
        }
    }
}

impl TryFrom<InternalPartitionSearchWireResponse> for SearchResponse {
    type Error = anyhow::Error;

    fn try_from(value: InternalPartitionSearchWireResponse) -> Result<Self, Self::Error> {
        if value.schema_version != INTERNAL_PARTITION_SEARCH_SCHEMA_VERSION {
            anyhow::bail!(
                "internal search schema mismatch: got {}, expected {}",
                value.schema_version,
                INTERNAL_PARTITION_SEARCH_SCHEMA_VERSION
            );
        }
        Ok(Self {
            neighbors: value.neighbors.into_iter().map(Neighbor::from).collect(),
        })
    }
}

fn api_error(status: StatusCode, message: impl Into<String>) -> ApiError {
    (
        status,
        Json(ErrorResponse {
            error: message.into(),
            replication_reject: None,
        }),
    )
}

fn ensure_global_leader(state: &AppState) -> Result<(), ApiError> {
    if state.leader.is_leader() {
        return Ok(());
    }
    Err(api_error(
        StatusCode::CONFLICT,
        "write rejected: this node is not leader",
    ))
}

fn local_partition_state(
    state: &AppState,
    partition_id: u32,
) -> Result<Arc<PartitionRuntime>, ApiError> {
    state.partitions.get(&partition_id).cloned().ok_or_else(|| {
        api_error(
            StatusCode::NOT_FOUND,
            format!("partition {} not hosted by this node", partition_id),
        )
    })
}

fn ensure_internal_auth(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(expected) = state.internal_auth_token.as_deref() else {
        return Ok(());
    };
    let provided = headers
        .get(INTERNAL_AUTH_HEADER)
        .and_then(|value| value.to_str().ok());
    if provided == Some(expected) {
        return Ok(());
    }
    Err(api_error(
        StatusCode::UNAUTHORIZED,
        "unauthorized internal request",
    ))
}

fn is_not_leader_message(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("not leader") || normalized.contains("leader term unavailable")
}

fn is_not_leader_api_error(error: &ApiError) -> bool {
    error.0 == StatusCode::CONFLICT && is_not_leader_message(error.1.error.as_str())
}

fn aggregate_replication_state(state: &AppState) -> Result<ReplicationState, ApiError> {
    if state.partitions.is_empty() {
        return Ok(ReplicationState {
            first_seq: 1,
            next_seq: 1,
            last_applied_seq: 0,
            max_term_seen: 0,
        });
    }

    let mut first_seq = u64::MAX;
    let mut next_seq = 1u64;
    let mut last_applied_seq = u64::MAX;
    let mut max_term_seen = 0u64;
    for partition in state.partitions.values() {
        let journal = partition
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        let snapshot = journal.snapshot_state();
        first_seq = first_seq.min(snapshot.first_seq);
        next_seq = next_seq.max(snapshot.next_seq);
        last_applied_seq = last_applied_seq.min(snapshot.last_applied_seq);
        max_term_seen = max_term_seen.max(snapshot.max_term_seen);
    }

    Ok(ReplicationState {
        first_seq,
        next_seq,
        last_applied_seq,
        max_term_seen,
    })
}

fn collect_local_partition_replication(
    state: &AppState,
) -> Result<Vec<PartitionReplicationState>, ApiError> {
    let mut out = Vec::with_capacity(state.partitions.len());
    let mut ids: Vec<u32> = state.partitions.keys().copied().collect();
    ids.sort_unstable();
    for partition_id in ids {
        let Some(partition) = state.partitions.get(&partition_id) else {
            continue;
        };
        let journal = partition
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        let snapshot = journal.snapshot_state();
        out.push(PartitionReplicationState {
            partition_id,
            leader: partition.leader.is_leader(),
            first_seq: snapshot.first_seq,
            next_seq: snapshot.next_seq,
            last_applied_seq: snapshot.last_applied_seq,
            max_term_seen: snapshot.max_term_seen,
        });
    }
    Ok(out)
}

fn sum_partition_stats(items: Vec<SpFreshLayerDbShardedStats>) -> SpFreshLayerDbShardedStats {
    let mut out = SpFreshLayerDbShardedStats {
        shard_count: 0,
        total_rows: 0,
        total_upserts: 0,
        total_deletes: 0,
        persist_errors: 0,
        total_persist_upsert_us: 0,
        total_persist_delete_us: 0,
        rebuild_successes: 0,
        rebuild_failures: 0,
        total_rebuild_applied_ids: 0,
        total_searches: 0,
        total_search_latency_us: 0,
        pending_ops: 0,
    };
    for item in items {
        out.shard_count = out.shard_count.saturating_add(item.shard_count);
        out.total_rows = out.total_rows.saturating_add(item.total_rows);
        out.total_upserts = out.total_upserts.saturating_add(item.total_upserts);
        out.total_deletes = out.total_deletes.saturating_add(item.total_deletes);
        out.persist_errors = out.persist_errors.saturating_add(item.persist_errors);
        out.total_persist_upsert_us = out
            .total_persist_upsert_us
            .saturating_add(item.total_persist_upsert_us);
        out.total_persist_delete_us = out
            .total_persist_delete_us
            .saturating_add(item.total_persist_delete_us);
        out.rebuild_successes = out.rebuild_successes.saturating_add(item.rebuild_successes);
        out.rebuild_failures = out.rebuild_failures.saturating_add(item.rebuild_failures);
        out.total_rebuild_applied_ids = out
            .total_rebuild_applied_ids
            .saturating_add(item.total_rebuild_applied_ids);
        out.total_searches = out.total_searches.saturating_add(item.total_searches);
        out.total_search_latency_us = out
            .total_search_latency_us
            .saturating_add(item.total_search_latency_us);
        out.pending_ops = out.pending_ops.saturating_add(item.pending_ops);
    }
    out
}

fn collect_partition_mutations(
    topology: &ClusterTopology,
    mutations: Vec<MutationInput>,
) -> Vec<(u32, Vec<MutationInput>)> {
    let capacity = mutations
        .len()
        .min(topology.partition_count as usize)
        .max(1);
    let mut grouped: FxHashMap<u32, Vec<MutationInput>> =
        FxHashMap::with_capacity_and_hasher(capacity, Default::default());
    for mutation in mutations {
        let partition_id = topology.partition_for_id(mutation.id());
        grouped.entry(partition_id).or_default().push(mutation);
    }
    let mut out: Vec<(u32, Vec<MutationInput>)> = grouped.into_iter().collect();
    out.sort_unstable_by_key(|(partition_id, _)| *partition_id);
    out
}

fn ordered_partition_endpoints(
    state: &AppState,
    partition_id: u32,
) -> Result<Vec<String>, ApiError> {
    let placement = state
        .topology
        .placement_for_partition(partition_id)
        .ok_or_else(|| {
            api_error(
                StatusCode::BAD_REQUEST,
                format!("unknown partition {}", partition_id),
            )
        })?;
    let mut out = Vec::with_capacity(placement.replicas.len());
    let mut seen = BTreeSet::new();

    if let Ok(preferred) = state.preferred_partition_endpoints.read() {
        if let Some(endpoint) = preferred.get(&partition_id) {
            if placement.replicas.iter().any(|replica| replica == endpoint)
                && seen.insert(endpoint.clone())
            {
                out.push(endpoint.clone());
            }
        }
    }

    if seen.insert(placement.leader.clone()) {
        out.push(placement.leader.clone());
    }
    for replica in &placement.replicas {
        if seen.insert(replica.clone()) {
            out.push(replica.clone());
        }
    }
    Ok(out)
}

fn mark_partition_preferred_endpoint(state: &AppState, partition_id: u32, endpoint: String) {
    if let Ok(mut preferred) = state.preferred_partition_endpoints.write() {
        preferred.insert(partition_id, endpoint);
    }
}

#[derive(Debug)]
struct HeapNeighbor(Neighbor);

impl PartialEq for HeapNeighbor {
    fn eq(&self, other: &Self) -> bool {
        self.0.id == other.0.id && self.0.distance.to_bits() == other.0.distance.to_bits()
    }
}

impl Eq for HeapNeighbor {}

impl PartialOrd for HeapNeighbor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapNeighbor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .distance
            .total_cmp(&other.0.distance)
            .then_with(|| self.0.id.cmp(&other.0.id))
    }
}

fn neighbor_cmp(lhs: &Neighbor, rhs: &Neighbor) -> std::cmp::Ordering {
    lhs.distance
        .total_cmp(&rhs.distance)
        .then_with(|| lhs.id.cmp(&rhs.id))
}

fn push_heap_topk(heap: &mut BinaryHeap<HeapNeighbor>, neighbor: Neighbor, k: usize) {
    if k == 0 {
        return;
    }
    if heap.len() < k {
        heap.push(HeapNeighbor(neighbor));
        return;
    }
    let replace = heap
        .peek()
        .map(|worst| neighbor_cmp(&neighbor, &worst.0).is_lt())
        .unwrap_or(true);
    if replace {
        let _ = heap.pop();
        heap.push(HeapNeighbor(neighbor));
    }
}

fn finalize_heap_topk(heap: BinaryHeap<HeapNeighbor>) -> Vec<Neighbor> {
    let mut out: Vec<Neighbor> = heap.into_iter().map(|entry| entry.0).collect();
    out.sort_unstable_by(neighbor_cmp);
    out
}

async fn collect_partition_apply_results_until_done<S>(
    results: &mut S,
) -> Result<ApplyMutationsResponse, ApiError>
where
    S: futures::Stream<Item = Result<ApplyMutationsResponse, ApiError>> + Unpin,
{
    let mut out = ApplyMutationsResponse {
        applied_upserts: 0,
        applied_deletes: 0,
        replicated: ReplicationSummary {
            attempted: 0,
            succeeded: 0,
            failed: Vec::new(),
        },
    };
    let mut first_error: Option<ApiError> = None;
    while let Some(result) = results.next().await {
        match result {
            Ok(part) => {
                out.applied_upserts = out.applied_upserts.saturating_add(part.applied_upserts);
                out.applied_deletes = out.applied_deletes.saturating_add(part.applied_deletes);
                out.replicated.attempted = out
                    .replicated
                    .attempted
                    .saturating_add(part.replicated.attempted);
                out.replicated.succeeded = out
                    .replicated
                    .succeeded
                    .saturating_add(part.replicated.succeeded);
                out.replicated.failed.extend(part.replicated.failed);
            }
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }

    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(out)
}

#[cfg(test)]
fn merge_neighbors_topk(all: Vec<Neighbor>, k: usize) -> Vec<Neighbor> {
    if k == 0 || all.is_empty() {
        return Vec::new();
    }
    if all.len() <= k {
        let mut out = all;
        out.sort_unstable_by(neighbor_cmp);
        return out;
    }
    let mut heap = BinaryHeap::with_capacity(k);
    for neighbor in all {
        push_heap_topk(&mut heap, neighbor, k);
    }
    finalize_heap_topk(heap)
}

fn partition_parallelism(partition_count: usize) -> usize {
    let cpu = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4);
    let target = cpu.saturating_mul(4).max(8);
    partition_count.max(1).min(target)
}

fn search_local_partitions_topk(
    partitions: Vec<Arc<PartitionRuntime>>,
    query: Arc<Vec<f32>>,
    k: usize,
) -> anyhow::Result<Vec<Neighbor>> {
    if k == 0 || partitions.is_empty() {
        return Ok(Vec::new());
    }
    let mut top = BinaryHeap::with_capacity(k);
    for partition in partitions {
        let index = partition
            .index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        for neighbor in index.search(query.as_slice(), k) {
            push_heap_topk(&mut top, neighbor, k);
        }
    }
    Ok(finalize_heap_topk(top))
}

async fn role(State(state): State<Arc<AppState>>) -> ApiResult<RoleResponse> {
    let replication = aggregate_replication_state(&state)?;
    let local_partition_replication = collect_local_partition_replication(&state)?;
    Ok(Json(RoleResponse {
        node_id: state.node_id.clone(),
        role: state.leader.role().to_string(),
        leader: state.leader.is_leader(),
        replication,
        local_partition_replication,
    }))
}

async fn health(State(state): State<Arc<AppState>>) -> ApiResult<HealthResponse> {
    let partitions: Vec<Arc<PartitionRuntime>> = state.partitions.values().cloned().collect();
    let stats = tokio::task::spawn_blocking(move || {
        let mut out = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let index = partition
                .index
                .read()
                .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
            out.push(index.health_check()?);
        }
        Ok::<_, anyhow::Error>(sum_partition_stats(out))
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join health task: {err}"),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })?;

    let replication = aggregate_replication_state(&state)?;
    let local_partition_replication = collect_local_partition_replication(&state)?;

    Ok(Json(HealthResponse {
        node_id: state.node_id.clone(),
        role: state.leader.role().to_string(),
        leader: state.leader.is_leader(),
        stats,
        replication,
        local_partition_replication,
    }))
}

async fn search(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SearchRequest>,
) -> ApiResult<SearchResponse> {
    if req.k == 0 {
        return Err(api_error(StatusCode::BAD_REQUEST, "k must be > 0"));
    }
    if req.query.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "query must not be empty",
        ));
    }

    let SearchRequest { query, k } = req;
    let query = Arc::new(query);
    let local_partitions: Vec<Arc<PartitionRuntime>> = state.partitions.values().cloned().collect();
    let remote_partition_ids: Vec<u32> = (0..state.topology.partition_count)
        .filter(|partition_id| !state.partitions.contains_key(partition_id))
        .collect();
    let local_query = query.clone();
    let local_search_task = if local_partitions.is_empty() {
        None
    } else {
        Some(tokio::task::spawn_blocking(move || {
            search_local_partitions_topk(local_partitions, local_query, k)
        }))
    };
    let parallelism = partition_parallelism(remote_partition_ids.len());
    let mut futures = stream::iter(remote_partition_ids.into_iter())
        .map(|partition_id| {
            let state = state.clone();
            let query = query.clone();
            async move { route_partition_search(state, partition_id, query, k).await }
        })
        .buffer_unordered(parallelism);
    let mut top = BinaryHeap::with_capacity(k);
    while let Some(result) = futures.next().await {
        let partition_neighbors = result?;
        for neighbor in partition_neighbors {
            push_heap_topk(&mut top, neighbor, k);
        }
    }
    if let Some(task) = local_search_task {
        let local_neighbors = task
            .await
            .map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("join local partition search task: {err}"),
                )
            })
            .and_then(|result| {
                result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
            })?;
        for neighbor in local_neighbors {
            push_heap_topk(&mut top, neighbor, k);
        }
    }

    Ok(Json(SearchResponse {
        neighbors: finalize_heap_topk(top),
    }))
}

async fn force_rebuild(State(state): State<Arc<AppState>>) -> ApiResult<serde_json::Value> {
    ensure_global_leader(&state)?;
    let parallelism = partition_parallelism(state.topology.partition_count as usize);
    let mut futures = stream::iter(0..state.topology.partition_count)
        .map(|partition_id| {
            let state = state.clone();
            async move { route_partition_force_rebuild(state, partition_id).await }
        })
        .buffer_unordered(parallelism);
    while let Some(result) = futures.next().await {
        result?;
    }

    Ok(Json(serde_json::json!({"ok": true})))
}

async fn sync_to_s3(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncToS3Request>,
) -> ApiResult<SyncToS3Response> {
    ensure_global_leader(&state)?;
    let parallelism = partition_parallelism(state.topology.partition_count as usize);
    let max_files_per_shard = req.max_files_per_shard;
    let mut futures = stream::iter(0..state.topology.partition_count)
        .map(|partition_id| {
            let state = state.clone();
            async move { route_partition_sync_to_s3(state, partition_id, max_files_per_shard).await }
        })
        .buffer_unordered(parallelism);
    let mut moved = 0usize;
    while let Some(result) = futures.next().await {
        moved = moved.saturating_add(result?);
    }

    Ok(Json(SyncToS3Response { moved_files: moved }))
}

async fn apply_mutations(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ApplyMutationsRequest>,
) -> ApiResult<ApplyMutationsResponse> {
    if req.mutations.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "mutations must not be empty",
        ));
    }

    let mode_arg = req.commit_mode.unwrap_or(CommitModeArg::Durable);
    let grouped = collect_partition_mutations(&state.topology, req.mutations);
    if grouped.len() > 1 && !req.allow_partial {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "cross-partition mutation batch rejected: would be partial-commit; retry with allow_partial=true if this is intended",
        ));
    }
    if grouped.len() <= 1 || !req.allow_partial {
        let mut out = ApplyMutationsResponse {
            applied_upserts: 0,
            applied_deletes: 0,
            replicated: ReplicationSummary {
                attempted: 0,
                succeeded: 0,
                failed: Vec::new(),
            },
        };
        for (partition_id, mutations) in grouped {
            let part =
                route_partition_mutations(state.clone(), partition_id, mutations, mode_arg).await?;
            out.applied_upserts = out.applied_upserts.saturating_add(part.applied_upserts);
            out.applied_deletes = out.applied_deletes.saturating_add(part.applied_deletes);
            out.replicated.attempted = out
                .replicated
                .attempted
                .saturating_add(part.replicated.attempted);
            out.replicated.succeeded = out
                .replicated
                .succeeded
                .saturating_add(part.replicated.succeeded);
            out.replicated.failed.extend(part.replicated.failed);
        }
        return Ok(Json(out));
    }

    let parallelism = partition_parallelism(grouped.len());
    let mut futures = stream::iter(grouped.into_iter())
        .map(|(partition_id, mutations)| {
            let state = state.clone();
            async move { route_partition_mutations(state, partition_id, mutations, mode_arg).await }
        })
        .buffer_unordered(parallelism);

    // Drain all partition writes before returning so allow_partial semantics are deterministic.
    let out = collect_partition_apply_results_until_done(&mut futures).await?;

    Ok(Json(out))
}

async fn apply_partition_mutations_internal(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<InternalPartitionMutationsRequest>,
) -> ApiResult<ApplyMutationsResponse> {
    ensure_internal_auth(&state, &headers)?;
    if req.mutations.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "mutations must not be empty",
        ));
    }
    let mode = req.commit_mode.unwrap_or(CommitModeArg::Durable).into();
    apply_partition_mutations_local(state, req.partition_id, req.mutations, mode, true).await
}

async fn apply_partition_mutations_internal_wire(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Bytes, ApiError> {
    ensure_internal_auth(&state, &headers)?;
    let payload =
        decode_internal_partition_mutations_wire_request(body.as_ref()).map_err(|err| {
            api_error(
                StatusCode::BAD_REQUEST,
                format!("decode internal mutations wire request: {err:#}"),
            )
        })?;
    if payload.mutations.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "mutations must not be empty",
        ));
    }
    let mode = decode_commit_mode(payload.commit_mode).map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid internal mutations commit mode: {err:#}"),
        )
    })?;
    let response = apply_partition_mutations_local_wire(
        state,
        payload.partition_id,
        payload.mutations,
        mode,
        true,
    )
    .await?
    .0;
    let payload = InternalPartitionMutationsWireResponse::from(response);
    encode_internal_partition_mutations_wire_response(&payload).map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode internal mutations wire response: {err:#}"),
        )
    })
}

async fn partition_search_internal(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<InternalPartitionSearchRequest>,
) -> ApiResult<SearchResponse> {
    ensure_internal_auth(&state, &headers)?;
    if req.k == 0 {
        return Err(api_error(StatusCode::BAD_REQUEST, "k must be > 0"));
    }
    if req.query.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "query must not be empty",
        ));
    }
    let neighbors =
        search_partition_local(state, req.partition_id, Arc::new(req.query), req.k, false).await?;
    Ok(Json(SearchResponse { neighbors }))
}

async fn partition_search_internal_wire(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Bytes, ApiError> {
    ensure_internal_auth(&state, &headers)?;
    let payload = decode_internal_partition_search_wire_request(body.as_ref()).map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("decode internal search wire request: {err:#}"),
        )
    })?;
    if payload.k == 0 {
        return Err(api_error(StatusCode::BAD_REQUEST, "k must be > 0"));
    }
    if payload.query.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "query must not be empty",
        ));
    }
    let neighbors = search_partition_local(
        state,
        payload.partition_id,
        Arc::new(payload.query),
        usize::try_from(payload.k)
            .map_err(|_| api_error(StatusCode::BAD_REQUEST, "k does not fit usize"))?,
        false,
    )
    .await?;
    let payload = InternalPartitionSearchWireResponse::from(SearchResponse { neighbors });
    encode_internal_partition_search_wire_response(&payload).map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode internal search wire response: {err:#}"),
        )
    })
}

async fn force_rebuild_partition_local(
    state: Arc<AppState>,
    partition_id: u32,
    require_leader: bool,
) -> Result<(), ApiError> {
    let partition = local_partition_state(&state, partition_id)?;
    if require_leader && !partition.leader.is_leader() {
        return Err(api_error(
            StatusCode::CONFLICT,
            format!(
                "partition {} maintenance rejected: this node is not leader",
                partition_id
            ),
        ));
    }
    let index = partition.index.clone();
    tokio::task::spawn_blocking(move || {
        let index = index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        index
            .force_rebuild()
            .with_context(|| format!("force rebuild partition {}", partition_id))
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join rebuild task partition {}: {err}", partition_id),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })?;
    Ok(())
}

async fn sync_partition_to_s3_local(
    state: Arc<AppState>,
    partition_id: u32,
    max_files_per_shard: Option<usize>,
    require_leader: bool,
) -> Result<usize, ApiError> {
    let partition = local_partition_state(&state, partition_id)?;
    if require_leader && !partition.leader.is_leader() {
        return Err(api_error(
            StatusCode::CONFLICT,
            format!(
                "partition {} maintenance rejected: this node is not leader",
                partition_id
            ),
        ));
    }
    let index = partition.index.clone();
    tokio::task::spawn_blocking(move || {
        let index = index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        index
            .sync_to_s3(max_files_per_shard)
            .with_context(|| format!("sync partition {}", partition_id))
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join sync task partition {}: {err}", partition_id),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })
}

async fn partition_force_rebuild_internal(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<InternalPartitionRequest>,
) -> ApiResult<serde_json::Value> {
    ensure_internal_auth(&state, &headers)?;
    force_rebuild_partition_local(state, req.partition_id, true).await?;
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn partition_sync_to_s3_internal(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<InternalPartitionSyncRequest>,
) -> ApiResult<SyncToS3Response> {
    ensure_internal_auth(&state, &headers)?;
    let moved =
        sync_partition_to_s3_local(state, req.partition_id, req.max_files_per_shard, true).await?;
    Ok(Json(SyncToS3Response { moved_files: moved }))
}

async fn parse_remote_error(response: reqwest::Response) -> (StatusCode, String) {
    let status = response.status();
    let body = response
        .bytes()
        .await
        .unwrap_or_else(|_| Bytes::from_static(b""));
    let decoded = serde_json::from_slice::<ErrorResponse>(body.as_ref()).ok();
    let message = decoded
        .map(|payload| payload.error)
        .unwrap_or_else(|| String::from_utf8_lossy(body.as_ref()).into_owned());
    (status, message)
}

async fn call_remote_partition_mutations(
    state: Arc<AppState>,
    endpoint: &str,
    request_body: Bytes,
) -> Result<ApplyMutationsResponse, (StatusCode, String)> {
    let url = join_base_url(endpoint, "/v1/internal/partition-mutations-wire");
    let mut request_builder = state
        .http
        .post(url.clone())
        .timeout(state.replication_timeout)
        .header(CONTENT_TYPE, "application/octet-stream");
    if let Some(token) = &state.internal_auth_token {
        request_builder = request_builder.header(INTERNAL_AUTH_HEADER, token.as_str());
    }
    let response = request_builder
        .body(request_body)
        .send()
        .await
        .map_err(|err| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("request to {} failed: {}", url, err),
            )
        })?;
    if !response.status().is_success() {
        return Err(parse_remote_error(response).await);
    }
    let body = response.bytes().await.map_err(|err| {
        (
            StatusCode::BAD_GATEWAY,
            format!("read {} response body failed: {}", url, err),
        )
    })?;
    let decoded =
        decode_internal_partition_mutations_wire_response(body.as_ref()).map_err(|err| {
            (
                StatusCode::BAD_GATEWAY,
                format!("decode {} response failed: {err:#}", url),
            )
        })?;
    decoded.try_into().map_err(|err: anyhow::Error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("convert {} response failed: {err:#}", url),
        )
    })
}

async fn call_remote_partition_search(
    state: Arc<AppState>,
    endpoint: &str,
    request_body: Bytes,
) -> Result<SearchResponse, (StatusCode, String)> {
    let url = join_base_url(endpoint, "/v1/internal/partition-search-wire");
    let mut request_builder = state
        .http
        .post(url.clone())
        .timeout(state.replication_timeout)
        .header(CONTENT_TYPE, "application/octet-stream");
    if let Some(token) = &state.internal_auth_token {
        request_builder = request_builder.header(INTERNAL_AUTH_HEADER, token.as_str());
    }
    let response = request_builder
        .body(request_body)
        .send()
        .await
        .map_err(|err| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("request to {} failed: {}", url, err),
            )
        })?;
    if !response.status().is_success() {
        return Err(parse_remote_error(response).await);
    }
    let body = response.bytes().await.map_err(|err| {
        (
            StatusCode::BAD_GATEWAY,
            format!("read {} response body failed: {}", url, err),
        )
    })?;
    let decoded = decode_internal_partition_search_wire_response(body.as_ref()).map_err(|err| {
        (
            StatusCode::BAD_GATEWAY,
            format!("decode {} response failed: {err:#}", url),
        )
    })?;
    decoded.try_into().map_err(|err: anyhow::Error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("convert {} response failed: {err:#}", url),
        )
    })
}

async fn call_remote_partition_force_rebuild(
    state: Arc<AppState>,
    endpoint: &str,
    request: &InternalPartitionRequest,
) -> Result<(), (StatusCode, String)> {
    let url = join_base_url(endpoint, "/v1/internal/partition-force-rebuild");
    let mut request_builder = state
        .http
        .post(url.clone())
        .timeout(state.replication_timeout)
        .header(CONTENT_TYPE, "application/json");
    if let Some(token) = &state.internal_auth_token {
        request_builder = request_builder.header(INTERNAL_AUTH_HEADER, token.as_str());
    }
    let response = request_builder.json(request).send().await.map_err(|err| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("request to {} failed: {}", url, err),
        )
    })?;
    if !response.status().is_success() {
        return Err(parse_remote_error(response).await);
    }
    Ok(())
}

async fn call_remote_partition_sync_to_s3(
    state: Arc<AppState>,
    endpoint: &str,
    request: &InternalPartitionSyncRequest,
) -> Result<SyncToS3Response, (StatusCode, String)> {
    let url = join_base_url(endpoint, "/v1/internal/partition-sync-to-s3");
    let mut request_builder = state
        .http
        .post(url.clone())
        .timeout(state.replication_timeout)
        .header(CONTENT_TYPE, "application/json");
    if let Some(token) = &state.internal_auth_token {
        request_builder = request_builder.header(INTERNAL_AUTH_HEADER, token.as_str());
    }
    let response = request_builder.json(request).send().await.map_err(|err| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("request to {} failed: {}", url, err),
        )
    })?;
    if !response.status().is_success() {
        return Err(parse_remote_error(response).await);
    }
    response.json::<SyncToS3Response>().await.map_err(|err| {
        (
            StatusCode::BAD_GATEWAY,
            format!("decode {} response failed: {}", url, err),
        )
    })
}

async fn route_partition_search(
    state: Arc<AppState>,
    partition_id: u32,
    query: Arc<Vec<f32>>,
    k: usize,
) -> Result<Vec<Neighbor>, ApiError> {
    let endpoints = ordered_partition_endpoints(&state, partition_id)?;
    let mut failures = Vec::new();
    let mut remote_request_body: Option<Bytes> = None;
    for endpoint in endpoints {
        if endpoint == state.self_url {
            match search_partition_local(state.clone(), partition_id, query.clone(), k, false).await
            {
                Ok(neighbors) => {
                    mark_partition_preferred_endpoint(&state, partition_id, endpoint);
                    return Ok(neighbors);
                }
                Err(err) if is_not_leader_api_error(&err) => {
                    failures.push(err.1.error.clone());
                }
                Err(err) => return Err(err),
            }
            continue;
        }

        if remote_request_body.is_none() {
            let wire_request =
                InternalPartitionSearchWireRequest::from_parts(partition_id, query.as_slice(), k)
                    .map_err(|err| {
                    api_error(
                        StatusCode::BAD_REQUEST,
                        format!("encode internal search request failed: {err:#}"),
                    )
                })?;
            let encoded =
                encode_internal_partition_search_wire_request(&wire_request).map_err(|err| {
                    api_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("encode internal search wire request failed: {err:#}"),
                    )
                })?;
            remote_request_body = Some(encoded);
        }
        match call_remote_partition_search(
            state.clone(),
            endpoint.as_str(),
            remote_request_body.as_ref().expect("set above").clone(),
        )
        .await
        {
            Ok(response) => {
                mark_partition_preferred_endpoint(&state, partition_id, endpoint);
                return Ok(response.neighbors);
            }
            Err((status, message))
                if status == StatusCode::CONFLICT && is_not_leader_message(message.as_str()) =>
            {
                failures.push(message);
            }
            Err((status, message))
                if status.is_server_error()
                    || status == StatusCode::REQUEST_TIMEOUT
                    || status == StatusCode::TOO_MANY_REQUESTS =>
            {
                failures.push(message);
            }
            Err((status, message)) => return Err(api_error(status, message)),
        }
    }
    Err(api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        format!(
            "partition {} has no reachable leader for search: {}",
            partition_id,
            failures.join(" | ")
        ),
    ))
}

async fn route_partition_force_rebuild(
    state: Arc<AppState>,
    partition_id: u32,
) -> Result<(), ApiError> {
    let endpoints = ordered_partition_endpoints(&state, partition_id)?;
    let mut failures = Vec::new();
    for endpoint in endpoints {
        if endpoint == state.self_url {
            match force_rebuild_partition_local(state.clone(), partition_id, true).await {
                Ok(()) => {
                    mark_partition_preferred_endpoint(&state, partition_id, endpoint);
                    return Ok(());
                }
                Err(err) if is_not_leader_api_error(&err) => {
                    failures.push(err.1.error.clone());
                }
                Err(err) => return Err(err),
            }
            continue;
        }

        let request = InternalPartitionRequest { partition_id };
        match call_remote_partition_force_rebuild(state.clone(), endpoint.as_str(), &request).await
        {
            Ok(()) => {
                mark_partition_preferred_endpoint(&state, partition_id, endpoint);
                return Ok(());
            }
            Err((status, message))
                if status == StatusCode::CONFLICT && is_not_leader_message(message.as_str()) =>
            {
                failures.push(message);
            }
            Err((status, message))
                if status.is_server_error()
                    || status == StatusCode::REQUEST_TIMEOUT
                    || status == StatusCode::TOO_MANY_REQUESTS =>
            {
                failures.push(message);
            }
            Err((status, message)) => return Err(api_error(status, message)),
        }
    }
    Err(api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        format!(
            "partition {} has no reachable leader for force rebuild: {}",
            partition_id,
            failures.join(" | ")
        ),
    ))
}

async fn route_partition_sync_to_s3(
    state: Arc<AppState>,
    partition_id: u32,
    max_files_per_shard: Option<usize>,
) -> Result<usize, ApiError> {
    let endpoints = ordered_partition_endpoints(&state, partition_id)?;
    let mut failures = Vec::new();
    for endpoint in endpoints {
        if endpoint == state.self_url {
            match sync_partition_to_s3_local(state.clone(), partition_id, max_files_per_shard, true)
                .await
            {
                Ok(response) => {
                    mark_partition_preferred_endpoint(&state, partition_id, endpoint);
                    return Ok(response);
                }
                Err(err) if is_not_leader_api_error(&err) => {
                    failures.push(err.1.error.clone());
                }
                Err(err) => return Err(err),
            }
            continue;
        }

        let request = InternalPartitionSyncRequest {
            partition_id,
            max_files_per_shard,
        };
        match call_remote_partition_sync_to_s3(state.clone(), endpoint.as_str(), &request).await {
            Ok(response) => {
                mark_partition_preferred_endpoint(&state, partition_id, endpoint);
                return Ok(response.moved_files);
            }
            Err((status, message))
                if status == StatusCode::CONFLICT && is_not_leader_message(message.as_str()) =>
            {
                failures.push(message);
            }
            Err((status, message))
                if status.is_server_error()
                    || status == StatusCode::REQUEST_TIMEOUT
                    || status == StatusCode::TOO_MANY_REQUESTS =>
            {
                failures.push(message);
            }
            Err((status, message)) => return Err(api_error(status, message)),
        }
    }
    Err(api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        format!(
            "partition {} has no reachable leader for sync_to_s3: {}",
            partition_id,
            failures.join(" | ")
        ),
    ))
}

async fn route_partition_mutations(
    state: Arc<AppState>,
    partition_id: u32,
    mutations: Vec<MutationInput>,
    mode_arg: CommitModeArg,
) -> Result<ApplyMutationsResponse, ApiError> {
    let endpoints = ordered_partition_endpoints(&state, partition_id)?;
    let mut failures = Vec::new();
    let mut remote_request_body: Option<Bytes> = None;
    for endpoint in endpoints {
        if endpoint == state.self_url {
            if let Some(local_partition) = state.partitions.get(&partition_id) {
                if !local_partition.leader.is_leader() {
                    failures.push(format!(
                        "partition {} write rejected: this node is not leader",
                        partition_id
                    ));
                    continue;
                }
            }
            match apply_partition_mutations_local(
                state.clone(),
                partition_id,
                mutations.clone(),
                mode_arg.into(),
                true,
            )
            .await
            {
                Ok(response) => {
                    mark_partition_preferred_endpoint(&state, partition_id, endpoint);
                    return Ok(response.0);
                }
                Err(err) if is_not_leader_api_error(&err) => {
                    failures.push(err.1.error.clone());
                }
                Err(err) => return Err(err),
            }
            continue;
        }

        if remote_request_body.is_none() {
            let wire_request = InternalPartitionMutationsWireRequest::from_parts(
                partition_id,
                mode_arg,
                mutations.as_slice(),
            );
            let encoded =
                encode_internal_partition_mutations_wire_request(&wire_request).map_err(|err| {
                    api_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("encode internal mutations wire request failed: {err:#}"),
                    )
                })?;
            remote_request_body = Some(encoded);
        }
        match call_remote_partition_mutations(
            state.clone(),
            endpoint.as_str(),
            remote_request_body.as_ref().expect("set above").clone(),
        )
        .await
        {
            Ok(response) => {
                mark_partition_preferred_endpoint(&state, partition_id, endpoint);
                return Ok(response);
            }
            Err((status, message))
                if status == StatusCode::CONFLICT && is_not_leader_message(message.as_str()) =>
            {
                failures.push(message);
            }
            Err((status, message))
                if status.is_server_error()
                    || status == StatusCode::REQUEST_TIMEOUT
                    || status == StatusCode::TOO_MANY_REQUESTS =>
            {
                failures.push(message);
            }
            Err((status, message)) => return Err(api_error(status, message)),
        }
    }
    Err(api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        format!(
            "partition {} has no reachable leader for mutations: {}",
            partition_id,
            failures.join(" | ")
        ),
    ))
}

async fn search_partition_local(
    state: Arc<AppState>,
    partition_id: u32,
    query: Arc<Vec<f32>>,
    k: usize,
    require_leader: bool,
) -> Result<Vec<Neighbor>, ApiError> {
    let partition = local_partition_state(&state, partition_id)?;
    if require_leader && !partition.leader.is_leader() {
        return Err(api_error(
            StatusCode::CONFLICT,
            format!(
                "partition {} write/search rejected: this node is not leader",
                partition_id
            ),
        ));
    }

    let index = partition.index.clone();
    tokio::task::spawn_blocking(move || {
        let index = index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        Ok::<_, anyhow::Error>(index.search(query.as_slice(), k))
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join partition search task: {err}"),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })
}

async fn apply_partition_mutations_local(
    state: Arc<AppState>,
    partition_id: u32,
    mutations: Vec<MutationInput>,
    mode: MutationCommitMode,
    require_leader: bool,
) -> ApiResult<ApplyMutationsResponse> {
    apply_partition_mutations_local_wire(
        state,
        partition_id,
        mutations.into_iter().map(WireMutation::from).collect(),
        mode,
        require_leader,
    )
    .await
}

async fn apply_partition_mutations_local_wire(
    state: Arc<AppState>,
    partition_id: u32,
    mutations: Vec<WireMutation>,
    mode: MutationCommitMode,
    require_leader: bool,
) -> ApiResult<ApplyMutationsResponse> {
    let partition = local_partition_state(&state, partition_id)?;
    if require_leader && !partition.leader.is_leader() {
        return Err(api_error(
            StatusCode::CONFLICT,
            format!(
                "partition {} write rejected: this node is not leader",
                partition_id
            ),
        ));
    }
    for mutation in &mutations {
        let expected = state.topology.partition_for_id(mutation.id());
        if expected != partition_id {
            return Err(api_error(
                StatusCode::BAD_REQUEST,
                format!(
                    "mutation id {} belongs to partition {}, not {}",
                    mutation.id(),
                    expected,
                    partition_id
                ),
            ));
        }
    }

    let _gate = partition.mutation_gate.lock().await;
    let term = partition.leader.current_term();
    if term == 0 {
        return Err(api_error(
            StatusCode::CONFLICT,
            format!("partition {} leader term unavailable", partition_id),
        ));
    }
    let seq = {
        let journal = partition
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        journal.next_seq()
    };

    let payload = ReplicationPayload {
        schema_version: REPLICATION_PAYLOAD_SCHEMA_VERSION,
        partition_id,
        seq,
        term,
        commit_mode: encode_commit_mode(mode),
        mutations,
    };
    let encoded = encode_replication_payload(&payload).map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode replication payload: {err:#}"),
        )
    })?;

    let runtime_mutations = payload.into_runtime_mutations();
    let applied = apply_local_mutations_runtime(partition.clone(), runtime_mutations, mode).await?;
    let sync_journal = matches!(mode, MutationCommitMode::Durable);
    {
        let mut journal = partition
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        journal
            .append_and_mark_applied(seq, term, encoded.as_ref(), sync_journal)
            .map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "commit replication journal partition={} seq={}: {err:#}",
                        partition_id, seq
                    ),
                )
            })?;
    }

    let replicated = if matches!(mode, MutationCommitMode::Durable) {
        let follower_quorum = partition.quorum_size.saturating_sub(1);
        let replicated = replicate_to_followers_until_quorum(
            state.clone(),
            partition.clone(),
            seq,
            term,
            encoded.clone(),
            follower_quorum,
        )
        .await;
        if replicated.succeeded < replicated.attempted {
            let tail_state = state.clone();
            let tail_partition = partition.clone();
            let tail_payload = encoded.clone();
            tokio::spawn(async move {
                let _ = replicate_to_followers(tail_state, tail_partition, seq, term, tail_payload)
                    .await;
            });
        }
        replicated
    } else {
        replicate_to_followers(state.clone(), partition.clone(), seq, term, encoded.clone()).await
    };

    if matches!(mode, MutationCommitMode::Durable) {
        let quorum_acks = 1usize.saturating_add(replicated.succeeded);
        if quorum_acks < partition.quorum_size {
            return Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                format!(
                    "partition {} durable quorum not reached: quorum={} acks={} attempted_replicas={} failures={}",
                    partition_id,
                    partition.quorum_size,
                    quorum_acks,
                    replicated.attempted,
                    replicated.failed.join(" | ")
                ),
            ));
        }
    }

    Ok(Json(ApplyMutationsResponse {
        applied_upserts: applied.upserts,
        applied_deletes: applied.deletes,
        replicated,
    }))
}

fn replication_reject_error(error: String, journal: &ReplicationJournal) -> ApiError {
    let reject = ReplicationRejectResponse {
        error: error.clone(),
        expected_seq: journal.expected_next_seq(),
        first_available_seq: journal.first_seq(),
        last_available_seq: journal.last_seq(),
        max_term_seen: journal.max_term_seen(),
    };
    (
        StatusCode::CONFLICT,
        Json(ErrorResponse {
            error,
            replication_reject: Some(reject),
        }),
    )
}

async fn apply_replication(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<ApplyMutationsResponse> {
    ensure_internal_auth(&state, &headers)?;
    let payload = decode_replication_payload(body.as_ref()).map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("decode replication payload: {err:#}"),
        )
    })?;
    if payload.mutations.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "replication payload mutations must not be empty",
        ));
    }
    for mutation in &payload.mutations {
        let expected = state.topology.partition_for_id(mutation.id());
        if expected != payload.partition_id {
            return Err(api_error(
                StatusCode::BAD_REQUEST,
                format!(
                    "replication mutation id {} belongs to partition {}, not {}",
                    mutation.id(),
                    expected,
                    payload.partition_id
                ),
            ));
        }
    }
    let partition_id = payload.partition_id;
    let seq = payload.seq;
    let term = payload.term;
    let partition = local_partition_state(&state, partition_id)?;
    let _gate = partition.mutation_gate.lock().await;
    {
        let mut journal = partition
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        let expected = journal.expected_next_seq();
        if term < journal.max_term_seen() {
            return Err(replication_reject_error(
                format!(
                    "stale term {} < max_term_seen {}",
                    term,
                    journal.max_term_seen()
                ),
                &journal,
            ));
        }
        if seq < expected {
            if let Some(existing) = journal.read_entry(seq).map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("read replication seq={} from journal: {err:#}", seq),
                )
            })? {
                if existing.as_ref() == body.as_ref() {
                    return Ok(Json(ApplyMutationsResponse {
                        applied_upserts: 0,
                        applied_deletes: 0,
                        replicated: ReplicationSummary {
                            attempted: 0,
                            succeeded: 0,
                            failed: Vec::new(),
                        },
                    }));
                }
            }
            return Err(replication_reject_error(
                format!("duplicate or stale seq {} expected {}", seq, expected),
                &journal,
            ));
        }
        if seq > expected {
            return Err(replication_reject_error(
                format!("out-of-order seq {} expected {}", seq, expected),
                &journal,
            ));
        }
    }

    let mode = payload.commit_mode().map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid replication commit mode: {err:#}"),
        )
    })?;
    let runtime_mutations = payload.into_runtime_mutations();
    let applied = apply_local_mutations_runtime(partition.clone(), runtime_mutations, mode).await?;
    let sync_journal = matches!(mode, MutationCommitMode::Durable);
    {
        let mut journal = partition
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        journal
            .append_and_mark_applied(seq, term, body.as_ref(), sync_journal)
            .map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("commit replication seq={}: {err:#}", seq),
                )
            })?;
    }

    Ok(Json(ApplyMutationsResponse {
        applied_upserts: applied.upserts,
        applied_deletes: applied.deletes,
        replicated: ReplicationSummary {
            attempted: 0,
            succeeded: 0,
            failed: Vec::new(),
        },
    }))
}

async fn install_snapshot(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<SnapshotInstallResponse> {
    ensure_internal_auth(&state, &headers)?;
    let payload = decode_snapshot_payload(body.as_ref()).map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("decode snapshot payload: {err:#}"),
        )
    })?;
    let partition_id = payload.partition_id;
    let applied_seq = payload.applied_seq;
    let term = payload.term;
    for row in &payload.rows {
        let expected = state.topology.partition_for_id(row.id);
        if expected != partition_id {
            return Err(api_error(
                StatusCode::BAD_REQUEST,
                format!(
                    "snapshot row id {} belongs to partition {}, not {}",
                    row.id, expected, partition_id
                ),
            ));
        }
    }
    let rows = payload.into_runtime_rows();
    let row_count = rows.len();
    let partition = local_partition_state(&state, partition_id)?;
    let _gate = partition.mutation_gate.lock().await;
    {
        let journal = partition
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        if term < journal.max_term_seen() {
            return Err(replication_reject_error(
                format!(
                    "stale snapshot term {} < max_term_seen {}",
                    term,
                    journal.max_term_seen()
                ),
                &journal,
            ));
        }
    }

    let index = partition.index.clone();
    tokio::task::spawn_blocking(move || {
        let mut index = index
            .write()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        index
            .try_bulk_load_owned(rows)
            .context("apply snapshot rows")
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join snapshot apply task: {err}"),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })?;

    {
        let mut journal = partition
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        journal
            .reset_to_applied(applied_seq, term, true)
            .map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("reset replication journal to seq={}: {err:#}", applied_seq),
                )
            })?;
    }

    Ok(Json(SnapshotInstallResponse {
        applied_seq,
        rows: row_count,
    }))
}

async fn replication_log_range(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<LogRangeRequest>,
) -> Result<Bytes, ApiError> {
    ensure_internal_auth(&state, &headers)?;
    if req.max_entries == 0 {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "max_entries must be > 0",
        ));
    }

    let entries = {
        let partition = local_partition_state(&state, req.partition_id)?;
        let mut journal = partition
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        journal
            .read_range(req.from_seq, req.max_entries)
            .map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "read replication log range partition={} from_seq={}: {err:#}",
                        req.partition_id, req.from_seq
                    ),
                )
            })?
    };
    encode_payload_frame(&entries).map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode replication frame: {err:#}"),
        )
    })
}

async fn apply_local_mutations_runtime(
    partition: Arc<PartitionRuntime>,
    mutations: Vec<VectorMutation>,
    mode: MutationCommitMode,
) -> Result<VectorMutationBatchResult, ApiError> {
    let index = partition.index.clone();

    tokio::task::spawn_blocking(move || {
        let mut index = index
            .write()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        index.try_apply_batch_owned_with_commit_mode(mutations, mode)
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join apply task: {err}"),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })
}

fn normalize_base_url(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

fn build_http_client() -> anyhow::Result<Client> {
    Client::builder()
        .pool_max_idle_per_host(256)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_nodelay(true)
        .build()
        .context("build HTTP client")
}

fn canonicalize_http_base_url(url: &str, flag_name: &str) -> anyhow::Result<String> {
    let parsed = reqwest::Url::parse(url.trim())
        .with_context(|| format!("{flag_name} must be an absolute URL: {url}"))?;
    match parsed.scheme() {
        "http" | "https" => {}
        other => anyhow::bail!("{flag_name} must use http or https scheme, got {other}: {url}"),
    }
    if parsed.host_str().is_none() {
        anyhow::bail!("{flag_name} must include host: {url}");
    }
    if parsed.query().is_some() {
        anyhow::bail!("{flag_name} must not include query string: {url}");
    }
    if parsed.fragment().is_some() {
        anyhow::bail!("{flag_name} must not include fragment: {url}");
    }
    Ok(normalize_base_url(parsed.to_string().as_str()))
}

fn join_base_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn collect_cluster_endpoints(self_url: &str, replica_urls: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(replica_urls.len().saturating_add(1));
    let mut seen = HashSet::new();
    let self_base = normalize_base_url(self_url);
    if seen.insert(self_base.clone()) {
        out.push(self_base);
    }
    for replica in replica_urls.iter().map(|url| normalize_base_url(url)) {
        if seen.insert(replica.clone()) {
            out.push(replica);
        }
    }
    out
}

fn collect_replication_peers(self_url: &str, replica_urls: &[String]) -> Vec<String> {
    let self_base = normalize_base_url(self_url);
    let mut seen = HashSet::new();
    replica_urls
        .iter()
        .map(|url| normalize_base_url(url))
        .filter(|url| self_base != *url)
        .filter(|url| seen.insert(url.clone()))
        .collect()
}

fn replication_worker_handles(partition: &PartitionRuntime) -> Vec<ReplicationWorkerHandle> {
    partition.replication_workers.clone()
}

fn schedule_replication_worker(
    state: Arc<AppState>,
    partition: Arc<PartitionRuntime>,
    worker: ReplicationWorkerHandle,
    seq: u64,
    leader_term: u64,
    payload: Bytes,
) -> BoxFuture<'static, Result<(), String>> {
    let peer = worker.peer.clone();
    let (response_tx, response_rx) = oneshot::channel();
    let item = ReplicationWorkerItem {
        seq,
        leader_term,
        payload,
        response: response_tx,
    };
    match worker.tx.try_send(item) {
        Ok(()) => Box::pin(async move {
            match response_rx.await {
                Ok(result) => result,
                Err(_) => Err(format!(
                    "{peer}: replication worker response channel closed"
                )),
            }
        }),
        Err(TrySendError::Full(item)) => {
            let ReplicationWorkerItem {
                seq,
                leader_term,
                payload,
                ..
            } = item;
            Box::pin(async move {
                replicate_to_peer(state, partition, peer, seq, leader_term, payload).await
            })
        }
        Err(TrySendError::Closed(item)) => {
            let ReplicationWorkerItem {
                seq,
                leader_term,
                payload,
                ..
            } = item;
            Box::pin(async move {
                replicate_to_peer(state, partition, peer, seq, leader_term, payload).await
            })
        }
    }
}

async fn replication_worker_loop(
    state: Arc<AppState>,
    partition: Arc<PartitionRuntime>,
    peer: String,
    mut rx: mpsc::Receiver<ReplicationWorkerItem>,
) {
    while let Some(item) = rx.recv().await {
        let result = replicate_to_peer(
            state.clone(),
            partition.clone(),
            peer.clone(),
            item.seq,
            item.leader_term,
            item.payload,
        )
        .await;
        let _ = item.response.send(result);
    }
}

#[derive(Debug)]
enum ReplicationSendError {
    Reject(ReplicationRejectResponse),
    Other(String),
}

fn parse_replication_reject(body: &[u8]) -> Option<ReplicationRejectResponse> {
    if let Ok(reject) = serde_json::from_slice::<ReplicationRejectResponse>(body) {
        return Some(reject);
    }
    if let Ok(err) = serde_json::from_slice::<ErrorResponse>(body) {
        if let Some(reject) = err.replication_reject {
            return Some(reject);
        }
        if let Ok(reject) = serde_json::from_str::<ReplicationRejectResponse>(&err.error) {
            return Some(reject);
        }
    }
    None
}

async fn send_replication_payload(
    client: &Client,
    endpoint: &str,
    timeout: Duration,
    internal_auth_token: Option<&str>,
    payload: Bytes,
) -> Result<(), ReplicationSendError> {
    let mut request_builder = client
        .post(endpoint)
        .timeout(timeout)
        .header(CONTENT_TYPE, "application/octet-stream");
    if let Some(token) = internal_auth_token {
        request_builder = request_builder.header(INTERNAL_AUTH_HEADER, token);
    }
    let response = request_builder
        .body(payload)
        .send()
        .await
        .map_err(|err| ReplicationSendError::Other(format!("request failed: {err}")))?;
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }

    let body = response
        .bytes()
        .await
        .unwrap_or_else(|_| Bytes::from_static(b""));
    if status == StatusCode::CONFLICT {
        if let Some(reject) = parse_replication_reject(body.as_ref()) {
            return Err(ReplicationSendError::Reject(reject));
        }
    }
    let body_snippet = String::from_utf8_lossy(body.as_ref());
    let body_snippet = body_snippet
        .chars()
        .take(256)
        .collect::<String>()
        .replace('\n', "\\n");
    Err(ReplicationSendError::Other(format!(
        "http {status} body={body_snippet}"
    )))
}

async fn build_snapshot_payload(
    partition: Arc<PartitionRuntime>,
    applied_seq: u64,
    term: u64,
) -> Result<Bytes, String> {
    let partition_id = partition.partition_id;
    let index = partition.index.clone();
    let rows = tokio::task::spawn_blocking(move || {
        let index = index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        index.snapshot_rows()
    })
    .await
    .map_err(|err| format!("join snapshot build task: {err}"))?
    .map_err(|err| format!("build snapshot rows: {err:#}"))?;
    let payload = SnapshotPayload {
        schema_version: SNAPSHOT_PAYLOAD_SCHEMA_VERSION,
        partition_id,
        applied_seq,
        term,
        rows: rows.into_iter().map(WireSnapshotRow::from).collect(),
    };
    encode_snapshot_payload(&payload).map_err(|err| format!("encode snapshot payload: {err:#}"))
}

async fn install_snapshot_on_peer(
    state: Arc<AppState>,
    partition: Arc<PartitionRuntime>,
    peer: &str,
    applied_seq: u64,
    leader_term: u64,
) -> Result<(), String> {
    let endpoint = join_base_url(peer, "/v1/internal/install-snapshot");
    let payload = build_snapshot_payload(partition, applied_seq, leader_term).await?;
    let mut request_builder = state
        .http
        .post(&endpoint)
        .timeout(state.replication_snapshot_timeout)
        .header(CONTENT_TYPE, "application/octet-stream");
    if let Some(token) = state.internal_auth_token.as_deref() {
        request_builder = request_builder.header(INTERNAL_AUTH_HEADER, token);
    }
    let response = request_builder
        .body(payload)
        .send()
        .await
        .map_err(|err| format!("{peer}: snapshot install request failed: {err}"))?;
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let body = response
        .bytes()
        .await
        .unwrap_or_else(|_| Bytes::from_static(b""));
    if status == StatusCode::CONFLICT {
        if let Some(reject) = parse_replication_reject(body.as_ref()) {
            return Err(format!(
                "{peer}: snapshot rejected expected_seq={} first_available_seq={} max_term_seen={} reason={}",
                reject.expected_seq, reject.first_available_seq, reject.max_term_seen, reject.error
            ));
        }
    }
    let body_snippet = String::from_utf8_lossy(body.as_ref());
    let body_snippet = body_snippet
        .chars()
        .take(256)
        .collect::<String>()
        .replace('\n', "\\n");
    Err(format!(
        "{peer}: snapshot install http {status} body={body_snippet}"
    ))
}

async fn catch_up_peer(
    state: Arc<AppState>,
    partition: Arc<PartitionRuntime>,
    peer: &str,
    endpoint: &str,
    target_seq: u64,
    mut next_seq: u64,
    leader_term: u64,
) -> Result<(), String> {
    if next_seq > target_seq {
        return Ok(());
    }

    let mut rounds = 0usize;
    while next_seq <= target_seq {
        rounds = rounds.saturating_add(1);
        if rounds > 1_000_000 {
            return Err(format!(
                "{peer}: replication catch-up exceeded retry budget at seq {next_seq}"
            ));
        }

        let entries = {
            let mut journal = partition
                .journal
                .lock()
                .map_err(|_| format!("{peer}: journal lock poisoned"))?;
            let first = journal.first_seq();
            let next = journal.next_seq();
            if next_seq < first {
                return Err(format!(
                    "{peer}: catch-up gap next_seq={} below first_available_seq={first}",
                    next_seq
                ));
            }
            if next_seq >= next {
                return Err(format!(
                    "{peer}: catch-up unavailable next_seq={} local_next_seq={next}",
                    next_seq
                ));
            }
            journal
                .read_range_with_seq(next_seq, state.replication_catchup_batch)
                .map_err(|err| {
                    format!("{peer}: read replication range from seq={next_seq}: {err:#}")
                })?
        };

        if entries.is_empty() {
            return Err(format!(
                "{peer}: empty catch-up range at seq={next_seq} target_seq={target_seq}"
            ));
        }

        let mut restart_from = None;
        for (entry_seq, raw) in entries {
            if entry_seq < next_seq {
                continue;
            }
            if entry_seq > target_seq {
                return Ok(());
            }

            match send_replication_payload(
                &state.http,
                endpoint,
                state.replication_timeout,
                state.internal_auth_token.as_deref(),
                raw.clone(),
            )
            .await
            {
                Ok(()) => {
                    next_seq = entry_seq.saturating_add(1);
                }
                Err(ReplicationSendError::Reject(reject)) => {
                    if reject.max_term_seen > leader_term {
                        return Err(format!(
                            "{peer}: follower term {} exceeds leader term {}",
                            reject.max_term_seen, leader_term
                        ));
                    }
                    if reject.expected_seq == entry_seq.saturating_add(1) {
                        next_seq = reject.expected_seq;
                        continue;
                    }
                    if reject.expected_seq > target_seq {
                        return Ok(());
                    }
                    restart_from = Some(reject.expected_seq);
                    break;
                }
                Err(ReplicationSendError::Other(err)) => {
                    return Err(format!("{peer}: catch-up seq={} failed: {err}", entry_seq));
                }
            }
        }

        if let Some(restart) = restart_from {
            next_seq = restart;
        }
    }
    Ok(())
}

async fn replicate_to_peer(
    state: Arc<AppState>,
    partition: Arc<PartitionRuntime>,
    peer: String,
    seq: u64,
    leader_term: u64,
    payload: Bytes,
) -> Result<(), String> {
    let endpoint = join_base_url(&peer, "/v1/internal/replicate");
    match send_replication_payload(
        &state.http,
        &endpoint,
        state.replication_timeout,
        state.internal_auth_token.as_deref(),
        payload,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(ReplicationSendError::Reject(reject)) => {
            if reject.max_term_seen > leader_term {
                return Err(format!(
                    "{peer}: follower term {} exceeds leader term {}",
                    reject.max_term_seen, leader_term
                ));
            }
            if reject.expected_seq > seq.saturating_add(1) {
                return Err(format!(
                    "{peer}: follower expected seq {} beyond leader seq {}",
                    reject.expected_seq, seq
                ));
            }
            if reject.expected_seq == seq.saturating_add(1) {
                return Ok(());
            }
            let local_first_seq = {
                let journal = partition
                    .journal
                    .lock()
                    .map_err(|_| format!("{peer}: journal lock poisoned"))?;
                journal.first_seq()
            };
            if reject.expected_seq < local_first_seq {
                install_snapshot_on_peer(state.clone(), partition.clone(), &peer, seq, leader_term)
                    .await?;
                return Ok(());
            }
            catch_up_peer(
                state.clone(),
                partition,
                &peer,
                &endpoint,
                seq,
                reject.expected_seq,
                leader_term,
            )
            .await
        }
        Err(ReplicationSendError::Other(err)) => Err(format!("{peer}: {err}")),
    }
}

async fn collect_replication_results_until_quorum<Fut>(
    futures: impl IntoIterator<Item = Fut>,
    attempted: usize,
    required_successes: usize,
) -> ReplicationSummary
where
    Fut: std::future::Future<Output = Result<(), String>>,
{
    if attempted == 0 {
        return ReplicationSummary {
            attempted: 0,
            succeeded: 0,
            failed: Vec::new(),
        };
    }
    if required_successes == 0 {
        return ReplicationSummary {
            attempted,
            succeeded: 0,
            failed: Vec::new(),
        };
    }

    let mut pending = FuturesUnordered::new();
    for fut in futures {
        pending.push(fut);
    }

    let mut succeeded = 0usize;
    let mut failed = Vec::new();
    while let Some(result) = pending.next().await {
        match result {
            Ok(()) => {
                succeeded = succeeded.saturating_add(1);
                if succeeded >= required_successes {
                    break;
                }
            }
            Err(err) => failed.push(err),
        }
    }

    ReplicationSummary {
        attempted,
        succeeded,
        failed,
    }
}

async fn replicate_to_followers(
    state: Arc<AppState>,
    partition: Arc<PartitionRuntime>,
    seq: u64,
    leader_term: u64,
    payload: Bytes,
) -> ReplicationSummary {
    let workers = replication_worker_handles(partition.as_ref());
    if workers.is_empty() {
        return ReplicationSummary {
            attempted: 0,
            succeeded: 0,
            failed: Vec::new(),
        };
    }

    let attempted = workers.len();
    let futures = workers.into_iter().map(|worker| {
        schedule_replication_worker(
            state.clone(),
            partition.clone(),
            worker,
            seq,
            leader_term,
            payload.clone(),
        )
    });
    collect_replication_results_until_quorum(futures, attempted, attempted).await
}

async fn replicate_to_followers_until_quorum(
    state: Arc<AppState>,
    partition: Arc<PartitionRuntime>,
    seq: u64,
    leader_term: u64,
    payload: Bytes,
    required_successes: usize,
) -> ReplicationSummary {
    let workers = replication_worker_handles(partition.as_ref());
    if workers.is_empty() {
        return ReplicationSummary {
            attempted: 0,
            succeeded: 0,
            failed: Vec::new(),
        };
    }

    let attempted = workers.len();
    let required = required_successes.min(attempted);
    let futures = workers.into_iter().map(|worker| {
        schedule_replication_worker(
            state.clone(),
            partition.clone(),
            worker,
            seq,
            leader_term,
            payload.clone(),
        )
    });
    collect_replication_results_until_quorum(futures, attempted, required).await
}

fn build_index(args: &Args, root: &Path) -> anyhow::Result<SpFreshLayerDbShardedIndex> {
    if args.shards == 0 {
        anyhow::bail!("--shards must be > 0");
    }

    let shard_cfg = SpFreshLayerDbConfig {
        spfresh: SpFreshConfig {
            dim: args.dim,
            initial_postings: args.initial_postings,
            split_limit: args.split_limit,
            merge_limit: args.merge_limit,
            reassign_range: args.reassign_range,
            nprobe: args.nprobe,
            diskmeta_probe_multiplier: args.diskmeta_probe_multiplier,
            kmeans_iters: args.kmeans_iters,
        },
        rebuild_pending_ops: args.rebuild_pending_ops,
        rebuild_interval: Duration::from_millis(args.rebuild_interval_ms),
        memory_mode: args.memory_mode.into(),
        ..Default::default()
    };

    let cfg = SpFreshLayerDbShardedConfig {
        shard_count: args.shards,
        shard: shard_cfg,
        exact_shard_prune: false,
    };

    SpFreshLayerDbShardedIndex::open(root, cfg)
}

fn partition_election_key(base_election_key: &str, partition_id: u32) -> String {
    format!(
        "{}/partitions/{partition_id:08}/leader",
        base_election_key.trim_end_matches('/')
    )
}

fn topology_key(base_election_key: &str) -> String {
    format!(
        "{}/topology/current",
        base_election_key.trim_end_matches('/')
    )
}

async fn ensure_cluster_topology(
    cfg: &EtcdElectionConfig,
    key: &str,
    candidate: &ClusterTopology,
) -> anyhow::Result<ClusterTopology> {
    let options = {
        let mut opts = ConnectOptions::new()
            .with_require_leader(true)
            .with_connect_timeout(cfg.connect_timeout)
            .with_timeout(cfg.request_timeout)
            .with_keep_alive(Duration::from_secs(2), Duration::from_secs(2))
            .with_keep_alive_while_idle(true);
        if let (Some(username), Some(password)) = (&cfg.username, &cfg.password) {
            opts = opts.with_user(username.clone(), password.clone());
        }
        opts
    };

    let mut client = EtcdClient::connect(cfg.endpoints.clone(), Some(options))
        .await
        .with_context(|| format!("connect to etcd endpoints {}", cfg.endpoints.join(",")))?;

    let parse_existing = |raw: &[u8]| -> anyhow::Result<ClusterTopology> {
        let payload: ClusterTopologyEnvelope =
            serde_json::from_slice(raw).context("decode etcd topology payload")?;
        if payload.schema_version != CLUSTER_TOPOLOGY_SCHEMA_VERSION {
            anyhow::bail!(
                "unsupported topology schema version {}, expected {}",
                payload.schema_version,
                CLUSTER_TOPOLOGY_SCHEMA_VERSION
            );
        }
        payload
            .topology
            .validate()
            .context("validate stored topology")?;
        Ok(payload.topology)
    };

    let validate_compatible = |stored: &ClusterTopology| -> anyhow::Result<()> {
        if stored.partition_count != candidate.partition_count {
            anyhow::bail!(
                "topology partition_count mismatch: stored={} candidate={}",
                stored.partition_count,
                candidate.partition_count
            );
        }
        if stored.replication_factor != candidate.replication_factor {
            anyhow::bail!(
                "topology replication_factor mismatch: stored={} candidate={}",
                stored.replication_factor,
                candidate.replication_factor
            );
        }
        if stored.placements != candidate.placements {
            anyhow::bail!("topology placement mismatch with stored topology");
        }
        Ok(())
    };

    let existing = client
        .get(key.as_bytes(), None)
        .await
        .with_context(|| format!("read topology key {}", key))?;
    if let Some(kv) = existing.kvs().first() {
        let stored = parse_existing(kv.value())?;
        validate_compatible(&stored)?;
        return Ok(stored);
    }

    let payload = ClusterTopologyEnvelope {
        schema_version: CLUSTER_TOPOLOGY_SCHEMA_VERSION,
        topology: candidate.clone(),
    };
    let encoded = serde_json::to_vec(&payload).context("encode topology payload")?;
    let txn = Txn::new()
        .when([Compare::version(key.as_bytes(), CompareOp::Equal, 0)])
        .and_then([TxnOp::put(key.as_bytes(), encoded, None)]);
    let response = client
        .txn(txn)
        .await
        .with_context(|| format!("create topology key {}", key))?;
    if response.succeeded() {
        return Ok(candidate.clone());
    }

    let stored = client
        .get(key.as_bytes(), None)
        .await
        .with_context(|| format!("read topology key {}", key))?;
    let Some(kv) = stored.kvs().first() else {
        anyhow::bail!("topology key {} disappeared after create race", key);
    };
    let parsed = parse_existing(kv.value())?;
    validate_compatible(&parsed)?;
    Ok(parsed)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.etcd_username.is_some() ^ args.etcd_password.is_some() {
        anyhow::bail!("--etcd-username and --etcd-password must be set together");
    }
    if args.replication_catchup_batch == 0 {
        anyhow::bail!("--replication-catchup-batch must be > 0");
    }
    if args.logical_partitions == 0 {
        anyhow::bail!("--logical-partitions must be > 0");
    }
    if args.diskmeta_probe_multiplier == 0 {
        anyhow::bail!("--diskmeta-probe-multiplier must be > 0");
    }
    let internal_auth_token = args
        .internal_auth_token
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let self_url = canonicalize_http_base_url(args.self_url.as_str(), "--self-url")?;
    let mut replica_urls = Vec::with_capacity(args.replica_urls.len());
    for replica in &args.replica_urls {
        replica_urls.push(canonicalize_http_base_url(replica, "--replica-urls")?);
    }
    let cluster_endpoints = collect_cluster_endpoints(self_url.as_str(), &replica_urls);
    let replication_factor = if args.replication_factor == 0 {
        cluster_endpoints.len()
    } else {
        args.replication_factor
    };
    if cluster_endpoints.len() > 1 && internal_auth_token.is_none() {
        anyhow::bail!(
            "--internal-auth-token is required for multi-node deployments (cluster nodes={})",
            cluster_endpoints.len()
        );
    }

    let global_election_cfg = EtcdElectionConfig {
        endpoints: args.etcd_endpoints.clone(),
        election_key: args.etcd_election_key.clone(),
        leader_endpoint_key: etcd_leader_endpoint_key(&args.etcd_election_key),
        leader_endpoint_value: self_url.clone(),
        lease_ttl_secs: args.etcd_lease_ttl_secs.max(2),
        retry_backoff: Duration::from_millis(args.etcd_retry_ms.max(100)),
        connect_timeout: Duration::from_millis(args.etcd_connect_timeout_ms.max(100)),
        request_timeout: Duration::from_millis(args.etcd_request_timeout_ms.max(100)),
        username: args.etcd_username.clone(),
        password: args.etcd_password.clone(),
    };
    let candidate_topology = ClusterTopology::build_deterministic(
        1,
        args.logical_partitions as u32,
        replication_factor as u32,
        &cluster_endpoints,
    )
    .context("build deterministic cluster topology")?;
    let topology_store_key = topology_key(&args.etcd_election_key);
    let topology = Arc::new(
        ensure_cluster_topology(
            &global_election_cfg,
            topology_store_key.as_str(),
            &candidate_topology,
        )
        .await
        .context("ensure cluster topology in etcd")?,
    );
    let leader = Arc::new(
        EtcdLeaderElector::new(args.node_id.clone(), global_election_cfg)
            .context("init etcd global election")?,
    );
    let leader_task = tokio::spawn(leader.clone().run());

    let mut partitions = HashMap::new();
    let mut replication_worker_receivers: Vec<(
        Arc<PartitionRuntime>,
        String,
        mpsc::Receiver<ReplicationWorkerItem>,
    )> = Vec::new();
    let mut partition_leader_tasks = Vec::new();
    for partition_id in topology.hosted_partitions(self_url.as_str()) {
        let placement = topology
            .placement_for_partition(partition_id)
            .ok_or_else(|| anyhow::anyhow!("missing placement for partition {}", partition_id))?;
        let partition_root = args.db_root.join(format!("part-{partition_id:08}"));
        fs::create_dir_all(&partition_root)
            .with_context(|| format!("create {}", partition_root.display()))?;
        let index = build_index(&args, &partition_root)
            .with_context(|| format!("open partition {} index", partition_id))?;
        let journal =
            ReplicationJournal::open(&partition_root, args.replication_log_max_entries)
                .with_context(|| format!("open partition {} replication journal", partition_id))?;

        let partition_election_key = partition_election_key(&args.etcd_election_key, partition_id);
        let partition_leader = Arc::new(
            EtcdLeaderElector::new(
                args.node_id.clone(),
                EtcdElectionConfig {
                    endpoints: args.etcd_endpoints.clone(),
                    election_key: partition_election_key.clone(),
                    leader_endpoint_key: etcd_leader_endpoint_key(&partition_election_key),
                    leader_endpoint_value: self_url.clone(),
                    lease_ttl_secs: args.etcd_lease_ttl_secs.max(2),
                    retry_backoff: Duration::from_millis(args.etcd_retry_ms.max(100)),
                    connect_timeout: Duration::from_millis(args.etcd_connect_timeout_ms.max(100)),
                    request_timeout: Duration::from_millis(args.etcd_request_timeout_ms.max(100)),
                    username: args.etcd_username.clone(),
                    password: args.etcd_password.clone(),
                },
            )
            .with_context(|| format!("init partition {} election", partition_id))?,
        );
        partition_leader_tasks.push(tokio::spawn(partition_leader.clone().run()));
        let replica_peers = collect_replication_peers(self_url.as_str(), &placement.replicas);
        let mut replication_workers = Vec::with_capacity(replica_peers.len());
        let mut partition_receivers = Vec::with_capacity(replica_peers.len());
        for peer in replica_peers.iter().cloned() {
            let (tx, rx) = mpsc::channel(1024);
            replication_workers.push(ReplicationWorkerHandle {
                peer: peer.clone(),
                tx,
            });
            partition_receivers.push((peer, rx));
        }
        let quorum_size = placement.replicas.len() / 2 + 1;
        let partition_runtime = Arc::new(PartitionRuntime {
            partition_id,
            replication_workers,
            quorum_size,
            index: Arc::new(RwLock::new(index)),
            leader: partition_leader,
            journal: Arc::new(Mutex::new(journal)),
            mutation_gate: Arc::new(AsyncMutex::new(())),
        });
        for (peer, rx) in partition_receivers {
            replication_worker_receivers.push((partition_runtime.clone(), peer, rx));
        }
        partitions.insert(partition_id, partition_runtime);
    }

    let http = build_http_client()?;

    let state = Arc::new(AppState {
        node_id: args.node_id.clone(),
        self_url: self_url.clone(),
        topology: topology.clone(),
        partitions,
        preferred_partition_endpoints: RwLock::new(HashMap::new()),
        replication_timeout: Duration::from_millis(args.replication_timeout_ms.max(100)),
        replication_snapshot_timeout: Duration::from_millis(
            args.replication_snapshot_timeout_ms.max(1_000),
        ),
        replication_catchup_batch: args.replication_catchup_batch,
        leader,
        internal_auth_token,
        http,
    });
    let mut replication_worker_tasks = Vec::new();
    for (partition, peer, rx) in replication_worker_receivers {
        let worker_state = state.clone();
        replication_worker_tasks.push(tokio::spawn(async move {
            replication_worker_loop(worker_state, partition, peer, rx).await;
        }));
    }

    let app = Router::new()
        .route("/v1/role", get(role))
        .route("/v1/health", get(health))
        .route("/v1/search", post(search))
        .route("/v1/mutations", post(apply_mutations))
        .route(
            "/v1/internal/partition-mutations",
            post(apply_partition_mutations_internal),
        )
        .route(
            "/v1/internal/partition-mutations-wire",
            post(apply_partition_mutations_internal_wire),
        )
        .route(
            "/v1/internal/partition-search",
            post(partition_search_internal),
        )
        .route(
            "/v1/internal/partition-search-wire",
            post(partition_search_internal_wire),
        )
        .route(
            "/v1/internal/partition-force-rebuild",
            post(partition_force_rebuild_internal),
        )
        .route(
            "/v1/internal/partition-sync-to-s3",
            post(partition_sync_to_s3_internal),
        )
        .route("/v1/internal/replicate", post(apply_replication))
        .route("/v1/internal/install-snapshot", post(install_snapshot))
        .route("/v1/internal/log-range", post(replication_log_range))
        .route("/v1/force-rebuild", post(force_rebuild))
        .route("/v1/sync-to-s3", post(sync_to_s3))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;

    println!(
        "vectdb-deploy node_id={} listen={} shards={} logical_partitions={} replication_factor={} local_partitions={} db_root={} etcd_endpoints={} election_key={} cluster_nodes={}",
        args.node_id,
        args.listen,
        args.shards,
        topology.partition_count,
        topology.replication_factor,
        state.partitions.len(),
        args.db_root.display(),
        args.etcd_endpoints.join(","),
        args.etcd_election_key,
        cluster_endpoints.len(),
    );

    let server = axum::serve(listener, app);
    tokio::select! {
        result = server => {
            if let Err(err) = result {
                eprintln!("server error: {err}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("shutdown signal received");
        }
    }

    state.leader.release().await;
    leader_task.abort();
    let _ = leader_task.await;
    let partition_leaders: Vec<Arc<EtcdLeaderElector>> = state
        .partitions
        .values()
        .map(|partition| partition.leader.clone())
        .collect();
    for partition_leader in partition_leaders {
        partition_leader.release().await;
    }
    for task in partition_leader_tasks {
        task.abort();
        let _ = task.await;
    }
    for task in replication_worker_tasks {
        task.abort();
        let _ = task.await;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        canonicalize_http_base_url, collect_cluster_endpoints,
        collect_partition_apply_results_until_done, collect_partition_mutations,
        collect_replication_peers, collect_replication_results_until_quorum,
        decode_internal_partition_mutations_wire_request,
        decode_internal_partition_mutations_wire_response,
        decode_internal_partition_search_wire_request,
        decode_internal_partition_search_wire_response, decode_payload_frame,
        decode_replication_payload, decode_replication_payload_seq, decode_snapshot_payload,
        encode_commit_mode, encode_internal_partition_mutations_wire_request,
        encode_internal_partition_mutations_wire_response,
        encode_internal_partition_search_wire_request,
        encode_internal_partition_search_wire_response, encode_payload_frame,
        encode_replication_payload, encode_snapshot_payload, merge_neighbors_topk,
        parse_replication_reject, partition_parallelism, ApiError, ApplyMutationsResponse,
        ErrorResponse, EtcdElectionConfig, EtcdLeaderElector,
        InternalPartitionMutationsWireRequest, InternalPartitionMutationsWireResponse,
        InternalPartitionSearchWireRequest, InternalPartitionSearchWireResponse, MutationInput,
        ReplicationJournal, ReplicationPayload, ReplicationRejectResponse, ReplicationSummary,
        SearchResponse, SnapshotPayload, WireMutation, WireSnapshotRow,
        REPLICATION_PAYLOAD_SCHEMA_VERSION, SNAPSHOT_PAYLOAD_SCHEMA_VERSION,
    };
    use axum::http::StatusCode;
    use bytes::Bytes;
    use futures::stream::FuturesUnordered;
    use std::collections::BTreeMap;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::time::Duration;
    use vectdb::index::MutationCommitMode;
    use vectdb::topology::ClusterTopology;
    use vectdb::types::Neighbor;

    #[test]
    fn collect_replication_peers_dedups_and_skips_self() {
        let peers = vec![
            "http://127.0.0.1:8081/".to_string(),
            "http://127.0.0.1:8081".to_string(),
            "http://127.0.0.1:8080".to_string(),
            "http://127.0.0.1:8082/".to_string(),
        ];
        let out = collect_replication_peers("http://127.0.0.1:8080/", &peers);
        assert_eq!(
            out,
            vec![
                "http://127.0.0.1:8081".to_string(),
                "http://127.0.0.1:8082".to_string()
            ]
        );
    }

    #[test]
    fn collect_cluster_endpoints_includes_self_and_dedups() {
        let peers = vec![
            "http://127.0.0.1:8081/".to_string(),
            "http://127.0.0.1:8081".to_string(),
            "http://127.0.0.1:8080".to_string(),
            "http://127.0.0.1:8082/".to_string(),
        ];
        let out = collect_cluster_endpoints("http://127.0.0.1:8080/", &peers);
        assert_eq!(
            out,
            vec![
                "http://127.0.0.1:8080".to_string(),
                "http://127.0.0.1:8081".to_string(),
                "http://127.0.0.1:8082".to_string()
            ]
        );
    }

    #[test]
    fn canonicalize_http_base_url_accepts_http_and_https() {
        let root = canonicalize_http_base_url("http://127.0.0.1:8080/", "--self-url")
            .expect("http endpoint must pass");
        assert_eq!(root, "http://127.0.0.1:8080");
        let prefixed = canonicalize_http_base_url("https://vectdb.example:443/base/", "--self-url")
            .expect("https endpoint must pass");
        assert_eq!(prefixed, "https://vectdb.example/base");
    }

    #[test]
    fn canonicalize_http_base_url_rejects_invalid_scheme() {
        let err = canonicalize_http_base_url("ftp://127.0.0.1:8080", "--self-url")
            .expect_err("non-http scheme must fail");
        assert!(err.to_string().contains("http or https"));
    }

    #[test]
    fn canonicalize_http_base_url_rejects_query_and_fragment() {
        let query_err = canonicalize_http_base_url("http://127.0.0.1:8080?a=1", "--self-url")
            .expect_err("query string must fail");
        assert!(query_err.to_string().contains("query string"));

        let fragment_err = canonicalize_http_base_url("http://127.0.0.1:8080#frag", "--self-url")
            .expect_err("fragment must fail");
        assert!(fragment_err.to_string().contains("fragment"));
    }

    #[test]
    fn etcd_elector_rejects_empty_leader_endpoint_value() {
        let cfg = EtcdElectionConfig {
            endpoints: vec!["http://127.0.0.1:2379".to_string()],
            election_key: "/vectdb/prod/leader".to_string(),
            leader_endpoint_key: "/vectdb/prod/leader_endpoint".to_string(),
            leader_endpoint_value: String::new(),
            lease_ttl_secs: 5,
            retry_backoff: Duration::from_millis(500),
            connect_timeout: Duration::from_millis(1_000),
            request_timeout: Duration::from_millis(1_000),
            username: None,
            password: None,
        };
        let err = EtcdLeaderElector::new("node-a".to_string(), cfg)
            .expect_err("empty leader endpoint must fail");
        assert!(err
            .to_string()
            .contains("leader endpoint value must not be empty"));
    }

    async fn delayed_result(ms: u64, ok: bool) -> Result<(), String> {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        if ok {
            Ok(())
        } else {
            Err("replication failed".to_string())
        }
    }

    async fn delayed_apply_result(
        ms: u64,
        result: Result<ApplyMutationsResponse, ApiError>,
    ) -> Result<ApplyMutationsResponse, ApiError> {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        result
    }

    fn sample_apply_response(
        upserts: usize,
        deletes: usize,
        attempted: usize,
        succeeded: usize,
        failed: &[&str],
    ) -> ApplyMutationsResponse {
        ApplyMutationsResponse {
            applied_upserts: upserts,
            applied_deletes: deletes,
            replicated: ReplicationSummary {
                attempted,
                succeeded,
                failed: failed.iter().map(|entry| (*entry).to_string()).collect(),
            },
        }
    }

    #[tokio::test]
    async fn collect_replication_results_stops_after_quorum() {
        let started = std::time::Instant::now();
        let futures = vec![
            delayed_result(10, true),
            delayed_result(20, true),
            delayed_result(300, true),
        ];
        let summary = collect_replication_results_until_quorum(futures, 3, 2).await;
        assert_eq!(summary.attempted, 3);
        assert_eq!(summary.succeeded, 2);
        assert!(summary.failed.is_empty());
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "quorum collection should stop before slow tail completes"
        );
    }

    #[tokio::test]
    async fn collect_replication_results_records_failures_when_quorum_missed() {
        let futures = vec![
            delayed_result(1, false),
            delayed_result(2, true),
            delayed_result(3, false),
        ];
        let summary = collect_replication_results_until_quorum(futures, 3, 2).await;
        assert_eq!(summary.attempted, 3);
        assert_eq!(summary.succeeded, 1);
        assert_eq!(summary.failed.len(), 2);
    }

    #[tokio::test]
    async fn collect_partition_apply_results_sums_successes() {
        let mut futures = FuturesUnordered::new();
        futures.push(Box::pin(delayed_apply_result(
            15,
            Ok(sample_apply_response(2, 1, 3, 3, &[])),
        )));
        futures.push(Box::pin(delayed_apply_result(
            10,
            Ok(sample_apply_response(4, 0, 2, 2, &[])),
        )));
        futures.push(Box::pin(delayed_apply_result(
            5,
            Ok(sample_apply_response(1, 3, 2, 1, &["peer-x"])),
        )));
        let out = collect_partition_apply_results_until_done(&mut futures)
            .await
            .expect("aggregate apply result");
        assert_eq!(out.applied_upserts, 7);
        assert_eq!(out.applied_deletes, 4);
        assert_eq!(out.replicated.attempted, 7);
        assert_eq!(out.replicated.succeeded, 6);
        assert_eq!(out.replicated.failed, vec!["peer-x".to_string()]);
    }

    #[tokio::test]
    async fn collect_partition_apply_results_waits_for_inflight_successes_before_error() {
        let started = std::time::Instant::now();
        let mut futures = FuturesUnordered::new();
        futures.push(Box::pin(delayed_apply_result(
            5,
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(ErrorResponse {
                    error: "partition unavailable".to_string(),
                    replication_reject: None,
                }),
            )),
        )));
        futures.push(Box::pin(delayed_apply_result(
            60,
            Ok(sample_apply_response(2, 0, 1, 1, &[])),
        )));
        let err = collect_partition_apply_results_until_done(&mut futures)
            .await
            .expect_err("should surface first partition error");
        assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "collector should drain all in-flight partition writes before returning"
        );
    }

    fn build_payload(seq: u64, term: u64) -> Bytes {
        let payload = ReplicationPayload {
            schema_version: REPLICATION_PAYLOAD_SCHEMA_VERSION,
            partition_id: 0,
            seq,
            term,
            commit_mode: encode_commit_mode(MutationCommitMode::Durable),
            mutations: vec![WireMutation::Delete { id: seq }],
        };
        encode_replication_payload(&payload).expect("encode payload")
    }

    fn build_snapshot_payload(applied_seq: u64, term: u64) -> Bytes {
        let payload = SnapshotPayload {
            schema_version: SNAPSHOT_PAYLOAD_SCHEMA_VERSION,
            partition_id: 0,
            applied_seq,
            term,
            rows: vec![
                WireSnapshotRow {
                    id: 1,
                    values: vec![0.1, 0.2, 0.3],
                },
                WireSnapshotRow {
                    id: 2,
                    values: vec![0.4, 0.5, 0.6],
                },
            ],
        };
        encode_snapshot_payload(&payload).expect("encode snapshot")
    }

    #[test]
    fn replication_payload_detects_checksum_corruption() {
        let encoded = build_payload(1, 10);
        let mut corrupted = encoded.to_vec();
        corrupted[12] ^= 0x55;
        let err = decode_replication_payload(&corrupted).expect_err("decode must fail");
        assert!(err.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn replication_payload_seq_decoder_roundtrip() {
        let encoded = build_payload(17, 10);
        let seq = decode_replication_payload_seq(encoded.as_ref()).expect("decode seq");
        assert_eq!(seq, 17);
    }

    #[test]
    fn payload_frame_roundtrip() {
        let entries = vec![Bytes::from_static(b"abc"), build_payload(2, 20)];
        let encoded = encode_payload_frame(&entries).expect("encode frame");
        let decoded = decode_payload_frame(encoded.as_ref()).expect("decode frame");
        assert_eq!(entries, decoded);
    }

    #[test]
    fn internal_mutations_wire_request_roundtrip() {
        let request = InternalPartitionMutationsWireRequest {
            schema_version: 1,
            partition_id: 3,
            commit_mode: encode_commit_mode(MutationCommitMode::Acknowledged),
            mutations: vec![
                WireMutation::Upsert {
                    id: 7,
                    values: vec![0.1, 0.2],
                },
                WireMutation::Delete { id: 9 },
            ],
        };
        let encoded =
            encode_internal_partition_mutations_wire_request(&request).expect("encode request");
        let decoded = decode_internal_partition_mutations_wire_request(encoded.as_ref())
            .expect("decode request");
        assert_eq!(decoded.schema_version, request.schema_version);
        assert_eq!(decoded.partition_id, request.partition_id);
        assert_eq!(decoded.commit_mode, request.commit_mode);
        assert_eq!(decoded.mutations.len(), request.mutations.len());
    }

    #[test]
    fn internal_mutations_wire_response_roundtrip() {
        let response = InternalPartitionMutationsWireResponse::from(sample_apply_response(
            5,
            2,
            4,
            3,
            &["peer-a"],
        ));
        let encoded =
            encode_internal_partition_mutations_wire_response(&response).expect("encode response");
        let decoded = decode_internal_partition_mutations_wire_response(encoded.as_ref())
            .expect("decode response");
        assert_eq!(decoded.schema_version, response.schema_version);
        assert_eq!(decoded.applied_upserts, 5);
        assert_eq!(decoded.applied_deletes, 2);
        assert_eq!(decoded.replicated.attempted, 4);
        assert_eq!(decoded.replicated.succeeded, 3);
        assert_eq!(decoded.replicated.failed, vec!["peer-a".to_string()]);
    }

    #[test]
    fn internal_search_wire_request_response_roundtrip() {
        let request = InternalPartitionSearchWireRequest {
            schema_version: 1,
            partition_id: 2,
            query: vec![0.2, 0.4, 0.6],
            k: 7,
        };
        let encoded =
            encode_internal_partition_search_wire_request(&request).expect("encode search request");
        let decoded = decode_internal_partition_search_wire_request(encoded.as_ref())
            .expect("decode search request");
        assert_eq!(decoded.schema_version, request.schema_version);
        assert_eq!(decoded.partition_id, request.partition_id);
        assert_eq!(decoded.query, request.query);
        assert_eq!(decoded.k, request.k);

        let response = InternalPartitionSearchWireResponse::from(SearchResponse {
            neighbors: vec![
                Neighbor {
                    id: 3,
                    distance: 0.1,
                },
                Neighbor {
                    id: 4,
                    distance: 0.2,
                },
            ],
        });
        let encoded = encode_internal_partition_search_wire_response(&response)
            .expect("encode search response");
        let decoded = decode_internal_partition_search_wire_response(encoded.as_ref())
            .expect("decode search response");
        assert_eq!(decoded.schema_version, response.schema_version);
        assert_eq!(decoded.neighbors.len(), 2);
        assert_eq!(decoded.neighbors[0].id, 3);
    }

    #[test]
    fn snapshot_payload_roundtrip() {
        let encoded = build_snapshot_payload(3, 7);
        let decoded = decode_snapshot_payload(encoded.as_ref()).expect("decode snapshot payload");
        assert_eq!(decoded.applied_seq, 3);
        assert_eq!(decoded.term, 7);
        assert_eq!(decoded.rows.len(), 2);
        assert_eq!(decoded.rows[0].id, 1);
        assert_eq!(decoded.rows[1].values, vec![0.4, 0.5, 0.6]);
    }

    #[test]
    fn parse_replication_reject_from_error_response() {
        let reject = ReplicationRejectResponse {
            error: "out-of-order".to_string(),
            expected_seq: 11,
            first_available_seq: 5,
            last_available_seq: 42,
            max_term_seen: 9,
        };
        let body = serde_json::to_vec(&ErrorResponse {
            error: reject.error.clone(),
            replication_reject: Some(reject.clone()),
        })
        .expect("encode error response");
        let parsed = parse_replication_reject(&body).expect("reject should parse");
        assert_eq!(parsed.expected_seq, reject.expected_seq);
        assert_eq!(parsed.first_available_seq, reject.first_available_seq);
        assert_eq!(parsed.last_available_seq, reject.last_available_seq);
        assert_eq!(parsed.max_term_seen, reject.max_term_seen);
    }

    #[test]
    fn replication_journal_persists_and_recovers() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let payload_1 = build_payload(1, 2);
        let payload_2 = build_payload(2, 3);

        {
            let mut journal = ReplicationJournal::open(root, 8).expect("open journal");
            journal
                .append_entry(1, payload_1.as_ref(), true)
                .expect("append seq 1");
            journal.mark_applied(1, 2, true).expect("mark seq 1");
            journal
                .append_entry(2, payload_2.as_ref(), true)
                .expect("append seq 2");
            journal.mark_applied(2, 3, true).expect("mark seq 2");
            assert_eq!(journal.first_seq(), 1);
            assert_eq!(journal.next_seq(), 3);
            assert_eq!(journal.last_applied_seq(), 2);
        }

        let mut reopened = ReplicationJournal::open(root, 8).expect("reopen journal");
        assert_eq!(reopened.first_seq(), 1);
        assert_eq!(reopened.next_seq(), 3);
        assert_eq!(reopened.last_applied_seq(), 2);
        assert_eq!(reopened.max_term_seen(), 3);
        let read = reopened
            .read_entry(2)
            .expect("read seq 2")
            .expect("entry exists");
        assert_eq!(read, payload_2);
    }

    #[test]
    fn replication_journal_reopen_clamps_stale_meta_ahead_of_log() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let payload = build_payload(1, 1);

        let mut journal = ReplicationJournal::open(root, 8).expect("open journal");
        journal
            .append_and_mark_applied(1, 1, payload.as_ref(), true)
            .expect("append committed entry");

        let mut meta: super::ReplicationJournalMeta =
            serde_json::from_slice(&std::fs::read(&journal.meta_path).expect("read meta"))
                .expect("decode meta");
        meta.next_seq = 99;
        meta.last_applied_seq = 88;
        std::fs::write(
            &journal.meta_path,
            serde_json::to_vec(&meta).expect("encode meta"),
        )
        .expect("write poisoned meta");
        drop(journal);

        let reopened = ReplicationJournal::open(root, 8).expect("reopen journal");
        assert_eq!(reopened.first_seq(), 1);
        assert_eq!(reopened.next_seq(), 2);
        assert_eq!(reopened.last_applied_seq(), 1);
    }

    #[test]
    fn replication_journal_detects_record_checksum_corruption() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let payload = build_payload(1, 1);

        {
            let mut journal = ReplicationJournal::open(root, 8).expect("open journal");
            journal
                .append_and_mark_applied(1, 1, payload.as_ref(), true)
                .expect("append committed entry");
        }

        let log_path = root.join("replication").join("ops.log");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&log_path)
            .expect("open log for corruption");
        file.seek(SeekFrom::Start(12))
            .expect("seek to record checksum");
        let mut checksum = [0u8; 8];
        file.read_exact(&mut checksum).expect("read checksum");
        checksum[0] ^= 0x55;
        file.seek(SeekFrom::Start(12)).expect("rewind to checksum");
        file.write_all(&checksum).expect("write corrupted checksum");
        file.flush().expect("flush corruption");
        drop(file);

        match ReplicationJournal::open(root, 8) {
            Ok(_) => panic!("corruption must fail reopen"),
            Err(err) => assert!(err.to_string().contains("checksum mismatch")),
        }
    }

    #[test]
    fn replication_journal_compacts_old_entries() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let payload_1 = build_payload(1, 1);
        let payload_2 = build_payload(2, 1);
        let payload_3 = build_payload(3, 1);

        let mut journal = ReplicationJournal::open(root, 2).expect("open journal");
        journal
            .append_entry(1, payload_1.as_ref(), true)
            .expect("append seq 1");
        journal.mark_applied(1, 1, true).expect("mark seq 1");
        journal
            .append_entry(2, payload_2.as_ref(), true)
            .expect("append seq 2");
        journal.mark_applied(2, 1, true).expect("mark seq 2");
        journal
            .append_entry(3, payload_3.as_ref(), true)
            .expect("append seq 3");
        journal.mark_applied(3, 1, true).expect("mark seq 3");

        assert_eq!(journal.first_seq(), 2);
        assert!(journal.read_entry(1).expect("read seq 1").is_none());
        assert!(journal.read_entry(2).expect("read seq 2").is_some());
        assert!(journal.read_entry(3).expect("read seq 3").is_some());
    }

    #[test]
    fn replication_journal_reset_to_applied() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let payload_1 = build_payload(1, 1);
        let payload_2 = build_payload(2, 1);

        let mut journal = ReplicationJournal::open(root, 8).expect("open journal");
        journal
            .append_entry(1, payload_1.as_ref(), true)
            .expect("append seq 1");
        journal.mark_applied(1, 1, true).expect("mark seq 1");
        journal
            .append_entry(2, payload_2.as_ref(), true)
            .expect("append seq 2");
        journal.mark_applied(2, 1, true).expect("mark seq 2");

        journal.reset_to_applied(9, 3, true).expect("reset journal");
        assert_eq!(journal.first_seq(), 10);
        assert_eq!(journal.next_seq(), 10);
        assert_eq!(journal.last_applied_seq(), 9);
        assert_eq!(journal.max_term_seen(), 3);
        assert!(journal.read_entry(1).expect("read seq 1").is_none());
        assert!(journal.read_entry(2).expect("read seq 2").is_none());
    }

    #[test]
    fn partition_parallelism_is_bounded_and_non_zero() {
        assert_eq!(partition_parallelism(0), 1);
        assert_eq!(partition_parallelism(1), 1);
        let p = partition_parallelism(10_000);
        assert!(p > 0);
        assert!(p <= 10_000);
    }

    #[test]
    fn merge_neighbors_topk_matches_sorted_baseline() {
        let neighbors = vec![
            Neighbor {
                id: 11,
                distance: 0.7,
            },
            Neighbor {
                id: 8,
                distance: 0.2,
            },
            Neighbor {
                id: 5,
                distance: 0.2,
            },
            Neighbor {
                id: 9,
                distance: 0.4,
            },
            Neighbor {
                id: 1,
                distance: 0.9,
            },
            Neighbor {
                id: 2,
                distance: 0.3,
            },
        ];

        let mut expected = neighbors.clone();
        expected.sort_by(|lhs, rhs| {
            lhs.distance
                .total_cmp(&rhs.distance)
                .then_with(|| lhs.id.cmp(&rhs.id))
        });
        expected.truncate(4);

        let actual = merge_neighbors_topk(neighbors, 4);
        assert_eq!(actual, expected);
    }

    #[test]
    fn collect_partition_mutations_groups_by_partition() {
        let topology =
            ClusterTopology::build_deterministic(1, 8, 1, &["http://node-a:8080".to_string()])
                .expect("topology");
        let grouped = collect_partition_mutations(
            &topology,
            vec![
                MutationInput::Upsert {
                    id: 1,
                    values: vec![0.1, 0.2],
                },
                MutationInput::Delete { id: 9 },
                MutationInput::Upsert {
                    id: 2,
                    values: vec![0.3, 0.4],
                },
            ],
        );
        assert_eq!(grouped.len(), 2);
        let by_partition: BTreeMap<u32, usize> = grouped
            .into_iter()
            .map(|(partition_id, mutations)| (partition_id, mutations.len()))
            .collect();
        assert_eq!(by_partition.get(&1).copied(), Some(2));
        assert_eq!(by_partition.get(&2).copied(), Some(1));
    }
}
