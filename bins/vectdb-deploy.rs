use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::Context;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use etcd_client::{
    Client as EtcdClient, Compare, CompareOp, ConnectOptions, PutOptions, Txn, TxnOp,
};
use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::header::CONTENT_TYPE;
use reqwest::Client;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Infallible, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;
use vectdb::cluster::etcd_leader_endpoint_key;
use vectdb::index::{
    MutationCommitMode, SpFreshConfig, SpFreshLayerDbConfig, SpFreshLayerDbShardedConfig,
    SpFreshLayerDbShardedIndex, SpFreshLayerDbShardedStats, SpFreshMemoryMode, VectorMutation,
    VectorMutationBatchResult,
};
use vectdb::types::{Neighbor, VectorIndex, VectorRecord};

#[derive(Debug, Parser)]
#[command(name = "vectdb-deploy")]
#[command(about = "Sharded VectDB deployment server with leader-gated writes")]
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

    #[arg(long, default_value_t = 1_500)]
    replication_timeout_ms: u64,

    #[arg(long, default_value_t = 120_000)]
    replication_snapshot_timeout_ms: u64,

    #[arg(long, default_value_t = 500_000)]
    replication_log_max_entries: usize,

    #[arg(long, default_value_t = 512)]
    replication_catchup_batch: usize,

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

const REPLICATION_JOURNAL_SCHEMA_VERSION: u32 = 1;
const REPLICATION_PAYLOAD_SCHEMA_VERSION: u32 = 1;
const SNAPSHOT_PAYLOAD_SCHEMA_VERSION: u32 = 1;
const LOG_RANGE_FRAME_MAX_ENTRIES: usize = 4096;

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
}

impl From<MutationInput> for WireMutation {
    fn from(value: MutationInput) -> Self {
        match value {
            MutationInput::Upsert { id, values } => Self::Upsert { id, values },
            MutationInput::Delete { id } => Self::Delete { id },
        }
    }
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
struct ReplicationPayload {
    schema_version: u32,
    seq: u64,
    term: u64,
    commit_mode: u8,
    mutations: Vec<WireMutation>,
}

impl ReplicationPayload {
    fn to_runtime_mutations(&self) -> Vec<VectorMutation> {
        self.mutations
            .clone()
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
    applied_seq: u64,
    term: u64,
    rows: Vec<WireSnapshotRow>,
}

impl SnapshotPayload {
    fn to_runtime_rows(&self) -> Vec<VectorRecord> {
        self.rows
            .clone()
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
            let mut raw = vec![0u8; len as usize];
            file.read_exact(&mut raw)
                .with_context(|| format!("read {} payload len={}", log_path.display(), len))?;
            let seq = decode_replication_payload_seq(&raw)?;
            index.insert(
                seq,
                JournalIndexEntry {
                    offset: offset + 4,
                    len,
                },
            );
            offset = offset.saturating_add(4 + len as u64);
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
            meta.first_seq = min_seq;
        } else {
            meta.first_seq = meta.next_seq.max(1);
        }
        if let Some((&max_seq, _)) = index.last_key_value() {
            meta.next_seq = meta.next_seq.max(max_seq.saturating_add(1));
        }
        if meta.last_applied_seq.saturating_add(1) < meta.first_seq {
            meta.last_applied_seq = meta.first_seq.saturating_sub(1);
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
        self.file
            .write_all(&len.to_le_bytes())
            .context("append replication record length")?;
        self.file
            .write_all(raw_payload)
            .context("append replication record payload")?;
        if sync {
            self.file.sync_data().context("sync replication record")?;
        }
        self.index.insert(
            seq,
            JournalIndexEntry {
                offset: record_start + 4,
                len,
            },
        );
        self.meta.next_seq = seq.saturating_add(1);
        self.meta.first_seq = self.first_seq();
        self.persist_meta(sync)?;
        self.compact_if_needed()?;
        Ok(())
    }

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
                out.push(raw);
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
            tmp.write_all(&len.to_le_bytes())
                .context("write compacted replication record length")?;
            tmp.write_all(raw.as_ref())
                .context("write compacted replication record payload")?;
            new_index.insert(
                seq,
                JournalIndexEntry {
                    offset: offset + 4,
                    len,
                },
            );
            offset = offset.saturating_add(4 + len as u64);
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

fn encode_replication_payload(payload: &ReplicationPayload) -> anyhow::Result<Bytes> {
    let payload_bytes =
        rkyv::to_bytes::<_, 4096>(payload).context("serialize replication payload")?;
    let checksum = checksum_fnv1a64(payload_bytes.as_ref());
    let mut out = Vec::with_capacity(12 + payload_bytes.len());
    out.extend_from_slice(&checksum.to_le_bytes());
    let len = u32::try_from(payload_bytes.len()).context("replication payload too large")?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(payload_bytes.as_ref());
    Ok(Bytes::from(out))
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
    let checksum = checksum_fnv1a64(payload_bytes.as_ref());
    let mut out = Vec::with_capacity(12 + payload_bytes.len());
    out.extend_from_slice(&checksum.to_le_bytes());
    let len = u32::try_from(payload_bytes.len()).context("snapshot payload too large")?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(payload_bytes.as_ref());
    Ok(Bytes::from(out))
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
    replica_peers: Vec<String>,
    quorum_size: usize,
    replication_timeout: Duration,
    replication_snapshot_timeout: Duration,
    replication_catchup_batch: usize,
    index: Arc<RwLock<SpFreshLayerDbShardedIndex>>,
    leader: Arc<EtcdLeaderElector>,
    journal: Arc<Mutex<ReplicationJournal>>,
    mutation_gate: Arc<AsyncMutex<()>>,
    http: Client,
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
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    node_id: String,
    role: String,
    leader: bool,
    stats: SpFreshLayerDbShardedStats,
    replication: ReplicationState,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MutationInput {
    Upsert { id: u64, values: Vec<f32> },
    Delete { id: u64 },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ApplyMutationsRequest {
    mutations: Vec<MutationInput>,
    #[serde(default)]
    commit_mode: Option<CommitModeArg>,
}

#[derive(Debug, Serialize)]
struct ReplicationSummary {
    attempted: usize,
    succeeded: usize,
    failed: Vec<String>,
}

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
struct SearchResponse {
    neighbors: Vec<Neighbor>,
}

#[derive(Debug, Deserialize)]
struct SyncToS3Request {
    max_files_per_shard: Option<usize>,
}

#[derive(Debug, Serialize)]
struct SyncToS3Response {
    moved_files: usize,
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

fn ensure_leader(state: &AppState) -> Result<(), ApiError> {
    if state.leader.is_leader() {
        return Ok(());
    }
    Err(api_error(
        StatusCode::CONFLICT,
        "write rejected: this node is not leader",
    ))
}

async fn role(State(state): State<Arc<AppState>>) -> ApiResult<RoleResponse> {
    let replication = {
        let journal = state
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        journal.snapshot_state()
    };
    Ok(Json(RoleResponse {
        node_id: state.node_id.clone(),
        role: state.leader.role().to_string(),
        leader: state.leader.is_leader(),
        replication,
    }))
}

async fn health(State(state): State<Arc<AppState>>) -> ApiResult<HealthResponse> {
    let index = state.index.clone();
    let stats = tokio::task::spawn_blocking(move || {
        let index = index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        index.health_check()
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

    let replication = {
        let journal = state
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        journal.snapshot_state()
    };

    Ok(Json(HealthResponse {
        node_id: state.node_id.clone(),
        role: state.leader.role().to_string(),
        leader: state.leader.is_leader(),
        stats,
        replication,
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
    let index = state.index.clone();
    let neighbors = tokio::task::spawn_blocking(move || {
        let index = index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        Ok::<_, anyhow::Error>(index.search(&query, k))
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join search task: {err}"),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })?;

    Ok(Json(SearchResponse { neighbors }))
}

async fn force_rebuild(State(state): State<Arc<AppState>>) -> ApiResult<serde_json::Value> {
    ensure_leader(&state)?;

    let index = state.index.clone();
    tokio::task::spawn_blocking(move || {
        let index = index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        index.force_rebuild()
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join rebuild task: {err}"),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })?;

    Ok(Json(serde_json::json!({"ok": true})))
}

async fn sync_to_s3(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncToS3Request>,
) -> ApiResult<SyncToS3Response> {
    ensure_leader(&state)?;

    let index = state.index.clone();
    let moved = tokio::task::spawn_blocking(move || {
        let index = index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        index.sync_to_s3(req.max_files_per_shard)
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join sync task: {err}"),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })?;

    Ok(Json(SyncToS3Response { moved_files: moved }))
}

async fn apply_mutations(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ApplyMutationsRequest>,
) -> ApiResult<ApplyMutationsResponse> {
    ensure_leader(&state)?;
    if req.mutations.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "mutations must not be empty",
        ));
    }

    let _gate = state.mutation_gate.lock().await;
    let mode = req.commit_mode.unwrap_or(CommitModeArg::Durable).into();
    let term = state.leader.current_term();
    if term == 0 {
        return Err(api_error(
            StatusCode::CONFLICT,
            "write rejected: leader term unavailable",
        ));
    }
    let seq = {
        let journal = state
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        journal.next_seq()
    };
    let payload = ReplicationPayload {
        schema_version: REPLICATION_PAYLOAD_SCHEMA_VERSION,
        seq,
        term,
        commit_mode: encode_commit_mode(mode),
        mutations: req.mutations.into_iter().map(WireMutation::from).collect(),
    };
    let encoded = encode_replication_payload(&payload).map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode replication payload: {err:#}"),
        )
    })?;

    let applied =
        apply_local_mutations_runtime(state.clone(), payload.to_runtime_mutations(), mode).await?;
    let sync_journal = matches!(mode, MutationCommitMode::Durable);
    {
        let mut journal = state
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        journal
            .append_entry(seq, encoded.as_ref(), sync_journal)
            .map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("append replication journal seq={seq}: {err:#}"),
                )
            })?;
        journal
            .mark_applied(seq, term, sync_journal)
            .map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("mark replication journal seq={seq} applied: {err:#}"),
                )
            })?;
    }

    let peers = state.replica_peers.clone();
    let replicated = if matches!(mode, MutationCommitMode::Durable) {
        let follower_quorum = state.quorum_size.saturating_sub(1);
        let replicated = replicate_to_followers_until_quorum(
            state.clone(),
            peers.clone(),
            seq,
            term,
            encoded.clone(),
            follower_quorum,
        )
        .await;
        if replicated.succeeded < replicated.attempted {
            let tail_state = state.clone();
            let tail_payload = encoded.clone();
            tokio::spawn(async move {
                let _ = replicate_to_followers(tail_state, peers, seq, term, tail_payload).await;
            });
        }
        replicated
    } else {
        replicate_to_followers(state.clone(), peers, seq, term, encoded.clone()).await
    };

    if matches!(mode, MutationCommitMode::Durable) {
        let quorum_acks = 1usize.saturating_add(replicated.succeeded);
        if quorum_acks < state.quorum_size {
            return Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                format!(
                    "durable quorum not reached: quorum={} acks={} attempted_replicas={} failures={}",
                    state.quorum_size,
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
    body: Bytes,
) -> ApiResult<ApplyMutationsResponse> {
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
    let _gate = state.mutation_gate.lock().await;
    {
        let mut journal = state
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        let expected = journal.expected_next_seq();
        if payload.term < journal.max_term_seen() {
            return Err(replication_reject_error(
                format!(
                    "stale term {} < max_term_seen {}",
                    payload.term,
                    journal.max_term_seen()
                ),
                &journal,
            ));
        }
        if payload.seq < expected {
            if let Some(existing) = journal.read_entry(payload.seq).map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("read replication seq={} from journal: {err:#}", payload.seq),
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
                format!(
                    "duplicate or stale seq {} expected {}",
                    payload.seq, expected
                ),
                &journal,
            ));
        }
        if payload.seq > expected {
            return Err(replication_reject_error(
                format!("out-of-order seq {} expected {}", payload.seq, expected),
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
    let applied =
        apply_local_mutations_runtime(state.clone(), payload.to_runtime_mutations(), mode).await?;
    let sync_journal = matches!(mode, MutationCommitMode::Durable);
    {
        let mut journal = state
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        journal
            .append_entry(payload.seq, body.as_ref(), sync_journal)
            .map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("append replication seq={}: {err:#}", payload.seq),
                )
            })?;
        journal
            .mark_applied(payload.seq, payload.term, sync_journal)
            .map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("mark replication seq={} applied: {err:#}", payload.seq),
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
    body: Bytes,
) -> ApiResult<SnapshotInstallResponse> {
    let payload = decode_snapshot_payload(body.as_ref()).map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("decode snapshot payload: {err:#}"),
        )
    })?;
    let rows = payload.to_runtime_rows();
    let row_count = rows.len();
    let _gate = state.mutation_gate.lock().await;
    {
        let journal = state
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        if payload.term < journal.max_term_seen() {
            return Err(replication_reject_error(
                format!(
                    "stale snapshot term {} < max_term_seen {}",
                    payload.term,
                    journal.max_term_seen()
                ),
                &journal,
            ));
        }
    }

    let index = state.index.clone();
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
        let mut journal = state
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        journal
            .reset_to_applied(payload.applied_seq, payload.term, true)
            .map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "reset replication journal to seq={}: {err:#}",
                        payload.applied_seq
                    ),
                )
            })?;
    }

    Ok(Json(SnapshotInstallResponse {
        applied_seq: payload.applied_seq,
        rows: row_count,
    }))
}

async fn replication_log_range(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LogRangeRequest>,
) -> Result<Bytes, ApiError> {
    if req.max_entries == 0 {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "max_entries must be > 0",
        ));
    }

    let entries = {
        let mut journal = state
            .journal
            .lock()
            .map_err(|_| api_error(StatusCode::INTERNAL_SERVER_ERROR, "journal lock poisoned"))?;
        journal
            .read_range(req.from_seq, req.max_entries)
            .map_err(|err| {
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "read replication log range from_seq={}: {err:#}",
                        req.from_seq
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
    state: Arc<AppState>,
    mutations: Vec<VectorMutation>,
    mode: MutationCommitMode,
) -> Result<VectorMutationBatchResult, ApiError> {
    let index = state.index.clone();

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
    payload: Bytes,
) -> Result<(), ReplicationSendError> {
    let response = client
        .post(endpoint)
        .timeout(timeout)
        .header(CONTENT_TYPE, "application/octet-stream")
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
    state: Arc<AppState>,
    applied_seq: u64,
    term: u64,
) -> Result<Bytes, String> {
    let index = state.index.clone();
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
        applied_seq,
        term,
        rows: rows.into_iter().map(WireSnapshotRow::from).collect(),
    };
    encode_snapshot_payload(&payload).map_err(|err| format!("encode snapshot payload: {err:#}"))
}

async fn install_snapshot_on_peer(
    state: Arc<AppState>,
    peer: &str,
    applied_seq: u64,
    leader_term: u64,
) -> Result<(), String> {
    let endpoint = join_base_url(peer, "/v1/internal/install-snapshot");
    let payload = build_snapshot_payload(state.clone(), applied_seq, leader_term).await?;
    let response = state
        .http
        .post(&endpoint)
        .timeout(state.replication_snapshot_timeout)
        .header(CONTENT_TYPE, "application/octet-stream")
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
            let mut journal = state
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
                .read_range(next_seq, state.replication_catchup_batch)
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
        for raw in entries {
            let entry_seq = decode_replication_payload_seq(raw.as_ref()).map_err(|err| {
                format!("{peer}: decode local replication seq during catch-up: {err:#}")
            })?;
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
    peer: String,
    seq: u64,
    leader_term: u64,
    payload: Bytes,
) -> Result<(), String> {
    let endpoint = join_base_url(&peer, "/v1/internal/replicate");
    match send_replication_payload(&state.http, &endpoint, state.replication_timeout, payload).await
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
                let journal = state
                    .journal
                    .lock()
                    .map_err(|_| format!("{peer}: journal lock poisoned"))?;
                journal.first_seq()
            };
            if reject.expected_seq < local_first_seq {
                install_snapshot_on_peer(state.clone(), &peer, seq, leader_term).await?;
                return Ok(());
            }
            catch_up_peer(
                state.clone(),
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
    peers: Vec<String>,
    seq: u64,
    leader_term: u64,
    payload: Bytes,
) -> ReplicationSummary {
    if peers.is_empty() {
        return ReplicationSummary {
            attempted: 0,
            succeeded: 0,
            failed: Vec::new(),
        };
    }

    let attempted = peers.len();
    let futures = peers.iter().map(|peer| {
        replicate_to_peer(
            state.clone(),
            peer.clone(),
            seq,
            leader_term,
            payload.clone(),
        )
    });
    collect_replication_results_until_quorum(futures, attempted, attempted).await
}

async fn replicate_to_followers_until_quorum(
    state: Arc<AppState>,
    peers: Vec<String>,
    seq: u64,
    leader_term: u64,
    payload: Bytes,
    required_successes: usize,
) -> ReplicationSummary {
    if peers.is_empty() {
        return ReplicationSummary {
            attempted: 0,
            succeeded: 0,
            failed: Vec::new(),
        };
    }

    let attempted = peers.len();
    let required = required_successes.min(attempted);
    let futures = peers.iter().map(|peer| {
        replicate_to_peer(
            state.clone(),
            peer.clone(),
            seq,
            leader_term,
            payload.clone(),
        )
    });
    collect_replication_results_until_quorum(futures, attempted, required).await
}

fn build_index(args: &Args) -> anyhow::Result<SpFreshLayerDbShardedIndex> {
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

    SpFreshLayerDbShardedIndex::open(&args.db_root, cfg)
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
    if args.diskmeta_probe_multiplier == 0 {
        anyhow::bail!("--diskmeta-probe-multiplier must be > 0");
    }
    let self_url = canonicalize_http_base_url(args.self_url.as_str(), "--self-url")?;
    let mut replica_urls = Vec::with_capacity(args.replica_urls.len());
    for replica in &args.replica_urls {
        replica_urls.push(canonicalize_http_base_url(replica, "--replica-urls")?);
    }

    let index = build_index(&args).context("open sharded index")?;
    let journal = ReplicationJournal::open(&args.db_root, args.replication_log_max_entries)
        .context("open replication journal")?;

    let election_cfg = EtcdElectionConfig {
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
    let leader = Arc::new(
        EtcdLeaderElector::new(args.node_id.clone(), election_cfg).context("init etcd election")?,
    );
    let leader_task = tokio::spawn(leader.clone().run());
    let replica_peers = collect_replication_peers(self_url.as_str(), &replica_urls);
    let cluster_size = 1usize.saturating_add(replica_peers.len());
    let quorum_size = cluster_size / 2 + 1;
    let http = build_http_client()?;

    let state = Arc::new(AppState {
        node_id: args.node_id.clone(),
        replica_peers: replica_peers.clone(),
        quorum_size,
        replication_timeout: Duration::from_millis(args.replication_timeout_ms.max(100)),
        replication_snapshot_timeout: Duration::from_millis(
            args.replication_snapshot_timeout_ms.max(1_000),
        ),
        replication_catchup_batch: args.replication_catchup_batch,
        index: Arc::new(RwLock::new(index)),
        leader,
        journal: Arc::new(Mutex::new(journal)),
        mutation_gate: Arc::new(AsyncMutex::new(())),
        http,
    });

    let app = Router::new()
        .route("/v1/role", get(role))
        .route("/v1/health", get(health))
        .route("/v1/search", post(search))
        .route("/v1/mutations", post(apply_mutations))
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
        "vectdb-deploy node_id={} listen={} shards={} db_root={} etcd_endpoints={} election_key={} replicas={}",
        args.node_id,
        args.listen,
        args.shards,
        args.db_root.display(),
        args.etcd_endpoints.join(","),
        args.etcd_election_key,
        replica_peers.len(),
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        canonicalize_http_base_url, collect_replication_peers,
        collect_replication_results_until_quorum, decode_payload_frame, decode_replication_payload,
        decode_replication_payload_seq, decode_snapshot_payload, encode_commit_mode,
        encode_payload_frame, encode_replication_payload, encode_snapshot_payload,
        parse_replication_reject, ErrorResponse, EtcdElectionConfig, EtcdLeaderElector,
        ReplicationJournal, ReplicationPayload, ReplicationRejectResponse, SnapshotPayload,
        WireMutation, WireSnapshotRow, REPLICATION_PAYLOAD_SCHEMA_VERSION,
        SNAPSHOT_PAYLOAD_SCHEMA_VERSION,
    };
    use bytes::Bytes;
    use std::time::Duration;
    use vectdb::index::MutationCommitMode;

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

    fn build_payload(seq: u64, term: u64) -> Bytes {
        let payload = ReplicationPayload {
            schema_version: REPLICATION_PAYLOAD_SCHEMA_VERSION,
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
}
