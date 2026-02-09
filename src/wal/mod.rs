use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::db::{DbOptions, Op, OpKind, WriteOptions};
use crate::memtable::MemTableManager;
use crate::version::VersionSet;

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("wal is closed")]
    Closed,

    #[error("wal record too large")]
    RecordTooLarge,
}

/// WAL placeholder.
///
/// v1 will implement a segmented append log with record framing:
/// `[len][crc32c][seqno_base][count][ops..]`.
#[derive(Debug)]
pub struct Wal {
    tx: mpsc::UnboundedSender<WalRequest>,
    flush_tx: mpsc::UnboundedSender<FlushSignal>,
    _next_seqno: Arc<AtomicU64>,
    last_durable_seqno: Arc<AtomicU64>,
    wal_thread: Option<thread::JoinHandle<()>>,
    flush_thread: Option<thread::JoinHandle<()>>,
}

#[derive(Debug)]
struct WalRequest {
    ops: Vec<Op>,
    opts: WriteOptions,
    rotate_only: bool,
    done: oneshot::Sender<anyhow::Result<()>>,
}

#[derive(Debug)]
struct WalState {
    dir: PathBuf,
    options: DbOptions,
    memtables: Arc<MemTableManager>,
    versions: Arc<VersionSet>,
    next_seqno: Arc<AtomicU64>,
    last_durable_seqno: Arc<AtomicU64>,
    segment_id: u64,
    segment: std::fs::File,
    segment_bytes: u64,
    flush_tx: mpsc::UnboundedSender<FlushSignal>,
}

const WAL_RECORD_HEADER_BYTES: usize = 4 + 4 + 8 + 4;

#[repr(u8)]
enum WalOpKind {
    Put = 1,
    Del = 2,
    RangeDel = 3,
}

impl Wal {
    pub(crate) fn open(
        dir: &Path,
        options: &DbOptions,
        memtables: Arc<MemTableManager>,
        versions: Arc<VersionSet>,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir.join("wal"))?;

        let (flush_tx, flush_rx) = mpsc::unbounded_channel();
        let flush_tx_for_wal = flush_tx.clone();
        let flush_state = FlushState {
            dir: dir.to_path_buf(),
            memtables: memtables.clone(),
            versions: versions.clone(),
        };
        let flush_thread = thread::Builder::new()
            .name("layerdb-flush".to_string())
            .spawn(move || flush_thread_main(flush_state, flush_rx))
            .expect("spawn flush thread");

        let next_seqno = Arc::new(AtomicU64::new(1));
        let last_durable_seqno = Arc::new(AtomicU64::new(0));
        let snapshot_tracker = versions.snapshots_handle();

        let recovered = recover_from_wal(&dir.join("wal"), options, &memtables, &snapshot_tracker)?;
        let _ = flush_tx.send(FlushSignal::Kick);
        let base_seqno = versions.latest_seqno().saturating_add(1);
        let next_seqno_value = recovered.next_seqno.max(base_seqno).max(1);
        let last_durable = recovered
            .last_durable_seqno
            .max(base_seqno.saturating_sub(1));

        next_seqno.store(next_seqno_value, Ordering::Relaxed);
        last_durable_seqno.store(last_durable, Ordering::Relaxed);
        snapshot_tracker.set_latest_seqno(last_durable);

        let (tx, rx) = mpsc::unbounded_channel();

        let mut state = WalState::open_new_segment(
            dir.to_path_buf(),
            options.clone(),
            memtables,
            versions,
            next_seqno.clone(),
            last_durable_seqno.clone(),
            flush_tx_for_wal,
            recovered.last_segment_id + 1,
        )?;

        let wal_thread = thread::Builder::new()
            .name("layerdb-wal".to_string())
            .spawn(move || wal_thread_main(&mut state, rx))
            .expect("spawn wal thread");

        Ok(Self {
            tx,
            flush_tx,
            _next_seqno: next_seqno,
            last_durable_seqno,
            wal_thread: Some(wal_thread),
            flush_thread: Some(flush_thread),
        })
    }

    pub fn write_batch(&self, ops: &[Op], opts: WriteOptions) -> anyhow::Result<()> {
        self.send_request(ops.to_vec(), opts, false)
    }

    fn send_request(
        &self,
        ops: Vec<Op>,
        opts: WriteOptions,
        rotate_only: bool,
    ) -> anyhow::Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(WalRequest {
                ops,
                opts,
                rotate_only,
                done: done_tx,
            })
            .map_err(|_| WalError::Closed)?;
        done_rx.blocking_recv().map_err(|_| WalError::Closed)??;
        Ok(())
    }

    pub fn last_durable_seqno(&self) -> u64 {
        self.last_durable_seqno.load(Ordering::Relaxed)
    }

    pub(crate) fn force_rotate_for_flush(&self) -> anyhow::Result<()> {
        self.send_request(Vec::new(), WriteOptions { sync: true }, true)?;

        let (done_tx, done_rx) = oneshot::channel();
        self.flush_tx
            .send(FlushSignal::Barrier(done_tx))
            .map_err(|_| WalError::Closed)?;
        done_rx.blocking_recv().map_err(|_| WalError::Closed)?;
        Ok(())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        let (dummy_tx, _dummy_rx) = mpsc::unbounded_channel();
        let old_tx = std::mem::replace(&mut self.tx, dummy_tx);
        drop(old_tx);

        let (dummy_flush_tx, _dummy_flush_rx) = mpsc::unbounded_channel();
        let old_flush_tx = std::mem::replace(&mut self.flush_tx, dummy_flush_tx);
        drop(old_flush_tx);

        if let Some(handle) = self.wal_thread.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.flush_thread.take() {
            let _ = handle.join();
        }
    }
}

struct RecoveredWal {
    next_seqno: u64,
    last_durable_seqno: u64,
    last_segment_id: u64,
}

enum FlushSignal {
    Kick,
    Barrier(oneshot::Sender<()>),
}

#[derive(Debug, Clone)]
struct FlushState {
    dir: PathBuf,
    memtables: Arc<MemTableManager>,
    versions: Arc<VersionSet>,
}

fn wal_thread_main(state: &mut WalState, mut rx: mpsc::UnboundedReceiver<WalRequest>) {
    while let Some(req) = rx.blocking_recv() {
        let result = state.handle_request(req.ops, req.opts, req.rotate_only);
        let _ = req.done.send(result);
    }
}

fn flush_thread_main(mut state: FlushState, mut rx: mpsc::UnboundedReceiver<FlushSignal>) {
    while let Some(signal) = rx.blocking_recv() {
        flush_all(&mut state);

        if let FlushSignal::Barrier(done) = signal {
            let _ = done.send(());
        }
    }

    flush_all(&mut state);
}

fn flush_all(state: &mut FlushState) {
    loop {
        let mem = match state.memtables.oldest_immutable() {
            None => break,
            Some(m) => m,
        };

        match flush_one(state, &mem) {
            Ok(()) => {
                // Drop the memtable only after a successful flush.
                let _ = state
                    .memtables
                    .drop_oldest_immutable_if_segment_id(mem.wal_segment_id);
                maybe_delete_wal_segment(&state.dir, mem.wal_segment_id);
            }
            Err(e) => {
                eprintln!("layerdb: flush failed: {e:?}");
                break;
            }
        }
    }
}

fn flush_one(state: &mut FlushState, mem: &crate::memtable::MemTable) -> anyhow::Result<()> {
    let entries = mem.to_sorted_entries();
    if entries.is_empty() {
        return Ok(());
    }

    let file_id = state.versions.allocate_file_id();
    let sst_dir = state.dir.join("sst");
    let mut builder = crate::sst::SstBuilder::create(&sst_dir, file_id, 64 * 1024)?;
    for (key, value) in &entries {
        builder.add(key, value.as_ref())?;
    }
    let props = builder.finish()?;
    state.versions.install_sst(file_id, &props)?;
    Ok(())
}

fn maybe_delete_wal_segment(dir: &Path, segment_id: u64) {
    let wal_path = dir.join("wal").join(format!("wal_{segment_id:016x}.log"));
    let _ = std::fs::remove_file(wal_path);
}

impl WalState {
    fn open_new_segment(
        dir: PathBuf,
        options: DbOptions,
        memtables: Arc<MemTableManager>,
        versions: Arc<VersionSet>,
        next_seqno: Arc<AtomicU64>,
        last_durable_seqno: Arc<AtomicU64>,
        flush_tx: mpsc::UnboundedSender<FlushSignal>,
        segment_id: u64,
    ) -> anyhow::Result<Self> {
        let wal_dir = dir.join("wal");
        std::fs::create_dir_all(&wal_dir)?;
        let segment_path = wal_dir.join(format!("wal_{segment_id:016x}.log"));
        let segment = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&segment_path)?;
        let segment_bytes = segment.metadata().map(|m| m.len()).unwrap_or(0);
        sync_dir(&wal_dir)?;
        Ok(Self {
            dir,
            options,
            memtables,
            versions,
            next_seqno,
            last_durable_seqno,
            segment_id,
            segment,
            segment_bytes,
            flush_tx,
        })
    }

    fn rotate_segment_if_needed(&mut self, additional: u64) -> anyhow::Result<()> {
        if self.segment_bytes + additional <= self.options.wal_segment_bytes {
            return Ok(());
        }

        self.rotate_segment()
    }

    fn rotate_segment(&mut self) -> anyhow::Result<()> {
        self.segment.sync_data()?;
        self.segment_id += 1;

        let wal_dir = self.dir.join("wal");
        let segment_path = wal_dir.join(format!("wal_{:016x}.log", self.segment_id));
        self.segment = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&segment_path)?;
        self.segment_bytes = self.segment.metadata().map(|m| m.len()).unwrap_or(0);
        sync_dir(&wal_dir)?;

        self.memtables.rotate_memtable(self.segment_id);
        let _ = self.flush_tx.send(FlushSignal::Kick);
        Ok(())
    }

    fn handle_request(
        &mut self,
        ops: Vec<Op>,
        opts: WriteOptions,
        rotate_only: bool,
    ) -> anyhow::Result<()> {
        if rotate_only {
            return self.rotate_segment();
        }

        if ops.is_empty() {
            return Ok(());
        }

        let seqno_base = self
            .next_seqno
            .fetch_add(ops.len() as u64, Ordering::Relaxed);

        let record = encode_wal_record(seqno_base, &ops)?;
        self.rotate_segment_if_needed(record.len() as u64)?;

        use std::io::Write;
        self.segment.write_all(&record)?;
        self.segment_bytes += record.len() as u64;

        let do_sync = opts.sync || self.options.fsync_writes;
        if do_sync {
            self.segment.sync_data()?;
        }

        self.memtables.apply_batch(seqno_base, &ops)?;
        let last_seqno = seqno_base + ops.len() as u64 - 1;
        self.versions.snapshots().set_latest_seqno(last_seqno);
        if do_sync {
            self.last_durable_seqno.store(last_seqno, Ordering::Relaxed);
        }

        if self.memtables.mutable_approximate_bytes() > self.options.memtable_bytes {
            self.rotate_segment()?;
        }
        Ok(())
    }
}

fn sync_dir(path: &Path) -> anyhow::Result<()> {
    let dir_fd = std::fs::File::open(path)?;
    dir_fd.sync_all()?;
    Ok(())
}

fn encode_wal_record(seqno_base: u64, ops: &[Op]) -> anyhow::Result<Vec<u8>> {
    let mut ops_bytes = Vec::new();
    for op in ops {
        let kind = match op.kind {
            OpKind::Put => WalOpKind::Put as u8,
            OpKind::Del => WalOpKind::Del as u8,
            OpKind::RangeDel => WalOpKind::RangeDel as u8,
        };
        let key_len: u32 = op
            .key
            .len()
            .try_into()
            .map_err(|_| WalError::RecordTooLarge)?;
        ops_bytes.push(kind);
        ops_bytes.extend_from_slice(&key_len.to_le_bytes());
        ops_bytes.extend_from_slice(op.key.as_ref());
        match op.kind {
            OpKind::Put | OpKind::RangeDel => {
                let val_len: u32 = op
                    .value
                    .len()
                    .try_into()
                    .map_err(|_| WalError::RecordTooLarge)?;
                ops_bytes.extend_from_slice(&val_len.to_le_bytes());
                ops_bytes.extend_from_slice(op.value.as_ref());
            }
            OpKind::Del => {
                ops_bytes.extend_from_slice(&0u32.to_le_bytes());
            }
        }
    }

    let count: u32 = ops.len().try_into().map_err(|_| WalError::RecordTooLarge)?;
    let len: u32 = (8 + 4 + ops_bytes.len())
        .try_into()
        .map_err(|_| WalError::RecordTooLarge)?;
    let mut buf = Vec::with_capacity(WAL_RECORD_HEADER_BYTES + ops_bytes.len());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(&seqno_base.to_le_bytes());
    buf.extend_from_slice(&count.to_le_bytes());
    buf.extend_from_slice(&ops_bytes);

    let crc = crc32c::crc32c(&buf[(4 + 4)..]);
    buf[4..8].copy_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

fn recover_from_wal(
    dir: &Path,
    _options: &DbOptions,
    memtables: &MemTableManager,
    snapshots: &crate::db::snapshot::SnapshotTracker,
) -> anyhow::Result<RecoveredWal> {
    let mut entries: Vec<(u64, PathBuf)> = Vec::new();
    if dir.exists() {
        for item in std::fs::read_dir(dir)? {
            let item = item?;
            let path = item.path();
            if !path.is_file() {
                continue;
            }
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => continue,
            };
            if let Some(id) = parse_wal_segment_id(name) {
                entries.push((id, path));
            }
        }
    }
    entries.sort_by_key(|(id, _)| *id);

    let mut next_seqno = 1u64;
    let mut last_durable = 0u64;
    let last_segment_id = entries.last().map(|(id, _)| *id).unwrap_or(0);

    if let Some((first_id, _)) = entries.first() {
        memtables.reset_for_wal_recovery(*first_id);
    }

    for (idx, (segment_id, path)) in entries.iter().enumerate() {
        let data = std::fs::read(path)?;
        let mut offset = 0usize;
        while offset + WAL_RECORD_HEADER_BYTES <= data.len() {
            let len = u32::from_le_bytes(data[offset..(offset + 4)].try_into().unwrap()) as usize;
            let crc_expected =
                u32::from_le_bytes(data[(offset + 4)..(offset + 8)].try_into().unwrap());
            let start = offset + 8;
            let end = start + len;
            if end > data.len() {
                break;
            }
            let crc_actual = crc32c::crc32c(&data[start..end]);
            if crc_actual != crc_expected {
                break;
            }

            let (seqno_base, ops) = decode_wal_payload(&data[start..end])?;
            memtables.apply_batch(seqno_base, &ops)?;

            let record_last = seqno_base + ops.len() as u64 - 1;
            last_durable = last_durable.max(record_last);
            next_seqno = next_seqno.max(record_last + 1);
            offset = end;
        }

        snapshots.set_latest_seqno(last_durable);

        // Rotate between WAL segments so each segment maps to a single immutable memtable.
        // This allows flushing + WAL garbage collection to operate safely.
        let next_id = entries
            .get(idx + 1)
            .map(|(id, _)| *id)
            .unwrap_or(segment_id.saturating_add(1));
        memtables.rotate_memtable(next_id);
    }

    Ok(RecoveredWal {
        next_seqno,
        last_durable_seqno: last_durable,
        last_segment_id,
    })
}

fn decode_wal_payload(payload: &[u8]) -> anyhow::Result<(u64, Vec<Op>)> {
    if payload.len() < 8 + 4 {
        anyhow::bail!("truncated wal payload");
    }
    let seqno_base = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let count = u32::from_le_bytes(payload[8..12].try_into().unwrap()) as usize;
    let mut offset = 12usize;
    let mut ops = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 1 + 4 > payload.len() {
            anyhow::bail!("truncated wal op");
        }
        let kind = payload[offset];
        offset += 1;
        let key_len =
            u32::from_le_bytes(payload[offset..(offset + 4)].try_into().unwrap()) as usize;
        offset += 4;
        if offset + key_len + 4 > payload.len() {
            anyhow::bail!("truncated wal key");
        }
        let key = Bytes::copy_from_slice(&payload[offset..(offset + key_len)]);
        offset += key_len;
        let val_len =
            u32::from_le_bytes(payload[offset..(offset + 4)].try_into().unwrap()) as usize;
        offset += 4;

        let op_kind = match kind {
            1 => OpKind::Put,
            2 => OpKind::Del,
            3 => OpKind::RangeDel,
            other => anyhow::bail!("unknown wal op kind {other}"),
        };
        let value = match op_kind {
            OpKind::Put | OpKind::RangeDel => {
                if offset + val_len > payload.len() {
                    anyhow::bail!("truncated wal value");
                }
                let v = Bytes::copy_from_slice(&payload[offset..(offset + val_len)]);
                offset += val_len;
                v
            }
            OpKind::Del => Bytes::new(),
        };
        ops.push(Op {
            kind: op_kind,
            key,
            value,
        });
    }
    Ok((seqno_base, ops))
}

fn parse_wal_segment_id(name: &str) -> Option<u64> {
    if !name.starts_with("wal_") || !name.ends_with(".log") {
        return None;
    }
    let inner = &name[4..(name.len() - 4)];
    u64::from_str_radix(inner, 16).ok()
}
