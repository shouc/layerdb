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
    _next_seqno: Arc<AtomicU64>,
    last_durable_seqno: Arc<AtomicU64>,
}

#[derive(Debug)]
struct WalRequest {
    ops: Vec<Op>,
    opts: WriteOptions,
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
}

const WAL_RECORD_HEADER_BYTES: usize = 4 + 4 + 8 + 4;

#[repr(u8)]
enum WalOpKind {
    Put = 1,
    Del = 2,
}

impl Wal {
    pub fn open(
        dir: &Path,
        options: &DbOptions,
        memtables: Arc<MemTableManager>,
        versions: Arc<VersionSet>,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir.join("wal"))?;

        let next_seqno = Arc::new(AtomicU64::new(1));
        let last_durable_seqno = Arc::new(AtomicU64::new(0));
        let snapshot_tracker = versions.snapshots_handle();

        let recovered = recover_from_wal(
            &dir.join("wal"),
            options,
            &memtables,
            &snapshot_tracker,
        )?;
        next_seqno.store(recovered.next_seqno, Ordering::Relaxed);
        last_durable_seqno.store(recovered.last_durable_seqno, Ordering::Relaxed);
        snapshot_tracker.set_latest_seqno(recovered.last_durable_seqno);

        let (tx, rx) = mpsc::unbounded_channel();

        let mut state = WalState::open_new_segment(
            dir.to_path_buf(),
            options.clone(),
            memtables,
            versions,
            next_seqno.clone(),
            last_durable_seqno.clone(),
            recovered.last_segment_id + 1,
        )?;

        thread::Builder::new()
            .name("layerdb-wal".to_string())
            .spawn(move || wal_thread_main(&mut state, rx))
            .expect("spawn wal thread");

        Ok(Self {
            tx,
            _next_seqno: next_seqno,
            last_durable_seqno,
        })
    }

    pub fn write_batch(
        &self,
        ops: &[Op],
        opts: WriteOptions,
    ) -> anyhow::Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(WalRequest {
                ops: ops.to_vec(),
                opts,
                done: done_tx,
            })
            .map_err(|_| WalError::Closed)?;
        done_rx.blocking_recv().map_err(|_| WalError::Closed)??;
        Ok(())
    }

    pub fn last_durable_seqno(&self) -> u64 {
        self.last_durable_seqno.load(Ordering::Relaxed)
    }
}

struct RecoveredWal {
    next_seqno: u64,
    last_durable_seqno: u64,
    last_segment_id: u64,
}

fn wal_thread_main(state: &mut WalState, mut rx: mpsc::UnboundedReceiver<WalRequest>) {
    while let Some(req) = rx.blocking_recv() {
        let result = state.handle_request(req.ops, req.opts);
        let _ = req.done.send(result);
    }
}

impl WalState {
    fn open_new_segment(
        dir: PathBuf,
        options: DbOptions,
        memtables: Arc<MemTableManager>,
        versions: Arc<VersionSet>,
        next_seqno: Arc<AtomicU64>,
        last_durable_seqno: Arc<AtomicU64>,
        segment_id: u64,
    ) -> anyhow::Result<Self> {
        let wal_dir = dir.join("wal");
        std::fs::create_dir_all(&wal_dir)?;
        let segment_path = wal_dir.join(format!("wal_{segment_id:016}.log"));
        let segment = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&segment_path)?;
        Ok(Self {
            dir,
            options,
            memtables,
            versions,
            next_seqno,
            last_durable_seqno,
            segment_id,
            segment,
            segment_bytes: 0,
        })
    }

    fn rotate_segment_if_needed(&mut self, additional: u64) -> anyhow::Result<()> {
        if self.segment_bytes + additional <= self.options.wal_segment_bytes {
            return Ok(());
        }

        self.segment.sync_all()?;
        self.segment_id += 1;
        self.segment_bytes = 0;
        let wal_dir = self.dir.join("wal");
        let segment_path = wal_dir.join(format!("wal_{:016}.log", self.segment_id));
        self.segment = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&segment_path)?;
        self.memtables.rotate_memtable(self.segment_id);
        Ok(())
    }

    fn handle_request(&mut self, ops: Vec<Op>, opts: WriteOptions) -> anyhow::Result<()> {
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
        if do_sync {
            self.last_durable_seqno.store(last_seqno, Ordering::Relaxed);
            self.versions.snapshots().set_latest_seqno(last_seqno);
        }

        if self.memtables.approximate_bytes() > self.options.memtable_bytes {
            // v1: rotate memtable; background flush is wired later.
            self.memtables.rotate_memtable(self.segment_id);
        }
        Ok(())
    }
}

fn encode_wal_record(seqno_base: u64, ops: &[Op]) -> anyhow::Result<Vec<u8>> {
    let mut ops_bytes = Vec::new();
    for op in ops {
        let kind = match op.kind {
            OpKind::Put => WalOpKind::Put as u8,
            OpKind::Del => WalOpKind::Del as u8,
        };
        let key_len: u32 = op.key.len().try_into().map_err(|_| WalError::RecordTooLarge)?;
        ops_bytes.push(kind);
        ops_bytes.extend_from_slice(&key_len.to_le_bytes());
        ops_bytes.extend_from_slice(op.key.as_ref());
        match op.kind {
            OpKind::Put => {
                let val_len: u32 = op.value.len().try_into().map_err(|_| WalError::RecordTooLarge)?;
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
    let mut last_segment_id = 0u64;

    for (segment_id, path) in &entries {
        last_segment_id = last_segment_id.max(*segment_id);
        let data = std::fs::read(path)?;
        let mut offset = 0usize;
        while offset + WAL_RECORD_HEADER_BYTES <= data.len() {
            let len = u32::from_le_bytes(data[offset..(offset + 4)].try_into().unwrap()) as usize;
            let crc_expected = u32::from_le_bytes(data[(offset + 4)..(offset + 8)].try_into().unwrap());
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
            snapshots.set_latest_seqno(last_durable);

            offset = end;
        }
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
        let key_len = u32::from_le_bytes(payload[offset..(offset + 4)].try_into().unwrap()) as usize;
        offset += 4;
        if offset + key_len + 4 > payload.len() {
            anyhow::bail!("truncated wal key");
        }
        let key = Bytes::copy_from_slice(&payload[offset..(offset + key_len)]);
        offset += key_len;
        let val_len = u32::from_le_bytes(payload[offset..(offset + 4)].try_into().unwrap()) as usize;
        offset += 4;

        let op_kind = match kind {
            1 => OpKind::Put,
            2 => OpKind::Del,
            other => anyhow::bail!("unknown wal op kind {other}"),
        };
        let value = match op_kind {
            OpKind::Put => {
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
