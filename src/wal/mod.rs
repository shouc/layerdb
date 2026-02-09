use std::path::{Path, PathBuf};

use crate::db::{DbOptions, Op, WriteOptions};
use crate::memtable::MemTableManager;

/// WAL placeholder.
///
/// v1 will implement a segmented append log with record framing:
/// `[len][crc32c][seqno_base][count][ops..]`.
#[derive(Debug)]
pub struct Wal {
    _dir: PathBuf,
    _options: DbOptions,
}

impl Wal {
    pub fn open(dir: &Path, options: &DbOptions) -> anyhow::Result<Self> {
        Ok(Self {
            _dir: dir.to_path_buf(),
            _options: options.clone(),
        })
    }

    pub fn write_batch(
        &self,
        memtables: &MemTableManager,
        ops: &[Op],
        _opts: WriteOptions,
    ) -> anyhow::Result<()> {
        // v1: assign seqnos + write WAL + fsync + apply to memtables.
        memtables.apply_batch(0, ops)
    }
}

