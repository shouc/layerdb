use std::collections::BTreeMap;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::sst::TableRoot;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManifestRecord {
    AddFile(AddFile),
    DeleteFile(DeleteFile),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddFile {
    pub file_id: u64,
    pub level: u8,
    pub smallest_user_key: Bytes,
    pub largest_user_key: Bytes,
    pub table_root: TableRoot,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteFile {
    pub file_id: u64,
    pub level: u8,
}

#[derive(Debug, Clone, Default)]
pub struct ManifestState {
    pub levels: BTreeMap<u8, BTreeMap<u64, AddFile>>,
}

#[derive(Debug)]
pub struct Manifest {
    path: PathBuf,
    file: std::fs::File,
}

impl Manifest {
    pub fn open(dir: &Path) -> anyhow::Result<(Self, ManifestState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("MANIFEST");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        let mut data = Vec::new();
        file.seek(std::io::SeekFrom::Start(0))?;
        file.read_to_end(&mut data)?;
        let state = replay_manifest(&data)?;
        Ok((Self { path, file }, state))
    }

    pub fn append(&mut self, record: &ManifestRecord, sync: bool) -> anyhow::Result<()> {
        let payload = bincode::serialize(record)?;
        let len: u32 = payload.len().try_into()?;
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&payload);
        use std::io::Write;
        self.file.write_all(&buf)?;
        if sync {
            self.file.sync_data()?;
        }
        Ok(())
    }

    pub fn sync_dir(&self) -> anyhow::Result<()> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("manifest has no parent"))?;
        let dir_fd = std::fs::File::open(parent)?;
        dir_fd.sync_all()?;
        Ok(())
    }
}

fn replay_manifest(data: &[u8]) -> anyhow::Result<ManifestState> {
    let mut offset = 0usize;
    let mut state = ManifestState::default();
    while offset + 4 <= data.len() {
        let len = u32::from_le_bytes(data[offset..(offset + 4)].try_into().unwrap()) as usize;
        offset += 4;
        if offset + len > data.len() {
            break;
        }
        let record: ManifestRecord = bincode::deserialize(&data[offset..(offset + len)])?;
        apply_record(&mut state, record);
        offset += len;
    }
    Ok(state)
}

fn apply_record(state: &mut ManifestState, record: ManifestRecord) {
    match record {
        ManifestRecord::AddFile(add) => {
            state
                .levels
                .entry(add.level)
                .or_default()
                .insert(add.file_id, add);
        }
        ManifestRecord::DeleteFile(del) => {
            if let Some(level) = state.levels.get_mut(&del.level) {
                level.remove(&del.file_id);
            }
        }
    }
}
