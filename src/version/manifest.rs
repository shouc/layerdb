use std::collections::BTreeMap;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::sst::TableRoot;
use crate::tier::StorageTier;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManifestRecord {
    AddFile(AddFile),
    DeleteFile(DeleteFile),
    MoveFile(MoveFile),
    FreezeFile(FreezeFile),
    VersionEdit(VersionEdit),
    BranchHead(BranchHead),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VersionEdit {
    pub adds: Vec<AddFile>,
    pub deletes: Vec<DeleteFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddFile {
    pub file_id: u64,
    pub level: u8,
    pub smallest_user_key: Bytes,
    pub largest_user_key: Bytes,
    /// Maximum sequence number contained in this file.
    ///
    /// Used during recovery to restore the global seqno allocator even when
    /// WAL segments were garbage collected.
    pub max_seqno: u64,
    pub table_root: TableRoot,
    pub size_bytes: u64,

    /// Physical storage tier for this SST.
    ///
    /// Defaults to NVMe for backward compatibility with old manifest records.
    #[serde(default)]
    pub tier: StorageTier,

    /// SST format version written by `SstBuilder`.
    ///
    /// Defaults to v1 for backward compatibility.
    #[serde(default = "default_sst_format_version")]
    pub sst_format_version: u32,
}

fn default_sst_format_version() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteFile {
    pub file_id: u64,
    pub level: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoveFile {
    pub file_id: u64,
    pub level: u8,
    pub tier: StorageTier,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreezeFile {
    pub file_id: u64,
    pub level: u8,
    pub object_id: String,
    pub object_version: Option<String>,
    pub superblock_bytes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchHead {
    pub name: String,
    pub seqno: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ManifestState {
    pub levels: BTreeMap<u8, BTreeMap<u64, AddFile>>,
    pub branches: BTreeMap<String, u64>,
    pub frozen_objects: BTreeMap<u64, FreezeFile>,
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
            state.frozen_objects.remove(&add.file_id);
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
            state.frozen_objects.remove(&del.file_id);
        }
        ManifestRecord::MoveFile(mv) => {
            if let Some(level) = state.levels.get_mut(&mv.level) {
                if let Some(add) = level.get_mut(&mv.file_id) {
                    add.tier = mv.tier;
                }
            }
            if mv.tier != StorageTier::S3 {
                state.frozen_objects.remove(&mv.file_id);
            }
        }
        ManifestRecord::FreezeFile(freeze) => {
            if let Some(level) = state.levels.get_mut(&freeze.level) {
                if let Some(add) = level.get_mut(&freeze.file_id) {
                    add.tier = StorageTier::S3;
                }
            }
            state.frozen_objects.insert(freeze.file_id, freeze);
        }
        ManifestRecord::VersionEdit(edit) => {
            for add in edit.adds {
                state.frozen_objects.remove(&add.file_id);
                state
                    .levels
                    .entry(add.level)
                    .or_default()
                    .insert(add.file_id, add);
            }
            for del in edit.deletes {
                if let Some(level) = state.levels.get_mut(&del.level) {
                    level.remove(&del.file_id);
                }
                state.frozen_objects.remove(&del.file_id);
            }
        }
        ManifestRecord::BranchHead(branch) => {
            state.branches.insert(branch.name, branch.seqno);
        }
    }
}
