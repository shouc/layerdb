//! Tier routing and local tier abstractions.
//!
//! Milestone 4 introduces local NVMe/HDD tier routing. Remote S3 is kept as a
//! planned surface with a conservative placeholder.

use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StorageTier {
    Nvme,
    Hdd,
    S3,
}

#[derive(Debug, Clone)]
pub struct LocalFsTier {
    root: PathBuf,
    tier: StorageTier,
}

impl LocalFsTier {
    pub fn new(root: impl Into<PathBuf>, tier: StorageTier) -> anyhow::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root, tier })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn tier(&self) -> StorageTier {
        self.tier
    }

    pub fn sst_path(&self, file_id: u64) -> PathBuf {
        self.root.join(format!("sst_{file_id:016x}.sst"))
    }

    pub fn tmp_sst_path(&self, file_id: u64) -> PathBuf {
        self.root.join(format!("sst_{file_id:016x}.tmp"))
    }
}

#[derive(Debug, Clone)]
pub struct S3Tier {
    pub bucket: String,
    pub prefix: String,
}

impl S3Tier {
    pub fn object_key_for_superblock(
        &self,
        level: u8,
        object_id: &str,
        superblock_id: u32,
    ) -> String {
        format!(
            "{}/L{level}/{object_id}/sb_{superblock_id:08}.bin",
            self.prefix.trim_end_matches('/')
        )
    }
}

#[derive(Debug, Clone)]
pub struct TierRouter {
    pub nvme: LocalFsTier,
    pub hdd: Option<LocalFsTier>,
    pub s3: Option<S3Tier>,
    pub hot_levels_max: u8,
}

impl TierRouter {
    pub fn single_tier(nvme_root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        Ok(Self {
            nvme: LocalFsTier::new(nvme_root, StorageTier::Nvme)?,
            hdd: None,
            s3: None,
            hot_levels_max: 2,
        })
    }

    pub fn with_hdd(mut self, hdd_root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        self.hdd = Some(LocalFsTier::new(hdd_root, StorageTier::Hdd)?);
        Ok(self)
    }

    pub fn with_s3(mut self, bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        self.s3 = Some(S3Tier {
            bucket: bucket.into(),
            prefix: prefix.into(),
        });
        self
    }

    pub fn tier_for_level(&self, level: u8) -> StorageTier {
        if level <= self.hot_levels_max {
            StorageTier::Nvme
        } else if self.hdd.is_some() {
            StorageTier::Hdd
        } else {
            StorageTier::Nvme
        }
    }

    pub fn path_for_sst(&self, level: u8, file_id: u64) -> Option<PathBuf> {
        match self.tier_for_level(level) {
            StorageTier::Nvme => Some(self.nvme.sst_path(file_id)),
            StorageTier::Hdd => self.hdd.as_ref().map(|h| h.sst_path(file_id)),
            StorageTier::S3 => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_router_prefers_nvme_for_hot_levels() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let router = TierRouter::single_tier(dir.path()).expect("router");
        assert_eq!(router.tier_for_level(0), StorageTier::Nvme);
        assert_eq!(router.tier_for_level(2), StorageTier::Nvme);
        assert_eq!(router.tier_for_level(7), StorageTier::Nvme);
    }

    #[test]
    fn tier_router_uses_hdd_for_cold_levels() {
        let nvme = tempfile::TempDir::new().expect("tempdir");
        let hdd = tempfile::TempDir::new().expect("tempdir");

        let mut router = TierRouter::single_tier(nvme.path())
            .expect("router")
            .with_hdd(hdd.path())
            .expect("with_hdd");
        router.hot_levels_max = 1;

        assert_eq!(router.tier_for_level(0), StorageTier::Nvme);
        assert_eq!(router.tier_for_level(1), StorageTier::Nvme);
        assert_eq!(router.tier_for_level(2), StorageTier::Hdd);
    }

    #[test]
    fn s3_object_key_is_stable() {
        let s3 = S3Tier {
            bucket: "b".to_string(),
            prefix: "layerdb/objects".to_string(),
        };
        assert_eq!(
            s3.object_key_for_superblock(6, "abc", 12),
            "layerdb/objects/L6/abc/sb_00000012.bin"
        );
    }
}
