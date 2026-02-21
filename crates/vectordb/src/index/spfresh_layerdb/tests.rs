use std::collections::HashMap;
use std::fs;

use layerdb::{Db, DbOptions, Op, WriteOptions};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

use crate::types::{VectorIndex, VectorRecord};

pub(super) use super::{
    config, storage, PersistedStartupManifest, STARTUP_MANIFEST_SCHEMA_VERSION,
};
use super::{
    MutationCommitMode, SpFreshLayerDbConfig, SpFreshLayerDbIndex, SpFreshMemoryMode,
    VectorMutation,
};

fn minio_enabled() -> bool {
    matches!(
        std::env::var("LAYERDB_MINIO_INTEGRATION").ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

fn assert_index_matches_model(
    idx: &SpFreshLayerDbIndex,
    expected: &HashMap<u64, Vec<f32>>,
) -> anyhow::Result<()> {
    assert_eq!(idx.len(), expected.len());
    if expected.is_empty() {
        return Ok(());
    }

    let k = expected.len();
    for (id, vector) in expected {
        let got = idx.search(vector, k);
        anyhow::ensure!(
            got.iter().any(|n| n.id == *id),
            "expected id={} missing from search results",
            id
        );
    }
    Ok(())
}

mod basic;
mod batch;
mod diskmeta;
mod randomized;
mod s3;
mod startup;
