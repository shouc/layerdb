use std::path::{Path, PathBuf};

use anyhow::Context;
use layerdb::DbOptions;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::types::{Neighbor, VectorIndex, VectorRecord};

use super::spfresh_layerdb::{SpFreshLayerDbConfig, SpFreshLayerDbIndex, SpFreshLayerDbStats};

#[derive(Clone, Debug)]
pub struct SpFreshLayerDbShardedConfig {
    pub shard_count: usize,
    pub shard: SpFreshLayerDbConfig,
}

impl Default for SpFreshLayerDbShardedConfig {
    fn default() -> Self {
        Self {
            shard_count: 4,
            shard: SpFreshLayerDbConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpFreshLayerDbShardedStats {
    pub shard_count: usize,
    pub total_rows: u64,
    pub total_upserts: u64,
    pub total_deletes: u64,
    pub persist_errors: u64,
    pub total_persist_upsert_us: u64,
    pub total_persist_delete_us: u64,
    pub rebuild_successes: u64,
    pub rebuild_failures: u64,
    pub total_rebuild_applied_ids: u64,
    pub total_searches: u64,
    pub total_search_latency_us: u64,
    pub pending_ops: u64,
}

pub struct SpFreshLayerDbShardedIndex {
    root: PathBuf,
    cfg: SpFreshLayerDbShardedConfig,
    shards: Vec<SpFreshLayerDbIndex>,
}

impl SpFreshLayerDbShardedIndex {
    pub fn open(path: impl AsRef<Path>, cfg: SpFreshLayerDbShardedConfig) -> anyhow::Result<Self> {
        if cfg.shard_count == 0 {
            anyhow::bail!("shard_count must be > 0");
        }
        let root = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)
            .with_context(|| format!("create sharded index root {}", root.display()))?;
        let mut shards = Vec::with_capacity(cfg.shard_count);
        for shard_id in 0..cfg.shard_count {
            let shard_path = Self::shard_path(&root, shard_id);
            std::fs::create_dir_all(&shard_path)
                .with_context(|| format!("create shard dir {}", shard_path.display()))?;
            let shard = SpFreshLayerDbIndex::open(&shard_path, cfg.shard.clone())
                .with_context(|| format!("open shard {} at {}", shard_id, shard_path.display()))?;
            shards.push(shard);
        }
        Ok(Self { root, cfg, shards })
    }

    pub fn open_existing(
        path: impl AsRef<Path>,
        shard_count: usize,
        db_options: DbOptions,
    ) -> anyhow::Result<Self> {
        if shard_count == 0 {
            anyhow::bail!("shard_count must be > 0");
        }
        let root = path.as_ref().to_path_buf();
        let mut shards = Vec::with_capacity(shard_count);
        for shard_id in 0..shard_count {
            let shard_path = Self::shard_path(&root, shard_id);
            let shard = SpFreshLayerDbIndex::open_existing(&shard_path, db_options.clone())
                .with_context(|| {
                    format!(
                        "open existing shard {} at {}",
                        shard_id,
                        shard_path.display()
                    )
                })?;
            shards.push(shard);
        }
        Ok(Self {
            root,
            cfg: SpFreshLayerDbShardedConfig {
                shard_count,
                shard: SpFreshLayerDbConfig {
                    db_options,
                    ..Default::default()
                },
            },
            shards,
        })
    }

    fn shard_path(root: &Path, shard_id: usize) -> PathBuf {
        root.join(format!("shard-{shard_id:04}"))
    }

    fn shard_for_id(&self, id: u64) -> usize {
        (id as usize) % self.cfg.shard_count
    }

    fn merge_neighbors(mut all: Vec<Neighbor>, k: usize) -> Vec<Neighbor> {
        if k == 0 || all.is_empty() {
            return Vec::new();
        }
        let mut top = Vec::with_capacity(k);
        for neighbor in all.drain(..) {
            Self::push_neighbor_topk(&mut top, neighbor, k);
        }
        top.sort_by(Self::neighbor_cmp);
        top
    }

    fn neighbor_cmp(a: &Neighbor, b: &Neighbor) -> std::cmp::Ordering {
        a.distance
            .total_cmp(&b.distance)
            .then_with(|| a.id.cmp(&b.id))
    }

    fn push_neighbor_topk(top: &mut Vec<Neighbor>, candidate: Neighbor, k: usize) {
        if k == 0 {
            return;
        }
        if top.len() < k {
            top.push(candidate);
            return;
        }
        let mut worst_idx = 0usize;
        for idx in 1..top.len() {
            if Self::neighbor_cmp(&top[idx], &top[worst_idx]).is_gt() {
                worst_idx = idx;
            }
        }
        if Self::neighbor_cmp(&candidate, &top[worst_idx]).is_lt() {
            top[worst_idx] = candidate;
        }
    }

    pub fn try_bulk_load(&mut self, rows: &[VectorRecord]) -> anyhow::Result<()> {
        let mut partitioned = vec![Vec::<VectorRecord>::new(); self.cfg.shard_count];
        for row in rows {
            partitioned[self.shard_for_id(row.id)].push(row.clone());
        }
        for (shard_id, chunk) in partitioned.iter().enumerate() {
            self.shards[shard_id]
                .try_bulk_load(chunk)
                .with_context(|| format!("bulk load shard {}", shard_id))?;
        }
        Ok(())
    }

    pub fn try_upsert(&mut self, id: u64, vector: Vec<f32>) -> anyhow::Result<()> {
        let shard = self.shard_for_id(id);
        self.shards[shard]
            .try_upsert(id, vector)
            .with_context(|| format!("upsert shard {}", shard))
    }

    pub fn try_upsert_batch(&mut self, rows: &[VectorRecord]) -> anyhow::Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut partitioned = vec![Vec::<VectorRecord>::new(); self.cfg.shard_count];
        for row in rows {
            partitioned[self.shard_for_id(row.id)].push(row.clone());
        }
        let mut total = 0usize;
        for (shard_id, chunk) in partitioned.into_iter().enumerate() {
            if chunk.is_empty() {
                continue;
            }
            total += self.shards[shard_id]
                .try_upsert_batch(&chunk)
                .with_context(|| format!("upsert batch shard {}", shard_id))?;
        }
        Ok(total)
    }

    pub fn try_delete(&mut self, id: u64) -> anyhow::Result<bool> {
        let shard = self.shard_for_id(id);
        self.shards[shard]
            .try_delete(id)
            .with_context(|| format!("delete shard {}", shard))
    }

    pub fn try_delete_batch(&mut self, ids: &[u64]) -> anyhow::Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mut partitioned = vec![Vec::<u64>::new(); self.cfg.shard_count];
        for id in ids {
            partitioned[self.shard_for_id(*id)].push(*id);
        }
        let mut total_deleted = 0usize;
        for (shard_id, chunk) in partitioned.into_iter().enumerate() {
            if chunk.is_empty() {
                continue;
            }
            total_deleted += self.shards[shard_id]
                .try_delete_batch(&chunk)
                .with_context(|| format!("delete batch shard {}", shard_id))?;
        }
        Ok(total_deleted)
    }

    pub fn force_rebuild(&self) -> anyhow::Result<()> {
        for (shard_id, shard) in self.shards.iter().enumerate() {
            shard
                .force_rebuild()
                .with_context(|| format!("force rebuild shard {}", shard_id))?;
        }
        Ok(())
    }

    pub fn close(mut self) -> anyhow::Result<()> {
        for shard in self.shards.drain(..) {
            shard.close()?;
        }
        Ok(())
    }

    pub fn stats_per_shard(&self) -> Vec<SpFreshLayerDbStats> {
        self.shards.iter().map(SpFreshLayerDbIndex::stats).collect()
    }

    pub fn stats(&self) -> SpFreshLayerDbShardedStats {
        let mut out = SpFreshLayerDbShardedStats {
            shard_count: self.cfg.shard_count,
            ..Default::default()
        };
        for (i, shard) in self.shards.iter().enumerate() {
            let stats = shard.stats();
            out.total_rows += shard.len() as u64;
            out.total_upserts += stats.total_upserts;
            out.total_deletes += stats.total_deletes;
            out.persist_errors += stats.persist_errors;
            out.total_persist_upsert_us += stats.total_persist_upsert_us;
            out.total_persist_delete_us += stats.total_persist_delete_us;
            out.rebuild_successes += stats.rebuild_successes;
            out.rebuild_failures += stats.rebuild_failures;
            out.total_rebuild_applied_ids += stats.total_rebuild_applied_ids;
            out.total_searches += stats.total_searches;
            out.total_search_latency_us += stats.total_search_latency_us;
            out.pending_ops += stats.pending_ops;
            if i == 0 {
                // nothing else to do; prevents clippy from suggesting fold and keeps this explicit.
            }
        }
        out
    }

    pub fn health_check(&self) -> anyhow::Result<SpFreshLayerDbShardedStats> {
        for (shard_id, shard) in self.shards.iter().enumerate() {
            let _ = shard
                .health_check()
                .with_context(|| format!("health check shard {}", shard_id))?;
        }
        Ok(self.stats())
    }

    pub fn sync_to_s3(&self, max_files_per_shard: Option<usize>) -> anyhow::Result<usize> {
        let mut moved = 0usize;
        for (shard_id, shard) in self.shards.iter().enumerate() {
            moved += shard
                .sync_to_s3(max_files_per_shard)
                .with_context(|| format!("sync shard {} to s3", shard_id))?;
        }
        Ok(moved)
    }

    pub fn thaw_from_s3(&self, max_files_per_shard: Option<usize>) -> anyhow::Result<usize> {
        let mut thawed = 0usize;
        for (shard_id, shard) in self.shards.iter().enumerate() {
            thawed += shard
                .thaw_from_s3(max_files_per_shard)
                .with_context(|| format!("thaw shard {} from s3", shard_id))?;
        }
        Ok(thawed)
    }

    pub fn gc_orphaned_s3(&self) -> anyhow::Result<usize> {
        let mut removed = 0usize;
        for (shard_id, shard) in self.shards.iter().enumerate() {
            removed += shard
                .gc_orphaned_s3()
                .with_context(|| format!("gc orphaned s3 for shard {}", shard_id))?;
        }
        Ok(removed)
    }

    pub fn root_path(&self) -> &Path {
        &self.root
    }
}

impl VectorIndex for SpFreshLayerDbShardedIndex {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        self.try_upsert(id, vector)
            .unwrap_or_else(|err| panic!("sharded spfresh upsert failed for id={id}: {err:#}"));
    }

    fn delete(&mut self, id: u64) -> bool {
        self.try_delete(id)
            .unwrap_or_else(|err| panic!("sharded spfresh delete failed for id={id}: {err:#}"))
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<Neighbor> {
        let all: Vec<Neighbor> = self
            .shards
            .par_iter()
            .flat_map_iter(|shard| shard.search(query, k).into_iter())
            .collect();
        Self::merge_neighbors(all, k)
    }

    fn len(&self) -> usize {
        self.shards.iter().map(SpFreshLayerDbIndex::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::types::VectorIndex;

    use super::{SpFreshLayerDbShardedConfig, SpFreshLayerDbShardedIndex};

    #[test]
    fn sharded_index_round_trip_across_restart() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let cfg = SpFreshLayerDbShardedConfig {
            shard_count: 4,
            ..Default::default()
        };
        {
            let mut idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg.clone())?;
            idx.try_upsert(1, vec![0.1; cfg.shard.spfresh.dim])?;
            idx.try_upsert(2, vec![0.2; cfg.shard.spfresh.dim])?;
            idx.try_upsert(9, vec![0.9; cfg.shard.spfresh.dim])?;
            idx.close()?;
        }

        let idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg)?;
        assert_eq!(idx.len(), 3);
        let got = idx.search(&vec![0.9; 64], 3);
        assert!(got.iter().any(|n| n.id == 9));
        Ok(())
    }

    #[test]
    fn sharded_delete_routes_to_single_shard() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let cfg = SpFreshLayerDbShardedConfig {
            shard_count: 8,
            ..Default::default()
        };
        let mut idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(10, vec![0.1; cfg.shard.spfresh.dim])?;
        idx.try_upsert(18, vec![0.2; cfg.shard.spfresh.dim])?;
        idx.try_upsert(26, vec![0.3; cfg.shard.spfresh.dim])?;

        assert!(idx.try_delete(18)?);
        assert!(!idx.try_delete(18)?);
        assert_eq!(idx.len(), 2);
        let got = idx.search(&vec![0.2; 64], 3);
        assert!(got.iter().all(|n| n.id != 18));
        Ok(())
    }

    #[test]
    fn sharded_batch_upsert_delete_round_trip() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let cfg = SpFreshLayerDbShardedConfig {
            shard_count: 4,
            ..Default::default()
        };
        {
            let mut idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg.clone())?;
            let rows = vec![
                crate::types::VectorRecord::new(1, vec![0.1; cfg.shard.spfresh.dim]),
                crate::types::VectorRecord::new(2, vec![0.2; cfg.shard.spfresh.dim]),
                crate::types::VectorRecord::new(9, vec![0.9; cfg.shard.spfresh.dim]),
                crate::types::VectorRecord::new(1, vec![0.15; cfg.shard.spfresh.dim]),
            ];
            assert_eq!(idx.try_upsert_batch(&rows)?, 3);
            assert_eq!(idx.try_delete_batch(&[2, 2, 99])?, 1);
            idx.close()?;
        }

        let idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg)?;
        assert_eq!(idx.len(), 2);
        let got = idx.search(&vec![0.9; 64], 2);
        assert!(got.iter().all(|n| n.id != 2));
        assert!(got.iter().any(|n| n.id == 1));
        assert!(got.iter().any(|n| n.id == 9));
        Ok(())
    }

    #[test]
    fn sharded_stats_aggregate_across_shards() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let cfg = SpFreshLayerDbShardedConfig {
            shard_count: 2,
            ..Default::default()
        };
        let mut idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg.clone())?;
        idx.try_upsert(3, vec![0.3; cfg.shard.spfresh.dim])?;
        idx.try_upsert(4, vec![0.4; cfg.shard.spfresh.dim])?;
        assert!(idx.try_delete(3)?);
        idx.force_rebuild()?;
        let stats = idx.stats();
        assert_eq!(stats.shard_count, 2);
        assert_eq!(stats.total_rows, 1);
        assert_eq!(stats.total_upserts, 2);
        assert_eq!(stats.total_deletes, 1);
        Ok(())
    }
}
