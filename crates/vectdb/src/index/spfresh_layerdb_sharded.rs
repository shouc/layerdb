#[cfg(test)]
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use layerdb::DbOptions;
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};

use crate::types::{Neighbor, VectorIndex, VectorRecord};

use super::spfresh_layerdb::{
    MutationCommitMode, SpFreshLayerDbConfig, SpFreshLayerDbIndex, SpFreshLayerDbStats,
    VectorMutation, VectorMutationBatchResult,
};

#[derive(Clone, Debug)]
pub struct SpFreshLayerDbShardedConfig {
    pub shard_count: usize,
    pub shard: SpFreshLayerDbConfig,
    pub exact_shard_prune: bool,
}

impl Default for SpFreshLayerDbShardedConfig {
    fn default() -> Self {
        Self {
            shard_count: 4,
            shard: SpFreshLayerDbConfig::default(),
            exact_shard_prune: false,
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
    shard_mask: Option<usize>,
    shards: Vec<SpFreshLayerDbIndex>,
}

#[derive(Clone, Debug)]
#[cfg(test)]
struct HeapNeighbor(Neighbor);

#[cfg(test)]
impl PartialEq for HeapNeighbor {
    fn eq(&self, other: &Self) -> bool {
        self.0.id == other.0.id && self.0.distance.to_bits() == other.0.distance.to_bits()
    }
}

#[cfg(test)]
impl Eq for HeapNeighbor {}

#[cfg(test)]
impl PartialOrd for HeapNeighbor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
impl Ord for HeapNeighbor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .distance
            .total_cmp(&other.0.distance)
            .then_with(|| self.0.id.cmp(&other.0.id))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(test)]
struct ShardCursor {
    shard_id: usize,
    neighbor_idx: usize,
}

#[cfg(test)]
impl Ord for ShardCursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Ordering is supplied at call-site through heap payload tuple.
        self.shard_id
            .cmp(&other.shard_id)
            .then_with(|| self.neighbor_idx.cmp(&other.neighbor_idx))
    }
}

#[cfg(test)]
impl PartialOrd for ShardCursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl SpFreshLayerDbShardedIndex {
    pub fn open(path: impl AsRef<Path>, cfg: SpFreshLayerDbShardedConfig) -> anyhow::Result<Self> {
        if cfg.shard_count == 0 {
            anyhow::bail!("shard_count must be > 0");
        }
        let root = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)
            .with_context(|| format!("create sharded index root {}", root.display()))?;
        for shard_id in 0..cfg.shard_count {
            let shard_path = Self::shard_path(&root, shard_id);
            std::fs::create_dir_all(&shard_path)
                .with_context(|| format!("create shard dir {}", shard_path.display()))?;
        }
        let mut opened: Vec<anyhow::Result<(usize, SpFreshLayerDbIndex)>> = (0..cfg.shard_count)
            .into_par_iter()
            .map(|shard_id| {
                let shard_path = Self::shard_path(&root, shard_id);
                let shard = SpFreshLayerDbIndex::open(&shard_path, cfg.shard.clone())
                    .with_context(|| {
                        format!("open shard {} at {}", shard_id, shard_path.display())
                    })?;
                Ok((shard_id, shard))
            })
            .collect();
        let mut shards = Vec::with_capacity(cfg.shard_count);
        let mut pairs = Vec::with_capacity(cfg.shard_count);
        for result in opened.drain(..) {
            pairs.push(result?);
        }
        pairs.sort_by_key(|(id, _)| *id);
        for (_id, shard) in pairs {
            shards.push(shard);
        }
        let shard_mask = Self::shard_mask_for(cfg.shard_count);
        Ok(Self {
            root,
            cfg,
            shard_mask,
            shards,
        })
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
        let mut opened: Vec<anyhow::Result<(usize, SpFreshLayerDbIndex)>> = (0..shard_count)
            .into_par_iter()
            .map(|shard_id| {
                let shard_path = Self::shard_path(&root, shard_id);
                let shard = SpFreshLayerDbIndex::open_existing(&shard_path, db_options.clone())
                    .with_context(|| {
                        format!(
                            "open existing shard {} at {}",
                            shard_id,
                            shard_path.display()
                        )
                    })?;
                Ok((shard_id, shard))
            })
            .collect();
        let mut shards = Vec::with_capacity(shard_count);
        let mut pairs = Vec::with_capacity(shard_count);
        for result in opened.drain(..) {
            pairs.push(result?);
        }
        pairs.sort_by_key(|(id, _)| *id);
        for (_id, shard) in pairs {
            shards.push(shard);
        }
        let shard_mask = Self::shard_mask_for(shard_count);
        Ok(Self {
            root,
            cfg: SpFreshLayerDbShardedConfig {
                shard_count,
                shard: SpFreshLayerDbConfig {
                    db_options,
                    ..Default::default()
                },
                exact_shard_prune: false,
            },
            shard_mask,
            shards,
        })
    }

    fn shard_path(root: &Path, shard_id: usize) -> PathBuf {
        root.join(format!("shard-{shard_id:04}"))
    }

    fn shard_for_id(&self, id: u64) -> usize {
        if let Some(mask) = self.shard_mask {
            (id as usize) & mask
        } else {
            (id as usize) % self.cfg.shard_count
        }
    }

    fn shard_mask_for(shard_count: usize) -> Option<usize> {
        if shard_count.is_power_of_two() {
            Some(shard_count - 1)
        } else {
            None
        }
    }

    fn validate_rows_dims(&self, rows: &[VectorRecord]) -> anyhow::Result<()> {
        for row in rows {
            let shard_id = self.shard_for_id(row.id);
            let expected_dim = self.shards[shard_id].vector_dim();
            if row.values.len() != expected_dim {
                anyhow::bail!(
                    "invalid vector dim for id={}: got {}, expected {}",
                    row.id,
                    row.values.len(),
                    expected_dim
                );
            }
        }
        Ok(())
    }

    fn validate_mutations_dims(&self, mutations: &[VectorMutation]) -> anyhow::Result<()> {
        for mutation in mutations {
            let VectorMutation::Upsert(row) = mutation else {
                continue;
            };
            let shard_id = self.shard_for_id(row.id);
            let expected_dim = self.shards[shard_id].vector_dim();
            if row.values.len() != expected_dim {
                anyhow::bail!(
                    "invalid vector dim for id={}: got {}, expected {}",
                    row.id,
                    row.values.len(),
                    expected_dim
                );
            }
        }
        Ok(())
    }

    fn dedup_last_mutations_partitioned(
        &self,
        mutations: &[VectorMutation],
    ) -> Vec<Vec<VectorMutation>> {
        let mut shard_counts = vec![0usize; self.cfg.shard_count];
        for mutation in mutations {
            shard_counts[self.shard_for_id(mutation.id())] += 1;
        }
        let mut seen_by_shard: Vec<FxHashSet<u64>> = shard_counts
            .iter()
            .map(|count| FxHashSet::with_capacity_and_hasher(*count, Default::default()))
            .collect();
        let mut partitioned_rev: Vec<Vec<VectorMutation>> = shard_counts
            .iter()
            .map(|count| Vec::with_capacity(*count))
            .collect();
        for mutation in mutations.iter().rev() {
            let id = mutation.id();
            let shard = self.shard_for_id(id);
            if seen_by_shard[shard].insert(id) {
                partitioned_rev[shard].push(mutation.clone());
            }
        }
        for shard_mutations in &mut partitioned_rev {
            shard_mutations.reverse();
        }
        partitioned_rev
    }

    fn dedup_last_mutations_partitioned_owned(
        &self,
        mutations: Vec<VectorMutation>,
    ) -> Vec<Vec<VectorMutation>> {
        let mut shard_counts = vec![0usize; self.cfg.shard_count];
        for mutation in &mutations {
            shard_counts[self.shard_for_id(mutation.id())] += 1;
        }
        let mut seen_by_shard: Vec<FxHashSet<u64>> = shard_counts
            .iter()
            .map(|count| FxHashSet::with_capacity_and_hasher(*count, Default::default()))
            .collect();
        let mut partitioned_rev: Vec<Vec<VectorMutation>> = shard_counts
            .iter()
            .map(|count| Vec::with_capacity(*count))
            .collect();
        for mutation in mutations.into_iter().rev() {
            let id = mutation.id();
            let shard = self.shard_for_id(id);
            if seen_by_shard[shard].insert(id) {
                partitioned_rev[shard].push(mutation);
            }
        }
        for shard_mutations in &mut partitioned_rev {
            shard_mutations.reverse();
        }
        partitioned_rev
    }

    fn dedup_last_rows_partitioned(&self, rows: &[VectorRecord]) -> Vec<Vec<VectorRecord>> {
        let mut shard_counts = vec![0usize; self.cfg.shard_count];
        for row in rows {
            shard_counts[self.shard_for_id(row.id)] += 1;
        }
        let mut seen_by_shard: Vec<FxHashSet<u64>> = shard_counts
            .iter()
            .map(|count| FxHashSet::with_capacity_and_hasher(*count, Default::default()))
            .collect();
        let mut partitioned_rev: Vec<Vec<VectorRecord>> = shard_counts
            .iter()
            .map(|count| Vec::with_capacity(*count))
            .collect();
        for row in rows.iter().rev() {
            let shard = self.shard_for_id(row.id);
            if seen_by_shard[shard].insert(row.id) {
                partitioned_rev[shard].push(row.clone());
            }
        }
        for shard_rows in &mut partitioned_rev {
            shard_rows.reverse();
        }
        partitioned_rev
    }

    fn dedup_last_rows_partitioned_owned(&self, rows: Vec<VectorRecord>) -> Vec<Vec<VectorRecord>> {
        let mut shard_counts = vec![0usize; self.cfg.shard_count];
        for row in &rows {
            shard_counts[self.shard_for_id(row.id)] += 1;
        }
        let mut seen_by_shard: Vec<FxHashSet<u64>> = shard_counts
            .iter()
            .map(|count| FxHashSet::with_capacity_and_hasher(*count, Default::default()))
            .collect();
        let mut partitioned_rev: Vec<Vec<VectorRecord>> = shard_counts
            .iter()
            .map(|count| Vec::with_capacity(*count))
            .collect();
        for row in rows.into_iter().rev() {
            let shard = self.shard_for_id(row.id);
            if seen_by_shard[shard].insert(row.id) {
                partitioned_rev[shard].push(row);
            }
        }
        for shard_rows in &mut partitioned_rev {
            shard_rows.reverse();
        }
        partitioned_rev
    }

    fn dedup_ids_partitioned(&self, ids: &[u64]) -> Vec<Vec<u64>> {
        let mut shard_counts = vec![0usize; self.cfg.shard_count];
        for id in ids {
            shard_counts[self.shard_for_id(*id)] += 1;
        }
        let mut seen_by_shard: Vec<FxHashSet<u64>> = shard_counts
            .iter()
            .map(|count| FxHashSet::with_capacity_and_hasher(*count, Default::default()))
            .collect();
        let mut partitioned: Vec<Vec<u64>> = shard_counts
            .iter()
            .map(|count| Vec::with_capacity(*count))
            .collect();
        for id in ids {
            let shard = self.shard_for_id(*id);
            if seen_by_shard[shard].insert(*id) {
                partitioned[shard].push(*id);
            }
        }
        partitioned
    }

    fn single_non_empty_partition<T>(partitioned: &[Vec<T>]) -> Option<usize> {
        let mut found = None;
        for (idx, chunk) in partitioned.iter().enumerate() {
            if chunk.is_empty() {
                continue;
            }
            if found.is_some() {
                return None;
            }
            found = Some(idx);
        }
        found
    }

    #[cfg(test)]
    fn merge_neighbors(mut all: Vec<Neighbor>, k: usize) -> Vec<Neighbor> {
        if k == 0 || all.is_empty() {
            return Vec::new();
        }
        let mut heap = BinaryHeap::with_capacity(k);
        for neighbor in all.drain(..) {
            if heap.len() < k {
                heap.push(HeapNeighbor(neighbor));
                continue;
            }
            let replace = heap
                .peek()
                .map(|worst| Self::neighbor_cmp(&neighbor, &worst.0).is_lt())
                .unwrap_or(true);
            if replace {
                let _ = heap.pop();
                heap.push(HeapNeighbor(neighbor));
            }
        }
        let mut top: Vec<Neighbor> = heap.into_iter().map(|entry| entry.0).collect();
        top.sort_by(Self::neighbor_cmp);
        top
    }

    #[cfg(test)]
    fn merge_sorted_neighbors(per_shard: &[Vec<Neighbor>], k: usize) -> Vec<Neighbor> {
        if k == 0 || per_shard.is_empty() {
            return Vec::new();
        }
        // BinaryHeap is max-heap; wrap ordering to pop smallest distance first.
        let mut heap: BinaryHeap<(std::cmp::Reverse<HeapNeighbor>, ShardCursor)> =
            BinaryHeap::new();
        for (shard_id, neighbors) in per_shard.iter().enumerate() {
            if let Some(first) = neighbors.first() {
                heap.push((
                    std::cmp::Reverse(HeapNeighbor(first.clone())),
                    ShardCursor {
                        shard_id,
                        neighbor_idx: 0,
                    },
                ));
            }
        }
        let mut out = Vec::with_capacity(k.min(heap.len()));
        while let Some((std::cmp::Reverse(HeapNeighbor(neighbor)), cursor)) = heap.pop() {
            out.push(neighbor);
            if out.len() >= k {
                break;
            }
            let next_idx = cursor.neighbor_idx.saturating_add(1);
            if let Some(next_neighbor) = per_shard[cursor.shard_id].get(next_idx) {
                heap.push((
                    std::cmp::Reverse(HeapNeighbor(next_neighbor.clone())),
                    ShardCursor {
                        shard_id: cursor.shard_id,
                        neighbor_idx: next_idx,
                    },
                ));
            }
        }
        out
    }

    fn merge_two_sorted_neighbors(
        left: Vec<Neighbor>,
        right: Vec<Neighbor>,
        k: usize,
    ) -> Vec<Neighbor> {
        if k == 0 {
            return Vec::new();
        }
        if left.is_empty() {
            return right.into_iter().take(k).collect();
        }
        if right.is_empty() {
            return left.into_iter().take(k).collect();
        }
        let mut left_iter = left.into_iter().peekable();
        let mut right_iter = right.into_iter().peekable();
        let mut out = Vec::with_capacity(k);
        while out.len() < k {
            match (left_iter.peek(), right_iter.peek()) {
                (Some(l), Some(r)) => {
                    if Self::neighbor_cmp(l, r).is_le() {
                        if let Some(next) = left_iter.next() {
                            out.push(next);
                        }
                    } else if let Some(next) = right_iter.next() {
                        out.push(next);
                    }
                }
                (Some(_), None) => {
                    if let Some(next) = left_iter.next() {
                        out.push(next);
                    }
                }
                (None, Some(_)) => {
                    if let Some(next) = right_iter.next() {
                        out.push(next);
                    }
                }
                (None, None) => break,
            }
        }
        out
    }

    fn search_all_shards_parallel(&self, query: &[f32], k: usize) -> Vec<Neighbor> {
        self.shards
            .par_iter()
            .map(|shard| shard.search(query, k))
            .reduce(Vec::new, |left, right| {
                Self::merge_two_sorted_neighbors(left, right, k)
            })
    }

    fn search_with_exact_shard_prune(&self, query: &[f32], k: usize) -> Vec<Neighbor> {
        let mut lower_bounds: Vec<(usize, f32)> = self
            .shards
            .par_iter()
            .enumerate()
            .map(|(shard_id, shard)| {
                let best = shard
                    .search(query, 1)
                    .into_iter()
                    .next()
                    .map(|n| n.distance)
                    .unwrap_or(f32::INFINITY);
                (shard_id, best)
            })
            .collect();
        lower_bounds.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        let mut top: Vec<Neighbor> = Vec::with_capacity(k);
        let window_size = rayon::current_num_threads()
            .max(1)
            .min(self.shards.len().max(1));
        let mut cursor = 0usize;
        while cursor < lower_bounds.len() {
            let worst = top.last().map(|n| n.distance).unwrap_or(f32::INFINITY);
            if top.len() >= k && lower_bounds[cursor].1 >= worst {
                break;
            }

            let mut window = Vec::with_capacity(window_size);
            while cursor < lower_bounds.len() && window.len() < window_size {
                let (shard_id, lower_bound) = lower_bounds[cursor];
                if top.len() >= k {
                    let worst_now = top.last().map(|n| n.distance).unwrap_or(f32::INFINITY);
                    if lower_bound >= worst_now {
                        break;
                    }
                }
                window.push(shard_id);
                cursor = cursor.saturating_add(1);
            }
            if window.is_empty() {
                break;
            }

            let results: Vec<Vec<Neighbor>> = window
                .par_iter()
                .map(|shard_id| self.shards[*shard_id].search(query, k))
                .collect();
            for shard_neighbors in results {
                for neighbor in shard_neighbors {
                    Self::push_neighbor_topk(&mut top, neighbor, k);
                }
            }
            top.sort_by(Self::neighbor_cmp);
        }
        top.sort_by(Self::neighbor_cmp);
        if top.len() > k {
            top.truncate(k);
        }
        top
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

    fn neighbor_cmp(a: &Neighbor, b: &Neighbor) -> std::cmp::Ordering {
        a.distance
            .total_cmp(&b.distance)
            .then_with(|| a.id.cmp(&b.id))
    }

    pub fn try_bulk_load(&mut self, rows: &[VectorRecord]) -> anyhow::Result<()> {
        self.try_bulk_load_owned(rows.to_vec())
    }

    pub fn try_bulk_load_owned(&mut self, rows: Vec<VectorRecord>) -> anyhow::Result<()> {
        self.validate_rows_dims(rows.as_slice())?;
        let mut shard_counts = vec![0usize; self.cfg.shard_count];
        for row in &rows {
            shard_counts[self.shard_for_id(row.id)] += 1;
        }
        let mut partitioned: Vec<Vec<VectorRecord>> = shard_counts
            .iter()
            .map(|count| Vec::with_capacity(*count))
            .collect();
        for row in rows {
            partitioned[self.shard_for_id(row.id)].push(row);
        }
        let results: Vec<anyhow::Result<()>> = self
            .shards
            .par_iter_mut()
            .enumerate()
            .map(|(shard_id, shard)| {
                shard
                    .try_bulk_load(&partitioned[shard_id])
                    .with_context(|| format!("bulk load shard {}", shard_id))
            })
            .collect();
        for result in results {
            result?;
        }
        Ok(())
    }

    pub fn try_upsert(&mut self, id: u64, vector: Vec<f32>) -> anyhow::Result<()> {
        self.try_upsert_with_commit_mode(id, vector, MutationCommitMode::Durable)
    }

    pub fn try_upsert_with_commit_mode(
        &mut self,
        id: u64,
        vector: Vec<f32>,
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<()> {
        let shard = self.shard_for_id(id);
        self.shards[shard]
            .try_upsert_batch_with_commit_mode(&[VectorRecord::new(id, vector)], commit_mode)
            .map(|_| ())
            .with_context(|| format!("upsert shard {}", shard))
    }

    pub fn try_upsert_batch(&mut self, rows: &[VectorRecord]) -> anyhow::Result<usize> {
        self.try_upsert_batch_with_commit_mode(rows, MutationCommitMode::Durable)
    }

    pub fn try_upsert_batch_with_commit_mode(
        &mut self,
        rows: &[VectorRecord],
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.validate_rows_dims(rows)?;
        if self.shards.len() == 1 {
            return self.shards[0]
                .try_upsert_batch_with_commit_mode(rows, commit_mode)
                .with_context(|| "upsert batch shard 0".to_string());
        }
        if rows.len() == 1 {
            let shard_id = self.shard_for_id(rows[0].id);
            return self.shards[shard_id]
                .try_upsert_batch_with_commit_mode(rows, commit_mode)
                .with_context(|| format!("upsert batch shard {}", shard_id));
        }
        let mut partitioned = self.dedup_last_rows_partitioned(rows);
        if let Some(shard_id) = Self::single_non_empty_partition(&partitioned) {
            let chunk = std::mem::take(&mut partitioned[shard_id]);
            return self.shards[shard_id]
                .try_upsert_batch_deduped_owned_with_commit_mode(chunk, commit_mode)
                .with_context(|| format!("upsert batch shard {}", shard_id));
        }
        let results: Vec<anyhow::Result<usize>> = self
            .shards
            .par_iter_mut()
            .zip(partitioned.into_par_iter())
            .enumerate()
            .map(|(shard_id, (shard, chunk))| {
                if chunk.is_empty() {
                    Ok(0)
                } else {
                    shard
                        .try_upsert_batch_deduped_owned_with_commit_mode(chunk, commit_mode)
                        .with_context(|| format!("upsert batch shard {}", shard_id))
                }
            })
            .collect();
        let mut total = 0usize;
        for result in results {
            total += result?;
        }
        Ok(total)
    }

    pub fn try_upsert_batch_owned(&mut self, rows: Vec<VectorRecord>) -> anyhow::Result<usize> {
        self.try_upsert_batch_owned_with_commit_mode(rows, MutationCommitMode::Durable)
    }

    pub fn try_upsert_batch_owned_with_commit_mode(
        &mut self,
        rows: Vec<VectorRecord>,
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.validate_rows_dims(rows.as_slice())?;
        if self.shards.len() == 1 {
            return self.shards[0]
                .try_upsert_batch_owned_with_commit_mode(rows, commit_mode)
                .with_context(|| "upsert batch shard 0".to_string());
        }
        if rows.len() == 1 {
            let row = rows.into_iter().next().expect("rows.len() == 1");
            let shard_id = self.shard_for_id(row.id);
            return self.shards[shard_id]
                .try_upsert_batch_deduped_owned_with_commit_mode(vec![row], commit_mode)
                .with_context(|| format!("upsert batch shard {}", shard_id));
        }
        let mut partitioned = self.dedup_last_rows_partitioned_owned(rows);
        if let Some(shard_id) = Self::single_non_empty_partition(&partitioned) {
            let chunk = std::mem::take(&mut partitioned[shard_id]);
            return self.shards[shard_id]
                .try_upsert_batch_deduped_owned_with_commit_mode(chunk, commit_mode)
                .with_context(|| format!("upsert batch shard {}", shard_id));
        }
        let results: Vec<anyhow::Result<usize>> = self
            .shards
            .par_iter_mut()
            .zip(partitioned.into_par_iter())
            .enumerate()
            .map(|(shard_id, (shard, chunk))| {
                if chunk.is_empty() {
                    Ok(0)
                } else {
                    shard
                        .try_upsert_batch_deduped_owned_with_commit_mode(chunk, commit_mode)
                        .with_context(|| format!("upsert batch shard {}", shard_id))
                }
            })
            .collect();
        let mut total = 0usize;
        for result in results {
            total += result?;
        }
        Ok(total)
    }

    pub fn try_delete(&mut self, id: u64) -> anyhow::Result<bool> {
        self.try_delete_with_commit_mode(id, MutationCommitMode::Durable)
    }

    pub fn try_delete_with_commit_mode(
        &mut self,
        id: u64,
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<bool> {
        let shard = self.shard_for_id(id);
        self.shards[shard]
            .try_delete_batch_with_commit_mode(&[id], commit_mode)
            .map(|deleted| deleted > 0)
            .with_context(|| format!("delete shard {}", shard))
    }

    pub fn try_delete_batch(&mut self, ids: &[u64]) -> anyhow::Result<usize> {
        self.try_delete_batch_with_commit_mode(ids, MutationCommitMode::Durable)
    }

    pub fn try_delete_batch_with_commit_mode(
        &mut self,
        ids: &[u64],
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }
        if self.shards.len() == 1 {
            return self.shards[0]
                .try_delete_batch_with_commit_mode(ids, commit_mode)
                .with_context(|| "delete batch shard 0".to_string());
        }
        if ids.len() == 1 {
            return self
                .try_delete_with_commit_mode(ids[0], commit_mode)
                .map(usize::from);
        }
        let mut partitioned = self.dedup_ids_partitioned(ids);
        if let Some(shard_id) = Self::single_non_empty_partition(&partitioned) {
            let chunk = std::mem::take(&mut partitioned[shard_id]);
            return self.shards[shard_id]
                .try_delete_batch_deduped_owned_with_commit_mode(chunk, commit_mode)
                .with_context(|| format!("delete batch shard {}", shard_id));
        }
        let results: Vec<anyhow::Result<usize>> = self
            .shards
            .par_iter_mut()
            .zip(partitioned.into_par_iter())
            .enumerate()
            .map(|(shard_id, (shard, chunk))| {
                if chunk.is_empty() {
                    Ok(0)
                } else {
                    shard
                        .try_delete_batch_deduped_owned_with_commit_mode(chunk, commit_mode)
                        .with_context(|| format!("delete batch shard {}", shard_id))
                }
            })
            .collect();
        let mut total_deleted = 0usize;
        for result in results {
            total_deleted += result?;
        }
        Ok(total_deleted)
    }

    pub fn try_apply_batch(
        &mut self,
        mutations: &[VectorMutation],
    ) -> anyhow::Result<VectorMutationBatchResult> {
        self.try_apply_batch_with_commit_mode(mutations, MutationCommitMode::Durable)
    }

    pub fn try_apply_batch_with_commit_mode(
        &mut self,
        mutations: &[VectorMutation],
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<VectorMutationBatchResult> {
        if mutations.is_empty() {
            return Ok(VectorMutationBatchResult::default());
        }
        self.validate_mutations_dims(mutations)?;
        if self.shards.len() == 1 {
            return self.shards[0]
                .try_apply_batch_with_commit_mode(mutations, commit_mode)
                .with_context(|| "apply batch shard 0".to_string());
        }
        if mutations.len() == 1 {
            let shard_id = self.shard_for_id(mutations[0].id());
            return self.shards[shard_id]
                .try_apply_batch_with_commit_mode(mutations, commit_mode)
                .with_context(|| format!("apply batch shard {}", shard_id));
        }
        let partitioned = self.dedup_last_mutations_partitioned(mutations);
        self.try_apply_batch_partitioned_with_commit_mode(partitioned, commit_mode)
    }

    pub fn try_apply_batch_owned(
        &mut self,
        mutations: Vec<VectorMutation>,
    ) -> anyhow::Result<VectorMutationBatchResult> {
        self.try_apply_batch_owned_with_commit_mode(mutations, MutationCommitMode::Durable)
    }

    pub fn try_apply_batch_owned_with_commit_mode(
        &mut self,
        mutations: Vec<VectorMutation>,
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<VectorMutationBatchResult> {
        if mutations.is_empty() {
            return Ok(VectorMutationBatchResult::default());
        }
        self.validate_mutations_dims(mutations.as_slice())?;
        if self.shards.len() == 1 {
            return self.shards[0]
                .try_apply_batch_owned_with_commit_mode(mutations, commit_mode)
                .with_context(|| "apply batch shard 0".to_string());
        }
        if mutations.len() == 1 {
            let shard_id = self.shard_for_id(mutations[0].id());
            return self.shards[shard_id]
                .try_apply_batch_owned_with_commit_mode(mutations, commit_mode)
                .with_context(|| format!("apply batch shard {}", shard_id));
        }
        let partitioned = self.dedup_last_mutations_partitioned_owned(mutations);
        self.try_apply_batch_partitioned_with_commit_mode(partitioned, commit_mode)
    }

    fn try_apply_batch_partitioned_with_commit_mode(
        &mut self,
        mut partitioned: Vec<Vec<VectorMutation>>,
        commit_mode: MutationCommitMode,
    ) -> anyhow::Result<VectorMutationBatchResult> {
        if let Some(shard_id) = Self::single_non_empty_partition(&partitioned) {
            let chunk = std::mem::take(&mut partitioned[shard_id]);
            return self.shards[shard_id]
                .try_apply_batch_deduped_owned_with_commit_mode(chunk, commit_mode)
                .with_context(|| format!("apply batch shard {}", shard_id));
        }
        let results: Vec<anyhow::Result<VectorMutationBatchResult>> = self
            .shards
            .par_iter_mut()
            .zip(partitioned.into_par_iter())
            .enumerate()
            .map(|(shard_id, (shard, chunk))| {
                if chunk.is_empty() {
                    Ok(VectorMutationBatchResult::default())
                } else {
                    shard
                        .try_apply_batch_deduped_owned_with_commit_mode(chunk, commit_mode)
                        .with_context(|| format!("apply batch shard {}", shard_id))
                }
            })
            .collect();

        let mut aggregated = VectorMutationBatchResult::default();
        for result in results {
            let shard = result?;
            aggregated.upserts += shard.upserts;
            aggregated.deletes += shard.deletes;
        }
        Ok(aggregated)
    }

    pub fn force_rebuild(&self) -> anyhow::Result<()> {
        for (shard_id, shard) in self.shards.iter().enumerate() {
            shard
                .force_rebuild()
                .with_context(|| format!("force rebuild shard {}", shard_id))?;
        }
        Ok(())
    }

    pub fn snapshot_rows(&self) -> anyhow::Result<Vec<VectorRecord>> {
        let mut out = Vec::new();
        for (shard_id, shard) in self.shards.iter().enumerate() {
            let mut shard_rows = shard
                .snapshot_rows()
                .with_context(|| format!("snapshot rows shard {}", shard_id))?;
            out.append(&mut shard_rows);
        }
        Ok(out)
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
        if k == 0 {
            return Vec::new();
        }
        if self.cfg.exact_shard_prune {
            self.search_with_exact_shard_prune(query, k)
        } else {
            self.search_all_shards_parallel(query, k)
        }
    }

    fn len(&self) -> usize {
        self.shards.iter().map(SpFreshLayerDbIndex::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use layerdb::DbOptions;
    use tempfile::TempDir;

    use crate::index::VectorMutation;
    use crate::types::{Neighbor, VectorIndex};

    use super::{SpFreshLayerDbShardedConfig, SpFreshLayerDbShardedIndex};

    #[test]
    fn merge_neighbors_keeps_best_k() {
        let merged = SpFreshLayerDbShardedIndex::merge_neighbors(
            vec![
                Neighbor {
                    id: 4,
                    distance: 0.4,
                },
                Neighbor {
                    id: 2,
                    distance: 0.2,
                },
                Neighbor {
                    id: 3,
                    distance: 0.3,
                },
                Neighbor {
                    id: 1,
                    distance: 0.1,
                },
            ],
            2,
        );
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].id, 1);
        assert_eq!(merged[1].id, 2);
    }

    #[test]
    fn merge_sorted_neighbors_keeps_global_order() {
        let merged = SpFreshLayerDbShardedIndex::merge_sorted_neighbors(
            &[
                vec![
                    Neighbor {
                        id: 11,
                        distance: 0.11,
                    },
                    Neighbor {
                        id: 14,
                        distance: 0.40,
                    },
                ],
                vec![
                    Neighbor {
                        id: 21,
                        distance: 0.09,
                    },
                    Neighbor {
                        id: 22,
                        distance: 0.30,
                    },
                ],
                vec![Neighbor {
                    id: 31,
                    distance: 0.20,
                }],
            ],
            4,
        );
        let ids: Vec<u64> = merged.into_iter().map(|n| n.id).collect();
        assert_eq!(ids, vec![21, 11, 31, 22]);
    }

    #[test]
    fn merge_two_sorted_neighbors_matches_kway_merge() {
        let per_shard = vec![
            vec![
                Neighbor {
                    id: 10,
                    distance: 0.10,
                },
                Neighbor {
                    id: 12,
                    distance: 0.30,
                },
                Neighbor {
                    id: 13,
                    distance: 0.90,
                },
            ],
            vec![
                Neighbor {
                    id: 20,
                    distance: 0.05,
                },
                Neighbor {
                    id: 21,
                    distance: 0.25,
                },
            ],
            vec![
                Neighbor {
                    id: 30,
                    distance: 0.15,
                },
                Neighbor {
                    id: 31,
                    distance: 0.35,
                },
            ],
        ];
        let k = 5usize;
        let want = SpFreshLayerDbShardedIndex::merge_sorted_neighbors(per_shard.as_slice(), k);
        let got = per_shard
            .into_iter()
            .reduce(|left, right| {
                SpFreshLayerDbShardedIndex::merge_two_sorted_neighbors(left, right, k)
            })
            .unwrap_or_default();
        assert_eq!(got.len(), want.len());
        for (lhs, rhs) in got.iter().zip(want.iter()) {
            assert_eq!(lhs.id, rhs.id);
            assert!(
                (lhs.distance - rhs.distance).abs() < 1e-6,
                "distance mismatch id={} got={} want={}",
                lhs.id,
                lhs.distance,
                rhs.distance
            );
        }
    }

    #[test]
    fn exact_shard_prune_matches_full_shard_search() -> anyhow::Result<()> {
        let dir_full = TempDir::new()?;
        let dir_pruned = TempDir::new()?;
        let mut cfg_full = SpFreshLayerDbShardedConfig {
            shard_count: 8,
            ..Default::default()
        };
        cfg_full.exact_shard_prune = false;
        let mut cfg_pruned = cfg_full.clone();
        cfg_pruned.exact_shard_prune = true;

        let mut full = SpFreshLayerDbShardedIndex::open(dir_full.path(), cfg_full.clone())?;
        let mut pruned = SpFreshLayerDbShardedIndex::open(dir_pruned.path(), cfg_pruned.clone())?;
        let mut rows = Vec::new();
        for id in 0..4_000u64 {
            let mut v = vec![0.0f32; cfg_full.shard.spfresh.dim];
            v[0] = (id as f32 * 0.013).sin();
            v[1] = (id as f32 * 0.017).cos();
            v[2] = (id % 97) as f32 / 97.0;
            rows.push(crate::types::VectorRecord::new(id, v));
        }
        assert_eq!(full.try_upsert_batch_owned(rows.clone())?, rows.len());
        assert_eq!(pruned.try_upsert_batch_owned(rows.clone())?, rows.len());

        let mut queries = Vec::new();
        for i in 0..24usize {
            let mut q = vec![0.0f32; cfg_full.shard.spfresh.dim];
            q[0] = (i as f32 * 0.071).sin();
            q[1] = (i as f32 * 0.047).cos();
            q[2] = (i % 13) as f32 / 13.0;
            queries.push(q);
        }

        for query in queries {
            let got_full = full.search(query.as_slice(), 10);
            let got_pruned = pruned.search(query.as_slice(), 10);
            assert_eq!(got_full.len(), got_pruned.len());
            for (a, b) in got_full.iter().zip(got_pruned.iter()) {
                assert_eq!(a.id, b.id);
                assert!(
                    (a.distance - b.distance).abs() <= 1e-6,
                    "distance mismatch id={} {} vs {}",
                    a.id,
                    a.distance,
                    b.distance
                );
            }
        }
        Ok(())
    }

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
    fn sharded_singleton_batch_apis_round_trip() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let cfg = SpFreshLayerDbShardedConfig {
            shard_count: 4,
            ..Default::default()
        };
        let mut idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg.clone())?;

        assert_eq!(
            idx.try_upsert_batch_owned(vec![crate::types::VectorRecord::new(
                7,
                vec![0.7; cfg.shard.spfresh.dim]
            )])?,
            1
        );
        assert_eq!(idx.try_delete_batch(&[7])?, 1);

        let result = idx.try_apply_batch_owned(vec![VectorMutation::Upsert(
            crate::types::VectorRecord::new(7, vec![0.75; cfg.shard.spfresh.dim]),
        )])?;
        assert_eq!(result.upserts, 1);
        assert_eq!(result.deletes, 0);

        let got = idx.search(&vec![0.75; 64], 1);
        assert_eq!(got[0].id, 7);
        Ok(())
    }

    #[test]
    fn sharded_upsert_batch_rejects_invalid_row_even_if_overwritten() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let cfg = SpFreshLayerDbShardedConfig {
            shard_count: 4,
            ..Default::default()
        };
        let mut idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg.clone())?;

        let bad_then_good = vec![
            crate::types::VectorRecord::new(11, vec![0.1; cfg.shard.spfresh.dim - 1]),
            crate::types::VectorRecord::new(11, vec![0.2; cfg.shard.spfresh.dim]),
        ];
        assert!(idx.try_upsert_batch(&bad_then_good).is_err());
        assert!(idx.try_upsert_batch_owned(bad_then_good).is_err());
        Ok(())
    }

    #[test]
    fn sharded_apply_batch_rejects_invalid_upsert_even_if_overwritten() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let cfg = SpFreshLayerDbShardedConfig {
            shard_count: 4,
            ..Default::default()
        };
        let mut idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg.clone())?;

        let bad_then_good = vec![
            VectorMutation::Upsert(crate::types::VectorRecord::new(
                21,
                vec![0.1; cfg.shard.spfresh.dim - 1],
            )),
            VectorMutation::Upsert(crate::types::VectorRecord::new(
                21,
                vec![0.2; cfg.shard.spfresh.dim],
            )),
        ];
        assert!(idx.try_apply_batch(&bad_then_good).is_err());
        assert!(idx.try_apply_batch_owned(bad_then_good).is_err());
        Ok(())
    }

    #[test]
    fn sharded_open_existing_uses_persisted_dim_for_validation() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let mut cfg = SpFreshLayerDbShardedConfig {
            shard_count: 2,
            ..Default::default()
        };
        cfg.shard.spfresh.dim = 16;

        {
            let mut idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg.clone())?;
            idx.try_upsert(1, vec![0.1; cfg.shard.spfresh.dim])?;
            idx.close()?;
        }

        let mut idx =
            SpFreshLayerDbShardedIndex::open_existing(dir.path(), 2, DbOptions::default())?;
        assert_eq!(
            idx.try_upsert_batch(&[crate::types::VectorRecord::new(2, vec![0.2; 16])])?,
            1
        );
        assert!(idx
            .try_upsert_batch(&[crate::types::VectorRecord::new(3, vec![0.3; 64])])
            .is_err());
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

    #[test]
    fn sharded_mixed_mutation_batch_last_write_wins() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let cfg = SpFreshLayerDbShardedConfig {
            shard_count: 4,
            ..Default::default()
        };
        {
            let mut idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg.clone())?;
            let result = idx.try_apply_batch(&[
                VectorMutation::Upsert(crate::types::VectorRecord::new(
                    1,
                    vec![0.1; cfg.shard.spfresh.dim],
                )),
                VectorMutation::Upsert(crate::types::VectorRecord::new(
                    2,
                    vec![0.2; cfg.shard.spfresh.dim],
                )),
                VectorMutation::Delete { id: 1 },
                VectorMutation::Upsert(crate::types::VectorRecord::new(
                    1,
                    vec![0.15; cfg.shard.spfresh.dim],
                )),
                VectorMutation::Delete { id: 2 },
            ])?;
            assert_eq!(result.upserts, 1);
            assert_eq!(result.deletes, 0);
            idx.close()?;
        }

        let idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg)?;
        assert_eq!(idx.len(), 1);
        let got = idx.search(&vec![0.15; 64], 1);
        assert_eq!(got[0].id, 1);
        Ok(())
    }

    #[test]
    fn sharded_mixed_mutation_batch_owned_last_write_wins() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let cfg = SpFreshLayerDbShardedConfig {
            shard_count: 4,
            ..Default::default()
        };
        {
            let mut idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg.clone())?;
            let result = idx.try_apply_batch_owned(vec![
                VectorMutation::Upsert(crate::types::VectorRecord::new(
                    1,
                    vec![0.1; cfg.shard.spfresh.dim],
                )),
                VectorMutation::Upsert(crate::types::VectorRecord::new(
                    2,
                    vec![0.2; cfg.shard.spfresh.dim],
                )),
                VectorMutation::Delete { id: 1 },
                VectorMutation::Upsert(crate::types::VectorRecord::new(
                    1,
                    vec![0.15; cfg.shard.spfresh.dim],
                )),
                VectorMutation::Delete { id: 2 },
            ])?;
            assert_eq!(result.upserts, 1);
            assert_eq!(result.deletes, 0);
            idx.close()?;
        }

        let idx = SpFreshLayerDbShardedIndex::open(dir.path(), cfg)?;
        assert_eq!(idx.len(), 1);
        let got = idx.search(&vec![0.15; 64], 1);
        assert_eq!(got[0].id, 1);
        Ok(())
    }
}
