use std::collections::{HashMap, HashSet};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::linalg::{mean, squared_l2};
use crate::types::{Neighbor, VectorRecord};

use super::kmeans::l2_kmeans;
use super::SpFreshConfig;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Posting {
    id: usize,
    centroid: Vec<f32>,
    members: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct OffHeapPostingMeta {
    pub id: usize,
    pub centroid: Vec<f32>,
    pub size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct OffHeapMetaSnapshot {
    pub postings: Vec<OffHeapPostingMeta>,
    pub vector_posting: HashMap<u64, usize>,
    pub next_posting_id: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SpFreshOffHeapIndex {
    cfg: SpFreshConfig,
    postings: HashMap<usize, Posting>,
    vector_posting: HashMap<u64, usize>,
    next_posting_id: usize,
}

impl SpFreshOffHeapIndex {
    pub(crate) fn new(cfg: SpFreshConfig) -> Self {
        Self {
            cfg,
            postings: HashMap::new(),
            vector_posting: HashMap::new(),
            next_posting_id: 0,
        }
    }

    pub(crate) fn build(cfg: SpFreshConfig, base: &[VectorRecord]) -> Self {
        let mut out = Self::new(cfg);
        if base.is_empty() {
            return out;
        }

        let vectors: Vec<Vec<f32>> = base.iter().map(|r| r.values.clone()).collect();
        let k = out.cfg.initial_postings.max(1).min(vectors.len());
        let centroids = l2_kmeans(&vectors, k, out.cfg.kmeans_iters);
        for centroid in centroids {
            let pid = out.alloc_posting(centroid);
            out.postings.entry(pid).or_insert_with(|| Posting {
                id: pid,
                centroid: vec![0.0; out.cfg.dim],
                members: Vec::new(),
            });
        }

        let mut base_vectors = HashMap::with_capacity(base.len());
        for row in base {
            base_vectors.insert(row.id, row.values.clone());
            let posting_id = out
                .nearest_postings(&row.values, 1)
                .first()
                .map(|(id, _)| *id)
                .unwrap_or_else(|| out.alloc_posting(row.values.clone()));
            out.assign(row.id, posting_id);
        }

        let mut loader = |id: u64| Ok(base_vectors.get(&id).cloned());
        let posting_ids: Vec<usize> = out.postings.keys().copied().collect();
        for pid in posting_ids {
            out.recompute_centroid_with(pid, &mut loader)
                .expect("build-time centroid recompute");
        }
        out
    }

    pub(crate) fn len(&self) -> usize {
        self.vector_posting.len()
    }

    pub(crate) fn export_metadata(&self) -> OffHeapMetaSnapshot {
        let postings = self
            .postings
            .values()
            .map(|posting| OffHeapPostingMeta {
                id: posting.id,
                centroid: posting.centroid.clone(),
                size: posting.members.len() as u64,
            })
            .collect::<Vec<_>>();
        OffHeapMetaSnapshot {
            postings,
            vector_posting: self.vector_posting.clone(),
            next_posting_id: self.next_posting_id,
        }
    }

    fn alloc_posting(&mut self, centroid: Vec<f32>) -> usize {
        let id = self.next_posting_id;
        self.next_posting_id += 1;
        self.postings.insert(
            id,
            Posting {
                id,
                centroid,
                members: Vec::new(),
            },
        );
        id
    }

    fn nearest_postings(&self, query: &[f32], n: usize) -> Vec<(usize, f32)> {
        let mut all: Vec<(usize, f32)> = self
            .postings
            .values()
            .map(|p| (p.id, squared_l2(query, &p.centroid)))
            .collect();
        all.sort_by(|a, b| a.1.total_cmp(&b.1));
        all.truncate(n);
        all
    }

    fn assign(&mut self, id: u64, posting_id: usize) {
        if let Some(prev) = self.vector_posting.get(&id).copied() {
            if prev == posting_id {
                return;
            }
            if let Some(prev_posting) = self.postings.get_mut(&prev) {
                prev_posting.members.retain(|x| *x != id);
            }
        }
        self.vector_posting.insert(id, posting_id);
        if let Some(posting) = self.postings.get_mut(&posting_id) {
            if !posting.members.contains(&id) {
                posting.members.push(id);
            }
        }
    }

    fn recompute_centroid_with<F>(&mut self, posting_id: usize, load: &mut F) -> anyhow::Result<()>
    where
        F: FnMut(u64) -> anyhow::Result<Option<Vec<f32>>>,
    {
        let Some(posting) = self.postings.get(&posting_id) else {
            return Ok(());
        };
        let member_ids = posting.members.clone();
        let mut live_members = Vec::with_capacity(member_ids.len());
        let mut rows = Vec::with_capacity(member_ids.len());
        let mut stale_ids = Vec::new();
        for id in member_ids {
            match load(id).with_context(|| format!("load vector for centroid recompute id={id}"))? {
                Some(values) => {
                    live_members.push(id);
                    rows.push(values);
                }
                None => stale_ids.push(id),
            }
        }
        for id in stale_ids {
            self.vector_posting.remove(&id);
        }

        if live_members.is_empty() {
            self.postings.remove(&posting_id);
            return Ok(());
        }
        let centroid = mean(&rows, self.cfg.dim);
        if let Some(p) = self.postings.get_mut(&posting_id) {
            p.members = live_members;
            p.centroid = centroid;
        }
        Ok(())
    }

    fn split_if_needed_with<F>(&mut self, posting_id: usize, load: &mut F) -> anyhow::Result<()>
    where
        F: FnMut(u64) -> anyhow::Result<Option<Vec<f32>>>,
    {
        let Some(posting) = self.postings.get(&posting_id) else {
            return Ok(());
        };
        if posting.members.len() <= self.cfg.split_limit || posting.members.len() < 4 {
            return Ok(());
        }
        let old_centroid = posting.centroid.clone();
        let members = posting.members.clone();

        let mut vectors = Vec::with_capacity(members.len());
        let mut stale_ids = Vec::new();
        for id in members {
            match load(id).with_context(|| format!("load vector for split id={id}"))? {
                Some(values) => vectors.push((id, values)),
                None => stale_ids.push(id),
            }
        }
        if !stale_ids.is_empty() {
            let stale: HashSet<u64> = stale_ids.iter().copied().collect();
            if let Some(p) = self.postings.get_mut(&posting_id) {
                p.members.retain(|id| !stale.contains(id));
            }
            for id in stale_ids {
                self.vector_posting.remove(&id);
            }
        }
        if vectors.len() < 4 {
            self.recompute_centroid_with(posting_id, load)?;
            return Ok(());
        }

        let points: Vec<Vec<f32>> = vectors.iter().map(|(_, v)| v.clone()).collect();
        let seeds = pick_two_far_apart(&points);
        let mut centroids = vec![points[seeds.0].clone(), points[seeds.1].clone()];
        let mut assign = balanced_partition(&points, &centroids);
        for _ in 0..self.cfg.kmeans_iters.max(2) {
            assign = balanced_partition(&points, &centroids);
            for (cid, centroid) in centroids.iter_mut().enumerate().take(2) {
                let bucket: Vec<Vec<f32>> = points
                    .iter()
                    .enumerate()
                    .filter_map(|(i, p)| (assign[i] == cid).then_some(p.clone()))
                    .collect();
                if !bucket.is_empty() {
                    *centroid = mean(&bucket, self.cfg.dim);
                }
            }
        }

        let new_a = self.alloc_posting(centroids[0].clone());
        let new_b = self.alloc_posting(centroids[1].clone());
        for (idx, (id, _)) in vectors.iter().enumerate() {
            let target = if assign[idx] == 0 { new_a } else { new_b };
            self.assign(*id, target);
        }

        self.postings.remove(&posting_id);
        self.recompute_centroid_with(new_a, load)?;
        self.recompute_centroid_with(new_b, load)?;
        self.lire_reassign_with(old_centroid, new_a, new_b, load)?;

        self.split_if_needed_with(new_a, load)?;
        self.split_if_needed_with(new_b, load)?;
        Ok(())
    }

    fn lire_reassign_with<F>(
        &mut self,
        old_centroid: Vec<f32>,
        new_a: usize,
        new_b: usize,
        load: &mut F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(u64) -> anyhow::Result<Option<Vec<f32>>>,
    {
        let Some(a_centroid) = self.postings.get(&new_a).map(|p| p.centroid.clone()) else {
            return Ok(());
        };
        let Some(b_centroid) = self.postings.get(&new_b).map(|p| p.centroid.clone()) else {
            return Ok(());
        };

        let mut scope: Vec<(usize, f32)> = self
            .postings
            .values()
            .filter(|p| p.id != new_a && p.id != new_b)
            .map(|p| (p.id, squared_l2(&p.centroid, &old_centroid)))
            .collect();
        scope.sort_by(|x, y| x.1.total_cmp(&y.1));
        let scoped_neighbors: Vec<usize> = scope
            .into_iter()
            .take(self.cfg.reassign_range)
            .map(|(pid, _)| pid)
            .collect();

        let mut reassign = Vec::new();

        for pid in [new_a, new_b] {
            let Some(posting) = self.postings.get(&pid) else {
                continue;
            };
            let members = posting.members.clone();
            let current_centroid = posting.centroid.clone();
            for vid in &members {
                let Some(values) = load(*vid)
                    .with_context(|| format!("load vector for LIRE split posting id={vid}"))?
                else {
                    continue;
                };
                let d_old = squared_l2(&values, &old_centroid);
                let d_a = squared_l2(&values, &a_centroid);
                let d_b = squared_l2(&values, &b_centroid);
                let cond1 = d_old <= d_a && d_old <= d_b;
                if !cond1 {
                    continue;
                }

                let mut best = pid;
                let mut best_dist = squared_l2(&values, &current_centroid);
                for neighbor_pid in &scoped_neighbors {
                    let Some(candidate) = self.postings.get(neighbor_pid) else {
                        continue;
                    };
                    let d = squared_l2(&values, &candidate.centroid);
                    if d < best_dist {
                        best = *neighbor_pid;
                        best_dist = d;
                    }
                }
                if best != pid {
                    reassign.push((*vid, pid, best));
                }
            }
        }

        for pid in &scoped_neighbors {
            let Some(posting) = self.postings.get(pid) else {
                continue;
            };
            let members = posting.members.clone();
            let current_centroid = posting.centroid.clone();
            for vid in &members {
                let Some(values) = load(*vid)
                    .with_context(|| format!("load vector for LIRE neighbor posting id={vid}"))?
                else {
                    continue;
                };
                let d_old = squared_l2(&values, &old_centroid);
                let d_a = squared_l2(&values, &a_centroid);
                let d_b = squared_l2(&values, &b_centroid);
                let cond2 = d_a <= d_old || d_b <= d_old;
                if !cond2 {
                    continue;
                }
                let (best, best_dist) = if d_a <= d_b {
                    (new_a, d_a)
                } else {
                    (new_b, d_b)
                };
                let current_dist = squared_l2(&values, &current_centroid);
                if best_dist < current_dist {
                    reassign.push((*vid, *pid, best));
                }
            }
        }

        if reassign.is_empty() {
            return Ok(());
        }

        let mut touched = HashSet::new();
        for (vid, from, to) in reassign {
            if self.vector_posting.get(&vid).copied() != Some(from) {
                continue;
            }
            if let Some(p) = self.postings.get_mut(&from) {
                p.members.retain(|x| *x != vid);
            }
            if let Some(p) = self.postings.get_mut(&to) {
                if !p.members.contains(&vid) {
                    p.members.push(vid);
                }
            }
            self.vector_posting.insert(vid, to);
            touched.insert(from);
            touched.insert(to);
        }

        for pid in touched {
            self.recompute_centroid_with(pid, load)?;
        }
        Ok(())
    }

    fn merge_if_needed_with<F>(&mut self, posting_id: usize, load: &mut F) -> anyhow::Result<()>
    where
        F: FnMut(u64) -> anyhow::Result<Option<Vec<f32>>>,
    {
        if self.postings.len() <= 1 {
            return Ok(());
        }
        let Some(current) = self.postings.get(&posting_id) else {
            return Ok(());
        };
        if current.members.len() >= self.cfg.merge_limit || current.members.is_empty() {
            return Ok(());
        }

        let target = self
            .postings
            .values()
            .filter(|p| p.id != posting_id)
            .min_by(|a, b| {
                squared_l2(&current.centroid, &a.centroid)
                    .total_cmp(&squared_l2(&current.centroid, &b.centroid))
            })
            .map(|p| p.id);

        let Some(target) = target else {
            return Ok(());
        };

        let members = current.members.clone();
        for vid in members {
            self.assign(vid, target);
        }
        self.postings.remove(&posting_id);
        self.recompute_centroid_with(target, load)?;
        Ok(())
    }

    pub(crate) fn upsert_with<F>(
        &mut self,
        id: u64,
        vector: Vec<f32>,
        load: &mut F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(u64) -> anyhow::Result<Option<Vec<f32>>>,
    {
        if vector.len() != self.cfg.dim {
            return Ok(());
        }
        if self.vector_posting.contains_key(&id) {
            let _ = self.delete_with(id, load)?;
        }

        let posting = self
            .nearest_postings(&vector, 1)
            .first()
            .map(|(id, _)| *id)
            .unwrap_or_else(|| self.alloc_posting(vector.clone()));
        self.assign(id, posting);
        self.recompute_centroid_with(posting, load)?;
        self.split_if_needed_with(posting, load)?;
        Ok(())
    }

    pub(crate) fn delete_with<F>(&mut self, id: u64, load: &mut F) -> anyhow::Result<bool>
    where
        F: FnMut(u64) -> anyhow::Result<Option<Vec<f32>>>,
    {
        let Some(posting_id) = self.vector_posting.remove(&id) else {
            return Ok(false);
        };
        if let Some(posting) = self.postings.get_mut(&posting_id) {
            posting.members.retain(|x| *x != id);
        }
        self.recompute_centroid_with(posting_id, load)?;
        self.merge_if_needed_with(posting_id, load)?;
        Ok(true)
    }

    pub(crate) fn search_with<F>(
        &self,
        query: &[f32],
        k: usize,
        load: &mut F,
    ) -> anyhow::Result<Vec<Neighbor>>
    where
        F: FnMut(u64) -> anyhow::Result<Option<Vec<f32>>>,
    {
        if query.len() != self.cfg.dim || k == 0 {
            return Ok(Vec::new());
        }

        let probe_count = self
            .cfg
            .nprobe
            .max(k)
            .saturating_mul(8)
            .max(1)
            .min(self.postings.len().max(1));
        let probes = self.nearest_postings(query, probe_count);
        let mut out = Vec::new();
        for (pid, _) in probes {
            let Some(posting) = self.postings.get(&pid) else {
                continue;
            };
            for id in &posting.members {
                let Some(values) =
                    load(*id).with_context(|| format!("load vector for search id={id}"))?
                else {
                    continue;
                };
                out.push(Neighbor {
                    id: *id,
                    distance: squared_l2(query, &values),
                });
            }
        }
        let cmp = |a: &Neighbor, b: &Neighbor| {
            a.distance
                .total_cmp(&b.distance)
                .then_with(|| a.id.cmp(&b.id))
        };
        if out.len() > k {
            out.select_nth_unstable_by(k - 1, cmp);
            out.truncate(k);
        }
        out.sort_by(cmp);
        Ok(out)
    }
}

fn balanced_partition(points: &[Vec<f32>], centroids: &[Vec<f32>]) -> Vec<usize> {
    if points.len() <= 1 {
        return vec![0; points.len()];
    }
    let mut scored: Vec<(usize, f32)> = points
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let d0 = squared_l2(p, &centroids[0]);
            let d1 = squared_l2(p, &centroids[1]);
            (i, d0 - d1)
        })
        .collect();
    scored.sort_by(|a, b| a.1.total_cmp(&b.1));

    let split = points.len().div_ceil(2);
    let mut assign = vec![0usize; points.len()];
    for (rank, (idx, _)) in scored.into_iter().enumerate() {
        assign[idx] = if rank < split { 0 } else { 1 };
    }
    assign
}

fn pick_two_far_apart(points: &[Vec<f32>]) -> (usize, usize) {
    if points.len() < 2 {
        return (0, 0);
    }
    let mut best = (0usize, 1usize);
    let mut best_dist = squared_l2(&points[0], &points[1]);
    for i in 0..points.len() {
        for j in (i + 1)..points.len() {
            let d = squared_l2(&points[i], &points[j]);
            if d > best_dist {
                best = (i, j);
                best_dist = d;
            }
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::SpFreshOffHeapIndex;
    use crate::index::SpFreshConfig;
    use crate::types::VectorRecord;

    #[test]
    fn offheap_index_round_trip_with_loader() {
        let cfg = SpFreshConfig {
            dim: 2,
            initial_postings: 2,
            split_limit: 4,
            merge_limit: 1,
            reassign_range: 2,
            nprobe: 2,
            diskmeta_probe_multiplier: 1,
            kmeans_iters: 4,
        };
        let base = vec![
            VectorRecord::new(1, vec![0.0, 0.0]),
            VectorRecord::new(2, vec![1.0, 0.0]),
            VectorRecord::new(3, vec![5.0, 0.0]),
        ];
        let mut source: HashMap<u64, Vec<f32>> =
            base.iter().map(|r| (r.id, r.values.clone())).collect();
        let mut idx = SpFreshOffHeapIndex::build(cfg, &base);

        source.insert(4, vec![0.2, 0.0]);
        {
            let mut loader = |id: u64| Ok(source.get(&id).cloned());
            idx.upsert_with(4, vec![0.2, 0.0], &mut loader)
                .expect("upsert");
        }

        let mut loader = |id: u64| Ok(source.get(&id).cloned());
        let got = idx
            .search_with(&[0.1, 0.0], 4, &mut loader)
            .expect("search");
        assert_eq!(got.len(), 4);
        assert!(got.iter().any(|n| n.id == 1));
        assert!(got.iter().any(|n| n.id == 4));
    }
}
