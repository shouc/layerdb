use std::collections::{HashMap, HashSet};

use crate::linalg::{argmin_l2, mean, squared_l2};
use crate::types::{Neighbor, VectorIndex, VectorRecord};

#[derive(Clone, Debug)]
pub struct SpFreshConfig {
    pub dim: usize,
    pub initial_postings: usize,
    pub split_limit: usize,
    pub merge_limit: usize,
    pub reassign_range: usize,
    pub nprobe: usize,
    pub kmeans_iters: usize,
}

impl Default for SpFreshConfig {
    fn default() -> Self {
        Self {
            dim: 64,
            initial_postings: 64,
            split_limit: 512,
            merge_limit: 64,
            reassign_range: 64,
            nprobe: 8,
            kmeans_iters: 8,
        }
    }
}

#[derive(Clone, Debug)]
struct Posting {
    id: usize,
    centroid: Vec<f32>,
    members: Vec<u64>,
}

pub struct SpFreshIndex {
    cfg: SpFreshConfig,
    postings: HashMap<usize, Posting>,
    vectors: HashMap<u64, VectorRecord>,
    vector_posting: HashMap<u64, usize>,
    next_posting_id: usize,
}

impl SpFreshIndex {
    pub fn new(cfg: SpFreshConfig) -> Self {
        Self {
            cfg,
            postings: HashMap::new(),
            vectors: HashMap::new(),
            vector_posting: HashMap::new(),
            next_posting_id: 0,
        }
    }

    pub fn build(cfg: SpFreshConfig, base: &[VectorRecord]) -> Self {
        let mut out = Self::new(cfg);
        if base.is_empty() {
            return out;
        }

        let vectors: Vec<Vec<f32>> = base.iter().map(|r| r.values.clone()).collect();
        let k = out.cfg.initial_postings.max(1).min(vectors.len());
        let centroids = kmeans(&vectors, k, out.cfg.kmeans_iters);

        for centroid in centroids {
            let pid = out.alloc_posting(centroid);
            out.postings.entry(pid).or_insert_with(|| Posting {
                id: pid,
                centroid: vec![0.0; out.cfg.dim],
                members: Vec::new(),
            });
        }

        for row in base {
            out.vectors.insert(row.id, row.clone());
            let posting_id = out
                .nearest_postings(&row.values, 1)
                .first()
                .map(|(id, _)| *id)
                .unwrap_or_else(|| out.alloc_posting(row.values.clone()));
            out.assign(row.id, posting_id);
        }

        let posting_ids: Vec<usize> = out.postings.keys().copied().collect();
        for pid in posting_ids {
            out.recompute_centroid(pid);
        }

        out
    }

    pub fn posting_count(&self) -> usize {
        self.postings.len()
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

    fn recompute_centroid(&mut self, posting_id: usize) {
        let Some(posting) = self.postings.get(&posting_id) else {
            return;
        };
        let dim = self.cfg.dim;
        let rows: Vec<Vec<f32>> = posting
            .members
            .iter()
            .filter_map(|id| self.vectors.get(id).map(|r| r.values.clone()))
            .collect();

        if rows.is_empty() {
            self.postings.remove(&posting_id);
            return;
        }

        let centroid = mean(&rows, dim);
        if let Some(p) = self.postings.get_mut(&posting_id) {
            p.centroid = centroid;
        }
    }

    fn split_if_needed(&mut self, posting_id: usize) {
        let Some(posting) = self.postings.get(&posting_id) else {
            return;
        };
        if posting.members.len() <= self.cfg.split_limit || posting.members.len() < 4 {
            return;
        }
        let old_centroid = posting.centroid.clone();
        let members = posting.members.clone();

        let mut vectors = Vec::with_capacity(members.len());
        for id in &members {
            if let Some(v) = self.vectors.get(id) {
                vectors.push((v.id, v.values.clone()));
            }
        }
        if vectors.len() < 4 {
            return;
        }

        let points: Vec<Vec<f32>> = vectors.iter().map(|(_, v)| v.clone()).collect();
        let seeds = pick_two_far_apart(&points);
        let mut centroids = vec![points[seeds.0].clone(), points[seeds.1].clone()];
        let mut assign = balanced_partition(&points, &centroids);
        for _ in 0..self.cfg.kmeans_iters.max(2) {
            assign = balanced_partition(&points, &centroids);
            for cid in 0..2 {
                let bucket: Vec<Vec<f32>> = points
                    .iter()
                    .enumerate()
                    .filter_map(|(i, p)| (assign[i] == cid).then_some(p.clone()))
                    .collect();
                if !bucket.is_empty() {
                    centroids[cid] = mean(&bucket, self.cfg.dim);
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
        self.recompute_centroid(new_a);
        self.recompute_centroid(new_b);
        self.lire_reassign(posting_id, &old_centroid, new_a, new_b);

        self.split_if_needed(new_a);
        self.split_if_needed(new_b);
    }

    fn lire_reassign(
        &mut self,
        _old_posting_id: usize,
        old_centroid: &[f32],
        new_a: usize,
        new_b: usize,
    ) {
        let Some(a_centroid) = self.postings.get(&new_a).map(|p| p.centroid.clone()) else {
            return;
        };
        let Some(b_centroid) = self.postings.get(&new_b).map(|p| p.centroid.clone()) else {
            return;
        };

        let mut scope: Vec<(usize, f32)> = self
            .postings
            .values()
            .filter(|p| p.id != new_a && p.id != new_b)
            .map(|p| (p.id, squared_l2(&p.centroid, old_centroid)))
            .collect();
        scope.sort_by(|x, y| x.1.total_cmp(&y.1));

        let scoped_neighbors: Vec<usize> = scope
            .into_iter()
            .take(self.cfg.reassign_range)
            .map(|(pid, _)| pid)
            .collect();

        let mut reassign = Vec::new();

        // Equation (1): only vectors in split postings need this check.
        for pid in [new_a, new_b] {
            let Some(posting) = self.postings.get(&pid) else {
                continue;
            };
            let members = posting.members.clone();
            let current_centroid = posting.centroid.clone();
            for vid in &members {
                let Some(v) = self.vectors.get(vid) else {
                    continue;
                };
                let d_old = squared_l2(&v.values, old_centroid);
                let d_a = squared_l2(&v.values, &a_centroid);
                let d_b = squared_l2(&v.values, &b_centroid);
                let cond1 = d_old <= d_a && d_old <= d_b;
                if !cond1 {
                    continue;
                }

                let mut best = pid;
                let mut best_dist = squared_l2(&v.values, &current_centroid);
                for neighbor_pid in &scoped_neighbors {
                    let Some(candidate) = self.postings.get(neighbor_pid) else {
                        continue;
                    };
                    let d = squared_l2(&v.values, &candidate.centroid);
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

        // Equation (2): only nearby neighbor postings need this check.
        for pid in &scoped_neighbors {
            let Some(posting) = self.postings.get(pid) else {
                continue;
            };
            let members = posting.members.clone();
            let current_centroid = posting.centroid.clone();
            for vid in &members {
                let Some(v) = self.vectors.get(vid) else {
                    continue;
                };
                let d_old = squared_l2(&v.values, old_centroid);
                let d_a = squared_l2(&v.values, &a_centroid);
                let d_b = squared_l2(&v.values, &b_centroid);
                let cond2 = d_a <= d_old || d_b <= d_old;
                if !cond2 {
                    continue;
                }
                let (best, best_dist) = if d_a <= d_b { (new_a, d_a) } else { (new_b, d_b) };
                let current_dist = squared_l2(&v.values, &current_centroid);
                if best_dist < current_dist {
                    reassign.push((*vid, *pid, best));
                }
            }
        }

        if reassign.is_empty() {
            return;
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
            self.recompute_centroid(pid);
        }
    }

    fn merge_if_needed(&mut self, posting_id: usize) {
        if self.postings.len() <= 1 {
            return;
        }
        let Some(current) = self.postings.get(&posting_id) else {
            return;
        };
        if current.members.len() >= self.cfg.merge_limit || current.members.is_empty() {
            return;
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
            return;
        };

        let members = current.members.clone();
        for vid in members {
            self.assign(vid, target);
        }
        self.postings.remove(&posting_id);
        self.recompute_centroid(target);
    }
}

impl VectorIndex for SpFreshIndex {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        if vector.len() != self.cfg.dim {
            return;
        }
        if self.vectors.contains_key(&id) {
            let _ = self.delete(id);
        }
        self.vectors.insert(id, VectorRecord::new(id, vector.clone()));

        let posting = self
            .nearest_postings(&vector, 1)
            .first()
            .map(|(id, _)| *id)
            .unwrap_or_else(|| self.alloc_posting(vector.clone()));

        self.assign(id, posting);
        self.recompute_centroid(posting);
        self.split_if_needed(posting);
    }

    fn delete(&mut self, id: u64) -> bool {
        let Some(posting_id) = self.vector_posting.remove(&id) else {
            return false;
        };
        if let Some(posting) = self.postings.get_mut(&posting_id) {
            posting.members.retain(|x| *x != id);
        }
        self.vectors.remove(&id);
        self.recompute_centroid(posting_id);
        self.merge_if_needed(posting_id);
        true
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<Neighbor> {
        if query.len() != self.cfg.dim || k == 0 {
            return Vec::new();
        }

        let probes = self.nearest_postings(query, self.cfg.nprobe.max(1));
        let mut out = Vec::new();
        for (pid, _) in probes {
            let Some(posting) = self.postings.get(&pid) else {
                continue;
            };
            for id in &posting.members {
                let Some(row) = self.vectors.get(id) else {
                    continue;
                };
                out.push(Neighbor {
                    id: *id,
                    distance: squared_l2(query, &row.values),
                });
            }
        }

        out.sort_by(|a, b| a.distance.total_cmp(&b.distance).then_with(|| a.id.cmp(&b.id)));
        out.truncate(k);
        out
    }

    fn len(&self) -> usize {
        self.vectors.len()
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

fn kmeans(vectors: &[Vec<f32>], k: usize, iters: usize) -> Vec<Vec<f32>> {
    if vectors.is_empty() {
        return Vec::new();
    }
    let dim = vectors[0].len();
    let mut centroids: Vec<Vec<f32>> = vectors.iter().take(k).cloned().collect();
    if centroids.is_empty() {
        return vec![vec![0.0; dim]];
    }
    while centroids.len() < k {
        centroids.push(centroids[0].clone());
    }

    let mut assign = vec![0usize; vectors.len()];
    for _ in 0..iters.max(1) {
        for (i, v) in vectors.iter().enumerate() {
            assign[i] = argmin_l2(v, &centroids).unwrap_or(0);
        }
        for cid in 0..k {
            let rows: Vec<Vec<f32>> = vectors
                .iter()
                .enumerate()
                .filter_map(|(idx, v)| (assign[idx] == cid).then_some(v.clone()))
                .collect();
            if !rows.is_empty() {
                centroids[cid] = mean(&rows, dim);
            }
        }
    }
    centroids
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
    use crate::types::VectorIndex;

    use super::{SpFreshConfig, SpFreshIndex};

    #[test]
    fn spfresh_basic_search() {
        let cfg = SpFreshConfig {
            dim: 2,
            initial_postings: 2,
            split_limit: 3,
            merge_limit: 1,
            reassign_range: 2,
            nprobe: 2,
            kmeans_iters: 4,
        };

        let mut idx = SpFreshIndex::new(cfg);
        idx.upsert(1, vec![0.0, 0.0]);
        idx.upsert(2, vec![1.0, 0.0]);
        idx.upsert(3, vec![5.0, 0.0]);

        let got = idx.search(&[0.8, 0.0], 2);
        assert_eq!(got[0].id, 2);
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn spfresh_split_and_delete() {
        let cfg = SpFreshConfig {
            dim: 4,
            initial_postings: 1,
            split_limit: 4,
            merge_limit: 1,
            reassign_range: 4,
            nprobe: 3,
            kmeans_iters: 4,
        };

        let mut idx = SpFreshIndex::new(cfg);
        for i in 0..20u64 {
            idx.upsert(i, vec![i as f32, 0.0, 0.0, 0.0]);
        }

        assert!(idx.posting_count() > 1);
        assert!(idx.delete(3));
        assert!(!idx.delete(3));
        let result = idx.search(&[3.0, 0.0, 0.0, 0.0], 5);
        assert!(result.iter().all(|n| n.id != 3));
    }
}
