use std::collections::HashMap;

use crate::linalg::{argmin_l2, mean, squared_l2};
use crate::types::{Neighbor, VectorIndex, VectorRecord};

#[derive(Clone, Debug)]
pub struct AppendOnlyConfig {
    pub dim: usize,
    pub initial_postings: usize,
    pub nprobe: usize,
    pub kmeans_iters: usize,
}

impl Default for AppendOnlyConfig {
    fn default() -> Self {
        Self {
            dim: 64,
            initial_postings: 64,
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

pub struct AppendOnlyIndex {
    cfg: AppendOnlyConfig,
    postings: HashMap<usize, Posting>,
    vectors: HashMap<u64, VectorRecord>,
    vector_posting: HashMap<u64, usize>,
    next_posting_id: usize,
}

impl AppendOnlyIndex {
    pub fn new(cfg: AppendOnlyConfig) -> Self {
        Self {
            cfg,
            postings: HashMap::new(),
            vectors: HashMap::new(),
            vector_posting: HashMap::new(),
            next_posting_id: 0,
        }
    }

    pub fn build(cfg: AppendOnlyConfig, base: &[VectorRecord]) -> Self {
        let mut out = Self::new(cfg);
        if base.is_empty() {
            return out;
        }

        let vectors: Vec<Vec<f32>> = base.iter().map(|r| r.values.clone()).collect();
        let k = out.cfg.initial_postings.max(1).min(vectors.len());
        let centroids = kmeans(&vectors, k, out.cfg.kmeans_iters);
        for centroid in centroids {
            out.alloc_posting(centroid);
        }

        for row in base {
            out.vectors.insert(row.id, row.clone());
            let pid = out
                .nearest_postings(&row.values, 1)
                .first()
                .map(|(id, _)| *id)
                .unwrap_or_else(|| out.alloc_posting(row.values.clone()));
            out.assign(row.id, pid);
        }
        let ids: Vec<usize> = out.postings.keys().copied().collect();
        for id in ids {
            out.recompute_centroid(id);
        }
        out
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

    fn assign(&mut self, id: u64, pid: usize) {
        if let Some(prev) = self.vector_posting.get(&id).copied() {
            if prev == pid {
                return;
            }
            if let Some(prev_posting) = self.postings.get_mut(&prev) {
                prev_posting.members.retain(|x| *x != id);
            }
        }
        self.vector_posting.insert(id, pid);
        if let Some(posting) = self.postings.get_mut(&pid) {
            if !posting.members.contains(&id) {
                posting.members.push(id);
            }
        }
    }

    fn recompute_centroid(&mut self, pid: usize) {
        let Some(posting) = self.postings.get(&pid) else {
            return;
        };
        let rows: Vec<Vec<f32>> = posting
            .members
            .iter()
            .filter_map(|id| self.vectors.get(id).map(|r| r.values.clone()))
            .collect();
        if rows.is_empty() {
            return;
        }
        let centroid = mean(&rows, self.cfg.dim);
        if let Some(p) = self.postings.get_mut(&pid) {
            p.centroid = centroid;
        }
    }
}

impl VectorIndex for AppendOnlyIndex {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        if vector.len() != self.cfg.dim {
            return;
        }
        let _ = self.delete(id);
        self.vectors.insert(id, VectorRecord::new(id, vector.clone()));
        let posting = self
            .nearest_postings(&vector, 1)
            .first()
            .map(|(id, _)| *id)
            .unwrap_or_else(|| self.alloc_posting(vector.clone()));
        self.assign(id, posting);
        self.recompute_centroid(posting);
    }

    fn delete(&mut self, id: u64) -> bool {
        let Some(pid) = self.vector_posting.remove(&id) else {
            return false;
        };
        if let Some(posting) = self.postings.get_mut(&pid) {
            posting.members.retain(|x| *x != id);
        }
        self.vectors.remove(&id);
        self.recompute_centroid(pid);
        true
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<Neighbor> {
        if query.len() != self.cfg.dim || k == 0 {
            return Vec::new();
        }
        let mut out = Vec::new();
        for (pid, _) in self.nearest_postings(query, self.cfg.nprobe.max(1)) {
            let Some(posting) = self.postings.get(&pid) else {
                continue;
            };
            for vid in &posting.members {
                let Some(row) = self.vectors.get(vid) else {
                    continue;
                };
                out.push(Neighbor {
                    id: *vid,
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
        for (cid, centroid) in centroids.iter_mut().enumerate().take(k) {
            let rows: Vec<Vec<f32>> = vectors
                .iter()
                .enumerate()
                .filter_map(|(idx, v)| (assign[idx] == cid).then_some(v.clone()))
                .collect();
            if !rows.is_empty() {
                *centroid = mean(&rows, dim);
            }
        }
    }
    centroids
}

#[cfg(test)]
mod tests {
    use crate::types::VectorIndex;

    use super::{AppendOnlyConfig, AppendOnlyIndex};

    #[test]
    fn append_only_search_and_delete() {
        let mut idx = AppendOnlyIndex::new(AppendOnlyConfig {
            dim: 2,
            initial_postings: 1,
            nprobe: 1,
            kmeans_iters: 2,
        });
        idx.upsert(1, vec![0.0, 0.0]);
        idx.upsert(2, vec![1.0, 0.0]);
        idx.upsert(3, vec![5.0, 0.0]);
        let got = idx.search(&[0.9, 0.0], 2);
        assert_eq!(got[0].id, 2);
        assert!(idx.delete(2));
        let got = idx.search(&[0.9, 0.0], 2);
        assert!(got.iter().all(|n| n.id != 2));
    }
}
