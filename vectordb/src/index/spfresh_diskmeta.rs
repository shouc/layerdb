use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::linalg::squared_l2;
use crate::types::VectorRecord;

use super::kmeans::l2_kmeans;
use super::spfresh_offheap::SpFreshOffHeapIndex;
use super::SpFreshConfig;

fn default_coarse_refresh_interval() -> u64 {
    2_048
}

fn default_zero_u64() -> u64 {
    0
}

fn default_empty_posting_to_coarse() -> HashMap<usize, usize> {
    HashMap::new()
}

fn default_empty_coarse_to_postings() -> HashMap<usize, Vec<usize>> {
    HashMap::new()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DiskPosting {
    pub id: usize,
    pub centroid: Vec<f32>,
    pub size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SpFreshDiskMetaIndex {
    cfg: SpFreshConfig,
    postings: HashMap<usize, DiskPosting>,
    next_posting_id: usize,
    total_rows: u64,
    #[serde(default)]
    coarse_centroids: Vec<Vec<f32>>,
    #[serde(default = "default_empty_posting_to_coarse")]
    posting_to_coarse: HashMap<usize, usize>,
    #[serde(default = "default_empty_coarse_to_postings")]
    coarse_to_postings: HashMap<usize, Vec<usize>>,
    #[serde(default = "default_zero_u64")]
    mutations_since_coarse_refresh: u64,
    #[serde(default = "default_coarse_refresh_interval")]
    coarse_refresh_interval: u64,
}

impl SpFreshDiskMetaIndex {
    pub(crate) fn build_with_assignments(
        cfg: SpFreshConfig,
        rows: &[VectorRecord],
    ) -> (Self, HashMap<u64, usize>) {
        Self::build_from_rows_with_assignments(cfg, rows, None)
    }

    pub(crate) fn build_from_rows_with_assignments(
        cfg: SpFreshConfig,
        rows: &[VectorRecord],
        known_assignments: Option<&HashMap<u64, usize>>,
    ) -> (Self, HashMap<u64, usize>) {
        let mut out = Self {
            cfg,
            postings: HashMap::new(),
            next_posting_id: 0,
            total_rows: 0,
            coarse_centroids: Vec::new(),
            posting_to_coarse: HashMap::new(),
            coarse_to_postings: HashMap::new(),
            mutations_since_coarse_refresh: 0,
            coarse_refresh_interval: default_coarse_refresh_interval(),
        };
        if rows.is_empty() {
            return (out, HashMap::new());
        }

        if known_assignments.is_none() {
            let built = SpFreshOffHeapIndex::build(out.cfg.clone(), rows);
            let snapshot = built.export_metadata();
            let mut postings = HashMap::with_capacity(snapshot.postings.len());
            for posting in snapshot.postings {
                postings.insert(
                    posting.id,
                    DiskPosting {
                        id: posting.id,
                        centroid: posting.centroid,
                        size: posting.size,
                    },
                );
            }
            out.postings = postings;
            out.next_posting_id = snapshot.next_posting_id;
            out.total_rows = snapshot.vector_posting.len() as u64;
            out.rebuild_coarse_index();
            return (out, snapshot.vector_posting);
        }

        let vectors: Vec<Vec<f32>> = rows.iter().map(|r| r.values.clone()).collect();
        let k = out.cfg.initial_postings.max(1).min(vectors.len());
        let seeds = l2_kmeans(&vectors, k, out.cfg.kmeans_iters);
        for centroid in seeds {
            out.alloc_posting(centroid);
        }

        let mut assigns = HashMap::with_capacity(rows.len());
        let mut sums: HashMap<usize, Vec<f64>> = HashMap::new();
        let mut counts: HashMap<usize, u64> = HashMap::new();
        for row in rows {
            let posting_id = if let Some(assigns) = known_assignments {
                if let Some(pid) = assigns.get(&row.id).copied() {
                    if let std::collections::hash_map::Entry::Vacant(entry) =
                        out.postings.entry(pid)
                    {
                        entry.insert(DiskPosting {
                            id: pid,
                            centroid: row.values.clone(),
                            size: 0,
                        });
                        if out.next_posting_id <= pid {
                            out.next_posting_id = pid + 1;
                        }
                    }
                    pid
                } else {
                    out.nearest_postings(&row.values, 1)
                        .first()
                        .map(|(pid, _)| *pid)
                        .unwrap_or_else(|| out.alloc_posting(row.values.clone()))
                }
            } else {
                out.nearest_postings(&row.values, 1)
                    .first()
                    .map(|(pid, _)| *pid)
                    .unwrap_or_else(|| out.alloc_posting(row.values.clone()))
            };
            assigns.insert(row.id, posting_id);
            let sum = sums
                .entry(posting_id)
                .or_insert_with(|| vec![0.0; out.cfg.dim]);
            for (i, value) in row.values.iter().enumerate() {
                sum[i] += *value as f64;
            }
            *counts.entry(posting_id).or_insert(0) += 1;
        }

        out.total_rows = rows.len() as u64;
        for (pid, posting) in &mut out.postings {
            let count = counts.get(pid).copied().unwrap_or(0);
            posting.size = count;
            if count == 0 {
                continue;
            }
            if let Some(sum) = sums.get(pid) {
                posting.centroid = sum
                    .iter()
                    .map(|v| (*v / count as f64) as f32)
                    .collect::<Vec<_>>();
            }
        }

        out.postings.retain(|_, posting| posting.size > 0);
        out.rebuild_coarse_index();
        (out, assigns)
    }

    fn alloc_posting(&mut self, centroid: Vec<f32>) -> usize {
        let id = self.next_posting_id;
        self.next_posting_id += 1;
        self.postings.insert(
            id,
            DiskPosting {
                id,
                centroid,
                size: 0,
            },
        );
        id
    }

    fn rebuild_coarse_index(&mut self) {
        self.coarse_centroids.clear();
        self.posting_to_coarse.clear();
        self.coarse_to_postings.clear();

        if self.postings.is_empty() {
            self.mutations_since_coarse_refresh = 0;
            return;
        }

        let posting_ids: Vec<usize> = self.postings.keys().copied().collect();
        let posting_vectors: Vec<Vec<f32>> = posting_ids
            .iter()
            .filter_map(|pid| self.postings.get(pid).map(|p| p.centroid.clone()))
            .collect();
        if posting_vectors.is_empty() {
            self.mutations_since_coarse_refresh = 0;
            return;
        }

        let coarse_k = ((posting_vectors.len() as f64).sqrt() as usize)
            .max(1)
            .min(posting_vectors.len());
        let coarse_centroids = l2_kmeans(&posting_vectors, coarse_k, self.cfg.kmeans_iters.max(1));
        self.coarse_centroids = coarse_centroids;

        for pid in posting_ids {
            let Some(posting) = self.postings.get(&pid) else {
                continue;
            };
            let mut best_cid = 0usize;
            let mut best_dist = f32::INFINITY;
            for (cid, centroid) in self.coarse_centroids.iter().enumerate() {
                let d = squared_l2(&posting.centroid, centroid);
                if d < best_dist {
                    best_dist = d;
                    best_cid = cid;
                }
            }
            self.posting_to_coarse.insert(pid, best_cid);
            self.coarse_to_postings.entry(best_cid).or_default().push(pid);
        }
        self.mutations_since_coarse_refresh = 0;
    }

    fn maybe_refresh_coarse_index(&mut self) {
        self.mutations_since_coarse_refresh = self.mutations_since_coarse_refresh.saturating_add(1);
        if self.mutations_since_coarse_refresh >= self.coarse_refresh_interval.max(1) {
            self.rebuild_coarse_index();
        }
    }

    fn nearest_coarse_centroids(&self, query: &[f32], n: usize) -> Vec<usize> {
        if self.coarse_centroids.is_empty() || n == 0 {
            return Vec::new();
        }
        let mut all: Vec<(usize, f32)> = self
            .coarse_centroids
            .iter()
            .enumerate()
            .map(|(cid, centroid)| (cid, squared_l2(query, centroid)))
            .collect();
        all.sort_by(|a, b| a.1.total_cmp(&b.1));
        all.truncate(n.min(all.len()));
        all.into_iter().map(|(cid, _)| cid).collect()
    }

    fn candidate_postings_for_query(&self, query: &[f32]) -> Vec<usize> {
        if self.postings.len() <= self.cfg.initial_postings.max(1) {
            return self.postings.keys().copied().collect();
        }
        if self.coarse_centroids.is_empty() || self.coarse_to_postings.is_empty() {
            return self.postings.keys().copied().collect();
        }

        let coarse_probe = self
            .cfg
            .nprobe
            .max(1)
            .min(self.coarse_centroids.len().max(1));
        let coarse_ids = self.nearest_coarse_centroids(query, coarse_probe);
        let mut out = Vec::new();
        for cid in coarse_ids {
            if let Some(postings) = self.coarse_to_postings.get(&cid) {
                out.extend(postings.iter().copied());
            }
        }
        if out.is_empty() {
            self.postings.keys().copied().collect()
        } else {
            out.sort_unstable();
            out.dedup();
            out
        }
    }

    fn nearest_postings(&self, query: &[f32], n: usize) -> Vec<(usize, f32)> {
        let candidates = self.candidate_postings_for_query(query);
        let mut all: Vec<(usize, f32)> = candidates
            .iter()
            .filter_map(|pid| self.postings.get(pid).map(|p| (p.id, squared_l2(query, &p.centroid))))
            .collect();
        if all.is_empty() {
            all = self
                .postings
                .values()
                .map(|p| (p.id, squared_l2(query, &p.centroid)))
                .collect();
        }
        all.sort_by(|a, b| a.1.total_cmp(&b.1));
        all.truncate(n);
        all
    }

    pub(crate) fn choose_posting(&self, vector: &[f32]) -> Option<usize> {
        self.nearest_postings(vector, 1)
            .first()
            .map(|(pid, _)| *pid)
    }

    pub(crate) fn posting_centroid(&self, posting_id: usize) -> Option<&[f32]> {
        self.postings.get(&posting_id).map(|p| p.centroid.as_slice())
    }

    pub(crate) fn posting_ids(&self) -> Vec<usize> {
        let mut out: Vec<usize> = self.postings.keys().copied().collect();
        out.sort_unstable();
        out
    }

    pub(crate) fn choose_probe_postings(&self, query: &[f32], k: usize) -> Vec<usize> {
        if query.len() != self.cfg.dim || k == 0 {
            return Vec::new();
        }
        let max_probe = self
            .cfg
            .nprobe
            .max(1)
            .saturating_mul(8)
            .min(self.postings.len().max(1));
        let min_probe = self
            .cfg
            .nprobe
            .max(1)
            .saturating_mul(4)
            .min(max_probe.max(1));
        let mut nearest = self.nearest_postings(query, max_probe);
        if nearest.is_empty() {
            return Vec::new();
        }

        let probe_count = if nearest.len() < 2 || min_probe == max_probe {
            max_probe
        } else {
            // Adaptive probing: confident queries (clear first/second-centroid gap) probe fewer postings.
            let d0 = nearest[0].1.max(1e-12);
            let d1 = nearest[1].1.max(d0);
            let ratio = d1 / d0;
            if ratio >= 4.0 {
                min_probe
            } else if ratio >= 2.0 {
                min_probe.saturating_add((max_probe.saturating_sub(min_probe)) / 3)
            } else if ratio >= 1.4 {
                min_probe.saturating_add((max_probe.saturating_sub(min_probe) * 2) / 3)
            } else {
                max_probe
            }
        };

        nearest.truncate(probe_count.max(1));
        nearest.into_iter().map(|(pid, _)| pid).collect()
    }

    fn remove_from_posting(&mut self, posting_id: usize, old_vector: &[f32]) {
        let Some(posting) = self.postings.get_mut(&posting_id) else {
            return;
        };
        if posting.size <= 1 {
            posting.size = 0;
            return;
        }
        let n = posting.size as f64;
        for (i, v) in old_vector.iter().enumerate() {
            posting.centroid[i] =
                (((posting.centroid[i] as f64 * n) - *v as f64) / (n - 1.0)) as f32;
        }
        posting.size -= 1;
    }

    fn add_to_posting(&mut self, posting_id: usize, new_vector: &[f32]) {
        let Some(posting) = self.postings.get_mut(&posting_id) else {
            return;
        };
        if posting.size == 0 {
            posting.centroid = new_vector.to_vec();
            posting.size = 1;
            return;
        }
        let n = posting.size as f64;
        for (i, v) in new_vector.iter().enumerate() {
            posting.centroid[i] =
                (((posting.centroid[i] as f64 * n) + *v as f64) / (n + 1.0)) as f32;
        }
        posting.size += 1;
    }

    pub(crate) fn apply_upsert_ref(
        &mut self,
        old: Option<(usize, &[f32])>,
        new_posting: usize,
        new_vector: &[f32],
    ) {
        match old {
            Some((old_posting, old_vector)) if old_posting == new_posting => {
                if let Some(posting) = self.postings.get_mut(&old_posting) {
                    let n = posting.size.max(1) as f64;
                    for i in 0..self.cfg.dim {
                        let delta = (new_vector[i] - old_vector[i]) as f64 / n;
                        posting.centroid[i] += delta as f32;
                    }
                }
            }
            Some((old_posting, old_vector)) => {
                self.remove_from_posting(old_posting, old_vector);
                self.add_to_posting(new_posting, new_vector);
            }
            None => {
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    self.postings.entry(new_posting)
                {
                    entry.insert(DiskPosting {
                        id: new_posting,
                        centroid: new_vector.to_vec(),
                        size: 0,
                    });
                    if self.next_posting_id <= new_posting {
                        self.next_posting_id = new_posting + 1;
                    }
                }
                self.add_to_posting(new_posting, new_vector);
                self.total_rows = self.total_rows.saturating_add(1);
            }
        }
        self.postings.retain(|_, posting| posting.size > 0);
        self.maybe_refresh_coarse_index();
    }

    pub(crate) fn apply_upsert(
        &mut self,
        old: Option<(usize, Vec<f32>)>,
        new_posting: usize,
        new_vector: Vec<f32>,
    ) {
        let old_ref = old.as_ref().map(|(posting, values)| (*posting, values.as_slice()));
        self.apply_upsert_ref(old_ref, new_posting, new_vector.as_slice());
    }

    pub(crate) fn apply_delete_ref(&mut self, old: Option<(usize, &[f32])>) -> bool {
        let Some((old_posting, old_vector)) = old else {
            return false;
        };
        self.remove_from_posting(old_posting, old_vector);
        self.total_rows = self.total_rows.saturating_sub(1);
        self.postings.retain(|_, posting| posting.size > 0);
        self.maybe_refresh_coarse_index();
        true
    }

    pub(crate) fn apply_delete(&mut self, old: Option<(usize, Vec<f32>)>) -> bool {
        let old_ref = old.as_ref().map(|(posting, values)| (*posting, values.as_slice()));
        self.apply_delete_ref(old_ref)
    }

    pub(crate) fn len(&self) -> usize {
        self.total_rows as usize
    }
}

#[cfg(test)]
mod tests {
    use super::SpFreshDiskMetaIndex;
    use crate::index::SpFreshConfig;
    use crate::types::VectorRecord;

    fn synthetic_rows(dim: usize, count: usize) -> Vec<VectorRecord> {
        let mut rows = Vec::with_capacity(count);
        for i in 0..count {
            let mut v = vec![0.0f32; dim];
            v[0] = (i as f32 * 0.37).sin();
            v[1] = (i as f32 * 0.23).cos();
            v[2] = (i % 17) as f32 / 17.0;
            rows.push(VectorRecord::new(i as u64, v));
        }
        rows
    }

    #[test]
    fn coarse_routing_returns_probe_postings() {
        let cfg = SpFreshConfig {
            dim: 16,
            initial_postings: 32,
            nprobe: 4,
            ..Default::default()
        };
        let rows = synthetic_rows(cfg.dim, 2_000);
        let (index, _assignments) = SpFreshDiskMetaIndex::build_with_assignments(cfg, &rows);
        let probes = index.choose_probe_postings(&rows[17].values, 10);
        assert!(!probes.is_empty());
        assert!(probes.len() <= index.postings.len());
    }

    #[test]
    fn coarse_index_refreshes_under_mutation() {
        let cfg = SpFreshConfig {
            dim: 16,
            initial_postings: 16,
            nprobe: 4,
            ..Default::default()
        };
        let rows = synthetic_rows(cfg.dim, 512);
        let (mut index, _assignments) = SpFreshDiskMetaIndex::build_with_assignments(cfg, &rows);
        let before = index.coarse_centroids.len();
        for i in 0..3_000usize {
            let id = rows[i % rows.len()].id;
            let vec = rows[i % rows.len()].values.clone();
            let posting = index.choose_posting(&vec).unwrap_or_default();
            index.apply_upsert(Some((posting, vec.clone())), posting, vec);
            let _ = id;
        }
        assert!(!index.coarse_centroids.is_empty());
        assert!(index.coarse_centroids.len() == before || before == 0);
    }
}
