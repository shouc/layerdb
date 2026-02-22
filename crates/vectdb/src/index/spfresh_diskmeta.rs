use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::linalg::{norm2, squared_l2};
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

fn default_empty_posting_to_coarse() -> FxHashMap<usize, usize> {
    FxHashMap::default()
}

fn default_empty_coarse_to_postings() -> FxHashMap<usize, Vec<usize>> {
    FxHashMap::default()
}

fn default_infinite_f32() -> f32 {
    f32::INFINITY
}

fn default_empty_sum_values() -> Vec<f64> {
    Vec::new()
}

fn l2_norm(values: &[f32]) -> f32 {
    norm2(values).sqrt()
}

#[derive(Clone, Debug, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize)]
pub(crate) struct DiskPosting {
    pub id: usize,
    pub centroid: Vec<f32>,
    pub size: u64,
    #[serde(default = "default_infinite_f32")]
    pub max_radius: f32,
    #[serde(default = "default_infinite_f32")]
    pub max_l2_norm: f32,
    #[serde(default = "default_empty_sum_values")]
    pub sum_values: Vec<f64>,
}

#[derive(Clone, Debug, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize)]
pub(crate) struct SpFreshDiskMetaIndex {
    cfg: SpFreshConfig,
    postings: FxHashMap<usize, DiskPosting>,
    next_posting_id: usize,
    total_rows: u64,
    #[serde(default)]
    coarse_centroids: Vec<Vec<f32>>,
    #[serde(default = "default_empty_posting_to_coarse")]
    posting_to_coarse: FxHashMap<usize, usize>,
    #[serde(default = "default_empty_coarse_to_postings")]
    coarse_to_postings: FxHashMap<usize, Vec<usize>>,
    #[serde(default = "default_zero_u64")]
    mutations_since_coarse_refresh: u64,
    #[serde(default = "default_coarse_refresh_interval")]
    coarse_refresh_interval: u64,
}

impl SpFreshDiskMetaIndex {
    pub(crate) fn build_with_assignments(
        cfg: SpFreshConfig,
        rows: &[VectorRecord],
    ) -> (Self, FxHashMap<u64, usize>) {
        Self::build_from_rows_with_assignments(cfg, rows, None)
    }

    pub(crate) fn build_from_rows_with_assignments(
        cfg: SpFreshConfig,
        rows: &[VectorRecord],
        known_assignments: Option<&FxHashMap<u64, usize>>,
    ) -> (Self, FxHashMap<u64, usize>) {
        let mut out = Self {
            cfg,
            postings: FxHashMap::default(),
            next_posting_id: 0,
            total_rows: 0,
            coarse_centroids: Vec::new(),
            posting_to_coarse: FxHashMap::default(),
            coarse_to_postings: FxHashMap::default(),
            mutations_since_coarse_refresh: 0,
            coarse_refresh_interval: default_coarse_refresh_interval(),
        };
        if rows.is_empty() {
            return (out, FxHashMap::default());
        }

        if known_assignments.is_none() {
            let built = SpFreshOffHeapIndex::build(out.cfg.clone(), rows);
            let snapshot = built.export_metadata();
            let mut postings =
                FxHashMap::with_capacity_and_hasher(snapshot.postings.len(), Default::default());
            for posting in snapshot.postings {
                postings.insert(
                    posting.id,
                    DiskPosting {
                        id: posting.id,
                        centroid: posting.centroid,
                        size: posting.size,
                        max_radius: f32::INFINITY,
                        max_l2_norm: f32::INFINITY,
                        sum_values: vec![0.0; out.cfg.dim],
                    },
                );
            }
            out.postings = postings;
            out.next_posting_id = snapshot.next_posting_id;
            out.total_rows = snapshot.vector_posting.len() as u64;
            let mut assignments = FxHashMap::with_capacity_and_hasher(
                snapshot.vector_posting.len(),
                Default::default(),
            );
            for (id, posting_id) in snapshot.vector_posting {
                assignments.insert(id, posting_id);
            }
            let mut sums: FxHashMap<usize, Vec<f64>> = FxHashMap::default();
            let mut counts: FxHashMap<usize, u64> = FxHashMap::default();
            let mut max_norms: FxHashMap<usize, f32> = FxHashMap::default();
            let mut max_radii: FxHashMap<usize, f32> = FxHashMap::default();
            for row in rows {
                let Some(posting_id) = assignments.get(&row.id).copied() else {
                    continue;
                };
                let sum = sums
                    .entry(posting_id)
                    .or_insert_with(|| vec![0.0; out.cfg.dim]);
                for (i, value) in row.values.iter().enumerate() {
                    sum[i] += *value as f64;
                }
                *counts.entry(posting_id).or_insert(0) += 1;
                let norm = l2_norm(&row.values);
                let max_norm = max_norms.entry(posting_id).or_insert(0.0);
                if norm > *max_norm {
                    *max_norm = norm;
                }
                if let Some(posting) = out.postings.get(&posting_id) {
                    let radius = squared_l2(&row.values, &posting.centroid).sqrt();
                    let max_radius = max_radii.entry(posting_id).or_insert(0.0);
                    if radius > *max_radius {
                        *max_radius = radius;
                    }
                }
            }
            for (posting_id, posting) in &mut out.postings {
                posting.size = counts.get(posting_id).copied().unwrap_or(posting.size);
                posting.sum_values = sums
                    .get(posting_id)
                    .cloned()
                    .unwrap_or_else(|| vec![0.0; out.cfg.dim]);
                posting.max_l2_norm = max_norms.get(posting_id).copied().unwrap_or(f32::INFINITY);
                posting.max_radius = max_radii.get(posting_id).copied().unwrap_or(f32::INFINITY);
            }
            out.rebuild_coarse_index();
            return (out, assignments);
        }

        let vectors: Vec<Vec<f32>> = rows.iter().map(|r| r.values.clone()).collect();
        let k = out.cfg.initial_postings.max(1).min(vectors.len());
        let seeds = l2_kmeans(&vectors, k, out.cfg.kmeans_iters);
        for centroid in seeds {
            out.alloc_posting(centroid);
        }

        let mut assigns = FxHashMap::with_capacity_and_hasher(rows.len(), Default::default());
        let mut sums: FxHashMap<usize, Vec<f64>> = FxHashMap::default();
        let mut counts: FxHashMap<usize, u64> = FxHashMap::default();
        let mut max_norms: FxHashMap<usize, f32> = FxHashMap::default();
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
                            max_radius: f32::INFINITY,
                            max_l2_norm: f32::INFINITY,
                            sum_values: vec![0.0; out.cfg.dim],
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
            let norm = l2_norm(&row.values);
            let entry = max_norms.entry(posting_id).or_insert(0.0);
            if norm > *entry {
                *entry = norm;
            }
        }

        out.total_rows = rows.len() as u64;
        for (pid, posting) in &mut out.postings {
            let count = counts.get(pid).copied().unwrap_or(0);
            posting.size = count;
            if count == 0 {
                posting.sum_values = vec![0.0; out.cfg.dim];
                posting.max_l2_norm = f32::INFINITY;
                posting.max_radius = f32::INFINITY;
                continue;
            }
            if let Some(sum) = sums.get(pid) {
                posting.sum_values = sum.clone();
                posting.centroid = sum
                    .iter()
                    .map(|v| (*v / count as f64) as f32)
                    .collect::<Vec<_>>();
            }
            posting.max_l2_norm = max_norms.get(pid).copied().unwrap_or(f32::INFINITY);
            posting.max_radius = 0.0;
        }

        for row in rows {
            let Some(posting_id) = assigns.get(&row.id).copied() else {
                continue;
            };
            let Some(posting) = out.postings.get_mut(&posting_id) else {
                continue;
            };
            let radius = squared_l2(&row.values, &posting.centroid).sqrt();
            if radius > posting.max_radius {
                posting.max_radius = radius;
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
                max_radius: f32::INFINITY,
                max_l2_norm: f32::INFINITY,
                sum_values: vec![0.0; self.cfg.dim],
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
            self.coarse_to_postings
                .entry(best_cid)
                .or_default()
                .push(pid);
        }
        self.mutations_since_coarse_refresh = 0;
    }

    fn maybe_refresh_coarse_index(&mut self) {
        self.mutations_since_coarse_refresh = self.mutations_since_coarse_refresh.saturating_add(1);
        if self.mutations_since_coarse_refresh >= self.coarse_refresh_interval.max(1) {
            self.rebuild_coarse_index();
        }
    }

    fn scaled_probe_count(&self, base: usize, cap: usize) -> usize {
        if cap == 0 {
            return 0;
        }
        base.max(1)
            .saturating_mul(self.cfg.diskmeta_probe_multiplier.max(1))
            .min(cap)
    }

    fn ensure_sum_values(posting: &mut DiskPosting, dim: usize) {
        if posting.sum_values.len() == dim {
            return;
        }
        let n = posting.size as f64;
        posting.sum_values = posting
            .centroid
            .iter()
            .map(|value| *value as f64 * n)
            .collect();
    }

    fn probe_cmp(a: &(usize, f32), b: &(usize, f32)) -> std::cmp::Ordering {
        a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0))
    }

    fn recompute_worst_probe_idx(top: &[(usize, f32)]) -> Option<usize> {
        if top.is_empty() {
            return None;
        }
        let mut worst = 0usize;
        for idx in 1..top.len() {
            if Self::probe_cmp(&top[idx], &top[worst]).is_gt() {
                worst = idx;
            }
        }
        Some(worst)
    }

    fn push_probe_topn(
        top: &mut Vec<(usize, f32)>,
        worst_idx: &mut Option<usize>,
        candidate: (usize, f32),
        n: usize,
    ) {
        if n == 0 {
            return;
        }
        if top.len() < n {
            top.push(candidate);
            if top.len() == n {
                *worst_idx = Self::recompute_worst_probe_idx(top);
            }
            return;
        }
        let worst = match *worst_idx {
            Some(idx) if idx < top.len() => idx,
            _ => {
                let idx = Self::recompute_worst_probe_idx(top).expect("non-empty top");
                *worst_idx = Some(idx);
                idx
            }
        };
        if Self::probe_cmp(&candidate, &top[worst]).is_lt() {
            top[worst] = candidate;
            *worst_idx = Self::recompute_worst_probe_idx(top);
        }
    }

    fn nearest_coarse_centroids(&self, query: &[f32], n: usize) -> Vec<usize> {
        if self.coarse_centroids.is_empty() || n == 0 {
            return Vec::new();
        }
        let limit = n.min(self.coarse_centroids.len());
        let mut top = Vec::with_capacity(limit);
        let mut worst_idx = None;
        for (cid, centroid) in self.coarse_centroids.iter().enumerate() {
            Self::push_probe_topn(
                &mut top,
                &mut worst_idx,
                (cid, squared_l2(query, centroid)),
                limit,
            );
        }
        top.sort_by(Self::probe_cmp);
        top.into_iter().map(|(cid, _)| cid).collect()
    }

    fn candidate_postings_for_query(&self, query: &[f32], coarse_probe: usize) -> Vec<usize> {
        if self.postings.len() <= self.cfg.initial_postings.max(1) {
            return self.postings.keys().copied().collect();
        }
        if self.coarse_centroids.is_empty() || self.coarse_to_postings.is_empty() {
            return self.postings.keys().copied().collect();
        }

        let coarse_probe = coarse_probe.max(1).min(self.coarse_centroids.len().max(1));
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

    fn nearest_postings_with_coarse(
        &self,
        query: &[f32],
        n: usize,
        coarse_probe: usize,
    ) -> Vec<(usize, f32)> {
        if n == 0 {
            return Vec::new();
        }
        let candidates = self.candidate_postings_for_query(query, coarse_probe);
        let mut top = Vec::with_capacity(n.min(candidates.len().max(1)));
        let mut worst_idx = None;
        for posting_id in candidates {
            let Some(posting) = self.postings.get(&posting_id) else {
                continue;
            };
            Self::push_probe_topn(
                &mut top,
                &mut worst_idx,
                (posting.id, squared_l2(query, &posting.centroid)),
                n,
            );
        }
        if top.is_empty() {
            for posting in self.postings.values() {
                Self::push_probe_topn(
                    &mut top,
                    &mut worst_idx,
                    (posting.id, squared_l2(query, &posting.centroid)),
                    n,
                );
            }
        }
        top.sort_by(Self::probe_cmp);
        top
    }

    fn nearest_postings(&self, query: &[f32], n: usize) -> Vec<(usize, f32)> {
        self.nearest_postings_with_coarse(query, n, n)
    }

    pub(crate) fn choose_posting(&self, vector: &[f32]) -> Option<usize> {
        if vector.len() != self.cfg.dim {
            return None;
        }
        let coarse_probe =
            self.scaled_probe_count(self.cfg.nprobe.max(1), self.coarse_centroids.len());
        let candidates = self.candidate_postings_for_query(vector, coarse_probe);
        let mut best: Option<(usize, f32)> = None;
        for posting_id in candidates {
            let Some(posting) = self.postings.get(&posting_id) else {
                continue;
            };
            let distance = squared_l2(vector, posting.centroid.as_slice());
            match best {
                Some((_best_id, best_distance)) if distance >= best_distance => {}
                _ => best = Some((posting_id, distance)),
            }
        }
        if best.is_none() {
            for posting in self.postings.values() {
                let distance = squared_l2(vector, posting.centroid.as_slice());
                match best {
                    Some((_best_id, best_distance)) if distance >= best_distance => {}
                    _ => best = Some((posting.id, distance)),
                }
            }
        }
        best.map(|(posting_id, _)| posting_id)
    }

    pub(crate) fn posting_ids(&self) -> Vec<usize> {
        let mut out: Vec<usize> = self.postings.keys().copied().collect();
        out.sort_unstable();
        out
    }

    fn posting_query_lower_bound_sq(
        posting: &DiskPosting,
        query_norm: f32,
        centroid_distance_sq: f32,
    ) -> f32 {
        let centroid_lb = if posting.max_radius.is_finite() {
            let d = centroid_distance_sq.sqrt() - posting.max_radius.max(0.0);
            if d > 0.0 {
                d * d
            } else {
                0.0
            }
        } else {
            0.0
        };
        let norm_lb = if posting.max_l2_norm.is_finite() {
            let d = query_norm - posting.max_l2_norm.max(0.0);
            if d > 0.0 {
                d * d
            } else {
                0.0
            }
        } else {
            0.0
        };
        centroid_lb.max(norm_lb)
    }

    pub(crate) fn choose_probe_postings_with_bounds(
        &self,
        query: &[f32],
        k: usize,
    ) -> Vec<(usize, f32)> {
        if query.len() != self.cfg.dim || k == 0 {
            return Vec::new();
        }
        let posting_count = self.postings.len();
        if posting_count == 0 {
            return Vec::new();
        }
        let base_probe = self.cfg.nprobe.max(k).min(posting_count);
        let probe_count = self.scaled_probe_count(base_probe, posting_count);
        let coarse_probe = self.scaled_probe_count(base_probe, self.coarse_centroids.len());
        let query_norm = l2_norm(query);
        let mut out: Vec<(usize, f32, f32)> = self
            .nearest_postings_with_coarse(query, probe_count, coarse_probe)
            .into_iter()
            .filter_map(|(posting_id, centroid_distance_sq)| {
                self.postings.get(&posting_id).map(|posting| {
                    (
                        posting_id,
                        Self::posting_query_lower_bound_sq(
                            posting,
                            query_norm,
                            centroid_distance_sq,
                        ),
                        centroid_distance_sq,
                    )
                })
            })
            .collect();
        out.sort_by(|a, b| {
            a.1.total_cmp(&b.1)
                .then_with(|| a.2.total_cmp(&b.2))
                .then_with(|| a.0.cmp(&b.0))
        });
        out.into_iter()
            .map(|(posting_id, lb, _)| (posting_id, lb))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn choose_probe_postings(&self, query: &[f32], k: usize) -> Vec<usize> {
        self.choose_probe_postings_with_bounds(query, k)
            .into_iter()
            .map(|(pid, _)| pid)
            .collect()
    }

    fn remove_from_posting(&mut self, posting_id: usize, old_vector: &[f32]) -> bool {
        let Some(posting) = self.postings.get_mut(&posting_id) else {
            return false;
        };
        Self::ensure_sum_values(posting, self.cfg.dim);
        if posting.size <= 1 {
            posting.size = 0;
            posting.sum_values = vec![0.0; self.cfg.dim];
            posting.max_radius = f32::INFINITY;
            posting.max_l2_norm = f32::INFINITY;
            return true;
        }
        for (i, v) in old_vector.iter().enumerate() {
            posting.sum_values[i] -= *v as f64;
        }
        posting.size -= 1;
        let inv = 1.0 / posting.size as f64;
        let mut centroid_shift_sq = 0.0f32;
        for i in 0..self.cfg.dim {
            let old_centroid = posting.centroid[i];
            let new_centroid = (posting.sum_values[i] * inv) as f32;
            let delta = old_centroid - new_centroid;
            centroid_shift_sq += delta * delta;
            posting.centroid[i] = new_centroid;
        }
        if posting.max_radius.is_finite() {
            posting.max_radius = posting.max_radius.max(0.0) + centroid_shift_sq.sqrt();
        }
        if posting.max_l2_norm.is_finite() {
            posting.max_l2_norm = posting.max_l2_norm.max(l2_norm(old_vector));
        }
        false
    }

    fn add_to_posting(&mut self, posting_id: usize, new_vector: &[f32]) {
        let Some(posting) = self.postings.get_mut(&posting_id) else {
            return;
        };
        Self::ensure_sum_values(posting, self.cfg.dim);
        if posting.size == 0 {
            posting.centroid = new_vector.to_vec();
            posting.size = 1;
            posting.sum_values = new_vector.iter().map(|v| *v as f64).collect();
            posting.max_radius = 0.0;
            posting.max_l2_norm = l2_norm(new_vector);
            return;
        }
        for (i, v) in new_vector.iter().enumerate() {
            posting.sum_values[i] += *v as f64;
        }
        posting.size += 1;
        let inv = 1.0 / posting.size as f64;
        let mut centroid_shift_sq = 0.0f32;
        for i in 0..self.cfg.dim {
            let old_centroid = posting.centroid[i];
            let new_centroid = (posting.sum_values[i] * inv) as f32;
            let delta = old_centroid - new_centroid;
            centroid_shift_sq += delta * delta;
            posting.centroid[i] = new_centroid;
        }
        if posting.max_radius.is_finite() {
            posting.max_radius = posting.max_radius.max(0.0) + centroid_shift_sq.sqrt();
            let new_radius = squared_l2(new_vector, &posting.centroid).sqrt();
            if new_radius > posting.max_radius {
                posting.max_radius = new_radius;
            }
        }
        if posting.max_l2_norm.is_finite() {
            posting.max_l2_norm = posting.max_l2_norm.max(l2_norm(new_vector));
        }
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
                    Self::ensure_sum_values(posting, self.cfg.dim);
                    let size = posting.size.max(1) as f64;
                    let mut centroid_shift_sq = 0.0f32;
                    for i in 0..self.cfg.dim {
                        let old_centroid = posting.centroid[i];
                        posting.sum_values[i] += (new_vector[i] - old_vector[i]) as f64;
                        let new_centroid = (posting.sum_values[i] / size) as f32;
                        let delta = old_centroid - new_centroid;
                        centroid_shift_sq += delta * delta;
                        posting.centroid[i] = new_centroid;
                    }
                    if posting.max_radius.is_finite() {
                        posting.max_radius = posting.max_radius.max(0.0) + centroid_shift_sq.sqrt();
                        let new_radius = squared_l2(new_vector, &posting.centroid).sqrt();
                        if new_radius > posting.max_radius {
                            posting.max_radius = new_radius;
                        }
                    }
                    if posting.max_l2_norm.is_finite() {
                        posting.max_l2_norm = posting.max_l2_norm.max(l2_norm(new_vector));
                    }
                }
            }
            Some((old_posting, old_vector)) => {
                let emptied = self.remove_from_posting(old_posting, old_vector);
                if emptied {
                    self.postings.remove(&old_posting);
                }
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
                        max_radius: f32::INFINITY,
                        max_l2_norm: f32::INFINITY,
                        sum_values: vec![0.0; self.cfg.dim],
                    });
                    if self.next_posting_id <= new_posting {
                        self.next_posting_id = new_posting + 1;
                    }
                }
                self.add_to_posting(new_posting, new_vector);
                self.total_rows = self.total_rows.saturating_add(1);
            }
        }
        self.maybe_refresh_coarse_index();
    }

    pub(crate) fn apply_upsert(
        &mut self,
        old: Option<(usize, Vec<f32>)>,
        new_posting: usize,
        new_vector: Vec<f32>,
    ) {
        let old_ref = old
            .as_ref()
            .map(|(posting, values)| (*posting, values.as_slice()));
        self.apply_upsert_ref(old_ref, new_posting, new_vector.as_slice());
    }

    pub(crate) fn apply_delete_ref(&mut self, old: Option<(usize, &[f32])>) -> bool {
        let Some((old_posting, old_vector)) = old else {
            return false;
        };
        let emptied = self.remove_from_posting(old_posting, old_vector);
        if emptied {
            self.postings.remove(&old_posting);
        }
        self.total_rows = self.total_rows.saturating_sub(1);
        self.maybe_refresh_coarse_index();
        true
    }

    pub(crate) fn apply_delete(&mut self, old: Option<(usize, Vec<f32>)>) -> bool {
        let old_ref = old
            .as_ref()
            .map(|(posting, values)| (*posting, values.as_slice()));
        self.apply_delete_ref(old_ref)
    }

    pub(crate) fn len(&self) -> usize {
        self.total_rows as usize
    }
}

#[cfg(test)]
mod tests {
    use rustc_hash::FxHashMap;

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

    fn naive_nearest_postings(
        index: &SpFreshDiskMetaIndex,
        query: &[f32],
        n: usize,
    ) -> Vec<(usize, f32)> {
        if n == 0 {
            return Vec::new();
        }
        let candidates = index.candidate_postings_for_query(query, n);
        let mut all: Vec<(usize, f32)> = candidates
            .iter()
            .filter_map(|pid| {
                index
                    .postings
                    .get(pid)
                    .map(|p| (p.id, crate::linalg::squared_l2(query, &p.centroid)))
            })
            .collect();
        if all.is_empty() {
            all = index
                .postings
                .values()
                .map(|p| (p.id, crate::linalg::squared_l2(query, &p.centroid)))
                .collect();
        }
        all.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        all.truncate(n.min(all.len()));
        all
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

    #[test]
    fn upsert_removes_emptied_old_posting() {
        let cfg = SpFreshConfig {
            dim: 4,
            initial_postings: 1,
            nprobe: 1,
            ..Default::default()
        };
        let rows = vec![VectorRecord::new(7, vec![0.0, 0.0, 0.0, 0.0])];
        let (mut index, _assignments) = SpFreshDiskMetaIndex::build_with_assignments(cfg, &rows);
        let old_vec = rows[0].values.clone();
        let old_posting = index.choose_posting(&old_vec).unwrap_or_default();
        let new_posting = index.next_posting_id.saturating_add(11);
        let bootstrap_vec = vec![8.0, 0.0, 0.0, 0.0];
        index.apply_upsert(None, new_posting, bootstrap_vec);
        let moved_vec = vec![9.0, 0.0, 0.0, 0.0];

        index.apply_upsert(Some((old_posting, old_vec)), new_posting, moved_vec);

        assert!(
            !index.postings.contains_key(&old_posting),
            "old posting should be removed once emptied"
        );
        assert!(
            index.postings.contains_key(&new_posting),
            "new posting should exist after upsert"
        );
    }

    #[test]
    fn nearest_postings_topn_matches_naive_sort() {
        let cfg = SpFreshConfig {
            dim: 16,
            initial_postings: 32,
            nprobe: 8,
            ..Default::default()
        };
        let rows = synthetic_rows(cfg.dim, 2_048);
        let (index, _assignments) = SpFreshDiskMetaIndex::build_with_assignments(cfg, &rows);

        for &n in &[1usize, 2, 4, 8, 16] {
            for query in rows
                .iter()
                .step_by(73)
                .take(16)
                .map(|r| r.values.as_slice())
            {
                let got = index.nearest_postings(query, n);
                let want = naive_nearest_postings(&index, query, n);
                assert_eq!(got, want, "n={n} query_first_dim={}", query[0]);
            }
        }
    }

    #[test]
    fn diskmeta_probe_multiplier_scales_search_probe_budget() {
        let cfg = SpFreshConfig {
            dim: 16,
            initial_postings: 64,
            nprobe: 1,
            diskmeta_probe_multiplier: 4,
            ..Default::default()
        };
        let rows = synthetic_rows(cfg.dim, 512);
        let (index, _assignments) = SpFreshDiskMetaIndex::build_with_assignments(cfg, &rows);
        let probes = index.choose_probe_postings(&rows[11].values, 1);
        let expected = 4usize.min(index.postings.len());
        assert_eq!(probes.len(), expected);
    }

    #[test]
    fn mutation_bounds_remain_finite_and_sound() {
        let cfg = SpFreshConfig {
            dim: 8,
            initial_postings: 16,
            nprobe: 4,
            ..Default::default()
        };
        let rows = synthetic_rows(cfg.dim, 256);
        let (mut index, mut assignments) = SpFreshDiskMetaIndex::build_with_assignments(cfg, &rows);
        let mut values_by_id: FxHashMap<u64, Vec<f32>> = rows
            .iter()
            .map(|row| (row.id, row.values.clone()))
            .collect();

        for step in 0..1_024usize {
            let id = rows[step % rows.len()].id;
            let old_posting = *assignments
                .get(&id)
                .expect("assignment must exist for all synthetic rows");
            let old_values = values_by_id
                .get(&id)
                .expect("value must exist for all synthetic rows")
                .clone();
            let mut new_values = old_values.clone();
            new_values[0] += ((step as f32) * 0.013).sin() * 0.07;
            new_values[1] += ((step as f32) * 0.009).cos() * 0.05;
            let new_posting = if step % 7 == 0 {
                index.choose_posting(&new_values).unwrap_or(old_posting)
            } else {
                old_posting
            };
            index.apply_upsert(
                Some((old_posting, old_values)),
                new_posting,
                new_values.clone(),
            );
            assignments.insert(id, new_posting);
            values_by_id.insert(id, new_values);

            if step % 64 != 0 {
                continue;
            }
            let mut member_counts: FxHashMap<usize, usize> = FxHashMap::default();
            let mut member_ids: FxHashMap<usize, Vec<u64>> = FxHashMap::default();
            for (&row_id, &posting_id) in &assignments {
                *member_counts.entry(posting_id).or_insert(0) += 1;
                member_ids.entry(posting_id).or_default().push(row_id);
            }
            for (&posting_id, posting) in &index.postings {
                assert!(
                    posting.max_radius.is_finite(),
                    "posting={posting_id} max_radius should stay finite under mutations"
                );
                assert!(
                    posting.max_l2_norm.is_finite(),
                    "posting={posting_id} max_l2_norm should stay finite under mutations"
                );
                let count = member_counts.get(&posting_id).copied().unwrap_or(0);
                assert_eq!(
                    posting.size as usize, count,
                    "posting={posting_id} size mismatch"
                );
                let mut true_max_norm = 0.0f32;
                let mut true_max_radius = 0.0f32;
                if let Some(ids) = member_ids.get(&posting_id) {
                    for row_id in ids {
                        let values = values_by_id
                            .get(row_id)
                            .expect("member row should have values");
                        let norm = crate::linalg::norm2(values).sqrt();
                        if norm > true_max_norm {
                            true_max_norm = norm;
                        }
                        let radius = crate::linalg::squared_l2(values, &posting.centroid).sqrt();
                        if radius > true_max_radius {
                            true_max_radius = radius;
                        }
                    }
                }
                assert!(
                    posting.max_l2_norm + 1e-4 >= true_max_norm,
                    "posting={posting_id} max_l2_norm={} true={true_max_norm}",
                    posting.max_l2_norm
                );
                assert!(
                    posting.max_radius + 1e-4 >= true_max_radius,
                    "posting={posting_id} max_radius={} true={true_max_radius}",
                    posting.max_radius
                );
            }
        }
    }
}
