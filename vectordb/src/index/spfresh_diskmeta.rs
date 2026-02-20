use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::linalg::squared_l2;
use crate::types::VectorRecord;

use super::kmeans::l2_kmeans;
use super::spfresh_offheap::SpFreshOffHeapIndex;
use super::SpFreshConfig;

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

    pub(crate) fn choose_posting(&self, vector: &[f32]) -> Option<usize> {
        self.nearest_postings(vector, 1)
            .first()
            .map(|(pid, _)| *pid)
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

    pub(crate) fn apply_upsert(
        &mut self,
        old: Option<(usize, Vec<f32>)>,
        new_posting: usize,
        new_vector: Vec<f32>,
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
                self.remove_from_posting(old_posting, &old_vector);
                self.add_to_posting(new_posting, &new_vector);
            }
            None => {
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    self.postings.entry(new_posting)
                {
                    entry.insert(DiskPosting {
                        id: new_posting,
                        centroid: new_vector.clone(),
                        size: 0,
                    });
                    if self.next_posting_id <= new_posting {
                        self.next_posting_id = new_posting + 1;
                    }
                }
                self.add_to_posting(new_posting, &new_vector);
                self.total_rows = self.total_rows.saturating_add(1);
            }
        }
        self.postings.retain(|_, posting| posting.size > 0);
    }

    pub(crate) fn apply_delete(&mut self, old: Option<(usize, Vec<f32>)>) -> bool {
        let Some((old_posting, old_vector)) = old else {
            return false;
        };
        self.remove_from_posting(old_posting, &old_vector);
        self.total_rows = self.total_rows.saturating_sub(1);
        self.postings.retain(|_, posting| posting.size > 0);
        true
    }

    pub(crate) fn len(&self) -> usize {
        self.total_rows as usize
    }
}
