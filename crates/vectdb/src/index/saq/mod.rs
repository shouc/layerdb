mod plan;
mod quantize;

#[cfg(test)]
mod tests;

use std::collections::HashMap;

use crate::linalg::{argmin_l2, norm2, squared_l2};
use crate::types::{Neighbor, VectorIndex, VectorRecord};

use super::kmeans::l2_kmeans;
use plan::train_plan;
use quantize::{decode_reordered, encode_vector, reorder, restore_order, saq_distance};

#[derive(Clone, Debug)]
pub struct SaqConfig {
    pub dim: usize,
    pub total_bits: usize,
    pub min_bits_per_dim: usize,
    pub max_bits_per_dim: usize,
    pub min_dims_per_segment: usize,
    pub max_dims_per_segment: usize,
    pub ivf_clusters: usize,
    pub nprobe: usize,
    pub caq_rounds: usize,
    pub train_sample: usize,
    pub use_joint_dp: bool,
    pub use_variance_permutation: bool,
}

impl Default for SaqConfig {
    fn default() -> Self {
        Self {
            dim: 64,
            total_bits: 256,
            min_bits_per_dim: 1,
            max_bits_per_dim: 8,
            min_dims_per_segment: 4,
            max_dims_per_segment: 16,
            ivf_clusters: 64,
            nprobe: 8,
            caq_rounds: 1,
            train_sample: 50_000,
            use_joint_dp: true,
            use_variance_permutation: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SaqSegment {
    pub start_dim: usize,
    pub dim_count: usize,
    pub bits: usize,
}

#[derive(Clone, Debug)]
pub struct SaqPlan {
    pub dim_order: Vec<usize>,
    pub inv_dim_order: Vec<usize>,
    pub segments: Vec<SaqSegment>,
}

#[derive(Clone, Debug)]
struct EncodedVector {
    id: u64,
    codes: Vec<u8>,
    v_max: f32,
    norm_sq: f32,
}

#[derive(Clone, Debug)]
struct Cluster {
    id: usize,
    centroid: Vec<f32>,
    rows: Vec<EncodedVector>,
}

pub struct SaqIndex {
    cfg: SaqConfig,
    plan: Option<SaqPlan>,
    clusters: Vec<Cluster>,
    positions: HashMap<u64, (usize, usize)>,
}

impl SaqIndex {
    pub fn new(cfg: SaqConfig) -> Self {
        Self {
            cfg,
            plan: None,
            clusters: Vec::new(),
            positions: HashMap::new(),
        }
    }

    pub fn plan(&self) -> Option<&SaqPlan> {
        self.plan.as_ref()
    }

    pub fn quantization_mse(&self, vectors: &[Vec<f32>]) -> Option<f64> {
        let plan = self.plan.as_ref()?;
        if vectors.is_empty() {
            return Some(0.0);
        }
        let mut total = 0.0f64;
        for v in vectors {
            if v.len() != self.cfg.dim {
                continue;
            }
            let enc = encode_vector(plan, v, self.cfg.caq_rounds);
            let decoded_reordered = decode_reordered(plan, &enc.codes, enc.v_max);
            let decoded = restore_order(&decoded_reordered, &plan.inv_dim_order);
            total += squared_l2(v, &decoded) as f64 / self.cfg.dim as f64;
        }
        Some(total / vectors.len() as f64)
    }

    pub fn build(cfg: SaqConfig, base: &[VectorRecord]) -> Self {
        let mut out = Self::new(cfg);
        if base.is_empty() {
            return out;
        }

        let sample_count = out.cfg.train_sample.min(base.len());
        let sample: Vec<Vec<f32>> = base
            .iter()
            .take(sample_count)
            .map(|r| r.values.clone())
            .collect();
        let plan = train_plan(&out.cfg, &sample);
        let centroids = l2_kmeans(
            &base.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
            out.cfg.ivf_clusters.max(1).min(base.len()),
            8,
        );
        let mut clusters: Vec<Cluster> = centroids
            .into_iter()
            .enumerate()
            .map(|(id, centroid)| Cluster {
                id,
                centroid,
                rows: Vec::new(),
            })
            .collect();

        let mut positions = HashMap::new();
        for row in base {
            let cid = argmin_l2(
                &row.values,
                &clusters
                    .iter()
                    .map(|c| c.centroid.clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap_or(0);
            let encoded = encode_vector(&plan, &row.values, out.cfg.caq_rounds);
            let pos = clusters[cid].rows.len();
            clusters[cid].rows.push(EncodedVector {
                id: row.id,
                codes: encoded.codes,
                v_max: encoded.v_max,
                norm_sq: encoded.norm_sq,
            });
            positions.insert(row.id, (cid, pos));
        }

        out.plan = Some(plan);
        out.clusters = clusters;
        out.positions = positions;
        out
    }

    fn nearest_clusters(&self, query: &[f32], nprobe: usize) -> Vec<usize> {
        let mut d: Vec<(usize, f32)> = self
            .clusters
            .iter()
            .map(|c| (c.id, squared_l2(query, &c.centroid)))
            .collect();
        d.sort_by(|a, b| a.1.total_cmp(&b.1));
        d.into_iter().take(nprobe).map(|x| x.0).collect()
    }

    fn rebuild_positions_for_cluster(&mut self, cid: usize) {
        if let Some(cluster) = self.clusters.get(cid) {
            for (pos, row) in cluster.rows.iter().enumerate() {
                self.positions.insert(row.id, (cid, pos));
            }
        }
    }
}

impl VectorIndex for SaqIndex {
    fn upsert(&mut self, id: u64, vector: Vec<f32>) {
        if vector.len() != self.cfg.dim {
            return;
        }
        let _ = self.delete(id);

        if self.plan.is_none() {
            self.plan = Some(train_plan(&self.cfg, std::slice::from_ref(&vector)));
            self.clusters = vec![Cluster {
                id: 0,
                centroid: vector.clone(),
                rows: Vec::new(),
            }];
        }
        let Some(plan) = self.plan.as_ref() else {
            return;
        };

        let cid = self
            .nearest_clusters(&vector, 1)
            .first()
            .copied()
            .unwrap_or(0);
        let encoded = encode_vector(plan, &vector, self.cfg.caq_rounds);
        let pos = self.clusters[cid].rows.len();
        self.clusters[cid].rows.push(EncodedVector {
            id,
            codes: encoded.codes,
            v_max: encoded.v_max,
            norm_sq: encoded.norm_sq,
        });
        self.positions.insert(id, (cid, pos));
    }

    fn delete(&mut self, id: u64) -> bool {
        let Some((cid, pos)) = self.positions.remove(&id) else {
            return false;
        };
        if let Some(cluster) = self.clusters.get_mut(cid) {
            if pos < cluster.rows.len() {
                cluster.rows.swap_remove(pos);
                self.rebuild_positions_for_cluster(cid);
                return true;
            }
        }
        false
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<Neighbor> {
        if k == 0 || query.len() != self.cfg.dim {
            return Vec::new();
        }
        let Some(plan) = self.plan.as_ref() else {
            return Vec::new();
        };

        let projected_query = reorder(query, &plan.dim_order);
        let query_norm = norm2(&projected_query);

        let mut out = Vec::new();
        for cid in self.nearest_clusters(query, self.cfg.nprobe.max(1)) {
            let cluster = &self.clusters[cid];
            for row in &cluster.rows {
                let dist = saq_distance(
                    plan,
                    &projected_query,
                    query_norm,
                    &row.codes,
                    row.v_max,
                    row.norm_sq,
                );
                out.push(Neighbor {
                    id: row.id,
                    distance: dist,
                });
            }
        }
        out.sort_by(|a, b| {
            a.distance
                .total_cmp(&b.distance)
                .then_with(|| a.id.cmp(&b.id))
        });
        out.truncate(k);
        out
    }

    fn len(&self) -> usize {
        self.positions.len()
    }
}
