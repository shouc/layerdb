use std::collections::HashMap;

use crate::linalg::{argmin_l2, dot, norm2, squared_l2};
use crate::types::{Neighbor, VectorIndex, VectorRecord};

use super::kmeans::l2_kmeans;

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
                let dist = saq_distance(plan, &projected_query, query_norm, row);
                out.push(Neighbor {
                    id: row.id,
                    distance: dist,
                });
            }
        }
        out.sort_by(|a, b| a.distance.total_cmp(&b.distance).then_with(|| a.id.cmp(&b.id)));
        out.truncate(k);
        out
    }

    fn len(&self) -> usize {
        self.positions.len()
    }
}

struct EncTmp {
    codes: Vec<u8>,
    v_max: f32,
    norm_sq: f32,
}

fn train_plan(cfg: &SaqConfig, sample: &[Vec<f32>]) -> SaqPlan {
    let dim = cfg.dim;
    let variances = dimension_variances(sample, dim);
    let mut ordered: Vec<usize> = (0..dim).collect();
    if cfg.use_variance_permutation {
        ordered.sort_by(|a, b| variances[*b].total_cmp(&variances[*a]));
    }

    let reordered_vars: Vec<f32> = ordered.iter().map(|i| variances[*i]).collect();
    let segments = if cfg.use_joint_dp {
        allocate_joint_segments(cfg, &reordered_vars)
    } else {
        uniform_segments(cfg, dim)
    };
    let mut inv = vec![0usize; dim];
    for (new_idx, old_idx) in ordered.iter().enumerate() {
        inv[*old_idx] = new_idx;
    }

    SaqPlan {
        dim_order: ordered,
        inv_dim_order: inv,
        segments,
    }
}

fn uniform_segments(cfg: &SaqConfig, dim: usize) -> Vec<SaqSegment> {
    let mut bits = cfg.total_bits / dim.max(1);
    bits = bits.max(cfg.min_bits_per_dim);
    bits = bits.min(cfg.max_bits_per_dim);
    let mut out = Vec::new();
    let seg_size = cfg.max_dims_per_segment.max(cfg.min_dims_per_segment).min(dim.max(1));
    let mut start = 0usize;
    while start < dim {
        let len = (dim - start).min(seg_size);
        out.push(SaqSegment {
            start_dim: start,
            dim_count: len,
            bits,
        });
        start += len;
    }
    if out.is_empty() {
        out.push(SaqSegment {
            start_dim: 0,
            dim_count: dim,
            bits,
        });
    }
    out
}

fn dimension_variances(sample: &[Vec<f32>], dim: usize) -> Vec<f32> {
    if sample.len() <= 1 {
        return vec![1.0; dim];
    }
    let n = sample.len() as f32;
    let mut means = vec![0.0; dim];
    for row in sample {
        for (d, x) in row.iter().enumerate().take(dim) {
            means[d] += *x;
        }
    }
    for m in &mut means {
        *m /= n;
    }

    let mut vars = vec![0.0; dim];
    for row in sample {
        for d in 0..dim {
            let diff = row[d] - means[d];
            vars[d] += diff * diff;
        }
    }
    let denom = (sample.len() - 1) as f32;
    for v in &mut vars {
        *v /= denom;
        if *v < 1e-9 {
            *v = 1e-9;
        }
    }
    vars
}

fn allocate_joint_segments(cfg: &SaqConfig, dim_variances: &[f32]) -> Vec<SaqSegment> {
    let d = dim_variances.len();
    let q = cfg.total_bits;
    let inf = f32::INFINITY;
    let mut prefix = vec![0.0f32; d + 1];
    for i in 0..d {
        prefix[i + 1] = prefix[i] + dim_variances[i];
    }

    #[derive(Clone, Copy, Default)]
    struct Choice {
        start: usize,
        bits: usize,
    }

    let mut dp = vec![vec![inf; q + 1]; d + 1];
    let mut bt = vec![vec![Choice::default(); q + 1]; d + 1];
    dp[0][0] = 0.0;

    let max_seg = cfg.max_dims_per_segment.max(cfg.min_dims_per_segment).min(d);
    let min_seg = cfg.min_dims_per_segment.max(1);

    for d_end in 1..=d {
        if d_end < min_seg {
            continue;
        }
        let start_lo = d_end.saturating_sub(max_seg);
        let start_hi = d_end.saturating_sub(min_seg);
        for d_start in start_lo..=start_hi {
            let seg_len = d_end - d_start;
            let var_sum = prefix[d_end] - prefix[d_start];
            for bits in cfg.min_bits_per_dim..=cfg.max_bits_per_dim {
                let bit_cost = bits * seg_len;
                if bit_cost > q {
                    break;
                }
                let distortion = if bits == 0 {
                    var_sum
                } else {
                    // MSE-oriented scalar quantization surrogate: error scales with 2^(-2b).
                    var_sum / ((1usize << (2 * bits)) as f32)
                };
                for q_prev in 0..=(q - bit_cost) {
                    if !dp[d_start][q_prev].is_finite() {
                        continue;
                    }
                    let q_new = q_prev + bit_cost;
                    let total = dp[d_start][q_prev] + distortion;
                    if total < dp[d_end][q_new] {
                        dp[d_end][q_new] = total;
                        bt[d_end][q_new] = Choice {
                            start: d_start,
                            bits,
                        };
                    }
                }
            }
        }
    }

    let mut best_q = 0usize;
    let mut best = inf;
    for b in 0..=q {
        if dp[d][b] < best {
            best = dp[d][b];
            best_q = b;
        }
    }

    if !best.is_finite() {
        // Fallback to uniform segments and equal bits-per-dim.
        let mut bits = (cfg.total_bits / d.max(1)).max(cfg.min_bits_per_dim);
        bits = bits.min(cfg.max_bits_per_dim);
        return vec![SaqSegment {
            start_dim: 0,
            dim_count: d,
            bits,
        }];
    }

    let mut segments = Vec::new();
    let mut cur_d = d;
    let mut cur_q = best_q;
    while cur_d > 0 {
        let c = bt[cur_d][cur_q];
        let seg_len = cur_d - c.start;
        segments.push(SaqSegment {
            start_dim: c.start,
            dim_count: seg_len,
            bits: c.bits,
        });
        cur_q -= c.bits * seg_len;
        cur_d = c.start;
    }
    segments.reverse();
    segments
}

fn encode_vector(plan: &SaqPlan, vector: &[f32], caq_rounds: usize) -> EncTmp {
    let reordered = reorder(vector, &plan.dim_order);
    let mut v_max = reordered
        .iter()
        .fold(0.0f32, |m, x| if x.abs() > m { x.abs() } else { m });
    if v_max < 1e-9 {
        v_max = 1e-9;
    }

    let mut codes = vec![0u8; reordered.len()];
    for seg in &plan.segments {
        let levels = (1usize << seg.bits).max(1);
        let delta = (2.0 * v_max) / levels as f32;
        for d in seg.start_dim..(seg.start_dim + seg.dim_count) {
            let q = ((reordered[d] + v_max) / delta).floor() as i32;
            let q = q.clamp(0, levels as i32 - 1);
            codes[d] = q as u8;
        }
    }

    if caq_rounds > 0 {
        caq_refine(plan, &reordered, &mut codes, v_max, caq_rounds);
    }

    let decoded = decode_reordered(plan, &codes, v_max);
    let norm_sq = norm2(&decoded);
    EncTmp {
        codes,
        v_max,
        norm_sq,
    }
}

fn decode_reordered(plan: &SaqPlan, codes: &[u8], v_max: f32) -> Vec<f32> {
    let mut out = vec![0.0f32; codes.len()];
    for seg in &plan.segments {
        let levels = (1usize << seg.bits).max(1);
        let delta = (2.0 * v_max) / levels as f32;
        for d in seg.start_dim..(seg.start_dim + seg.dim_count) {
            out[d] = delta * (codes[d] as f32 + 0.5) - v_max;
        }
    }
    out
}

fn caq_refine(plan: &SaqPlan, original: &[f32], codes: &mut [u8], v_max: f32, rounds: usize) {
    let mut recon = decode_reordered(plan, codes, v_max);
    let x_norm = norm2(original).sqrt().max(1e-9);
    let mut x_dot = dot(original, &recon);
    let mut o_norm_sq = norm2(&recon).max(1e-9);

    for _ in 0..rounds {
        let mut changed = false;
        for seg in &plan.segments {
            let levels = (1usize << seg.bits).max(1);
            let delta = (2.0 * v_max) / levels as f32;
            let max_code = levels as i32 - 1;

            for d in seg.start_dim..(seg.start_dim + seg.dim_count) {
                let cur = codes[d] as i32;
                let cur_o = recon[d];
                let mut best_code = cur;
                let mut best_x_dot = x_dot;
                let mut best_o_norm = o_norm_sq;
                let mut best_cos = x_dot / (x_norm * o_norm_sq.sqrt());

                for candidate in [cur - 1, cur + 1] {
                    if candidate < 0 || candidate > max_code {
                        continue;
                    }
                    let new_o = delta * (candidate as f32 + 0.5) - v_max;
                    let new_x_dot = x_dot + (new_o - cur_o) * original[d];
                    let new_o_norm = o_norm_sq + (new_o * new_o - cur_o * cur_o);
                    if new_o_norm <= 0.0 {
                        continue;
                    }
                    let new_cos = new_x_dot / (x_norm * new_o_norm.sqrt());
                    if new_cos > best_cos {
                        best_cos = new_cos;
                        best_code = candidate;
                        best_x_dot = new_x_dot;
                        best_o_norm = new_o_norm;
                    }
                }

                if best_code != cur {
                    codes[d] = best_code as u8;
                    recon[d] = delta * (best_code as f32 + 0.5) - v_max;
                    x_dot = best_x_dot;
                    o_norm_sq = best_o_norm;
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
}

fn saq_distance(plan: &SaqPlan, query_reordered: &[f32], query_norm_sq: f32, row: &EncodedVector) -> f32 {
    let decoded = decode_reordered(plan, &row.codes, row.v_max);
    let ip = dot(query_reordered, &decoded);
    query_norm_sq + row.norm_sq - 2.0 * ip
}

fn reorder(values: &[f32], order: &[usize]) -> Vec<f32> {
    let mut out = vec![0.0; values.len()];
    for (new_idx, old_idx) in order.iter().enumerate() {
        out[new_idx] = values[*old_idx];
    }
    out
}

fn restore_order(values_reordered: &[f32], inv_order: &[usize]) -> Vec<f32> {
    let mut out = vec![0.0; values_reordered.len()];
    for (old_idx, new_idx) in inv_order.iter().enumerate() {
        out[old_idx] = values_reordered[*new_idx];
    }
    out
}

#[cfg(test)]
mod tests {
    use crate::types::{VectorIndex, VectorRecord};

    use super::{allocate_joint_segments, train_plan, SaqConfig, SaqIndex};

    #[test]
    fn joint_allocation_respects_budget() {
        let cfg = SaqConfig {
            dim: 16,
            total_bits: 64,
            min_bits_per_dim: 1,
            max_bits_per_dim: 4,
            min_dims_per_segment: 2,
            max_dims_per_segment: 8,
            ..Default::default()
        };
        let vars = vec![1.0f32; 16];
        let segments = allocate_joint_segments(&cfg, &vars);
        let used: usize = segments.iter().map(|s| s.bits * s.dim_count).sum();
        assert!(used <= cfg.total_bits);
        assert!(!segments.is_empty());
    }

    #[test]
    fn saq_plan_trains() {
        let cfg = SaqConfig {
            dim: 8,
            total_bits: 24,
            ivf_clusters: 2,
            ..Default::default()
        };
        let sample = vec![
            vec![0.0, 0.1, 0.2, 0.3, 0.0, 0.1, 0.2, 0.3],
            vec![0.5, 0.1, 0.0, 0.3, 0.7, 0.2, 0.2, 0.1],
            vec![0.4, 0.0, 0.2, 0.4, 0.8, 0.3, 0.1, 0.3],
        ];
        let plan = train_plan(&cfg, &sample);
        assert_eq!(plan.dim_order.len(), 8);
        assert!(!plan.segments.is_empty());
    }

    #[test]
    fn saq_index_search_works() {
        let cfg = SaqConfig {
            dim: 4,
            total_bits: 16,
            ivf_clusters: 2,
            nprobe: 2,
            ..Default::default()
        };
        let base = vec![
            VectorRecord::new(1, vec![0.0, 0.0, 0.0, 0.0]),
            VectorRecord::new(2, vec![1.0, 0.0, 0.0, 0.0]),
            VectorRecord::new(3, vec![5.0, 0.0, 0.0, 0.0]),
        ];
        let idx = SaqIndex::build(cfg, &base);
        let got = idx.search(&[0.8, 0.0, 0.0, 0.0], 2);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].id, 2);
        let mse = idx
            .quantization_mse(&base.iter().map(|r| r.values.clone()).collect::<Vec<_>>())
            .unwrap();
        assert!(mse.is_finite());
    }
}
