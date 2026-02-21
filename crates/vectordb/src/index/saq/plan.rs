use super::{SaqConfig, SaqPlan, SaqSegment};

pub(crate) fn train_plan(cfg: &SaqConfig, sample: &[Vec<f32>]) -> SaqPlan {
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
    let seg_size = cfg
        .max_dims_per_segment
        .max(cfg.min_dims_per_segment)
        .min(dim.max(1));
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

pub(crate) fn allocate_joint_segments(cfg: &SaqConfig, dim_variances: &[f32]) -> Vec<SaqSegment> {
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

    let max_seg = cfg
        .max_dims_per_segment
        .max(cfg.min_dims_per_segment)
        .min(d);
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
