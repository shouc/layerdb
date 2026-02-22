use crate::linalg::{dot, norm2};

use super::SaqPlan;

pub(crate) struct EncTmp {
    pub(crate) codes: Vec<u8>,
    pub(crate) v_max: f32,
    pub(crate) norm_sq: f32,
}

pub(crate) fn encode_vector(plan: &SaqPlan, vector: &[f32], caq_rounds: usize) -> EncTmp {
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

pub(crate) fn decode_reordered(plan: &SaqPlan, codes: &[u8], v_max: f32) -> Vec<f32> {
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

pub(crate) fn saq_distance(
    plan: &SaqPlan,
    query_reordered: &[f32],
    query_norm_sq: f32,
    row_codes: &[u8],
    row_v_max: f32,
    row_norm_sq: f32,
) -> f32 {
    let decoded = decode_reordered(plan, row_codes, row_v_max);
    let ip = dot(query_reordered, &decoded);
    query_norm_sq + row_norm_sq - 2.0 * ip
}

pub(crate) fn reorder(values: &[f32], order: &[usize]) -> Vec<f32> {
    let mut out = vec![0.0; values.len()];
    for (new_idx, old_idx) in order.iter().enumerate() {
        out[new_idx] = values[*old_idx];
    }
    out
}

pub(crate) fn restore_order(values_reordered: &[f32], inv_order: &[usize]) -> Vec<f32> {
    let mut out = vec![0.0; values_reordered.len()];
    for (old_idx, new_idx) in inv_order.iter().enumerate() {
        out[old_idx] = values_reordered[*new_idx];
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
