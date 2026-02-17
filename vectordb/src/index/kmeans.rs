use crate::linalg::{argmin_l2, mean};

pub(crate) fn l2_kmeans(vectors: &[Vec<f32>], k: usize, iters: usize) -> Vec<Vec<f32>> {
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
