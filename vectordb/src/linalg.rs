pub fn squared_l2(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = x - y;
            d * d
        })
        .sum()
}

pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

pub fn norm2(v: &[f32]) -> f32 {
    dot(v, v)
}

pub fn add_inplace(dst: &mut [f32], src: &[f32]) {
    debug_assert_eq!(dst.len(), src.len());
    for (d, s) in dst.iter_mut().zip(src.iter()) {
        *d += *s;
    }
}

pub fn scale_inplace(dst: &mut [f32], factor: f32) {
    for d in dst {
        *d *= factor;
    }
}

pub fn mean(vectors: &[Vec<f32>], dim: usize) -> Vec<f32> {
    if vectors.is_empty() {
        return vec![0.0; dim];
    }
    let mut out = vec![0.0; dim];
    for v in vectors {
        add_inplace(&mut out, v);
    }
    scale_inplace(&mut out, 1.0 / vectors.len() as f32);
    out
}

pub fn argmin_l2(query: &[f32], centroids: &[Vec<f32>]) -> Option<usize> {
    centroids
        .iter()
        .enumerate()
        .map(|(i, c)| (i, squared_l2(query, c)))
        .min_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(i, _)| i)
}

#[cfg(test)]
mod tests {
    use super::{argmin_l2, dot, mean, squared_l2};

    #[test]
    fn l2_and_dot_are_stable() {
        let a = [1.0, 2.0, 3.0];
        let b = [1.0, 2.5, -1.0];
        assert!((dot(&a, &b) - 3.0).abs() < 1e-5);
        assert!((squared_l2(&a, &b) - 16.25).abs() < 1e-5);
    }

    #[test]
    fn mean_vector_is_correct() {
        let m = mean(&[vec![1.0, 3.0], vec![3.0, 5.0], vec![5.0, 7.0]], 2);
        assert_eq!(m, vec![3.0, 5.0]);
    }

    #[test]
    fn argmin_selects_closest() {
        let query = vec![0.9, 0.1];
        let centroids = vec![vec![0.0, 0.0], vec![1.0, 0.0], vec![0.0, 1.0]];
        assert_eq!(argmin_l2(&query, &centroids), Some(1));
    }
}
