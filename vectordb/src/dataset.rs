use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::types::VectorRecord;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyntheticDataset {
    pub base: Vec<VectorRecord>,
    pub updates: Vec<(u64, Vec<f32>)>,
    pub queries: Vec<Vec<f32>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyntheticConfig {
    pub seed: u64,
    pub dim: usize,
    pub base_count: usize,
    pub update_count: usize,
    pub query_count: usize,
}

impl Default for SyntheticConfig {
    fn default() -> Self {
        Self {
            seed: 0x5EED_1234_ABCD,
            dim: 64,
            base_count: 10_000,
            update_count: 1_000,
            query_count: 200,
        }
    }
}

pub fn generate_synthetic(config: &SyntheticConfig) -> SyntheticDataset {
    let mut rng = StdRng::seed_from_u64(config.seed);
    let mut base = Vec::with_capacity(config.base_count);
    for id in 0..config.base_count as u64 {
        base.push(VectorRecord::new(id, random_vector(config.dim, &mut rng)));
    }

    let mut updates = Vec::with_capacity(config.update_count);
    for _ in 0..config.update_count {
        let id = rng.gen_range(0..config.base_count as u64);
        let mut v = random_vector(config.dim, &mut rng);
        if rng.gen_bool(0.2) {
            // Simulate distribution drift with denser tail dimensions.
            for x in &mut v[config.dim / 2..] {
                *x *= 2.5;
            }
        }
        updates.push((id, v));
    }

    let mut queries = Vec::with_capacity(config.query_count);
    for _ in 0..config.query_count {
        queries.push(random_vector(config.dim, &mut rng));
    }

    SyntheticDataset {
        base,
        updates,
        queries,
    }
}

fn random_vector(dim: usize, rng: &mut StdRng) -> Vec<f32> {
    let mut out = Vec::with_capacity(dim);
    for _ in 0..dim {
        out.push(rng.gen_range(-1.0..1.0));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{generate_synthetic, SyntheticConfig};

    #[test]
    fn synthetic_is_reproducible() {
        let cfg = SyntheticConfig {
            seed: 42,
            dim: 8,
            base_count: 10,
            update_count: 3,
            query_count: 2,
        };
        let a = generate_synthetic(&cfg);
        let b = generate_synthetic(&cfg);
        assert_eq!(a.base[0].values, b.base[0].values);
        assert_eq!(a.updates[0].1, b.updates[0].1);
        assert_eq!(a.queries[0], b.queries[0]);
    }
}
