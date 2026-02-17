use std::collections::HashSet;

use crate::linalg::squared_l2;
use crate::types::{Neighbor, VectorRecord};

pub fn exact_knn(records: &[VectorRecord], query: &[f32], k: usize) -> Vec<Neighbor> {
    if k == 0 {
        return Vec::new();
    }

    let mut out: Vec<Neighbor> = records
        .iter()
        .filter(|r| !r.deleted)
        .map(|r| Neighbor {
            id: r.id,
            distance: squared_l2(&r.values, query),
        })
        .collect();
    out.sort_by(|a, b| a.distance.total_cmp(&b.distance).then_with(|| a.id.cmp(&b.id)));
    out.truncate(k);
    out
}

pub fn recall_at_k(approx: &[Neighbor], exact: &[Neighbor], k: usize) -> f32 {
    if k == 0 {
        return 1.0;
    }
    let exact_ids: HashSet<u64> = exact.iter().take(k).map(|n| n.id).collect();
    if exact_ids.is_empty() {
        return 1.0;
    }
    let hit = approx
        .iter()
        .take(k)
        .filter(|n| exact_ids.contains(&n.id))
        .count();
    hit as f32 / exact_ids.len() as f32
}

#[cfg(test)]
mod tests {
    use super::{exact_knn, recall_at_k};
    use crate::types::{Neighbor, VectorRecord};

    #[test]
    fn exact_knn_orders_by_distance() {
        let rows = vec![
            VectorRecord::new(1, vec![0.0, 0.0]),
            VectorRecord::new(2, vec![1.0, 0.0]),
            VectorRecord::new(3, vec![4.0, 0.0]),
        ];
        let result = exact_knn(&rows, &[1.1, 0.0], 2);
        assert_eq!(result[0].id, 2);
        assert_eq!(result[1].id, 1);
    }

    #[test]
    fn recall_is_fractional() {
        let approx = vec![
            Neighbor {
                id: 1,
                distance: 0.1,
            },
            Neighbor {
                id: 2,
                distance: 0.2,
            },
        ];
        let exact = vec![
            Neighbor {
                id: 2,
                distance: 0.0,
            },
            Neighbor {
                id: 3,
                distance: 0.1,
            },
        ];
        assert!((recall_at_k(&approx, &exact, 2) - 0.5).abs() < 1e-5);
    }
}
