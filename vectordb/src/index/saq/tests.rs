use crate::types::{VectorIndex, VectorRecord};

use super::plan::{allocate_joint_segments, train_plan};
use super::{SaqConfig, SaqIndex};

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
