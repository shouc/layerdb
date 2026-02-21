//! Compaction planning and execution scaffolding.
//!
//! Milestone 1 compaction currently lives inside `version::VersionSet`.
//! This module defines reusable types for milestone 3+ where compaction picks,
//! metrics, and execution become decoupled and tier-aware.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::version::manifest::AddFile;

#[derive(Debug, Clone, Default)]
pub struct LevelMetrics {
    pub bytes: u64,
    pub file_count: usize,
    pub overlap_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub struct CompactionScore {
    pub level: u8,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct CompactionTask {
    pub source_level: u8,
    pub target_level: u8,
    pub smallest_user_key: Bytes,
    pub largest_user_key: Bytes,
    pub inputs: Vec<AddFile>,
}

#[derive(Debug, Clone)]
pub struct CompactionOptions {
    pub target_level_bytes: BTreeMap<u8, u64>,
    pub l0_file_trigger: usize,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        let mut target_level_bytes = BTreeMap::new();
        target_level_bytes.insert(0, 256 * 1024 * 1024);
        target_level_bytes.insert(1, 512 * 1024 * 1024);
        target_level_bytes.insert(2, 2 * 1024 * 1024 * 1024);
        Self {
            target_level_bytes,
            l0_file_trigger: 4,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CompactionPicker;

impl CompactionPicker {
    pub fn level_score(level: u8, metrics: &LevelMetrics, options: &CompactionOptions) -> f64 {
        if level == 0 {
            let by_files = metrics.file_count as f64 / options.l0_file_trigger.max(1) as f64;
            let target = options
                .target_level_bytes
                .get(&0)
                .copied()
                .unwrap_or(256 * 1024 * 1024);
            let by_bytes = metrics.bytes as f64 / target as f64;
            by_files.max(by_bytes)
        } else {
            let target = options.target_level_bytes.get(&level).copied().unwrap_or(1);
            let base = metrics.bytes as f64 / target as f64;
            let overlap_penalty = if metrics.bytes == 0 {
                0.0
            } else {
                metrics.overlap_bytes as f64 / metrics.bytes as f64
            };
            base + overlap_penalty
        }
    }

    pub fn pick_highest_score(
        level_metrics: &BTreeMap<u8, LevelMetrics>,
        options: &CompactionOptions,
    ) -> Option<CompactionScore> {
        level_metrics
            .iter()
            .map(|(level, metrics)| CompactionScore {
                level: *level,
                score: Self::level_score(*level, metrics, options),
            })
            .max_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }

    pub fn should_compact(
        level_metrics: &BTreeMap<u8, LevelMetrics>,
        options: &CompactionOptions,
    ) -> bool {
        Self::pick_highest_score(level_metrics, options)
            .map(|s| s.score >= 1.0)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CompactionRunner;

impl CompactionRunner {
    pub fn estimate_output_files(input_bytes: u64, target_file_size_bytes: u64) -> usize {
        if input_bytes == 0 {
            return 0;
        }
        let target = target_file_size_bytes.max(1);
        input_bytes.div_ceil(target) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picker_prefers_overfull_level() {
        let opts = CompactionOptions::default();
        let mut levels = BTreeMap::new();
        levels.insert(
            0,
            LevelMetrics {
                bytes: 20,
                file_count: 6,
                overlap_bytes: 0,
            },
        );
        levels.insert(
            1,
            LevelMetrics {
                bytes: 10,
                file_count: 1,
                overlap_bytes: 0,
            },
        );

        let pick = CompactionPicker::pick_highest_score(&levels, &opts).expect("pick");
        assert_eq!(pick.level, 0);
        assert!(pick.score >= 1.0);
        assert!(CompactionPicker::should_compact(&levels, &opts));
    }

    #[test]
    fn output_file_estimate_rounds_up() {
        assert_eq!(CompactionRunner::estimate_output_files(0, 100), 0);
        assert_eq!(CompactionRunner::estimate_output_files(100, 100), 1);
        assert_eq!(CompactionRunner::estimate_output_files(101, 100), 2);
    }
}
