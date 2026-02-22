use std::collections::HashSet;

use anyhow::Context;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionPlacement {
    pub partition_id: u32,
    pub replicas: Vec<String>,
    pub leader: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterTopology {
    pub epoch: u64,
    pub partition_count: u32,
    pub replication_factor: u32,
    pub placements: Vec<PartitionPlacement>,
}

impl ClusterTopology {
    pub fn build_deterministic(
        epoch: u64,
        partition_count: u32,
        replication_factor: u32,
        endpoints: &[String],
    ) -> anyhow::Result<Self> {
        if partition_count == 0 {
            anyhow::bail!("partition_count must be > 0");
        }
        if replication_factor == 0 {
            anyhow::bail!("replication_factor must be > 0");
        }

        let mut unique = Vec::with_capacity(endpoints.len());
        let mut seen = HashSet::with_capacity(endpoints.len());
        for endpoint in endpoints {
            let value = endpoint.trim();
            if value.is_empty() {
                continue;
            }
            if seen.insert(value.to_string()) {
                unique.push(value.to_string());
            }
        }
        if unique.is_empty() {
            anyhow::bail!("at least one endpoint is required");
        }
        if replication_factor as usize > unique.len() {
            anyhow::bail!(
                "replication_factor={} exceeds endpoint count={}",
                replication_factor,
                unique.len()
            );
        }

        let mut placements = Vec::with_capacity(partition_count as usize);
        for partition_id in 0..partition_count {
            let mut scored: Vec<(u64, &str)> = unique
                .iter()
                .map(|endpoint| {
                    (
                        rendezvous_score(partition_id, endpoint.as_str()),
                        endpoint.as_str(),
                    )
                })
                .collect();
            scored.sort_unstable_by(|(lhs_score, lhs_ep), (rhs_score, rhs_ep)| {
                rhs_score.cmp(lhs_score).then_with(|| lhs_ep.cmp(rhs_ep))
            });
            let replicas: Vec<String> = scored
                .iter()
                .take(replication_factor as usize)
                .map(|(_, endpoint)| (*endpoint).to_string())
                .collect();
            let leader = replicas
                .first()
                .cloned()
                .context("replica set must have at least one endpoint")?;
            placements.push(PartitionPlacement {
                partition_id,
                replicas,
                leader,
            });
        }

        let topology = Self {
            epoch,
            partition_count,
            replication_factor,
            placements,
        };
        topology.validate()?;
        Ok(topology)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.partition_count == 0 {
            anyhow::bail!("partition_count must be > 0");
        }
        if self.replication_factor == 0 {
            anyhow::bail!("replication_factor must be > 0");
        }
        if self.placements.len() != self.partition_count as usize {
            anyhow::bail!(
                "placement count {} does not match partition_count {}",
                self.placements.len(),
                self.partition_count
            );
        }
        let mut seen_partition_ids = vec![false; self.partition_count as usize];
        for placement in &self.placements {
            let id = placement.partition_id as usize;
            if id >= self.partition_count as usize {
                anyhow::bail!(
                    "partition placement id {} out of range [0, {})",
                    placement.partition_id,
                    self.partition_count
                );
            }
            if seen_partition_ids[id] {
                anyhow::bail!(
                    "duplicate placement for partition {}",
                    placement.partition_id
                );
            }
            seen_partition_ids[id] = true;
            if placement.replicas.len() != self.replication_factor as usize {
                anyhow::bail!(
                    "partition {} has {} replicas, expected {}",
                    placement.partition_id,
                    placement.replicas.len(),
                    self.replication_factor
                );
            }
            if placement.leader.is_empty() {
                anyhow::bail!(
                    "partition {} leader must not be empty",
                    placement.partition_id
                );
            }
            if !placement
                .replicas
                .iter()
                .any(|endpoint| endpoint == &placement.leader)
            {
                anyhow::bail!(
                    "partition {} leader {} not in replica set",
                    placement.partition_id,
                    placement.leader
                );
            }
            let mut seen_replicas = HashSet::with_capacity(placement.replicas.len());
            for endpoint in &placement.replicas {
                if endpoint.is_empty() {
                    anyhow::bail!(
                        "partition {} has empty replica endpoint",
                        placement.partition_id
                    );
                }
                if !seen_replicas.insert(endpoint) {
                    anyhow::bail!(
                        "partition {} has duplicate replica endpoint {}",
                        placement.partition_id,
                        endpoint
                    );
                }
            }
        }
        if seen_partition_ids.iter().any(|seen| !seen) {
            anyhow::bail!("missing partition placement");
        }
        Ok(())
    }

    pub fn partition_for_id(&self, id: u64) -> u32 {
        (id % self.partition_count as u64) as u32
    }

    pub fn placement_for_partition(&self, partition_id: u32) -> Option<&PartitionPlacement> {
        self.placements.get(partition_id as usize)
    }

    pub fn leader_for_partition(&self, partition_id: u32) -> Option<&str> {
        self.placement_for_partition(partition_id)
            .map(|placement| placement.leader.as_str())
    }

    pub fn replicas_for_partition(&self, partition_id: u32) -> Option<&[String]> {
        self.placement_for_partition(partition_id)
            .map(|placement| placement.replicas.as_slice())
    }

    pub fn hosted_partitions(&self, endpoint: &str) -> Vec<u32> {
        self.placements
            .iter()
            .filter(|placement| placement.replicas.iter().any(|replica| replica == endpoint))
            .map(|placement| placement.partition_id)
            .collect()
    }

    pub fn is_replica(&self, partition_id: u32, endpoint: &str) -> bool {
        self.replicas_for_partition(partition_id)
            .map(|replicas| replicas.iter().any(|replica| replica == endpoint))
            .unwrap_or(false)
    }
}

fn rendezvous_score(partition_id: u32, endpoint: &str) -> u64 {
    let mut high = Hasher::new();
    high.update(partition_id.to_le_bytes().as_ref());
    high.update(&[0x5a, 0x9d, 0x37, 0x11]);
    high.update(endpoint.as_bytes());
    let high = high.finalize() as u64;

    let mut low = Hasher::new();
    low.update(partition_id.to_be_bytes().as_ref());
    low.update(&[0xd3, 0x27, 0x8c, 0x44]);
    low.update(endpoint.as_bytes());
    let low = low.finalize() as u64;
    (high << 32) | low
}

#[cfg(test)]
mod tests {
    use super::ClusterTopology;

    fn endpoints() -> Vec<String> {
        vec![
            "http://node1:8080".to_string(),
            "http://node2:8080".to_string(),
            "http://node3:8080".to_string(),
        ]
    }

    #[test]
    fn deterministic_layout_is_stable() {
        let left = ClusterTopology::build_deterministic(3, 32, 2, &endpoints())
            .expect("left topology should build");
        let right = ClusterTopology::build_deterministic(3, 32, 2, &endpoints())
            .expect("right topology should build");
        assert_eq!(left, right);
    }

    #[test]
    fn validate_rejects_invalid_factor() {
        let err = ClusterTopology::build_deterministic(1, 8, 4, &endpoints())
            .expect_err("rf>node_count should fail");
        assert!(err.to_string().contains("replication_factor"));
    }

    #[test]
    fn hosted_partitions_are_reported() {
        let topology = ClusterTopology::build_deterministic(1, 16, 2, &endpoints())
            .expect("topology should build");
        let hosted = topology.hosted_partitions("http://node1:8080");
        assert!(!hosted.is_empty());
        assert!(hosted.iter().all(|partition| *partition < 16));
    }

    #[test]
    fn partition_mapping_uses_modulo() {
        let topology = ClusterTopology::build_deterministic(1, 8, 2, &endpoints())
            .expect("topology should build");
        assert_eq!(topology.partition_for_id(0), 0);
        assert_eq!(topology.partition_for_id(7), 7);
        assert_eq!(topology.partition_for_id(8), 0);
        assert_eq!(topology.partition_for_id(15), 7);
    }

    #[test]
    fn leader_is_in_replica_set() {
        let topology = ClusterTopology::build_deterministic(1, 12, 2, &endpoints())
            .expect("topology should build");
        for partition in 0..topology.partition_count {
            let leader = topology
                .leader_for_partition(partition)
                .expect("leader should exist");
            let replicas = topology
                .replicas_for_partition(partition)
                .expect("replicas should exist");
            assert!(replicas.iter().any(|replica| replica == leader));
        }
    }
}
