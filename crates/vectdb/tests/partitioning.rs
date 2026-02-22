use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Context;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use layerdb::DbOptions;
use serde::Deserialize;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use vectdb::cluster::{
    ApplyMutationsRequest, ClusterClientError, CommitMode, HealthResponse, Mutation,
    PartitionReplicationState, ReplicationState, RoleResponse, VectDbClusterClient,
};
use vectdb::index::{
    SpFreshConfig, SpFreshLayerDbConfig, SpFreshLayerDbShardedConfig, SpFreshLayerDbShardedIndex,
    SpFreshLayerDbShardedStats, VectorMutation,
};
use vectdb::topology::ClusterTopology;
use vectdb::types::VectorIndex;
use vectdb::VectorRecord;

#[derive(Clone)]
struct PartitionServerState {
    node_id: String,
    partition_count: u32,
    leader: Arc<AtomicBool>,
    partition_replication: Vec<PartitionReplicationState>,
    applied: Arc<Mutex<BTreeMap<u32, BTreeSet<u64>>>>,
}

impl PartitionServerState {
    fn new(node_id: &str, partition_count: u32, leader: Arc<AtomicBool>) -> Self {
        Self {
            node_id: node_id.to_string(),
            partition_count,
            leader,
            partition_replication: Vec::new(),
            applied: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn with_partition_replication(mut self, replication: Vec<PartitionReplicationState>) -> Self {
        self.partition_replication = replication;
        self
    }

    fn snapshot_applied_ids(&self) -> BTreeMap<u32, Vec<u64>> {
        let guard = self.applied.lock().expect("partition state lock poisoned");
        guard
            .iter()
            .map(|(partition, ids)| (*partition, ids.iter().copied().collect()))
            .collect()
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MutationWire {
    Upsert { id: u64, values: Vec<f32> },
    Delete { id: u64 },
}

impl MutationWire {
    fn id(&self) -> u64 {
        match self {
            Self::Upsert { id, .. } | Self::Delete { id } => *id,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ApplyMutationsWireRequest {
    mutations: Vec<MutationWire>,
    #[serde(default)]
    allow_partial: bool,
}

fn partition_for_id(partition_count: u32, id: u64) -> u32 {
    (id % partition_count as u64) as u32
}

fn sample_replication_state() -> ReplicationState {
    ReplicationState {
        first_seq: 1,
        next_seq: 1,
        last_applied_seq: 0,
        max_term_seen: 0,
    }
}

fn sample_stats() -> SpFreshLayerDbShardedStats {
    SpFreshLayerDbShardedStats {
        shard_count: 4,
        total_rows: 16,
        total_upserts: 10,
        total_deletes: 2,
        persist_errors: 0,
        total_persist_upsert_us: 0,
        total_persist_delete_us: 0,
        rebuild_successes: 0,
        rebuild_failures: 0,
        total_rebuild_applied_ids: 0,
        total_searches: 0,
        total_search_latency_us: 0,
        pending_ops: 0,
    }
}

async fn role(State(state): State<PartitionServerState>) -> Json<RoleResponse> {
    let leader = state.leader.load(Ordering::Relaxed);
    Json(RoleResponse {
        node_id: state.node_id,
        role: if leader { "leader" } else { "follower" }.to_string(),
        leader,
        replication: sample_replication_state(),
        local_partition_replication: state.partition_replication,
    })
}

async fn health(State(state): State<PartitionServerState>) -> Json<HealthResponse> {
    let leader = state.leader.load(Ordering::Relaxed);
    Json(HealthResponse {
        node_id: state.node_id,
        role: if leader { "leader" } else { "follower" }.to_string(),
        leader,
        stats: sample_stats(),
        replication: sample_replication_state(),
        local_partition_replication: state.partition_replication,
    })
}

async fn apply_mutations(
    State(state): State<PartitionServerState>,
    Json(req): Json<ApplyMutationsWireRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    if !state.leader.load(Ordering::Relaxed) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "write rejected: this node is not leader"
            })),
        );
    }
    if req.mutations.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "mutations must not be empty"
            })),
        );
    }

    let mut touched = BTreeSet::new();
    for mutation in &req.mutations {
        touched.insert(partition_for_id(state.partition_count, mutation.id()));
    }
    if touched.len() > 1 && !req.allow_partial {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "cross-partition mutation batch rejected"
            })),
        );
    }

    let mut applied_upserts = 0usize;
    let mut applied_deletes = 0usize;
    let mut guard = state.applied.lock().expect("partition state lock poisoned");
    for mutation in req.mutations {
        let partition = partition_for_id(state.partition_count, mutation.id());
        let bucket = guard.entry(partition).or_default();
        match mutation {
            MutationWire::Upsert { id, values } => {
                let _ = values.len();
                bucket.insert(id);
                applied_upserts = applied_upserts.saturating_add(1);
            }
            MutationWire::Delete { id } => {
                let _ = bucket.remove(&id);
                applied_deletes = applied_deletes.saturating_add(1);
            }
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "applied_upserts": applied_upserts,
            "applied_deletes": applied_deletes,
            "replicated": { "attempted": 0, "succeeded": 0, "failed": [] }
        })),
    )
}

async fn spawn_partition_server(
    state: PartitionServerState,
) -> anyhow::Result<(String, JoinHandle<anyhow::Result<()>>)> {
    let app = Router::new()
        .route("/v1/role", get(role))
        .route("/v1/health", get(health))
        .route("/v1/mutations", post(apply_mutations))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind partition test server")?;
    let addr = listener.local_addr().context("read local addr")?;
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await?;
        Ok(())
    });
    Ok((format!("http://{addr}"), server))
}

async fn legacy_role() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "node_id": "legacy-node",
        "role": "leader",
        "leader": true,
        "replication": {
            "first_seq": 1,
            "next_seq": 2,
            "last_applied_seq": 1,
            "max_term_seen": 5
        }
    }))
}

async fn legacy_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "node_id": "legacy-node",
        "role": "leader",
        "leader": true,
        "stats": {
            "shard_count": 4,
            "total_rows": 10,
            "total_upserts": 10,
            "total_deletes": 0,
            "persist_errors": 0,
            "total_persist_upsert_us": 0,
            "total_persist_delete_us": 0,
            "rebuild_successes": 0,
            "rebuild_failures": 0,
            "total_rebuild_applied_ids": 0,
            "total_searches": 0,
            "total_search_latency_us": 0,
            "pending_ops": 0
        },
        "replication": {
            "first_seq": 1,
            "next_seq": 2,
            "last_applied_seq": 1,
            "max_term_seen": 5
        }
    }))
}

async fn spawn_legacy_partition_server() -> anyhow::Result<(String, JoinHandle<anyhow::Result<()>>)>
{
    let app = Router::new()
        .route("/v1/role", get(legacy_role))
        .route("/v1/health", get(legacy_health));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind legacy partition test server")?;
    let addr = listener.local_addr().context("read local addr")?;
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await?;
        Ok(())
    });
    Ok((format!("http://{addr}"), server))
}

fn upsert(id: u64) -> Mutation {
    Mutation::Upsert {
        id,
        values: vec![id as f32, (id % 7) as f32],
    }
}

#[tokio::test]
async fn cross_partition_mutations_require_allow_partial() -> anyhow::Result<()> {
    let leader = Arc::new(AtomicBool::new(true));
    let state = PartitionServerState::new("node-a", 8, leader);
    let (base_url, _server) = spawn_partition_server(state.clone()).await?;
    let client = VectDbClusterClient::new(base_url.as_str())?;

    let err = client
        .apply_mutations(&ApplyMutationsRequest {
            mutations: vec![upsert(1), upsert(2)],
            commit_mode: Some(CommitMode::Durable),
            allow_partial: false,
        })
        .await
        .expect_err("cross-partition batch without allow_partial must fail");
    match err {
        ClusterClientError::Http(context) => {
            assert_eq!(context.status, StatusCode::BAD_REQUEST);
            assert!(context.message.contains("cross-partition"));
        }
        other => anyhow::bail!("unexpected error kind: {other}"),
    }
    assert!(state.snapshot_applied_ids().is_empty());

    let response = client
        .apply_mutations(&ApplyMutationsRequest {
            mutations: vec![upsert(1), upsert(2)],
            commit_mode: Some(CommitMode::Durable),
            allow_partial: true,
        })
        .await?;
    assert_eq!(response.applied_upserts, 2);
    assert_eq!(response.applied_deletes, 0);

    let applied = state.snapshot_applied_ids();
    assert_eq!(applied.get(&1).cloned().unwrap_or_default(), vec![1]);
    assert_eq!(applied.get(&2).cloned().unwrap_or_default(), vec![2]);
    Ok(())
}

#[tokio::test]
async fn single_partition_mutations_do_not_require_allow_partial() -> anyhow::Result<()> {
    let leader = Arc::new(AtomicBool::new(true));
    let state = PartitionServerState::new("node-a", 8, leader);
    let (base_url, _server) = spawn_partition_server(state.clone()).await?;
    let client = VectDbClusterClient::new(base_url.as_str())?;

    let response = client
        .apply_mutations(&ApplyMutationsRequest {
            mutations: vec![upsert(1), upsert(9), upsert(17)],
            commit_mode: Some(CommitMode::Durable),
            allow_partial: false,
        })
        .await?;
    assert_eq!(response.applied_upserts, 3);

    let applied = state.snapshot_applied_ids();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied.get(&1).cloned().unwrap_or_default(), vec![1, 9, 17]);
    Ok(())
}

#[tokio::test]
async fn leader_failover_preserves_partitioned_write_semantics() -> anyhow::Result<()> {
    let node_a_leader = Arc::new(AtomicBool::new(false));
    let node_b_leader = Arc::new(AtomicBool::new(true));
    let node_a = PartitionServerState::new("node-a", 8, node_a_leader.clone());
    let node_b = PartitionServerState::new("node-b", 8, node_b_leader.clone());
    let (node_a_url, _node_a_server) = spawn_partition_server(node_a.clone()).await?;
    let (node_b_url, _node_b_server) = spawn_partition_server(node_b.clone()).await?;
    let client = VectDbClusterClient::from_endpoints([node_a_url.as_str(), node_b_url.as_str()])?;

    let first = client
        .apply_mutations(&ApplyMutationsRequest {
            mutations: vec![upsert(3), upsert(12)],
            commit_mode: Some(CommitMode::Durable),
            allow_partial: true,
        })
        .await?;
    assert_eq!(first.applied_upserts, 2);
    assert!(node_a.snapshot_applied_ids().is_empty());
    let leader_applied = node_b.snapshot_applied_ids();
    assert_eq!(leader_applied.get(&3).cloned().unwrap_or_default(), vec![3]);
    assert_eq!(
        leader_applied.get(&4).cloned().unwrap_or_default(),
        vec![12]
    );

    node_a_leader.store(true, Ordering::Relaxed);
    node_b_leader.store(false, Ordering::Relaxed);

    let second = client
        .apply_mutations(&ApplyMutationsRequest {
            mutations: vec![upsert(4), upsert(13)],
            commit_mode: Some(CommitMode::Durable),
            allow_partial: true,
        })
        .await?;
    assert_eq!(second.applied_upserts, 2);

    let node_a_applied = node_a.snapshot_applied_ids();
    assert_eq!(node_a_applied.get(&4).cloned().unwrap_or_default(), vec![4]);
    assert_eq!(
        node_a_applied.get(&5).cloned().unwrap_or_default(),
        vec![13]
    );
    let node_b_after = node_b.snapshot_applied_ids();
    assert_eq!(node_b_after.get(&3).cloned().unwrap_or_default(), vec![3]);
    assert_eq!(node_b_after.get(&4).cloned().unwrap_or_default(), vec![12]);
    Ok(())
}

#[test]
fn topology_partitioning_is_order_independent_and_complete() -> anyhow::Result<()> {
    let endpoints = vec![
        "http://node-a:8080".to_string(),
        "http://node-b:8080".to_string(),
        "http://node-c:8080".to_string(),
        "http://node-d:8080".to_string(),
        "http://node-e:8080".to_string(),
    ];
    let mut reversed = endpoints.clone();
    reversed.reverse();

    let topology = ClusterTopology::build_deterministic(42, 257, 3, &endpoints)?;
    let reversed_topology = ClusterTopology::build_deterministic(42, 257, 3, &reversed)?;
    assert_eq!(topology, reversed_topology);

    let mut replica_slots_by_endpoint: BTreeMap<String, usize> = BTreeMap::new();
    for placement in &topology.placements {
        assert_eq!(placement.replicas.len(), 3);
        let unique: BTreeSet<_> = placement.replicas.iter().collect();
        assert_eq!(unique.len(), 3);
        assert!(placement
            .replicas
            .iter()
            .any(|endpoint| endpoint == &placement.leader));
        for endpoint in &placement.replicas {
            *replica_slots_by_endpoint
                .entry(endpoint.clone())
                .or_default() += 1;
        }
    }
    assert_eq!(
        replica_slots_by_endpoint.values().sum::<usize>(),
        topology.partition_count as usize * topology.replication_factor as usize
    );
    for endpoint in &endpoints {
        assert!(
            replica_slots_by_endpoint
                .get(endpoint)
                .copied()
                .unwrap_or_default()
                > 0
        );
    }

    for partition_id in 0..topology.partition_count {
        let placement = topology
            .placement_for_partition(partition_id)
            .context("placement must exist")?;
        assert_eq!(placement.partition_id, partition_id);
    }
    assert_eq!(
        topology.partition_for_id(u64::MAX),
        (u64::MAX % topology.partition_count as u64) as u32
    );
    Ok(())
}

#[test]
fn topology_deduplicates_trimmed_endpoints_and_validates_unique_factor() -> anyhow::Result<()> {
    let endpoints = vec![
        " http://node-a:8080 ".to_string(),
        "http://node-a:8080".to_string(),
        "http://node-b:8080".to_string(),
        "  ".to_string(),
    ];
    let topology = ClusterTopology::build_deterministic(1, 32, 2, &endpoints)?;
    for placement in &topology.placements {
        assert_eq!(placement.replicas.len(), 2);
        assert_eq!(
            placement.replicas[0].trim(),
            placement.replicas[0],
            "replica endpoint should be trimmed"
        );
    }

    let err = ClusterTopology::build_deterministic(1, 32, 3, &endpoints)
        .expect_err("rf over unique endpoint count must fail");
    assert!(err.to_string().contains("replication_factor"));
    Ok(())
}

#[tokio::test]
async fn cluster_client_parses_partition_replication_fields() -> anyhow::Result<()> {
    let leader = Arc::new(AtomicBool::new(true));
    let partition_replication = vec![
        PartitionReplicationState {
            partition_id: 1,
            leader: true,
            first_seq: 10,
            next_seq: 22,
            last_applied_seq: 21,
            max_term_seen: 7,
        },
        PartitionReplicationState {
            partition_id: 5,
            leader: false,
            first_seq: 4,
            next_seq: 9,
            last_applied_seq: 8,
            max_term_seen: 7,
        },
    ];
    let state = PartitionServerState::new("node-a", 8, leader)
        .with_partition_replication(partition_replication.clone());
    let (base_url, _server) = spawn_partition_server(state).await?;
    let client = VectDbClusterClient::new(base_url.as_str())?;

    let role = client.role().await?;
    assert_eq!(role.local_partition_replication, partition_replication);
    let health = client.health().await?;
    assert_eq!(health.local_partition_replication, partition_replication);
    Ok(())
}

#[tokio::test]
async fn cluster_client_defaults_missing_partition_replication_fields() -> anyhow::Result<()> {
    let (base_url, _server) = spawn_legacy_partition_server().await?;
    let client = VectDbClusterClient::new(base_url.as_str())?;
    let role = client.role().await?;
    assert!(role.local_partition_replication.is_empty());
    let health = client.health().await?;
    assert!(health.local_partition_replication.is_empty());
    Ok(())
}

fn model_from_rows(rows: Vec<VectorRecord>) -> BTreeMap<u64, Vec<f32>> {
    let mut out = BTreeMap::new();
    for row in rows {
        out.insert(row.id, row.values);
    }
    out
}

fn apply_model_mutation(model: &mut BTreeMap<u64, Vec<f32>>, mutation: &VectorMutation) {
    match mutation {
        VectorMutation::Upsert(row) => {
            model.insert(row.id, row.values.clone());
        }
        VectorMutation::Delete { id } => {
            model.remove(id);
        }
    }
}

#[test]
fn sharded_index_partition_round_trip_with_non_power_of_two_shards() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cfg = SpFreshLayerDbShardedConfig {
        shard_count: 3,
        exact_shard_prune: false,
        shard: SpFreshLayerDbConfig {
            spfresh: SpFreshConfig {
                dim: 4,
                ..Default::default()
            },
            db_options: DbOptions {
                memtable_shards: 4,
                memtable_bytes: 2 * 1024,
                wal_segment_bytes: 16 * 1024,
                ..Default::default()
            },
            ..Default::default()
        },
    };

    let mut index = SpFreshLayerDbShardedIndex::open(dir.path(), cfg.clone())?;
    let mut model = BTreeMap::new();

    for id in 0..48u64 {
        let values = vec![
            id as f32,
            (id % 5) as f32,
            (id % 11) as f32,
            (id % 17) as f32,
        ];
        index.try_upsert(id, values.clone())?;
        model.insert(id, values);
    }

    let mutations = vec![
        VectorMutation::Upsert(VectorRecord::new(5, vec![500.0, 5.0, 0.0, 1.0])),
        VectorMutation::Delete { id: 8 },
        VectorMutation::Upsert(VectorRecord::new(8, vec![8.1, 8.2, 8.3, 8.4])),
        VectorMutation::Delete { id: 12 },
        VectorMutation::Delete { id: 12 },
        VectorMutation::Upsert(VectorRecord::new(33, vec![330.0, 1.0, 2.0, 3.0])),
        VectorMutation::Upsert(VectorRecord::new(33, vec![333.0, 1.0, 2.0, 3.0])),
    ];
    let result = index.try_apply_batch_owned(mutations.clone())?;
    assert_eq!(result.upserts, 3);
    assert_eq!(result.deletes, 1);
    for mutation in &mutations {
        apply_model_mutation(&mut model, mutation);
    }

    let deleted = index.try_delete_batch(&[1, 4, 7, 10, 13, 16])?;
    assert_eq!(deleted, 6);
    for id in [1u64, 4, 7, 10, 13, 16] {
        model.remove(&id);
    }

    let snapshot_before_close = model_from_rows(index.snapshot_rows()?);
    assert_eq!(snapshot_before_close, model);
    let query = vec![9.0, 1.0, 3.0, 2.0];
    let search_before_close = index.search(&query, 10);
    index.close()?;

    let reopened = SpFreshLayerDbShardedIndex::open_existing(
        dir.path(),
        cfg.shard_count,
        cfg.shard.db_options.clone(),
    )?;
    let snapshot_after_reopen = model_from_rows(reopened.snapshot_rows()?);
    assert_eq!(snapshot_after_reopen, model);
    let search_after_reopen = reopened.search(&query, 10);
    assert_eq!(search_before_close, search_after_reopen);
    Ok(())
}
