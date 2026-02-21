use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::Context;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use etcd_client::{
    Client as EtcdClient, Compare, CompareOp, ConnectOptions, PutOptions, Txn, TxnOp,
};
use futures::future::join_all;
use reqwest::header::CONTENT_TYPE;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use vectordb::index::{
    MutationCommitMode, SpFreshConfig, SpFreshLayerDbConfig, SpFreshLayerDbShardedConfig,
    SpFreshLayerDbShardedIndex, SpFreshLayerDbShardedStats, SpFreshMemoryMode, VectorMutation,
    VectorMutationBatchResult,
};
use vectordb::types::{Neighbor, VectorIndex, VectorRecord};

#[derive(Debug, Parser)]
#[command(name = "vectordb-deploy")]
#[command(about = "Sharded VectorDB deployment server with leader-gated writes")]
struct Args {
    #[arg(long)]
    db_root: PathBuf,

    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: String,

    #[arg(long)]
    node_id: String,

    #[arg(long)]
    self_url: Option<String>,

    #[arg(long, value_delimiter = ',', num_args = 0..)]
    replica_urls: Vec<String>,

    #[arg(long, value_delimiter = ',', default_value = "http://127.0.0.1:2379")]
    etcd_endpoints: Vec<String>,

    #[arg(long, default_value = "/vectordb/deploy/leader")]
    etcd_election_key: String,

    #[arg(long, default_value_t = 10)]
    etcd_lease_ttl_secs: i64,

    #[arg(long, default_value_t = 500)]
    etcd_retry_ms: u64,

    #[arg(long, default_value_t = 1_500)]
    etcd_connect_timeout_ms: u64,

    #[arg(long, default_value_t = 2_000)]
    etcd_request_timeout_ms: u64,

    #[arg(long)]
    etcd_username: Option<String>,

    #[arg(long)]
    etcd_password: Option<String>,

    #[arg(long, default_value_t = 1_500)]
    replication_timeout_ms: u64,

    #[arg(long, default_value_t = 4)]
    shards: usize,

    #[arg(long, default_value_t = 64)]
    dim: usize,

    #[arg(long, default_value_t = 64)]
    initial_postings: usize,

    #[arg(long, default_value_t = 512)]
    split_limit: usize,

    #[arg(long, default_value_t = 64)]
    merge_limit: usize,

    #[arg(long, default_value_t = 64)]
    reassign_range: usize,

    #[arg(long, default_value_t = 8)]
    nprobe: usize,

    #[arg(long, default_value_t = 8)]
    kmeans_iters: usize,

    #[arg(long, value_enum, default_value_t = MemoryModeArg::Resident)]
    memory_mode: MemoryModeArg,

    #[arg(long, default_value_t = 2_000)]
    rebuild_pending_ops: usize,

    #[arg(long, default_value_t = 500)]
    rebuild_interval_ms: u64,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum MemoryModeArg {
    Resident,
    Offheap,
    Diskmeta,
}

impl From<MemoryModeArg> for SpFreshMemoryMode {
    fn from(value: MemoryModeArg) -> Self {
        match value {
            MemoryModeArg::Resident => SpFreshMemoryMode::Resident,
            MemoryModeArg::Offheap => SpFreshMemoryMode::OffHeap,
            MemoryModeArg::Diskmeta => SpFreshMemoryMode::OffHeapDiskMeta,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CommitModeArg {
    Durable,
    Acknowledged,
}

impl From<CommitModeArg> for MutationCommitMode {
    fn from(value: CommitModeArg) -> Self {
        match value {
            CommitModeArg::Durable => MutationCommitMode::Durable,
            CommitModeArg::Acknowledged => MutationCommitMode::Acknowledged,
        }
    }
}

#[derive(Debug)]
struct EtcdLeaderElector {
    node_id: String,
    cfg: EtcdElectionConfig,
    is_leader: AtomicBool,
    ownership: Mutex<Option<LeaderOwnership>>,
}

#[derive(Debug, Clone)]
struct EtcdElectionConfig {
    endpoints: Vec<String>,
    election_key: String,
    lease_ttl_secs: i64,
    retry_backoff: Duration,
    connect_timeout: Duration,
    request_timeout: Duration,
    username: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct LeaderOwnership {
    lease_id: i64,
}

impl EtcdLeaderElector {
    fn new(node_id: String, cfg: EtcdElectionConfig) -> anyhow::Result<Self> {
        if cfg.endpoints.is_empty() {
            anyhow::bail!("--etcd-endpoints must not be empty");
        }
        if cfg.election_key.is_empty() {
            anyhow::bail!("--etcd-election-key must not be empty");
        }
        if cfg.lease_ttl_secs < 2 {
            anyhow::bail!("--etcd-lease-ttl-secs must be >= 2");
        }
        if cfg.username.is_some() ^ cfg.password.is_some() {
            anyhow::bail!("--etcd-username and --etcd-password must be set together");
        }

        Ok(Self {
            node_id,
            cfg,
            is_leader: AtomicBool::new(false),
            ownership: Mutex::new(None),
        })
    }

    fn connect_options(&self) -> ConnectOptions {
        let mut options = ConnectOptions::new()
            .with_require_leader(true)
            .with_connect_timeout(self.cfg.connect_timeout)
            .with_timeout(self.cfg.request_timeout)
            .with_keep_alive(Duration::from_secs(2), Duration::from_secs(2))
            .with_keep_alive_while_idle(true);
        if let (Some(username), Some(password)) = (&self.cfg.username, &self.cfg.password) {
            options = options.with_user(username.clone(), password.clone());
        }
        options
    }

    async fn connect(&self) -> anyhow::Result<EtcdClient> {
        EtcdClient::connect(self.cfg.endpoints.clone(), Some(self.connect_options()))
            .await
            .with_context(|| format!("connect to etcd endpoints {}", self.cfg.endpoints.join(",")))
    }

    fn mark_leader(&self, lease_id: i64) {
        if let Ok(mut ownership) = self.ownership.lock() {
            *ownership = Some(LeaderOwnership { lease_id });
        }
        self.is_leader.store(true, Ordering::Relaxed);
    }

    fn clear_leader(&self) -> Option<LeaderOwnership> {
        self.is_leader.store(false, Ordering::Relaxed);
        match self.ownership.lock() {
            Ok(mut ownership) => ownership.take(),
            Err(_) => None,
        }
    }

    fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
    }

    fn role(&self) -> &'static str {
        if self.is_leader() {
            "leader"
        } else {
            "follower"
        }
    }

    async fn release(&self) {
        let Some(lease) = self.clear_leader() else {
            return;
        };
        if let Ok(mut client) = self.connect().await {
            let _ = client.lease_revoke(lease.lease_id).await;
        }
    }

    async fn run(self: Arc<Self>) {
        loop {
            if let Err(err) = self.election_round().await {
                self.clear_leader();
                eprintln!("leader-election error: {err:#}");
            }
            tokio::time::sleep(self.cfg.retry_backoff).await;
        }
    }

    async fn election_round(&self) -> anyhow::Result<()> {
        let mut client = self.connect().await?;
        let lease = client
            .lease_grant(self.cfg.lease_ttl_secs, None)
            .await
            .context("grant etcd lease")?;
        let lease_id = lease.id();

        let tx = Txn::new()
            .when(vec![Compare::version(
                self.cfg.election_key.as_bytes(),
                CompareOp::Equal,
                0,
            )])
            .and_then(vec![TxnOp::put(
                self.cfg.election_key.as_bytes(),
                self.node_id.as_bytes(),
                Some(PutOptions::new().with_lease(lease_id)),
            )]);

        let txn = client.txn(tx).await.context("acquire etcd leader key")?;
        if !txn.succeeded() {
            let _ = client.lease_revoke(lease_id).await;
            return Ok(());
        }

        self.mark_leader(lease_id);

        let keepalive_every = Duration::from_secs((self.cfg.lease_ttl_secs / 3).max(1) as u64);
        let (mut keeper, mut stream) = client
            .lease_keep_alive(lease_id)
            .await
            .context("open etcd keepalive stream")?;

        keeper
            .keep_alive()
            .await
            .context("send initial keepalive")?;
        stream
            .message()
            .await
            .context("read initial keepalive response")?
            .ok_or_else(|| anyhow::anyhow!("etcd keepalive stream closed"))?;

        loop {
            tokio::time::sleep(keepalive_every).await;
            if !self.is_leader() {
                break;
            }

            keeper
                .keep_alive()
                .await
                .context("send keepalive heartbeat")?;
            let ack = stream
                .message()
                .await
                .context("read keepalive heartbeat response")?
                .ok_or_else(|| anyhow::anyhow!("etcd keepalive stream closed"))?;
            if ack.ttl() <= 0 {
                anyhow::bail!("etcd lease ttl expired");
            }

            let response = client
                .get(self.cfg.election_key.as_bytes(), None)
                .await
                .context("verify leader key ownership")?;
            let Some(kv) = response.kvs().first() else {
                anyhow::bail!("leader key deleted");
            };
            if kv.lease() != lease_id {
                anyhow::bail!("leader key lease mismatch");
            }
            if kv.value() != self.node_id.as_bytes() {
                anyhow::bail!("leader key value mismatch");
            }
        }

        let _ = client.lease_revoke(lease_id).await;
        self.clear_leader();
        Ok(())
    }
}

#[derive(Clone)]
struct AppState {
    node_id: String,
    self_url: Option<String>,
    replica_urls: Vec<String>,
    replication_timeout: Duration,
    index: Arc<RwLock<SpFreshLayerDbShardedIndex>>,
    leader: Arc<EtcdLeaderElector>,
    http: Client,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

type ApiError = (StatusCode, Json<ErrorResponse>);
type ApiResult<T> = Result<Json<T>, ApiError>;

#[derive(Debug, Serialize)]
struct RoleResponse {
    node_id: String,
    role: String,
    leader: bool,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    node_id: String,
    role: String,
    leader: bool,
    stats: SpFreshLayerDbShardedStats,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MutationInput {
    Upsert { id: u64, values: Vec<f32> },
    Delete { id: u64 },
}

impl MutationInput {
    fn into_runtime(self) -> VectorMutation {
        match self {
            MutationInput::Upsert { id, values } => {
                VectorMutation::Upsert(VectorRecord::new(id, values))
            }
            MutationInput::Delete { id } => VectorMutation::Delete { id },
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ApplyMutationsRequest {
    mutations: Vec<MutationInput>,
    #[serde(default)]
    commit_mode: Option<CommitModeArg>,
}

#[derive(Debug, Serialize)]
struct ReplicationSummary {
    attempted: usize,
    succeeded: usize,
    failed: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ApplyMutationsResponse {
    applied_upserts: usize,
    applied_deletes: usize,
    replicated: ReplicationSummary,
}

#[derive(Debug, Deserialize)]
struct SearchRequest {
    query: Vec<f32>,
    k: usize,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    neighbors: Vec<Neighbor>,
}

#[derive(Debug, Deserialize)]
struct SyncToS3Request {
    max_files_per_shard: Option<usize>,
}

#[derive(Debug, Serialize)]
struct SyncToS3Response {
    moved_files: usize,
}

fn api_error(status: StatusCode, message: impl Into<String>) -> ApiError {
    (
        status,
        Json(ErrorResponse {
            error: message.into(),
        }),
    )
}

fn ensure_leader(state: &AppState) -> Result<(), ApiError> {
    if state.leader.is_leader() {
        return Ok(());
    }
    Err(api_error(
        StatusCode::CONFLICT,
        "write rejected: this node is not leader",
    ))
}

async fn role(State(state): State<Arc<AppState>>) -> ApiResult<RoleResponse> {
    Ok(Json(RoleResponse {
        node_id: state.node_id.clone(),
        role: state.leader.role().to_string(),
        leader: state.leader.is_leader(),
    }))
}

async fn health(State(state): State<Arc<AppState>>) -> ApiResult<HealthResponse> {
    let index = state.index.clone();
    let stats = tokio::task::spawn_blocking(move || {
        let index = index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        index.health_check()
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join health task: {err}"),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })?;

    Ok(Json(HealthResponse {
        node_id: state.node_id.clone(),
        role: state.leader.role().to_string(),
        leader: state.leader.is_leader(),
        stats,
    }))
}

async fn search(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SearchRequest>,
) -> ApiResult<SearchResponse> {
    if req.k == 0 {
        return Err(api_error(StatusCode::BAD_REQUEST, "k must be > 0"));
    }
    if req.query.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "query must not be empty",
        ));
    }

    let SearchRequest { query, k } = req;
    let index = state.index.clone();
    let neighbors = tokio::task::spawn_blocking(move || {
        let index = index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        Ok::<_, anyhow::Error>(index.search(&query, k))
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join search task: {err}"),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })?;

    Ok(Json(SearchResponse { neighbors }))
}

async fn force_rebuild(State(state): State<Arc<AppState>>) -> ApiResult<serde_json::Value> {
    ensure_leader(&state)?;

    let index = state.index.clone();
    tokio::task::spawn_blocking(move || {
        let index = index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        index.force_rebuild()
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join rebuild task: {err}"),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })?;

    Ok(Json(serde_json::json!({"ok": true})))
}

async fn sync_to_s3(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncToS3Request>,
) -> ApiResult<SyncToS3Response> {
    ensure_leader(&state)?;

    let index = state.index.clone();
    let moved = tokio::task::spawn_blocking(move || {
        let index = index
            .read()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        index.sync_to_s3(req.max_files_per_shard)
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join sync task: {err}"),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })?;

    Ok(Json(SyncToS3Response { moved_files: moved }))
}

async fn apply_mutations(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ApplyMutationsRequest>,
) -> ApiResult<ApplyMutationsResponse> {
    ensure_leader(&state)?;
    if req.mutations.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "mutations must not be empty",
        ));
    }

    let peers = replication_peers(&state);
    let replication_payload = if peers.is_empty() {
        None
    } else {
        Some(serde_json::to_vec(&req).map(Bytes::from).map_err(|err| {
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("encode replication payload: {err}"),
            )
        })?)
    };

    let applied = apply_local_mutations(state.clone(), req).await?;
    let replicated = if let Some(payload) = replication_payload {
        replicate_to_followers(state.clone(), peers, payload).await
    } else {
        ReplicationSummary {
            attempted: 0,
            succeeded: 0,
            failed: Vec::new(),
        }
    };

    Ok(Json(ApplyMutationsResponse {
        applied_upserts: applied.upserts,
        applied_deletes: applied.deletes,
        replicated,
    }))
}

async fn apply_replication(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ApplyMutationsRequest>,
) -> ApiResult<ApplyMutationsResponse> {
    if req.mutations.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "mutations must not be empty",
        ));
    }

    let applied = apply_local_mutations(state, req).await?;
    Ok(Json(ApplyMutationsResponse {
        applied_upserts: applied.upserts,
        applied_deletes: applied.deletes,
        replicated: ReplicationSummary {
            attempted: 0,
            succeeded: 0,
            failed: Vec::new(),
        },
    }))
}

async fn apply_local_mutations(
    state: Arc<AppState>,
    req: ApplyMutationsRequest,
) -> Result<VectorMutationBatchResult, ApiError> {
    let ApplyMutationsRequest {
        mutations,
        commit_mode,
    } = req;
    let mode = commit_mode.unwrap_or(CommitModeArg::Durable).into();
    let mutations: Vec<VectorMutation> = mutations
        .into_iter()
        .map(MutationInput::into_runtime)
        .collect();
    let index = state.index.clone();

    tokio::task::spawn_blocking(move || {
        let mut index = index
            .write()
            .map_err(|_| anyhow::anyhow!("index lock poisoned"))?;
        index.try_apply_batch_with_commit_mode(&mutations, mode)
    })
    .await
    .map_err(|err| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("join apply task: {err}"),
        )
    })
    .and_then(|result| {
        result.map_err(|err| api_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    })
}

fn normalize_base_url(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

fn join_base_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn replication_peers(state: &AppState) -> Vec<String> {
    collect_replication_peers(state.self_url.as_deref(), &state.replica_urls)
}

fn collect_replication_peers(self_url: Option<&str>, replica_urls: &[String]) -> Vec<String> {
    let self_base = self_url.map(normalize_base_url);
    let mut seen = HashSet::new();
    replica_urls
        .iter()
        .map(|url| normalize_base_url(url))
        .filter(|url| {
            self_base
                .as_ref()
                .map(|self_url| self_url != url)
                .unwrap_or(true)
        })
        .filter(|url| seen.insert(url.clone()))
        .collect()
}

async fn replicate_to_followers(
    state: Arc<AppState>,
    peers: Vec<String>,
    payload: Bytes,
) -> ReplicationSummary {
    if peers.is_empty() {
        return ReplicationSummary {
            attempted: 0,
            succeeded: 0,
            failed: Vec::new(),
        };
    }

    let futures = peers.iter().map(|peer| {
        let endpoint = join_base_url(peer, "/v1/internal/replicate");
        let peer_name = peer.clone();
        let body = payload.clone();
        let client = state.http.clone();
        let timeout = state.replication_timeout;
        async move {
            let response = client
                .post(&endpoint)
                .timeout(timeout)
                .header(CONTENT_TYPE, "application/json")
                .body(body)
                .send()
                .await;
            match response {
                Ok(resp) if resp.status().is_success() => Ok(peer_name),
                Ok(resp) => Err(format!("{peer_name}: http {}", resp.status())),
                Err(err) => Err(format!("{peer_name}: {err}")),
            }
        }
    });

    let mut succeeded = 0usize;
    let mut failed = Vec::new();
    for result in join_all(futures).await {
        match result {
            Ok(_) => succeeded += 1,
            Err(err) => failed.push(err),
        }
    }

    ReplicationSummary {
        attempted: peers.len(),
        succeeded,
        failed,
    }
}

fn build_index(args: &Args) -> anyhow::Result<SpFreshLayerDbShardedIndex> {
    if args.shards == 0 {
        anyhow::bail!("--shards must be > 0");
    }

    let shard_cfg = SpFreshLayerDbConfig {
        spfresh: SpFreshConfig {
            dim: args.dim,
            initial_postings: args.initial_postings,
            split_limit: args.split_limit,
            merge_limit: args.merge_limit,
            reassign_range: args.reassign_range,
            nprobe: args.nprobe,
            kmeans_iters: args.kmeans_iters,
        },
        rebuild_pending_ops: args.rebuild_pending_ops,
        rebuild_interval: Duration::from_millis(args.rebuild_interval_ms),
        memory_mode: args.memory_mode.into(),
        ..Default::default()
    };

    let cfg = SpFreshLayerDbShardedConfig {
        shard_count: args.shards,
        shard: shard_cfg,
    };

    SpFreshLayerDbShardedIndex::open(&args.db_root, cfg)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.etcd_username.is_some() ^ args.etcd_password.is_some() {
        anyhow::bail!("--etcd-username and --etcd-password must be set together");
    }

    let index = build_index(&args).context("open sharded index")?;

    let election_cfg = EtcdElectionConfig {
        endpoints: args.etcd_endpoints.clone(),
        election_key: args.etcd_election_key.clone(),
        lease_ttl_secs: args.etcd_lease_ttl_secs.max(2),
        retry_backoff: Duration::from_millis(args.etcd_retry_ms.max(100)),
        connect_timeout: Duration::from_millis(args.etcd_connect_timeout_ms.max(100)),
        request_timeout: Duration::from_millis(args.etcd_request_timeout_ms.max(100)),
        username: args.etcd_username.clone(),
        password: args.etcd_password.clone(),
    };
    let leader = Arc::new(
        EtcdLeaderElector::new(args.node_id.clone(), election_cfg).context("init etcd election")?,
    );
    let leader_task = tokio::spawn(leader.clone().run());

    let state = Arc::new(AppState {
        node_id: args.node_id.clone(),
        self_url: args.self_url.clone().map(|u| normalize_base_url(&u)),
        replica_urls: args.replica_urls.clone(),
        replication_timeout: Duration::from_millis(args.replication_timeout_ms.max(100)),
        index: Arc::new(RwLock::new(index)),
        leader,
        http: Client::new(),
    });

    let app = Router::new()
        .route("/v1/role", get(role))
        .route("/v1/health", get(health))
        .route("/v1/search", post(search))
        .route("/v1/mutations", post(apply_mutations))
        .route("/v1/internal/replicate", post(apply_replication))
        .route("/v1/force-rebuild", post(force_rebuild))
        .route("/v1/sync-to-s3", post(sync_to_s3))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;

    println!(
        "vectordb-deploy node_id={} listen={} shards={} db_root={} etcd_endpoints={} election_key={} replicas={}",
        args.node_id,
        args.listen,
        args.shards,
        args.db_root.display(),
        args.etcd_endpoints.join(","),
        args.etcd_election_key,
        args.replica_urls.len(),
    );

    let server = axum::serve(listener, app);
    tokio::select! {
        result = server => {
            if let Err(err) = result {
                eprintln!("server error: {err}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("shutdown signal received");
        }
    }

    state.leader.release().await;
    leader_task.abort();
    let _ = leader_task.await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::collect_replication_peers;

    #[test]
    fn collect_replication_peers_dedups_and_skips_self() {
        let peers = vec![
            "http://127.0.0.1:8081/".to_string(),
            "http://127.0.0.1:8081".to_string(),
            "http://127.0.0.1:8080".to_string(),
            "http://127.0.0.1:8082/".to_string(),
        ];
        let out = collect_replication_peers(Some("http://127.0.0.1:8080/"), &peers);
        assert_eq!(
            out,
            vec![
                "http://127.0.0.1:8081".to_string(),
                "http://127.0.0.1:8082".to_string()
            ]
        );
    }
}
