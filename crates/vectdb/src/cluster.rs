use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;

use crate::index::SpFreshLayerDbShardedStats;
use crate::Neighbor;
use bytes::Bytes;
use etcd_client::{Client as EtcdClient, ConnectOptions};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Client, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const LEADER_FAILOVER_MAX_ROUNDS: usize = 4;
const LEADER_FAILOVER_BACKOFF_MS: u64 = 150;
const DEFAULT_ETCD_ELECTION_KEY: &str = "/vectdb/deploy/leader";
const ETCD_CONNECT_TIMEOUT_MS: u64 = 1_500;
const ETCD_REQUEST_TIMEOUT_MS: u64 = 2_000;
const ETCD_DISCOVERY_MAX_ATTEMPTS: usize = 3;
const ETCD_DISCOVERY_RETRY_MS: u64 = 50;
const HTTP_POOL_MAX_IDLE_PER_HOST: usize = 256;
const HTTP_POOL_IDLE_TIMEOUT_SECS: u64 = 90;
const HTTP_CONNECT_TIMEOUT_MS: u64 = 1_500;

pub fn etcd_leader_endpoint_key(election_key: &str) -> String {
    format!("{election_key}_endpoint")
}

#[derive(Debug)]
struct VectDbClusterClientInner {
    endpoints: Vec<Url>,
    http: Client,
    preferred: AtomicUsize,
    etcd_discovery: Option<EtcdDiscoveryConfig>,
    discovered_leader: StdRwLock<Option<Url>>,
}

#[derive(Clone, Debug)]
struct EtcdDiscoveryConfig {
    endpoints: Vec<String>,
    election_key: String,
    leader_endpoint_key: String,
    connect_timeout: Duration,
    request_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct VectDbClusterClient {
    inner: Arc<VectDbClusterClientInner>,
}

impl VectDbClusterClient {
    pub fn new(base_url: &str) -> Result<Self, ClusterClientError> {
        Self::with_http_client(base_url, build_default_http_client()?)
    }

    pub fn with_http_client(base_url: &str, http: Client) -> Result<Self, ClusterClientError> {
        Self::from_endpoints_with_http_client([base_url], http)
    }

    pub fn from_endpoints<I, S>(endpoints: I) -> Result<Self, ClusterClientError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::from_endpoints_with_http_client(endpoints, build_default_http_client()?)
    }

    pub fn from_endpoints_with_http_client<I, S>(
        endpoints: I,
        http: Client,
    ) -> Result<Self, ClusterClientError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let parsed = parse_http_endpoints(endpoints)?;
        Self::build(parsed, http, None)
    }

    pub async fn from_etcd_endpoint(etcd_endpoint: &str) -> Result<Self, ClusterClientError> {
        Self::from_etcd_endpoints([etcd_endpoint]).await
    }

    pub async fn from_etcd_endpoints<I, S>(etcd_endpoints: I) -> Result<Self, ClusterClientError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::from_etcd_endpoints_with_key(etcd_endpoints, DEFAULT_ETCD_ELECTION_KEY).await
    }

    pub async fn from_etcd_endpoints_with_key<I, S>(
        etcd_endpoints: I,
        election_key: &str,
    ) -> Result<Self, ClusterClientError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let discovery = EtcdDiscoveryConfig::new(etcd_endpoints, election_key)?;
        let leader_endpoint = resolve_leader_endpoint_from_etcd(&discovery).await?;
        Self::build(
            vec![leader_endpoint],
            build_default_http_client()?,
            Some(discovery),
        )
    }

    fn build(
        endpoints: Vec<Url>,
        http: Client,
        etcd_discovery: Option<EtcdDiscoveryConfig>,
    ) -> Result<Self, ClusterClientError> {
        if endpoints.is_empty() {
            return Err(ClusterClientError::InvalidBaseUrl(
                "no endpoints configured".to_string(),
            ));
        }
        Ok(Self {
            inner: Arc::new(VectDbClusterClientInner {
                endpoints,
                http,
                preferred: AtomicUsize::new(0),
                etcd_discovery,
                discovered_leader: StdRwLock::new(None),
            }),
        })
    }

    pub fn base_url(&self) -> Url {
        self.cached_discovered_endpoint()
            .unwrap_or_else(|| self.inner.endpoints[self.preferred_index()].clone())
    }

    pub fn endpoints(&self) -> &[Url] {
        self.inner.endpoints.as_slice()
    }

    pub async fn discover_leader(&self) -> Result<RoleResponse, ClusterClientError> {
        let mut last_error = None;
        if let Some(cached) = self.cached_discovered_endpoint() {
            match self.get_json_url::<RoleResponse>(&cached, "v1/role").await {
                Ok(role) => {
                    if role.leader {
                        self.maybe_mark_preferred_url(&cached);
                        return Ok(role);
                    }
                    self.clear_cached_discovered_endpoint_if(&cached);
                }
                Err(err) => {
                    last_error = Some(err);
                    self.clear_cached_discovered_endpoint_if(&cached);
                }
            }
        }

        for idx in self.ordered_endpoint_indices() {
            match self.get_json_at::<RoleResponse>(idx, "v1/role").await {
                Ok(role) => {
                    if role.leader {
                        self.set_preferred(idx);
                        return Ok(role);
                    }
                }
                Err(err) => {
                    last_error = Some(err);
                }
            }
        }
        let discovered = match self.resolve_discovery_leader_endpoint().await {
            Ok(discovered) => discovered,
            Err(err) => {
                last_error = Some(err);
                None
            }
        };
        if let Some(url) = discovered {
            match self.get_json_url::<RoleResponse>(&url, "v1/role").await {
                Ok(role) => {
                    if role.leader {
                        self.set_cached_discovered_endpoint(url);
                        return Ok(role);
                    }
                    self.clear_cached_discovered_endpoint_if(&url);
                }
                Err(err) => last_error = Some(err),
            }
        }
        Err(last_error.unwrap_or(ClusterClientError::LeaderNotFound {
            checked: self.inner.endpoints.len(),
        }))
    }

    pub async fn role(&self) -> Result<RoleResponse, ClusterClientError> {
        self.get_json_failover("v1/role", FailoverClass::AnyEndpoint)
            .await
    }

    pub async fn health(&self) -> Result<HealthResponse, ClusterClientError> {
        self.get_json_failover("v1/health", FailoverClass::AnyEndpoint)
            .await
    }

    pub async fn search(
        &self,
        query: Vec<f32>,
        k: usize,
    ) -> Result<Vec<Neighbor>, ClusterClientError> {
        let response = self.search_with(&SearchRequest { query, k }).await?;
        Ok(response.neighbors)
    }

    pub async fn search_with(
        &self,
        request: &SearchRequest,
    ) -> Result<SearchResponse, ClusterClientError> {
        self.post_json_failover("v1/search", request, FailoverClass::AnyEndpoint)
            .await
    }

    pub async fn apply_mutations(
        &self,
        request: &ApplyMutationsRequest,
    ) -> Result<ApplyMutationsResponse, ClusterClientError> {
        self.post_json_failover("v1/mutations", request, FailoverClass::LeaderWrite)
            .await
    }

    pub async fn upsert(
        &self,
        id: u64,
        values: Vec<f32>,
        commit_mode: CommitMode,
    ) -> Result<ApplyMutationsResponse, ClusterClientError> {
        self.apply_mutations(&ApplyMutationsRequest {
            mutations: vec![Mutation::Upsert { id, values }],
            commit_mode: Some(commit_mode),
            allow_partial: false,
        })
        .await
    }

    pub async fn delete(
        &self,
        id: u64,
        commit_mode: CommitMode,
    ) -> Result<ApplyMutationsResponse, ClusterClientError> {
        self.apply_mutations(&ApplyMutationsRequest {
            mutations: vec![Mutation::Delete { id }],
            commit_mode: Some(commit_mode),
            allow_partial: false,
        })
        .await
    }

    async fn get_json_failover<T>(
        &self,
        path: &str,
        failover: FailoverClass,
    ) -> Result<T, ClusterClientError>
    where
        T: DeserializeOwned,
    {
        let mut last_error = None;
        for round in 0..failover.max_rounds() {
            let mut attempted_cached = None;
            if let Some(cached) = self.cached_discovered_endpoint() {
                attempted_cached = Some(cached.clone());
                match self.get_json_url(&cached, path).await {
                    Ok(decoded) => {
                        self.maybe_mark_preferred_url(&cached);
                        return Ok(decoded);
                    }
                    Err(err) => {
                        if should_retry(&err, failover) {
                            last_error = Some(err);
                            self.clear_cached_discovered_endpoint_if(&cached);
                        } else {
                            return Err(err);
                        }
                    }
                }
            }

            for idx in self.ordered_endpoint_indices() {
                if attempted_cached
                    .as_ref()
                    .map(|cached| self.endpoint_matches_url(idx, cached))
                    .unwrap_or(false)
                {
                    continue;
                }
                match self.get_json_at(idx, path).await {
                    Ok(decoded) => {
                        self.set_preferred(idx);
                        return Ok(decoded);
                    }
                    Err(err) => {
                        if should_retry(&err, failover) {
                            last_error = Some(err);
                            continue;
                        }
                        return Err(err);
                    }
                }
            }
            let discovered = match self.resolve_discovery_leader_endpoint().await {
                Ok(discovered) => discovered,
                Err(err) => {
                    last_error = Some(err);
                    None
                }
            };
            if let Some(url) = discovered {
                match self.get_json_url(&url, path).await {
                    Ok(decoded) => {
                        self.set_cached_discovered_endpoint(url.clone());
                        self.maybe_mark_preferred_url(&url);
                        return Ok(decoded);
                    }
                    Err(err) => {
                        if should_retry(&err, failover) {
                            last_error = Some(err);
                            self.clear_cached_discovered_endpoint_if(&url);
                        } else {
                            return Err(err);
                        }
                    }
                }
            }
            if round + 1 < failover.max_rounds() {
                tokio::time::sleep(Duration::from_millis(failover.backoff_ms())).await;
            }
        }
        Err(last_error.unwrap_or_else(|| match failover {
            FailoverClass::AnyEndpoint => ClusterClientError::NoReachableEndpoints {
                checked: self.inner.endpoints.len(),
            },
            FailoverClass::LeaderWrite => ClusterClientError::LeaderNotFound {
                checked: self.inner.endpoints.len(),
            },
        }))
    }

    async fn post_json_failover<Req, Resp>(
        &self,
        path: &str,
        request: &Req,
        failover: FailoverClass,
    ) -> Result<Resp, ClusterClientError>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        let body = encode_json_body(request)?;
        let mut last_error = None;
        for round in 0..failover.max_rounds() {
            let mut attempted_cached = None;
            if let Some(cached) = self.cached_discovered_endpoint() {
                attempted_cached = Some(cached.clone());
                match self.post_json_url(&cached, path, body.clone()).await {
                    Ok(decoded) => {
                        self.maybe_mark_preferred_url(&cached);
                        return Ok(decoded);
                    }
                    Err(err) => {
                        if should_retry(&err, failover) {
                            last_error = Some(err);
                            self.clear_cached_discovered_endpoint_if(&cached);
                        } else {
                            return Err(err);
                        }
                    }
                }
            }

            for idx in self.ordered_endpoint_indices() {
                if attempted_cached
                    .as_ref()
                    .map(|cached| self.endpoint_matches_url(idx, cached))
                    .unwrap_or(false)
                {
                    continue;
                }
                match self.post_json_at(idx, path, body.clone()).await {
                    Ok(decoded) => {
                        self.set_preferred(idx);
                        return Ok(decoded);
                    }
                    Err(err) => {
                        if should_retry(&err, failover) {
                            last_error = Some(err);
                            continue;
                        }
                        return Err(err);
                    }
                }
            }
            let discovered = match self.resolve_discovery_leader_endpoint().await {
                Ok(discovered) => discovered,
                Err(err) => {
                    last_error = Some(err);
                    None
                }
            };
            if let Some(url) = discovered {
                match self.post_json_url(&url, path, body.clone()).await {
                    Ok(decoded) => {
                        self.set_cached_discovered_endpoint(url.clone());
                        self.maybe_mark_preferred_url(&url);
                        return Ok(decoded);
                    }
                    Err(err) => {
                        if should_retry(&err, failover) {
                            last_error = Some(err);
                            self.clear_cached_discovered_endpoint_if(&url);
                        } else {
                            return Err(err);
                        }
                    }
                }
            }
            if round + 1 < failover.max_rounds() {
                tokio::time::sleep(Duration::from_millis(failover.backoff_ms())).await;
            }
        }
        Err(last_error.unwrap_or_else(|| match failover {
            FailoverClass::AnyEndpoint => ClusterClientError::NoReachableEndpoints {
                checked: self.inner.endpoints.len(),
            },
            FailoverClass::LeaderWrite => ClusterClientError::LeaderNotFound {
                checked: self.inner.endpoints.len(),
            },
        }))
    }

    async fn get_json_at<T>(&self, endpoint_idx: usize, path: &str) -> Result<T, ClusterClientError>
    where
        T: DeserializeOwned,
    {
        let endpoint = self.endpoint_url_at(endpoint_idx, path)?;
        let response = self
            .inner
            .http
            .get(endpoint.clone())
            .send()
            .await
            .map_err(ClusterClientError::Transport)?;
        decode_json_response(endpoint.as_str(), response).await
    }

    async fn post_json_at<Resp>(
        &self,
        endpoint_idx: usize,
        path: &str,
        body: Bytes,
    ) -> Result<Resp, ClusterClientError>
    where
        Resp: DeserializeOwned,
    {
        let endpoint = self.endpoint_url_at(endpoint_idx, path)?;
        let response = self
            .inner
            .http
            .post(endpoint.clone())
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(ClusterClientError::Transport)?;
        decode_json_response(endpoint.as_str(), response).await
    }

    async fn get_json_url<T>(&self, base: &Url, path: &str) -> Result<T, ClusterClientError>
    where
        T: DeserializeOwned,
    {
        let endpoint = endpoint_url_from_base(base, path)?;
        let response = self
            .inner
            .http
            .get(endpoint.clone())
            .send()
            .await
            .map_err(ClusterClientError::Transport)?;
        decode_json_response(endpoint.as_str(), response).await
    }

    async fn post_json_url<Resp>(
        &self,
        base: &Url,
        path: &str,
        body: Bytes,
    ) -> Result<Resp, ClusterClientError>
    where
        Resp: DeserializeOwned,
    {
        let endpoint = endpoint_url_from_base(base, path)?;
        let response = self
            .inner
            .http
            .post(endpoint.clone())
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(ClusterClientError::Transport)?;
        decode_json_response(endpoint.as_str(), response).await
    }

    fn preferred_index(&self) -> usize {
        let len = self.inner.endpoints.len();
        let preferred = self.inner.preferred.load(Ordering::Relaxed);
        if preferred < len {
            preferred
        } else {
            0
        }
    }

    fn set_preferred(&self, idx: usize) {
        let len = self.inner.endpoints.len().max(1);
        self.inner.preferred.store(idx % len, Ordering::Relaxed);
    }

    fn ordered_endpoint_indices(&self) -> impl Iterator<Item = usize> + '_ {
        let len = self.inner.endpoints.len();
        let preferred = self.preferred_index();
        (0..len).map(move |offset| (preferred + offset) % len)
    }

    fn endpoint_url_at(&self, endpoint_idx: usize, path: &str) -> Result<Url, ClusterClientError> {
        let base = self.inner.endpoints.get(endpoint_idx).ok_or_else(|| {
            ClusterClientError::InvalidBaseUrl(format!("unknown endpoint index {endpoint_idx}"))
        })?;
        endpoint_url_from_base(base, path)
    }

    fn endpoint_matches_url(&self, endpoint_idx: usize, url: &Url) -> bool {
        self.inner
            .endpoints
            .get(endpoint_idx)
            .map(|candidate| candidate == url)
            .unwrap_or(false)
    }

    fn maybe_mark_preferred_url(&self, url: &Url) {
        if let Some((idx, _)) = self
            .inner
            .endpoints
            .iter()
            .enumerate()
            .find(|(_, candidate)| *candidate == url)
        {
            self.set_preferred(idx);
        }
    }

    async fn resolve_discovery_leader_endpoint(&self) -> Result<Option<Url>, ClusterClientError> {
        let Some(discovery) = self.inner.etcd_discovery.as_ref() else {
            return Ok(None);
        };
        let resolved = resolve_leader_endpoint_from_etcd(discovery).await?;
        Ok(Some(resolved))
    }

    fn cached_discovered_endpoint(&self) -> Option<Url> {
        match self.inner.discovered_leader.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn set_cached_discovered_endpoint(&self, url: Url) {
        match self.inner.discovered_leader.write() {
            Ok(mut guard) => *guard = Some(url),
            Err(poisoned) => {
                let mut inner = poisoned.into_inner();
                *inner = Some(url);
            }
        }
    }

    fn clear_cached_discovered_endpoint_if(&self, url: &Url) {
        match self.inner.discovered_leader.write() {
            Ok(mut cached) => {
                if cached.as_ref() == Some(url) {
                    *cached = None;
                }
            }
            Err(poisoned) => {
                let mut cached = poisoned.into_inner();
                if cached.as_ref() == Some(url) {
                    *cached = None;
                }
            }
        }
    }
}

fn parse_endpoint(base_url: &str) -> Result<Url, ClusterClientError> {
    let mut parsed = Url::parse(base_url)
        .map_err(|err| ClusterClientError::InvalidBaseUrl(format!("{base_url}: {err}")))?;
    match parsed.scheme() {
        "http" | "https" => {}
        other => {
            return Err(ClusterClientError::InvalidBaseUrl(format!(
                "{base_url}: unsupported scheme {other}"
            )));
        }
    }
    if parsed.path().is_empty() {
        parsed.set_path("/");
    }
    parsed.set_query(None);
    parsed.set_fragment(None);
    Ok(parsed)
}

fn parse_http_endpoints<I, S>(endpoints: I) -> Result<Vec<Url>, ClusterClientError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut parsed = Vec::new();
    for endpoint in endpoints {
        let parsed_endpoint = parse_endpoint(endpoint.as_ref())?;
        if !parsed.iter().any(|url| url == &parsed_endpoint) {
            parsed.push(parsed_endpoint);
        }
    }
    if parsed.is_empty() {
        return Err(ClusterClientError::InvalidBaseUrl(
            "no endpoints configured".to_string(),
        ));
    }
    Ok(parsed)
}

fn endpoint_url_from_base(base: &Url, path: &str) -> Result<Url, ClusterClientError> {
    let suffix = path.trim_start_matches('/');
    let base_path = base.path().trim_end_matches('/');
    let joined = if base_path.is_empty() || base_path == "/" {
        format!("/{suffix}")
    } else {
        format!("{base_path}/{suffix}")
    };
    let mut url = base.clone();
    url.set_path(joined.as_str());
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn build_default_http_client() -> Result<Client, ClusterClientError> {
    Client::builder()
        .pool_max_idle_per_host(HTTP_POOL_MAX_IDLE_PER_HOST)
        .pool_idle_timeout(Duration::from_secs(HTTP_POOL_IDLE_TIMEOUT_SECS))
        .connect_timeout(Duration::from_millis(HTTP_CONNECT_TIMEOUT_MS))
        .tcp_nodelay(true)
        .build()
        .map_err(ClusterClientError::Transport)
}

fn encode_json_body<Req>(request: &Req) -> Result<Bytes, ClusterClientError>
where
    Req: Serialize + ?Sized,
{
    serde_json::to_vec(request)
        .map(Bytes::from)
        .map_err(ClusterClientError::Encode)
}

impl EtcdDiscoveryConfig {
    fn new<I, S>(etcd_endpoints: I, election_key: &str) -> Result<Self, ClusterClientError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let endpoints: Vec<String> = etcd_endpoints
            .into_iter()
            .map(|value| value.as_ref().trim().to_string())
            .filter(|value| !value.is_empty())
            .collect();
        if endpoints.is_empty() {
            return Err(ClusterClientError::InvalidBaseUrl(
                "no etcd endpoints configured".to_string(),
            ));
        }
        let trimmed_key = election_key.trim();
        if trimmed_key.is_empty() {
            return Err(ClusterClientError::InvalidBaseUrl(
                "election key must not be empty".to_string(),
            ));
        }
        Ok(Self {
            endpoints,
            election_key: trimmed_key.to_string(),
            leader_endpoint_key: etcd_leader_endpoint_key(trimmed_key),
            connect_timeout: Duration::from_millis(ETCD_CONNECT_TIMEOUT_MS),
            request_timeout: Duration::from_millis(ETCD_REQUEST_TIMEOUT_MS),
        })
    }
}

async fn resolve_leader_endpoint_from_etcd(
    discovery: &EtcdDiscoveryConfig,
) -> Result<Url, ClusterClientError> {
    let options = ConnectOptions::new()
        .with_require_leader(true)
        .with_connect_timeout(discovery.connect_timeout)
        .with_timeout(discovery.request_timeout)
        .with_keep_alive(Duration::from_secs(2), Duration::from_secs(2))
        .with_keep_alive_while_idle(true);
    let mut client = EtcdClient::connect(discovery.endpoints.clone(), Some(options))
        .await
        .map_err(|err| {
            ClusterClientError::Etcd(format!(
                "connect to etcd endpoints {}: {err:#}",
                discovery.endpoints.join(",")
            ))
        })?;

    for attempt in 0..ETCD_DISCOVERY_MAX_ATTEMPTS {
        let leader_response = client
            .get(discovery.election_key.as_bytes(), None)
            .await
            .map_err(|err| {
                ClusterClientError::Etcd(format!(
                    "read election key {}: {err:#}",
                    discovery.election_key
                ))
            })?;
        let Some(leader_kv) = leader_response.kvs().first() else {
            return Err(ClusterClientError::LeaderNotFoundEtcd {
                election_key: discovery.election_key.clone(),
            });
        };
        let leader_value = std::str::from_utf8(leader_kv.value())
            .map_err(|err| {
                ClusterClientError::Etcd(format!(
                    "leader key {} is not valid UTF-8: {err}",
                    discovery.election_key
                ))
            })?
            .trim()
            .to_string();
        let leader_lease = leader_kv.lease();

        let endpoint_response = client
            .get(discovery.leader_endpoint_key.as_bytes(), None)
            .await
            .map_err(|err| {
                ClusterClientError::Etcd(format!(
                    "read leader endpoint key {}: {err:#}",
                    discovery.leader_endpoint_key
                ))
            })?;
        if let Some(endpoint_kv) = endpoint_response.kvs().first() {
            let endpoint_value = std::str::from_utf8(endpoint_kv.value())
                .map_err(|err| {
                    ClusterClientError::Etcd(format!(
                        "leader endpoint key {} is not valid UTF-8: {err}",
                        discovery.leader_endpoint_key
                    ))
                })?
                .trim()
                .to_string();
            if endpoint_kv.lease() == leader_lease && !endpoint_value.is_empty() {
                return parse_endpoint(endpoint_value.as_str());
            }
        }

        if attempt + 1 < ETCD_DISCOVERY_MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(ETCD_DISCOVERY_RETRY_MS)).await;
            continue;
        }

        return Err(ClusterClientError::LeaderEndpointMissing {
            election_key: discovery.election_key.clone(),
            leader_value,
            endpoint_key: discovery.leader_endpoint_key.clone(),
            reason: "endpoint key missing/empty or lease mismatch with election key; election key value is not used as endpoint".to_string(),
        });
    }

    Err(ClusterClientError::LeaderEndpointMissing {
        election_key: discovery.election_key.clone(),
        leader_value: "<unresolved>".to_string(),
        endpoint_key: discovery.leader_endpoint_key.clone(),
        reason: "exhausted discovery attempts".to_string(),
    })
}

#[derive(Clone, Copy, Debug)]
enum FailoverClass {
    AnyEndpoint,
    LeaderWrite,
}

impl FailoverClass {
    fn max_rounds(self) -> usize {
        match self {
            Self::AnyEndpoint => 1,
            Self::LeaderWrite => LEADER_FAILOVER_MAX_ROUNDS,
        }
    }

    fn backoff_ms(self) -> u64 {
        match self {
            Self::AnyEndpoint => 0,
            Self::LeaderWrite => LEADER_FAILOVER_BACKOFF_MS,
        }
    }
}

fn should_retry(error: &ClusterClientError, failover: FailoverClass) -> bool {
    match error {
        ClusterClientError::Transport(_) => true,
        ClusterClientError::Decode { .. } => true,
        ClusterClientError::Encode(_) => false,
        ClusterClientError::Http(ctx) => {
            if ctx.status.is_server_error()
                || ctx.status == StatusCode::REQUEST_TIMEOUT
                || ctx.status == StatusCode::TOO_MANY_REQUESTS
            {
                return true;
            }
            matches!(failover, FailoverClass::LeaderWrite)
                && ctx.status == StatusCode::CONFLICT
                && is_leader_rejection(ctx)
        }
        ClusterClientError::InvalidBaseUrl(_)
        | ClusterClientError::Etcd(_)
        | ClusterClientError::LeaderNotFoundEtcd { .. }
        | ClusterClientError::LeaderEndpointMissing { .. }
        | ClusterClientError::LeaderNotFound { .. }
        | ClusterClientError::NoReachableEndpoints { .. } => false,
    }
}

fn is_leader_rejection(ctx: &HttpErrorContext) -> bool {
    let message = ctx.message.to_ascii_lowercase();
    if message.contains("not leader") || message.contains("leader term unavailable") {
        return true;
    }
    ctx.error
        .as_ref()
        .map(|err| {
            let text = err.error.to_ascii_lowercase();
            text.contains("not leader") || text.contains("leader term unavailable")
        })
        .unwrap_or(false)
}

#[derive(Debug)]
pub enum ClusterClientError {
    InvalidBaseUrl(String),
    Transport(reqwest::Error),
    Encode(serde_json::Error),
    Http(Box<HttpErrorContext>),
    Decode {
        endpoint: String,
        source: serde_json::Error,
        body: String,
    },
    Etcd(String),
    LeaderNotFoundEtcd {
        election_key: String,
    },
    LeaderEndpointMissing {
        election_key: String,
        leader_value: String,
        endpoint_key: String,
        reason: String,
    },
    LeaderNotFound {
        checked: usize,
    },
    NoReachableEndpoints {
        checked: usize,
    },
}

#[derive(Debug)]
pub struct HttpErrorContext {
    pub endpoint: String,
    pub status: StatusCode,
    pub message: String,
    pub error: Option<ApiErrorResponse>,
    pub body: String,
}

impl fmt::Display for ClusterClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBaseUrl(message) => write!(f, "invalid cluster base URL: {message}"),
            Self::Transport(err) => write!(f, "cluster transport error: {err}"),
            Self::Encode(err) => write!(f, "cluster request encode failed: {err}"),
            Self::Http(ctx) => write!(
                f,
                "cluster request failed endpoint={} status={}: {}",
                ctx.endpoint, ctx.status, ctx.message
            ),
            Self::Decode {
                endpoint, source, ..
            } => write!(
                f,
                "cluster response decode failed endpoint={endpoint}: {source}"
            ),
            Self::Etcd(message) => write!(f, "cluster etcd discovery error: {message}"),
            Self::LeaderNotFoundEtcd { election_key } => write!(
                f,
                "cluster etcd leader key missing at {election_key}"
            ),
            Self::LeaderEndpointMissing {
                election_key,
                leader_value,
                endpoint_key,
                reason,
            } => write!(
                f,
                "cluster etcd leader endpoint missing: election_key={} leader_value={} endpoint_key={} reason={}",
                election_key, leader_value, endpoint_key, reason
            ),
            Self::LeaderNotFound { checked } => write!(
                f,
                "cluster leader not found after checking {checked} endpoints"
            ),
            Self::NoReachableEndpoints { checked } => write!(
                f,
                "no reachable cluster endpoints after checking {checked} endpoints"
            ),
        }
    }
}

impl Error for ClusterClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(err) => Some(err),
            Self::Encode(err) => Some(err),
            Self::Decode { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoleResponse {
    pub node_id: String,
    pub role: String,
    pub leader: bool,
    pub replication: ReplicationState,
    #[serde(default)]
    pub local_partition_replication: Vec<PartitionReplicationState>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub node_id: String,
    pub role: String,
    pub leader: bool,
    pub stats: SpFreshLayerDbShardedStats,
    pub replication: ReplicationState,
    #[serde(default)]
    pub local_partition_replication: Vec<PartitionReplicationState>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionReplicationState {
    pub partition_id: u32,
    pub leader: bool,
    pub first_seq: u64,
    pub next_seq: u64,
    pub last_applied_seq: u64,
    pub max_term_seen: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationState {
    pub first_seq: u64,
    pub next_seq: u64,
    pub last_applied_seq: u64,
    pub max_term_seen: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitMode {
    Durable,
    Acknowledged,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Mutation {
    Upsert { id: u64, values: Vec<f32> },
    Delete { id: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplyMutationsRequest {
    pub mutations: Vec<Mutation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_mode: Option<CommitMode>,
    #[serde(default, skip_serializing_if = "bool_is_false")]
    pub allow_partial: bool,
}

fn bool_is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplyMutationsResponse {
    pub applied_upserts: usize,
    pub applied_deletes: usize,
    pub replicated: ReplicationSummary,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationSummary {
    pub attempted: usize,
    pub succeeded: usize,
    pub failed: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchRequest {
    pub query: Vec<f32>,
    pub k: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResponse {
    pub neighbors: Vec<Neighbor>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiErrorResponse {
    pub error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication_reject: Option<ReplicationRejectResponse>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationRejectResponse {
    pub error: String,
    pub expected_seq: u64,
    pub first_available_seq: u64,
    pub last_available_seq: u64,
    pub max_term_seen: u64,
}

async fn decode_json_response<T>(
    endpoint: &str,
    response: reqwest::Response,
) -> Result<T, ClusterClientError>
where
    T: DeserializeOwned,
{
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(ClusterClientError::Transport)?;
    if status.is_success() {
        return serde_json::from_slice(bytes.as_ref()).map_err(|source| {
            let body = String::from_utf8_lossy(bytes.as_ref()).into_owned();
            ClusterClientError::Decode {
                endpoint: endpoint.to_string(),
                source,
                body,
            }
        });
    }

    let decoded_error = serde_json::from_slice::<ApiErrorResponse>(bytes.as_ref()).ok();
    let body = String::from_utf8_lossy(bytes.as_ref()).into_owned();
    let message = decoded_error
        .as_ref()
        .map(|value| value.error.clone())
        .unwrap_or_else(|| {
            if body.is_empty() {
                status
                    .canonical_reason()
                    .unwrap_or("unknown status")
                    .to_string()
            } else {
                body.clone()
            }
        });
    Err(ClusterClientError::Http(Box::new(HttpErrorContext {
        endpoint: endpoint.to_string(),
        status,
        message,
        error: decoded_error,
        body,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

    use axum::extract::State;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    #[derive(Clone)]
    struct TestState {
        role: RoleResponse,
        health: HealthResponse,
    }

    #[derive(Clone)]
    struct LeaderSwitchState {
        node_id: String,
        leader: Arc<AtomicBool>,
        mutation_hits: Arc<AtomicUsize>,
    }

    fn sample_replication_state() -> ReplicationState {
        ReplicationState {
            first_seq: 1,
            next_seq: 4,
            last_applied_seq: 3,
            max_term_seen: 11,
        }
    }

    async fn spawn_test_server(
        path_prefix: &str,
    ) -> anyhow::Result<(String, JoinHandle<anyhow::Result<()>>)> {
        let prefix = path_prefix.trim_matches('/');
        let prefixed = |path: &str| {
            if prefix.is_empty() {
                format!("/{path}")
            } else {
                format!("/{prefix}/{path}")
            }
        };

        let state = TestState {
            role: RoleResponse {
                node_id: "node-a".to_string(),
                role: "leader".to_string(),
                leader: true,
                replication: sample_replication_state(),
                local_partition_replication: Vec::new(),
            },
            health: HealthResponse {
                node_id: "node-a".to_string(),
                role: "leader".to_string(),
                leader: true,
                stats: SpFreshLayerDbShardedStats {
                    shard_count: 4,
                    total_rows: 9,
                    total_upserts: 3,
                    total_deletes: 1,
                    persist_errors: 0,
                    total_persist_upsert_us: 0,
                    total_persist_delete_us: 0,
                    rebuild_successes: 0,
                    rebuild_failures: 0,
                    total_rebuild_applied_ids: 0,
                    total_searches: 0,
                    total_search_latency_us: 0,
                    pending_ops: 0,
                },
                replication: sample_replication_state(),
                local_partition_replication: Vec::new(),
            },
        };

        let app = Router::new()
            .route(prefixed("v1/role").as_str(), get(test_role))
            .route(prefixed("v1/health").as_str(), get(test_health))
            .route(prefixed("v1/search").as_str(), post(test_search))
            .route(prefixed("v1/mutations").as_str(), post(test_mutations))
            .with_state(state);

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await?;
            Ok(())
        });
        Ok((format!("http://{addr}"), server))
    }

    async fn spawn_leader_switch_server(
        node_id: &str,
        leader: Arc<AtomicBool>,
    ) -> anyhow::Result<(String, JoinHandle<anyhow::Result<()>>)> {
        let (url, _hits, server) = spawn_leader_switch_server_with_hits(node_id, leader).await?;
        Ok((url, server))
    }

    async fn spawn_leader_switch_server_with_hits(
        node_id: &str,
        leader: Arc<AtomicBool>,
    ) -> anyhow::Result<(String, Arc<AtomicUsize>, JoinHandle<anyhow::Result<()>>)> {
        let mutation_hits = Arc::new(AtomicUsize::new(0));
        let state = LeaderSwitchState {
            node_id: node_id.to_string(),
            leader,
            mutation_hits: mutation_hits.clone(),
        };
        let app = Router::new()
            .route("/v1/role", get(leader_switch_role))
            .route("/v1/mutations", post(leader_switch_mutations))
            .with_state(state);

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await?;
            Ok(())
        });
        Ok((format!("http://{addr}"), mutation_hits, server))
    }

    async fn test_role(State(state): State<TestState>) -> Json<RoleResponse> {
        Json(state.role)
    }

    async fn test_health(State(state): State<TestState>) -> Json<HealthResponse> {
        Json(state.health)
    }

    async fn test_search(Json(_req): Json<SearchRequest>) -> Json<SearchResponse> {
        Json(SearchResponse {
            neighbors: vec![Neighbor {
                id: 42,
                distance: 0.125,
            }],
        })
    }

    async fn test_mutations(
        Json(req): Json<ApplyMutationsRequest>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        if req.mutations.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "mutations must not be empty"
                })),
            );
        }
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "applied_upserts": 1,
                "applied_deletes": 0,
                "replicated": {"attempted": 2, "succeeded": 2, "failed": []}
            })),
        )
    }

    async fn leader_switch_role(State(state): State<LeaderSwitchState>) -> Json<RoleResponse> {
        let leader = state.leader.load(AtomicOrdering::Relaxed);
        let role = if leader { "leader" } else { "follower" };
        Json(RoleResponse {
            node_id: state.node_id,
            role: role.to_string(),
            leader,
            replication: sample_replication_state(),
            local_partition_replication: Vec::new(),
        })
    }

    async fn leader_switch_mutations(
        State(state): State<LeaderSwitchState>,
        Json(req): Json<ApplyMutationsRequest>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        state.mutation_hits.fetch_add(1, AtomicOrdering::Relaxed);
        if req.mutations.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "mutations must not be empty"
                })),
            );
        }
        if !state.leader.load(AtomicOrdering::Relaxed) {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "write rejected: this node is not leader"
                })),
            );
        }
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "applied_upserts": 1,
                "applied_deletes": 0,
                "replicated": {"attempted": 0, "succeeded": 0, "failed": []}
            })),
        )
    }

    fn normalize_base(url: &str) -> String {
        let mut parsed = Url::parse(url).expect("parse test url");
        if parsed.path().is_empty() {
            parsed.set_path("/");
        }
        parsed.to_string()
    }

    #[tokio::test]
    async fn role_health_and_search_round_trip() -> anyhow::Result<()> {
        let (base_url, _server) = spawn_test_server("").await?;
        let client = VectDbClusterClient::new(base_url.as_str())?;

        let role = client.role().await?;
        assert_eq!(role.node_id, "node-a");
        assert_eq!(role.role, "leader");
        assert!(role.leader);

        let health = client.health().await?;
        assert_eq!(health.stats.shard_count, 4);
        assert_eq!(health.replication.next_seq, 4);

        let neighbors = client.search(vec![0.1, 0.2], 1).await?;
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].id, 42);
        Ok(())
    }

    #[tokio::test]
    async fn base_url_with_path_prefix_is_supported() -> anyhow::Result<()> {
        let (base_url, _server) = spawn_test_server("cluster-a").await?;
        let client = VectDbClusterClient::new(format!("{base_url}/cluster-a").as_str())?;
        let role = client.role().await?;
        assert_eq!(role.node_id, "node-a");
        Ok(())
    }

    #[tokio::test]
    async fn apply_mutations_surfaces_http_error_payload() -> anyhow::Result<()> {
        let (base_url, _server) = spawn_test_server("").await?;
        let client = VectDbClusterClient::new(base_url.as_str())?;
        let err = client
            .apply_mutations(&ApplyMutationsRequest {
                mutations: Vec::new(),
                commit_mode: Some(CommitMode::Durable),
                allow_partial: false,
            })
            .await
            .expect_err("expected bad request");
        match err {
            ClusterClientError::Http(ctx) => {
                assert_eq!(ctx.status, StatusCode::BAD_REQUEST);
                assert_eq!(ctx.message, "mutations must not be empty");
                assert_eq!(
                    ctx.error.as_ref().map(|value| value.error.as_str()),
                    Some("mutations must not be empty")
                );
            }
            other => anyhow::bail!("unexpected error kind: {other}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn writes_fail_over_from_follower_to_leader() -> anyhow::Result<()> {
        let follower_flag = Arc::new(AtomicBool::new(false));
        let leader_flag = Arc::new(AtomicBool::new(true));
        let (follower_url, _follower_server) =
            spawn_leader_switch_server("node-follower", follower_flag).await?;
        let (leader_url, _leader_server) =
            spawn_leader_switch_server("node-leader", leader_flag).await?;

        let client =
            VectDbClusterClient::from_endpoints([follower_url.as_str(), leader_url.as_str()])?;

        let response = client
            .upsert(7, vec![0.1, 0.2], CommitMode::Durable)
            .await?;
        assert_eq!(response.applied_upserts, 1);
        assert_eq!(
            client.base_url().as_str(),
            normalize_base(leader_url.as_str())
        );
        Ok(())
    }

    #[tokio::test]
    async fn writes_recover_after_leader_switch() -> anyhow::Result<()> {
        let node1_leader = Arc::new(AtomicBool::new(true));
        let node2_leader = Arc::new(AtomicBool::new(false));
        let (node1_url, _node1_server) =
            spawn_leader_switch_server("node-1", node1_leader.clone()).await?;
        let (node2_url, _node2_server) =
            spawn_leader_switch_server("node-2", node2_leader.clone()).await?;

        let client = VectDbClusterClient::from_endpoints([node1_url.as_str(), node2_url.as_str()])?;

        let first = client
            .upsert(1, vec![0.1, 0.2], CommitMode::Durable)
            .await?;
        assert_eq!(first.applied_upserts, 1);
        assert_eq!(
            client.base_url().as_str(),
            normalize_base(node1_url.as_str())
        );

        node1_leader.store(false, AtomicOrdering::Relaxed);
        node2_leader.store(true, AtomicOrdering::Relaxed);

        let second = client
            .upsert(2, vec![0.2, 0.3], CommitMode::Durable)
            .await?;
        assert_eq!(second.applied_upserts, 1);
        assert_eq!(
            client.base_url().as_str(),
            normalize_base(node2_url.as_str())
        );
        Ok(())
    }

    #[tokio::test]
    async fn cached_discovered_leader_avoids_stale_static_endpoint() -> anyhow::Result<()> {
        let stale_flag = Arc::new(AtomicBool::new(false));
        let leader_flag = Arc::new(AtomicBool::new(true));
        let (stale_url, stale_hits, _stale_server) =
            spawn_leader_switch_server_with_hits("node-stale", stale_flag).await?;
        let (leader_url, leader_hits, _leader_server) =
            spawn_leader_switch_server_with_hits("node-leader", leader_flag).await?;

        let client = VectDbClusterClient::from_endpoints([stale_url.as_str()])?;
        let discovered = parse_endpoint(leader_url.as_str())?;
        client.set_cached_discovered_endpoint(discovered);

        let first = client
            .upsert(41, vec![0.1, 0.2], CommitMode::Durable)
            .await?;
        assert_eq!(first.applied_upserts, 1);
        let second = client
            .upsert(42, vec![0.2, 0.3], CommitMode::Durable)
            .await?;
        assert_eq!(second.applied_upserts, 1);
        assert_eq!(
            client.base_url().as_str(),
            normalize_base(leader_url.as_str())
        );

        assert_eq!(
            stale_hits.load(AtomicOrdering::Relaxed),
            0,
            "stale static endpoint should not be used while cached leader is healthy"
        );
        assert_eq!(leader_hits.load(AtomicOrdering::Relaxed), 2);
        Ok(())
    }

    #[tokio::test]
    async fn transport_error_falls_back_to_reachable_node() -> anyhow::Result<()> {
        let down_listener = TcpListener::bind("127.0.0.1:0").await?;
        let down_url = format!("http://{}", down_listener.local_addr()?);
        drop(down_listener);

        let (live_url, _server) = spawn_test_server("").await?;
        let client = VectDbClusterClient::from_endpoints([down_url.as_str(), live_url.as_str()])?;

        let role = client.role().await?;
        assert_eq!(role.node_id, "node-a");
        Ok(())
    }

    #[test]
    fn invalid_base_url_is_rejected() {
        let err =
            VectDbClusterClient::new("localhost:8080").expect_err("url without scheme must fail");
        assert!(matches!(err, ClusterClientError::InvalidBaseUrl(_)));
    }

    #[test]
    fn endpoint_joining_preserves_prefix() -> anyhow::Result<()> {
        let client = VectDbClusterClient::new("http://127.0.0.1:8080/prefix")?;
        let endpoint = client.endpoint_url_at(0, "/v1/role")?;
        assert_eq!(endpoint.as_str(), "http://127.0.0.1:8080/prefix/v1/role");
        Ok(())
    }

    #[test]
    fn etcd_config_derives_endpoint_key() -> anyhow::Result<()> {
        let config = EtcdDiscoveryConfig::new(["http://127.0.0.1:2379"], "/vectdb/prod/leader")?;
        assert_eq!(config.election_key, "/vectdb/prod/leader");
        assert_eq!(config.leader_endpoint_key, "/vectdb/prod/leader_endpoint");
        Ok(())
    }

    #[test]
    fn etcd_config_rejects_empty_inputs() {
        let err =
            EtcdDiscoveryConfig::new::<Vec<String>, String>(Vec::new(), "/vectdb/prod/leader")
                .expect_err("missing endpoints must fail");
        assert!(matches!(err, ClusterClientError::InvalidBaseUrl(_)));

        let err = EtcdDiscoveryConfig::new(["http://127.0.0.1:2379"], "   ")
            .expect_err("empty election key must fail");
        assert!(matches!(err, ClusterClientError::InvalidBaseUrl(_)));
    }
}
