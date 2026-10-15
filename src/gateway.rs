//! MCP-facing service and controls. Reuse never stores authorization decisions.
use crate::{
    config::{self, Config, Server, User},
    mcp::digest,
    upstream::{Catalog, Client, Connector, MAX_MESSAGE},
};
use anyhow::{ensure, Context, Result};
use axum::http::HeaderMap;
use rand::Rng;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use wayframe_cache::{Cache, Evidence, Key, Scope};
pub(crate) mod state;
use state::{Lineage, RemoteState};

mod http;
mod network;
mod stdio;
pub use stdio::serve_stdio;

pub(crate) struct Actor {
    pub settings: User,
    token_hash: String,
    pub enabled: AtomicBool,
    bucket: Mutex<Bucket>,
}
struct Bucket {
    tokens: f64,
    updated: Instant,
}
impl Actor {
    fn consume(&self) -> bool {
        let mut bucket = self.bucket.lock().expect("rate bucket poisoned");
        let now = Instant::now();
        bucket.tokens = (bucket.tokens
            + now.duration_since(bucket.updated).as_secs_f64()
                * self.settings.requests_per_second as f64)
            .min(self.settings.burst as f64);
        bucket.updated = now;
        if bucket.tokens < 1.0 {
            return false;
        }
        bucket.tokens -= 1.0;
        true
    }
    pub(crate) fn permits(&self, tool: &str) -> bool {
        self.enabled.load(Ordering::Acquire) && config::permits(&self.settings.tools, tool)
    }
}

pub(crate) struct Session {
    pub id: String,
    pub actor: Arc<Actor>,
    pub initialized: AtomicBool,
    pub version: String,
    touched: Mutex<Instant>,
    clients: BTreeMap<String, Arc<Client>>,
    jobs: Mutex<HashMap<String, CancellationToken>>,
    pub stopped: CancellationToken,
}
impl Session {
    pub(crate) fn client(&self, server: &str) -> Option<Arc<Client>> {
        self.clients.get(server).cloned()
    }
    fn cancel(&self) {
        self.stopped.cancel();
        for token in self.jobs.lock().expect("session jobs poisoned").values() {
            token.cancel();
        }
    }
}

#[derive(Default)]
struct Metrics {
    requests: AtomicU64,
    upstream_calls: Arc<AtomicU64>,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_bypasses: AtomicU64,
    state_checks: AtomicU64,
    state_conflicts: AtomicU64,
    denied: AtomicU64,
    prewarms: AtomicU64,
    prewarm_errors: AtomicU64,
    rejected: AtomicU64,
    expired: AtomicU64,
    connections: Arc<AtomicU64>,
}

#[derive(Default)]
struct Profile {
    runs: u64,
    servers: BTreeMap<String, u64>,
}

pub struct Gateway {
    pub(crate) config: Config,
    actors: Vec<Arc<Actor>>,
    admin_hash: String,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    pub(crate) epoch: AtomicU64,
    cache: Cache<Value>,
    admission: Arc<Semaphore>,
    warming: Arc<Semaphore>,
    profiles: Mutex<HashMap<String, Profile>>,
    metrics: Metrics,
    pub(crate) workflows: BTreeMap<String, crate::workflow::Compiled>,
    direct: crate::workflow::Single,
    connectors: BTreeMap<String, Arc<Connector>>,
    server_admission: BTreeMap<String, Arc<Semaphore>>,
    upstream_admission: Arc<Semaphore>,
    preparing: Arc<Semaphore>,
    body_readers: Arc<Semaphore>,
    body_bytes: Arc<Semaphore>,
    admin_admission: Arc<Semaphore>,
    cleanup: Arc<crate::upstream_cleanup::Cleanup>,
    policy: Arc<tokio::sync::Mutex<crate::policy::Policy>>,
    draining: AtomicBool,
    network_stop: CancellationToken,
}

#[derive(Debug)]
pub(crate) struct Fault {
    pub code: i64,
    pub message: &'static str,
}
impl Fault {
    pub(crate) fn denied() -> Self {
        Self {
            code: -32003,
            message: "tool access denied or permissions changed",
        }
    }
    pub(crate) fn invalid() -> Self {
        Self {
            code: -32602,
            message: "invalid method parameters",
        }
    }
}

pub(crate) struct Outcome {
    pub result: Value,
    pub disposition: &'static str,
    pub elapsed_us: u128,
    pub lineage: Lineage,
}

impl Gateway {
    pub fn new(config: Config) -> Result<Arc<Self>> {
        config.validate()?;
        let admin_hash = secret(&config.admin_token_env)?;
        let mut hashes = BTreeSet::from([admin_hash.clone()]);
        let policy = crate::policy::Policy::load(
            config.state_path.clone(),
            &config
                .users
                .iter()
                .map(|user| user.id.clone())
                .collect::<Vec<_>>(),
        )?;
        let mut actors = Vec::new();
        for user in &config.users {
            let token_hash = secret(&user.token_env)?;
            ensure!(
                hashes.insert(token_hash.clone()),
                "user and administrator credentials must be distinct"
            );
            actors.push(Arc::new(Actor {
                settings: user.clone(),
                token_hash,
                enabled: AtomicBool::new(policy.enabled(&user.id, user.enabled)),
                bucket: Mutex::new(Bucket {
                    tokens: user.burst as f64,
                    updated: Instant::now(),
                }),
            }));
        }
        let mut workflows = BTreeMap::new();
        for definition in &config.workflows {
            workflows.insert(
                format!("waypilot__{}", definition.name),
                crate::workflow::Compiled::new(definition.clone(), config.max_in_flight)?,
            );
        }
        let cleanup = crate::upstream_cleanup::Cleanup::new(config.limits.cleanup_pending);
        let preparing = Arc::new(Semaphore::new(config.limits.upstream_initializations));
        let mut connectors = BTreeMap::new();
        let mut server_admission = BTreeMap::new();
        for server in &config.servers {
            connectors.insert(
                server.id.clone(),
                Connector::new(
                    server.endpoint.clone(),
                    Some(cleanup.clone()),
                    config.limits.upstream_idle_per_host,
                    preparing.clone(),
                )?,
            );
            server_admission.insert(
                server.id.clone(),
                Arc::new(Semaphore::new(server.max_in_flight)),
            );
        }
        Ok(Arc::new(Self {
            direct: crate::workflow::Single::new(config.max_in_flight)?,
            body_readers: Arc::new(Semaphore::new(config.limits.body_readers)),
            body_bytes: Arc::new(Semaphore::new(config.limits.buffered_bytes)),
            admin_admission: Arc::new(Semaphore::new(config.limits.admin_requests)),
            upstream_admission: Arc::new(Semaphore::new(config.limits.upstream_calls)),
            cleanup,
            preparing,
            connectors,
            server_admission,
            policy: Arc::new(tokio::sync::Mutex::new(policy)),
            draining: AtomicBool::new(false),
            network_stop: CancellationToken::new(),
            admission: Arc::new(Semaphore::new(config.max_in_flight)),
            warming: Arc::new(Semaphore::new(2)),
            config,
            actors,
            admin_hash,
            sessions: Mutex::new(HashMap::new()),
            epoch: AtomicU64::new(1),
            cache: Cache::new(512, 16 * 1024 * 1024, 128)?,
            profiles: Mutex::new(HashMap::new()),
            metrics: Metrics::default(),
            workflows,
        }))
    }

    fn authenticate(&self, headers: &HeaderMap) -> Option<Arc<Actor>> {
        let incoming = bearer(headers)?;
        let mut found = None;
        for actor in &self.actors {
            if bool::from(actor.token_hash.as_bytes().ct_eq(incoming.as_bytes()))
                && actor.enabled.load(Ordering::Acquire)
            {
                found = Some(actor.clone());
            }
        }
        found
    }
    fn admin(&self, headers: &HeaderMap) -> bool {
        bearer(headers)
            .is_some_and(|value| bool::from(self.admin_hash.as_bytes().ct_eq(value.as_bytes())))
    }
    fn origin(&self, headers: &HeaderMap) -> bool {
        headers.get("origin").is_none_or(|origin| {
            origin.to_str().ok().is_some_and(|origin| {
                self.config
                    .allowed_origins
                    .iter()
                    .any(|allowed| allowed == origin)
            })
        })
    }
    pub(crate) fn permits(&self, actor: &Actor, tool: &str) -> bool {
        actor.permits(tool)
            && tool
                .split_once("__")
                .and_then(|(server, name)| {
                    self.config
                        .servers
                        .iter()
                        .find(|item| item.id == server)
                        .and_then(|item| item.tools.get(name))
                        .and_then(|rule| rule.state_tool.as_ref())
                        .map(|validator| actor.permits(&format!("{server}__{validator}")))
                })
                .unwrap_or(true)
    }
    pub(crate) fn authorize(&self, session: &Session, tool: &str, epoch: u64) -> Result<(), Fault> {
        if session.stopped.is_cancelled()
            || self.epoch.load(Ordering::Acquire) != epoch
            || !self.permits(&session.actor, tool)
        {
            self.metrics.denied.fetch_add(1, Ordering::Relaxed);
            return Err(Fault::denied());
        }
        Ok(())
    }

    fn live(&self, session: &Session, epoch: u64) -> anyhow::Result<()> {
        ensure!(
            !session.stopped.is_cancelled()
                && session.actor.enabled.load(Ordering::Acquire)
                && self.epoch.load(Ordering::Acquire) == epoch,
            "session authorization changed"
        );
        Ok(())
    }

    fn create_session(&self, actor: Arc<Actor>, version: &str) -> Result<Arc<Session>> {
        ensure!(!self.draining.load(Ordering::Acquire), "gateway draining");
        let mut sessions = self.sessions.lock().expect("sessions poisoned");
        ensure!(
            sessions.len() < self.config.max_sessions,
            "session capacity reached"
        );
        let mut clients = BTreeMap::new();
        for server in &self.config.servers {
            if server_allowed(&actor, server) {
                clients.insert(
                    server.id.clone(),
                    Arc::new(Client::with_connector(
                        self.connectors[&server.id].clone(),
                        Some(self.metrics.upstream_calls.clone()),
                    )?),
                );
            }
        }
        let session = Arc::new(Session {
            id: format!(
                "{:032x}{:032x}",
                rand::rng().random::<u128>(),
                rand::rng().random::<u128>()
            ),
            actor,
            initialized: AtomicBool::new(false),
            version: version.into(),
            touched: Mutex::new(Instant::now()),
            clients,
            jobs: Mutex::new(HashMap::new()),
            stopped: CancellationToken::new(),
        });
        sessions.insert(session.id.clone(), session.clone());
        Ok(session)
    }

    fn session(&self, actor: &Actor, id: &str) -> Option<Arc<Session>> {
        let session = self
            .sessions
            .lock()
            .expect("sessions poisoned")
            .get(id)
            .cloned()?;
        if session.actor.settings.id != actor.settings.id || session.stopped.is_cancelled() {
            return None;
        }
        let mut touched = session.touched.lock().expect("session clock poisoned");
        if touched.elapsed() >= Duration::from_secs(self.config.session_ttl_seconds)
            && session
                .jobs
                .lock()
                .expect("session jobs poisoned")
                .is_empty()
        {
            session.cancel();
            return None;
        }
        *touched = Instant::now();
        drop(touched);
        Some(session)
    }

    pub(crate) async fn invoke(
        self: &Arc<Self>,
        session: Arc<Session>,
        name: &str,
        arguments: Value,
        deadline: tokio::time::Instant,
        epoch: u64,
        parents: &Lineage,
    ) -> Result<Outcome, Fault> {
        let started = Instant::now();
        self.authorize(&session, name, epoch)?;
        if !session.actor.consume() {
            return Err(Fault {
                code: -32029,
                message: "tool rate limit exceeded",
            });
        }
        let (server_id, tool) = name.split_once("__").ok_or_else(Fault::invalid)?;
        let server = self
            .config
            .servers
            .iter()
            .find(|server| server.id == server_id)
            .ok_or_else(Fault::invalid)?;
        let client = session.client(server_id).ok_or_else(Fault::denied)?;
        let catalog = client
            .catalog_checked(deadline, || self.live(&session, epoch))
            .await
            .map_err(|error| {
                if let Err(denied) = self.authorize(&session, name, epoch) {
                    return denied;
                }
                tracing::warn!(
                    cause = crate::upstream::failure_kind(&error),
                    "upstream preparation failed"
                );
                Fault {
                    code: -32002,
                    message: "upstream initialization or catalog failed",
                }
            })?;
        if !catalog
            .tools
            .iter()
            .any(|definition| definition["name"] == tool)
        {
            return Err(Fault {
                code: -32602,
                message: "unknown upstream tool",
            });
        }
        self.authorize(&session, name, epoch)?;
        self.validate_states(&session, &parents.states, deadline, epoch)
            .await?;
        let rule = server.tools.get(tool);
        let reusable =
            parents.cacheable && rule.is_some_and(|rule| rule.reusable && rule.cache_ttl_ms > 0);
        let stateful = rule.and_then(|rule| rule.state_tool.as_ref()).is_some();
        let key = if reusable || stateful {
            Some(
                Key::new(
                    Scope {
                        tenant: &session.actor.settings.tenant,
                        subject: &session.actor.settings.id,
                        permissions: &session.actor.settings.tools,
                        generation: epoch,
                        route: &server.id,
                    },
                    "tools/call",
                    &format!("{}:{}:{}", server.revision, catalog.revision, tool),
                    &arguments,
                )
                .map_err(|_| Fault::invalid())?,
            )
        } else {
            None
        };
        let mut lineage = Lineage {
            cacheable: false,
            receipts: Vec::new(),
            states: parents.states.clone(),
        };
        let own_state = if let Some(validator) = rule.and_then(|rule| rule.state_tool.as_ref()) {
            let mut state = RemoteState {
                validator: format!("{server_id}__{validator}"),
                arguments: json!({"tool":tool,"arguments":arguments}),
                evidence: Evidence::default(),
                key: key.as_ref().expect("state key").clone(),
            };
            state.evidence = match self.probe_state(&session, &state, deadline, epoch).await {
                Ok(evidence) => evidence,
                Err(error) => {
                    let _ = self.cache.invalidate(&state.key);
                    self.metrics.state_conflicts.fetch_add(1, Ordering::Relaxed);
                    return Err(error);
                }
            };
            Some(state)
        } else {
            None
        };
        let evidence = own_state
            .as_ref()
            .map(|state| state.evidence.clone())
            .unwrap_or_default();
        let mut receipt = None;
        let (value, disposition) = if reusable {
            let key = key.expect("reusable key");
            let lookup = || {
                tracing::info_span!("waypilot.cache.lookup").in_scope(|| {
                    self.cache
                        .lookup(&key, &parents.receipts, &evidence)
                        .map_err(|_| Fault {
                            code: -32603,
                            message: "result cache unavailable",
                        })
                })
            };
            if let Some(hit) = lookup()? {
                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                receipt = Some(hit.dependency);
                ((*hit.value).clone(), "reused")
            } else {
                let _flight = self
                    .cache
                    .singleflight(&key, deadline)
                    .await
                    .map_err(|_| Fault {
                        code: -32008,
                        message: "result wait deadline or capacity exceeded",
                    })?;
                self.authorize(&session, name, epoch)?;
                // A follower's evidence must still be checked at the release barrier.
                if let Some(hit) = lookup()? {
                    self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                    receipt = Some(hit.dependency);
                    ((*hit.value).clone(), "joined")
                } else {
                    self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
                    let value = self
                        .call_upstream(&session, name, &client, arguments, deadline, epoch)
                        .await?;
                    if let Some(state) = &own_state {
                        self.check_observation(state, &value)?;
                    }
                    if value["isError"] != true {
                        let bytes = serde_json::to_vec(&value)
                            .map_err(|_| Fault::invalid())?
                            .len();
                        if let Ok(retained) = self.cache.retain(
                            key,
                            value.clone(),
                            bytes,
                            Duration::from_millis(rule.expect("reusable rule").cache_ttl_ms),
                            &parents.receipts,
                            evidence,
                        ) {
                            receipt = Some(retained.dependency);
                        }
                    }
                    (value, "executed")
                }
            }
        } else {
            self.metrics.cache_bypasses.fetch_add(1, Ordering::Relaxed);
            let value = self
                .call_upstream(&session, name, &client, arguments, deadline, epoch)
                .await?;
            if let Some(state) = &own_state {
                self.check_observation(state, &value)?;
            }
            (value, "bypassed")
        };
        if let Some(receipt) = receipt {
            lineage.cacheable = true;
            lineage.receipts.push(receipt);
        }
        if let Some(state) = own_state {
            lineage.add_state(Arc::new(state))?;
        }
        self.authorize(&session, name, epoch)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(Fault {
                code: -32008,
                message: "request deadline exceeded",
            });
        }
        self.observe(&session.actor, server_id);
        tracing::info!(
            tool = name,
            disposition,
            elapsed_us = started.elapsed().as_micros() as u64,
            "tool completed"
        );
        Ok(Outcome {
            result: value,
            disposition,
            elapsed_us: started.elapsed().as_micros(),
            lineage,
        })
    }

    async fn call_upstream(
        &self,
        session: &Session,
        exposed: &str,
        client: &Client,
        arguments: Value,
        deadline: tokio::time::Instant,
        epoch: u64,
    ) -> Result<Value, Fault> {
        let tool = exposed.split_once("__").ok_or_else(Fault::invalid)?.1;
        let server = exposed.split_once("__").ok_or_else(Fault::invalid)?.0;
        let _server_slot = tokio::time::timeout_at(
            deadline,
            self.server_admission[server].clone().acquire_owned(),
        )
        .await
        .map_err(|_| Fault {
            code: -32008,
            message: "upstream queue deadline exceeded",
        })?
        .map_err(|_| Fault::denied())?;
        let _global_slot =
            tokio::time::timeout_at(deadline, self.upstream_admission.clone().acquire_owned())
                .await
                .map_err(|_| Fault {
                    code: -32008,
                    message: "upstream queue deadline exceeded",
                })?
                .map_err(|_| Fault::denied())?;
        self.authorize(session, exposed, epoch)?;
        client
            .call_checked(tool, arguments, deadline, || {
                self.authorize(session, exposed, epoch)
                    .map_err(|_| anyhow::anyhow!("tool access changed"))
            })
            .await
            .map_err(|error| {
                tracing::warn!(
                    cause = crate::upstream::failure_kind(&error),
                    server,
                    tool,
                    "upstream invocation failed"
                );
                Fault {
                    code: -32002,
                    message: "upstream invocation failed; it was not replayed",
                }
            })
    }

    fn observe(&self, actor: &Actor, server: &str) {
        let mut profiles = self.profiles.lock().expect("profiles poisoned");
        let profile = profiles.entry(actor.settings.id.clone()).or_default();
        profile.runs = profile.runs.saturating_add(1);
        let count = profile.servers.entry(server.into()).or_default();
        *count = count.saturating_add(1);
    }

    fn predictions(&self, actor: &Actor) -> Vec<String> {
        let profiles = self.profiles.lock().expect("profiles poisoned");
        let Some(profile) = profiles.get(&actor.settings.id) else {
            return Vec::new();
        };
        if profile.runs < 2 {
            return Vec::new();
        }
        self.config
            .servers
            .iter()
            .filter(|server| {
                server.prewarm
                    && server_allowed(actor, server)
                    && profile.servers.get(&server.id).copied().unwrap_or(0)
                        >= profile.runs.div_ceil(2)
            })
            .take(2)
            .map(|server| server.id.clone())
            .collect()
    }

    async fn prewarm(
        &self,
        session: Arc<Session>,
        servers: Vec<String>,
        deadline: tokio::time::Instant,
    ) -> Result<usize, Fault> {
        let _permit = self
            .warming
            .clone()
            .try_acquire_owned()
            .map_err(|_| Fault {
                code: -32029,
                message: "prewarm capacity exceeded",
            })?;
        if !session.actor.consume() {
            return Err(Fault {
                code: -32029,
                message: "prewarm rate limit exceeded",
            });
        }
        let epoch = self.epoch.load(Ordering::Acquire);
        let mut work = Vec::new();
        for id in servers {
            self.config
                .servers
                .iter()
                .find(|server| {
                    server.id == id && server.prewarm && server_allowed(&session.actor, server)
                })
                .ok_or_else(Fault::denied)?;
            if session.stopped.is_cancelled()
                || self.epoch.load(Ordering::Acquire) != epoch
                || !session.actor.enabled.load(Ordering::Acquire)
            {
                return Err(Fault::denied());
            }
            let client = session.client(&id).ok_or_else(Fault::denied)?;
            let current = session.clone();
            work.push(async move {
                client
                    .catalog_checked(deadline, || self.live(&current, epoch))
                    .await
            });
        }
        let count = work.len();
        let results = tokio::select! {
            _ = session.stopped.cancelled() => return Err(Fault::denied()),
            results = futures_util::future::join_all(work) => results,
        };
        for result in results {
            if result.is_err() {
                self.metrics.prewarm_errors.fetch_add(1, Ordering::Relaxed);
                return Err(Fault {
                    code: -32002,
                    message: "prewarming failed",
                });
            }
            self.metrics.prewarms.fetch_add(1, Ordering::Relaxed);
        }
        if self.epoch.load(Ordering::Acquire) != epoch
            || !session.actor.enabled.load(Ordering::Acquire)
            || session.stopped.is_cancelled()
        {
            return Err(Fault::denied());
        }
        Ok(count)
    }

    pub(crate) async fn tool(
        self: &Arc<Self>,
        session: Arc<Session>,
        name: &str,
        arguments: Value,
        deadline: tokio::time::Instant,
    ) -> Result<Value, Fault> {
        let epoch = self.epoch.load(Ordering::Acquire);
        self.authorize(&session, name, epoch)?;
        let mut outcome = if let Some(workflow) = self.workflows.get(name) {
            if !session.actor.consume() {
                return Err(Fault {
                    code: -32029,
                    message: "tool rate limit exceeded",
                });
            }
            workflow
                .run(self.clone(), session.clone(), arguments, deadline, epoch)
                .await?
        } else {
            self.direct
                .run(
                    self.clone(),
                    session.clone(),
                    name.into(),
                    arguments,
                    deadline,
                    epoch,
                )
                .await?
        };
        self.authorize(&session, name, epoch)?;
        outcome.result["_meta"]["io.waypilot/runtime"] = json!({"disposition":outcome.disposition,"elapsed_us":outcome.elapsed_us,"generation":epoch});
        Ok(outcome.result)
    }

    async fn tools(
        &self,
        session: &Session,
        deadline: tokio::time::Instant,
    ) -> Result<Value, Fault> {
        let epoch = self.epoch.load(Ordering::Acquire);
        let mut tools = Vec::new();
        for (server, client) in &session.clients {
            let Catalog { tools: catalog, .. } = client
                .catalog_checked(deadline, || self.live(session, epoch))
                .await
                .map_err(|_| Fault {
                    code: -32002,
                    message: "upstream tool discovery failed",
                })?;
            for mut tool in catalog {
                let name = format!(
                    "{}__{}",
                    server,
                    tool["name"].as_str().ok_or_else(Fault::invalid)?
                );
                if name.len() > 128 {
                    return Err(Fault {
                        code: -32002,
                        message: "prefixed tool name exceeds supported limit",
                    });
                }
                if self.permits(&session.actor, &name) {
                    tool["name"] = json!(name);
                    tools.push(tool);
                }
            }
        }
        for (name, workflow) in &self.workflows {
            if self.permits(&session.actor, name) && workflow.allowed(self, &session.actor) {
                tools.push(json!({"name":name,"description":workflow.definition.description,"inputSchema":workflow.definition.input_schema}));
            }
        }
        if self.epoch.load(Ordering::Acquire) != epoch
            || !session.actor.enabled.load(Ordering::Acquire)
        {
            return Err(Fault::denied());
        }
        ensure_size(&json!({"tools":tools})).map_err(|_| Fault {
            code: -32002,
            message: "combined tool catalog exceeds limit",
        })?;
        Ok(json!({"tools":tools}))
    }

    async fn message(
        self: &Arc<Self>,
        session: Arc<Session>,
        value: Value,
        original_deadline: Option<tokio::time::Instant>,
    ) -> Option<Value> {
        let id = value.get("id").cloned();
        let method = value["method"].as_str().unwrap_or("");
        if id.is_none() {
            if method == "notifications/initialized" {
                session.initialized.store(true, Ordering::Release);
            }
            if method == "notifications/cancelled" {
                if let Some(id) = value.pointer("/params/requestId") {
                    if let Some(token) = session
                        .jobs
                        .lock()
                        .expect("jobs poisoned")
                        .get(&id.to_string())
                    {
                        token.cancel();
                    }
                }
            }
            return None;
        }
        let id = id.expect("request ID");
        if !session.initialized.load(Ordering::Acquire) {
            return Some(rpc_error(
                id,
                -32000,
                "session initialization is incomplete",
            ));
        }
        if self.draining.load(Ordering::Acquire) {
            return Some(rpc_error(id, -32000, "gateway draining"));
        }
        let _capacity = match self.admission.clone().try_acquire_owned() {
            Ok(capacity) => capacity,
            Err(_) => {
                self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
                return Some(rpc_error(id, -32029, "gateway request capacity exceeded"));
            }
        };
        let token = CancellationToken::new();
        {
            let mut jobs = session.jobs.lock().expect("jobs poisoned");
            if jobs.len() >= 32 || jobs.contains_key(&id.to_string()) {
                return Some(rpc_error(
                    id,
                    -32029,
                    "duplicate request ID or session capacity exceeded",
                ));
            }
            jobs.insert(id.to_string(), token.clone());
        }
        let _job = Job {
            session: session.clone(),
            id: id.to_string(),
        };
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        let deadline = original_deadline.unwrap_or_else(|| {
            tokio::time::Instant::now() + Duration::from_millis(self.config.request_timeout_ms)
        });
        let operation = async {
            match method {
                "ping" => Ok(json!({})),
                "tools/list" => {
                    if value.pointer("/params/cursor").is_some() {
                        return Err(Fault::invalid());
                    }
                    self.tools(&session, deadline).await
                }
                "tools/call" => {
                    let name = value
                        .pointer("/params/name")
                        .and_then(Value::as_str)
                        .ok_or_else(Fault::invalid)?;
                    let args = value
                        .pointer("/params/arguments")
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    if !args.is_object() {
                        return Err(Fault::invalid());
                    }
                    self.tool(session.clone(), name, args, deadline).await
                }
                _ => Err(Fault {
                    code: -32601,
                    message: "method not supported",
                }),
            }
        };
        let outcome = tokio::select! {
            _ = token.cancelled() => Err(Fault { code: -32800, message: "request cancelled; issued effects may have completed" }),
            _ = session.stopped.cancelled() => Err(Fault::denied()),
            result = tokio::time::timeout_at(deadline, operation).instrument(tracing::info_span!("mcp.gateway.request", rpc.method = if matches!(method, "ping" | "tools/list" | "tools/call") { method } else { "unsupported" })) => result.unwrap_or(Err(Fault { code: -32008, message: "request deadline exceeded; issued effects may have completed" })),
        };
        Some(match outcome {
            Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
            Err(error) => rpc_error(id, error.code, error.message),
        })
    }

    pub async fn shutdown(&self) {
        self.draining.store(true, Ordering::Release);
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(self.config.limits.shutdown_grace_ms);
        while self.admission.available_permits() < self.config.max_in_flight
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let sessions: Vec<_> = self
            .sessions
            .lock()
            .expect("sessions poisoned")
            .drain()
            .map(|(_, session)| session)
            .collect();
        self.close_sessions(sessions).await;
        let cleanup_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while self.preparing.available_permits() < self.config.limits.upstream_initializations
            && tokio::time::Instant::now() < cleanup_deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.cleanup.drain(cleanup_deadline).await;
        self.network_stop.cancel();
    }

    async fn close_sessions(&self, sessions: Vec<Arc<Session>>) {
        use futures_util::StreamExt;
        for session in &sessions {
            session.cancel();
        }
        futures_util::stream::iter(sessions)
            .for_each_concurrent(32, |session| async move {
                for client in session.clients.values() {
                    client.close().await;
                }
            })
            .await;
    }

    async fn maintain(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! { _ = self.network_stop.cancelled() => break, _ = interval.tick() => {} }
            let expired = {
                let mut sessions = self.sessions.lock().expect("sessions poisoned");
                let ids: Vec<_> = sessions
                    .iter()
                    .filter(|(_, session)| {
                        session.stopped.is_cancelled()
                            || (session
                                .touched
                                .lock()
                                .expect("session clock poisoned")
                                .elapsed()
                                >= Duration::from_secs(self.config.session_ttl_seconds)
                                && session
                                    .jobs
                                    .lock()
                                    .expect("session jobs poisoned")
                                    .is_empty())
                    })
                    .map(|(id, _)| id.clone())
                    .collect();
                ids.into_iter()
                    .filter_map(|id| sessions.remove(&id))
                    .collect::<Vec<_>>()
            };
            self.metrics
                .expired
                .fetch_add(expired.len() as u64, Ordering::Relaxed);
            self.close_sessions(expired).await;
        }
    }

    fn snapshot(&self) -> Value {
        let metrics = &self.metrics;
        json!({"generation":self.epoch.load(Ordering::Acquire),"sessions":self.sessions.lock().expect("sessions poisoned").len(),
            "requests":metrics.requests.load(Ordering::Relaxed),"upstream_calls":metrics.upstream_calls.load(Ordering::Relaxed),
            "cache_hits":metrics.cache_hits.load(Ordering::Relaxed),"cache_misses":metrics.cache_misses.load(Ordering::Relaxed),
            "cache_bypasses":metrics.cache_bypasses.load(Ordering::Relaxed),"state_checks":metrics.state_checks.load(Ordering::Relaxed),"state_conflicts":metrics.state_conflicts.load(Ordering::Relaxed),"denied":metrics.denied.load(Ordering::Relaxed),
            "prewarms":metrics.prewarms.load(Ordering::Relaxed),"prewarm_errors":metrics.prewarm_errors.load(Ordering::Relaxed),
            "rejected_capacity":metrics.rejected.load(Ordering::Relaxed),"sessions_expired":metrics.expired.load(Ordering::Relaxed),
            "active_requests":self.config.max_in_flight-self.admission.available_permits(),
            "active_upstream":self.config.limits.upstream_calls-self.upstream_admission.available_permits(),
            "active_initializations":self.config.limits.upstream_initializations-self.preparing.available_permits(),
            "body_readers":self.config.limits.body_readers-self.body_readers.available_permits(),
            "buffered_bytes":self.config.limits.buffered_bytes-self.body_bytes.available_permits(),
            "connections":metrics.connections.load(Ordering::Relaxed),"draining":self.draining.load(Ordering::Relaxed),
            "cleanup":self.cleanup.stats(),"cache":self.cache.stats().unwrap_or(Value::Null)})
    }
}

struct Job {
    session: Arc<Session>,
    id: String,
}
impl Drop for Job {
    fn drop(&mut self) {
        self.session
            .jobs
            .lock()
            .expect("jobs poisoned")
            .remove(&self.id);
        *self.session.touched.lock().expect("session clock poisoned") = Instant::now();
    }
}

fn secret(name: &str) -> Result<String> {
    let value =
        std::env::var(name).with_context(|| format!("missing credential environment {name}"))?;
    ensure!(
        (16..=4096).contains(&value.len()) && value.bytes().all(|b| (0x21..=0x7e).contains(&b)),
        "credential must contain 16..4096 visible ASCII bytes"
    );
    Ok(digest(value.as_bytes()))
}
fn bearer(headers: &HeaderMap) -> Option<String> {
    let (scheme, value) = headers
        .get("authorization")?
        .to_str()
        .ok()?
        .split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    if value.len() > 4096 {
        return None;
    }
    Some(digest(value.as_bytes()))
}
fn server_allowed(actor: &Actor, server: &Server) -> bool {
    actor.settings.tools.contains("*")
        || actor
            .settings
            .tools
            .iter()
            .any(|tool| tool.starts_with(&format!("{}__", server.id)))
}
fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
fn valid_request(value: &Value) -> bool {
    value.is_object()
        && value["jsonrpc"] == "2.0"
        && value["method"]
            .as_str()
            .is_some_and(|method| !method.is_empty() && method.len() <= 128)
        && value.get("id").is_none_or(|id| {
            id.as_str().is_some_and(|id| id.len() <= 128) || id.is_i64() || id.is_u64()
        })
        && value.get("params").is_none_or(Value::is_object)
        && value.get("result").is_none()
        && value.get("error").is_none()
}
fn ensure_size(value: &Value) -> Result<()> {
    ensure!(
        serde_json::to_vec(value)?.len() <= MAX_MESSAGE,
        "message exceeds limit"
    );
    Ok(())
}
