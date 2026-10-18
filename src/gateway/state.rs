//! Waypilot state-v1 adapter. Versions are supplied by an explicitly trusted upstream.
use super::{Fault, Gateway, Session};
use futures_util::{stream, StreamExt};
use rand::Rng;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant as Clock},
};
use tokio::time::Instant;
use tracing::Instrument;
use wayframe_cache::{Dependency, Evidence, Key};

pub const META: &str = "io.waypilot/state";
pub const OBSERVATIONS: &str = "io.waypilot/observations";
const BATCH_META: &str = "io.waypilot/state-batch";

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObservationRequest {
    #[serde(default)]
    pub capture: bool,
    #[serde(default)]
    pub require: Vec<String>,
}
impl ObservationRequest {
    pub fn parse(value: Option<&Value>) -> Result<Self, Fault> {
        let request: Self = value.map_or_else(
            || Ok(Self::default()),
            |value| serde_json::from_value(value.clone()).map_err(|_| Fault::invalid()),
        )?;
        if request.require.len() > 64
            || request
                .require
                .iter()
                .any(|token| token.len() != 64 || !token.bytes().all(|c| c.is_ascii_hexdigit()))
        {
            return Err(Fault::invalid());
        }
        Ok(request)
    }
}
struct Observation {
    states: Vec<Arc<RemoteState>>,
    epoch: u64,
    expires: Clock,
    bytes: usize,
    _budget: tokio::sync::OwnedSemaphorePermit,
}
pub(crate) struct Observations(BTreeMap<String, Observation>, Arc<tokio::sync::Semaphore>);
#[cfg(test)]
impl Default for Observations {
    fn default() -> Self {
        Self::new(Arc::new(tokio::sync::Semaphore::new(16 * 1024 * 1024)))
    }
}
impl Observations {
    pub fn new(budget: Arc<tokio::sync::Semaphore>) -> Self {
        Self(BTreeMap::new(), budget)
    }
    pub fn clear(&mut self) {
        self.0.clear();
    }
    pub fn prune(&mut self, epoch: u64, now: Clock) {
        self.0
            .retain(|_, entry| entry.epoch == epoch && now < entry.expires);
    }
    pub fn resolve(
        &mut self,
        tokens: &[String],
        epoch: u64,
        now: Clock,
    ) -> Result<Vec<Arc<RemoteState>>, Fault> {
        self.prune(epoch, now);
        let mut lineage = Lineage::default();
        for token in tokens {
            let entry = self.0.get(token).ok_or_else(conflict)?;
            for state in &entry.states {
                lineage.add_state(state.clone())?;
            }
        }
        Ok(lineage.states)
    }
    pub fn retain(
        &mut self,
        states: Vec<Arc<RemoteState>>,
        epoch: u64,
        now: Clock,
    ) -> Result<String, Fault> {
        self.prune(epoch, now);
        let bytes = states
            .iter()
            .map(|state| {
                state.arguments.to_string().len()
                    + state
                        .evidence
                        .resources()
                        .iter()
                        .map(|(k, v)| k.len() + v.len())
                        .sum::<usize>()
                    + state.tool.len()
                    + state.validator.len()
                    + state.batch_validator.as_ref().map_or(0, String::len)
                    + [
                        &state.key.tenant,
                        &state.key.subject,
                        &state.key.permissions,
                        &state.key.route,
                        &state.key.operation,
                        &state.key.target,
                        &state.key.arguments,
                    ]
                    .into_iter()
                    .map(String::len)
                    .sum::<usize>()
                    + state.evidence.resources().len() * 64
                    + 512
            })
            .sum::<usize>();
        if states.len() > 64
            || self.0.len() >= 64
            || self.0.values().map(|entry| entry.bytes).sum::<usize>() + bytes > 1_048_576
        {
            return Err(Fault {
                code: -32029,
                message: "observation capacity exceeded; issued effects may have completed",
            });
        }
        let budget = self
            .1
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| Fault {
                code: -32029,
                message: "global observation capacity exceeded; issued effects may have completed",
            })?;
        let token = format!(
            "{:032x}{:032x}",
            rand::rng().random::<u128>(),
            rand::rng().random::<u128>()
        );
        self.0.insert(
            token.clone(),
            Observation {
                states,
                epoch,
                expires: now + Duration::from_secs(60),
                bytes,
                _budget: budget,
            },
        );
        Ok(token)
    }
}
#[derive(PartialEq)]
pub(crate) struct RemoteState {
    pub validator: String,
    pub tool: String,
    pub batch_validator: Option<String>,
    pub arguments: Value,
    pub evidence: Evidence,
    pub key: Key,
}
#[derive(Clone)]
pub(crate) struct Lineage {
    pub cacheable: bool,
    pub receipts: Vec<Dependency>,
    pub states: Vec<Arc<RemoteState>>,
}
impl Default for Lineage {
    fn default() -> Self {
        Self {
            cacheable: true,
            receipts: vec![],
            states: vec![],
        }
    }
}
impl Lineage {
    pub fn add_state(&mut self, state: Arc<RemoteState>) -> Result<(), Fault> {
        if !self.states.contains(&state) {
            self.states.push(state);
        }
        if self.states.len() > 64 {
            return Err(conflict());
        }
        Ok(())
    }
    pub fn merge(&mut self, other: &Self) -> Result<(), Fault> {
        self.cacheable &= other.cacheable;
        for receipt in &other.receipts {
            if !self.receipts.contains(receipt) {
                self.receipts.push(receipt.clone());
            }
        }
        for state in &other.states {
            self.add_state(state.clone())?;
        }
        Ok(())
    }
}
pub(crate) fn conflict() -> Fault {
    Fault {
        code: -32010,
        message: "remote state could not be validated; request was not replayed",
    }
}
pub(super) fn evidence(value: &Value) -> Result<Evidence, Fault> {
    if value["isError"] == true {
        return Err(conflict());
    }
    let state = &value["_meta"][META];
    if state["version"] != 1 {
        return Err(conflict());
    }
    let resources = state["resources"].as_object().ok_or_else(conflict)?;
    let resources = resources
        .iter()
        .map(|(key, value)| {
            value
                .as_str()
                .map(|version| (key.clone(), version.into()))
                .ok_or_else(conflict)
        })
        .collect::<Result<_, _>>()?;
    Evidence::new(resources).map_err(|_| conflict())
}
// IDs associate each witness with the exact request, independent of response order.
fn batch_evidence(value: &Value, count: usize) -> Result<Vec<Evidence>, Fault> {
    let batch = &value["_meta"][BATCH_META];
    if value["isError"] == true || batch["version"] != 1 {
        return Err(conflict());
    }
    let observations = batch["observations"].as_array().ok_or_else(conflict)?;
    if observations.len() != count {
        return Err(conflict());
    }
    let mut results = BTreeMap::new();
    for observation in observations {
        let id = observation["id"].as_str().ok_or_else(conflict)?;
        let index: usize = id.parse().map_err(|_| conflict())?;
        if index >= count
            || index.to_string() != id
            || results
                .insert(
                    index,
                    evidence(&serde_json::json!({"_meta":{META:observation["state"]}}))?,
                )
                .is_some()
        {
            return Err(conflict());
        }
    }
    Ok(results.into_values().collect())
}
impl Gateway {
    pub(crate) fn check_observation(
        &self,
        state: &RemoteState,
        value: &Value,
    ) -> Result<(), Fault> {
        if evidence(value).is_ok_and(|observed| observed == state.evidence) {
            return Ok(());
        }
        let _ = self.cache.invalidate(&state.key);
        self.metrics.state_conflicts.fetch_add(1, Ordering::Relaxed);
        Err(conflict())
    }
    pub(crate) async fn probe_state(
        &self,
        session: &Arc<Session>,
        state: &RemoteState,
        deadline: Instant,
        epoch: u64,
    ) -> Result<Evidence, Fault> {
        async {
            self.authorize(session, &state.tool, epoch)?;
            self.authorize(session, &state.validator, epoch)?;
            if !session.actor.consume() {
                return Err(Fault {
                    code: -32029,
                    message: "state validation rate limit exceeded",
                });
            }
            let server = state
                .validator
                .split_once("__")
                .ok_or_else(Fault::invalid)?
                .0;
            let client = session.client(server).ok_or_else(Fault::denied)?;
            self.metrics.state_checks.fetch_add(1, Ordering::Relaxed);
            let value = self
                .call_upstream(
                    session,
                    &state.validator,
                    &client,
                    state.arguments.clone(),
                    deadline,
                    epoch,
                )
                .await?;
            self.authorize(session, &state.tool, epoch)?;
            self.authorize(session, &state.validator, epoch)?;
            evidence(&value)
        }
        .instrument(tracing::info_span!("mcp.state.validate"))
        .await
    }
    fn invalidate_state(&self, state: &RemoteState) {
        let _ = self.cache.invalidate(&state.key);
        self.metrics.state_conflicts.fetch_add(1, Ordering::Relaxed);
    }
    async fn validate_one(
        &self,
        session: &Arc<Session>,
        state: &RemoteState,
        deadline: Instant,
        epoch: u64,
    ) -> Result<(), Fault> {
        match self.probe_state(session, state, deadline, epoch).await {
            Ok(current) if current == state.evidence => Ok(()),
            result => {
                self.invalidate_state(state);
                Err(result.err().unwrap_or_else(conflict))
            }
        }
    }
    async fn probe_batch(
        &self,
        session: &Arc<Session>,
        validator: &str,
        states: &[&Arc<RemoteState>],
        deadline: Instant,
        epoch: u64,
    ) -> Result<Vec<Evidence>, Fault> {
        async {
            self.authorize(session, validator, epoch)?;
            for state in states {
                self.authorize(session, &state.tool, epoch)?;
                self.authorize(session, &state.validator, epoch)?;
                if !session.actor.consume() {
                    return Err(Fault {
                        code: -32029,
                        message: "state validation rate limit exceeded",
                    });
                }
            }
            let server = validator.split_once("__").ok_or_else(Fault::invalid)?.0;
            let client = session.client(server).ok_or_else(Fault::denied)?;
            self.metrics
                .state_checks
                .fetch_add(states.len() as u64, Ordering::Relaxed);
            self.metrics.state_batches.fetch_add(1, Ordering::Relaxed);
            let requests: Vec<_> = states
                .iter()
                .enumerate()
                .map(|(id, state)| {
                    serde_json::json!({"id":id.to_string(), "tool":state.arguments["tool"],
                    "arguments":state.arguments["arguments"]})
                })
                .collect();
            let value = self
                .call_upstream(
                    session,
                    validator,
                    &client,
                    serde_json::json!({"version":1,"observations":requests}),
                    deadline,
                    epoch,
                )
                .await?;
            self.authorize(session, validator, epoch)?;
            for state in states {
                self.authorize(session, &state.tool, epoch)?;
                self.authorize(session, &state.validator, epoch)?;
            }
            batch_evidence(&value, states.len())
        }
        .instrument(tracing::info_span!(
            "mcp.state.validate_batch",
            observations = states.len()
        ))
        .await
    }
    pub(crate) async fn validate_states(
        &self,
        session: &Arc<Session>,
        states: &[Arc<RemoteState>],
        deadline: Instant,
        epoch: u64,
    ) -> Result<(), Fault> {
        let parallelism = self.config.limits.validation_parallelism;
        if parallelism == 1 || states.len() < 2 {
            return self
                .validate_state_group(session, states, deadline, epoch)
                .await;
        }
        // Each logical server retains its existing validation order and session
        // serialization. Only independent servers overlap; validators are required
        // to have no effects. A failure drops outstanding validation futures.
        let mut groups: BTreeMap<&str, Vec<Arc<RemoteState>>> = BTreeMap::new();
        for state in states {
            let server = state
                .validator
                .split_once("__")
                .ok_or_else(Fault::invalid)?
                .0;
            groups.entry(server).or_default().push(state.clone());
        }
        let span = tracing::info_span!(
            "mcp.state.validate_set",
            server_groups = groups.len(),
            parallelism
        );
        async {
            let mut work = stream::iter(groups.into_values().map(|states| async move {
                self.validate_state_group(session, &states, deadline, epoch)
                    .await
            }))
            .buffer_unordered(parallelism);
            while let Some(result) = work.next().await {
                result?;
            }
            Ok(())
        }
        .instrument(span)
        .await
    }
    async fn validate_state_group(
        &self,
        session: &Arc<Session>,
        states: &[Arc<RemoteState>],
        deadline: Instant,
        epoch: u64,
    ) -> Result<(), Fault> {
        let mut batches: BTreeMap<&str, Vec<&Arc<RemoteState>>> = BTreeMap::new();
        for state in states {
            if let Some(batch) = state.batch_validator.as_deref() {
                batches.entry(batch).or_default().push(state);
            } else {
                self.validate_one(session, state, deadline, epoch).await?;
            }
        }
        for (validator, group) in batches {
            if group.len() == 1 {
                self.validate_one(session, group[0], deadline, epoch)
                    .await?;
                continue;
            }
            let result = self
                .probe_batch(session, validator, &group, deadline, epoch)
                .await;
            match result {
                Ok(current) => {
                    let mut changed = false;
                    for (state, evidence) in group.iter().zip(current) {
                        if evidence != state.evidence {
                            self.invalidate_state(state);
                            changed = true;
                        }
                    }
                    if changed {
                        return Err(conflict());
                    }
                }
                Err(error) => {
                    for state in group {
                        self.invalidate_state(state);
                    }
                    return Err(error);
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(Fault {
                code: -32008,
                message: "state validation deadline exceeded",
            });
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn observation() -> Arc<RemoteState> {
        let permissions = std::collections::BTreeSet::from(["server__read".to_string()]);
        Arc::new(RemoteState {
            validator: "server__version".into(),
            tool: "server__read".into(),
            batch_validator: None,
            arguments: json!({"tool":"read","arguments":{}}),
            evidence: Evidence::new(BTreeMap::from([("r".into(), "v1".into())])).unwrap(),
            key: Key::new(
                wayframe_cache::Scope {
                    tenant: "t",
                    subject: "s",
                    permissions: &permissions,
                    generation: 1,
                    route: "server",
                },
                "tools/call",
                "r1",
                &json!({}),
            )
            .unwrap(),
        })
    }
    #[test]
    fn observation_budget_is_shared_and_reclaimed_on_session_cleanup() {
        let now = Clock::now();
        let budget = Arc::new(tokio::sync::Semaphore::new(1024));
        let mut left = Observations::new(budget.clone());
        let mut right = Observations::new(budget.clone());
        left.retain(vec![observation()], 1, now).unwrap();
        assert_eq!(
            right.retain(vec![observation()], 1, now).unwrap_err().code,
            -32029
        );
        left.clear();
        right.retain(vec![observation()], 1, now).unwrap();
        right.prune(1, now + Duration::from_secs(60));
        assert_eq!(budget.available_permits(), 1024);
    }
    #[test]
    fn receipts_expire_and_bind_to_session_and_policy_generation() {
        let now = Clock::now();
        let mut store = Observations::default();
        let token = store.retain(vec![observation()], 1, now).unwrap();
        assert_eq!(
            store
                .resolve(&[token.clone(), token.clone()], 1, now)
                .unwrap()
                .len(),
            1
        );
        assert!(Observations::default()
            .resolve(std::slice::from_ref(&token), 1, now)
            .is_err());
        assert!(store
            .resolve(
                std::slice::from_ref(&token),
                1,
                now + Duration::from_secs(60)
            )
            .is_err());
        let token = store.retain(vec![observation()], 1, now).unwrap();
        assert!(store.resolve(&[token], 2, now).is_err());
        assert!(store.0.is_empty());
    }
    #[test]
    fn receipt_count_and_bytes_are_bounded_and_expiry_releases_capacity() {
        let now = Clock::now();
        let mut store = Observations::default();
        for _ in 0..64 {
            store.retain(vec![observation()], 1, now).unwrap();
        }
        assert_eq!(
            store.retain(vec![observation()], 1, now).unwrap_err().code,
            -32029
        );
        store
            .retain(vec![observation()], 1, now + Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.0.len(), 1);
        let mut large = observation();
        Arc::get_mut(&mut large).unwrap().arguments = json!({"private":"x".repeat(1_048_576)});
        assert_eq!(store.retain(vec![large], 1, now).unwrap_err().code, -32029);
        store.clear();
        assert!(store.0.is_empty());
    }
    #[test]
    fn observation_metadata_rejects_unknown_fields_types_and_excessive_handles() {
        for invalid in [
            json!(null),
            json!({"capture":1}),
            json!({"require":["bad"]}),
            json!({"extra":true}),
            json!({"require":vec!["a".repeat(64);65]}),
        ] {
            assert_eq!(
                ObservationRequest::parse(Some(&invalid))
                    .err()
                    .unwrap()
                    .code,
                -32602
            );
        }
        assert!(ObservationRequest::parse(None).unwrap().require.is_empty());
    }
    #[test]
    fn batch_response_ids_are_exact_complete_unique_and_order_independent() {
        let entry = |id| json!({"id":id,"state":{"version":1,"resources":{"r":"v1"}}});
        let valid =
            json!({"_meta":{BATCH_META:{"version":1,"observations":[entry("1"),entry("0")]}}});
        assert_eq!(batch_evidence(&valid, 2).unwrap().len(), 2);
        for entries in [
            json!([]),
            json!([entry("0")]),
            json!([entry("0"), entry("0")]),
            json!([entry("0"), entry("2")]),
            json!([entry("0"), entry("01")]),
            json!([entry("0"), entry("x")]),
            json!([entry("0"),{"id":"1","state":{}}]),
        ] {
            assert!(batch_evidence(
                &json!({"_meta":{BATCH_META:{"version":1,"observations":entries}}}),
                2
            )
            .is_err());
        }
        let mut bad = valid.clone();
        bad["isError"] = json!(true);
        assert!(batch_evidence(&bad, 2).is_err());
        let mut bad = valid;
        bad["_meta"][BATCH_META]["version"] = json!(2);
        assert!(batch_evidence(&bad, 2).is_err());
    }
    #[test]
    fn batch_validator_configuration_requires_a_state_contract() {
        let mut config: crate::config::Config =
            toml::from_str(include_str!("../../examples/local.toml")).unwrap();
        for server in &mut config.servers {
            server.endpoint = crate::upstream::Endpoint::Http {
                url: "http://127.0.0.1:1/mcp".into(),
                bearer_env: None,
                allow_insecure_http: false,
            };
        }
        config.validate().unwrap();
        let rule = config.servers[0].tools.values_mut().next().unwrap();
        rule.state_batch_tool = Some("batch".into());
        rule.state_tool = None;
        assert!(config.validate().is_err());
        let rule = config.servers[0].tools.values_mut().next().unwrap();
        rule.state_tool = Some("version".into());
        rule.reusable = true;
        config.validate().unwrap();
    }
    #[test]
    fn state_validation_configuration_requires_explicit_contract() {
        let mut config: crate::config::Config =
            toml::from_str(include_str!("../../examples/local.toml")).unwrap();
        for server in &mut config.servers {
            server.endpoint = crate::upstream::Endpoint::Http {
                url: "http://127.0.0.1:1/mcp".into(),
                bearer_env: None,
                allow_insecure_http: false,
            };
        }
        config.validate().unwrap();
        let rule = config.servers[0].tools.values_mut().next().unwrap();
        rule.state_tool = Some(String::new());
        assert!(config.validate().is_err());
        let rule = config.servers[0].tools.values_mut().next().unwrap();
        rule.state_tool = Some("version".into());
        rule.reusable = false;
        rule.cache_ttl_ms = 0;
        assert!(config.validate().is_err());
    }
    #[test]
    fn malformed_missing_error_and_unbounded_witnesses_fail_closed() {
        for value in [
            json!({}),
            json!({"_meta":{META:{"version":2,"resources":{"r":"1"}}}}),
            json!({"_meta":{META:{"version":1,"resources":{}}}}),
            json!({"_meta":{META:{"version":1,"resources":{"r":1}}}}),
            json!({"isError":true,"_meta":{META:{"version":1,"resources":{"r":"1"}}}}),
        ] {
            assert!(evidence(&value).is_err());
        }
        assert!(
            evidence(&json!({"_meta":{META:{"version":1,"resources":{"r":"epoch-1:rev-1"}}}}))
                .is_ok()
        );
    }
}
