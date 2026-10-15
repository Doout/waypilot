//! Waypilot state-v1 adapter. Versions are supplied by an explicitly trusted upstream.
use super::{Fault, Gateway, Session};
use serde_json::Value;
use std::sync::{atomic::Ordering, Arc};
use tokio::time::Instant;
use tracing::Instrument;
use wayframe_cache::{Dependency, Evidence, Key};

pub const META: &str = "io.waypilot/state";
#[derive(PartialEq)]
pub(crate) struct RemoteState {
    pub validator: String,
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
fn evidence(value: &Value) -> Result<Evidence, Fault> {
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
            self.authorize(session, &state.validator, epoch)?;
            evidence(&value)
        }
        .instrument(tracing::info_span!("mcp.state.validate"))
        .await
    }
    pub(crate) async fn validate_states(
        &self,
        session: &Arc<Session>,
        states: &[Arc<RemoteState>],
        deadline: Instant,
        epoch: u64,
    ) -> Result<(), Fault> {
        for state in states {
            match self.probe_state(session, state, deadline, epoch).await {
                Ok(current) if current == state.evidence => (),
                result => {
                    let _ = self.cache.invalidate(&state.key);
                    self.metrics.state_conflicts.fetch_add(1, Ordering::Relaxed);
                    return Err(result.err().unwrap_or_else(conflict));
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
    #[test]
    fn state_validation_configuration_requires_explicit_contract() {
        let mut config: crate::config::Config =
            toml::from_str(include_str!("../../examples/local.toml")).unwrap();
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
