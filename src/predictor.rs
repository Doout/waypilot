//! A bounded frequency baseline learned from successful execution traces.
//! It predicts environment use; it never establishes a tool effect contract.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Predictor {
    traces: u64,
    servers: BTreeMap<String, u64>,
}

impl Predictor {
    pub fn observe(&mut self, servers: impl IntoIterator<Item = &'static str>) {
        self.traces = self.traces.saturating_add(1);
        for server in servers.into_iter().collect::<BTreeSet<_>>() {
            if !crate::mcp::ROLES.contains(&server) {
                continue;
            }
            let count = self.servers.entry(server.into()).or_default();
            *count = count.saturating_add(1);
        }
    }

    pub fn predict(&self) -> Vec<&'static str> {
        if self.traces < 2 {
            return Vec::new();
        }
        crate::mcp::ROLES
            .into_iter()
            .filter(|server| {
                self.servers.get(*server).copied().unwrap_or(0) >= self.traces / 2 + self.traces % 2
            })
            .collect()
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.servers.len() <= 2
                && self.servers.iter().all(|(server, count)| crate::mcp::ROLES
                    .contains(&server.as_str())
                    && *count <= self.traces),
            "invalid prediction profile"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn requires_support_and_counts_a_server_once_per_trace() {
        let mut predictor = Predictor::default();
        predictor.observe(["dependencies", "dependencies"]);
        assert!(predictor.predict().is_empty());
        predictor.observe(["source"]);
        assert_eq!(predictor.predict(), vec!["dependencies", "source"]);
        predictor.observe(["source"]);
        assert_eq!(predictor.predict(), vec!["source"]);
    }
}
