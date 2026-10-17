//! Operator configuration. Credentials are resolved from named environment variables.
use crate::upstream::{valid_env, Endpoint};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "listen")]
    pub listen: SocketAddr,
    pub admin_token_env: String,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default = "request_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "sessions")]
    pub max_sessions: usize,
    #[serde(default = "in_flight")]
    pub max_in_flight: usize,
    #[serde(default = "session_ttl")]
    pub session_ttl_seconds: u64,
    #[serde(default)]
    pub limits: ResourceLimits,
    #[serde(default)]
    pub state_path: Option<PathBuf>,
    pub users: Vec<User>,
    pub servers: Vec<Server>,
    #[serde(default)]
    pub workflows: Vec<Workflow>,
}
/// Independent resource budgets; request admission is not a memory or socket bound.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceLimits {
    pub max_connections: usize,
    pub connection_idle_ms: u64,
    pub body_readers: usize,
    pub admin_requests: usize,
    pub request_bytes: usize,
    pub buffered_bytes: usize,
    pub observation_bytes: usize,
    pub upstream_calls: usize,
    pub validation_parallelism: usize,
    pub upstream_initializations: usize,
    pub upstream_idle_per_host: usize,
    pub cleanup_pending: usize,
    pub shutdown_grace_ms: u64,
}
impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_connections: 2048,
            connection_idle_ms: 180_000,
            body_readers: 64,
            admin_requests: 16,
            request_bytes: 1024 * 1024,
            buffered_bytes: 32 * 1024 * 1024,
            observation_bytes: 16 * 1024 * 1024,
            upstream_calls: 512,
            validation_parallelism: 1,
            upstream_initializations: 64,
            upstream_idle_per_host: 64,
            cleanup_pending: 4096,
            shutdown_grace_ms: 10_000,
        }
    }
}

fn server_calls() -> usize {
    512
}
fn listen() -> SocketAddr {
    "127.0.0.1:7400".parse().expect("literal address")
}
fn request_ms() -> u64 {
    30_000
}
fn sessions() -> usize {
    128
}
fn in_flight() -> usize {
    32
}
fn session_ttl() -> u64 {
    900
}
fn enabled() -> bool {
    true
}
fn rate() -> u64 {
    20
}
fn burst() -> u64 {
    40
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct User {
    pub id: String,
    pub tenant: String,
    pub token_env: String,
    pub tools: BTreeSet<String>,
    #[serde(default = "enabled")]
    pub enabled: bool,
    #[serde(default = "rate")]
    pub requests_per_second: u64,
    #[serde(default = "burst")]
    pub burst: u64,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Server {
    pub id: String,
    pub revision: String,
    pub endpoint: Endpoint,
    #[serde(default)]
    pub prewarm: bool,
    #[serde(default = "server_calls")]
    pub max_in_flight: usize,
    #[serde(default)]
    pub tools: BTreeMap<String, ToolRule>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRule {
    /// Explicit operator opt-in to bounded-staleness result reuse. No annotation inference.
    #[serde(default)]
    pub cache_ttl_ms: u64,
    #[serde(default)]
    pub reusable: bool,
    /// Opt-in state contract; validator is called with the original tool name and arguments.
    #[serde(default)]
    pub state_tool: Option<String>,
    /// Optional state-batch-v1 validator; each observation still consumes rate budget.
    #[serde(default)]
    pub state_batch_tool: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Workflow {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub steps: Vec<Step>,
    pub outputs: Vec<String>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    pub id: String,
    pub tool: String,
    pub arguments: BTreeMap<String, Binding>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default)]
    pub step: Option<String>,
    #[serde(default)]
    pub pointer: String,
    #[serde(default)]
    pub literal: Option<Value>,
}

pub fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 32
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

pub fn permits(patterns: &BTreeSet<String>, tool: &str) -> bool {
    patterns.contains("*")
        || patterns.contains(tool)
        || tool
            .split_once("__")
            .is_some_and(|(server, _)| patterns.contains(&format!("{server}__*")))
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        ensure!(
            std::fs::metadata(path)?.len() <= 256 * 1024,
            "configuration exceeds limit"
        );
        let mut config: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        let directory = path
            .canonicalize()?
            .parent()
            .context("configuration has no directory")?
            .to_path_buf();
        for server in &mut config.servers {
            if let Endpoint::Stdio { command, args, .. } = &mut server.endpoint {
                let value = command.to_string_lossy();
                if let Some(name) = value.strip_prefix("env:") {
                    ensure!(valid_env(name), "invalid executable environment variable");
                    *command = std::env::var(name)
                        .with_context(|| format!("missing executable environment {name}"))?
                        .into();
                }
                for arg in args {
                    *arg = arg.replace("{config_dir}", &directory.to_string_lossy());
                }
            }
        }
        if let Some(state) = &mut config.state_path {
            if state.is_relative() {
                *state = directory.join(&*state);
            }
        }
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            valid_env(&self.admin_token_env),
            "invalid administrator credential environment name"
        );
        ensure!(
            (100..=120_000).contains(&self.request_timeout_ms)
                && (1..=100_000).contains(&self.max_sessions)
                && (1..=4096).contains(&self.max_in_flight)
                && (10..=3600).contains(&self.session_ttl_seconds),
            "invalid runtime limits"
        );
        let limits = &self.limits;
        ensure!(
            (16..=100_000).contains(&limits.max_connections)
                && (1000..=600_000).contains(&limits.connection_idle_ms)
                && (1..=4096).contains(&limits.body_readers)
                && (1..=256).contains(&limits.admin_requests)
                && (1024..=1024 * 1024).contains(&limits.request_bytes)
                && (limits.request_bytes..=1024 * 1024 * 1024).contains(&limits.buffered_bytes)
                && (1024..=256 * 1024 * 1024).contains(&limits.observation_bytes)
                && (1..=8).contains(&limits.validation_parallelism)
                && (1..=8192).contains(&limits.upstream_calls)
                && (1..=1024).contains(&limits.upstream_initializations)
                && (0..=8192).contains(&limits.upstream_idle_per_host)
                && (16..=100_000).contains(&limits.cleanup_pending)
                && (100..=120_000).contains(&limits.shutdown_grace_ms),
            "invalid resource budgets"
        );
        ensure!(
            !self.users.is_empty()
                && self.users.len() <= 64
                && !self.servers.is_empty()
                && self.servers.len() <= 16
                && self.workflows.len() <= 32,
            "configuration count exceeds limit"
        );
        ensure!(
            self.allowed_origins.len() <= 32
                && self.allowed_origins.iter().all(|origin| origin.len() <= 512
                    && (origin.starts_with("http://") || origin.starts_with("https://"))),
            "invalid allowed origins"
        );
        let mut users = BTreeSet::new();
        for user in &self.users {
            ensure!(
                identifier(&user.id)
                    && identifier(&user.tenant)
                    && users.insert(&user.id)
                    && valid_env(&user.token_env),
                "invalid or duplicate user"
            );
            ensure!(
                !user.tools.is_empty()
                    && user.tools.len() <= 128
                    && user.tools.iter().all(|tool| !tool.is_empty()
                        && tool.len() <= 128
                        && !tool.chars().any(char::is_control)),
                "invalid user tool rules"
            );
            ensure!(
                (1..=10000).contains(&user.requests_per_second)
                    && (1..=10000).contains(&user.burst),
                "invalid user rate limit"
            );
        }
        let mut servers = BTreeSet::new();
        for server in &self.servers {
            ensure!(
                identifier(&server.id) && server.id != "waypilot" && servers.insert(&server.id),
                "invalid or duplicate server ID"
            );
            ensure!(
                !server.revision.is_empty()
                    && server.revision.len() <= 128
                    && !server.revision.chars().any(char::is_control)
                    && server.tools.len() <= 512,
                "invalid server revision/rules"
            );
            ensure!(
                (1..=8192).contains(&server.max_in_flight),
                "invalid upstream call budget"
            );
            server.endpoint.validate()?;
            for (tool, rule) in &server.tools {
                if let Some(batch) = &rule.state_batch_tool {
                    ensure!(
                        rule.state_tool.is_some()
                            && batch != tool
                            && !batch.is_empty()
                            && batch.len() <= 128
                            && !batch.chars().any(char::is_control),
                        "batch validation requires state_tool and a distinct validator"
                    );
                    ensure!(
                        server.tools.get(batch).is_none_or(
                            |rule| rule.state_tool.is_none() && rule.state_batch_tool.is_none()
                        ),
                        "batch validators cannot require validation"
                    );
                }
                if let Some(validator) = &rule.state_tool {
                    ensure!(
                        server
                            .tools
                            .get(validator)
                            .is_none_or(|rule| rule.state_tool.is_none()),
                        "state validators cannot require another state validator"
                    );
                    ensure!(rule.reusable && validator != tool && !validator.is_empty()
                        && validator.len() <= 128 && !validator.chars().any(char::is_control),
                        "state validation requires an explicit reusable tool and a distinct validator");
                }
                ensure!(
                    !tool.is_empty()
                        && tool.len() <= 128
                        && rule.cache_ttl_ms <= 3_600_000
                        && (rule.cache_ttl_ms == 0 || rule.reusable),
                    "result reuse requires explicit reusable=true and bounded TTL"
                );
            }
        }
        let mut workflows = BTreeSet::new();
        for workflow in &self.workflows {
            ensure!(
                identifier(&workflow.name)
                    && workflows.insert(&workflow.name)
                    && workflow.description.len() <= 1024
                    && workflow.input_schema.is_object()
                    && workflow.input_schema["type"] == "object",
                "invalid workflow definition"
            );
            ensure!(
                !workflow.steps.is_empty()
                    && workflow.steps.len() <= 16
                    && !workflow.outputs.is_empty()
                    && workflow.outputs.len() <= 16,
                "invalid workflow size"
            );
            let mut steps = BTreeSet::new();
            for step in &workflow.steps {
                ensure!(
                    identifier(&step.id) && !steps.contains(&step.id),
                    "invalid or duplicate workflow step"
                );
                let (server, tool) = step
                    .tool
                    .split_once("__")
                    .context("workflow tool needs a server prefix")?;
                ensure!(
                    servers.iter().any(|value| value.as_str() == server)
                        && !tool.is_empty()
                        && step.arguments.len() <= 64,
                    "unknown workflow server or invalid arguments"
                );
                for binding in step.arguments.values() {
                    let choices = usize::from(binding.input.is_some())
                        + usize::from(binding.step.is_some())
                        + usize::from(binding.literal.is_some());
                    ensure!(
                        choices == 1
                            && (binding.pointer.is_empty() || binding.pointer.starts_with('/')),
                        "binding must have exactly one source and a JSON pointer"
                    );
                    if let Some(input) = &binding.input {
                        ensure!(
                            input.is_empty() || input.starts_with('/'),
                            "input binding needs a JSON pointer"
                        );
                    }
                    if let Some(parent) = &binding.step {
                        ensure!(
                            steps.iter().any(|value| *value == parent),
                            "workflow dependencies must reference an earlier step"
                        );
                    }
                }
                steps.insert(&step.id);
            }
            ensure!(
                workflow
                    .outputs
                    .iter()
                    .all(|output| steps.iter().any(|step| *step == output)),
                "unknown workflow output"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn grants_match_exact_server_boundaries() {
        let rules = BTreeSet::from(["docs__*".into(), "local__echo".into()]);
        assert!(permits(&rules, "docs__search"));
        assert!(permits(&rules, "local__echo"));
        assert!(!permits(&rules, "docs-other__search"));
        assert!(!permits(&rules, "local__write"));
    }
}
