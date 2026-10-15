//! MCP stdio adapter using Wayframe's supervised process transport.
use anyhow::{bail, ensure, Context as _, Result};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{sync::OnceCell, time::Instant};
use tracing::Instrument;
use wayframe_transport::stdio::{Config, Stdio};

pub const ROLES: [&str; 2] = ["dependencies", "source"];

pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Clone)]
pub struct Fixture {
    python: PathBuf,
    script: PathBuf,
    script_digest: String,
    boot_ms: u64,
    call_ms: u64,
}

impl Fixture {
    pub fn new(python: PathBuf, script: PathBuf, boot_ms: u64, call_ms: u64) -> Result<Self> {
        ensure!(
            python.is_absolute() && python.is_file(),
            "Python executable must be an absolute file path"
        );
        ensure!(
            script.is_absolute() && script.is_file(),
            "fixture must be an absolute file path"
        );
        ensure!(
            boot_ms <= 2000 && call_ms <= 2000,
            "fixture delay exceeds limit"
        );
        ensure!(
            std::fs::metadata(&script)?.len() <= 64 * 1024,
            "fixture too large"
        );
        let script_digest = digest(&std::fs::read(&script)?);
        Ok(Self {
            python,
            script,
            script_digest,
            boot_ms,
            call_ms,
        })
    }

    pub fn revision(&self) -> &str {
        &self.script_digest
    }
}

#[derive(Default)]
pub struct Counters {
    spawned: AtomicU64,
    calls: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Counts {
    pub spawned: u64,
    pub calls: u64,
}

impl Counts {
    pub fn since(self, before: Self) -> Self {
        Self {
            spawned: self.spawned - before.spawned,
            calls: self.calls - before.calls,
        }
    }
}

impl Counters {
    pub fn snapshot(&self) -> Counts {
        Counts {
            spawned: self.spawned.load(Ordering::Relaxed),
            calls: self.calls.load(Ordering::Relaxed),
        }
    }
}

struct Session {
    transport: Arc<Stdio>,
}

impl Session {
    async fn request(&self, method: &str, params: Value, deadline: Instant) -> Result<Value> {
        let reply = self
            .transport
            .call(
                json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}),
                deadline,
            )
            .await?
            .context("missing MCP response")?;
        if reply.get("error").is_some() {
            bail!("MCP {method} failed");
        }
        reply.get("result").cloned().context("missing MCP result")
    }
}

pub struct Pool {
    fixture: Fixture,
    dependencies: OnceCell<Arc<Session>>,
    source: OnceCell<Arc<Session>>,
    pub counters: Counters,
}

impl Pool {
    pub fn new(fixture: Fixture) -> Self {
        Self {
            fixture,
            dependencies: OnceCell::new(),
            source: OnceCell::new(),
            counters: Counters::default(),
        }
    }

    async fn session(&self, role: &'static str, deadline: Instant) -> Result<Arc<Session>> {
        let cell = match role {
            "dependencies" => &self.dependencies,
            "source" => &self.source,
            _ => bail!("unconfigured MCP role"),
        };
        // A disconnected session is failed closed. Replacing generations and reconnecting
        // after cancellation is a later milestone, not an implicit retry of tool calls.
        let session = cell
            .get_or_try_init(|| self.connect(role, deadline))
            .instrument(tracing::info_span!("mcp.prewarm", server = role))
            .await?;
        ensure!(
            session.transport.ready(),
            "MCP session unavailable; create a new runtime generation"
        );
        Ok(session.clone())
    }

    async fn connect(&self, role: &'static str, deadline: Instant) -> Result<Arc<Session>> {
        ensure!(
            digest(&std::fs::read(&self.fixture.script)?) == self.fixture.script_digest,
            "fixture changed: create a new runtime generation"
        );
        let transport = Stdio::spawn(Config {
            command: self.fixture.python.clone(),
            args: vec![
                self.fixture.script.to_string_lossy().into(),
                "--role".into(),
                role.into(),
                "--boot-ms".into(),
                self.fixture.boot_ms.to_string(),
                "--call-ms".into(),
                self.fixture.call_ms.to_string(),
            ],
            environment: BTreeMap::new(),
            inherit_env: vec![],
            max_in_flight: 4,
            max_message_bytes: 64 * 1024,
        })?;
        self.counters.spawned.fetch_add(1, Ordering::Relaxed);
        let session = Arc::new(Session { transport });
        let initialize = json!({
            "protocolVersion":"2025-11-25",
            "capabilities":{},
            "clientInfo":{"name":"waypilot","version":env!("CARGO_PKG_VERSION")}
        });
        let initialized = session.request("initialize", initialize, deadline).await?;
        ensure!(
            initialized["protocolVersion"] == "2025-11-25"
                && initialized["serverInfo"]["version"] == "1",
            "unexpected MCP protocol/server revision"
        );
        session
            .transport
            .call(
                json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                deadline,
            )
            .await?;
        let catalog = session.request("tools/list", json!({}), deadline).await?;
        let mut names = catalog["tools"]
            .as_array()
            .context("invalid tool catalog")?
            .iter()
            .map(|tool| tool["name"].as_str().context("tool missing name"))
            .collect::<Result<Vec<_>>>()?;
        names.sort_unstable();
        let expected = if role == "dependencies" {
            vec!["dependency_index", "parse_manifest"]
        } else {
            vec!["parse_source", "source_index"]
        };
        ensure!(
            names == expected,
            "tool catalog differs from authored contract"
        );
        Ok(session)
    }

    pub async fn prewarm(&self, role: &'static str, deadline: Instant) -> Result<()> {
        self.session(role, deadline).await?;
        Ok(())
    }

    pub async fn call(
        &self,
        role: &'static str,
        tool: &'static str,
        value: Value,
        deadline: Instant,
    ) -> Result<Value> {
        let session = self.session(role, deadline).await?;
        self.counters.calls.fetch_add(1, Ordering::Relaxed);
        let result = session
            .request(
                "tools/call",
                json!({"name":tool,"arguments":{"value":value}}),
                deadline,
            )
            .instrument(tracing::info_span!("mcp.tool", server = role, tool))
            .await?;
        ensure!(result["isError"] != true, "MCP tool reported an error");
        result
            .get("structuredContent")
            .cloned()
            .context("tool omitted structured content")
    }
}
