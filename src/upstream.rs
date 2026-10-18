//! Serialized, bounded MCP client sessions. A failed invocation is never replayed.
use anyhow::{bail, ensure, Context, Result};
use futures_util::StreamExt;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::Mutex,
    time::{timeout_at, Instant},
};
use tracing::Instrument;
use wayframe_transport::stdio::{Config as StdioConfig, Stdio};

pub const VERSIONS: [&str; 3] = ["2025-11-25", "2025-06-18", "2025-03-26"];
pub const MAX_MESSAGE: usize = 1024 * 1024;

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "transport", rename_all = "snake_case", deny_unknown_fields)]
pub enum Endpoint {
    Stdio {
        command: PathBuf,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        inherit_env: Vec<String>,
    },
    Http {
        url: String,
        #[serde(default)]
        bearer_env: Option<String>,
        #[serde(default)]
        allow_insecure_http: bool,
    },
}

impl Endpoint {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Stdio {
                command,
                args,
                inherit_env,
            } => {
                ensure!(
                    command.is_absolute() && command.is_file(),
                    "stdio command must be an absolute executable path"
                );
                ensure!(
                    args.len() <= 128 && args.iter().all(|v| v.len() <= 8192),
                    "invalid process arguments"
                );
                ensure!(
                    inherit_env.len() <= 128 && inherit_env.iter().all(|v| valid_env(v)),
                    "invalid inherited environment names"
                );
            }
            Self::Http {
                url,
                bearer_env,
                allow_insecure_http,
            } => {
                let url: reqwest::Url = url.parse()?;
                ensure!(
                    url.host_str().is_some()
                        && url.username().is_empty()
                        && url.password().is_none()
                        && url.fragment().is_none(),
                    "invalid MCP URL"
                );
                let local = matches!(
                    url.host_str(),
                    Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
                );
                ensure!(
                    url.scheme() == "https"
                        || (url.scheme() == "http" && (local || *allow_insecure_http)),
                    "MCP URL requires HTTPS, loopback HTTP or explicit insecure HTTP opt-in"
                );
                if let Some(name) = bearer_env {
                    ensure!(valid_env(name), "invalid credential environment name");
                }
            }
        }
        Ok(())
    }
}

pub fn valid_env(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

enum Wire {
    Stdio(std::sync::Arc<Stdio>),
    Http {
        client: reqwest::Client,
        url: reqwest::Url,
        headers: Box<HeaderMap>,
        session: Option<String>,
    },
}

pub(crate) struct Connector {
    endpoint: Endpoint,
    http: Option<reqwest::Client>,
    cleanup: Option<Arc<crate::upstream_cleanup::Cleanup>>,
    preparing: Arc<tokio::sync::Semaphore>,
}
impl Connector {
    pub fn new(
        endpoint: Endpoint,
        cleanup: Option<Arc<crate::upstream_cleanup::Cleanup>>,
        idle_connections: usize,
        preparing: Arc<tokio::sync::Semaphore>,
    ) -> Result<Arc<Self>> {
        endpoint.validate()?;
        let http = if matches!(endpoint, Endpoint::Http { .. }) {
            Some(
                reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .retry(reqwest::retry::never())
                    .no_proxy()
                    .connect_timeout(Duration::from_secs(5))
                    .pool_max_idle_per_host(idle_connections)
                    .pool_idle_timeout(Duration::from_secs(30))
                    .build()?,
            )
        } else {
            None
        };
        Ok(Arc::new(Self {
            endpoint,
            http,
            cleanup,
            preparing,
        }))
    }
}
struct Connection {
    wire: Wire,
    version: String,
    tools: Arc<Vec<Value>>,
    catalog_revision: String,
    active_request: Option<u64>,
    cleanup: Option<Arc<crate::upstream_cleanup::Cleanup>>,
}

/// Each downstream session owns a separate client. Holding this lock during a
/// call prevents stdio cancellation from killing another in-flight invocation.
pub struct Client {
    connector: Arc<Connector>,
    connection: Mutex<Option<Connection>>,
    sequence: Arc<AtomicU64>,
    pub calls: AtomicU64,
    pub connects: Arc<AtomicU64>,
    meter: Option<Arc<AtomicU64>>,
}

#[derive(Clone)]
pub struct Catalog {
    pub tools: Arc<Vec<Value>>,
    pub revision: String,
}

impl Client {
    pub fn new(endpoint: Endpoint) -> Result<Self> {
        Self::with_connector(
            Connector::new(
                endpoint,
                None,
                16,
                Arc::new(tokio::sync::Semaphore::new(16)),
            )?,
            None,
        )
    }

    pub(crate) fn with_connector(
        connector: Arc<Connector>,
        meter: Option<Arc<AtomicU64>>,
    ) -> Result<Self> {
        Ok(Self {
            connector,
            connection: Mutex::new(None),
            sequence: Arc::new(AtomicU64::new(1)),
            calls: AtomicU64::new(0),
            connects: Arc::new(AtomicU64::new(0)),
            meter,
        })
    }

    async fn connect_owned<F>(
        connector: Arc<Connector>,
        sequence: Arc<AtomicU64>,
        connects: Arc<AtomicU64>,
        deadline: Instant,
        abandoned: F,
    ) -> Result<Connection>
    where
        F: Fn() -> bool + Send + Sync,
    {
        ensure!(Instant::now() < deadline, "upstream deadline exceeded");
        let wire = match &connector.endpoint {
            Endpoint::Stdio {
                command,
                args,
                inherit_env,
            } => Wire::Stdio(Stdio::spawn(StdioConfig {
                command: command.clone(),
                args: args.clone(),
                environment: BTreeMap::new(),
                inherit_env: inherit_env.clone(),
                max_in_flight: 1,
                max_message_bytes: MAX_MESSAGE,
            })?),
            Endpoint::Http {
                url, bearer_env, ..
            } => {
                let mut headers = HeaderMap::new();
                if let Some(name) = bearer_env {
                    let token = std::env::var(name).with_context(|| {
                        format!("missing upstream credential environment {name}")
                    })?;
                    ensure!(
                        !token.is_empty() && token.len() <= 8192,
                        "invalid upstream credential"
                    );
                    let mut header = HeaderValue::from_str(&format!("Bearer {token}"))
                        .context("invalid upstream credential header")?;
                    header.set_sensitive(true);
                    headers.insert(reqwest::header::AUTHORIZATION, header);
                }
                let client = connector.http.as_ref().expect("HTTP connector").clone();
                Wire::Http {
                    client,
                    url: url.parse()?,
                    headers: Box::new(headers),
                    session: None,
                }
            }
        };
        connects.fetch_add(1, Ordering::Relaxed);
        let mut connection = Connection {
            wire,
            version: VERSIONS[0].into(),
            tools: Arc::new(Vec::new()),
            catalog_revision: String::new(),
            active_request: None,
            cleanup: connector.cleanup.clone(),
        };
        let id = next_id(&sequence)?;
        let reply = exchange(&mut connection, json!({"jsonrpc":"2.0","id":id,"method":"initialize","params":{"protocolVersion":VERSIONS[0],"capabilities":{},"clientInfo":{"name":"waypilot","version":env!("CARGO_PKG_VERSION")}}}), deadline).await?;
        let initialized = result(reply, id)?;
        let version = initialized["protocolVersion"]
            .as_str()
            .context("server did not negotiate a protocol version")?;
        ensure!(
            VERSIONS.contains(&version),
            "unsupported upstream protocol version"
        );
        ensure!(
            initialized["capabilities"]["tools"].is_object(),
            "upstream does not advertise tools"
        );
        connection.version = version.into();
        ensure!(!abandoned(), "preparation abandoned after initialization");
        exchange(
            &mut connection,
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            deadline,
        )
        .await?;
        let mut cursor: Option<Value> = None;
        let mut seen = HashSet::new();
        let mut names = HashSet::new();
        let mut catalog_bytes = 2usize;
        for _ in 0..16 {
            ensure!(
                !abandoned(),
                "preparation abandoned during catalog discovery"
            );
            let id = next_id(&sequence)?;
            let params = cursor
                .as_ref()
                .map(|v| json!({"cursor":v}))
                .unwrap_or_else(|| json!({}));
            let page = result(
                exchange(
                    &mut connection,
                    json!({"jsonrpc":"2.0","id":id,"method":"tools/list","params":params}),
                    deadline,
                )
                .await?,
                id,
            )?;
            for tool in page["tools"].as_array().context("invalid tool catalog")? {
                let name = tool["name"].as_str().context("tool missing name")?;
                ensure!(
                    !name.is_empty()
                        && name.len() <= 128
                        && !name.chars().any(char::is_control)
                        && names.insert(name.to_owned())
                        && tool["inputSchema"].is_object(),
                    "invalid or duplicate tool definition"
                );
                catalog_bytes = catalog_bytes.saturating_add(serde_json::to_vec(tool)?.len() + 1);
                ensure!(
                    catalog_bytes <= MAX_MESSAGE,
                    "combined catalog exceeds byte limit"
                );
                Arc::get_mut(&mut connection.tools)
                    .expect("unshared catalog during preparation")
                    .push(tool.clone());
                ensure!(
                    connection.tools.len() <= 512,
                    "upstream tool catalog exceeds limit"
                );
            }
            cursor = page.get("nextCursor").filter(|v| !v.is_null()).cloned();
            if cursor.is_none() {
                connection.catalog_revision =
                    crate::mcp::digest(&serde_json::to_vec(connection.tools.as_ref())?);
                return Ok(connection);
            }
            let next = cursor
                .as_ref()
                .and_then(Value::as_str)
                .context("invalid pagination cursor")?;
            ensure!(
                next.len() <= 4096 && seen.insert(next.to_owned()),
                "repeated or oversized catalog cursor"
            );
        }
        bail!("catalog pagination exceeds limit")
    }

    fn next_id(&self) -> Result<u64> {
        next_id(&self.sequence)
    }

    pub async fn catalog(&self, deadline: Instant) -> Result<Catalog> {
        self.catalog_checked(deadline, || Ok(())).await
    }

    pub async fn catalog_checked<F>(&self, deadline: Instant, check: F) -> Result<Catalog>
    where
        F: Fn() -> Result<()> + Send + Sync,
    {
        timeout_at(deadline, async {
            let mut guard = self.connection.lock().await;
            ensure!(
                Instant::now() < deadline,
                "upstream deadline exceeded before preparation"
            );
            check()?;
            self.prepare(&mut guard, deadline, &check).await?;
            check()?;
            let connection = guard.as_ref().expect("prepared connection");
            Ok(Catalog {
                tools: connection.tools.clone(),
                revision: connection.catalog_revision.clone(),
            })
        })
        .instrument(tracing::info_span!("mcp.prepare"))
        .await?
    }

    async fn prepare<F>(
        &self,
        guard: &mut Option<Connection>,
        deadline: Instant,
        check: &F,
    ) -> Result<()>
    where
        F: Fn() -> Result<()> + Send + Sync,
    {
        if guard.as_ref().is_some_and(
            |connection| matches!(&connection.wire, Wire::Stdio(process) if !process.ready()),
        ) {
            *guard = None;
        }
        if guard.is_none() {
            let preparation =
                timeout_at(deadline, self.connector.preparing.clone().acquire_owned()).await??;
            ensure!(
                Instant::now() < deadline,
                "preparation queue deadline exceeded"
            );
            check()?;
            let connector = self.connector.clone();
            let sequence = self.sequence.clone();
            let connects = self.connects.clone();
            let (sender, receiver) = tokio::sync::oneshot::channel();
            // Complete an admitted handshake under its original deadline even if
            // its caller disappears. A discarded result owns the new session ID
            // and queues deletion. No tools/call is ever issued by this task.
            tokio::spawn(async move {
                let _preparation = preparation;
                let result = Self::connect_owned(connector, sequence, connects, deadline, || {
                    sender.is_closed()
                })
                .await;
                let _ = sender.send(result);
            });
            *guard = Some(receiver.await??);
        }
        Ok(())
    }

    pub async fn call(&self, name: &str, arguments: Value, deadline: Instant) -> Result<Value> {
        self.call_checked(name, arguments, deadline, || Ok(()))
            .await
    }

    pub async fn call_checked<F>(
        &self,
        name: &str,
        arguments: Value,
        deadline: Instant,
        check: F,
    ) -> Result<Value>
    where
        F: Fn() -> Result<()> + Send + Sync,
    {
        timeout_at(deadline, async {
            let queued = Instant::now();
            let mut guard = self.connection.lock().await;
            tracing::Span::current().record("queue_us", queued.elapsed().as_micros() as u64);
            ensure!(Instant::now() < deadline, "upstream deadline exceeded before admission");
            check()?;
            self.prepare(&mut guard, deadline, &check).await?;
            check()?;
            ensure!(Instant::now() < deadline, "upstream deadline exceeded before dispatch");
            // Ownership stays with this invocation while I/O is pending. On
            // cancellation the next distinct call creates a fresh session.
            let mut connection = guard.take().expect("prepared connection");
            ensure!(connection.tools.iter().any(|tool| tool["name"] == name), "unknown upstream tool");
            let id = self.next_id()?;
            self.calls.fetch_add(1, Ordering::Relaxed);
            if let Some(meter) = &self.meter { meter.fetch_add(1, Ordering::Relaxed); }
            connection.active_request = Some(id);
            let sent = Instant::now();
            let response = exchange(&mut connection, json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":arguments}}), deadline).await;
            // A transport/protocol failure invalidates the session. Never replay this
            // invocation: it may already have caused an external effect.
            tracing::Span::current().record("exchange_us", sent.elapsed().as_micros() as u64);
            let reply = response?;
            connection.active_request = None;
            let value = result(reply, id)?;
            ensure!(value.is_object() && value["content"].is_array(), "invalid MCP tool result");
            ensure!(value.get("isError").is_none_or(Value::is_boolean) && value.get("_meta").is_none_or(Value::is_object), "invalid MCP tool result metadata");
            *guard = Some(connection);
            Ok(value)
        }).instrument(tracing::info_span!("mcp.invoke", tool = name, queue_us = tracing::field::Empty, exchange_us = tracing::field::Empty)).await?
    }

    pub async fn close(&self) {
        let connection = self.connection.lock().await.take();
        if let Some(mut connection) = connection {
            if connection.cleanup.is_some() {
                drop(connection);
                return;
            }
            if let Wire::Http {
                client,
                url,
                headers,
                session,
            } = &mut connection.wire
            {
                if let Some(session) = session.take() {
                    let _ = client
                        .delete(url.clone())
                        .headers(headers.as_ref().clone())
                        .header("MCP-Session-Id", session)
                        .header("MCP-Protocol-Version", &connection.version)
                        .timeout(Duration::from_secs(2))
                        .send()
                        .await;
                }
            }
        }
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        if let (
            Some(cleanup),
            Wire::Http {
                client,
                url,
                headers,
                session,
            },
        ) = (&self.cleanup, &mut self.wire)
        {
            if session.is_some() || self.active_request.is_some() {
                cleanup.enqueue(crate::upstream_cleanup::Action {
                    client: client.clone(),
                    url: url.clone(),
                    headers: headers.as_ref().clone(),
                    session: session.take(),
                    version: self.version.clone(),
                    request_id: self.active_request.take(),
                });
            }
        }
    }
}

fn next_id(sequence: &AtomicU64) -> Result<u64> {
    sequence
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| anyhow::anyhow!("MCP request IDs exhausted"))
}

fn result(reply: Option<Value>, id: u64) -> Result<Value> {
    let reply = reply.context("missing JSON-RPC response")?;
    ensure!(
        reply["jsonrpc"] == "2.0"
            && reply["id"] == id
            && reply.get("result").is_some() != reply.get("error").is_some(),
        "invalid JSON-RPC response correlation"
    );
    if reply.get("error").is_some() {
        bail!("upstream returned a JSON-RPC error");
    }
    reply
        .get("result")
        .cloned()
        .context("missing JSON-RPC result")
}

async fn exchange(
    connection: &mut Connection,
    message: Value,
    deadline: Instant,
) -> Result<Option<Value>> {
    ensure!(
        serde_json::to_vec(&message)?.len() <= MAX_MESSAGE,
        "outgoing MCP message exceeds limit"
    );
    match &mut connection.wire {
        Wire::Stdio(process) => process.call(message, deadline).await,
        Wire::Http {
            client,
            url,
            headers,
            session,
        } => {
            let notification = message.get("id").is_none();
            let initializing = message["method"] == "initialize";
            let mut request = client
                .post(url.clone())
                .headers(headers.as_ref().clone())
                .header("Accept", "application/json, text/event-stream")
                .json(&message);
            if !initializing {
                request = request.header("MCP-Protocol-Version", &connection.version);
            }
            if let Some(session) = session.as_ref() {
                request = request.header("MCP-Session-Id", session);
            }
            let response = timeout_at(deadline, request.send()).await??;
            if response.status() == StatusCode::NOT_FOUND && session.is_some() {
                bail!("upstream session expired; invocation was not replayed");
            }
            ensure!(
                response.status().is_success(),
                "upstream HTTP status {}",
                response.status().as_u16()
            );
            if initializing {
                if let Some(value) = response.headers().get("MCP-Session-Id") {
                    let value = value.to_str()?;
                    ensure!(
                        !value.is_empty()
                            && value.len() <= 1024
                            && value.bytes().all(|b| (0x21..=0x7e).contains(&b)),
                        "invalid upstream session ID"
                    );
                    *session = Some(value.into());
                }
            }
            if notification {
                ensure!(
                    response.status() == StatusCode::ACCEPTED,
                    "MCP notification was not accepted"
                );
                return Ok(None);
            }
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase();
            ensure!(
                content_type == "application/json" || content_type == "text/event-stream",
                "unsupported upstream content type"
            );
            if let Some(length) = response.content_length() {
                ensure!(
                    length <= MAX_MESSAGE as u64,
                    "upstream response exceeds limit"
                );
            }
            let mut stream = response.bytes_stream();
            let mut bytes = Vec::new();
            let mut received = 0usize;
            while let Some(chunk) = timeout_at(deadline, stream.next()).await? {
                let chunk = chunk?;
                received = received.saturating_add(chunk.len());
                ensure!(received <= MAX_MESSAGE, "upstream response exceeds limit");
                bytes.extend_from_slice(&chunk);
                if content_type == "text/event-stream" {
                    if let Some(reply) =
                        parse_sse(&mut bytes, message.get("id").expect("request ID"))?
                    {
                        return Ok(Some(reply));
                    }
                }
            }
            if content_type == "application/json" {
                return Ok(Some(wayframe_codec::parse_unique(&bytes)?));
            }
            bail!("upstream event stream ended before its response; invocation was not replayed")
        }
    }
}

fn parse_sse(buffer: &mut Vec<u8>, expected: &Value) -> Result<Option<Value>> {
    loop {
        let lf = buffer
            .windows(2)
            .position(|window| window == b"\n\n")
            .map(|index| (index, 2));
        let crlf = buffer
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| (index, 4));
        let delimiter = [lf, crlf]
            .into_iter()
            .flatten()
            .min_by_key(|(index, _)| *index);
        let Some((end, width)) = delimiter else {
            return Ok(None);
        };
        let event = std::str::from_utf8(&buffer[..end])?.trim_start_matches('\u{feff}');
        let data = event
            .lines()
            .filter_map(|line| {
                line.strip_prefix("data:")
                    .map(|value| value.strip_prefix(' ').unwrap_or(value))
            })
            .collect::<Vec<_>>()
            .join("\n");
        let reply = if data.is_empty() {
            None
        } else {
            Some(wayframe_codec::parse_unique(data.as_bytes())?)
        };
        buffer.drain(..end + width);
        if let Some(reply) = reply {
            ensure!(reply["jsonrpc"] == "2.0", "invalid JSON-RPC SSE event");
            if reply.get("method").is_some() {
                ensure!(
                    reply.get("id").is_none(),
                    "upstream client callbacks are unsupported"
                );
                continue;
            }
            ensure!(
                reply.get("id") == Some(expected),
                "unexpected SSE response ID"
            );
            return Ok(Some(reply));
        }
    }
}

/// Stable error category without URLs, headers, credentials or response payloads.
pub(crate) fn failure_kind(error: &anyhow::Error) -> &'static str {
    for cause in error.chain() {
        if let Some(error) = cause.downcast_ref::<std::io::Error>() {
            match error.kind() {
                std::io::ErrorKind::ConnectionReset => return "connection_reset",
                std::io::ErrorKind::ConnectionAborted => return "connection_aborted",
                std::io::ErrorKind::UnexpectedEof => return "unexpected_eof",
                _ => {}
            }
        }
    }
    if let Some(error) = error.downcast_ref::<reqwest::Error>() {
        if error.is_timeout() {
            "http_timeout"
        } else if error.is_connect() {
            "http_connect"
        } else if error.is_body() {
            "http_body"
        } else if error.is_request() {
            "http_request"
        } else {
            "http_protocol"
        }
    } else if error
        .downcast_ref::<tokio::time::error::Elapsed>()
        .is_some()
    {
        "deadline"
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        "io"
    } else {
        "mcp_protocol"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fragmented_sse_notifications_and_multiline_data() {
        let mut bytes = b": heartbeat\r\n\r\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\ndata: {\"jsonrpc\":\"2.0\",\n".to_vec();
        assert!(parse_sse(&mut bytes, &json!(9)).unwrap().is_none());
        bytes.extend_from_slice(b"data: \"id\":9,\"result\":{}}\n\n");
        assert_eq!(
            parse_sse(&mut bytes, &json!(9)).unwrap().unwrap()["result"],
            json!({})
        );
    }
    #[test]
    fn rejects_callbacks_and_wrong_ids() {
        for event in [
            "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sampling/createMessage\"}\n\n",
            "data: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\n\n",
        ] {
            assert!(parse_sse(&mut event.as_bytes().to_vec(), &json!(1)).is_err());
        }
    }
}
