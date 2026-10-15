//! Bounded JSONL MCP ingress for local clients.
use super::{rpc_error, valid_request, Gateway, Session};
use crate::upstream::{MAX_MESSAGE, VERSIONS};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

/// Local stdio uses the operator-selected principal and the same controls and
/// compiled graphs as HTTP. stdout contains only MCP JSON-RPC messages.
pub async fn serve_stdio(gateway: Arc<Gateway>, user: &str) -> Result<()> {
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;
    use tokio_util::codec::{FramedRead, LinesCodec};
    let actor = gateway
        .actors
        .iter()
        .find(|actor| actor.settings.id == user && actor.enabled.load(Ordering::Acquire))
        .context("unknown or disabled stdio principal")?
        .clone();
    let mut session: Option<Arc<Session>> = None;
    let mut input = FramedRead::new(
        tokio::io::stdin(),
        LinesCodec::new_with_max_length(MAX_MESSAGE),
    );
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Value>(32);
    let writer = tokio::spawn(async move {
        let mut output = tokio::io::stdout();
        while let Some(value) = receiver.recv().await {
            let mut bytes = serde_json::to_vec(&value)?;
            if bytes.len() > MAX_MESSAGE {
                bytes = serde_json::to_vec(&rpc_error(
                    value["id"].clone(),
                    -32002,
                    "response exceeds limit",
                ))?;
            }
            bytes.push(b'\n');
            tokio::time::timeout(Duration::from_secs(5), output.write_all(&bytes)).await??;
            output.flush().await?;
        }
        Ok::<_, anyhow::Error>(())
    });
    let mut jobs = tokio::task::JoinSet::new();
    while let Some(line) = input.next().await {
        let line = line?;
        while jobs.try_join_next().is_some() {}
        let value = match wayframe_codec::parse_unique(line.as_bytes()) {
            Ok(value) => value,
            Err(_) => {
                sender
                    .send(rpc_error(Value::Null, -32700, "invalid JSON"))
                    .await?;
                continue;
            }
        };
        let id = value.get("id").cloned().unwrap_or(Value::Null);
        if !valid_request(&value) {
            sender
                .send(rpc_error(Value::Null, -32600, "invalid JSON-RPC request"))
                .await?;
            continue;
        }
        if value["method"] == "initialize" {
            if session.is_some() || id.is_null() {
                sender
                    .send(rpc_error(
                        id,
                        -32600,
                        "already initialized or missing request ID",
                    ))
                    .await?;
                continue;
            }
            let version = value
                .pointer("/params/protocolVersion")
                .and_then(Value::as_str)
                .filter(|value| VERSIONS.contains(value))
                .unwrap_or(VERSIONS[0]);
            session = Some(gateway.create_session(actor.clone(), version)?);
            sender.send(json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":version,"capabilities":{"tools":{}},"serverInfo":{"name":"waypilot","version":env!("CARGO_PKG_VERSION")}}})).await?;
            continue;
        }
        let Some(session) = session.as_ref().cloned() else {
            sender
                .send(rpc_error(id, -32000, "initialize first"))
                .await?;
            continue;
        };
        if value.get("id").is_none() {
            gateway.message(session, value, None).await;
            continue;
        }
        if jobs.len() >= 32 {
            sender
                .send(rpc_error(id, -32029, "stdio request capacity exceeded"))
                .await?;
            continue;
        }
        let gateway = gateway.clone();
        let sender = sender.clone();
        jobs.spawn(async move {
            if let Some(reply) = gateway.message(session, value, None).await {
                let _ = sender.send(reply).await;
            }
        });
    }
    if let Some(session) = session {
        session.cancel();
    }
    while jobs.join_next().await.is_some() {}
    drop(sender);
    writer.await??;
    gateway.shutdown().await;
    Ok(())
}
