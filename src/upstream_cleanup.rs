//! Bounded best-effort MCP cancellation and session deletion. Never replays tools.
use reqwest::{header::HeaderMap, Client, Url};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinSet, time::Instant};

pub(crate) struct Action {
    pub client: Client,
    pub url: Url,
    pub headers: HeaderMap,
    pub session: Option<String>,
    pub version: String,
    pub request_id: Option<u64>,
}
#[derive(Default)]
struct Stats {
    pending: AtomicU64,
    active: AtomicU64,
    dropped: AtomicU64,
    errors: AtomicU64,
    completed: AtomicU64,
}
pub(crate) struct Cleanup {
    sender: mpsc::Sender<Action>,
    receiver: Mutex<Option<mpsc::Receiver<Action>>>,
    stats: Arc<Stats>,
}
impl Cleanup {
    pub fn new(capacity: usize) -> Arc<Self> {
        let (sender, receiver) = mpsc::channel(capacity);
        Arc::new(Self {
            sender,
            receiver: Mutex::new(Some(receiver)),
            stats: Arc::default(),
        })
    }
    pub fn enqueue(&self, action: Action) {
        self.stats.pending.fetch_add(1, Ordering::Relaxed);
        if self.sender.try_send(action).is_err() {
            self.stats.pending.fetch_sub(1, Ordering::Relaxed);
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let mut receiver = self
            .receiver
            .lock()
            .expect("cleanup receiver poisoned")
            .take()
            .expect("start cleanup once");
        let stats = self.stats.clone();
        tokio::spawn(async move {
            let mut jobs = JoinSet::new();
            loop {
                tokio::select! {
                    item = receiver.recv(), if jobs.len() < 32 => match item {
                        Some(action) => {
                            let stats = stats.clone();
                            stats.active.fetch_add(1, Ordering::Relaxed);
                            jobs.spawn(async move {
                                let _guard = Completion(stats.clone());
                                let success = tokio::time::timeout(Duration::from_secs(2), action.run()).await.unwrap_or_else(|_| { tracing::error!(phase = "deadline", "upstream cleanup failed"); false });
                                if !success { stats.errors.fetch_add(1, Ordering::Relaxed); }
                            });
                        },
                        None => break,
                    },
                    _ = jobs.join_next(), if !jobs.is_empty() => {},
                }
            }
            while jobs.join_next().await.is_some() {}
        })
    }
    pub async fn drain(&self, deadline: Instant) -> bool {
        while self.stats.pending.load(Ordering::Acquire) != 0 {
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        true
    }
    pub fn stats(&self) -> serde_json::Value {
        let s = &self.stats;
        json!({"pending":s.pending.load(Ordering::Relaxed),"active":s.active.load(Ordering::Relaxed),"dropped":s.dropped.load(Ordering::Relaxed),"errors":s.errors.load(Ordering::Relaxed),"completed":s.completed.load(Ordering::Relaxed)})
    }
}
struct Completion(Arc<Stats>);
impl Drop for Completion {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
        self.0.pending.fetch_sub(1, Ordering::Release);
        self.0.completed.fetch_add(1, Ordering::Relaxed);
    }
}
impl Action {
    async fn run(self) -> bool {
        let mut headers = self.headers;
        if let Some(session) = &self.session {
            let Ok(value) = session.parse() else {
                return false;
            };
            headers.insert("MCP-Session-Id", value);
        }
        let Ok(version) = self.version.parse() else {
            return false;
        };
        headers.insert("MCP-Protocol-Version", version);
        let mut success = true;
        if let Some(id) = self.request_id {
            success = self.client.post(self.url.clone()).headers(headers.clone())
                .header("Accept", "application/json, text/event-stream")
                .json(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":id}}))
                .send().await.map_or_else(|_| { tracing::error!(phase = "cancel_transport", "upstream cleanup failed"); false }, |response| {
                    // A session-qualified 404 means the session is already gone;
                    // neither its request nor the session needs further cancellation.
                    let success = response.status().is_success()
                        || (self.session.is_some() && response.status().as_u16() == 404);
                    if !success { tracing::error!(phase = "cancel", status = response.status().as_u16(), "upstream cleanup failed"); }
                    success
                });
        }
        if self.session.is_some() {
            success &= self
                .client
                .delete(self.url)
                .headers(headers)
                .send()
                .await
                .map_or_else(
                    |_| {
                        tracing::error!(phase = "delete_transport", "upstream cleanup failed");
                        false
                    },
                    |response| {
                        let success = response.status().is_success()
                            || response.status().as_u16() == 404
                            || response.status().as_u16() == 405;
                        if !success {
                            tracing::error!(
                                phase = "delete",
                                status = response.status().as_u16(),
                                "upstream cleanup failed"
                            );
                        }
                        success
                    },
                );
        }
        success
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn expired_sessions_are_terminal_but_authorization_failures_are_reported() {
        use axum::{http::StatusCode, routing::post, Router};
        for (status, session, expected) in [
            (StatusCode::NOT_FOUND, Some("expired".to_string()), 0),
            (StatusCode::NOT_FOUND, None, 1),
            (StatusCode::UNAUTHORIZED, Some("denied".to_string()), 1),
        ] {
            let app = Router::new().route(
                "/mcp",
                post(move || async move { status }).delete(move || async move { status }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let cleanup = Cleanup::new(2);
            let worker = cleanup.start();
            cleanup.enqueue(Action {
                client: Client::builder().no_proxy().build().unwrap(),
                url: format!("http://{address}/mcp").parse().unwrap(),
                headers: HeaderMap::new(),
                session,
                version: "2025-11-25".into(),
                request_id: Some(7),
            });
            assert!(cleanup.drain(Instant::now() + Duration::from_secs(2)).await);
            assert_eq!(cleanup.stats()["errors"], expected);
            assert_eq!(cleanup.stats()["completed"], 1);
            assert_eq!(cleanup.stats()["pending"], 0);
            worker.abort();
            server.abort();
        }
    }
    #[tokio::test]
    async fn overflow_is_bounded_and_reported() {
        let cleanup = Cleanup::new(1);
        for _ in 0..2 {
            cleanup.enqueue(Action {
                client: Client::new(),
                url: "http://127.0.0.1:1/mcp".parse().unwrap(),
                headers: HeaderMap::new(),
                session: Some("owned".into()),
                version: "2025-11-25".into(),
                request_id: None,
            });
        }
        let stats = cleanup.stats();
        assert_eq!(stats["pending"], 1);
        assert_eq!(stats["dropped"], 1);
        assert_eq!(stats["active"], 0);
    }
}
