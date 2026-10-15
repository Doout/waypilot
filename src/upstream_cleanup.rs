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
                                let success = tokio::time::timeout(Duration::from_secs(2), action.run()).await.unwrap_or(false);
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
                .send().await.is_ok_and(|response| response.status().is_success());
        }
        if self.session.is_some() {
            success &= self
                .client
                .delete(self.url)
                .headers(headers)
                .send()
                .await
                .is_ok_and(|response| {
                    response.status().is_success()
                        || response.status().as_u16() == 404
                        || response.status().as_u16() == 405
                });
        }
        success
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
