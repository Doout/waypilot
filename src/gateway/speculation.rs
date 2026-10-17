//! Opt-in next-call nomination. Learned frequency never grants permission.
use super::*;
use crate::config::Speculation;
use serde_json::Map;

#[derive(Default)]
pub(super) struct SessionState {
    previous: Vec<(usize, Value)>,
    pub(super) learning: learning::SessionLearning,
    pub(super) epoch: u64,
    pending: Option<Pending>,
}
struct Pending {
    key: String,
    epoch: u64,
    cancel: CancellationToken,
    metrics: Arc<Counters>,
    selected: bool,
    trial: Option<learning::Trial>,
}
impl Drop for Pending {
    fn drop(&mut self) {
        if !self.selected {
            self.metrics.unused.fetch_add(1, Ordering::Relaxed);
            self.cancel.cancel();
        }
    }
}
pub(super) struct Selection(CancellationToken, Option<learning::Trial>);
impl Selection {
    pub fn finish(&mut self, outcome: &Outcome, wait_us: f64) {
        if let Some(trial) = &mut self.1 {
            trial.finish(
                outcome.result["isError"] != true
                    && matches!(outcome.disposition, "reused" | "joined"),
                wait_us,
            );
        }
    }
}
impl Drop for Selection {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
#[derive(Default)]
struct Counts {
    seen: u64,
    matched: u64,
}
#[derive(Default)]
struct Counters {
    started: AtomicU64,
    selected: AtomicU64,
    reused: AtomicU64,
    unused: AtomicU64,
    errors: AtomicU64,
    skipped: AtomicU64,
    active: AtomicU64,
}
pub(super) struct Speculator {
    slots: Arc<Semaphore>,
    profiles: Mutex<BTreeMap<String, (u64, Vec<Counts>)>>,
    metrics: Arc<Counters>,
    pub(super) learner: Option<Arc<learning::Learner>>,
}
impl Speculator {
    pub fn new(config: &Config, decisions: BTreeMap<String, bool>) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(
                config.speculation.as_ref().map_or(0, |c| c.max_in_flight),
            )),
            learner: learning::Learner::new(config, decisions),
            profiles: Mutex::new(BTreeMap::new()),
            metrics: Arc::default(),
        }
    }
    pub fn stats(&self) -> Value {
        let m = &self.metrics;
        json!({"started":m.started.load(Ordering::Relaxed),"selected":m.selected.load(Ordering::Relaxed),
            "reused":m.reused.load(Ordering::Relaxed),"unused":m.unused.load(Ordering::Relaxed),
            "errors":m.errors.load(Ordering::Relaxed),"skipped":m.skipped.load(Ordering::Relaxed),
            "active":m.active.load(Ordering::Relaxed)})
    }
}
struct Active(Arc<Counters>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}
fn key(name: &str, args: &Value) -> String {
    json!([name, args]).to_string()
}
fn eligible(count: &Counts, config: &Speculation) -> bool {
    count.seen >= config.min_samples
        && u128::from(count.matched) * 100
            >= u128::from(count.seen) * u128::from(config.confidence_percent)
}
impl Gateway {
    pub(super) fn select_prediction(
        &self,
        session: &Session,
        name: &str,
        args: &Value,
    ) -> Option<Selection> {
        let mut pending = session
            .speculation
            .lock()
            .expect("speculation poisoned")
            .pending
            .take()?;
        if pending.epoch == self.epoch.load(Ordering::Acquire)
            && pending.key == key(name, args)
            && pending
                .trial
                .as_ref()
                .is_none_or(|t| !t.cancel.is_cancelled())
        {
            pending.selected = true;
            // Do not cancel selected work: the ordinary path still performs its
            // fresh version checks and may join the bounded cache singleflight.
            self.speculator
                .metrics
                .selected
                .fetch_add(1, Ordering::Relaxed);
            // The demand's guard cancels remaining work on completion or abort.
            Some(Selection(pending.cancel.clone(), pending.trial.take()))
        } else {
            None
        }
    }
    #[tracing::instrument(name = "mcp.predict", skip_all)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn predict_next(
        self: &Arc<Self>,
        session: &Arc<Session>,
        name: &str,
        args: &Value,
        outcome: &Outcome,
        deadline: tokio::time::Instant,
        epoch: u64,
        selected: bool,
        learned: Option<learning::Nomination>,
    ) {
        let Some(config) = &self.config.speculation else {
            return;
        };
        if selected && matches!(outcome.disposition, "reused" | "joined") {
            self.speculator
                .metrics
                .reused
                .fetch_add(1, Ordering::Relaxed);
        }
        let mut state = session.speculation.lock().expect("speculation poisoned");
        if state.epoch != epoch {
            state.previous.clear();
            state.pending.take();
            state.epoch = epoch;
        }
        let mut profiles = self
            .speculator
            .profiles
            .lock()
            .expect("prediction profiles poisoned");
        let (generation, counts) = profiles
            .entry(session.actor.settings.id.clone())
            .or_insert_with(|| (epoch, vec![]));
        if *generation != epoch {
            *generation = epoch;
            counts.clear();
        }
        counts.resize_with(config.rules.len(), Counts::default);
        for (index, expected) in state.previous.drain(..) {
            let count = &mut counts[index];
            count.seen = count.seen.saturating_add(1);
            if config.rules[index].tool == name && expected == *args {
                count.matched = count.matched.saturating_add(1);
            }
        }
        let mut bytes = 0;
        for (index, rule) in config
            .rules
            .iter()
            .enumerate()
            .filter(|(_, r)| r.after == name)
        {
            let mut arguments = Map::new();
            let mut complete = true;
            for (field, binding) in &rule.arguments {
                let value = binding
                    .input
                    .as_ref()
                    .and_then(|p| args.pointer(p))
                    .or_else(|| {
                        binding
                            .result
                            .as_ref()
                            .and_then(|p| outcome.result.pointer(p))
                    });
                if let Some(value) = value {
                    let size = field.len() + value.to_string().len() + 4;
                    if bytes + size > 16384 {
                        complete = false;
                        break;
                    }
                    bytes += size;
                    arguments.insert(field.clone(), value.clone());
                } else {
                    complete = false;
                    break;
                }
            }
            if complete {
                let arguments = Value::Object(arguments);
                state.previous.push((index, arguments));
            }
        }
        let candidate = state
            .previous
            .iter()
            .filter(|(i, _)| eligible(&counts[*i], config))
            .filter(|(i, _)| {
                session
                    .actor
                    .settings
                    .speculative_tools
                    .contains(&config.rules[*i].tool)
                    && self.permits(&session.actor, &config.rules[*i].tool)
            })
            .max_by_key(|(i, _)| {
                u128::from(counts[*i].matched) * 10000 / u128::from(counts[*i].seen)
            })
            .map(|(i, args)| (config.rules[*i].tool.clone(), args.clone()));
        drop(profiles);
        let learned_id = learned.as_ref().map(|n| (n.id.clone(), n.revision.clone()));
        let candidate = learned.map(|n| (n.tool, n.arguments)).or(candidate);
        let Some((tool, arguments)) = candidate else {
            return;
        };
        if self.authorize(session, &tool, epoch).is_err()
            || self.draining.load(Ordering::Acquire)
            || tokio::time::Instant::now() >= deadline
        {
            return;
        }
        let Ok(slot) = self.speculator.slots.clone().try_acquire_owned() else {
            self.speculator
                .metrics
                .skipped
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        if self.admission.available_permits() <= 1 {
            self.speculator
                .metrics
                .skipped
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Ok(admission) = self.admission.clone().try_acquire_owned() else {
            return;
        };
        let server = tool
            .split_once("__")
            .expect("configured prediction prefix")
            .0;
        let Ok(client) = Client::with_connector(
            self.connectors[server].clone(),
            Some(self.metrics.upstream_calls.clone()),
        ) else {
            return;
        };
        let client = Arc::new(client);
        let trial = if let Some((id, _)) = &learned_id {
            let Some(trial) = self
                .speculator
                .learner
                .as_ref()
                .and_then(|l| l.start(id, epoch))
            else {
                return;
            };
            Some(trial)
        } else {
            None
        };
        let rule_cancel = trial.as_ref().map(|t| t.cancel.clone());
        let cancel = session.stopped.child_token();
        let isolated = Arc::new(Session {
            id: session.id.clone(),
            actor: session.actor.clone(),
            initialized: AtomicBool::new(true),
            version: session.version.clone(),
            touched: Mutex::new(Instant::now()),
            clients: BTreeMap::from([(server.into(), client.clone())]),
            jobs: Mutex::default(),
            observations: Mutex::new(state::Observations::new(self.observation_bytes.clone())),
            stopped: cancel.clone(),
            speculation: Mutex::default(),
        });
        state.pending = Some(Pending {
            key: key(&tool, &arguments),
            epoch,
            cancel: cancel.clone(),
            metrics: self.speculator.metrics.clone(),
            selected: false,
            trial,
        });
        drop(state);
        let deadline = deadline
            .min(tokio::time::Instant::now() + Duration::from_millis(config.max_duration_ms));
        let gateway = self.clone();
        self.speculator
            .metrics
            .started
            .fetch_add(1, Ordering::Relaxed);
        self.speculator
            .metrics
            .active
            .fetch_add(1, Ordering::Relaxed);
        let span = tracing::info_span!("mcp.speculate", tool = tool);
        tokio::spawn(
            async move {
                let _active = Active(gateway.speculator.metrics.clone());
                let _slot = slot;
                let _admission = admission;
                let result = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => None,
                    _ = async { if let Some(token) = &rule_cancel { token.cancelled().await } else { std::future::pending::<()>().await } } => None,
                    result = async {
                        if let Some((_, revision)) = &learned_id {
                            let catalog = client.catalog_checked(deadline, || gateway.live(&isolated, epoch)).await.map_err(|_| Fault::denied())?;
                            if &catalog.revision != revision { return Err(Fault::denied()); }
                        }
                        gateway.direct.run(gateway.clone(), isolated, tool.clone(), arguments, deadline, epoch).await
                    } => Some(result),
                };
                if result.as_ref().is_some_and(|r| r.as_ref().map_or(true, |o| o.result["isError"] == true)) {
                    gateway.speculator.metrics.errors.fetch_add(1, Ordering::Relaxed);
                }
                client.close().await;
            }.instrument(span),
        );
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frequency_needs_support_and_tracks_misses() {
        let c = Speculation {
            max_in_flight: 1,
            min_samples: 3,
            confidence_percent: 75,
            max_duration_ms: 1000,
            rules: vec![],
            learning: None,
        };
        assert!(!eligible(
            &Counts {
                seen: 2,
                matched: 2
            },
            &c
        ));
        assert!(eligible(
            &Counts {
                seen: 4,
                matched: 3
            },
            &c
        ));
        assert!(!eligible(
            &Counts {
                seen: 5,
                matched: 3
            },
            &c
        ));
    }
}
