//! Bounded adjacent-call discovery. Evidence nominates work; contracts authorize it.
use super::*;
use crate::config::{Learning, LearningMode};
use serde::Serialize;
use std::collections::VecDeque;

const CONTEXT_LIMIT: usize = 16384;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct Source {
    result: bool,
    pointer: String,
}
// Source uses explicit fields in the admin representation; no payload values survive.
#[derive(Clone)]
struct Binding {
    sources: Vec<Source>,
    first: String,
    varied: bool,
}
struct Rule {
    id: String,
    instance: u64,
    user: String,
    after: String,
    source_revision: String,
    context: Vec<String>,
    tool: String,
    target_revision: String,
    shape: Value,
    bindings: BTreeMap<String, Binding>,
    samples: VecDeque<Sample>,
    controls: VecDeque<f64>,
    feedback: VecDeque<Feedback>,
    observations: u64,
    opportunities: u64,
    holdouts: u64,
    holdout_at: u64,
    started: u64,
    reused: u64,
    unused: u64,
    retired: Option<(u64, &'static str)>,
    retirements: u32,
    recovery_progress: u64,
    recoveries: u64,
    trial_generation: u64,
    cancel: CancellationToken,
}
struct Sample {
    matched: bool,
    gap_us: f64,
}
struct Feedback {
    reused: bool,
    wait_us: f64,
    saved_us: f64,
}
struct State {
    epoch: u64,
    mode: LearningMode,
    rules: Vec<Rule>,
    decisions: BTreeMap<String, bool>,
    dropped: u64,
    next_instance: u64,
}
pub(super) struct Learner {
    config: Learning,
    min_samples: u64,
    confidence: u64,
    fingerprint: String,
    salt: String,
    bytes: Arc<Semaphore>,
    state: Mutex<State>,
}
#[derive(Default)]
pub(super) struct SessionLearning {
    previous: Option<Observation>,
    busy: usize,
    overlap: bool,
    sequence: u64,
}
struct Observation {
    name: String,
    context: Vec<String>,
    revision: String,
    sources: BTreeMap<Source, Value>,
    finished: Instant,
    _bytes: tokio::sync::OwnedSemaphorePermit,
}
pub(super) struct Demand {
    session: Arc<Session>,
    learner: Arc<Learner>,
    previous: Option<Observation>,
    started: Instant,
    sequence: u64,
    epoch: u64,
    finished: bool,
}
pub(super) struct Nomination {
    pub id: String,
    pub tool: String,
    pub arguments: Value,
    pub revision: String,
}
pub(super) struct Trial {
    learner: Arc<Learner>,
    id: String,
    epoch: u64,
    baseline_us: f64,
    cohort: (u64, u64),
    finished: bool,
    pub cancel: CancellationToken,
}
impl Drop for Trial {
    fn drop(&mut self) {
        if !self.finished {
            self.learner.feedback(
                &self.id,
                self.epoch,
                self.cohort,
                false,
                0.0,
                self.baseline_us,
            );
        }
    }
}
impl Trial {
    pub fn finish(&mut self, reused: bool, wait_us: f64) {
        self.finished = true;
        self.learner.feedback(
            &self.id,
            self.epoch,
            self.cohort,
            reused,
            wait_us,
            self.baseline_us,
        );
    }
}
impl Drop for Demand {
    fn drop(&mut self) {
        if !self.finished {
            let mut state = self
                .session
                .speculation
                .lock()
                .expect("speculation poisoned");
            let learning = &mut state.learning;
            learning.previous = None;
            learning.busy = learning.busy.saturating_sub(1);
            if learning.busy == 0 {
                learning.overlap = false;
            }
        }
    }
}
fn bounded_push<T>(values: &mut VecDeque<T>, value: T, limit: usize) {
    if values.len() == limit {
        values.pop_front();
    }
    values.push_back(value);
}
fn escape(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}
// Bound traversal as well as retained bytes. Booleans/null and literal constants
// cannot establish changing argument provenance, so they are not inferred.
fn leaves(
    value: &Value,
    path: &str,
    depth: usize,
    output: &mut BTreeMap<String, Value>,
) -> Option<()> {
    if depth > 8 || path.len() > 256 || output.len() >= 128 {
        return None;
    }
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                leaves(value, &format!("{path}/{}", escape(key)), depth + 1, output)?;
            }
        }
        Value::Array(array) => {
            for (i, value) in array.iter().enumerate() {
                leaves(value, &format!("{path}/{i}"), depth + 1, output)?;
            }
        }
        Value::String(_) | Value::Number(_) => {
            output.insert(path.into(), value.clone());
        }
        _ => {}
    }
    Some(())
}
fn shape(
    value: &Value,
    path: &str,
    depth: usize,
    output: &mut BTreeMap<String, Value>,
) -> Option<Value> {
    if depth > 8 || path.len() > 256 || output.len() >= 32 {
        return None;
    }
    Some(match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| {
                    Some((
                        key.clone(),
                        shape(value, &format!("{path}/{}", escape(key)), depth + 1, output)?,
                    ))
                })
                .collect::<Option<_>>()?,
        ),
        Value::Array(array) => Value::Array(
            array
                .iter()
                .enumerate()
                .map(|(i, value)| shape(value, &format!("{path}/{i}"), depth + 1, output))
                .collect::<Option<_>>()?,
        ),
        Value::String(_) | Value::Number(_) => {
            output.insert(path.into(), value.clone());
            Value::Null
        }
        _ => return None,
    })
}
impl Rule {
    fn arguments(&self, source: &Observation) -> Option<Value> {
        let mut args = self.shape.clone();
        for (target, binding) in &self.bindings {
            let mut values = binding.sources.iter().map(|s| source.sources.get(s));
            let value = values.next()??;
            // Ambiguous pointers are usable only while they all agree. A later
            // real call can disambiguate them without guessing an identity.
            if !values.all(|v| v == Some(value)) {
                return None;
            }
            *args.pointer_mut(target)? = value.clone();
        }
        Some(args)
    }
    fn recovery_samples(&self, learner: &Learner) -> u64 {
        learner.config.recovery_samples
    }
    fn retire(&mut self, reason: &'static str) {
        if self.retired.is_none() {
            self.retirements = self.retirements.saturating_add(1);
            self.trial_generation += 1;
        }
        self.retired = Some((self.observations, reason));
        self.recovery_progress = 0;
        self.cancel.cancel();
    }
    fn baseline(&self) -> f64 {
        self.controls.iter().sum::<f64>() / self.controls.len().max(1) as f64
    }
    fn confidence(&self) -> f64 {
        self.samples.iter().filter(|s| s.matched).count() as f64 / self.samples.len().max(1) as f64
    }
    fn estimated_saving(&self) -> f64 {
        let baseline = self.baseline();
        self.samples
            .iter()
            .map(|s| {
                if s.matched {
                    s.gap_us.min(baseline)
                } else {
                    -baseline
                }
            })
            .sum::<f64>()
            / self.samples.len().max(1) as f64
    }
    fn qualification_reason(&self, learner: &Learner) -> Option<&'static str> {
        if (self.samples.len() as u64) < learner.min_samples {
            Some("sample_count")
        } else if !self.bindings.values().all(|b| b.varied) {
            Some("argument_diversity")
        } else if self.controls.len() < 2 {
            Some("control_count")
        } else if self.confidence() * 100.0 + f64::EPSILON < learner.confidence as f64 {
            Some("confidence")
        } else if self.estimated_saving() < learner.config.min_savings_ms as f64 * 1000.0 {
            Some("expected_benefit")
        } else {
            None
        }
    }
    fn qualified(&self, learner: &Learner) -> bool {
        self.qualification_reason(learner).is_none()
    }
    fn status(&self, learner: &Learner, state: &State) -> &'static str {
        if state.decisions.get(&self.id) == Some(&false) {
            return "rejected";
        }
        if self.retired.is_some() {
            return "retired";
        }
        if !self.qualified(learner) {
            return "learning";
        }
        match state.mode {
            LearningMode::Observe => "observing",
            LearningMode::Approval if state.decisions.get(&self.id) != Some(&true) => {
                "pending_approval"
            }
            _ => "active",
        }
    }
    fn identify(&mut self, fingerprint: &str) {
        let bindings: BTreeMap<_, _> = self.bindings.iter().map(|(p, b)| (p, &b.sources)).collect();
        self.id = digest(
            &serde_json::to_vec(&(
                fingerprint,
                &self.user,
                &self.after,
                &self.source_revision,
                &self.context,
                &self.tool,
                &self.target_revision,
                &self.shape,
                bindings,
            ))
            .expect("rule identity"),
        );
    }
}
impl Learner {
    pub fn new(config: &Config, decisions: BTreeMap<String, bool>) -> Option<Arc<Self>> {
        let speculation = config.speculation.as_ref()?;
        let learning = speculation.learning.as_ref()?;
        Some(Arc::new(Self {
            config: learning.clone(),
            min_samples: speculation.min_samples,
            confidence: speculation.confidence_percent,
            fingerprint: digest(&serde_json::to_vec(config).expect("configuration identity")),
            salt: format!("{:032x}", rand::random::<u128>()),
            bytes: Arc::new(Semaphore::new(learning.context_bytes)),
            state: Mutex::new(State {
                epoch: 1,
                mode: learning.mode,
                rules: vec![],
                decisions,
                dropped: 0,
                next_instance: 0,
            }),
        }))
    }
    fn reset(state: &mut State, epoch: u64) {
        if state.epoch < epoch {
            for rule in &state.rules {
                rule.cancel.cancel();
            }
            state.rules.clear();
            state.epoch = epoch;
        }
    }
    fn value_hash(&self, value: &Value) -> String {
        digest(&serde_json::to_vec(&(&self.salt, value)).expect("value identity"))
    }
    fn observation(
        &self,
        name: &str,
        args: &Value,
        outcome: &Outcome,
        context: Vec<String>,
    ) -> Option<Observation> {
        if outcome.catalog_revision.is_empty() || name.len() > 256 {
            return None;
        }
        let result = outcome
            .result
            .get("structuredContent")
            .unwrap_or(&Value::Null);
        let size = args.to_string().len() + result.to_string().len();
        if size > CONTEXT_LIMIT {
            return None;
        }
        let mut input = BTreeMap::new();
        let mut output = BTreeMap::new();
        leaves(args, "", 0, &mut input)?;
        leaves(result, "/structuredContent", 0, &mut output)?;
        let sources: BTreeMap<_, _> = input
            .into_iter()
            .map(|(pointer, v)| {
                (
                    Source {
                        result: false,
                        pointer,
                    },
                    v,
                )
            })
            .chain(output.into_iter().map(|(pointer, v)| {
                (
                    Source {
                        result: true,
                        pointer,
                    },
                    v,
                )
            }))
            .collect();
        let bytes = sources
            .iter()
            .map(|(p, v)| p.pointer.len() + v.to_string().len() + 128)
            .sum::<usize>()
            + context.iter().map(|name| name.len() + 32).sum::<usize>()
            + name.len()
            + outcome.catalog_revision.len();
        if bytes > CONTEXT_LIMIT {
            return None;
        }
        Some(Observation {
            name: name.into(),
            context,
            revision: outcome.catalog_revision.clone(),
            sources,
            finished: Instant::now(),
            _bytes: self
                .bytes
                .clone()
                .try_acquire_many_owned(bytes.max(1) as u32)
                .ok()?,
        })
    }
    #[tracing::instrument(name = "mcp.learn", skip_all)]
    #[allow(clippy::too_many_arguments)]
    fn observe(
        &self,
        user: &str,
        epoch: u64,
        prior: &Observation,
        name: &str,
        args: &Value,
        outcome: &Outcome,
        gap_us: f64,
        elapsed_us: f64,
        selected: bool,
        eligible: bool,
    ) {
        if args.to_string().len() > CONTEXT_LIMIT {
            return;
        }
        let mut fields = BTreeMap::new();
        let target_shape = shape(args, "", 0, &mut fields);
        let inferred: Option<BTreeMap<_, _>> = fields
            .iter()
            .map(|(path, value)| {
                let sources: Vec<_> = prior
                    .sources
                    .iter()
                    .filter(|(_, v)| *v == value)
                    .map(|(p, _)| p.clone())
                    .collect();
                (!sources.is_empty() && sources.len() <= 8).then(|| {
                    (
                        path.clone(),
                        Binding {
                            sources,
                            first: self.value_hash(value),
                            varied: false,
                        },
                    )
                })
            })
            .collect();
        let mut state = self.state.lock().expect("learner poisoned");
        if state.epoch > epoch {
            return;
        }
        Self::reset(&mut state, epoch);
        let mut found = false;
        for rule in state.rules.iter_mut().filter(|r| {
            r.user == user
                && r.after == prior.name
                && r.source_revision == prior.revision
                && r.context == prior.context
        }) {
            let same = rule.tool == name
                && rule.target_revision == outcome.catalog_revision
                && Some(&rule.shape) == target_shape.as_ref();
            let compatible = same
                && inferred.as_ref().is_some_and(|new| {
                    rule.bindings.iter().all(|(path, b)| {
                        new.get(path)
                            .is_some_and(|n| b.sources.iter().any(|s| n.sources.contains(s)))
                    })
                });
            if compatible {
                found = true;
                for (path, binding) in &mut rule.bindings {
                    let new = &inferred.as_ref().expect("inference")[path];
                    binding.sources.retain(|s| new.sources.contains(s));
                    binding.varied |= binding.first != new.first;
                }
                let old_id = rule.id.clone();
                rule.identify(&self.fingerprint);
                if rule.id != old_id {
                    rule.cancel.cancel();
                    rule.cancel = CancellationToken::new();
                    rule.feedback.clear();
                    rule.started = 0;
                    rule.reused = 0;
                    rule.unused = 0;
                    rule.holdouts = 0;
                    rule.opportunities = 0;
                    rule.retired = None;
                    rule.retirements = 0;
                    rule.recovery_progress = 0;
                    rule.recoveries = 0;
                    rule.trial_generation += 1;
                    // Changed provenance needs its own approval and outcome evidence.
                }
            }
            let matched = same && rule.arguments(prior).as_ref() == Some(args);
            bounded_push(
                &mut rule.samples,
                Sample { matched, gap_us },
                self.config.window,
            );
            rule.observations += 1;
            if matched && !selected {
                bounded_push(&mut rule.controls, elapsed_us, self.config.window);
            }
            if !rule.qualified(self) {
                rule.cancel.cancel();
                if rule.started > 0 && rule.retired.is_none() {
                    rule.retire(if rule.confidence() * 100.0 < self.confidence as f64 {
                        "low_confidence"
                    } else {
                        "low_expected_benefit"
                    });
                }
            }
            // Only real, unselected demand supplies recovery evidence. Misses
            // and absent wait benefit erode that evidence; predictions cannot
            // train themselves back into service. Recent qualification gates
            // and the ordinary trial waste budget still apply.
            if rule.retired.is_some() && !selected {
                if matched && gap_us.min(elapsed_us) >= self.config.min_savings_ms as f64 * 1000.0 {
                    rule.recovery_progress =
                        (rule.recovery_progress + 1).min(rule.recovery_samples(self));
                } else {
                    rule.recovery_progress = rule.recovery_progress.saturating_sub(1);
                }
            }
            if rule.retired.is_some()
                && rule.recovery_progress >= rule.recovery_samples(self)
                && rule.qualified(self)
            {
                rule.retired = None;
                rule.feedback.clear();
                rule.cancel = CancellationToken::new();
                rule.recoveries += 1;
                rule.recovery_progress = 0;
            }
        }
        if !found && eligible && !outcome.catalog_revision.is_empty() {
            if let (Some(shape), Some(bindings)) = (target_shape, inferred) {
                if state.rules.len() == self.config.max_rules {
                    // Free exhausted automatic candidates while keeping explicit
                    // decisions as tombstones. Active or approved rules are not evicted.
                    let victim = state.rules.iter().position(|r| {
                        state.decisions.get(&r.id) != Some(&true)
                            && (state.decisions.get(&r.id) == Some(&false)
                                || r.retired.is_some()
                                || (r.samples.len() == self.config.window && !r.qualified(self)))
                    });
                    if let Some(index) = victim {
                        state.rules.remove(index).cancel.cancel();
                    } else {
                        state.dropped += 1;
                        return;
                    }
                }
                state.next_instance += 1;
                let mut rule = Rule {
                    id: String::new(),
                    instance: state.next_instance,
                    user: user.into(),
                    after: prior.name.clone(),
                    source_revision: prior.revision.clone(),
                    context: prior.context.clone(),
                    tool: name.into(),
                    target_revision: outcome.catalog_revision.clone(),
                    shape,
                    bindings,
                    samples: VecDeque::from([Sample {
                        matched: true,
                        gap_us,
                    }]),
                    controls: VecDeque::new(),
                    feedback: VecDeque::new(),
                    observations: 1,
                    opportunities: 0,
                    holdouts: 0,
                    holdout_at: 0,
                    started: 0,
                    reused: 0,
                    unused: 0,
                    retired: None,
                    retirements: 0,
                    recovery_progress: 0,
                    recoveries: 0,
                    trial_generation: 0,
                    cancel: CancellationToken::new(),
                };
                if !selected {
                    rule.controls.push_back(elapsed_us);
                }
                rule.identify(&self.fingerprint);
                state.rules.push(rule);
            }
        }
    }
    fn nominate(&self, user: &str, epoch: u64, current: &Observation) -> Option<Nomination> {
        let mut state = self.state.lock().expect("learner poisoned");
        if state.epoch > epoch {
            return None;
        }
        Self::reset(&mut state, epoch);
        let index = state
            .rules
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                r.user == user
                    && r.after == current.name
                    && r.source_revision == current.revision
                    && r.context == current.context
                    && r.status(self, &state) == "active"
                    && r.arguments(current).is_some()
            })
            .max_by(|(_, a), (_, b)| a.estimated_saving().total_cmp(&b.estimated_saving()))
            .map(|(i, _)| i)?;
        let rule = &mut state.rules[index];
        let position = rule.opportunities % self.config.holdout_every;
        if position == 0 {
            // One randomized control per block avoids synchronizing holdouts
            // with periodic branches or with another rule in the same agent.
            rule.holdout_at = rand::random_range(0..self.config.holdout_every);
        }
        rule.opportunities += 1;
        if position == rule.holdout_at {
            rule.holdouts += 1;
            return None;
        }
        Some(Nomination {
            id: rule.id.clone(),
            tool: rule.tool.clone(),
            arguments: rule.arguments(current)?,
            revision: rule.target_revision.clone(),
        })
    }
    pub fn start(self: &Arc<Self>, id: &str, epoch: u64) -> Option<Trial> {
        let mut state = self.state.lock().expect("learner poisoned");
        if state.epoch != epoch {
            return None;
        }
        let index = state
            .rules
            .iter()
            .position(|r| r.id == id && r.status(self, &state) == "active")?;
        let rule = &mut state.rules[index];
        if rule.cancel.is_cancelled() {
            rule.cancel = CancellationToken::new();
        }
        rule.started += 1;
        Some(Trial {
            learner: self.clone(),
            id: id.into(),
            epoch,
            baseline_us: rule.baseline(),
            cohort: (rule.instance, rule.trial_generation),
            finished: false,
            cancel: rule.cancel.child_token(),
        })
    }
    fn feedback(
        &self,
        id: &str,
        epoch: u64,
        cohort: (u64, u64),
        reused: bool,
        wait_us: f64,
        baseline_us: f64,
    ) {
        let mut state = self.state.lock().expect("learner poisoned");
        if state.epoch != epoch {
            return;
        }
        let Some(rule) = state.rules.iter_mut().find(|r| r.id == id) else {
            return;
        };
        if cohort.0 != rule.instance {
            return;
        }
        if reused {
            rule.reused += 1;
        } else {
            rule.unused += 1;
        }
        if cohort.1 != rule.trial_generation {
            return;
        }
        let saved_us = if reused {
            baseline_us - wait_us
        } else {
            -baseline_us
        };
        bounded_push(
            &mut rule.feedback,
            Feedback {
                reused,
                wait_us,
                saved_us,
            },
            self.config.window,
        );
        if rule.feedback.len() >= self.config.evaluation_samples {
            let waste = rule.feedback.iter().filter(|f| !f.reused).count() as f64
                / rule.feedback.len() as f64
                * 100.0;
            let saving =
                rule.feedback.iter().map(|f| f.saved_us).sum::<f64>() / rule.feedback.len() as f64;
            if waste > self.config.max_waste_percent as f64
                || saving < self.config.min_savings_ms as f64 * 1000.0
            {
                rule.retire(if waste > self.config.max_waste_percent as f64 {
                    "waste_budget"
                } else {
                    "no_wait_benefit"
                });
            }
        }
    }
    pub fn has_rule(&self, id: &str, epoch: u64) -> bool {
        let mut state = self.state.lock().expect("learner poisoned");
        if state.epoch > epoch {
            return false;
        }
        Self::reset(&mut state, epoch);
        state.rules.iter().any(|r| r.id == id)
    }
    pub fn decide(&self, id: &str, approved: bool) {
        let mut state = self.state.lock().expect("learner poisoned");
        state.decisions.insert(id.into(), approved);
        if !approved {
            for rule in state.rules.iter().filter(|r| r.id == id) {
                rule.cancel.cancel();
            }
        }
    }
    pub fn mode(&self, mode: LearningMode) {
        let mut state = self.state.lock().expect("learner poisoned");
        state.mode = mode;
        for rule in &state.rules {
            if rule.status(self, &state) != "active" {
                rule.cancel.cancel();
            }
        }
    }
    pub fn report(&self, epoch: u64) -> Value {
        let mut state = self.state.lock().expect("learner poisoned");
        Self::reset(&mut state, epoch);
        let rules: Vec<_> = state.rules.iter().map(|r| {
            let bindings: BTreeMap<_,_> = r.bindings.iter().map(|(p,b)| (p, json!({"sources":b.sources,"varied":b.varied}))).collect();
            json!({"id":r.id,"user":r.user,"after":r.after,"context":r.context,"tool":r.tool,"arguments":bindings,
                "argument_shape":r.shape,"status":r.status(self,&state),"qualification_reason":r.qualification_reason(self),"approved":state.decisions.get(&r.id),"retired_reason":r.retired.map(|(_,why)|why),
                "samples":r.samples.len(),"matches":r.samples.iter().filter(|s|s.matched).count(),
                "confidence_percent":r.confidence()*100.0,"control_samples":r.controls.len(),
                "baseline_mean_ms":r.baseline()/1000.0,"estimated_net_saving_ms":r.estimated_saving()/1000.0,
                "retirements":r.retirements,"recoveries":r.recoveries,"recovery_progress":r.recovery_progress,"recovery_observations_required":r.recovery_samples(self),
                "prediction_samples":r.feedback.len(),"started":r.started,"reused":r.reused,"unused":r.unused,"holdouts":r.holdouts,
                "mean_demand_wait_ms":r.feedback.iter().filter(|f|f.reused).map(|f|f.wait_us).sum::<f64>() / r.feedback.iter().filter(|f|f.reused).count().max(1) as f64 / 1000.0,
                "baseline_relative_net_saving_ms":r.feedback.iter().map(|f|f.saved_us).sum::<f64>()/r.feedback.len().max(1) as f64/1000.0})
        }).collect();
        json!({"mode":state.mode,"generation":state.epoch,"rules":rules,"dropped_candidates":state.dropped,
            "context_bytes":self.config.context_bytes-self.bytes.available_permits(),"limits":self.config,
            "min_samples":self.min_samples,"confidence_threshold_percent":self.confidence,
            "estimates":"Shadow estimates use observed call gaps and unselected demand controls, including cache hits. Outcome savings compare demand wait to recent controls and charge a full control cost for unused predictions; they are not a causal end-to-end speedup."})
    }
}
impl Gateway {
    pub(super) fn begin_learning(
        self: &Arc<Self>,
        session: &Arc<Session>,
        epoch: u64,
    ) -> Option<Demand> {
        let learner = self.speculator.learner.as_ref()?.clone();
        let mut state = session.speculation.lock().expect("speculation poisoned");
        if state.epoch != epoch {
            state.learning.previous = None;
        }
        let learning = &mut state.learning;
        learning.busy += 1;
        learning.sequence += 1;
        if learning.busy > 1 {
            learning.overlap = true;
        }
        let previous = learning.previous.take();
        Some(Demand {
            session: session.clone(),
            learner,
            previous,
            started: Instant::now(),
            sequence: learning.sequence,
            epoch,
            finished: false,
        })
    }
    pub(super) fn learning_eligible(&self, session: &Session, name: &str) -> bool {
        session.actor.settings.speculative_tools.contains(name)
            && self.permits(&session.actor, name)
            && name
                .split_once("__")
                .and_then(|(server, tool)| {
                    self.config
                        .servers
                        .iter()
                        .find(|s| s.id == server)?
                        .tools
                        .get(tool)
                })
                .is_some_and(|r| {
                    r.speculative && r.reusable && r.cache_ttl_ms > 0 && r.state_tool.is_some()
                })
    }
}
impl Demand {
    #[tracing::instrument(name = "mcp.learning", skip_all)]
    pub fn finish(
        mut self,
        gateway: &Gateway,
        name: &str,
        args: &Value,
        outcome: &Outcome,
        selected: bool,
    ) -> Option<Nomination> {
        self.finished = true;
        let mut state = self
            .session
            .speculation
            .lock()
            .expect("speculation poisoned");
        let learning = &mut state.learning;
        let valid = learning.busy == 1
            && !learning.overlap
            && learning.sequence == self.sequence
            && gateway.epoch.load(Ordering::Acquire) == self.epoch
            && !self.session.stopped.is_cancelled();
        learning.busy = learning.busy.saturating_sub(1);
        if learning.busy == 0 {
            learning.overlap = false;
        }
        if !valid {
            return None;
        }
        let context = self.previous.as_ref().map_or_else(Vec::new, |prior| {
            let mut context = prior.context.clone();
            context.push(prior.name.clone());
            let remove = context
                .len()
                .saturating_sub(self.learner.config.context_depth);
            context.drain(..remove);
            context
        });
        if let Some(prior) = self.previous.take() {
            self.learner.observe(
                &self.session.actor.settings.id,
                self.epoch,
                &prior,
                name,
                args,
                outcome,
                self.started
                    .saturating_duration_since(prior.finished)
                    .as_secs_f64()
                    * 1e6,
                self.started.elapsed().as_secs_f64() * 1e6,
                selected,
                gateway.learning_eligible(&self.session, name),
            );
        }
        let current = self.learner.observation(name, args, outcome, context);
        let nomination = current.as_ref().and_then(|current| {
            self.learner
                .nominate(&self.session.actor.settings.id, self.epoch, current)
        });
        learning.previous = current;
        nomination
    }
}

#[cfg(test)]
mod tests;
