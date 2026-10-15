use crate::{
    cache::Cache,
    mcp::{digest, Counts, Fixture, Pool},
    predictor::Predictor,
};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tracing::Instrument;
use wayframe_core::{Context, Decision, Phase, Reason};
use wayframe_flow::{Builder, Error, Flow, Limits, Port, Priority};

#[derive(Clone, Debug, Serialize)]
pub struct Scope {
    tenant: String,
    subject: String,
}
impl Scope {
    pub fn new(tenant: &str, subject: &str) -> Result<Self> {
        ensure!(
            [tenant, subject]
                .iter()
                .all(|s| !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control)),
            "invalid identity scope"
        );
        Ok(Self {
            tenant: tenant.into(),
            subject: subject.into(),
        })
    }
}

/// Demonstration authorization handle, supplied by the embedding application.
/// This does not authenticate users or replace a production identity provider.
#[derive(Clone)]
pub struct Permit {
    scope: Scope,
    allowed: Arc<AtomicBool>,
}
impl Permit {
    pub fn new(scope: Scope) -> Self {
        Self {
            scope,
            allowed: Arc::new(AtomicBool::new(true)),
        }
    }
    pub fn revoke(&self) {
        self.allowed.store(false, Ordering::Release);
    }
    fn check(&self) -> Result<(), Error> {
        if self.allowed.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Error::Denied(Reason::Forbidden, "access_revoked"))
        }
    }
}

/// Immutable input snapshot. Fixture tools consume these exact bytes, not mutable files.
#[derive(Clone)]
pub struct Project {
    pub manifest: String,
    pub source: String,
}

#[derive(Clone)]
pub struct Settings {
    pub reuse: bool,
    pub ttl: Duration,
    pub max_parallel: usize,
    pub contract_revision: String,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            reuse: true,
            ttl: Duration::from_secs(60),
            max_parallel: 2,
            contract_revision: "project-inspection/v1".into(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Branch {
    pub id: &'static str,
    pub disposition: &'static str,
    pub elapsed_us: u128,
    pub output: Value,
}

#[derive(Serialize)]
pub struct NodeTiming {
    pub id: String,
    pub state: String,
    pub dependency_us: u128,
    pub queue_us: u128,
    pub execution_us: u128,
}

#[derive(Serialize)]
pub struct Report {
    pub elapsed_us: u128,
    pub counts: Counts,
    pub branches: Vec<Branch>,
    pub nodes: Vec<NodeTiming>,
}

struct Shared {
    pool: Pool,
    cache: Mutex<Cache>,
    settings: Settings,
    revision: String,
}

struct Work {
    deadline: Instant,
    permit: Permit,
    project: Project,
    shared: Arc<Shared>,
}
impl Context for Work {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}

pub struct Engine {
    shared: Arc<Shared>,
    predictor: Mutex<Predictor>,
    flow: Flow<Work>,
    output: Port<Vec<Branch>>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    revision: String,
    predictor: Predictor,
}

impl Engine {
    pub fn new(fixture: Fixture, settings: Settings) -> Result<Self> {
        ensure!(
            (1..=2).contains(&settings.max_parallel),
            "parallelism must be 1 or 2"
        );
        ensure!(
            !settings.contract_revision.is_empty() && settings.contract_revision.len() <= 128,
            "invalid contract revision"
        );
        ensure!(
            settings.ttl <= Duration::from_secs(3600),
            "TTL exceeds limit"
        );
        let shared = Arc::new(Shared {
            revision: format!("{}:{}", settings.contract_revision, fixture.revision()),
            pool: Pool::new(fixture),
            cache: Mutex::new(Cache::new(128, 4 * 1024 * 1024)),
            settings,
        });
        let mut builder = Builder::<Work>::new()
            .limits(Limits {
                max_parallel: shared.settings.max_parallel,
                // Until sessions have independent consumer leases, a canceled run
                // must not terminate a subprocess used by another authoritative run.
                max_executions: 1,
                max_running_nodes: 64,
            })
            .priority(Priority::Measured);
        let root = builder.input();
        let admission = builder.gate("authorize", Phase::Authorize, &root, |work| {
            decision(&work.permit)
        })?;
        let dependencies =
            builder.asynchronous("dependencies", Phase::Authorize, &root, |work| async move {
                branch(work, "dependencies").await
            })?;
        let source =
            builder.asynchronous("source", Phase::Authorize, &root, |work| async move {
                branch(work, "source").await
            })?;
        // Tool calls have effects on external observers, so explicitly order them after admission.
        builder.depends_on(&dependencies, &admission)?;
        builder.depends_on(&source, &admission)?;
        let joined = builder.join(
            "collect",
            Phase::Authorize,
            &dependencies,
            &source,
            |a, b| Ok(vec![(*a).clone(), (*b).clone()]),
        )?;
        let current = builder.gate("authorize_release", Phase::Authorize, &root, |work| {
            decision(&work.permit)
        })?;
        builder.depends_on(&current, &joined)?;
        let output = builder.dispatch("release", &joined, |branches| async move {
            Ok((*branches).clone())
        })?;
        builder.depends_on(&output, &admission)?;
        builder.depends_on(&output, &current)?;
        Ok(Self {
            shared,
            predictor: Mutex::new(Predictor::default()),
            flow: builder.compile()?,
            output,
        })
    }

    pub fn counts(&self) -> Counts {
        self.shared.pool.counters.snapshot()
    }

    pub fn predictions(&self) -> Vec<&'static str> {
        self.predictor.lock().expect("predictor poisoned").predict()
    }

    /// Aggregate server-use observations, excluding arguments, results and identities.
    pub fn profile(&self) -> Value {
        serde_json::to_value(Profile {
            revision: self.shared.revision.clone(),
            predictor: self.predictor.lock().expect("predictor poisoned").clone(),
        })
        .expect("serializable profile")
    }

    pub fn load_profile(&self, profile: Value) -> Result<()> {
        ensure!(
            profile.to_string().len() <= 4096,
            "prediction profile exceeds limit"
        );
        let profile: Profile = serde_json::from_value(profile)?;
        ensure!(
            profile.revision == self.shared.revision,
            "profile belongs to another runtime generation"
        );
        profile.predictor.validate()?;
        *self.predictor.lock().expect("predictor poisoned") = profile.predictor;
        Ok(())
    }

    /// Only prepares environments. No tools/call is issued by this method.
    pub async fn prewarm_predictions(&self, permit: &Permit, budget: Duration) -> Result<usize> {
        permit.check()?;
        ensure!(
            budget > Duration::ZERO && budget <= Duration::from_secs(10),
            "invalid prewarm budget"
        );
        let roles = self.predictions();
        let deadline = tokio::time::Instant::now() + budget;
        let results = futures_util::future::join_all(
            roles
                .iter()
                .map(|role| self.shared.pool.prewarm(role, deadline)),
        )
        .await;
        for result in results {
            result?;
        }
        permit.check()?;
        Ok(roles.len())
    }

    pub async fn run(&self, permit: Permit, project: Project, budget: Duration) -> Result<Report> {
        ensure!(
            project.manifest.len() <= 8192 && project.source.len() <= 8192,
            "input snapshot exceeds limit"
        );
        ensure!(
            budget > Duration::ZERO && budget <= Duration::from_secs(60),
            "invalid run budget"
        );
        let started = Instant::now();
        let before = self.counts();
        let work = Arc::new(Work {
            deadline: started + budget,
            permit,
            project,
            shared: self.shared.clone(),
        });
        let run = self
            .flow
            .run(work.clone())
            .instrument(tracing::info_span!(
                "waypilot.run",
                contract = self.shared.settings.contract_revision
            ))
            .await?;
        work.permit.check()?;
        if Instant::now() >= work.deadline {
            return Err(Error::Deadline.into());
        }
        let branches = run.get(&self.output).expect("successful release").clone();
        // Learn from observed executions, including valid cache hits, only after successful release.
        self.predictor
            .lock()
            .expect("predictor poisoned")
            .observe(branches.iter().map(|branch| branch.id));
        let nodes = run
            .nodes
            .iter()
            .map(|node| NodeTiming {
                id: node.id.clone(),
                state: format!("{:?}", node.state),
                dependency_us: node.dependency_wait.as_micros(),
                queue_us: node.scheduler_wait.as_micros(),
                execution_us: node.elapsed.as_micros(),
            })
            .collect();
        Ok(Report {
            elapsed_us: started.elapsed().as_micros(),
            counts: self.counts().since(before),
            branches,
            nodes,
        })
    }
}

fn decision(permit: &Permit) -> Decision {
    permit
        .check()
        .map(|()| Decision::Continue)
        .unwrap_or_else(|error| error.decision())
}

async fn branch(work: Arc<Work>, role: &'static str) -> Result<Branch, Error> {
    let started = Instant::now();
    work.permit.check()?;
    let content = match role {
        "dependencies" => &work.project.manifest,
        "source" => &work.project.source,
        _ => return Err(Error::Failed("unknown_fragment")),
    };
    let key = digest(
        &serde_json::to_vec(&(&work.permit.scope, &work.shared.revision, role, content))
            .expect("serializable cache key"),
    );
    let cached = if work.shared.settings.reuse {
        work.shared
            .cache
            .lock()
            .map_err(|_| Error::Failed("cache_poisoned"))?
            .get(&key, Instant::now())
    } else {
        None
    };
    let span = tracing::info_span!(
        "waypilot.fragment",
        fragment = role,
        disposition = if cached.is_some() {
            "reused"
        } else {
            "executed"
        }
    );
    async {
        let (disposition, output) = if let Some(output) = cached {
            work.permit.check()?;
            ("reused", output)
        } else {
            let tools = if role == "dependencies" {
                ["parse_manifest", "dependency_index"]
            } else {
                ["parse_source", "source_index"]
            };
            let deadline = work.deadline.into();
            let mut value = json!(content);
            for tool in tools {
                work.permit.check()?;
                value = work
                    .shared
                    .pool
                    .call(role, tool, value, deadline)
                    .await
                    .map_err(|_| Error::Failed("mcp_call_failed"))?;
            }
            work.permit.check()?;
            if work.shared.settings.reuse && Instant::now() < work.deadline {
                work.shared
                    .cache
                    .lock()
                    .map_err(|_| Error::Failed("cache_poisoned"))?
                    .insert(key, value.clone(), work.shared.settings.ttl, Instant::now());
            }
            ("executed", value)
        };
        if Instant::now() >= work.deadline {
            return Err(Error::Deadline);
        }
        Ok(Branch {
            id: role,
            disposition,
            elapsed_us: started.elapsed().as_micros(),
            output,
        })
    }
    .instrument(span)
    .await
}
