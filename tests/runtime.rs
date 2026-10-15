use std::{path::PathBuf, time::Duration};
use waypilot::{mcp::Fixture, Engine, Permit, Project, Scope, Settings};

fn fixture(boot: u64, call: u64) -> Fixture {
    Fixture::new(
        std::env::var_os("WAYPILOT_PYTHON")
            .expect("set WAYPILOT_PYTHON to an absolute Python 3 executable")
            .into(),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/tools.py"),
        boot,
        call,
    )
    .unwrap()
}
fn project() -> Project {
    Project {
        manifest: "tokio serde".into(),
        source: "fn main() {}".into(),
    }
}
fn permit(tenant: &str, subject: &str) -> Permit {
    Permit::new(Scope::new(tenant, subject).unwrap())
}
fn budget() -> Duration {
    Duration::from_secs(5)
}

#[tokio::test]
async fn whole_branch_reuse_and_partial_input_invalidation() {
    let engine = Engine::new(fixture(0, 0), Settings::default()).unwrap();
    let actor = permit("a", "alice");
    let cold = engine
        .run(actor.clone(), project(), budget())
        .await
        .unwrap();
    assert_eq!(cold.counts.calls, 4);
    assert_eq!(cold.counts.spawned, 2);
    let repeat = engine
        .run(actor.clone(), project(), budget())
        .await
        .unwrap();
    assert_eq!(repeat.counts.calls, 0);
    assert!(repeat.branches.iter().all(|b| b.disposition == "reused"));
    assert_eq!(repeat.branches[0].output, cold.branches[0].output);
    let mut changed = project();
    changed.source.push_str("fn added() {}");
    let changed = engine.run(actor, changed, budget()).await.unwrap();
    assert_eq!(changed.counts.calls, 2);
    assert_eq!(changed.branches[0].disposition, "reused");
    assert_eq!(changed.branches[1].disposition, "executed");
    assert_ne!(changed.branches[1].output, cold.branches[1].output);
}

#[tokio::test]
async fn identity_boundaries_do_not_share_results() {
    let engine = Engine::new(fixture(0, 0), Settings::default()).unwrap();
    for actor in [
        permit("a", "alice"),
        permit("b", "alice"),
        permit("b", "bob"),
    ] {
        assert_eq!(
            engine
                .run(actor, project(), budget())
                .await
                .unwrap()
                .counts
                .calls,
            4
        );
    }
}

#[tokio::test]
async fn revoked_identity_cannot_read_cached_results_or_prewarm() {
    let engine = Engine::new(fixture(0, 0), Settings::default()).unwrap();
    let actor = permit("a", "alice");
    engine
        .run(actor.clone(), project(), budget())
        .await
        .unwrap();
    actor.revoke();
    let counts = engine.counts();
    assert!(engine
        .run(actor.clone(), project(), budget())
        .await
        .is_err());
    assert!(engine.prewarm_predictions(&actor, budget()).await.is_err());
    assert_eq!(engine.counts().calls, counts.calls);
}

#[tokio::test]
async fn prewarming_uses_observed_profile_and_issues_no_tool_calls() {
    let fixture = fixture(0, 0);
    let trained = Engine::new(fixture.clone(), Settings::default()).unwrap();
    let actor = permit("a", "alice");
    assert!(trained.predictions().is_empty());
    trained
        .run(actor.clone(), project(), budget())
        .await
        .unwrap();
    trained
        .run(actor.clone(), project(), budget())
        .await
        .unwrap();
    let cold = Engine::new(fixture, Settings::default()).unwrap();
    cold.load_profile(trained.profile()).unwrap();
    assert_eq!(cold.prewarm_predictions(&actor, budget()).await.unwrap(), 2);
    assert_eq!(cold.counts().spawned, 2);
    assert_eq!(cold.counts().calls, 0);
    assert_eq!(
        cold.run(actor, project(), budget())
            .await
            .unwrap()
            .counts
            .spawned,
        0
    );
}

#[tokio::test]
async fn foreign_and_invalid_prediction_profiles_are_rejected() {
    let engine = Engine::new(fixture(0, 0), Settings::default()).unwrap();
    let mut foreign = engine.profile();
    foreign["revision"] = "other".into();
    assert!(engine.load_profile(foreign).is_err());
    let mut invalid = engine.profile();
    invalid["predictor"]["servers"]["unconfigured"] = 1.into();
    assert!(engine.load_profile(invalid).is_err());
}

#[tokio::test]
async fn deadline_stops_cold_start_before_tool_dispatch() {
    let engine = Engine::new(fixture(100, 0), Settings::default()).unwrap();
    assert!(engine
        .run(permit("a", "alice"), project(), Duration::from_millis(5))
        .await
        .is_err());
    assert_eq!(engine.counts().calls, 0);
}

#[tokio::test]
async fn revocation_during_io_prevents_followup_tools_and_release() {
    let engine = Engine::new(fixture(0, 100), Settings::default()).unwrap();
    let actor = permit("a", "alice");
    let execution = engine.run(actor.clone(), project(), budget());
    let revoke = async {
        tokio::time::timeout(budget(), async {
            while engine.counts().calls < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        actor.revoke();
    };
    let (result, ()) = tokio::join!(execution, revoke);
    assert!(result.is_err());
    assert_eq!(engine.counts().calls, 2);
}

#[tokio::test]
async fn baseline_and_parallel_execution_have_identical_outputs() {
    let actor = permit("a", "alice");
    let serial = Engine::new(
        fixture(0, 0),
        Settings {
            reuse: false,
            max_parallel: 1,
            ..Settings::default()
        },
    )
    .unwrap();
    let parallel = Engine::new(
        fixture(0, 0),
        Settings {
            reuse: false,
            ..Settings::default()
        },
    )
    .unwrap();
    let a = serial
        .run(actor.clone(), project(), budget())
        .await
        .unwrap();
    let b = parallel.run(actor, project(), budget()).await.unwrap();
    assert_eq!(
        a.branches.iter().map(|b| &b.output).collect::<Vec<_>>(),
        b.branches.iter().map(|b| &b.output).collect::<Vec<_>>()
    );
    assert!(a
        .nodes
        .iter()
        .any(|n| n.id == "authorize_release" && n.state == "Succeeded"));
    assert_eq!(a.counts.calls, 4);
    assert_eq!(b.counts.calls, 4);
}

#[tokio::test]
async fn overlapping_runs_are_rejected_without_disturbing_the_active_run() {
    let engine = Engine::new(fixture(0, 100), Settings::default()).unwrap();
    let actor = permit("a", "alice");
    let active = engine.run(actor.clone(), project(), budget());
    let overlapping = async {
        tokio::time::timeout(budget(), async {
            while engine.counts().calls < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(engine.run(actor, project(), budget()).await.is_err());
    };
    let (result, ()) = tokio::join!(active, overlapping);
    assert!(result.is_ok());
    assert_eq!(engine.counts().calls, 4);
}
