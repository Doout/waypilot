use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use waypilot::{mcp::Fixture, Engine, Permit, Project, Scope, Settings};

fn project() -> Project {
    Project {
        manifest: "tokio = 1\nserde = 1".into(),
        source: "fn answer() -> u32 { 42 }".into(),
    }
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[(sorted.len() * percentile).div_ceil(100).saturating_sub(1)]
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = wayframe_observability::init()?;
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("demo") => {}
        _ => {
            println!("Usage: waypilot demo --python /absolute/path/python3 [--samples 5]\n\nRuns real local MCP subprocesses with simulated tool and model delays.\nWrites raw benchmark JSON to stdout and tracing to stderr.");
            return Ok(());
        }
    }
    let mut python = std::env::var_os("WAYPILOT_PYTHON").map(PathBuf::from);
    let mut samples = 5usize;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--python" => python = Some(args.next().context("--python needs a path")?.into()),
            "--samples" => samples = args.next().context("--samples needs a count")?.parse()?,
            _ => bail!("unknown argument: {arg}"),
        }
    }
    ensure!((1..=100).contains(&samples), "samples must be 1..100");
    let fixture = Fixture::new(
        python.context("provide --python or WAYPILOT_PYTHON")?,
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/tools.py"),
        60,
        15,
    )?;
    let permit = Permit::new(Scope::new("demo", "developer")?);
    let budget = Duration::from_secs(5);
    // Two successful real executions supply the observations used by the warmup predictor.
    let training = Engine::new(
        fixture.clone(),
        Settings {
            reuse: false,
            ..Settings::default()
        },
    )?;
    training.run(permit.clone(), project(), budget).await?;
    training.run(permit.clone(), project(), budget).await?;
    let profile = training.profile();
    let mut cases = Vec::new();
    let mut expected: Option<Vec<Value>> = None;
    for (name, parallel, warm) in [
        ("serial_cold", 1, false),
        ("parallel_cold", 2, false),
        ("parallel_predicted_prewarm", 2, true),
    ] {
        let mut trials = Vec::new();
        let mut elapsed = Vec::new();
        for _ in 0..samples {
            let engine = Engine::new(
                fixture.clone(),
                Settings {
                    reuse: false,
                    max_parallel: parallel,
                    ..Settings::default()
                },
            )?;
            engine.load_profile(profile.clone())?;
            let started = Instant::now();
            if warm {
                let (result, ()) = tokio::join!(
                    engine.prewarm_predictions(&permit, budget),
                    tokio::time::sleep(Duration::from_millis(100))
                );
                result?;
            } else {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let report = engine.run(permit.clone(), project(), budget).await?;
            let e2e_us = started.elapsed().as_micros();
            let outputs: Vec<_> = report
                .branches
                .iter()
                .map(|branch| branch.output.clone())
                .collect();
            if let Some(expected) = &expected {
                ensure!(*expected == outputs, "baseline output mismatch");
            } else {
                expected = Some(outputs);
            }
            elapsed.push(e2e_us);
            trials.push(json!({"e2e_us":e2e_us,"total_counts":engine.counts(),"run":report}));
        }
        cases.push(json!({"name":name,"p50_us":percentile(&elapsed,50),"p95_us":percentile(&elapsed,95),"trials":trials}));
    }
    let incremental = Engine::new(fixture, Settings::default())?;
    let first = incremental.run(permit.clone(), project(), budget).await?;
    let repeat = incremental.run(permit.clone(), project(), budget).await?;
    let mut changed = project();
    changed.source.push_str("\nfn other() {}\n");
    let changed = incremental.run(permit, changed, budget).await?;
    ensure!(
        repeat.counts.calls == 0 && changed.counts.calls == 2,
        "incremental reuse failed"
    );
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version":1,
            "methodology":{"samples":samples,"model":"simulated 100ms reasoning gap; no LLM calls","fixture_boot_ms":60,"fixture_tool_ms":15,"transport":"real MCP stdio","prewarm_training_runs":2,"training_counts":training.counts(),"statistics":"nearest-rank; small samples are descriptive, not a production SLO","timing":"e2e includes model gap and prewarming; per-run counters cover engine activity during run, total_counts includes prewarming"},
            "output_equivalence":true,"cases":cases,"incremental":{"cold":first,"repeat":repeat,"source_changed":changed},"profile":profile
        }))?
    );
    Ok(())
}
