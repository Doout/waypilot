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
        Some("--version") => {
            println!("waypilot {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some("demo") => fixture_demo(args).await,
        Some("serve") | Some("serve-stdio") => {
            let stdio = std::env::args().nth(1).as_deref() == Some("serve-stdio");
            let mut config = None;
            let mut user = None;
            let mut listen = None;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--config" => {
                        config = Some(PathBuf::from(args.next().context("--config needs a file")?))
                    }
                    "--user" => user = Some(args.next().context("--user needs an ID")?),
                    "--listen" => {
                        listen = Some(args.next().context("--listen needs an address")?.parse()?)
                    }
                    _ => bail!("unknown argument: {arg}"),
                }
            }
            let mut config = waypilot::config::Config::load(&config.context("provide --config")?)?;
            if let Some(listen) = listen {
                config.listen = listen;
            }
            let listen = config.listen;
            let gateway = waypilot::gateway::Gateway::new(config)?;
            if stdio {
                return waypilot::gateway::serve_stdio(
                    gateway,
                    &user.context("stdio requires --user")?,
                )
                .await;
            }
            let listener = tokio::net::TcpListener::bind(listen).await?;
            println!(
                "{}",
                json!({"listen":listener.local_addr()?.to_string(),"transport":"streamable_http"})
            );
            gateway.serve_http(listener, shutdown_signal()).await?;
            Ok(())
        }
        Some(command @ ("list" | "call")) => {
            let mut url = None;
            let mut token_env = None;
            let mut tool = None;
            let mut arguments = json!({});
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--url" => url = Some(args.next().context("--url needs an endpoint")?),
                    "--token-env" => {
                        token_env = Some(args.next().context("--token-env needs a name")?)
                    }
                    "--tool" => tool = Some(args.next().context("--tool needs a name")?),
                    "--args" => {
                        arguments =
                            serde_json::from_str(&args.next().context("--args needs JSON")?)?
                    }
                    _ => bail!("unknown argument: {arg}"),
                }
            }
            ensure!(arguments.is_object(), "--args must be an object");
            let client = waypilot::upstream::Client::new(waypilot::upstream::Endpoint::Http {
                url: url.context("provide --url")?,
                bearer_env: token_env,
                allow_insecure_http: false,
            })?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            let result = if command == "list" {
                json!({"tools":client.catalog(deadline).await?.tools.as_ref()})
            } else {
                client
                    .call(&tool.context("provide --tool")?, arguments, deadline)
                    .await?
            };
            println!("{}", serde_json::to_string_pretty(&result)?);
            client.close().await;
            Ok(())
        }
        Some("admin") => {
            let mut url = None;
            let mut token_env = None;
            let mut action = "stats".to_string();
            let mut user = None;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--url" => url = Some(args.next().context("--url needs a base URL")?),
                    "--token-env" => {
                        token_env = Some(args.next().context("--token-env needs a variable name")?)
                    }
                    "--action" => {
                        action = args
                            .next()
                            .context("--action needs stats, users, invalidate, enable or disable")?
                    }
                    "--user" => user = Some(args.next().context("--user needs an ID")?),
                    _ => bail!("unknown argument: {arg}"),
                }
            }
            let base = url.context("provide --url")?;
            waypilot::upstream::Endpoint::Http {
                url: base.clone(),
                bearer_env: None,
                allow_insecure_http: false,
            }
            .validate()?;
            let token = std::env::var(token_env.context("provide --token-env")?)?;
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .timeout(Duration::from_secs(10))
                .build()?;
            let request = match action.as_str() {
                "stats" => client.get(format!("{}/admin/stats", base.trim_end_matches('/'))),
                "users" => client.get(format!("{}/admin/users", base.trim_end_matches('/'))),
                "invalidate" => {
                    client.post(format!("{}/admin/invalidate", base.trim_end_matches('/')))
                }
                "enable" | "disable" => client
                    .post(format!("{}/admin/users/update", base.trim_end_matches('/')))
                    .json(
                        &json!({"id":user.context("provide --user")?,"enabled":action == "enable"}),
                    ),
                _ => bail!("unknown administrative action"),
            };
            let response = request.bearer_auth(token).send().await?;
            ensure!(
                response.status().is_success(),
                "administrative HTTP status {}",
                response.status().as_u16()
            );
            let result: Value = response.json().await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
        _ => {
            println!("Waypilot MCP runtime\n\n  serve --config FILE [--listen 127.0.0.1:7400]\n  serve-stdio --config FILE --user ID\n  list --url URL [--token-env NAME]\n  call --url URL [--token-env NAME] --tool NAME --args JSON\n  admin --url BASE --token-env NAME --action stats|users|invalidate|enable|disable [--user ID]\n  demo --python /absolute/path/python3 [--samples 5]\n\nHTTP clients use Streamable HTTP. Logs and traces go to stderr.");
            Ok(())
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("signal handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

async fn fixture_demo(mut args: impl Iterator<Item = String>) -> Result<()> {
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
