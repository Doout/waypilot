# Waypilot

An MCP execution service built on [Wayframe](https://github.com/Doout/wayframe). Point an MCP client at Waypilot and route tool calls to local processes or remote MCP servers. Explicit workflows can run independent tool branches in parallel, join their results, and continue with synchronous work.

The service includes authenticated HTTP and local stdio ingress, tool grants, rate limits, bounded connections and memory admission, durable user revocation, cancellation, exact result reuse, environment prewarming, and OpenTelemetry export. It works without an agent or model API. Automatic workflow mining and speculative tool invocation remain planned work.

## Run the demo

Requirements: Git, Rust **1.99.0**, Python **3.11+**. No Docker, GPU, model key, or paid service is required for the owned fixtures.

```sh
git clone git@github.com:Doout/waypilot.git
cd waypilot
export WAYPILOT_PYTHON="$(command -v python3)"
bash scripts/demo.sh
```

This builds Waypilot, starts an owned HTTP MCP server and the gateway on ephemeral loopback ports, sends real MCP requests, checks their results, and shuts everything down. Two more upstream servers run over stdio. Credentials are generated for the run and kept out of its JSON output.

Expected evidence:

| Operation | Actual upstream tool calls |
| --- | ---: |
| Prepare two MCP environments | 0 |
| Run the four-tool `waypilot__inspect` workflow | 4 |
| Repeat the same inputs | 0 |
| Change only the source input | 2 |
| Invoke the mutable counter twice | 2 |

To also send a tool call through the gateway to a third-party server:

```sh
bash scripts/demo.sh --remote
```

This calls [Microsoft Learn's public MCP endpoint](https://learn.microsoft.com/en-us/training/support/mcp) using a fixed documentation query. Internet access is required; availability and results depend on the external service. Result reuse is disabled for that server. The default demo and CI use owned fixtures and remain independent of it.

## Run a gateway and send calls

```sh
export WAYPILOT_PYTHON="$(command -v python3)"
export WAYPILOT_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export WAYPILOT_ADMIN_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
cargo build --locked

target/debug/waypilot serve --config examples/local.toml &
waypilot_pid=$!
until curl --silent --fail http://127.0.0.1:7400/healthz >/dev/null; do
  kill -0 "$waypilot_pid" || exit 1
  sleep 0.1
done

target/debug/waypilot list \
  --url http://127.0.0.1:7400/mcp --token-env WAYPILOT_TOKEN

target/debug/waypilot call \
  --url http://127.0.0.1:7400/mcp --token-env WAYPILOT_TOKEN \
  --tool waypilot__inspect \
  --args '{"manifest":"tokio serde","source":"fn main() {}"}'

# Repeat that call for reuse, or change source to recompute its branch.
# When finished:
kill "$waypilot_pid"
```

Keep the service in the foreground by omitting `&`. HTTP binds to `127.0.0.1:7400` by default. `--listen 127.0.0.1:0` selects a free port, printed in a startup JSON record. Tool names are prefixed with their configured server ID, for example `learn__microsoft_docs_search`; authored workflows use `waypilot__`.

Use `examples/microsoft-learn.toml` instead of `examples/local.toml` to serve the public remote upstream. Its search call is:

```sh
target/debug/waypilot call \
  --url http://127.0.0.1:7400/mcp --token-env WAYPILOT_TOKEN \
  --tool learn__microsoft_docs_search --args '{"query":"MCP Streamable HTTP"}'
```

For a local MCP client that launches its server over stdio, use the absolute path to the built binary with arguments `serve-stdio --config /absolute/path/gateway.toml --user developer`. Supply the configured environment variables to that process. The operator selects the local principal; stdout contains only MCP JSON-RPC, and logs go to stderr.

## Configure your own servers

Configuration is TOML; secrets are read from named environment variables. Start with [the local workflow](examples/local.toml), [a remote server](examples/microsoft-learn.toml), or [the 1,000-client service configuration](examples/service.toml). A minimal remote configuration is:

```toml
listen = "127.0.0.1:7400"
admin_token_env = "WAYPILOT_ADMIN_TOKEN"
request_timeout_ms = 30000

[[users]]
id = "developer"
tenant = "local"
token_env = "WAYPILOT_TOKEN"
tools = ["docs__*"]
requests_per_second = 10
burst = 20

[[servers]]
id = "docs"
revision = "operator-contract-v1"
prewarm = true
[servers.endpoint]
transport = "http"
url = "https://your-server.example/mcp"
# bearer_env = "DOCS_MCP_TOKEN"
```

HTTP upstream credentials are separate from gateway user tokens. Redirects and environment proxy discovery are disabled. HTTPS is required except for loopback HTTP or explicit `allow_insecure_http = true`. Operator configuration is trusted: it chooses destinations and executable programs. For network hosting, terminate TLS at your ingress and apply your deployment's connection and request limits. See [operations and protocol support](docs/gateway.md).

Stdio endpoints use `transport = "stdio"`, an absolute `command`, `args`, and optional `inherit_env` names. `command = "env:WAYPILOT_PYTHON"` resolves an executable from the environment; `{config_dir}` in arguments resolves relative fixture paths. Processes launch directly with a cleared environment plus the explicit allowlist.

Result reuse is off by default. Enable it only for a tool whose complete dependency and freshness contract you control:

```toml
[servers.tools.echo]
reusable = true
cache_ttl_ms = 60000
```

The key includes tenant, user, grants, generation, server contract revision, catalog digest, tool name and exact arguments. Current access is checked before lookup, before dispatch and before release. A TTL bounds retention; it does not detect external changes. Unknown or mutable tools bypass reuse, and execution errors are never retained. Server `readOnlyHint` annotations do not grant reuse eligibility.

The [local configuration](examples/local.toml) also demonstrates authored workflow bindings, independent branches, and a synchronous collection step. Dependencies must refer to earlier steps; unresolved inputs fail before tool dispatch. Reusing every tool in an unchanged branch avoids all of that branch's calls. Arbitrary whole subgraphs are not yet stored as one reusable fragment.

## Users and controls

Users, tenants, static bearer credentials, tool grants and token-bucket limits are configured in TOML. Grants accept exact tool names, `server__*`, or `*`. A workflow requires its own grant and grants for every child tool. Tool invocations, including reuse hits, consume tokens; a workflow also consumes one token for its entry. Each prewarm batch consumes one token and can prepare at most two opted-in servers.

```sh
target/debug/waypilot admin --url http://127.0.0.1:7400 \
  --token-env WAYPILOT_ADMIN_TOKEN --action stats

target/debug/waypilot admin --url http://127.0.0.1:7400 \
  --token-env WAYPILOT_ADMIN_TOKEN --action users

target/debug/waypilot admin --url http://127.0.0.1:7400 \
  --token-env WAYPILOT_ADMIN_TOKEN --action disable --user developer
```

Actions also include `enable` and `invalidate`. Disabling a user cancels their sessions, prevents queued dispatch and blocks cached results. Enabling requires a new session. Every administrative change increments the global generation and clears retained results conservatively. Set `state_path = "../var/policy.json"` to persist enable/disable overrides across restart. An update is acknowledged after an atomic, synced write and in-memory publication; an unwritable state file fails startup. One process owns that file. Without `state_path`, overrides remain in memory. Users, credentials, grants and rate settings still come from TOML/environment. There is no identity provider, self-service registration or OAuth.

## Test

```sh
python3 -m venv target/sdk-venv
target/sdk-venv/bin/python -m pip install -r requirements-dev.txt
export WAYPILOT_PYTHON="$(command -v python3)"
export WAYPILOT_SDK_PYTHON="$PWD/target/sdk-venv/bin/python"
bash scripts/verify.sh
```

Verification runs Rust tests, formatting, Clippy with warnings denied, Python syntax checks, live JSON/SSE and stdio peers, the independent official Python MCP SDK, an actual OTLP collector, and a 64-client/32-call scale smoke test with sampled traces. Regressions cover reuse and partial recomputation, identity isolation, grants, rate limits, learned preparation, session binding, deadlines, cancellation, revocation while queued, malformed requests, and lost responses after effects. CI repeats this on Linux and macOS. Linux also qualifies 1,000 connected clients and 500 concurrent calls, including a 60-second open-loop workload at 500 calls/second. CI uploads raw qualification, benchmark and demo output even on failure.

To run the full single-node qualification locally:

```sh
cargo build --locked --release
python3 load/qualify.py target/release/waypilot \
  --clients 1000 --concurrency 500 --seconds 60 --rate 500 \
  --output target/scale-qualification.json
```

Add `--trace-ratio 0.1` for an owned OTLP collector and sampled export. The suite checks actual upstream concurrency, parallel workflows, overload, large bodies, cancellation, deadlines, session churn, identity isolation, no replay of uncertain effects, queued revocation, persistence, idle expiry and graceful shutdown. It records every sustained-load latency trial, generator lag, sampled RSS and descriptors. See [scale methodology and limits](docs/scale.md).

## Benchmarks and traces

```sh
cargo build --locked --release
python3 scripts/benchmark_gateway.py target/release/waypilot \
  --samples 100 > target/gateway-benchmark.json
cargo run --locked --release -- demo --samples 20 > target/benchmark.json
```

The first compares initialized direct and gateway MCP calls against the same owned loopback server, with result reuse disabled, equivalent outputs and alternating measurement order. The second compares serial, parallel and learned prewarming execution with controlled fixture waits and a simulated model reasoning gap. Both emit every trial and nearest-rank p50/p95. See [methodology](docs/benchmarks.md) and [measured qualification](docs/validation.md); these are local workload results, not production SLOs or agent-quality claims.

```sh
OTEL_SERVICE_NAME=waypilot \
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces \
OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/json \
target/debug/waypilot serve --config examples/local.toml
```

Spans cover HTTP ingress, MCP requests, preparation, invocation and Wayframe graph nodes. Node reports distinguish dependency wait, scheduler wait and execution; tool results add elapsed time and reuse disposition in `_meta["io.waypilot/runtime"]`. Arguments, credentials and result payloads are excluded from application trace fields. Tools themselves may log data separately. The exporter also supports `http/protobuf`.

## Scope and next steps

Supported ingress and upstream transports are stdio and classic MCP Streamable HTTP with JSON or SSE POST responses, negotiating `2025-11-25`, `2025-06-18`, or `2025-03-26`. HTTP ingress returns JSON. There is no legacy two-endpoint SSE, GET event stream, resumable delivery, server-initiated sampling/elicitation, OAuth exchange, durable jobs, or agent adapter yet. Full input JSON Schema validation is delegated to the upstream tool; workflow bindings are checked locally.

Same-key reusable requests are coalesced with Wayframe's bounded singleflight store. Calls within one upstream session are serialized; different servers and downstream sessions can run concurrently. Cancellation drops the local invocation/session, queues bounded MCP cancellation and session deletion, and permits a later distinct call to initialize afresh. Cleanup failures and overflow are observable in admin stats. Already-issued remote effects may complete. An uncertain call is never automatically retried or rolled back.

Learned prewarming uses per-user aggregate server-use frequencies to prepare permitted environments on later sessions. It sends initialization and catalog requests, never speculative `tools/call`. Automatic trace-derived workflows, dependency witnesses for mutable resources, speculative execution leases and durable continuations are future milestones in the [delivery plan](docs/plan.md).

Wayframe supplies typed graphs, mandatory gates, bounded scheduling, deadlines, supervised processes, caching/singleflight and telemetry. The service uses public APIs pinned to `ec4d27224ec213eabc2eae9b474b522d1cab8874`; it needs no uncommitted sibling checkout or framework patch. [Architecture](docs/architecture.md) and [Wayframe evolution](docs/wayframe-plan.md) describe the boundaries.

Apache-2.0. Pre-release; see the measured qualification and deployment limits before hosting.
