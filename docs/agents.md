# Incident agents

The owned workbench models two incidents: checkout errors caused by a connection-limit
change (`INC-101`), and an image-processing backlog caused by insufficient workers
(`INC-202`). An investigator reads the incident, runs a parallel triage graph, follows
the selected runbook, and appends a local note. A read-only reviewer checks the proposal
against its own fresh observations. Neither agent can change infrastructure.

## Run locally

Requires Python 3.11+ and the built Waypilot binary:

```sh
cargo build --locked
python3 -m venv target/sdk-venv
target/sdk-venv/bin/python -m pip install -r requirements-dev.txt
bash scripts/agent_demo.sh --output target/agents/demo.json
bash scripts/agent_demo.sh --incident INC-202 --transport stdio
```

The default `policy` mode needs no model key. It is an evidence-driven state machine,
not an LLM or a recorded transcript. The two incidents and a recovered service choose
different runbooks and actions. `--agent investigator`, `--agent reviewer`, `--repeat N`
and `--no-reuse` select a role, repeat the task or disable caching.

Each run starts an official `mcp==2.0.1` HTTP server and a real Waypilot process on
loopback, uses temporary credentials and deletes its temporary configuration afterward.
The client uses the same official SDK over HTTP or stdio ingress. The two logical
upstreams share the workbench but have separate MCP sessions, so telemetry and
knowledge reads can overlap. Notes are bounded, in-memory fixture effects.
Stdio starts a separate gateway per agent session, with no shared cache or HTTP
admin statistics across sessions; use HTTP for cross-session reuse comparisons.

## Use a model or another gateway

Model mode uses [Responses function calling](https://developers.openai.com/api/docs/guides/function-calling).
It discovers MCP tools, sends their schemas to the selected model and returns each tool
result to the next model turn. It preserves reasoning items, disables response storage,
and reports model token usage. Supply a model available to your account:

```sh
export OPENAI_API_KEY='your-key'
bash scripts/agent_demo.sh --mode model --model YOUR_MODEL --repeat 1
```

`--model-endpoint` accepts another HTTPS or loopback Responses endpoint;
`--model-key-env` selects its credential variable. Chat Completions is not supported.
Model mode sends the task, tool schemas and observed tool data to that endpoint.

To use tools already configured on another Waypilot gateway:

```sh
export WAYPILOT_TOKEN='your-gateway-token'
bash scripts/agent_demo.sh --mode model --model YOUR_MODEL \
  --gateway-url https://gateway.example/mcp --agent reviewer --repeat 1 \
  --task 'Investigate the checkout incident using the available tools; recommend a mitigation.'
```

Use a principal with the intended tool grants. The reviewer role's write restriction
comes from the owned gateway configuration; its prompt does not enforce remote
permissions. Unknown remote tools execute sequentially. Only the owned read contract
enables parallel agent batches; tool annotations alone do not establish independence.

## Verify and compare

```sh
target/sdk-venv/bin/python scripts/check_agents.py target/debug/waypilot
cargo build --locked --release
target/sdk-venv/bin/python scripts/benchmark_agents.py target/release/waypilot \
  --runs 20 --output target/agents/benchmark.json
target/sdk-venv/bin/python scripts/benchmark_agents.py target/release/waypilot \
  --runs 128 --concurrency 32 --output target/agents/concurrent.json
```

Regression tests cover changed decisions, same-value resource revisions, descendant
invalidation, independent-branch reuse, overlapping reads, uncached equivalence, exact
note counts, denied writes, revocation, failures, deadlines, budgets and concurrent
agents. A local fake provider tests the model wire loop; CI never calls a paid model.
Live model reasoning quality requires a separate run with credentials.

The benchmark runs the same two incidents and write count with reuse disabled and
state-validated reuse enabled. Each mode starts from an empty cache. It reports
p50/p95/p99, completed agents per second, business reads, validation RPCs, writes and
total upstream calls. Model calls are zero. This measures the local tool workload,
not LLM latency or arbitrary remote-server performance. The 32-agent workload is
separate from the existing [1,000-client gateway qualification](testing.md#load-test).

Local release measurements on 2026-10-17, macOS ARM64, Python 3.14.7, with 30 ms
business tools (1 ms validators); cold starts are included:

| Workload | Mode | p50 / p95 ms | Business / validation RPCs |
| --- | --- | --- | --- |
| 20 agents, serial | Uncached | 216 / 219 | 140 / 0 |
| 20 agents, serial | Validated reuse | 84 / 251 | 12 / 280 |
| 128 agents, concurrency 32 | Uncached | 399 / 528 | 896 / 0 |
| 128 agents, concurrency 32 | Validated reuse | 760 / 830 | 12 / 1792 |

Both modes returned matching tool observations and decisions, with one note per agent.
The concurrent run reversed the mode order (`--reuse-first`). Reuse reduced reads but
increased total RPCs and lost throughput under concurrency. These are workload-specific
results; the validation overhead is a performance limit to address, not a speedup claim.

Reports include discovery, planning and per-tool elapsed times and gateway dispositions.
Set the existing [OTLP environment variables](gateway.md#tracing) to export gateway
preparation, cache, validation and timed graph-node spans. `check_otel.py` exercises the
agent workload against a local collector. Agent planning is timed in JSON; it does not
currently produce OTLP spans or share a trace context across separate MCP calls.

Every turn is bounded by the run timeout, ten planning turns and 24 tool calls, with
at most four calls per batch. Argument validation precedes batch dispatch. Failed
tools, access faults and exhausted budgets stop the run; effects are never retried.
Cancellation cannot undo a note already appended upstream. State validation applies
to individual gateway calls and authored graph dependencies, not an atomic snapshot
of an entire agent conversation.

## Sustained E2E

```sh
target/sdk-venv/bin/python load/agent_soak.py target/release/waypilot \
  --seconds 600 --wave-seconds 20 --rate 12 --concurrency 32 \
  --revision "$(git rev-parse HEAD)" --output-dir target/agent-soak
```

This runs ten minutes per mode: uncached, then validated reuse. Each 20-second wave
starts 32 agents together and continues at 12 agents/second. Three quarters investigate
and write; the rest review earlier proposals. Between drained waves the upstream changes
values, advances revisions without changing values, or remains unchanged. Every decision
is checked against a separate scenario oracle, and normalized structured observations
must match across modes. A final upstream ledger must contain exactly one note for each
successful investigation and none for reviews.

The runner checks session/call/singleflight/cleanup drainage, cache bounds, descriptors,
RSS and sampled OTLP node/validation spans. It writes per-wave progress, resource samples,
per-run evidence hashes and a final `summary.json`; a missing or non-passing summary is
not success. The default profile stays below the fixture's 10,000-note cap. It is an
agent workload at 32 concurrency, distinct from the 1,000-client/500-call qualification.
State changes occur at barriers; the separate state suite tests mutations during calls.
An eight-second-per-mode version runs in normal verification and CI.

Run `target/sdk-venv/bin/python load/agent_faults.py target/release/waypilot --iterations 10`
for controlled mutation, revocation, read-error, deadline and cancellation races during
agent execution. It checks exact RPC error classes, recovery, upstream dispatch counts
and cancellation after a note has already been applied. A rendezvous fixture separately
requires both independent graph branches to arrive before either can finish; parallel
execution does not depend on a short wall-clock overlap happening by chance.
The fault runner also demonstrates the inter-call boundary: changing state after a graph
returns can leave the investigator's later note based on an earlier observation. A fresh
review detects that change; the gateway does not provide a conversation-wide commit barrier.
