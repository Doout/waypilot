# Waypilot

An experimental adaptive tool execution runtime built on [Wayframe](https://github.com/Doout/wayframe).

Waypilot explores how execution traces, MCP environment prewarming, speculative work and incremental branch reuse can work together. The first runnable slice uses real local MCP subprocesses and an authored graph to establish the execution and validation baseline.

## Current implementation

- Two independent, two-tool branches run through Wayframe and join before a synchronous collection step and an authorized release.
- Exact branch results are retained in a bounded, expiring, identity-scoped cache. Changing the source input recomputes the source branch while retaining the dependency branch.
- A frequency baseline learns server use from successful execution traces. Its exported profile can prewarm a fresh runtime's MCP processes, initialization and tool catalogs during a simulated model reasoning gap.
- Current authorization runs before work and before release, including cache hits. Tool calls check the demonstration grant before each invocation.
- Wayframe supplies bounded graph scheduling, deadlines, supervised stdio processes and OpenTelemetry export. Each engine admits one active run; its two independent branches can run concurrently.

**Not implemented yet:** automatic workflow mining, speculative `tools/call`, joining shared in-flight branches, external mutable-resource invalidation, durable execution, HTTP MCP sessions, or a production authentication service. The `Permit` API is an embedding/test control, not user authentication. See the [delivery plan](docs/plan.md) and [Wayframe changes](docs/wayframe-plan.md).

## Setup

Requirements: Git, Rust **1.99.0**, Python **3.11+**. No model API key, GPU, Docker or paid service is needed.

```sh
git clone git@github.com:Doout/waypilot.git
cd waypilot
rustup show
export WAYPILOT_PYTHON="$(command -v python3)"
cargo run --locked -- demo --samples 3
```

`WAYPILOT_PYTHON` must be an absolute executable path. You can also pass `--python /absolute/path/python3` to the CLI. Commands are invoked directly; no shell is used to launch tools. Fixtures receive a cleared environment.

The Wayframe dependencies are pinned to commit `ec4d27224ec213eabc2eae9b474b522d1cab8874`, the graph-specialization implementation associated with [Wayframe PR #10](https://github.com/Doout/wayframe/pull/10). That PR was open when this project was scaffolded. Cargo fetches the exact commit, so setup does not require a sibling checkout or an uncommitted framework patch. Update the pin and rerun qualification when adopting another revision.

## Test

```sh
export WAYPILOT_PYTHON="$(command -v python3)"
bash scripts/verify.sh
```

Verification checks formatting, Rust tests, Clippy, Python fixture syntax and live OTLP/HTTP JSON export. Integration tests launch actual MCP subprocesses and assert call counts, output equivalence, branch reuse, identity isolation, revocation, profile validation and deadline behavior. They are local integration evidence, not broad MCP interoperability certification.

## Benchmark

```sh
mkdir -p target
cargo run --locked --release -- demo --samples 20 > target/benchmark.json
```

The JSON includes every trial, nearest-rank p50/p95, tool calls, process starts, per-node timings and output-equivalence checks. Cases compare serial cold execution, ordinary parallel cold execution, and learned prewarming with the same parallel graph. Training takes two real executions and its work is reported separately. Incremental cases show the first execution, an identical repeat, and a source-only change.

Startup and tool waits are configurable fixture delays; the model gap is a simulated 100 ms wait. Results measure this workload on the running machine. They do not establish real-agent quality, production tail latency or a general speedup. A small sample count is for inspection; do not interpret its p95 as a production SLO. Per-run counter deltas include any engine activity during the interval; `total_counts` includes prewarming. Counters are process-local, not per-request accounting under concurrent callers.

See [benchmark methodology](docs/benchmarks.md) for the comparison matrix and acceptance conditions for later milestones, and [initial local qualification](docs/validation.md) for measured results and their limits.

## Tracing

```sh
OTEL_SERVICE_NAME=waypilot \
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces \
OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/json \
cargo run --locked -- demo --samples 3
```

The exporter also supports `http/protobuf`. Run `python3 scripts/check_otel.py target/debug/waypilot` to start an ephemeral local collector and verify actual exported spans without installing a tracing backend. Set `WAYPILOT_PYTHON` first.

Spans cover `waypilot.run`, `waypilot.fragment`, `mcp.prewarm`, `mcp.tool`, Wayframe workflow nodes and stdio transport. Fragment dispositions distinguish `executed` from `reused`; workflow reports separate dependency wait, scheduling wait and execution. Arguments, source contents, identities and result payloads are excluded from span fields and prediction profiles. Demo result JSON intentionally contains fixture outputs.

## Architecture and scope

[Architecture](docs/architecture.md) describes the current boundaries and validity model. [The plan](docs/plan.md) sequences prewarming, reuse, speculative execution, trace-derived fragments and dynamic continuations. [Wayframe's plan](docs/wayframe-plan.md) maps the reusable requirements to its existing crates and defines a cross-repository delivery process.

Waypilot owns MCP sessions, trusted tool contracts, prediction and result validity. Wayframe remains a transport-neutral execution framework. New framework primitives should be justified by a working Waypilot scenario and a second non-agent workflow before their API is generalized.

Apache-2.0. This is an experimental developer project.
